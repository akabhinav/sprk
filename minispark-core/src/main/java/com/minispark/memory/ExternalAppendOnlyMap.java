package com.minispark.memory;

import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.storage.BlockId;
import com.minispark.storage.BlockManager;
import com.minispark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hash map that <i>spills to disk</i> when execution memory is exhausted. The
 * Spark-shaped solution to one fat group blowing up a JVM heap. Used as the
 * map-side combine container in {@link com.minispark.api.PairRDDFunctions} and
 * therefore by every {@code reduceByKey} / {@link com.minispark.sql.execution.HashAggregateExec}.
 *
 * <p><b>Mechanism</b>:
 * <ol>
 *   <li>{@link #insert} accumulates entries in a normal {@link HashMap} and
 *       grows the consumer's execution-memory acquisition page by page.</li>
 *   <li>When {@link TaskMemoryManager#acquireExecutionMemory} can't grant any
 *       more (the pool is full <i>and</i> peers have nothing to spill), this
 *       consumer spills itself: entries are sorted by {@code hash(key)},
 *       serialized to a {@link BlockId.SpillBlock} on disk, the in-memory map
 *       is reset, and the acquired memory is released.</li>
 *   <li>{@link #iterator} merges the remaining in-memory entries with every
 *       spilled file, hash-bucket by hash-bucket. Within each bucket the
 *       same-key values are reduced with the user's combiner. Output is
 *       therefore one entry per distinct key, just like a non-spilling
 *       {@code HashMap} would have produced.</li>
 * </ol>
 *
 * <p><b>Why hashCode-based ordering instead of natural key order.</b> The
 * arbitrary {@code K} type may not be {@code Comparable}. {@code hashCode}
 * is always defined and consistent within a JVM run; collisions are resolved
 * by exact key equality inside each hash bucket. This is the same trick real
 * Spark uses.
 *
 * Real Spark equivalent: org.apache.spark.util.collection.ExternalAppendOnlyMap.
 */
public final class ExternalAppendOnlyMap<K, V> extends MemoryConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalAppendOnlyMap.class);

    /** Initial / per-grow allocation request. Real Spark calls this a "page". */
    private static final long INITIAL_ACQUIRE = 1024;
    /** Per-entry cost estimate: rough average overhead of {@code HashMap.Node + boxed key + boxed value}. */
    private static final long ENTRY_BYTES = 80;
    /** Spill-file sequence number, unique per JVM (used in SpillBlock id). */
    private static final AtomicInteger SPILL_SEQ = new AtomicInteger();

    private final RDD.SerializableBiFunction<V, V, V> combiner;
    private final BlockManager blockManager;

    private HashMap<K, V> inMemory = new HashMap<>();
    private long estimatedSize = 0;
    private long acquiredBytes = 0;
    private final List<BlockId.SpillBlock> spillFiles = new ArrayList<>();

    public ExternalAppendOnlyMap(TaskMemoryManager taskMemoryManager,
                                  RDD.SerializableBiFunction<V, V, V> combiner,
                                  BlockManager blockManager) {
        super(taskMemoryManager);
        this.combiner = combiner;
        this.blockManager = blockManager;
        if (taskMemoryManager != null) taskMemoryManager.registerConsumer(this);
    }

    /** Insert/merge one key-value pair. Triggers a spill if memory can't grow. */
    public void insert(K key, V value) {
        V existing = inMemory.get(key);
        if (existing == null) {
            inMemory.put(key, value);
            estimatedSize += ENTRY_BYTES;
            memoryUsed = estimatedSize;
        } else {
            inMemory.put(key, combiner.apply(existing, value));
            // In-place merge: no new entry, no new bytes (ignoring V's growth).
        }
        // Grow the acquired budget in coarse pages so we don't pester the
        // TaskMemoryManager on every insert.
        while (estimatedSize > acquiredBytes && taskMemoryManager != null) {
            long want = Math.max(INITIAL_ACQUIRE, estimatedSize - acquiredBytes);
            long got = taskMemoryManager.acquireExecutionMemory(want, this);
            if (got <= 0) {
                // Pool said no AND no peer could spill. Spill ourselves.
                try { spill(estimatedSize); }
                catch (IOException e) { throw new RuntimeException("spill failed", e); }
                break;
            }
            acquiredBytes += got;
            if (acquiredBytes >= estimatedSize) break;
        }
    }

    @Override
    public long spill(long required) throws IOException {
        if (inMemory.isEmpty()) return 0;
        long releasing = acquiredBytes;
        BlockId.SpillBlock spillId = writeSpill();
        spillFiles.add(spillId);
        LOG.debug("Spilled {} entries ({} bytes) to {}", inMemory.size(), releasing, spillId);
        // Reset in-memory state.
        inMemory = new HashMap<>();
        estimatedSize = 0;
        memoryUsed = 0;
        // Release back to the execution pool so peers (or our next page) can use it.
        if (taskMemoryManager != null && releasing > 0) {
            taskMemoryManager.releaseExecutionMemory(releasing, this);
        }
        acquiredBytes = 0;
        return releasing;
    }

    /**
     * Iterate every distinct key, value-combined across the in-memory tail and
     * all spilled files. Output is in hash-bucket order — not key order, since
     * K is not required to be {@code Comparable} — but each key appears at
     * most once.
     */
    public Iterator<Tuple2<K, V>> iterator() {
        if (spillFiles.isEmpty()) {
            // Fast path: nothing spilled, just iterate the map.
            return inMemory.entrySet().stream()
                    .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                    .iterator();
        }
        // Merge path: sort the in-memory tail by hash, then merge with each
        // spill file by hash bucket. Within a bucket, combine same-key entries.
        List<Iterator<Tuple2<K, V>>> streams = new ArrayList<>(spillFiles.size() + 1);
        if (!inMemory.isEmpty()) streams.add(sortedInMemoryIterator());
        for (BlockId.SpillBlock id : spillFiles) streams.add(readSpillIterator(id));
        return new MergedIterator(streams);
    }

    // ----- helpers -----

    /** Write the current in-memory map to a spill block, sorted by hash(key). */
    private BlockId.SpillBlock writeSpill() throws IOException {
        List<Map.Entry<K, V>> sorted = new ArrayList<>(inMemory.entrySet());
        sorted.sort(Comparator.comparingInt(e -> Objects.hashCode(e.getKey())));
        BlockId.SpillBlock id = new BlockId.SpillBlock(
                taskMemoryManager == null ? 0 : taskMemoryManager.taskAttemptId(),
                SPILL_SEQ.incrementAndGet());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeInt(sorted.size());
            for (Map.Entry<K, V> e : sorted) {
                oos.writeObject(e.getKey());
                oos.writeObject(e.getValue());
            }
        }
        // DISK_ONLY: spills don't compete with the storage pool we just relieved.
        blockManager.putBlock(id, baos.toByteArray(), StorageLevel.DISK_ONLY);
        return id;
    }

    /** Iterator over the in-memory map sorted by hash(key), so it can merge with spilled streams. */
    private Iterator<Tuple2<K, V>> sortedInMemoryIterator() {
        List<Map.Entry<K, V>> sorted = new ArrayList<>(inMemory.entrySet());
        sorted.sort(Comparator.comparingInt(e -> Objects.hashCode(e.getKey())));
        Iterator<Map.Entry<K, V>> it = sorted.iterator();
        return new Iterator<>() {
            public boolean hasNext() { return it.hasNext(); }
            public Tuple2<K, V> next() {
                Map.Entry<K, V> e = it.next();
                return new Tuple2<>(e.getKey(), e.getValue());
            }
        };
    }

    @SuppressWarnings("unchecked")
    private Iterator<Tuple2<K, V>> readSpillIterator(BlockId.SpillBlock id) {
        byte[] bytes = blockManager.getBlock(id).orElseThrow(
                () -> new IllegalStateException("spill block missing: " + id));
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            int n = ois.readInt();
            return new Iterator<>() {
                int remaining = n;
                public boolean hasNext() { return remaining > 0; }
                public Tuple2<K, V> next() {
                    if (remaining == 0) throw new NoSuchElementException();
                    try {
                        K k = (K) ois.readObject();
                        V v = (V) ois.readObject();
                        remaining--;
                        return new Tuple2<>(k, v);
                    } catch (IOException | ClassNotFoundException e) {
                        throw new RuntimeException("spill read failed for " + id, e);
                    }
                }
            };
        } catch (IOException e) {
            throw new RuntimeException("spill read failed for " + id, e);
        }
    }

    /**
     * N-way merge of hash-sorted streams. For each (lowest) hash bucket across
     * the streams, drain every entry whose key hashes there, combine by exact
     * key equality, and emit one output per distinct key.
     */
    private final class MergedIterator implements Iterator<Tuple2<K, V>> {
        private final List<PeekIt> sources;
        private final java.util.ArrayDeque<Tuple2<K, V>> pending = new java.util.ArrayDeque<>();

        MergedIterator(List<Iterator<Tuple2<K, V>>> streams) {
            this.sources = new ArrayList<>(streams.size());
            for (Iterator<Tuple2<K, V>> s : streams) sources.add(new PeekIt(s));
        }

        @Override
        public boolean hasNext() {
            if (!pending.isEmpty()) return true;
            advance();
            return !pending.isEmpty();
        }

        @Override
        public Tuple2<K, V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            return pending.removeFirst();
        }

        /** Pull the next hash-bucket's worth of entries and combine same-key values. */
        private void advance() {
            int minHash = Integer.MAX_VALUE;
            boolean any = false;
            for (PeekIt p : sources) {
                if (p.hasPeek()) {
                    int h = Objects.hashCode(p.peek()._1());
                    if (!any || h < minHash) { minHash = h; any = true; }
                }
            }
            if (!any) return;

            HashMap<K, V> bucket = new HashMap<>();
            for (PeekIt p : sources) {
                while (p.hasPeek() && Objects.hashCode(p.peek()._1()) == minHash) {
                    Tuple2<K, V> kv = p.takePeek();
                    bucket.merge(kv._1(), kv._2(), combiner::apply);
                }
            }
            for (Map.Entry<K, V> e : bucket.entrySet()) pending.add(new Tuple2<>(e.getKey(), e.getValue()));
        }

        private final class PeekIt {
            private final Iterator<Tuple2<K, V>> it;
            private Tuple2<K, V> head;
            PeekIt(Iterator<Tuple2<K, V>> it) { this.it = it; advance(); }
            private void advance() { head = it.hasNext() ? it.next() : null; }
            boolean hasPeek() { return head != null; }
            Tuple2<K, V> peek() { return head; }
            Tuple2<K, V> takePeek() { Tuple2<K, V> r = head; advance(); return r; }
        }
    }
}
