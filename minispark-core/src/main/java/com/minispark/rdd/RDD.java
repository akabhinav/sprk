package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.serializer.Serializer;
import com.minispark.storage.BlockId;
import com.minispark.storage.BlockManager;
import com.minispark.storage.StorageLevel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A Resilient Distributed Dataset: an immutable, partitioned collection of
 * elements built up by chaining transformations.
 *
 * <p>Three things every concrete RDD must define:
 * <ol>
 *   <li>{@link #getPartitions()} — how the dataset is sliced.</li>
 *   <li>{@link #compute(Partition, TaskContext)} — how to materialize one slice.</li>
 *   <li>{@link #getDependencies()} — what parent RDD(s) feed this one.</li>
 * </ol>
 *
 * <p><b>Why transformations are lazy:</b> calling {@code map} returns a new
 * RDD object that holds the function; nothing runs. The DAG only executes
 * when an <i>action</i> (collect, count, reduce, ...) demands a value. This
 * laziness is what lets the scheduler see the whole pipeline at once, fuse
 * narrow operations into a single stage, and recompute lost partitions from
 * lineage instead of replicating state.
 *
 * Real Spark equivalent: org.apache.spark.rdd.RDD
 */
public abstract class RDD<T> implements Serializable {

    private static final AtomicInteger ID_GEN = new AtomicInteger();

    private final int id = ID_GEN.incrementAndGet();
    // Transient: the context is a driver-side object and must not ship to executors.
    private final transient MiniSparkContext sc;
    // Cache directive set by cache()/persist(). Travels with the RDD to executors
    // so each task knows whether to consult / populate the BlockManager.
    private StorageLevel storageLevel = StorageLevel.NONE;
    // Checkpoint state. checkpointPath is the reliable-storage directory for this
    // RDD's partition files (travels to executors). `checkpointed` flips true on
    // the driver once all partition files exist, which truncates the lineage.
    private String checkpointPath;
    private volatile boolean checkpointed = false;

    protected RDD(MiniSparkContext sc) {
        this.sc = sc;
    }

    public final int id() { return id; }
    public final MiniSparkContext context() { return sc; }
    public final StorageLevel storageLevel() { return storageLevel; }
    public final boolean isCheckpointed() { return checkpointed; }
    final void markCheckpointed() { this.checkpointed = true; }
    final String checkpointPath() { return checkpointPath; }

    /**
     * Mark this RDD to be checkpointed: on the next action that materializes it,
     * each partition is written to reliable storage under
     * {@code <checkpointDir>/rdd-<id>/part-<idx>}. After that, {@link #getDependencies}
     * reports no parents and {@link #compute} reads the saved files — the lineage
     * before this point is truncated, so a later failure won't recompute it.
     *
     * <p>Requires {@link MiniSparkContext#setCheckpointDir} to have been called.
     *
     * Real Spark equivalent: org.apache.spark.rdd.RDD#checkpoint (ReliableCheckpointRDD).
     */
    public final void checkpoint() {
        String dir = sc.checkpointDir();
        if (dir == null) throw new IllegalStateException(
                "Call sc.setCheckpointDir(...) before rdd.checkpoint()");
        this.checkpointPath = dir + java.io.File.separator + "rdd-" + id;
        sc.registerForCheckpoint(this);
    }

    public abstract List<Partition> getPartitions();
    public abstract Iterator<T> compute(Partition split, TaskContext ctx);
    public abstract List<Dependency<?>> getDependencies();

    /**
     * Hosts where this partition's data already lives, so the scheduler can try
     * to run the task there and avoid a network fetch ("data locality"). Default
     * is empty (no preference → run anywhere). A source RDD over a distributed
     * file would return the hosts holding each block.
     *
     * Real Spark equivalent: org.apache.spark.rdd.RDD#getPreferredLocations
     */
    public List<String> preferredLocations(Partition split) { return List.of(); }

    /**
     * Shorthand for {@code persist(MEMORY_ONLY)}. After {@code cache()}, the
     * first task that materializes a partition writes it into the executor's
     * BlockManager; subsequent reads on the same executor skip {@link #compute}
     * entirely. If an executor dies, the cache dies with it, and lineage
     * re-computes the partition next time it's needed — the "R" in RDD.
     */
    public final RDD<T> cache() { return persist(StorageLevel.MEMORY_ONLY); }

    public final RDD<T> persist(StorageLevel level) {
        // Mutating an existing field on an immutable-ish object is the same
        // compromise Spark makes — cache() is a directive, not a transformation.
        this.storageLevel = level;
        return this;
    }

    /**
     * The entry point tasks actually call. Wraps {@link #compute} with a
     * BlockManager check when caching is on. Materializes the iterator into a
     * list so the cached form is a concrete value (real Spark also has to
     * materialize before storing, since iterators are one-shot).
     *
     * <p>Cache scope is per-executor: a partition cached on executor A is
     * invisible to executor B (it'll recompute). Simple and matches the
     * Spark default for memory-only — sufficient to teach the idea.
     */
    @SuppressWarnings("unchecked")
    public final Iterator<T> iterator(Partition split, TaskContext ctx) {
        // 1. Checkpoint read: if a saved partition file exists, it IS the data —
        //    serving it is what makes the truncated lineage cheap.
        if (checkpointPath != null) {
            java.nio.file.Path file = checkpointFile(split.index());
            if (java.nio.file.Files.exists(file)) {
                return readCheckpoint(file).iterator();
            }
        }

        // 2. Cache lookup / compute.
        List<T> materialized;
        if (storageLevel != StorageLevel.NONE) {
            BlockManager bm = SparkEnv.get().blockManager();
            Serializer ser = SparkEnv.get().serializer();
            BlockId.RDDBlock blockId = new BlockId.RDDBlock(id, split.index());
            Optional<byte[]> hit = bm.getBlock(blockId);
            if (hit.isPresent()) {
                materialized = (List<T>) ser.deserialize(hit.get());
            } else {
                materialized = new ArrayList<>();
                compute(split, ctx).forEachRemaining(materialized::add);
                bm.putBlock(blockId, ser.serialize((java.io.Serializable) materialized), storageLevel);
            }
        } else {
            materialized = new ArrayList<>();
            compute(split, ctx).forEachRemaining(materialized::add);
        }

        // 3. Checkpoint write: persist this partition to reliable storage.
        if (checkpointPath != null) {
            writeCheckpoint(split.index(), materialized);
        }
        return materialized.iterator();
    }

    private java.nio.file.Path checkpointFile(int partitionIndex) {
        return java.nio.file.Path.of(checkpointPath, String.format("part-%05d", partitionIndex));
    }

    @SuppressWarnings("unchecked")
    private List<T> readCheckpoint(java.nio.file.Path file) {
        try {
            byte[] bytes = java.nio.file.Files.readAllBytes(file);
            return (List<T>) SparkEnv.get().serializer().deserialize(bytes);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed reading checkpoint " + file, e);
        }
    }

    private void writeCheckpoint(int partitionIndex, List<T> data) {
        try {
            java.nio.file.Path dir = java.nio.file.Path.of(checkpointPath);
            java.nio.file.Files.createDirectories(dir);
            byte[] bytes = SparkEnv.get().serializer().serialize((java.io.Serializable) data);
            // Write to a temp file then atomically move, so a concurrent reader
            // never sees a half-written checkpoint.
            java.nio.file.Path tmp = java.nio.file.Files.createTempFile(dir, "part-", ".tmp");
            java.nio.file.Files.write(tmp, bytes);
            java.nio.file.Files.move(tmp, checkpointFile(partitionIndex),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed writing checkpoint for partition " + partitionIndex, e);
        }
    }

    /**
     * If non-null, hints the {@link com.minispark.shuffle.Partitioner} this RDD is
     * already partitioned by — e.g. a {@code ShuffledRDD}. Used by Phase 2's
     * scheduler to avoid redundant shuffles.
     */
    public com.minispark.shuffle.Partitioner partitioner() { return null; }

    // ----- transformations (lazy) -----

    public <U> RDD<U> map(SerializableFunction<T, U> f) {
        return new MapPartitionsRDD<>(sc, this,
                (ctx, part, it) -> new TransformingIterator<>(it, f));
    }

    public RDD<T> filter(SerializablePredicate<T> p) {
        return new MapPartitionsRDD<>(sc, this,
                (ctx, part, it) -> new FilteringIterator<>(it, p));
    }

    public <U> RDD<U> flatMap(SerializableFunction<T, Iterator<U>> f) {
        return new MapPartitionsRDD<>(sc, this,
                (ctx, part, it) -> new FlatteningIterator<>(it, f));
    }

    /**
     * Pair-RDD entry point. {@link com.minispark.api.PairRDDFunctions} adds
     * key/value operations like {@code reduceByKey}, {@code groupByKey},
     * {@code join} — each of which introduces a shuffle.
     */
    public <K, V> com.minispark.api.PairRDDFunctions<K, V> mapToPair(
            SerializableFunction<T, com.minispark.api.Tuple2<K, V>> toPair) {
        RDD<com.minispark.api.Tuple2<K, V>> mapped = map(toPair);
        return new com.minispark.api.PairRDDFunctions<>(mapped);
    }

    // ----- actions (eager) -----

    public List<T> collect() {
        return sc.runJob(this, (ctx, it) -> {
            List<T> out = new ArrayList<>();
            it.forEachRemaining(out::add);
            return out;
        }).stream().flatMap(List::stream).toList();
    }

    public long count() {
        return sc.runJob(this, (ctx, it) -> {
            long n = 0;
            while (it.hasNext()) { it.next(); n++; }
            return n;
        }).stream().mapToLong(Long::longValue).sum();
    }

    public T reduce(SerializableBiFunction<T, T, T> op) {
        List<T> partials = sc.runJob(this, (ctx, it) -> {
            if (!it.hasNext()) return null;
            T acc = it.next();
            while (it.hasNext()) acc = op.apply(acc, it.next());
            return acc;
        });
        T result = null;
        for (T p : partials) {
            if (p == null) continue;
            result = (result == null) ? p : op.apply(result, p);
        }
        if (result == null) throw new NoSuchElementException("reduce on empty RDD");
        return result;
    }

    public void foreach(SerializableConsumer<T> f) {
        sc.runJob(this, (ctx, it) -> { it.forEachRemaining(f::accept); return null; });
    }

    /**
     * First {@code n} elements, in partition-order. Real Spark runs partitions
     * one (or a few) at a time and stops as soon as it has enough; we run all
     * partitions in parallel and truncate — simpler, but reads more than it
     * needs to on huge inputs. Adequate for a learning engine.
     */
    public List<T> take(int n) {
        if (n <= 0) return new ArrayList<>();
        List<List<T>> perPartition = sc.runJob(this, (ctx, it) -> {
            List<T> out = new ArrayList<>();
            while (it.hasNext() && out.size() < n) out.add(it.next());
            return out;
        });
        List<T> result = new ArrayList<>(n);
        for (List<T> part : perPartition) {
            for (T x : part) {
                if (result.size() == n) return result;
                result.add(x);
            }
        }
        return result;
    }

    public T first() {
        List<T> head = take(1);
        if (head.isEmpty()) throw new NoSuchElementException("first on empty RDD");
        return head.get(0);
    }

    /**
     * Smallest {@code n} elements per the comparator, globally. Each partition
     * keeps its top n; the driver merges and trims.
     */
    public List<T> takeOrdered(int n, SerializableComparator<? super T> cmp) {
        if (n <= 0) return new ArrayList<>();
        List<List<T>> perPart = sc.runJob(this, (ctx, it) -> {
            // Bounded-size max-heap of size n: cheapest way to keep the bottom n.
            java.util.PriorityQueue<T> pq = new java.util.PriorityQueue<>(n, cmp.reversed());
            while (it.hasNext()) {
                T v = it.next();
                if (pq.size() < n) pq.offer(v);
                else if (cmp.compare(v, pq.peek()) < 0) { pq.poll(); pq.offer(v); }
            }
            List<T> out = new ArrayList<>(pq);
            out.sort(cmp);
            return out;
        });
        List<T> all = new ArrayList<>();
        for (List<T> p : perPart) all.addAll(p);
        all.sort(cmp);
        return all.size() > n ? all.subList(0, n) : all;
    }

    /**
     * Write each partition to {@code path/part-NNNNN}. Real Spark writes via
     * Hadoop's OutputFormat; we use plain UTF-8 files (one per partition) so
     * the result is browsable without any extra runtime.
     */
    public void saveAsTextFile(String path) {
        java.nio.file.Path dir = java.nio.file.Path.of(path);
        try {
            java.nio.file.Files.createDirectories(dir);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Cannot create output dir " + path, e);
        }
        // Capture path as a Serializable string; the lambda runs on executors.
        final String outPath = path;
        sc.runJob(this, (ctx, it) -> {
            java.nio.file.Path file = java.nio.file.Path.of(outPath,
                    String.format("part-%05d", ctx.partitionId()));
            try (java.io.BufferedWriter w = java.nio.file.Files.newBufferedWriter(
                    file, java.nio.charset.StandardCharsets.UTF_8)) {
                while (it.hasNext()) {
                    w.write(String.valueOf(it.next()));
                    w.write('\n');
                }
            } catch (java.io.IOException e) {
                throw new RuntimeException("Failed writing " + file, e);
            }
            return null;
        });
    }

    // ----- serializable functional interfaces -----
    // Closures cross the SchedulerBackend seam, so they must be Serializable from day one.

    @FunctionalInterface public interface SerializableFunction<A, B> extends Function<A, B>, Serializable {}
    @FunctionalInterface public interface SerializableBiFunction<A, B, C> extends BiFunction<A, B, C>, Serializable {}
    @FunctionalInterface public interface SerializablePredicate<A> extends Predicate<A>, Serializable {}
    @FunctionalInterface public interface SerializableConsumer<A> extends java.util.function.Consumer<A>, Serializable {}
    @FunctionalInterface public interface SerializableComparator<A> extends java.util.Comparator<A>, Serializable {}

    // ----- tiny iterator adapters -----

    private static final class TransformingIterator<A, B> implements Iterator<B> {
        private final Iterator<A> src; private final Function<A, B> f;
        TransformingIterator(Iterator<A> src, Function<A, B> f) { this.src = src; this.f = f; }
        public boolean hasNext() { return src.hasNext(); }
        public B next() { return f.apply(src.next()); }
    }

    private static final class FilteringIterator<A> implements Iterator<A> {
        private final Iterator<A> src; private final Predicate<A> p;
        private A nxt; private boolean ready;
        FilteringIterator(Iterator<A> src, Predicate<A> p) { this.src = src; this.p = p; }
        public boolean hasNext() {
            while (!ready && src.hasNext()) {
                A v = src.next();
                if (p.test(v)) { nxt = v; ready = true; }
            }
            return ready;
        }
        public A next() {
            if (!hasNext()) throw new NoSuchElementException();
            ready = false; A v = nxt; nxt = null; return v;
        }
    }

    private static final class FlatteningIterator<A, B> implements Iterator<B> {
        private final Iterator<A> src; private final Function<A, Iterator<B>> f;
        private Iterator<B> current = java.util.Collections.emptyIterator();
        FlatteningIterator(Iterator<A> src, Function<A, Iterator<B>> f) { this.src = src; this.f = f; }
        public boolean hasNext() {
            while (!current.hasNext() && src.hasNext()) current = f.apply(src.next());
            return current.hasNext();
        }
        public B next() {
            if (!hasNext()) throw new NoSuchElementException();
            return current.next();
        }
    }

}
