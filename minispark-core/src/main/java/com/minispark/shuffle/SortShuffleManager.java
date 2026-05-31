package com.minispark.shuffle;

import com.minispark.api.Tuple2;
import com.minispark.serializer.Serializer;
import com.minispark.storage.BlockId;
import com.minispark.storage.BlockManager;
import com.minispark.storage.MapOutputTracker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Sort-based shuffle: each map task writes a <b>single consolidated block</b>
 * ({@link BlockId.ShuffleDataBlock}) containing all of its records grouped by
 * reduce partition, plus an index of where each partition's records start.
 * Reducers fetch that one block per map and slice out their own partition.
 *
 * <p><b>Why this replaced hash shuffle in real Spark.</b> Hash shuffle creates
 * {@code numMaps × numReduces} files/blocks; at scale (10k maps × 10k reduces =
 * 100M files) that exhausts file descriptors and inodes. Sort shuffle creates
 * {@code numMaps} files regardless of reducer count — the records are sorted by
 * partition id and an index makes each reducer's slice an O(1) lookup. We keep
 * the data in memory rather than on disk (a learning simplification), but the
 * structure — one indexed file per map — is the real design.
 *
 * <p>This class is a drop-in for {@link HashShuffleManager}: same
 * {@link ShuffleManager} interface, selected by {@code minispark.shuffle.manager}.
 * That the DAGScheduler/tasks need zero changes to switch is the whole point of
 * the shuffle seam.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.sort.SortShuffleManager
 */
public final class SortShuffleManager implements ShuffleManager {

    private final BlockManager blockManager;
    private final MapOutputTracker tracker;
    private final Serializer serializer;
    private final ConcurrentMap<Integer, ShuffleHandle> handles = new ConcurrentHashMap<>();

    public SortShuffleManager(BlockManager blockManager, MapOutputTracker tracker, Serializer serializer) {
        this.blockManager = blockManager;
        this.tracker = tracker;
        this.serializer = serializer;
    }

    @Override
    public ShuffleHandle registerShuffle(int shuffleId, int numMaps, Partitioner partitioner) {
        ShuffleHandle h = new ShuffleHandle(shuffleId, numMaps, partitioner);
        handles.put(shuffleId, h);
        tracker.registerShuffle(shuffleId, numMaps);
        return h;
    }

    @Override
    public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId) {
        return new SortWriter<>(handle, mapId);
    }

    @Override
    public <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle, int startPartition, int endPartition) {
        return new SortReader<>(handle, startPartition, endPartition);
    }

    @Override
    public void unregisterShuffle(int shuffleId) {
        handles.remove(shuffleId);
        tracker.unregisterShuffle(shuffleId);
    }

    // ----- writer: one indexed block per map task -----
    private final class SortWriter<K, V> implements ShuffleWriter<K, V> {
        private final ShuffleHandle handle;
        private final int mapId;

        SortWriter(ShuffleHandle handle, int mapId) { this.handle = handle; this.mapId = mapId; }

        @Override
        public long[] write(Iterator<Tuple2<K, V>> records) {
            int r = handle.partitioner.numPartitions();
            List<List<Tuple2<K, V>>> byReducer = new ArrayList<>(r);
            for (int i = 0; i < r; i++) byReducer.add(new ArrayList<>());

            // "Sort by partition id" — we bucket directly, which yields the same
            // partition-ordered layout an actual sort would, without the O(n log n).
            while (records.hasNext()) {
                Tuple2<K, V> kv = records.next();
                int p = handle.partitioner.getPartition(kv._1());
                byReducer.get(p).add(kv);
            }

            // Serialize as [int numPartitions][per-partition: int count, records...].
            // The per-partition counts ARE the index: a reader skips to its slice.
            // Track each partition's byte contribution by sampling baos.size()
            // before/after — those are the AQE inputs that drive coalesce decisions.
            long[] sizes = new long[r];
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(r);
                oos.flush();
                int prev = baos.size();
                for (int p = 0; p < r; p++) {
                    List<Tuple2<K, V>> bucket = byReducer.get(p);
                    oos.writeInt(bucket.size());
                    for (Tuple2<K, V> kv : bucket) oos.writeObject(kv);
                    oos.flush();
                    int now = baos.size();
                    sizes[p] = now - prev;
                    prev = now;
                }
                blockManager.putBlock(new BlockId.ShuffleDataBlock(handle.shuffleId, mapId),
                        baos.toByteArray());
            } catch (Exception e) {
                throw new RuntimeException("sort-shuffle write failed", e);
            }
            return sizes;
        }

        @Override public void stop(boolean success) { /* in-memory: nothing to release */ }
    }

    // ----- reader: fetch each map's single block, slice out our partition -----
    private final class SortReader<K, V> implements ShuffleReader<K, V> {
        private final ShuffleHandle handle;
        private final int startPartition;
        private final int endPartition;

        SortReader(ShuffleHandle handle, int startPartition, int endPartition) {
            this.handle = handle;
            this.startPartition = startPartition;
            this.endPartition = endPartition;
        }

        @Override
        public Iterator<Tuple2<K, V>> read() {
            List<MapOutputTracker.MapStatus> statuses = tracker.getMapStatuses(handle.shuffleId);
            List<Iterator<Tuple2<K, V>>> sources = new ArrayList<>();
            for (MapOutputTracker.MapStatus s : statuses) {
                BlockId.ShuffleDataBlock id = new BlockId.ShuffleDataBlock(handle.shuffleId, s.mapId());
                byte[] bytes;
                try {
                    bytes = blockManager.getRemoteBlock(id, s.location())
                            .orElseThrow(() -> new FetchFailedException(
                                    handle.shuffleId, s.mapId(), startPartition, s.location(),
                                    "missing shuffle data block " + id, null));
                } catch (FetchFailedException ffe) {
                    throw ffe;
                } catch (Exception e) {
                    throw new FetchFailedException(handle.shuffleId, s.mapId(), startPartition,
                            s.location(), "fetch from " + s.location() + " failed: " + e.getMessage(), e);
                }
                sources.add(sliceForReducers(bytes));
            }
            return concat(sources);
        }

        /** Read the map block and concatenate only the partitions in [start,end). */
        private Iterator<Tuple2<K, V>> sliceForReducers(byte[] bytes) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
                int numPartitions = ois.readInt();
                List<Tuple2<K, V>> out = new ArrayList<>();
                for (int p = 0; p < numPartitions; p++) {
                    int count = ois.readInt();
                    boolean wanted = p >= startPartition && p < endPartition;
                    for (int i = 0; i < count; i++) {
                        @SuppressWarnings("unchecked")
                        Tuple2<K, V> kv = (Tuple2<K, V>) ois.readObject();
                        if (wanted) out.add(kv);
                    }
                }
                return out.iterator();
            } catch (Exception e) {
                throw new RuntimeException("sort-shuffle read failed", e);
            }
        }
    }

    private static <X> Iterator<X> concat(List<Iterator<X>> iters) {
        return new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() {
                while (i < iters.size() && !iters.get(i).hasNext()) i++;
                return i < iters.size();
            }
            @Override public X next() {
                if (!hasNext()) throw new NoSuchElementException();
                return iters.get(i).next();
            }
        };
    }
}
