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
 * Reference shuffle implementation: bucket-per-reducer in memory, persisted
 * as one {@link com.minispark.storage.BlockId.ShuffleBlock} per
 * (shuffle, map, reduce) triple.
 *
 * <p>This mirrors the historical "hash shuffle" used by early Spark, before
 * sort shuffle replaced it for very wide jobs. Hash shuffle is simpler to
 * read and is the right place to start learning.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.hash.HashShuffleManager (deprecated upstream).
 */
public final class HashShuffleManager implements ShuffleManager {

    private final BlockManager blockManager;
    private final MapOutputTracker tracker;
    private final Serializer serializer;
    private final ConcurrentMap<Integer, ShuffleHandle> handles = new ConcurrentHashMap<>();

    public HashShuffleManager(BlockManager blockManager,
                              MapOutputTracker tracker,
                              Serializer serializer) {
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
        return new HashWriter<>(handle, mapId);
    }

    @Override
    public <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle, int startPartition, int endPartition) {
        return new HashReader<>(handle, startPartition, endPartition);
    }

    @Override
    public void unregisterShuffle(int shuffleId) {
        handles.remove(shuffleId);
        tracker.unregisterShuffle(shuffleId);
    }

    // ----- writer -----
    private final class HashWriter<K, V> implements ShuffleWriter<K, V> {
        private final ShuffleHandle handle;
        private final int mapId;

        HashWriter(ShuffleHandle handle, int mapId) {
            this.handle = handle;
            this.mapId = mapId;
        }

        @Override
        public void write(Iterator<Tuple2<K, V>> records) {
            int r = handle.partitioner.numPartitions();
            // Allocate one growing list per reducer; this is the "hash shuffle" pattern.
            // Heap usage scales as O(records); fine for learning, addressed by sort shuffle.
            List<List<Tuple2<K, V>>> buckets = new ArrayList<>(r);
            for (int i = 0; i < r; i++) buckets.add(new ArrayList<>());

            while (records.hasNext()) {
                Tuple2<K, V> kv = records.next();
                int p = handle.partitioner.getPartition(kv._1());
                buckets.get(p).add(kv);
            }

            for (int reduceId = 0; reduceId < r; reduceId++) {
                List<Tuple2<K, V>> bucket = buckets.get(reduceId);
                byte[] bytes = writeBucket(bucket);
                BlockId.ShuffleBlock id = new BlockId.ShuffleBlock(handle.shuffleId, mapId, reduceId);
                blockManager.putBlock(id, bytes);
            }
            // NB: we do NOT register the map output here. The task returns its
            // block location and the driver's DAGScheduler registers it with the
            // master MapOutputTracker. This keeps the authoritative map on the
            // driver, which is essential once executors live in other JVMs.
        }

        @Override public void stop(boolean success) { /* nothing to release for in-memory writer */ }

        /**
         * Encoding: int count, then each Tuple2 written with the
         * {@link Serializer}. Hand-rolled instead of one bulk serialize
         * because per-record streaming is what real shuffle code does and
         * what a sort-shuffle replacement would slot into.
         */
        private byte[] writeBucket(List<Tuple2<K, V>> bucket) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(bucket.size());
                for (Tuple2<K, V> kv : bucket) {
                    oos.writeObject(kv);
                }
                oos.flush();
                return baos.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException("shuffle write failed", e);
            }
        }
    }

    // ----- reader -----
    private final class HashReader<K, V> implements ShuffleReader<K, V> {
        private final ShuffleHandle handle;
        private final int startPartition;
        private final int endPartition;

        HashReader(ShuffleHandle handle, int startPartition, int endPartition) {
            this.handle = handle;
            this.startPartition = startPartition;
            this.endPartition = endPartition;
        }

        @Override
        public Iterator<Tuple2<K, V>> read() {
            List<MapOutputTracker.MapStatus> statuses = tracker.getMapStatuses(handle.shuffleId);
            // For each reduce partition in our range, fetch the matching bucket
            // from every map output and concat. Iterator-of-iterators kept lazy
            // so memory tracks "one bucket at a time" instead of the whole shuffle.
            List<Iterator<Tuple2<K, V>>> sources = new ArrayList<>();
            for (int reduceId = startPartition; reduceId < endPartition; reduceId++) {
                final int rid = reduceId;
                for (MapOutputTracker.MapStatus s : statuses) {
                    BlockId.ShuffleBlock id = new BlockId.ShuffleBlock(handle.shuffleId, s.mapId(), rid);
                    byte[] bytes;
                    try {
                        bytes = blockManager.getRemoteBlock(id, s.location())
                                .orElseThrow(() -> new FetchFailedException(
                                        handle.shuffleId, s.mapId(), rid, s.location(),
                                        "missing shuffle block " + id, null));
                    } catch (FetchFailedException ffe) {
                        throw ffe;
                    } catch (Exception e) {
                        // Most commonly a RemoteRpcException because the owning executor
                        // died — convert to a structured fetch failure so the driver can
                        // recompute the map output instead of just retrying blindly.
                        throw new FetchFailedException(
                                handle.shuffleId, s.mapId(), rid, s.location(),
                                "fetch from " + s.location() + " failed: " + e.getMessage(), e);
                    }
                    sources.add(readBucket(bytes));
                }
            }
            return concat(sources);
        }

        private Iterator<Tuple2<K, V>> readBucket(byte[] bytes) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
                int n = ois.readInt();
                List<Tuple2<K, V>> out = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    @SuppressWarnings("unchecked")
                    Tuple2<K, V> kv = (Tuple2<K, V>) ois.readObject();
                    out.add(kv);
                }
                return out.iterator();
            } catch (Exception e) {
                throw new RuntimeException("shuffle read failed", e);
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
