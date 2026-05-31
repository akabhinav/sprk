package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.shuffle.Partitioner;
import com.minispark.shuffle.ShuffleReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An RDD whose elements come from reading one reduce partition off a
 * {@link ShuffleDependency}. Its {@code compute} grabs a {@link ShuffleReader}
 * from the {@link com.minispark.shuffle.ShuffleManager}, and the
 * DAGScheduler will have already arranged for the upstream map stage to
 * have populated those buckets before this RDD's tasks run.
 *
 * Real Spark equivalent: org.apache.spark.rdd.ShuffledRDD
 */
public final class ShuffledRDD<K, V> extends RDD<Tuple2<K, V>> {

    /**
     * One post-shuffle partition. Spans the half-open range
     * {@code [startReduceId, endReduceId)} of the original reducer ids and,
     * optionally, the half-open range {@code [startMapId, endMapId)} of map
     * ids (sentinels {@code -1, -1} mean "all maps"). Default: single
     * reducer, all maps. AQE rewrites:
     *
     * <ul>
     *   <li><b>Coalesce</b> widens the reducer range to fuse small
     *       contiguous reducers.</li>
     *   <li><b>Skew split</b> narrows the map-id range so multiple slices
     *       share a single reducer-id but each reads a fraction of the
     *       map outputs — exploding one fat partition into N tasks.</li>
     * </ul>
     */
    private static final class ShuffleSlice implements Partition {
        private final int idx;
        final int startReduceId;
        final int endReduceId;
        final int startMapId;
        final int endMapId;
        ShuffleSlice(int idx, int startReduceId, int endReduceId) {
            this(idx, startReduceId, endReduceId, -1, -1);
        }
        ShuffleSlice(int idx, int startReduceId, int endReduceId, int startMapId, int endMapId) {
            this.idx = idx;
            this.startReduceId = startReduceId;
            this.endReduceId = endReduceId;
            this.startMapId = startMapId;
            this.endMapId = endMapId;
        }
        public int index() { return idx; }
    }

    private final ShuffleDependency<K, V> dep;
    private List<Partition> partitions;
    private boolean coalesced; // once true, partitioner() returns null (count differs)

    public ShuffledRDD(MiniSparkContext sc, RDD<Tuple2<K, V>> parent, Partitioner partitioner) {
        super(sc);
        this.dep = new ShuffleDependency<>(parent, partitioner, sc.shuffleManager());
        List<Partition> p = new ArrayList<>(partitioner.numPartitions());
        for (int i = 0; i < partitioner.numPartitions(); i++) p.add(new ShuffleSlice(i, i, i + 1));
        this.partitions = p;
    }

    @Override public List<Partition> getPartitions() { return partitions; }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext ctx) {
        ShuffleSlice s = (ShuffleSlice) split;
        // SparkEnv lookup, not context() — `context()` is transient and is
        // null after the RDD has been deserialized onto an executor.
        ShuffleReader<K, V> reader = SparkEnv.get().shuffleManager()
                .getReader(dep.handle(), s.startReduceId, s.endReduceId, s.startMapId, s.endMapId);
        return reader.read();
    }

    @Override public List<Dependency<?>> getDependencies() { return List.of(dep); }

    /**
     * After AQE coalesce the output partition count no longer matches the
     * shuffle's partitioner, so we must declare "unknown partitioning" to any
     * downstream operator that asks (e.g. a co-partitioned join check). Coalesce
     * is gated on no downstream shuffle existing in this job, so returning null
     * is safe — nothing in the current job will short-circuit on it.
     */
    @Override public Partitioner partitioner() { return coalesced ? null : dep.partitioner(); }

    public ShuffleDependency<K, V> shuffleDep() { return dep; }

    /**
     * Replace the default one-reducer-per-partition layout with the ranges
     * computed by {@link com.minispark.scheduler.adaptive.CoalesceShufflePartitionsRule}.
     * Called by the DAGScheduler between the map stage finishing and the
     * downstream stage being submitted, so the new partition list is what
     * {@code getPartitions()} returns when tasks are built.
     */
    public synchronized void applyCoalescedRanges(List<int[]> ranges) {
        List<Partition> p = new ArrayList<>(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            int[] r = ranges.get(i);
            p.add(new ShuffleSlice(i, r[0], r[1]));
        }
        this.partitions = p;
        this.coalesced = true;
    }

    /**
     * Explode the slices for one skewed reducer into multiple sub-slices, each
     * carrying a map-id range. The input {@code mapIdSplits} is a list of
     * {@code [startMapId, endMapId)} ranges that together cover {@code [0, numMaps)}.
     * Slices for other reducers are untouched. Called after
     * {@link #applyCoalescedRanges} (if any), so we look only at single-reducer
     * slices — coalesced multi-reducer ranges are skipped to keep the two AQE
     * rules from interleaving in confusing ways.
     */
    public synchronized void applySkewSplit(int skewedReduceId, List<int[]> mapIdSplits) {
        List<Partition> p = new ArrayList<>(partitions.size() + mapIdSplits.size());
        int nextIdx = 0;
        for (Partition existing : partitions) {
            ShuffleSlice s = (ShuffleSlice) existing;
            boolean isThisSingleReducer = (s.endReduceId - s.startReduceId == 1)
                    && s.startReduceId == skewedReduceId
                    && s.startMapId < 0;   // only un-split slices
            if (isThisSingleReducer) {
                for (int[] r : mapIdSplits) {
                    p.add(new ShuffleSlice(nextIdx++, skewedReduceId, skewedReduceId + 1, r[0], r[1]));
                }
            } else {
                p.add(new ShuffleSlice(nextIdx++,
                        s.startReduceId, s.endReduceId, s.startMapId, s.endMapId));
            }
        }
        this.partitions = p;
        this.coalesced = true; // partition count no longer matches the partitioner
    }
}
