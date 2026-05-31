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
     * {@code [startReduceId, endReduceId)} of the original reducer ids: by
     * default each slice is a single reducer ({@code end = start + 1}), but
     * the AQE coalesce rule can replace the partition list with wider slices.
     */
    private static final class ShuffleSlice implements Partition {
        private final int idx;
        final int startReduceId;
        final int endReduceId;
        ShuffleSlice(int idx, int startReduceId, int endReduceId) {
            this.idx = idx;
            this.startReduceId = startReduceId;
            this.endReduceId = endReduceId;
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
                .getReader(dep.handle(), s.startReduceId, s.endReduceId);
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
}
