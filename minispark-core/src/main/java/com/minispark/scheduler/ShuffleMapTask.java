package com.minispark.scheduler;

import com.minispark.api.Tuple2;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;
import com.minispark.shuffle.ShuffleHandle;
import com.minispark.shuffle.ShuffleWriter;

import java.util.Iterator;

/**
 * Map-side task: runs the parent RDD's {@code compute} for one partition and
 * pushes the records through a {@link ShuffleWriter}, which bucketizes them
 * by reduce-partition and persists each bucket as a shuffle block.
 *
 * <p>The task returns a {@link MapTaskOutput} — the executor location where
 * it wrote its blocks plus the per-reducer byte sizes. The driver registers
 * these with the master {@link com.minispark.storage.MapOutputTracker} so
 * reducers (possibly on other executors) can locate the output, and so AQE
 * rules can re-plan downstream stages from the materialised sizes.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.ShuffleMapTask (returns MapStatus)
 */
public final class ShuffleMapTask<K, V> extends Task<MapTaskOutput> {

    private final RDD<Tuple2<K, V>> rdd;
    private final Partition partition;
    private final ShuffleHandle handle;

    public ShuffleMapTask(int stageId, int partitionId,
                          RDD<Tuple2<K, V>> rdd, Partition partition,
                          ShuffleHandle handle) {
        super(stageId, partitionId, rdd.preferredLocations(partition));
        this.rdd = rdd;
        this.partition = partition;
        this.handle = handle;
    }

    @Override
    public MapTaskOutput run(TaskContext ctx) {
        // iterator() applies the RDD's cache directive if any, else falls through to compute().
        Iterator<Tuple2<K, V>> records = rdd.iterator(partition, ctx);
        // Tasks fetch executor-local services via SparkEnv. The reference must
        // not travel inside the serialized task because each executor has its
        // own ShuffleManager / BlockManager.
        SparkEnv env = SparkEnv.get();
        ShuffleWriter<K, V> writer = env.shuffleManager().getWriter(handle, partitionId());
        long[] sizes;
        try {
            sizes = writer.write(records);
        } finally {
            writer.stop(true);
        }
        return new MapTaskOutput(env.blockManager().location(), sizes);
    }
}
