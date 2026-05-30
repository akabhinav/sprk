package com.minispark.scheduler;

import com.minispark.api.Tuple2;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;
import com.minispark.shuffle.ShuffleHandle;
import com.minispark.shuffle.ShuffleWriter;
import com.minispark.storage.ExecutorLocation;

import java.util.Iterator;

/**
 * Map-side task: runs the parent RDD's {@code compute} for one partition and
 * pushes the records through a {@link ShuffleWriter}, which bucketizes them
 * by reduce-partition and persists each bucket as a shuffle block.
 *
 * <p>The task returns the {@link ExecutorLocation} where it wrote its blocks.
 * The driver registers that with the master {@link com.minispark.storage.MapOutputTracker}
 * so reducers (possibly on other executors) can locate and fetch the output.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.ShuffleMapTask (returns MapStatus)
 */
public final class ShuffleMapTask<K, V> extends Task<ExecutorLocation> {

    private final RDD<Tuple2<K, V>> rdd;
    private final Partition partition;
    private final ShuffleHandle handle;

    public ShuffleMapTask(int stageId, int partitionId,
                          RDD<Tuple2<K, V>> rdd, Partition partition,
                          ShuffleHandle handle) {
        super(stageId, partitionId);
        this.rdd = rdd;
        this.partition = partition;
        this.handle = handle;
    }

    @Override
    public ExecutorLocation run(TaskContext ctx) {
        // iterator() applies the RDD's cache directive if any, else falls through to compute().
        Iterator<Tuple2<K, V>> records = rdd.iterator(partition, ctx);
        // Tasks fetch executor-local services via SparkEnv. The reference must
        // not travel inside the serialized task because each executor has its
        // own ShuffleManager / BlockManager.
        SparkEnv env = SparkEnv.get();
        ShuffleWriter<K, V> writer = env.shuffleManager().getWriter(handle, partitionId());
        try {
            writer.write(records);
        } finally {
            writer.stop(true);
        }
        // Report where the blocks landed so the driver can register the output.
        return env.blockManager().location();
    }
}
