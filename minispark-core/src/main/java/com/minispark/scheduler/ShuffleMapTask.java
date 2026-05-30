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
 * <p>The task itself returns nothing meaningful (just a marker); reducers
 * locate the output via the {@link com.minispark.storage.MapOutputTracker}.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.ShuffleMapTask
 */
public final class ShuffleMapTask<K, V> extends Task<Void> {

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
    public Void run(TaskContext ctx) {
        Iterator<Tuple2<K, V>> records = rdd.compute(partition, ctx);
        // Tasks fetch executor-local services via SparkEnv. The reference must
        // not travel inside the serialized task because each executor has its
        // own ShuffleManager / BlockManager.
        ShuffleWriter<K, V> writer = SparkEnv.get().shuffleManager().getWriter(handle, partitionId());
        try {
            writer.write(records);
        } finally {
            writer.stop(true);
        }
        return null;
    }
}
