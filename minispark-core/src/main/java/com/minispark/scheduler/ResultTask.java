package com.minispark.scheduler;

import com.minispark.executor.TaskContext;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Final-stage task: runs the user's per-partition function and ships its
 * return value back to the driver.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.ResultTask
 */
public final class ResultTask<T, U> extends Task<U> {

    @FunctionalInterface
    public interface ResultHandler<T, U> extends Serializable {
        U apply(TaskContext ctx, Iterator<T> partitionIter);
    }

    private final RDD<T> rdd;
    private final Partition partition;
    private final ResultHandler<T, U> handler;
    private final int outputId; // 0-based position of this output in the job's result list

    public ResultTask(int stageId, int partitionId, int outputId,
                      RDD<T> rdd, Partition partition, ResultHandler<T, U> handler) {
        super(stageId, partitionId, rdd.preferredLocations(partition));
        this.rdd = rdd;
        this.partition = partition;
        this.handler = handler;
        this.outputId = outputId;
    }

    public int outputId() { return outputId; }

    @Override
    public U run(TaskContext ctx) {
        return handler.apply(ctx, rdd.iterator(partition, ctx));
    }
}
