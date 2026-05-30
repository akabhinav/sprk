package com.minispark.scheduler;

import com.minispark.rdd.RDD;

import java.util.List;

/**
 * The final stage of a job: its tasks return results to the driver.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.ResultStage
 */
public final class ResultStage extends Stage {
    private final int[] partitionsToCompute;

    public ResultStage(int id, RDD<?> rdd, List<Stage> parents, int[] partitionsToCompute) {
        super(id, rdd, parents);
        this.partitionsToCompute = partitionsToCompute;
    }

    public int[] partitionsToCompute() { return partitionsToCompute; }

    @Override public int numTasks() { return partitionsToCompute.length; }
}
