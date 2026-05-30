package com.minispark.scheduler;

import com.minispark.rdd.RDD;

import java.util.List;

/**
 * A set of tasks that can run in parallel without any shuffle between them.
 * Stage boundaries are determined entirely by {@link com.minispark.rdd.ShuffleDependency}s
 * in the RDD lineage.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.Stage
 */
public abstract class Stage {
    private final int id;
    private final RDD<?> rdd; // final RDD of this stage
    private final List<Stage> parents;

    protected Stage(int id, RDD<?> rdd, List<Stage> parents) {
        this.id = id;
        this.rdd = rdd;
        this.parents = parents;
    }

    public int id() { return id; }
    public RDD<?> rdd() { return rdd; }
    public List<Stage> parents() { return parents; }
    public int numTasks() { return rdd.getPartitions().size(); }
}
