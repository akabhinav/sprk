package com.minispark.rdd;

import java.util.List;

/**
 * A dependency where each child partition reads from a bounded number of
 * parent partitions. Stays inside one stage; no shuffle needed.
 *
 * Real Spark equivalent: org.apache.spark.NarrowDependency
 */
public abstract class NarrowDependency<T> extends Dependency<T> {
    private final RDD<T> rdd;

    protected NarrowDependency(RDD<T> rdd) {
        this.rdd = rdd;
    }

    @Override public RDD<T> rdd() { return rdd; }

    /** Parent partition indices feeding child partition {@code partitionId}. */
    public abstract List<Integer> getParents(int partitionId);
}
