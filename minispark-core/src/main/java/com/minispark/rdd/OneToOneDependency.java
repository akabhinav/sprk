package com.minispark.rdd;

import java.util.List;

/**
 * Child partition i depends only on parent partition i. The simplest narrow
 * dependency; it is what lets {@code map}, {@code filter}, and {@code flatMap}
 * chain inside a single stage without materializing intermediate data.
 *
 * Real Spark equivalent: org.apache.spark.OneToOneDependency
 */
public final class OneToOneDependency<T> extends NarrowDependency<T> {
    public OneToOneDependency(RDD<T> rdd) {
        super(rdd);
    }

    @Override
    public List<Integer> getParents(int partitionId) {
        return List.of(partitionId);
    }
}
