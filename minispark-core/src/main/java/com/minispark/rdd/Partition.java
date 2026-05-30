package com.minispark.rdd;

import java.io.Serializable;

/**
 * A slice of an RDD. Each partition is computed by exactly one task.
 *
 * <p>Must be serializable because in cluster mode the driver ships partition
 * descriptors to executors as part of the task payload.
 *
 * Real Spark equivalent: org.apache.spark.Partition
 */
public interface Partition extends Serializable {
    /** Position of this partition within its RDD (0-based, dense). */
    int index();
}
