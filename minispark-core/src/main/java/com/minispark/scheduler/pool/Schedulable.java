package com.minispark.scheduler.pool;

/**
 * Anything that can be ordered by a {@link Pool} when allocating resources —
 * either a leaf (a runnable stage / TaskSet manager) or a nested {@link Pool}.
 * Spark's scheduler is a tree of these; we keep it to two levels (root pool →
 * named pools → stages) but the interface allows arbitrary nesting.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.Schedulable
 */
public interface Schedulable {

    /** Stable name, used to look up a pool by id. */
    String name();

    /** Relative weight when sharing fairly (higher = more share). */
    int weight();

    /** Guaranteed slots this schedulable gets before fair sharing kicks in. */
    int minShare();

    /** Submission sequence number, used as the FIFO tie-breaker. */
    long priority();

    /** Tasks currently running across this subtree (for fair-share fairness). */
    int runningTasks();

    /** Whether this subtree has any task ready to be offered a slot. */
    boolean hasPendingTasks();
}
