package com.minispark.scheduler;

import com.minispark.executor.TaskContext;

import java.io.Serializable;

/**
 * Unit of work shipped to an executor. Subclasses:
 * {@link ShuffleMapTask} (writes shuffle output) and
 * {@link ResultTask} (returns a value to the driver).
 *
 * <p>Serializable from day one: even when there is no network, going through
 * serialize/deserialize on every dispatch guarantees closures captured by
 * RDD lambdas don't accidentally smuggle non-shippable state. This shakes
 * out distribution bugs in Phase 1 instead of Phase 5.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.Task
 */
public abstract class Task<T> implements Serializable {
    private final int stageId;
    private final int partitionId;
    // Hosts this task prefers to run on (data locality). Empty = run anywhere.
    private final java.util.List<String> preferredLocations;

    protected Task(int stageId, int partitionId) {
        this(stageId, partitionId, java.util.List.of());
    }

    protected Task(int stageId, int partitionId, java.util.List<String> preferredLocations) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.preferredLocations = preferredLocations == null ? java.util.List.of() : preferredLocations;
    }

    public int stageId() { return stageId; }
    public int partitionId() { return partitionId; }
    public java.util.List<String> preferredLocations() { return preferredLocations; }

    /** Run on the executor. Returns the value reported back to the driver. */
    public abstract T run(TaskContext ctx);
}
