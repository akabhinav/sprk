package com.minispark.executor;

/**
 * Per-task runtime info handed to every {@code RDD.compute()} invocation.
 *
 * <p>Stays deliberately small in early phases; Phase 6 will grow it with
 * metrics, accumulators, and cancellation hooks.
 *
 * Real Spark equivalent: org.apache.spark.TaskContext
 */
public final class TaskContext {
    private final int stageId;
    private final int partitionId;
    private final int attemptNumber;

    public TaskContext(int stageId, int partitionId, int attemptNumber) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.attemptNumber = attemptNumber;
    }

    public int stageId() { return stageId; }
    public int partitionId() { return partitionId; }
    public int attemptNumber() { return attemptNumber; }

    @Override public String toString() {
        return "TaskContext(stage=" + stageId + ", part=" + partitionId + ", attempt=" + attemptNumber + ")";
    }
}
