package com.minispark.scheduler;

import java.io.Serializable;

/**
 * Pair of (task index within its TaskSet, value returned by the task).
 * Backends report these to the {@link TaskScheduler}; the scheduler routes
 * them to the {@link DAGScheduler}.
 */
public final class TaskResult<T> implements Serializable {
    public final int stageId;
    public final int partitionId;
    public final T value;

    public TaskResult(int stageId, int partitionId, T value) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.value = value;
    }
}
