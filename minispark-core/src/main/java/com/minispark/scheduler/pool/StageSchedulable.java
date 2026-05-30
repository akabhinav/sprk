package com.minispark.scheduler.pool;

import java.util.function.IntSupplier;

/**
 * Leaf {@link Schedulable} representing one stage's pending TaskSet. Its
 * {@code runningTasks}/{@code hasPendingTasks} are read live from the
 * TaskScheduler's bookkeeping via suppliers, so the pool ordering always
 * reflects current state without the pool layer duplicating it.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.TaskSetManager (as a Schedulable leaf).
 */
public final class StageSchedulable implements Schedulable {

    private final int stageId;
    private final long priority;        // submission order, FIFO tie-breaker
    private final IntSupplier runningTasks;
    private final IntSupplier pendingTasks;

    public StageSchedulable(int stageId, long priority,
                            IntSupplier runningTasks, IntSupplier pendingTasks) {
        this.stageId = stageId;
        this.priority = priority;
        this.runningTasks = runningTasks;
        this.pendingTasks = pendingTasks;
    }

    public int stageId() { return stageId; }

    @Override public String name() { return "stage-" + stageId; }
    @Override public int weight() { return 1; }
    @Override public int minShare() { return 0; }
    @Override public long priority() { return priority; }
    @Override public int runningTasks() { return runningTasks.getAsInt(); }
    @Override public boolean hasPendingTasks() { return pendingTasks.getAsInt() > 0; }
}
