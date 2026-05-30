package com.minispark.scheduler;

import java.util.List;

/**
 * A batch of independent tasks for one {@link Stage} submitted together to
 * the {@link TaskScheduler}.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.TaskSet
 */
public final class TaskSet {
    private final int stageId;
    private final List<Task<?>> tasks;

    public TaskSet(int stageId, List<Task<?>> tasks) {
        this.stageId = stageId;
        this.tasks = tasks;
    }

    public int stageId() { return stageId; }
    public List<Task<?>> tasks() { return tasks; }
    public int size() { return tasks.size(); }
}
