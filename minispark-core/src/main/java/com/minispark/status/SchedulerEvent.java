package com.minispark.status;

/**
 * Events the scheduler posts to the {@link LiveListenerBus} as a job runs.
 * Listeners (notably {@link AppStatusStore}, which backs the web UI) consume
 * them to build a live picture of jobs, stages, tasks, and executors.
 *
 * <p>Decoupling "doing the work" from "observing the work" via an event bus is
 * how real Spark keeps its scheduler clean while still feeding a rich UI,
 * metrics system, and event log. The scheduler just fires events; it doesn't
 * know or care who is listening.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.SparkListenerEvent hierarchy.
 */
public sealed interface SchedulerEvent {

    record JobStart(int jobId, java.util.List<Integer> stageIds, long timeMs) implements SchedulerEvent {}
    record JobEnd(int jobId, boolean success, long timeMs) implements SchedulerEvent {}

    record StageSubmitted(int stageId, String name, int numTasks,
                          java.util.List<Integer> parentStageIds, long timeMs) implements SchedulerEvent {}
    record StageCompleted(int stageId, boolean success, long timeMs) implements SchedulerEvent {}

    record TaskStart(int stageId, int partitionId, String executorId, long timeMs) implements SchedulerEvent {}
    record TaskEnd(int stageId, int partitionId, boolean success, long timeMs) implements SchedulerEvent {}

    record ExecutorAdded(String executorId, String host, int port, int cores, long timeMs) implements SchedulerEvent {}
    record ExecutorRemoved(String executorId, String reason, long timeMs) implements SchedulerEvent {}
}
