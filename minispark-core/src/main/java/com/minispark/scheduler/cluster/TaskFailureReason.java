package com.minispark.scheduler.cluster;

import com.minispark.storage.ExecutorLocation;

import java.io.Serializable;

/**
 * Structured cause of a task failure as reported by an executor back to the
 * driver. The driver dispatches on the variant: {@link FetchFailed} drives a
 * stage recomputation; {@link ExecutorLost} is synthesised on the driver when
 * heartbeats stop; {@link GenericError} hits the normal retry path.
 *
 * <p>Passing a string error message would lose this structure across the
 * wire — and FetchFailed must be reified so the DAGScheduler can react to it
 * specifically (clean up the bad map output, re-run the map task).
 *
 * Real Spark equivalent: org.apache.spark.TaskEndReason hierarchy.
 */
public sealed interface TaskFailureReason extends Serializable
        permits TaskFailureReason.GenericError,
                TaskFailureReason.FetchFailed,
                TaskFailureReason.ExecutorLost {

    /** Any garden-variety task error: NPE, IO error, user lambda blew up. */
    record GenericError(String message) implements TaskFailureReason {}

    /**
     * A reducer couldn't fetch a shuffle block from {@code badLocation}. The
     * driver will: remove the map output, re-run the missing map task, then
     * retry the reduce. This is the recovery path that makes the engine
     * "Resilient" in the face of executor loss between stages.
     */
    record FetchFailed(int shuffleId, int mapId, int reduceId,
                       ExecutorLocation badLocation, String message)
            implements TaskFailureReason {}

    /** Synthesised by the driver when an executor's heartbeat times out. */
    record ExecutorLost(String executorId) implements TaskFailureReason {}
}
