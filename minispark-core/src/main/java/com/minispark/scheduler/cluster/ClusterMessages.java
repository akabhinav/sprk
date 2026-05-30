package com.minispark.scheduler.cluster;

import java.io.Serializable;

/**
 * The driver↔executor wire protocol. These are the messages that, in real
 * Spark, flow between {@code CoarseGrainedSchedulerBackend} (driver) and
 * {@code CoarseGrainedExecutorBackend} (executor).
 *
 * <p>Note every message carries only primitives / byte arrays / addresses —
 * never an {@link com.minispark.rpc.RpcEndpointRef}. Each side rebuilds the
 * refs it needs from its own {@link com.minispark.rpc.RpcEnv} using the
 * host/port in the message. This sidesteps having to rebind serialized refs
 * on the receiving env (a simplification vs real Spark, which does rebind).
 *
 * Real Spark equivalent: org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages
 */
public final class ClusterMessages {

    private ClusterMessages() {}

    /** Executor → Driver (ask): "I'm up, here's how to reach me and my capacity." */
    public record RegisterExecutor(String executorId, String host, int port, int cores)
            implements Serializable {}

    /** Driver → Executor (reply): registration accepted. */
    public record RegisteredExecutor() implements Serializable {}

    /** Driver → Executor (reply): registration rejected. */
    public record RegisterExecutorFailed(String reason) implements Serializable {}

    /** Driver → Executor (send): run this serialized task. */
    public record LaunchTask(int stageId, int partitionId, byte[] taskBytes)
            implements Serializable {}

    /**
     * Executor → Driver (send): task outcome.
     * {@code resultBytes} is the serialized return value on success;
     * {@code failureReason} is the structured cause on failure;
     * {@code accumulatorUpdates} carries per-accumulator deltas the task
     * produced (sent on success and on failure, just like real Spark).
     */
    public record StatusUpdate(String executorId, int stageId, int partitionId, int attempt,
                               TaskState state, byte[] resultBytes,
                               TaskFailureReason failureReason,
                               java.util.Map<Long, Object> accumulatorUpdates)
            implements Serializable {}

    /**
     * Executor → Driver (send): "I'm still alive." The driver tracks last-seen
     * time per executor and marks one lost if heartbeats stop. No reply: a
     * round-trip would amplify the cost of a slow driver into stalled executors.
     */
    public record Heartbeat(String executorId) implements Serializable {}

    /** Driver self-message (send): there may be resources to offer; try to schedule. */
    public record ReviveOffers() implements Serializable {}

    /** Driver → Executor (send): shut down. */
    public record StopExecutor() implements Serializable {}
}
