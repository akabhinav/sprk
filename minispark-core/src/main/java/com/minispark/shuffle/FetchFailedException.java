package com.minispark.shuffle;

import com.minispark.storage.ExecutorLocation;

/**
 * Thrown by a reducer when fetching a shuffle block fails (executor unreachable,
 * block missing). Carries the structured triple {@code (shuffleId, mapId,
 * reduceId)} and the location it tried so the executor can package the failure
 * as a {@link com.minispark.scheduler.cluster.TaskFailureReason.FetchFailed}
 * and the driver can react with stage recomputation.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.FetchFailedException
 */
public final class FetchFailedException extends RuntimeException {
    private final int shuffleId;
    private final int mapId;
    private final int reduceId;
    private final ExecutorLocation badLocation;

    public FetchFailedException(int shuffleId, int mapId, int reduceId,
                                ExecutorLocation badLocation, String message, Throwable cause) {
        super(message, cause);
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.reduceId = reduceId;
        this.badLocation = badLocation;
    }

    public int shuffleId() { return shuffleId; }
    public int mapId() { return mapId; }
    public int reduceId() { return reduceId; }
    public ExecutorLocation badLocation() { return badLocation; }
}
