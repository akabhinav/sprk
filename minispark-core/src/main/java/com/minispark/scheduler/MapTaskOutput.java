package com.minispark.scheduler;

import com.minispark.storage.ExecutorLocation;

import java.io.Serializable;

/**
 * What a {@link ShuffleMapTask} reports back to the driver: where it wrote
 * its shuffle blocks plus the byte size of each reducer-partition bucket.
 *
 * <p>The per-reducer sizes are the input that AQE-style rules (e.g.
 * {@link com.minispark.scheduler.adaptive.CoalesceShufflePartitionsRule})
 * use to re-plan the downstream stage after the map stage materialises.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.MapStatus (sizes per reducer)
 */
public record MapTaskOutput(ExecutorLocation location, long[] partitionBytes)
        implements Serializable {}
