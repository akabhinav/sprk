package com.minispark.storage;

import java.io.Serializable;

/**
 * Where (and how) a cached RDD partition lives. Spark exposes a richer matrix
 * (memory/disk × deserialized/serialized × replicated); we expose only the
 * variants needed to teach the {@code rdd.cache()} idea.
 *
 * <ul>
 *   <li>{@link #NONE} — not cached. Tasks recompute every time.</li>
 *   <li>{@link #MEMORY_ONLY} — keep computed partitions in the executor's
 *       {@link BlockManager} (heap). If the executor dies, the cache dies
 *       with it; lineage re-computes the partition next time it's needed.
 *       Exactly the trade-off in real Spark's default cache level.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.storage.StorageLevel
 */
public enum StorageLevel implements Serializable {
    NONE, MEMORY_ONLY
}
