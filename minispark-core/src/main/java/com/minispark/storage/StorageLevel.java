package com.minispark.storage;

import java.io.Serializable;

/**
 * Where (and how) a cached RDD partition lives. Spark exposes a richer matrix
 * (memory/disk × deserialized/serialized × replicated); we expose only the
 * variants needed to teach the {@code rdd.cache()} idea.
 *
 * <ul>
 *   <li>{@link #NONE} — not cached. Tasks recompute every time.</li>
 *   <li>{@link #MEMORY_ONLY} — keep partitions in the executor's bounded
 *       memory store. If it doesn't fit (after LRU eviction of other cached
 *       blocks), the block is simply not cached and gets recomputed later.</li>
 *   <li>{@link #MEMORY_AND_DISK} — prefer memory; if it won't fit, or it gets
 *       evicted under pressure, spill the bytes to the local disk store
 *       instead of dropping them. Survives memory pressure, not executor death.</li>
 *   <li>{@link #DISK_ONLY} — always write to the disk store, never hold in the
 *       memory budget. Useful for very large partitions.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.storage.StorageLevel
 */
public enum StorageLevel implements Serializable {
    NONE, MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY;

    public boolean useMemory() { return this == MEMORY_ONLY || this == MEMORY_AND_DISK; }
    public boolean useDisk()   { return this == MEMORY_AND_DISK || this == DISK_ONLY; }
}
