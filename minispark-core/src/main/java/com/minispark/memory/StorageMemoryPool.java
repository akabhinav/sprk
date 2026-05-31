package com.minispark.memory;

import com.minispark.storage.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks bytes in use by the storage tier (RDD cache, broadcasts). The
 * {@link MemoryStore} acquires from here before adding a block, and releases
 * when a block is evicted or removed. When the execution side needs more
 * room than its pool currently holds and the storage pool can give up bytes,
 * {@link #freeSpaceToShrinkPool} asks the MemoryStore to evict LRU blocks
 * up to the requested amount.
 *
 * Real Spark equivalent: org.apache.spark.memory.StorageMemoryPool.
 */
public final class StorageMemoryPool extends MemoryPool {

    private static final Logger LOG = LoggerFactory.getLogger(StorageMemoryPool.class);

    // Set once, after the MemoryStore is constructed (chicken-and-egg with SparkEnv wiring).
    private MemoryStore memoryStore;

    public StorageMemoryPool(long initialPoolSize) { super(initialPoolSize); }

    public synchronized void setMemoryStore(MemoryStore store) { this.memoryStore = store; }

    /**
     * Acquire {@code n} bytes for caching. Returns {@code false} if even after
     * evicting LRU evictable blocks the pool still can't fit it — the caller
     * should fall back to disk-only or drop.
     */
    public synchronized boolean acquireMemory(long n) {
        if (n <= memoryFree()) {
            memoryUsed += n;
            return true;
        }
        long shortfall = n - memoryFree();
        long freed = (memoryStore == null) ? 0 : memoryStore.evictBytesUpTo(shortfall);
        if (freed < shortfall) return false;
        // memoryUsed went down by `freed` via the eviction callback;
        // recompute here to be defensive against drift.
        if (n > memoryFree()) return false;
        memoryUsed += n;
        return true;
    }

    /** Release {@code n} bytes back to the pool — called when MemoryStore drops a block. */
    public synchronized void releaseMemory(long n) {
        memoryUsed = Math.max(0, memoryUsed - n);
    }

    /**
     * Bookkeeping-only acquire used by the MemoryStore's onAcquire callback.
     * MemoryStore has already accepted the put; the pool just needs its
     * accounting to mirror MemoryStore.usedBytes. May briefly exceed the
     * pool's poolSize when MemoryStore.put borrowed via its own LRU
     * eviction — that's a learning-grade simplification; real Spark
     * gates put with acquireMemory up front. Callers are responsible for
     * keeping this in sync.
     */
    public synchronized void incrementUsedDirectly(long n) {
        memoryUsed += n;
    }

    /**
     * Asked by {@link UnifiedMemoryManager} when execution needs more room.
     * Returns the number of bytes actually freed (which may be 0 if every
     * block in MemoryStore is pinned, or less than requested if the LRU
     * doesn't have enough evictable blocks).
     */
    public synchronized long freeSpaceToShrinkPool(long bytes) {
        if (bytes <= memoryFree()) return bytes;
        long extra = bytes - memoryFree();
        long evicted = (memoryStore == null) ? 0 : memoryStore.evictBytesUpTo(extra);
        long freed = memoryFree() + evicted;
        LOG.debug("freeSpaceToShrinkPool({}) evicted {} bytes, freed total {}", bytes, evicted, freed);
        return Math.min(bytes, freed);
    }
}
