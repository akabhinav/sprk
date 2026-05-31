package com.minispark.memory;

import com.minispark.storage.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits a fixed executor heap budget into a storage pool and an execution
 * pool with a <b>movable boundary</b>: either side can grow at the other's
 * expense as long as the floors are respected.
 *
 * <p><b>Rules</b> (matches real Spark's UnifiedMemoryManager):
 * <ul>
 *   <li>Storage may borrow from execution's <i>free</i> bytes (no eviction
 *       on the execution side; execution acquisitions just see a smaller
 *       pool until something releases).</li>
 *   <li>Execution may evict storage down to the storage floor
 *       ({@code unifiedMax * storageFraction}). Stored blocks below the
 *       floor are protected — a runaway aggregator can't kill the cache
 *       entirely.</li>
 * </ul>
 *
 * <p>Sizing knobs (mirror Spark's keys):
 * <ul>
 *   <li>{@code minispark.memory.executorMaxBytes} — total budget split between
 *       the two pools (default 512 MiB, same as the previous standalone cap).</li>
 *   <li>{@code minispark.memory.storageFraction} — fraction of that going to
 *       storage initially / as floor (default 0.5).</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.memory.UnifiedMemoryManager.
 */
public final class UnifiedMemoryManager {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedMemoryManager.class);

    private final long unifiedMaxBytes;
    private final long storagePoolFloor;
    private final StorageMemoryPool storagePool;
    private final ExecutionMemoryPool executionPool;

    public UnifiedMemoryManager(long unifiedMaxBytes, double storageFraction) {
        if (unifiedMaxBytes <= 0) throw new IllegalArgumentException("unifiedMaxBytes must be > 0");
        if (storageFraction < 0 || storageFraction > 1)
            throw new IllegalArgumentException("storageFraction must be in [0, 1]");
        this.unifiedMaxBytes = unifiedMaxBytes;
        long initialStorage = (long) (unifiedMaxBytes * storageFraction);
        this.storagePoolFloor = initialStorage;
        this.storagePool = new StorageMemoryPool(initialStorage);
        this.executionPool = new ExecutionMemoryPool(unifiedMaxBytes - initialStorage);
    }

    public long unifiedMaxBytes() { return unifiedMaxBytes; }
    public StorageMemoryPool storagePool() { return storagePool; }
    public ExecutionMemoryPool executionPool() { return executionPool; }

    /**
     * Acquire {@code n} bytes for storage. Borrows free execution bytes if the
     * storage pool isn't big enough on its own. Returns {@code false} if even
     * after borrowing + the MemoryStore evicting its LRU, the bytes still
     * don't fit — the caller (typically NetworkBlockManager) should then drop
     * the block or spill it to disk per its StorageLevel.
     */
    public boolean acquireStorageMemory(long n) {
        synchronized (this) {
            // Try to grow the storage pool from execution's currently-free bytes.
            long needed = n - storagePool.memoryFree();
            if (needed > 0) {
                long borrow = Math.min(needed, executionPool.memoryFree());
                if (borrow > 0) {
                    executionPool.decrementPoolSize(borrow);
                    storagePool.incrementPoolSize(borrow);
                    LOG.debug("Storage borrowed {} bytes from execution (storage={}, exec={})",
                            borrow, storagePool.poolSize(), executionPool.poolSize());
                }
            }
            // The pool itself may still need to evict LRU; that's handled
            // inside StorageMemoryPool.acquireMemory via the MemoryStore.
            return storagePool.acquireMemory(n);
        }
    }

    public void releaseStorageMemory(long n) {
        storagePool.releaseMemory(n);
    }

    /**
     * Acquire up to {@code n} bytes for execution. May evict storage down to
     * the storage floor when execution's pool is too small. Returns the
     * actual amount granted (may be less than {@code n}; may be 0). The
     * caller — {@link TaskMemoryManager} — is responsible for asking
     * registered consumers to spill if the grant is insufficient.
     */
    public long acquireExecutionMemory(long n, long taskAttemptId) {
        synchronized (this) {
            // Grow execution at storage's expense, down to the storage floor.
            long shortfall = n - executionPool.memoryFree();
            if (shortfall > 0) {
                long evictable = Math.max(0, storagePool.poolSize() - storagePoolFloor);
                long reclaim = Math.min(shortfall, evictable);
                if (reclaim > 0) {
                    long actuallyFreed = storagePool.freeSpaceToShrinkPool(reclaim);
                    if (actuallyFreed > 0) {
                        storagePool.decrementPoolSize(actuallyFreed);
                        executionPool.incrementPoolSize(actuallyFreed);
                        LOG.debug("Execution reclaimed {} bytes from storage (storage={}, exec={})",
                                actuallyFreed, storagePool.poolSize(), executionPool.poolSize());
                    }
                }
            }
            return executionPool.acquireMemory(n, taskAttemptId);
        }
    }

    public void releaseExecutionMemory(long n, long taskAttemptId) {
        executionPool.releaseMemory(n, taskAttemptId);
    }

    public long releaseAllExecutionMemoryForTask(long taskAttemptId) {
        return executionPool.releaseAllMemoryForTask(taskAttemptId);
    }

    public void setMemoryStore(MemoryStore store) { storagePool.setMemoryStore(store); }
}
