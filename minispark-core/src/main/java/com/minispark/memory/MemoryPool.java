package com.minispark.memory;

/**
 * Reserved region of bytes from which {@link MemoryConsumer}s acquire and
 * release. Two concrete pools share the executor's heap budget:
 * {@link StorageMemoryPool} (RDD cache + broadcasts) and
 * {@link ExecutionMemoryPool} (shuffle / aggregation / sort buffers). The
 * pool boundary is mutable: {@link UnifiedMemoryManager} can grow one pool
 * at the other's expense as long as floors are respected.
 *
 * <p>Threading: all state mutation goes through synchronized methods. The
 * {@code UnifiedMemoryManager} holds the lock when manipulating multiple
 * pools so a borrow doesn't race against a concurrent acquire.
 *
 * Real Spark equivalent: org.apache.spark.memory.MemoryPool.
 */
public abstract class MemoryPool {

    protected long poolSize;
    protected long memoryUsed;

    protected MemoryPool(long initialPoolSize) {
        if (initialPoolSize < 0) throw new IllegalArgumentException("pool size must be non-negative");
        this.poolSize = initialPoolSize;
    }

    public synchronized long poolSize() { return poolSize; }
    public synchronized long memoryUsed() { return memoryUsed; }
    public synchronized long memoryFree() { return Math.max(0, poolSize - memoryUsed); }

    /** Grow this pool by {@code delta} bytes. The caller must have shrunk the other pool by the same amount. */
    public synchronized void incrementPoolSize(long delta) {
        if (delta < 0) throw new IllegalArgumentException("delta must be non-negative; use decrementPoolSize");
        poolSize += delta;
    }

    /** Shrink this pool by {@code delta} bytes. Caller-guarded: must not reduce below {@link #memoryUsed}. */
    public synchronized void decrementPoolSize(long delta) {
        if (delta < 0) throw new IllegalArgumentException("delta must be non-negative");
        if (poolSize - delta < memoryUsed) {
            throw new IllegalStateException(
                    "cannot shrink pool below memoryUsed (poolSize=" + poolSize
                            + ", memoryUsed=" + memoryUsed + ", delta=" + delta + ")");
        }
        poolSize -= delta;
    }
}
