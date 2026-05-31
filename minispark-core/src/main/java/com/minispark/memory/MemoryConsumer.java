package com.minispark.memory;

import java.io.IOException;

/**
 * Anything that holds execution memory and can release some by spilling on
 * demand. Registered with a {@link TaskMemoryManager}; when a peer consumer's
 * acquisition can't be satisfied, the TaskMemoryManager calls
 * {@link #spill(long)} on the others.
 *
 * Real Spark equivalent: org.apache.spark.memory.MemoryConsumer.
 */
public abstract class MemoryConsumer {

    protected final TaskMemoryManager taskMemoryManager;
    protected long memoryUsed;

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
        this.taskMemoryManager = taskMemoryManager;
    }

    public final long memoryUsed() { return memoryUsed; }

    /**
     * Release at least {@code required} bytes by spilling internal state to
     * disk (or wherever). Implementations should return the actual amount
     * released. Returning {@code 0} signals "nothing to spill right now"
     * and the requester will fail or block its acquisition.
     */
    public abstract long spill(long required) throws IOException;
}
