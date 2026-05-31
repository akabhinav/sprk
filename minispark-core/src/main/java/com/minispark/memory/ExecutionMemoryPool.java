package com.minispark.memory;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks bytes in use by the execution tier (shuffle / aggregation / sort
 * buffers — typically a {@link MemoryConsumer} like
 * {@link ExternalAppendOnlyMap}). Per-task accounting so a runaway task
 * can't starve siblings: each {@code taskAttemptId} has its own running
 * total, and acquisition is fair-share — a task is granted memory up to
 * roughly {@code poolSize / numActiveTasks}.
 *
 * Real Spark equivalent: org.apache.spark.memory.ExecutionMemoryPool.
 */
public final class ExecutionMemoryPool extends MemoryPool {

    /** Per-task usage. Tracks who holds how much so {@code releaseAllMemoryForTask} can clean up. */
    private final Map<Long, Long> memoryUsedByTask = new HashMap<>();

    public ExecutionMemoryPool(long initialPoolSize) { super(initialPoolSize); }

    public synchronized long memoryUsedForTask(long taskAttemptId) {
        return memoryUsedByTask.getOrDefault(taskAttemptId, 0L);
    }

    /**
     * Try to acquire up to {@code n} bytes for {@code taskAttemptId}. Returns
     * the actual amount granted, which may be less than requested if the pool
     * is partly committed elsewhere. A fair-share bound applies: a task can
     * hold at most {@code poolSize / numActiveTasks} (rounded up by 1 so a
     * single task gets the whole pool).
     *
     * <p>The caller may attempt {@link MemoryConsumer#spill} on other
     * consumers and retry — that's what {@link TaskMemoryManager} does.
     */
    public synchronized long acquireMemory(long n, long taskAttemptId) {
        memoryUsedByTask.putIfAbsent(taskAttemptId, 0L);
        // Fair-share cap: one task shouldn't drink more than its share.
        int activeTasks = Math.max(1, memoryUsedByTask.size());
        long maxPerTask = poolSize / activeTasks;
        long alreadyHeld = memoryUsedByTask.get(taskAttemptId);
        long maxToGrant = Math.max(0, maxPerTask - alreadyHeld);
        long granted = Math.min(Math.min(n, memoryFree()), maxToGrant);
        if (granted > 0) {
            memoryUsed += granted;
            memoryUsedByTask.merge(taskAttemptId, granted, Long::sum);
        }
        return granted;
    }

    /** Release {@code n} bytes held by {@code taskAttemptId}. */
    public synchronized void releaseMemory(long n, long taskAttemptId) {
        long held = memoryUsedByTask.getOrDefault(taskAttemptId, 0L);
        long actual = Math.min(n, held);
        if (actual <= 0) return;
        memoryUsedByTask.merge(taskAttemptId, -actual, Long::sum);
        if (memoryUsedByTask.get(taskAttemptId) <= 0) memoryUsedByTask.remove(taskAttemptId);
        memoryUsed -= actual;
    }

    /** Release everything held by a task at end-of-task. Returns the total bytes released. */
    public synchronized long releaseAllMemoryForTask(long taskAttemptId) {
        Long held = memoryUsedByTask.remove(taskAttemptId);
        if (held == null || held <= 0) return 0;
        memoryUsed -= held;
        return held;
    }
}
