package com.minispark.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Per-task wrapper around {@link UnifiedMemoryManager}. Tracks the set of
 * {@link MemoryConsumer}s holding execution memory for one task attempt; if
 * one consumer can't get the bytes it asks for, the manager walks the others
 * largest-first and asks them to {@link MemoryConsumer#spill(long) spill}.
 *
 * <p>End-of-task: call {@link #releaseAllForTask} to release any straggler
 * acquisitions back to the pool. The executor backend does this in a finally
 * block around every task body.
 *
 * Real Spark equivalent: org.apache.spark.memory.TaskMemoryManager.
 */
public final class TaskMemoryManager {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMemoryManager.class);

    private final UnifiedMemoryManager mm;
    private final long taskAttemptId;
    private final List<MemoryConsumer> consumers = new ArrayList<>();

    public TaskMemoryManager(UnifiedMemoryManager mm, long taskAttemptId) {
        this.mm = mm;
        this.taskAttemptId = taskAttemptId;
    }

    public long taskAttemptId() { return taskAttemptId; }

    public synchronized void registerConsumer(MemoryConsumer c) {
        consumers.add(c);
    }

    /**
     * Try to acquire {@code n} bytes for the requesting consumer. Returns
     * the actual amount granted. If the pool can't satisfy the full request,
     * peer consumers are asked to spill (largest first) and the acquisition
     * is retried. Returns whatever total we managed to gather; the caller
     * decides whether that's enough.
     */
    public long acquireExecutionMemory(long n, MemoryConsumer requester) {
        long granted = mm.acquireExecutionMemory(n, taskAttemptId);
        if (granted >= n) return granted;

        // Not enough — ask peers to spill largest-first. Spill returns the
        // amount they released to execution pool; we re-acquire from there.
        List<MemoryConsumer> peers;
        synchronized (this) {
            peers = new ArrayList<>(consumers);
        }
        peers.sort(Comparator.comparingLong(MemoryConsumer::memoryUsed).reversed());
        for (MemoryConsumer peer : peers) {
            if (peer == requester) continue;
            long needed = n - granted;
            if (needed <= 0) break;
            long released;
            try {
                released = peer.spill(needed);
            } catch (IOException e) {
                LOG.warn("Spill failed on consumer {}: {}", peer, e.toString());
                continue;
            }
            if (released > 0) {
                long more = mm.acquireExecutionMemory(needed, taskAttemptId);
                granted += more;
                LOG.debug("Spilled {} bytes from peer, re-acquired {} more (granted now {})",
                        released, more, granted);
            }
        }
        return granted;
    }

    public void releaseExecutionMemory(long n, MemoryConsumer ignored) {
        mm.releaseExecutionMemory(n, taskAttemptId);
    }

    /**
     * Drain everything this task ever acquired. Called from the executor's
     * task-finished hook so a task that forgot to release some bytes
     * doesn't permanently shrink the pool.
     */
    public long releaseAllForTask() {
        return mm.releaseAllExecutionMemoryForTask(taskAttemptId);
    }
}
