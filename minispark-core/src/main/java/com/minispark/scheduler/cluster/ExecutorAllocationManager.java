package com.minispark.scheduler.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Drives dynamic allocation: on a timer, reads the backlog and executor state
 * from the backend, asks {@link ExecutorAllocationPolicy} what to do, and tells
 * the {@link ExecutorLauncher} to add or the backend to release executors.
 *
 * <p>Outstanding-request tracking prevents over-provisioning: requested-but-not-
 * yet-registered executors are subtracted from the deficit, and the count is
 * reconciled down as the backend's executor set grows.
 *
 * Real Spark equivalent: org.apache.spark.ExecutorAllocationManager (the timer
 * + the scaling decisions; the policy math lives in ExecutorAllocationPolicy).
 */
public final class ExecutorAllocationManager {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorAllocationManager.class);

    private final CoarseGrainedSchedulerBackend backend;
    private final ExecutorAllocationPolicy policy;
    private final long intervalMs;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "executor-allocation");
                t.setDaemon(true);
                return t;
            });
    // Executors requested but not yet seen registered, so we don't keep
    // re-requesting while spawns are in flight.
    private volatile int outstanding = 0;
    private int lastSeenExecutors = 0;

    public ExecutorAllocationManager(CoarseGrainedSchedulerBackend backend,
                                     ExecutorAllocationPolicy policy, long intervalMs) {
        this.backend = backend;
        this.policy = policy;
        this.intervalMs = intervalMs;
    }

    public void start() {
        timer.scheduleAtFixedRate(this::tick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Dynamic allocation enabled (min={}, max={}, every {}ms)",
                policy.minExecutors(), policy.maxExecutors(), intervalMs);
    }

    public void stop() { timer.shutdownNow(); }

    private void tick() {
        try {
            reconcileOutstanding();
            scaleUpIfBacklog();
            scaleDownIdle();
        } catch (Throwable t) {
            LOG.warn("Allocation tick failed: {}", t.toString());
        }
    }

    /** As new executors register, drop them from the outstanding count. */
    private void reconcileOutstanding() {
        int now = backend.numExecutors();
        int registeredSinceLast = now - lastSeenExecutors;
        if (registeredSinceLast > 0) {
            outstanding = Math.max(0, outstanding - registeredSinceLast);
        }
        lastSeenExecutors = now;
    }

    private void scaleUpIfBacklog() {
        if (!backend.launcher().supportsDynamicAllocation()) return;
        int toAdd = policy.executorsToAdd(
                backend.pendingTaskCount(), backend.runningTaskCount(),
                backend.numExecutors(), outstanding);
        if (toAdd > 0) {
            LOG.info("Dynamic allocation: requesting {} executor(s) (pending={}, have={}, outstanding={})",
                    toAdd, backend.pendingTaskCount(), backend.numExecutors(), outstanding);
            backend.launcher().requestExecutors(toAdd);
            outstanding += toAdd;
        }
    }

    private void scaleDownIdle() {
        if (!backend.launcher().supportsDynamicAllocation()) return;
        // Only release when there's no backlog to serve.
        if (backend.pendingTaskCount() > 0) return;
        for (Map.Entry<String, Long> e : backend.idleExecutors().entrySet()) {
            if (policy.shouldRemoveIdle(backend.numExecutors(), e.getValue())) {
                backend.releaseExecutor(e.getKey());
            }
        }
    }
}
