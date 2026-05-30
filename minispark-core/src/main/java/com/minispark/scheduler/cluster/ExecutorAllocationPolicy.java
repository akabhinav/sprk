package com.minispark.scheduler.cluster;

/**
 * The pure decision function behind dynamic allocation: given the current
 * backlog, running executors, and how long executors have sat idle, decide how
 * many to add or which to remove. Kept free of threads/timers/RPC so it can be
 * unit-tested exhaustively; the {@link ExecutorAllocationManager} wraps it with
 * the periodic polling and the actual launcher calls.
 *
 * <p>Policy (a simplified {@code ExecutorAllocationManager}):
 * <ul>
 *   <li><b>Scale up</b> when there are pending tasks the current executors
 *       can't immediately serve. Target = ceil(pending+running tasks / cores
 *       per executor), clamped to {@code [min, max]}; we only ever <i>request</i>
 *       the difference above what we have.</li>
 *   <li><b>Scale down</b> an executor that has been idle (no running tasks)
 *       longer than {@code idleTimeoutMs}, never going below {@code min}.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.ExecutorAllocationManager.
 */
public final class ExecutorAllocationPolicy {

    private final int minExecutors;
    private final int maxExecutors;
    private final int coresPerExecutor;
    private final long idleTimeoutMs;

    public ExecutorAllocationPolicy(int minExecutors, int maxExecutors,
                                    int coresPerExecutor, long idleTimeoutMs) {
        this.minExecutors = minExecutors;
        this.maxExecutors = maxExecutors;
        this.coresPerExecutor = Math.max(1, coresPerExecutor);
        this.idleTimeoutMs = idleTimeoutMs;
    }

    /**
     * How many <i>new</i> executors to request right now. {@code outstanding}
     * is the count of executors already requested but not yet registered, so we
     * don't over-request while spawns are in flight.
     */
    public int executorsToAdd(int pendingTasks, int runningTasks,
                              int currentExecutors, int outstanding) {
        if (pendingTasks <= 0) return 0;
        int totalLoad = pendingTasks + runningTasks;
        int needed = (int) Math.ceil((double) totalLoad / coresPerExecutor);
        int target = Math.min(maxExecutors, Math.max(minExecutors, needed));
        int have = currentExecutors + outstanding;
        return Math.max(0, target - have);
    }

    /** Whether an executor idle for {@code idleMs} may be removed. */
    public boolean shouldRemoveIdle(int currentExecutors, long idleMs) {
        return currentExecutors > minExecutors && idleMs >= idleTimeoutMs;
    }

    public int minExecutors() { return minExecutors; }
    public int maxExecutors() { return maxExecutors; }
}
