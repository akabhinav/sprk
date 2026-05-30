package com.minispark.scheduler;

import com.minispark.scheduler.cluster.TaskFailureReason;
import com.minispark.storage.ExecutorLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Queues TaskSets, hands them to the {@link SchedulerBackend}, tracks
 * per-stage completion, and retries failed tasks up to {@link #maxAttempts}.
 *
 * <p>Phase 6: per-partition state — completed vs not, attempt count, last
 * error — so a single transient failure doesn't fail the whole stage.
 * {@code FetchFailed} is forwarded to the DAGScheduler hook which handles
 * stage recomputation; everything else just retries on the next offer.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.TaskSchedulerImpl
 *                        + TaskSetManager (which is where retries live there).
 */
public final class TaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    /** Callback the DAGScheduler registers to handle FetchFailed/executor-lost. */
    public interface DAGEventHandler {
        /** A reducer hit a missing/unreachable map output. */
        void onFetchFailed(int stageId, int partitionId, TaskFailureReason.FetchFailed reason);
        /** Executor went away. Map outputs on it are gone. */
        void onExecutorLost(ExecutorLocation loc);
    }

    private SchedulerBackend backend;
    private DAGEventHandler dagHandler = new DAGEventHandler() {
        @Override public void onFetchFailed(int s, int p, TaskFailureReason.FetchFailed r) {}
        @Override public void onExecutorLost(ExecutorLocation l) {}
    };
    private final int maxAttempts;

    private final Deque<TaskSet> pending = new ArrayDeque<>();
    private final Deque<Task<?>> retries = new ArrayDeque<>();

    private static final class PartitionState {
        Task<?> task;          // last task object (re-dispatched on retry)
        int attempts;          // attempts already started
        boolean completed;
        Object result;
        // Speculation bookkeeping: time the most-recent attempt started, and
        // whether we have already launched a duplicate copy.
        long launchedAtMs;
        boolean speculated;
        PartitionState(Task<?> task) { this.task = task; }
    }

    private static final class StageBook {
        final int totalTasks;
        final Map<Integer, PartitionState> byPartition;
        final CompletableFuture<List<TaskResult<?>>> done = new CompletableFuture<>();
        int remaining;
        StageBook(List<Task<?>> tasks) {
            this.totalTasks = tasks.size();
            this.remaining = tasks.size();
            this.byPartition = new HashMap<>(tasks.size());
            for (Task<?> t : tasks) byPartition.put(t.partitionId(), new PartitionState(t));
        }
    }
    private final ConcurrentMap<Integer, StageBook> books = new ConcurrentHashMap<>();

    // Speculation knobs (configurable in MiniSparkContext via setters below).
    private boolean speculationEnabled = false;
    private long speculationIntervalMs = 500;
    /** Fraction of tasks in a stage that must finish before we consider speculating. */
    private double speculationQuantile = 0.75;
    /** Multiplier over the median completed-task runtime; anything slower is a straggler. */
    private double speculationMultiplier = 1.5;

    private final ScheduledExecutorService speculator =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "speculator");
                t.setDaemon(true);
                return t;
            });
    private final AtomicBoolean speculatorStarted = new AtomicBoolean(false);

    public TaskScheduler() { this(4); }
    public TaskScheduler(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public void setSpeculation(boolean enabled, long intervalMs, double quantile, double multiplier) {
        this.speculationEnabled = enabled;
        this.speculationIntervalMs = intervalMs;
        this.speculationQuantile = quantile;
        this.speculationMultiplier = multiplier;
        if (enabled && speculatorStarted.compareAndSet(false, true)) {
            speculator.scheduleAtFixedRate(this::checkSpeculatableTasks,
                    intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public void setBackend(SchedulerBackend backend) { this.backend = backend; }
    public void setDAGEventHandler(DAGEventHandler handler) { this.dagHandler = handler; }
    public int maxAttempts() { return maxAttempts; }

    public CompletableFuture<List<TaskResult<?>>> submitTasks(TaskSet ts) {
        StageBook book = new StageBook(ts.tasks());
        books.put(ts.stageId(), book);
        synchronized (pending) {
            pending.add(ts);
            for (PartitionState ps : book.byPartition.values()) ps.attempts = 1;
        }
        LOG.debug("Submitted stage {} with {} tasks", ts.stageId(), ts.size());
        backend.reviveOffers();
        return book.done;
    }

    /**
     * Resubmit a single task on the next offer. The DAGScheduler calls this
     * after handling a FetchFailed (once it has cleaned up the bad map output
     * and queued the map task to re-run, the reduce task can be retried).
     */
    public void resubmitTask(Task<?> task) {
        synchronized (pending) {
            retries.add(task);
            StageBook book = books.get(task.stageId());
            if (book != null) {
                PartitionState ps = book.byPartition.get(task.partitionId());
                if (ps != null) ps.attempts++;
            }
        }
        backend.reviveOffers();
    }

    /**
     * Backend hook: a task has been dispatched to an executor. Used by the
     * speculator to measure runtime and by the UI/listener bus to time things.
     */
    public void taskLaunched(int stageId, int partitionId) {
        StageBook book = books.get(stageId);
        if (book == null) return;
        PartitionState ps = book.byPartition.get(partitionId);
        if (ps != null) ps.launchedAtMs = System.currentTimeMillis();
    }

    /** Called by the backend to drain everything (initial submissions + retries). */
    public List<TaskSet> drainPending() {
        synchronized (pending) {
            List<TaskSet> out = new ArrayList<>(pending);
            pending.clear();
            // Retries become single-task TaskSets, grouped by stage so the
            // backend still sees a coherent view of "this stage has more work".
            Map<Integer, List<Task<?>>> byStage = new HashMap<>();
            for (Task<?> t : retries) byStage.computeIfAbsent(t.stageId(), k -> new ArrayList<>()).add(t);
            retries.clear();
            for (Map.Entry<Integer, List<Task<?>>> e : byStage.entrySet()) {
                out.add(new TaskSet(e.getKey(), e.getValue()));
            }
            return out;
        }
    }

    public void taskCompleted(int stageId, int partitionId, Object value) {
        StageBook book = books.get(stageId);
        if (book == null) return;
        boolean stageDone;
        synchronized (book) {
            PartitionState ps = book.byPartition.get(partitionId);
            if (ps == null || ps.completed) return; // late duplicate (e.g. from a retry race)
            ps.completed = true;
            ps.result = value;
            book.remaining--;
            stageDone = book.remaining == 0;
        }
        if (stageDone) {
            LOG.debug("Stage {} complete ({} tasks)", stageId, book.totalTasks);
            List<TaskResult<?>> results = new ArrayList<>(book.totalTasks);
            for (PartitionState ps : book.byPartition.values()) {
                results.add(new TaskResult<>(stageId, ps.task.partitionId(), ps.result));
            }
            book.done.complete(results);
            books.remove(stageId);
        }
    }

    /**
     * Backend reports a task failure with a structured reason. We dispatch on it:
     * <ul>
     *   <li>{@link TaskFailureReason.FetchFailed} → DAGScheduler recovery path.</li>
     *   <li>Anything else, if attempts left → retry; else abort the stage.</li>
     * </ul>
     */
    public void taskFailed(int stageId, int partitionId, int attempt, TaskFailureReason reason) {
        StageBook book = books.get(stageId);
        if (book == null) return;
        PartitionState ps = book.byPartition.get(partitionId);
        if (ps == null || ps.completed) return;

        if (reason instanceof TaskFailureReason.FetchFailed ff) {
            LOG.warn("Stage {} partition {} hit FetchFailed at {}; handing to DAG layer for recovery",
                    stageId, partitionId, ff.badLocation());
            // The DAGScheduler will clean up the lost map output, schedule the
            // re-map, and then call resubmitTask() for this reduce task.
            dagHandler.onFetchFailed(stageId, partitionId, ff);
            return;
        }

        // Generic retry policy.
        int attemptsSoFar;
        synchronized (book) { attemptsSoFar = ps.attempts; }
        if (attemptsSoFar < maxAttempts) {
            LOG.warn("Task {}/{} failed (attempt {}/{}): {} — retrying",
                    stageId, partitionId, attemptsSoFar, maxAttempts, reason);
            resubmitTask(ps.task);
        } else {
            LOG.error("Task {}/{} failed after {} attempts: {}; aborting stage",
                    stageId, partitionId, maxAttempts, reason);
            book.done.completeExceptionally(
                    new RuntimeException("Stage " + stageId + " aborted: task " + partitionId
                            + " failed " + maxAttempts + " times. Last reason: " + reason));
            books.remove(stageId);
        }
    }

    /** Backend tells us an executor died. Forward to DAG for shuffle map cleanup. */
    public void executorLost(ExecutorLocation loc) {
        dagHandler.onExecutorLost(loc);
    }

    /**
     * Abort a stage immediately (used by job cancellation): complete its future
     * exceptionally so the DAGScheduler thread blocked on {@code .get()} wakes
     * up and unwinds. In-flight tasks will still report back but their stage
     * book is gone, so the results are dropped.
     */
    public void abortStage(int stageId, String reason) {
        StageBook book = books.remove(stageId);
        if (book != null && !book.done.isDone()) {
            book.done.completeExceptionally(
                    new RuntimeException("Stage " + stageId + " aborted: " + reason));
        }
    }

    /**
     * Periodic check: in any in-flight stage, if a significant fraction of tasks
     * have completed and any running task is much slower than the median
     * completed runtime, launch a duplicate of the straggler. The Phase-6
     * "first success wins" logic in {@link #taskCompleted} ensures we just
     * take whichever copy finishes first; the duplicate doesn't double-count.
     */
    private void checkSpeculatableTasks() {
        if (!speculationEnabled) return;
        long now = System.currentTimeMillis();
        for (Map.Entry<Integer, StageBook> e : books.entrySet()) {
            StageBook book = e.getValue();
            int total = book.totalTasks;
            // Per-stage: collect runtimes of completed tasks for a median.
            List<Long> completedRuntimes = new ArrayList<>();
            List<PartitionState> running = new ArrayList<>();
            synchronized (book) {
                for (PartitionState ps : book.byPartition.values()) {
                    if (ps.completed) {
                        completedRuntimes.add(Math.max(1L, ps.launchedAtMs == 0 ? 0 : 0));
                    } else if (ps.launchedAtMs > 0 && !ps.speculated) {
                        running.add(ps);
                    }
                }
            }
            int completed = total - book.remaining;
            if (completed < Math.max(1, (int) Math.ceil(total * speculationQuantile))) continue;
            // Use the median of *current* runtime estimates rather than recorded
            // completed durations (we don't store those today). Median runtime ~
            // (now - earliest still-running launch) gives a workable signal.
            // Simpler proxy: use 1.5× the time since the stage's first running
            // task launched.
            long oldestLaunch = Long.MAX_VALUE;
            synchronized (book) {
                for (PartitionState ps : book.byPartition.values()) {
                    if (ps.launchedAtMs > 0) oldestLaunch = Math.min(oldestLaunch, ps.launchedAtMs);
                }
            }
            if (oldestLaunch == Long.MAX_VALUE) continue;
            long stageElapsed = now - oldestLaunch;
            long thresholdMs = (long) (speculationMultiplier * stageElapsed / Math.max(1, completed)) * completed;
            // Effective threshold: a task that's been running > multiplier×(stage so far / completed).
            long perTaskMedian = Math.max(50, stageElapsed / Math.max(1, completed));
            long stragglerMs = (long) (speculationMultiplier * perTaskMedian);
            for (PartitionState ps : running) {
                if (now - ps.launchedAtMs < stragglerMs) continue;
                LOG.warn("Stage {} partition {} running {}ms (>{}ms) — launching speculative copy",
                        e.getKey(), ps.task.partitionId(), now - ps.launchedAtMs, stragglerMs);
                ps.speculated = true;
                synchronized (pending) { retries.add(ps.task); }
                backend.reviveOffers();
            }
        }
    }

    public void stop() { speculator.shutdownNow(); }

    public int defaultParallelism() { return backend.defaultParallelism(); }
}
