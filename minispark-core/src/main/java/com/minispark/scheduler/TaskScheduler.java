package com.minispark.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Queues TaskSets, hands them to the {@link SchedulerBackend}, and tracks
 * per-stage completion. Has no idea whether the backend is local or cluster
 * — that's the whole point of the seam.
 *
 * <p>This is a deliberately minimal scheduler: FIFO over TaskSets, with a
 * single completion future per stage. Real Spark adds locality scheduling,
 * pools, and speculation here; those slot in without changing the surface.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.TaskSchedulerImpl
 */
public final class TaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    private SchedulerBackend backend;

    // FIFO of TaskSets the backend has yet to drain.
    private final Deque<TaskSet> pending = new ArrayDeque<>();

    private static final class StageBook {
        final int totalTasks;
        final AtomicInteger remaining;
        final List<TaskResult<?>> results;
        final CompletableFuture<List<TaskResult<?>>> done = new CompletableFuture<>();
        StageBook(int totalTasks) {
            this.totalTasks = totalTasks;
            this.remaining = new AtomicInteger(totalTasks);
            this.results = new ArrayList<>(totalTasks);
        }
    }
    private final ConcurrentMap<Integer, StageBook> books = new ConcurrentHashMap<>();

    public void setBackend(SchedulerBackend backend) { this.backend = backend; }

    /**
     * Submits a TaskSet and returns a future that completes when every task
     * in the set has reported back.
     */
    public CompletableFuture<List<TaskResult<?>>> submitTasks(TaskSet ts) {
        StageBook book = books.computeIfAbsent(ts.stageId(), id -> new StageBook(ts.size()));
        synchronized (pending) {
            pending.add(ts);
        }
        LOG.debug("Submitted stage {} with {} tasks", ts.stageId(), ts.size());
        backend.reviveOffers();
        return book.done;
    }

    /** Called by the backend to drain the queue. */
    public List<TaskSet> drainPending() {
        synchronized (pending) {
            List<TaskSet> out = new ArrayList<>(pending);
            pending.clear();
            return out;
        }
    }

    /** Backend reports a task's outcome here. */
    public void taskCompleted(int stageId, int partitionId, Object value) {
        StageBook book = books.get(stageId);
        if (book == null) return;
        synchronized (book.results) {
            book.results.add(new TaskResult<>(stageId, partitionId, value));
        }
        if (book.remaining.decrementAndGet() == 0) {
            LOG.debug("Stage {} complete ({} tasks)", stageId, book.totalTasks);
            book.done.complete(new ArrayList<>(book.results));
            books.remove(stageId);
        }
    }

    public void taskFailed(int stageId, int partitionId, Throwable t) {
        StageBook book = books.get(stageId);
        if (book == null) return;
        book.done.completeExceptionally(
                new RuntimeException("Task " + stageId + "/" + partitionId + " failed", t));
        books.remove(stageId);
    }

    public int defaultParallelism() { return backend.defaultParallelism(); }
}
