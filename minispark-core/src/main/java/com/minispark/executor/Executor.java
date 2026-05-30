package com.minispark.executor;

import com.minispark.scheduler.Task;
import com.minispark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * A pool of worker threads that runs serialized {@link Task}s. The pool uses
 * Java 21 virtual threads — tasks are typically a mix of CPU and I/O
 * (especially shuffle fetches), and virtual threads let us pretend the I/O
 * is free without managing a separate I/O pool.
 *
 * <p>Each task is sent in serialized form so this same code path works
 * unchanged across a real network in Phase 4.
 *
 * Real Spark equivalent: org.apache.spark.executor.Executor
 */
public final class Executor {

    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
    private static final AtomicInteger ATTEMPT = new AtomicInteger();

    private final ExecutorService pool;
    private final Serializer serializer;
    private final String executorId;

    public Executor(String executorId, int cores, Serializer serializer) {
        this.executorId = executorId;
        this.serializer = serializer;
        // Virtual-thread-per-task: matches Spark's "task = thread" mental model
        // without the OS-thread cost. The `cores` value is reported as
        // parallelism to the scheduler; the pool itself is unbounded.
        this.pool = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("minispark-exec-" + executorId + "-", 0).factory());
        LOG.info("Executor {} started with {} cores (virtual-thread pool)", executorId, cores);
    }

    public String executorId() { return executorId; }

    /**
     * Asynchronously runs the serialized task. {@code onSuccess} is invoked
     * with the deserialized result on completion; {@code onFailure} with the
     * exception. Both run on a pool thread.
     */
    public <T> void launchTask(byte[] serializedTask,
                               int stageId, int partitionId,
                               BiConsumer<TaskContext, Object> onSuccess,
                               BiConsumer<TaskContext, Throwable> onFailure) {
        pool.submit(() -> {
            int attempt = ATTEMPT.incrementAndGet();
            TaskContext ctx = new TaskContext(stageId, partitionId, attempt);
            try {
                Task<?> task = serializer.deserialize(serializedTask);
                LOG.debug("Running {}", ctx);
                Object result = task.run(ctx);
                onSuccess.accept(ctx, result);
            } catch (Throwable t) {
                LOG.warn("Task {} failed: {}", ctx, t.toString());
                onFailure.accept(ctx, t);
            }
        });
    }

    public void shutdown() {
        pool.shutdown();
        LOG.info("Executor {} shut down", executorId);
    }
}
