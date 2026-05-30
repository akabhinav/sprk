package com.minispark.scheduler;

import com.minispark.executor.Executor;
import com.minispark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process backend: one {@link Executor} running in the driver JVM.
 *
 * <p>Even though there is no network, tasks still pass through the
 * {@link Serializer} on the way out and back. This is intentional — it
 * surfaces "this closure captured something non-serializable" failures in
 * local tests, long before Phase 5 starts shipping bytes across the wire.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.local.LocalSchedulerBackend
 */
public final class LocalSchedulerBackend implements SchedulerBackend {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSchedulerBackend.class);

    private final TaskScheduler scheduler;
    private final Serializer serializer;
    private final int cores;
    private Executor executor;

    public LocalSchedulerBackend(TaskScheduler scheduler, Serializer serializer, int cores) {
        this.scheduler = scheduler;
        this.serializer = serializer;
        this.cores = cores;
    }

    @Override
    public void start() {
        executor = new Executor("driver", cores, serializer);
        LOG.info("LocalSchedulerBackend started with {} cores", cores);
    }

    @Override
    public void stop() {
        if (executor != null) executor.shutdown();
    }

    @Override
    public void reviveOffers() {
        for (TaskSet ts : scheduler.drainPending()) {
            for (Task<?> task : ts.tasks()) {
                byte[] bytes = serializer.serialize(task);
                executor.launchTask(
                        bytes,
                        task.stageId(), task.partitionId(),
                        (ctx, result) -> scheduler.taskCompleted(task.stageId(), task.partitionId(), result),
                        (ctx, err)    -> scheduler.taskFailed(task.stageId(), task.partitionId(), err));
            }
        }
    }

    @Override public int defaultParallelism() { return cores; }
}
