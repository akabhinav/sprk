package com.minispark.scheduler;

/**
 * <b>The central seam.</b> Local mode and cluster mode differ in exactly one
 * thing: which implementation of this interface they install. The
 * {@link TaskScheduler} stays identical across modes.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #start} — bring up executors (in-process or via the cluster manager).</li>
 *   <li>{@link #reviveOffers} — the scheduler is telling us "there is work to run, please
 *       offer it to your executors". Implementations decide how to dispatch.</li>
 *   <li>{@link #stop} — tear down executors.</li>
 * </ol>
 *
 * Real Spark equivalent: org.apache.spark.scheduler.SchedulerBackend
 */
public interface SchedulerBackend {
    void start();
    void stop();

    /** Pull pending TaskSets from the scheduler and dispatch them onto available resources. */
    void reviveOffers();

    /** Total task slots currently registered (cores across all executors). */
    int defaultParallelism();
}
