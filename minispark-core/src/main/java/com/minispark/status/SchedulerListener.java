package com.minispark.status;

/**
 * Consumer of {@link SchedulerEvent}s delivered by the {@link LiveListenerBus}.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.SparkListener
 */
public interface SchedulerListener {
    void onEvent(SchedulerEvent event);
}
