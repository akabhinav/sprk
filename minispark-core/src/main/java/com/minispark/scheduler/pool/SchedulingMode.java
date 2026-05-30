package com.minispark.scheduler.pool;

/**
 * How a {@link Pool} orders the schedulables it contains when handing out
 * resources.
 *
 * <ul>
 *   <li>{@code FIFO} — earlier-submitted stages get all the resources they can
 *       use before later ones get any. Simple, but a big early job starves
 *       everything behind it.</li>
 *   <li>{@code FAIR} — resources are shared across pools by weight, with a
 *       per-pool minimum share honored first. A short interactive job in its
 *       own pool isn't blocked behind a long batch job.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.scheduler.SchedulingMode
 */
public enum SchedulingMode {
    FIFO, FAIR
}
