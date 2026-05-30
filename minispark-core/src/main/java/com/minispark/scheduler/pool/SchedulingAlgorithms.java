package com.minispark.scheduler.pool;

import java.util.Comparator;

/**
 * The two orderings a {@link Pool} can use to decide which child gets the next
 * resource offer. Pulled out as pure {@link Comparator}s so the decision is
 * trivially unit-testable in isolation.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.SchedulingAlgorithm
 *                        (FIFOSchedulingAlgorithm / FairSchedulingAlgorithm).
 */
public final class SchedulingAlgorithms {

    private SchedulingAlgorithms() {}

    /** Lower submission priority first (earlier jobs win). */
    public static final Comparator<Schedulable> FIFO =
            Comparator.comparingLong(Schedulable::priority);

    /**
     * Fair ordering, matching Spark's FairSchedulingAlgorithm:
     * <ol>
     *   <li>A schedulable below its {@code minShare} outranks one that's met it
     *       (guaranteed capacity is honored before anything else).</li>
     *   <li>If both are below min share, the one further below (by ratio
     *       {@code running/minShare}) wins.</li>
     *   <li>If both are at/above min share, the one with the lower
     *       weighted load ({@code running/weight}) wins — that's the actual
     *       fairness: heavier pools are allowed proportionally more running
     *       tasks before they're considered "full".</li>
     *   <li>Ties broken by name for determinism.</li>
     * </ol>
     */
    public static final Comparator<Schedulable> FAIR = (a, b) -> {
        int aRun = a.runningTasks(), bRun = b.runningTasks();
        int aMin = a.minShare(), bMin = b.minShare();
        boolean aNeedy = aRun < aMin;
        boolean bNeedy = bRun < bMin;

        if (aNeedy && !bNeedy) return -1;
        if (!aNeedy && bNeedy) return 1;

        if (aNeedy) {
            // Both under min share: prefer the one proportionally further below.
            double aRatio = aMin == 0 ? Double.MAX_VALUE : (double) aRun / aMin;
            double bRatio = bMin == 0 ? Double.MAX_VALUE : (double) bRun / bMin;
            int c = Double.compare(aRatio, bRatio);
            if (c != 0) return c;
        } else {
            // Both met min share: prefer the lower weighted task load.
            double aLoad = (double) aRun / Math.max(1, a.weight());
            double bLoad = (double) bRun / Math.max(1, b.weight());
            int c = Double.compare(aLoad, bLoad);
            if (c != 0) return c;
        }
        return a.name().compareTo(b.name());
    };

    public static Comparator<Schedulable> forMode(SchedulingMode mode) {
        return mode == SchedulingMode.FAIR ? FAIR : FIFO;
    }
}
