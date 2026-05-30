package com.minispark.scheduler.pool;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The FIFO and FAIR orderings in isolation. */
final class SchedulingAlgorithmsTest {

    /** Minimal Schedulable for testing the comparators. */
    private record Sched(String name, int weight, int minShare, long priority, int runningTasks)
            implements Schedulable {
        @Override public boolean hasPendingTasks() { return true; }
    }

    @Test
    void fifo_orders_by_submission_priority() {
        Sched early = new Sched("a", 1, 0, 1, 100);
        Sched late = new Sched("b", 1, 0, 2, 0);
        // Earlier priority wins regardless of load.
        assertThat(SchedulingAlgorithms.FIFO.compare(early, late)).isNegative();
    }

    @Test
    void fair_prefers_pool_below_min_share() {
        Sched needy = new Sched("needy", 1, 4, 5, 1);   // 1 < 4 min share
        Sched satisfied = new Sched("ok", 1, 0, 1, 0);  // met its (zero) min share
        assertThat(SchedulingAlgorithms.FAIR.compare(needy, satisfied)).isNegative();
    }

    @Test
    void fair_prefers_lower_weighted_load_when_both_met_min_share() {
        // Both above min share (0). a has weight 1 running 4 → load 4.
        // b has weight 2 running 4 → load 2. b should be preferred (lower load).
        Sched a = new Sched("a", 1, 0, 1, 4);
        Sched b = new Sched("b", 2, 0, 2, 4);
        assertThat(SchedulingAlgorithms.FAIR.compare(a, b)).isPositive();
        assertThat(SchedulingAlgorithms.FAIR.compare(b, a)).isNegative();
    }

    @Test
    void fair_higher_weight_pool_tolerates_more_running_before_being_full() {
        // Weight-3 pool running 6 → load 2; weight-1 pool running 3 → load 3.
        // The weight-1 pool is "more loaded" and should yield to the weight-3 one
        // only when the weight-3 one's load is lower. Here w3 has lower load → wins.
        Sched heavy = new Sched("heavy", 3, 0, 1, 6);
        Sched light = new Sched("light", 1, 0, 2, 3);
        assertThat(SchedulingAlgorithms.FAIR.compare(heavy, light)).isNegative();
    }
}
