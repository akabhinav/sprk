package com.minispark.scheduler.pool;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** The pool tree's stage ordering, across FIFO and FAIR roots. */
final class PoolSchedulerTest {

    @Test
    void fifo_root_orders_stages_by_submission() {
        PoolScheduler ps = new PoolScheduler(SchedulingMode.FIFO);
        // All stages in the default pool, submitted in order 10, 11, 12.
        ps.addStage(10, null, 1, () -> 0, () -> 5);
        ps.addStage(11, null, 2, () -> 0, () -> 5);
        ps.addStage(12, null, 3, () -> 0, () -> 5);
        assertThat(ps.orderedStageIds()).containsExactly(10, 11, 12);
    }

    @Test
    void fair_root_interleaves_pools_and_favors_the_less_loaded() {
        PoolScheduler ps = new PoolScheduler(SchedulingMode.FAIR);
        ps.configurePool("batch", 1, 0);
        ps.configurePool("adhoc", 1, 0);

        AtomicInteger batchRunning = new AtomicInteger(8);  // batch already busy
        AtomicInteger adhocRunning = new AtomicInteger(0);  // adhoc idle

        ps.addStage(1, "batch", 1, batchRunning::get, () -> 4);
        ps.addStage(2, "adhoc", 2, adhocRunning::get, () -> 4);

        // FAIR: adhoc (running 0) is far less loaded than batch (running 8),
        // so adhoc's stage should be offered resources first.
        assertThat(ps.orderedStageIds().get(0)).isEqualTo(2);
    }

    @Test
    void completed_stages_drop_out_of_the_order() {
        PoolScheduler ps = new PoolScheduler(SchedulingMode.FIFO);
        AtomicInteger pending10 = new AtomicInteger(3);
        ps.addStage(10, null, 1, () -> 0, pending10::get);
        ps.addStage(11, null, 2, () -> 0, () -> 2);

        assertThat(ps.orderedStageIds()).containsExactly(10, 11);
        pending10.set(0); // stage 10 has no more pending tasks
        assertThat(ps.orderedStageIds()).containsExactly(11);
        ps.removeStage(11);
        assertThat(ps.orderedStageIds()).isEmpty();
    }
}
