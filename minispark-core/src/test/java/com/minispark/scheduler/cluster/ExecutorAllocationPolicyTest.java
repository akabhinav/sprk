package com.minispark.scheduler.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The dynamic-allocation decision function in isolation. */
final class ExecutorAllocationPolicyTest {

    // min=1, max=10, 2 cores/executor, 3s idle timeout
    private final ExecutorAllocationPolicy policy =
            new ExecutorAllocationPolicy(1, 10, 2, 3000);

    @Test
    void no_pending_tasks_means_no_scale_up() {
        assertThat(policy.executorsToAdd(0, 4, 2, 0)).isZero();
    }

    @Test
    void scales_up_to_cover_backlog() {
        // 8 pending + 0 running = 8 tasks / 2 cores = need 4 executors; have 1 → add 3.
        assertThat(policy.executorsToAdd(8, 0, 1, 0)).isEqualTo(3);
    }

    @Test
    void does_not_over_request_while_spawns_outstanding() {
        // Need 4, have 1, but 3 already requested → add 0.
        assertThat(policy.executorsToAdd(8, 0, 1, 3)).isZero();
    }

    @Test
    void respects_max_executors() {
        // 100 tasks / 2 = 50 needed, capped at max=10; have 2 → add 8.
        assertThat(policy.executorsToAdd(100, 0, 2, 0)).isEqualTo(8);
    }

    @Test
    void removes_idle_executor_past_timeout_but_not_below_min() {
        assertThat(policy.shouldRemoveIdle(3, 5000)).isTrue();   // idle 5s > 3s, have 3 > min 1
        assertThat(policy.shouldRemoveIdle(3, 1000)).isFalse();  // not idle long enough
        assertThat(policy.shouldRemoveIdle(1, 5000)).isFalse();  // already at min
    }
}
