package com.minispark.scheduler.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The locality placement decision in isolation. */
final class LocalitySchedulerTest {

    private static LocalityScheduler.ExecutorSlot slot(String id, String host, int free) {
        return new LocalityScheduler.ExecutorSlot(id, host, free);
    }

    @Test
    void prefers_node_local_executor_when_available() {
        List<LocalityScheduler.ExecutorSlot> slots = List.of(
                slot("e1", "hostA", 1),
                slot("e2", "hostB", 1));
        // Task prefers hostB → should land on e2, not the first-iterated e1.
        assertThat(LocalityScheduler.select(slots, List.of("hostB"))).isEqualTo("e2");
    }

    @Test
    void falls_back_to_any_free_executor_when_no_local_slot() {
        List<LocalityScheduler.ExecutorSlot> slots = List.of(
                slot("e1", "hostA", 0),   // preferred host but no free core
                slot("e2", "hostB", 1));
        assertThat(LocalityScheduler.select(slots, List.of("hostA"))).isEqualTo("e2");
    }

    @Test
    void no_preference_picks_any_free() {
        List<LocalityScheduler.ExecutorSlot> slots = List.of(
                slot("e1", "hostA", 0),
                slot("e2", "hostB", 2));
        assertThat(LocalityScheduler.select(slots, List.of())).isEqualTo("e2");
    }

    @Test
    void returns_null_when_no_capacity() {
        List<LocalityScheduler.ExecutorSlot> slots = List.of(
                slot("e1", "hostA", 0),
                slot("e2", "hostB", 0));
        assertThat(LocalityScheduler.select(slots, List.of("hostA"))).isNull();
    }
}
