package com.minispark.scheduler.cluster;

import java.util.Collection;
import java.util.List;

/**
 * The data-locality placement decision, pulled out of the backend so it can be
 * reasoned about and unit-tested in isolation.
 *
 * <p>Given a task's preferred hosts and the currently free executor slots, pick
 * an executor — preferring one whose host matches a preferred location
 * (node-local), and otherwise falling back to any free executor (the "ANY"
 * locality level). This is the heart of what real Spark's
 * {@code TaskSetManager.resourceOffer} does, minus the multi-level delay
 * scheduling ladder (PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY).
 *
 * Real Spark equivalent: org.apache.spark.scheduler.TaskSetManager locality logic.
 */
public final class LocalityScheduler {

    private LocalityScheduler() {}

    /** A free executor slot the scheduler may place a task on. */
    public record ExecutorSlot(String executorId, String host, int freeCores) {}

    /**
     * Choose an executor id for a task with {@code preferredHosts}. Returns the
     * node-local executor if one has a free core; else any executor with a free
     * core; else {@code null} (no capacity right now).
     */
    public static String select(Collection<ExecutorSlot> slots, List<String> preferredHosts) {
        ExecutorSlot anyFree = null;
        for (ExecutorSlot s : slots) {
            if (s.freeCores() <= 0) continue;
            if (preferredHosts != null && !preferredHosts.isEmpty()
                    && preferredHosts.contains(s.host())) {
                return s.executorId(); // node-local: best possible, take it
            }
            if (anyFree == null) anyFree = s;
        }
        return anyFree == null ? null : anyFree.executorId();
    }
}
