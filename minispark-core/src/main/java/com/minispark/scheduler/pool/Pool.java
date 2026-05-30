package com.minispark.scheduler.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentMap;

/**
 * A node in the scheduling tree that holds child {@link Schedulable}s and
 * orders them by its {@link SchedulingMode}. {@link #orderedSchedulables()}
 * returns the children that have pending work, sorted by the active algorithm —
 * the order in which they should be offered resources.
 *
 * <p>We use a two-level tree like Spark: a root pool (FAIR across named pools)
 * whose children are named {@code Pool}s (each FIFO across its stages). A stage
 * with no explicit pool lands in the {@code "default"} pool.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.Pool
 */
public final class Pool implements Schedulable {

    private final String name;
    private final SchedulingMode mode;
    private final int weight;
    private final int minShare;
    private final long priority;

    private final List<Schedulable> children = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, Pool> childPools = new ConcurrentHashMap<>();

    public Pool(String name, SchedulingMode mode, int weight, int minShare, long priority) {
        this.name = name;
        this.mode = mode;
        this.weight = Math.max(1, weight);
        this.minShare = Math.max(0, minShare);
        this.priority = priority;
    }

    public SchedulingMode mode() { return mode; }

    public void addSchedulable(Schedulable s) { children.add(s); }
    public void removeSchedulable(Schedulable s) { children.remove(s); }

    /** Get or create a named child pool (used for the root → named-pool level). */
    public Pool getOrCreatePool(String poolName, SchedulingMode childMode, int weight, int minShare) {
        return childPools.computeIfAbsent(poolName, n -> {
            Pool p = new Pool(n, childMode, weight, minShare, 0);
            children.add(p);
            return p;
        });
    }

    /** Children with pending tasks, ordered by this pool's scheduling algorithm. */
    public List<Schedulable> orderedSchedulables() {
        List<Schedulable> active = new ArrayList<>();
        for (Schedulable c : children) if (c.hasPendingTasks()) active.add(c);
        active.sort(SchedulingAlgorithms.forMode(mode));
        return active;
    }

    @Override public String name() { return name; }
    @Override public int weight() { return weight; }
    @Override public int minShare() { return minShare; }
    @Override public long priority() { return priority; }

    @Override
    public int runningTasks() {
        int sum = 0;
        for (Schedulable c : children) sum += c.runningTasks();
        return sum;
    }

    @Override
    public boolean hasPendingTasks() {
        for (Schedulable c : children) if (c.hasPendingTasks()) return true;
        return false;
    }
}
