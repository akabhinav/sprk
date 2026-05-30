package com.minispark.scheduler.pool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/**
 * The scheduling tree for one application: a root {@link Pool} whose children
 * are named pools, each holding {@link StageSchedulable} leaves. Produces a flat
 * {@link #orderedStageIds()} — the order stages should be offered resources in,
 * recomputed live from running-task counts so FAIR sharing actually rebalances.
 *
 * <p>Two-level tree, exactly like Spark's fair scheduler:
 * <pre>
 *   root (FAIR across pools)
 *     ├─ "default" (FIFO across its stages)
 *     ├─ "etl"     (FIFO …)   weight=1 minShare=0
 *     └─ "adhoc"   (FIFO …)   weight=2 minShare=4
 * </pre>
 *
 * Real Spark equivalent: org.apache.spark.scheduler.{Pool, SchedulableBuilder}.
 */
public final class PoolScheduler {

    public static final String DEFAULT_POOL = "default";

    private final Pool root;
    private final SchedulingMode rootMode;
    // stageId -> its leaf, so we can remove it when the stage finishes.
    private final Map<Integer, StageSchedulable> stageLeaves = new HashMap<>();
    private final Map<Integer, Pool> stagePool = new HashMap<>();

    public PoolScheduler(SchedulingMode mode) {
        this.rootMode = mode;
        // Root orders pools by `mode`; within a pool, stages are FIFO.
        this.root = new Pool("root", mode, 1, 0, 0);
    }

    public SchedulingMode mode() { return rootMode; }

    /** Register a pool's weight/minShare (idempotent; created on first use). */
    public synchronized void configurePool(String poolName, int weight, int minShare) {
        root.getOrCreatePool(poolName, SchedulingMode.FIFO, weight, minShare);
    }

    /**
     * Add a stage to a named pool (created with defaults if unknown). Suppliers
     * read live running/pending counts from the TaskScheduler so ordering stays
     * current.
     */
    public synchronized void addStage(int stageId, String poolName, long priority,
                                      IntSupplier runningTasks, IntSupplier pendingTasks) {
        Pool pool = root.getOrCreatePool(
                poolName == null ? DEFAULT_POOL : poolName, SchedulingMode.FIFO, 1, 0);
        StageSchedulable leaf = new StageSchedulable(stageId, priority, runningTasks, pendingTasks);
        stageLeaves.put(stageId, leaf);
        stagePool.put(stageId, pool);
        pool.addSchedulable(leaf);
    }

    public synchronized void removeStage(int stageId) {
        StageSchedulable leaf = stageLeaves.remove(stageId);
        Pool pool = stagePool.remove(stageId);
        if (leaf != null && pool != null) pool.removeSchedulable(leaf);
    }

    /**
     * Flatten the tree into the stage order resources should be offered in:
     * root orders pools (FAIR/FIFO), each pool orders its stages (FIFO). Only
     * stages with pending tasks appear.
     */
    public synchronized List<Integer> orderedStageIds() {
        List<Integer> out = new ArrayList<>();
        for (Schedulable poolSch : root.orderedSchedulables()) {
            if (poolSch instanceof Pool p) {
                for (Schedulable stage : p.orderedSchedulables()) {
                    if (stage instanceof StageSchedulable ss) out.add(ss.stageId());
                }
            } else if (poolSch instanceof StageSchedulable ss) {
                out.add(ss.stageId());
            }
        }
        return out;
    }

    /** Rank map: stageId -> position in {@link #orderedStageIds()} (lower = sooner). */
    public synchronized Map<Integer, Integer> stageRanks() {
        List<Integer> ordered = orderedStageIds();
        Map<Integer, Integer> ranks = new HashMap<>();
        for (int i = 0; i < ordered.size(); i++) ranks.put(ordered.get(i), i);
        return ranks;
    }
}
