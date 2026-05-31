package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.sql.plan.BroadcastHint;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.plan.LocalRelation;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;

/**
 * Turns an optimized logical plan into a {@link PhysicalPlan} by matching each
 * logical operator to its physical strategy. Mostly one-to-one (Project →
 * ProjectExec, etc.); the {@link com.minispark.sql.plan.Join} rule picks
 * between {@link ShuffledHashJoinExec} and {@link BroadcastHashJoinExec}
 * based on hints and a static row-count threshold for {@link LocalRelation}
 * sides. Real Spark also uses sort-merge join above a cost threshold.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.SparkPlanner / SparkStrategies.
 */
public final class SparkPlanner {

    /** Auto-broadcast a {@link LocalRelation} side whose row count is at or below this. */
    private static final String AUTO_BCAST_THRESHOLD_KEY = "minispark.sql.autoBroadcastJoinThreshold.rows";
    private static final int AUTO_BCAST_THRESHOLD_DEFAULT = 1000;

    private final MiniSparkContext sc;
    private final int numPartitions;
    private final int autoBroadcastRowThreshold;

    public SparkPlanner(MiniSparkContext sc, int numPartitions) {
        this.sc = sc;
        this.numPartitions = numPartitions;
        this.autoBroadcastRowThreshold =
                sc.conf().getInt(AUTO_BCAST_THRESHOLD_KEY, AUTO_BCAST_THRESHOLD_DEFAULT);
    }

    public PhysicalPlan plan(LogicalPlan logical) {
        if (logical instanceof BroadcastHint h) {
            // Hint is transparent to physical execution; the join planner has
            // already inspected it. If a BroadcastHint sits on a non-join path
            // (e.g. a user broadcast-hinted a DataFrame then called .show on it
            // directly), just plan the child.
            return plan(h.child());
        }
        if (logical instanceof LocalRelation r) {
            return new LocalTableScanExec(r.schema(), r.rows(), sc, numPartitions);
        }
        if (logical instanceof com.minispark.sql.plan.LogicalRDD lr) {
            return new RDDScanExec(lr);
        }
        if (logical instanceof Project p) {
            return new ProjectExec(p.projectList(), p.schema(), plan(p.child()));
        }
        if (logical instanceof Filter f) {
            return new FilterExec(f.condition(), plan(f.child()));
        }
        if (logical instanceof com.minispark.sql.plan.Aggregate a) {
            return new HashAggregateExec(a.groupingExprs(), a.aggregates(), a.schema(), plan(a.child()), sc);
        }
        if (logical instanceof com.minispark.sql.plan.Join j) {
            return planJoin(j);
        }
        if (logical instanceof com.minispark.sql.plan.Sort s) {
            return new SortExec(s.orders(), plan(s.child()));
        }
        if (logical instanceof com.minispark.sql.plan.Limit l) {
            return new LimitExec(l.limit(), plan(l.child()), sc);
        }
        if (logical instanceof com.minispark.sql.plan.Distinct d) {
            return new DistinctExec(plan(d.child()));
        }
        throw new UnsupportedOperationException("No physical strategy for " + logical);
    }

    /**
     * Join strategy selection — the closest thing this planner has to a cost
     * decision. Order:
     * <ol>
     *   <li>If a side carries a {@link BroadcastHint} AND the join type allows
     *       building that side, use {@link BroadcastHashJoinExec}. If both sides
     *       are hinted, prefer the right side (Spark's tiebreak too).</li>
     *   <li>Else auto-broadcast a side that is a small {@link LocalRelation}
     *       under the row-count threshold.</li>
     *   <li>Else fall back to {@link ShuffledHashJoinExec}.</li>
     * </ol>
     * FULL OUTER never broadcasts (the map-only operator can't emit unmatched
     * build-side rows correctly), so it always shuffles.
     */
    private PhysicalPlan planJoin(com.minispark.sql.plan.Join j) {
        JoinType jt = j.joinType();
        if (jt == JoinType.FULL) {
            return new ShuffledHashJoinExec(j.leftKeys(), j.rightKeys(), jt, j.schema(),
                    plan(j.left()), plan(j.right()));
        }

        boolean rightHinted = hasBroadcastHint(j.right());
        boolean leftHinted  = hasBroadcastHint(j.left());

        // Hint wins over auto.
        if (rightHinted && BroadcastHashJoinExec.canBuildRight(jt)) {
            return broadcast(j, /*buildIsLeft=*/false);
        }
        if (leftHinted && BroadcastHashJoinExec.canBuildLeft(jt)) {
            return broadcast(j, /*buildIsLeft=*/true);
        }

        // Auto-broadcast small static sides. Prefer right so INNER joins
        // (always-eligible) stay symmetric with the hint path above.
        Integer rightRows = staticRowCount(j.right());
        if (rightRows != null && rightRows <= autoBroadcastRowThreshold
                && BroadcastHashJoinExec.canBuildRight(jt)) {
            return broadcast(j, /*buildIsLeft=*/false);
        }
        Integer leftRows = staticRowCount(j.left());
        if (leftRows != null && leftRows <= autoBroadcastRowThreshold
                && BroadcastHashJoinExec.canBuildLeft(jt)) {
            return broadcast(j, /*buildIsLeft=*/true);
        }

        return new ShuffledHashJoinExec(j.leftKeys(), j.rightKeys(), jt, j.schema(),
                plan(j.left()), plan(j.right()));
    }

    private PhysicalPlan broadcast(com.minispark.sql.plan.Join j, boolean buildIsLeft) {
        return new BroadcastHashJoinExec(j.leftKeys(), j.rightKeys(), j.joinType(), j.schema(),
                plan(j.left()), plan(j.right()), buildIsLeft, sc);
    }

    /** True if {@code p} is wrapped in a BroadcastHint (possibly under other passthroughs). */
    private static boolean hasBroadcastHint(LogicalPlan p) {
        return p instanceof BroadcastHint;
    }

    /**
     * Row count of {@code p} if we can read it statically off a LocalRelation
     * (possibly wrapped in a BroadcastHint), else {@code null} — meaning "we
     * don't know the size, don't auto-broadcast." Conservative on purpose:
     * silently broadcasting an arbitrary RDD-backed side would require
     * .count() at plan time, which could be a full scan.
     */
    private static Integer staticRowCount(LogicalPlan p) {
        LogicalPlan inner = (p instanceof BroadcastHint h) ? h.child() : p;
        if (inner instanceof LocalRelation r) return r.rows().size();
        return null;
    }
}
