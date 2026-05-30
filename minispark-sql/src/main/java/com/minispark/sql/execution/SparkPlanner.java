package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LocalRelation;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;

/**
 * Turns an optimized logical plan into a {@link PhysicalPlan} by matching each
 * logical operator to its physical strategy. One-to-one for now (Project →
 * ProjectExec, etc.); real Spark's planner picks among multiple strategies
 * (e.g. broadcast vs sort-merge join) by cost.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.SparkPlanner / SparkStrategies.
 */
public final class SparkPlanner {

    private final MiniSparkContext sc;
    private final int numPartitions;

    public SparkPlanner(MiniSparkContext sc, int numPartitions) {
        this.sc = sc;
        this.numPartitions = numPartitions;
    }

    public PhysicalPlan plan(LogicalPlan logical) {
        if (logical instanceof LocalRelation r) {
            return new LocalTableScanExec(r.schema(), r.rows(), sc, numPartitions);
        }
        if (logical instanceof Project p) {
            return new ProjectExec(p.projectList(), p.schema(), plan(p.child()));
        }
        if (logical instanceof Filter f) {
            return new FilterExec(f.condition(), plan(f.child()));
        }
        if (logical instanceof com.minispark.sql.plan.Aggregate a) {
            return new HashAggregateExec(a.groupingExprs(), a.aggregates(), a.schema(), plan(a.child()));
        }
        if (logical instanceof com.minispark.sql.plan.Join j) {
            return new ShuffledHashJoinExec(j.leftKeys(), j.rightKeys(), j.joinType(), j.schema(),
                    plan(j.left()), plan(j.right()));
        }
        if (logical instanceof com.minispark.sql.plan.Sort s) {
            return new SortExec(s.orders(), plan(s.child()));
        }
        if (logical instanceof com.minispark.sql.plan.Limit l) {
            return new LimitExec(l.limit(), plan(l.child()), sc);
        }
        throw new UnsupportedOperationException("No physical strategy for " + logical);
    }
}
