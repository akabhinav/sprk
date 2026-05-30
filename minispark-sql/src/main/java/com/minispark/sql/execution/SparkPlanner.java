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
        throw new UnsupportedOperationException("No physical strategy for " + logical);
    }
}
