package com.minispark.sql.plan;

import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * {@code SELECT DISTINCT}: deduplicate whole rows. Pass-through schema.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Distinct
 *                        (which is Aggregate with all columns as grouping keys).
 */
public final class Distinct implements LogicalPlan {

    private final LogicalPlan child;

    public Distinct(LogicalPlan child) { this.child = child; }

    public LogicalPlan child() { return child; }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) { return new Distinct(c.get(0)); }
    @Override public boolean resolved() { return child.resolved(); }
    @Override public String toString() { return "Distinct"; }
}
