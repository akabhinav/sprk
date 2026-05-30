package com.minispark.sql.plan;

import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * {@code LIMIT n}. Pass-through schema; keeps at most {@code n} rows.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit,LocalLimit}.
 */
public final class Limit implements LogicalPlan {

    private final int limit;
    private final LogicalPlan child;

    public Limit(int limit, LogicalPlan child) {
        this.limit = limit;
        this.child = child;
    }

    public int limit() { return limit; }
    public LogicalPlan child() { return child; }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) { return new Limit(limit, c.get(0)); }
    @Override public boolean resolved() { return child.resolved(); }
    @Override public String toString() { return "Limit " + limit; }
}
