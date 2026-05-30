package com.minispark.sql.plan;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A row filter: {@code WHERE condition}. Pass-through schema (same columns as
 * the child); only keeps rows where {@code condition} evaluates to true.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Filter.
 */
public final class Filter implements LogicalPlan {

    private final Expression condition;
    private final LogicalPlan child;

    public Filter(Expression condition, LogicalPlan child) {
        this.condition = condition;
        this.child = child;
    }

    public Expression condition() { return condition; }
    public LogicalPlan child() { return child; }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) {
        return new Filter(condition, c.get(0));
    }
    @Override public boolean resolved() { return child.resolved() && condition.resolved(); }
    @Override public String toString() { return "Filter [" + condition + "]"; }
}
