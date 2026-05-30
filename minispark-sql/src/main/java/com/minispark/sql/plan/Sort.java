package com.minispark.sql.plan;

import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * {@code ORDER BY orders}. A global sort: pass-through schema, total ordering
 * across all partitions (the physical node range-partitions then sorts).
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Sort.
 */
public final class Sort implements LogicalPlan {

    private final List<SortOrder> orders;
    private final LogicalPlan child;

    public Sort(List<SortOrder> orders, LogicalPlan child) {
        this.orders = List.copyOf(orders);
        this.child = child;
    }

    public List<SortOrder> orders() { return orders; }
    public LogicalPlan child() { return child; }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) { return new Sort(orders, c.get(0)); }
    @Override public boolean resolved() {
        if (!child.resolved()) return false;
        for (SortOrder o : orders) if (!o.expr().resolved()) return false;
        return true;
    }
    @Override public String toString() { return "Sort " + orders; }
}
