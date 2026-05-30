package com.minispark.sql.optimizer;

import com.minispark.sql.expr.BooleanOp;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Collapse adjacent filters: {@code Filter(a, Filter(b, child))} →
 * {@code Filter(a AND b, child)}. One fewer RDD pass at runtime, and it lets a
 * later pushdown move the combined predicate as a unit.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.optimizer.CombineFilters.
 */
public final class CombineFilters implements Rule {

    @Override public String name() { return "CombineFilters"; }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan c : plan.children()) newChildren.add(apply(c));
        LogicalPlan node = plan.children().isEmpty() ? plan : plan.withChildren(newChildren);

        if (node instanceof Filter outer && outer.child() instanceof Filter inner) {
            return new Filter(BooleanOp.and(outer.condition(), inner.condition()), inner.child());
        }
        return node;
    }
}
