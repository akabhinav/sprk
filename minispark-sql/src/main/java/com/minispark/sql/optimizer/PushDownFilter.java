package com.minispark.sql.optimizer;

import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * Predicate pushdown: rewrite {@code Filter(cond, Project(p, child))} into
 * {@code Project(p, Filter(cond, child))}, moving the filter below the
 * projection. Filtering before projecting means fewer rows flow through the
 * (potentially expensive) projection — the canonical optimizer win, and safe
 * here because our projections don't drop columns the predicate needs (the
 * predicate was resolved against the child's schema, which Project preserves
 * access to via its own child).
 *
 * <p>This simplified rule only pushes through a Project whose projection list
 * is "safe" (doesn't rename away a column the filter references); for the demo
 * grammar we conservatively push only when the predicate references columns
 * that still exist in the child schema.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.optimizer.PushDownPredicates.
 */
public final class PushDownFilter implements Rule {

    @Override public String name() { return "PushDownFilter"; }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        // Recurse first (bottom-up), then try to rewrite this node.
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan c : plan.children()) newChildren.add(apply(c));
        LogicalPlan node = plan.children().isEmpty() ? plan : plan.withChildren(newChildren);

        if (node instanceof Filter f && f.child() instanceof Project p) {
            // Only push when the predicate's columns are available in the
            // projection's input (child) schema — true for plain column refs.
            if (predicateColumnsAvailable(f, p)) {
                Filter pushed = new Filter(f.condition(), p.child());
                return new Project(p.projectList(), pushed);
            }
        }
        return node;
    }

    private boolean predicateColumnsAvailable(Filter f, Project p) {
        // The filter was resolved against Project's output schema. If every
        // column it needs also exists by name in Project's *input* schema, the
        // push is safe. (Aliases that rename would fail this check.)
        var inNames = p.child().schema().names();
        for (String col : ExpressionColumns.referenced(f.condition())) {
            if (!inNames.contains(col)) return false;
        }
        return true;
    }
}
