package com.minispark.sql.optimizer;

import com.minispark.sql.expr.BoundReference;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Predicate pushdown: rewrite {@code Filter(cond, Project(p, child))} into
 * {@code Project(p, Filter(cond', child))}, moving the filter below the
 * projection so fewer rows flow through it.
 *
 * <p><b>The subtlety</b>: the analyzer ran before us, so the filter's column
 * references are {@link BoundReference}s — ordinals into the Project's <i>output</i>
 * schema. After we push it below the Project those ordinals would index the
 * <i>child's</i> schema, which (if Project reorders or subsets columns) means
 * silently reading the wrong column. So we rebind each ordinal by name against
 * the child schema as we push, and we refuse to push when any required column
 * is missing downstream (e.g. an aliased / computed projection that doesn't
 * carry the source column).
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
            StructType childSchema = p.child().schema();
            // Try to rebind every BoundReference in the predicate against the
            // child schema. If any column required by the predicate isn't in the
            // child by name, the push isn't safe — keep the filter on top.
            Expression rebound = rebindAgainst(f.condition(), childSchema);
            if (rebound != null) {
                Filter pushed = new Filter(rebound, p.child());
                return new Project(p.projectList(), pushed);
            }
        }
        return node;
    }

    /**
     * Walk the expression tree replacing every {@link BoundReference}'s ordinal
     * with the ordinal of the same-named column in {@code targetSchema}.
     * Returns {@code null} if any reference's column is absent from the target,
     * signalling "do not push".
     */
    private Expression rebindAgainst(Expression e, StructType targetSchema) {
        if (e instanceof BoundReference br) {
            int idx = targetSchema.indexOf(br.name());
            if (idx < 0) return null;
            return new BoundReference(idx, targetSchema.type(idx), br.name());
        }
        List<Expression> children = e.children();
        if (children.isEmpty()) return e;
        List<Expression> rebound = new ArrayList<>(children.size());
        for (Expression c : children) {
            Expression r = rebindAgainst(c, targetSchema);
            if (r == null) return null;
            rebound.add(r);
        }
        return e.withChildren(rebound);
    }
}
