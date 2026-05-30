package com.minispark.sql.analysis;

import com.minispark.sql.expr.Alias;
import com.minispark.sql.expr.BoundReference;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.UnresolvedAttribute;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Resolves a freshly-built logical plan: rewrites every {@link
 * UnresolvedAttribute} (a column name) into a {@link BoundReference} (an
 * ordinal + type) against the schema of the node's input. After analysis the
 * plan is {@code resolved()} and can be optimized and executed.
 *
 * <p>Bottom-up: children are resolved first so each node's input schema is
 * itself resolved before we bind references against it.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.analysis.Analyzer
 *                        (the ResolveReferences rule, specifically).
 */
public final class Analyzer {

    public LogicalPlan analyze(LogicalPlan plan) {
        // Resolve children first.
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan c : plan.children()) newChildren.add(analyze(c));
        LogicalPlan withResolvedChildren =
                plan.children().isEmpty() ? plan : plan.withChildren(newChildren);

        if (withResolvedChildren instanceof Project p) {
            StructType in = p.child().schema();
            List<Expression> bound = new ArrayList<>();
            for (Expression e : p.projectList()) bound.add(bind(e, in));
            return new Project(bound, p.child());
        }
        if (withResolvedChildren instanceof Filter f) {
            StructType in = f.child().schema();
            return new Filter(bind(f.condition(), in), f.child());
        }
        return withResolvedChildren;
    }

    /** Recursively replace UnresolvedAttribute leaves with BoundReference. */
    private Expression bind(Expression e, StructType schema) {
        if (e instanceof UnresolvedAttribute u) {
            int ord = schema.indexOf(u.columnName());
            if (ord < 0) throw new AnalysisException(
                    "cannot resolve column '" + u.columnName() + "' among " + schema.names());
            return new BoundReference(ord, schema.type(ord), u.columnName());
        }
        // Preserve Alias names while binding the aliased child.
        if (e instanceof Alias a) {
            return new Alias(bind(a.child(), schema), a.name());
        }
        List<Expression> children = e.children();
        if (children.isEmpty()) return e;
        List<Expression> newChildren = new ArrayList<>(children.size());
        for (Expression c : children) newChildren.add(bind(c, schema));
        return e.withChildren(newChildren);
    }

    /** Thrown when a column name can't be resolved against the input schema. */
    public static final class AnalysisException extends RuntimeException {
        public AnalysisException(String message) { super(message); }
    }
}
