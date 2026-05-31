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

    private final Catalog catalog;

    public Analyzer() { this(new Catalog()); }
    public Analyzer(Catalog catalog) { this.catalog = catalog; }

    public Catalog catalog() { return catalog; }

    public LogicalPlan analyze(LogicalPlan plan) {
        // Resolve a FROM-table reference against the catalog before anything else,
        // so the substituted relation's schema is available to parents.
        if (plan instanceof com.minispark.sql.plan.UnresolvedRelation u) {
            return analyze(catalog.lookup(u.tableName()));
        }
        // Resolve children first.
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan c : plan.children()) newChildren.add(analyze(c));
        LogicalPlan withResolvedChildren =
                plan.children().isEmpty() ? plan : plan.withChildren(newChildren);

        if (withResolvedChildren instanceof Project p) {
            StructType in = p.child().schema();
            List<Expression> bound = new ArrayList<>();
            for (Expression e : p.projectList()) {
                // Expand SELECT * into one bound reference per input column.
                if (e instanceof com.minispark.sql.expr.Star) {
                    for (int i = 0; i < in.size(); i++) {
                        bound.add(new BoundReference(i, in.type(i), in.name(i)));
                    }
                } else {
                    bound.add(bind(e, in));
                }
            }
            return new Project(bound, p.child());
        }
        if (withResolvedChildren instanceof Filter f) {
            StructType in = f.child().schema();
            return new Filter(bind(f.condition(), in), f.child());
        }
        if (withResolvedChildren instanceof com.minispark.sql.plan.Sort s) {
            StructType in = s.child().schema();
            List<com.minispark.sql.plan.SortOrder> bound = new ArrayList<>();
            for (var o : s.orders()) {
                bound.add(new com.minispark.sql.plan.SortOrder(bind(o.expr(), in), o.ascending()));
            }
            return new com.minispark.sql.plan.Sort(bound, s.child());
        }
        // Limit/Distinct have no expressions to bind; their analyzed child is set above.
        if (withResolvedChildren instanceof com.minispark.sql.plan.Limit
                || withResolvedChildren instanceof com.minispark.sql.plan.Distinct) {
            return withResolvedChildren;
        }
        if (withResolvedChildren instanceof com.minispark.sql.plan.Join j) {
            StructType l = j.left().schema(), r = j.right().schema();
            List<Expression> lk = new ArrayList<>();
            for (Expression e : j.leftKeys()) lk.add(bind(e, l));
            List<Expression> rk = new ArrayList<>();
            for (Expression e : j.rightKeys()) rk.add(bind(e, r));
            // A non-equi condition spans both sides: bind it against the combined
            // left++right layout the executor evaluates it on.
            Expression cond = j.condition();
            if (cond != null) {
                cond = bind(cond, com.minispark.sql.plan.Join.combinedInputSchema(j.left(), j.right()));
            }
            return new com.minispark.sql.plan.Join(j.left(), j.right(), lk, rk, j.joinType(), cond);
        }
        if (withResolvedChildren instanceof com.minispark.sql.plan.Aggregate agg) {
            StructType in = agg.child().schema();
            List<Expression> boundGroups = new ArrayList<>();
            for (Expression g : agg.groupingExprs()) boundGroups.add(bind(g, in));
            List<com.minispark.sql.expr.agg.AggregateFunction> boundAggs = new ArrayList<>();
            for (var a : agg.aggregates()) boundAggs.add(a.bind(in, this::bind));
            return new com.minispark.sql.plan.Aggregate(boundGroups, boundAggs, agg.child());
        }
        if (withResolvedChildren instanceof com.minispark.sql.plan.Window win) {
            StructType in = win.child().schema();
            List<com.minispark.sql.expr.window.WindowExpression> boundExprs = new ArrayList<>();
            for (var w : win.windowExprs()) {
                var spec = w.spec();
                List<Expression> boundPart = new ArrayList<>();
                for (Expression p : spec.partitionBy()) boundPart.add(bind(p, in));
                List<com.minispark.sql.plan.SortOrder> boundOrder = new ArrayList<>();
                for (var o : spec.orderBy()) {
                    boundOrder.add(new com.minispark.sql.plan.SortOrder(bind(o.expr(), in), o.ascending()));
                }
                boundExprs.add(new com.minispark.sql.expr.window.WindowExpression(
                        w.function(), new com.minispark.sql.expr.window.WindowSpec(boundPart, boundOrder)));
            }
            return new com.minispark.sql.plan.Window(boundExprs, win.outputNames(), win.child());
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
