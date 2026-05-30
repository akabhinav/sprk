package com.minispark.sql.plan;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.agg.AggregateFunction;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code GROUP BY groupingExprs} producing one row per distinct key with the
 * grouping columns followed by the aggregate results. Output schema = grouping
 * fields (named by their expression) ++ one field per aggregate (named e.g.
 * {@code sum(age)}).
 *
 * <p>Note the aggregates' child expressions are resolved separately by the
 * analyzer (they aren't ordinary {@code children()} of this plan node, so the
 * analyzer special-cases Aggregate).
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Aggregate.
 */
public final class Aggregate implements LogicalPlan {

    private final List<Expression> groupingExprs;
    private final List<AggregateFunction> aggregates;
    private final LogicalPlan child;

    public Aggregate(List<Expression> groupingExprs, List<AggregateFunction> aggregates,
                     LogicalPlan child) {
        this.groupingExprs = List.copyOf(groupingExprs);
        this.aggregates = List.copyOf(aggregates);
        this.child = child;
    }

    public List<Expression> groupingExprs() { return groupingExprs; }
    public List<AggregateFunction> aggregates() { return aggregates; }
    public LogicalPlan child() { return child; }

    @Override
    public StructType schema() {
        StructType in = child.schema();
        List<StructField> fields = new ArrayList<>();
        for (Expression g : groupingExprs) fields.add(StructField.of(g.name(), g.dataType(in)));
        for (AggregateFunction a : aggregates) fields.add(StructField.of(a.name(), a.resultType(in)));
        return new StructType(fields);
    }

    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) {
        return new Aggregate(groupingExprs, aggregates, c.get(0));
    }

    @Override public boolean resolved() {
        if (!child.resolved()) return false;
        for (Expression g : groupingExprs) if (!g.resolved()) return false;
        return true; // aggregate child expressions are resolved at build/analyze time
    }

    @Override public String toString() {
        List<String> g = new ArrayList<>(); for (Expression e : groupingExprs) g.add(e.toString());
        List<String> a = new ArrayList<>(); for (AggregateFunction f : aggregates) a.add(f.name());
        return "Aggregate [groupBy=" + g + ", agg=" + a + "]";
    }
}
