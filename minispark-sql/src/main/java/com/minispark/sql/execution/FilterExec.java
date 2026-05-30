package com.minispark.sql.execution;

import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Physical filter: keeps rows where the (BOOLEAN) condition evaluates true.
 * Runs as an RDD {@code filter} — narrow, no shuffle. Null/false both drop the
 * row (SQL three-valued logic: only true passes a WHERE).
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.FilterExec.
 */
public final class FilterExec implements PhysicalPlan {

    private final Expression condition;
    private final PhysicalPlan child;

    public FilterExec(Expression condition, PhysicalPlan child) {
        this.condition = condition;
        this.child = child;
    }

    @Override public StructType schema() { return child.schema(); }

    @Override
    public RDD<Row> execute() {
        Expression cond = condition;
        return child.execute().filter((RDD.SerializablePredicate<Row>)
                row -> Boolean.TRUE.equals(cond.eval(row)));
    }

    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "FilterExec [" + condition + "]"; }
}
