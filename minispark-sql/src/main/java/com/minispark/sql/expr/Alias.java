package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Names a child expression: {@code col("a") + 1 AS "incremented"}. Transparent
 * at eval time (delegates to the child); only its {@link #name} matters, for
 * the output schema of a projection.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.Alias.
 */
public final class Alias implements Expression {

    private final Expression child;
    private final String alias;

    public Alias(Expression child, String alias) {
        this.child = child;
        this.alias = alias;
    }

    public Expression child() { return child; }

    @Override public DataType dataType(StructType s) { return child.dataType(s); }
    @Override public Object eval(Row input) { return child.eval(input); }
    @Override public List<Expression> children() { return List.of(child); }
    @Override public Expression withChildren(List<Expression> c) { return new Alias(c.get(0), alias); }
    @Override public String name() { return alias; }
    @Override public String toString() { return child + " AS " + alias; }
}
