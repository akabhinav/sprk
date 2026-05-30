package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * The {@code *} in {@code SELECT *}: a placeholder the analyzer expands into one
 * {@link UnresolvedAttribute} per column of the projection's input schema. It is
 * never evaluated (it's gone by the time the plan runs) and is always
 * unresolved until expanded.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.analysis.UnresolvedStar.
 */
public final class Star implements Expression {

    @Override public DataType dataType(StructType s) {
        throw new IllegalStateException("'*' must be expanded by the analyzer");
    }
    @Override public Object eval(Row input) {
        throw new IllegalStateException("'*' is not evaluable");
    }
    @Override public List<Expression> children() { return List.of(); }
    @Override public Expression withChildren(List<Expression> c) { return this; }
    @Override public boolean resolved() { return false; }
    @Override public String name() { return "*"; }
    @Override public String toString() { return "*"; }
}
