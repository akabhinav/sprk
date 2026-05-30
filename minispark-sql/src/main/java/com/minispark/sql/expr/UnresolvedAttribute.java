package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A column referenced by name before analysis has bound it to a position. The
 * analyzer rewrites every {@code UnresolvedAttribute} into a {@link
 * BoundReference} (ordinal + type) against the child plan's schema; until then
 * the expression is "unresolved" and cannot be evaluated.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute.
 */
public final class UnresolvedAttribute implements Expression {

    private final String columnName;

    public UnresolvedAttribute(String columnName) { this.columnName = columnName; }

    public String columnName() { return columnName; }

    @Override public DataType dataType(StructType inputSchema) {
        int i = inputSchema.indexOf(columnName);
        if (i < 0) throw new IllegalStateException("Cannot resolve column '" + columnName + "'");
        return inputSchema.type(i);
    }

    @Override public Object eval(Row input) {
        throw new IllegalStateException("Unresolved attribute '" + columnName + "' cannot be evaluated");
    }

    @Override public List<Expression> children() { return List.of(); }
    @Override public Expression withChildren(List<Expression> c) { return this; }
    @Override public boolean resolved() { return false; }
    @Override public String name() { return columnName; }
    @Override public String toString() { return "'" + columnName; }
}
