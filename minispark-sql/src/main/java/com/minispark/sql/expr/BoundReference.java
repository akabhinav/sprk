package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A resolved column reference: a fixed ordinal into the input row plus its
 * type and name. Produced by the analyzer from an {@link UnresolvedAttribute};
 * {@link #eval} is a direct array access with no name lookup.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.BoundReference.
 */
public final class BoundReference implements Expression {

    private final int ordinal;
    private final DataType dataType;
    private final String name;

    public BoundReference(int ordinal, DataType dataType, String name) {
        this.ordinal = ordinal;
        this.dataType = dataType;
        this.name = name;
    }

    public int ordinal() { return ordinal; }

    @Override public DataType dataType(StructType inputSchema) { return dataType; }
    @Override public Object eval(Row input) { return input.get(ordinal); }
    @Override public List<Expression> children() { return List.of(); }
    @Override public Expression withChildren(List<Expression> c) { return this; }
    @Override public boolean resolved() { return true; }
    @Override public String name() { return name; }
    @Override public String toString() { return name + "#" + ordinal; }
}
