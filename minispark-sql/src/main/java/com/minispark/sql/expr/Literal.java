package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A constant value. Carries its own {@link DataType} so the analyzer doesn't
 * have to infer it.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.Literal.
 */
public final class Literal implements Expression {

    private final Object value;
    private final DataType dataType;

    public Literal(Object value, DataType dataType) {
        this.value = value;
        this.dataType = dataType;
    }

    public static Literal of(int v)     { return new Literal(v, DataType.INT); }
    public static Literal of(long v)    { return new Literal(v, DataType.LONG); }
    public static Literal of(double v)  { return new Literal(v, DataType.DOUBLE); }
    public static Literal of(String v)  { return new Literal(v, DataType.STRING); }
    public static Literal of(boolean v) { return new Literal(v, DataType.BOOLEAN); }

    public Object value() { return value; }

    @Override public DataType dataType(StructType inputSchema) { return dataType; }
    @Override public Object eval(Row input) { return value; }
    @Override public List<Expression> children() { return List.of(); }
    @Override public Expression withChildren(List<Expression> c) { return this; }
    @Override public boolean resolved() { return true; }
    @Override public String name() { return String.valueOf(value); }
    @Override public String toString() {
        return dataType == DataType.STRING ? "'" + value + "'" : String.valueOf(value);
    }
}
