package com.minispark.sql.types;

import java.io.Serializable;

/**
 * The column types MiniSQL understands. Deliberately a small closed set — just
 * enough to express the example queries — rather than Spark's full type lattice
 * (decimals, timestamps, arrays, maps, structs, UDTs).
 *
 * Real Spark equivalent: org.apache.spark.sql.types.DataType (subclasses).
 */
public enum DataType implements Serializable {
    INT, LONG, DOUBLE, STRING, BOOLEAN;

    /** Whether {@code value} is a legal (nullable) instance of this type. */
    public boolean accepts(Object value) {
        if (value == null) return true;
        return switch (this) {
            case INT -> value instanceof Integer;
            case LONG -> value instanceof Long;
            case DOUBLE -> value instanceof Double;
            case STRING -> value instanceof String;
            case BOOLEAN -> value instanceof Boolean;
        };
    }
}
