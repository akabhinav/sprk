package com.minispark.sql.types;

import java.io.Serializable;

/**
 * One named, typed column in a {@link StructType} schema.
 *
 * Real Spark equivalent: org.apache.spark.sql.types.StructField.
 */
public record StructField(String name, DataType dataType, boolean nullable) implements Serializable {
    public static StructField of(String name, DataType dataType) {
        return new StructField(name, dataType, true);
    }
}
