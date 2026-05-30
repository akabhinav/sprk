package com.minispark.sql;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * A single row of a DataFrame: an ordered, positional tuple of values described
 * by a {@link com.minispark.sql.types.StructType}. Values are boxed {@code
 * Object}s (Integer/Long/Double/String/Boolean or null).
 *
 * <p>Rows are serializable because they cross the RDD/shuffle seam exactly like
 * any other record in the engine.
 *
 * Real Spark equivalent: org.apache.spark.sql.Row (GenericRow).
 */
public final class Row implements Serializable {

    private final Object[] values;

    public Row(Object[] values) { this.values = values; }

    public static Row of(Object... values) { return new Row(values); }

    public int size() { return values.length; }
    public Object get(int i) { return values[i]; }
    public boolean isNullAt(int i) { return values[i] == null; }

    public int getInt(int i) { return (Integer) values[i]; }
    public long getLong(int i) { return ((Number) values[i]).longValue(); }
    public double getDouble(int i) { return ((Number) values[i]).doubleValue(); }
    public String getString(int i) { return (String) values[i]; }
    public boolean getBoolean(int i) { return (Boolean) values[i]; }

    /** Defensive copy of the backing array. */
    public Object[] toArray() { return values.clone(); }
    public List<Object> toList() { return Arrays.asList(values); }

    @Override public String toString() { return "[" + String.join(", ", names()) + "]"; }

    private List<String> names() {
        java.util.ArrayList<String> out = new java.util.ArrayList<>(values.length);
        for (Object v : values) out.add(String.valueOf(v));
        return out;
    }

    @Override public boolean equals(Object o) {
        return o instanceof Row r && Arrays.equals(values, r.values);
    }
    @Override public int hashCode() { return Arrays.hashCode(values); }
}
