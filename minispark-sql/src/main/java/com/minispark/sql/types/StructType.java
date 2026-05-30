package com.minispark.sql.types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A row schema: an ordered list of {@link StructField}s. Every {@link
 * com.minispark.sql.Row} flowing through a plan is described by one of these,
 * and every {@link com.minispark.sql.expr.Expression} can report the schema it
 * produces — so the planner type-checks and resolves column names before
 * anything runs.
 *
 * Real Spark equivalent: org.apache.spark.sql.types.StructType.
 */
public final class StructType implements Serializable {

    private final List<StructField> fields;

    public StructType(List<StructField> fields) {
        this.fields = List.copyOf(fields);
    }

    public static StructType of(StructField... fields) {
        return new StructType(List.of(fields));
    }

    public List<StructField> fields() { return fields; }
    public int size() { return fields.size(); }
    public StructField field(int i) { return fields.get(i); }
    public String name(int i) { return fields.get(i).name(); }
    public DataType type(int i) { return fields.get(i).dataType(); }

    /** Column index by name, or -1 if absent. */
    public int indexOf(String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) return i;
        }
        return -1;
    }

    public boolean contains(String name) { return indexOf(name) >= 0; }

    public List<String> names() {
        List<String> out = new ArrayList<>(fields.size());
        for (StructField f : fields) out.add(f.name());
        return out;
    }

    /** A new schema with {@code field} appended. */
    public StructType add(StructField field) {
        List<StructField> copy = new ArrayList<>(fields);
        copy.add(field);
        return new StructType(copy);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("struct<");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields.get(i).name()).append(":").append(fields.get(i).dataType());
        }
        return sb.append(">").toString();
    }
}
