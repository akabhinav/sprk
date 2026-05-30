package com.minispark.sql;

import com.minispark.rdd.MapPartitionsRDD;
import com.minispark.rdd.RDD;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Entry point for writing a DataFrame to files: {@code df.write().option(...)
 * .csv(path)} / {@code .json(path)}. Compiles + executes the plan to an
 * {@code RDD<Row>}, formats each row to a line, and saves one {@code part-NNNNN}
 * file per partition via the engine's {@code saveAsTextFile} — so the write is
 * itself a distributed job.
 *
 * Real Spark equivalent: org.apache.spark.sql.DataFrameWriter.
 */
public final class DataFrameWriter {

    private final DataFrame df;
    private boolean header = false;
    private char delimiter = ',';

    DataFrameWriter(DataFrame df) { this.df = df; }

    public DataFrameWriter option(String key, String value) {
        switch (key.toLowerCase()) {
            case "header" -> header = Boolean.parseBoolean(value);
            case "delimiter", "sep" -> delimiter = value.charAt(0);
            default -> throw new IllegalArgumentException("unknown write option: " + key);
        }
        return this;
    }
    public DataFrameWriter option(String key, boolean value) { return option(key, String.valueOf(value)); }

    public void csv(String path) {
        StructType schema = df.schema();
        RDD<Row> rows = df.compile().execute();
        char delim = delimiter;

        RDD<String> lines = rows.map((RDD.SerializableFunction<Row, String>) row -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) sb.append(delim);
                sb.append(csvCell(row.get(i), delim));
            }
            return sb.toString();
        });

        if (header) {
            // Prepend the header as the first record of each partition file, so
            // every part-NNNNN is a self-describing CSV (Spark does the same).
            String headerLine = String.join(String.valueOf(delim), schema.names());
            lines = new MapPartitionsRDD<>(rows.context(), lines, (ctx, part, it) -> {
                List<String> out = new ArrayList<>();
                out.add(headerLine);
                it.forEachRemaining(out::add);
                return out.iterator();
            });
        }
        lines.saveAsTextFile(path);
    }

    public void json(String path) {
        StructType schema = df.schema();
        RDD<Row> rows = df.compile().execute();
        String[] names = schema.names().toArray(new String[0]);
        DataType[] types = schema.fields().stream()
                .map(com.minispark.sql.types.StructField::dataType).toArray(DataType[]::new);

        rows.map((RDD.SerializableFunction<Row, String>) row -> {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (int i = 0; i < row.size(); i++) {
                if (row.isNullAt(i)) continue; // omit nulls, like Spark's default
                if (!first) sb.append(",");
                first = false;
                sb.append('"').append(names[i]).append("\":").append(jsonValue(row.get(i), types[i]));
            }
            return sb.append("}").toString();
        }).saveAsTextFile(path);
    }

    // ----- formatting helpers (static so they're trivially serializable) -----

    /** Quote a CSV cell only if it contains the delimiter, a quote, or a newline. */
    private static String csvCell(Object v, char delim) {
        if (v == null) return "";
        String s = String.valueOf(v);
        if (s.indexOf(delim) >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0) {
            return '"' + s.replace("\"", "\"\"") + '"';
        }
        return s;
    }

    private static String jsonValue(Object v, DataType type) {
        if (v == null) return "null";
        return switch (type) {
            case STRING -> '"' + String.valueOf(v).replace("\"", "\\\"") + '"';
            default -> String.valueOf(v); // numbers/booleans unquoted
        };
    }
}
