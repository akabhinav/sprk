package com.minispark.sql;

import com.minispark.rdd.RDD;
import com.minispark.sql.plan.LogicalRDD;
import com.minispark.sql.sources.CsvParsing;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for reading external data into a DataFrame:
 * {@code spark.read().option(...).csv(path)} / {@code .json(path)}. Built on
 * the engine's {@code textFile}, so the read is a distributed job; only schema
 * inference samples a few rows on the driver.
 *
 * Real Spark equivalent: org.apache.spark.sql.DataFrameReader.
 */
public final class DataFrameReader {

    private final MiniSparkSession session;
    private boolean header = false;
    private boolean inferSchema = false;
    private char delimiter = ',';
    private StructType userSchema = null;

    DataFrameReader(MiniSparkSession session) { this.session = session; }

    public DataFrameReader option(String key, String value) {
        switch (key.toLowerCase()) {
            case "header" -> header = Boolean.parseBoolean(value);
            case "inferschema" -> inferSchema = Boolean.parseBoolean(value);
            case "delimiter", "sep" -> delimiter = value.charAt(0);
            default -> throw new IllegalArgumentException("unknown read option: " + key);
        }
        return this;
    }

    public DataFrameReader option(String key, boolean value) { return option(key, String.valueOf(value)); }

    /** Provide an explicit schema (skips inference). */
    public DataFrameReader schema(StructType schema) { this.userSchema = schema; return this; }

    // ----- CSV -----

    public DataFrame csv(String path) {
        var sc = session.sparkContext();
        int parts = sc.defaultParallelism();
        CsvParsing parsing = new CsvParsing(delimiter);

        // Determine the schema (and the header line text, if any) on the driver.
        List<String> sample = sampleLines(path, 100);
        if (sample.isEmpty()) {
            StructType empty = userSchema != null ? userSchema : new StructType(List.of());
            return emptyDataFrame(empty);
        }
        String headerLine = header ? sample.get(0) : null;
        // Drop ALL occurrences of the header from the inference sample, not just
        // the first line: a directory of part-files repeats the header in each.
        List<String> dataSample = new ArrayList<>();
        for (String l : sample) {
            if (header && l.equals(headerLine)) continue;
            dataSample.add(l);
        }
        StructType schema = resolveCsvSchema(parsing, headerLine, dataSample);

        // Build the RDD lazily: read lines, drop the header, parse each to a Row.
        boolean hasHeader = header;
        String headerForFilter = headerLine;
        java.util.function.Supplier<RDD<Row>> supplier = () -> {
            RDD<String> lines = sc.textFile(path, parts);
            RDD<String> body = hasHeader
                    ? lines.filter((RDD.SerializablePredicate<String>) l -> !l.equals(headerForFilter))
                    : lines;
            DataType[] types = schema.fields().stream().map(StructField::dataType).toArray(DataType[]::new);
            return body.filter((RDD.SerializablePredicate<String>) l -> !l.isBlank())
                    .map((RDD.SerializableFunction<String, Row>) line -> {
                        List<String> cells = parsing.split(line);
                        Object[] vals = new Object[types.length];
                        for (int i = 0; i < types.length; i++) {
                            vals[i] = i < cells.size() ? CsvParsing.coerce(cells.get(i), types[i]) : null;
                        }
                        return new Row(vals);
                    });
        };
        return new DataFrame(session, new LogicalRDD(schema, supplier, "csv " + path));
    }

    private StructType resolveCsvSchema(CsvParsing parsing, String headerLine, List<String> dataSample) {
        if (userSchema != null) return userSchema;

        // Column names: from the header, else c0, c1, ...
        int ncols;
        List<String> names = new ArrayList<>();
        if (headerLine != null) {
            names.addAll(parsing.split(headerLine));
            ncols = names.size();
        } else {
            ncols = dataSample.isEmpty() ? 0 : parsing.split(dataSample.get(0)).size();
            for (int i = 0; i < ncols; i++) names.add("c" + i);
        }

        // Types: inferred from the sample if requested, else all STRING.
        DataType[] types = new DataType[ncols];
        if (inferSchema) {
            for (String line : dataSample) {
                if (line.isBlank()) continue;
                List<String> cells = parsing.split(line);
                for (int i = 0; i < ncols && i < cells.size(); i++) {
                    types[i] = CsvParsing.widen(types[i], CsvParsing.inferType(cells.get(i)));
                }
            }
        }
        List<StructField> fields = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            DataType t = types[i] != null ? types[i] : DataType.STRING;
            fields.add(StructField.of(names.get(i), t));
        }
        return new StructType(fields);
    }

    // ----- JSON (flat objects of primitive values) -----

    public DataFrame json(String path) {
        var sc = session.sparkContext();
        int parts = sc.defaultParallelism();

        List<String> sample = sampleLines(path, 100);
        StructType schema = userSchema != null ? userSchema
                : com.minispark.sql.sources.JsonParsing.inferSchema(sample);

        StructType s = schema;
        java.util.function.Supplier<RDD<Row>> supplier = () ->
                sc.textFile(path, parts)
                        .filter((RDD.SerializablePredicate<String>) l -> !l.isBlank())
                        .map((RDD.SerializableFunction<String, Row>) line ->
                                com.minispark.sql.sources.JsonParsing.parseLine(line, s));
        return new DataFrame(session, new LogicalRDD(schema, supplier, "json " + path));
    }

    // ----- helpers -----

    /**
     * Read up to {@code n} non-empty lines on the driver for schema inference.
     * Accepts a single file or a directory of part-files (a written output dir),
     * matching what the distributed read will see.
     */
    private List<String> sampleLines(String path, int n) {
        try {
            java.nio.file.Path p = java.nio.file.Path.of(path);
            List<java.nio.file.Path> files;
            if (java.nio.file.Files.isDirectory(p)) {
                try (var st = java.nio.file.Files.list(p)) {
                    files = st.filter(java.nio.file.Files::isRegularFile)
                            .filter(f -> { String name = f.getFileName().toString();
                                           return !name.startsWith(".") && !name.startsWith("_"); })
                            .sorted().toList();
                }
            } else {
                files = List.of(p);
            }
            List<String> out = new ArrayList<>();
            for (java.nio.file.Path f : files) {
                for (String l : java.nio.file.Files.readAllLines(f)) {
                    if (out.size() >= n) return out;
                    if (!l.isBlank()) out.add(l);
                }
            }
            return out;
        } catch (java.io.IOException e) {
            throw new RuntimeException("cannot read " + path, e);
        }
    }

    private DataFrame emptyDataFrame(StructType schema) {
        return session.createDataFrame(List.of(), schema);
    }
}
