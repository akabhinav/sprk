package com.minispark.sql.plan;

import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A leaf plan over an in-memory list of rows with a known schema — the SQL
 * analogue of {@code sc.parallelize}. The starting point for
 * {@code session.createDataFrame(rows, schema)}.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.LocalRelation.
 */
public final class LocalRelation implements LogicalPlan {

    private final StructType schema;
    private final List<Row> rows;

    public LocalRelation(StructType schema, List<Row> rows) {
        this.schema = schema;
        this.rows = List.copyOf(rows);
    }

    public List<Row> rows() { return rows; }

    @Override public StructType schema() { return schema; }
    @Override public List<LogicalPlan> children() { return List.of(); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) { return this; }
    @Override public boolean resolved() { return true; }
    @Override public String toString() { return "LocalRelation " + schema + " (" + rows.size() + " rows)"; }
}
