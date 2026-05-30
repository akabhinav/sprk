package com.minispark.sql.plan;

import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;
import java.util.function.Supplier;

/**
 * A leaf plan backed by an already-built {@code RDD<Row>} with a known schema —
 * the bridge between an external data source (a file read that is itself a
 * distributed job) and the SQL plan tree. The {@link Supplier} is invoked lazily
 * at execution time so building the plan doesn't trigger the read.
 *
 * <p>The supplier is {@code transient}-ish in spirit: it holds driver-side RDD
 * construction and is only ever called on the driver during planning, never
 * shipped to executors.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.LogicalRDD / the
 *                        relation a DataSource scan produces.
 */
public final class LogicalRDD implements LogicalPlan {

    private final StructType schema;
    private final transient Supplier<RDD<Row>> rddSupplier;
    private final String description;

    public LogicalRDD(StructType schema, Supplier<RDD<Row>> rddSupplier, String description) {
        this.schema = schema;
        this.rddSupplier = rddSupplier;
        this.description = description;
    }

    public RDD<Row> buildRdd() { return rddSupplier.get(); }

    @Override public StructType schema() { return schema; }
    @Override public List<LogicalPlan> children() { return List.of(); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) { return this; }
    @Override public boolean resolved() { return true; }
    @Override public String toString() { return "LogicalRDD " + description + " " + schema; }
}
