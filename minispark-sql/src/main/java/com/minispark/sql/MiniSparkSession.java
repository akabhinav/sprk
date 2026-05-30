package com.minispark.sql;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.sql.analysis.Analyzer;
import com.minispark.sql.execution.SparkPlanner;
import com.minispark.sql.optimizer.Optimizer;
import com.minispark.sql.plan.LocalRelation;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * The SQL entry point: wraps a {@link MiniSparkContext} (the compute engine)
 * and owns the analyzer / optimizer / planner that compile DataFrames down to
 * RDD jobs. Mirrors Spark's {@code SparkSession} sitting on top of
 * {@code SparkContext}.
 *
 * Real Spark equivalent: org.apache.spark.sql.SparkSession.
 */
public final class MiniSparkSession implements AutoCloseable {

    private final MiniSparkContext sc;
    private final com.minispark.sql.analysis.Catalog catalog = new com.minispark.sql.analysis.Catalog();
    private final Analyzer analyzer = new Analyzer(catalog);
    private final Optimizer optimizer = new Optimizer();
    private final SparkPlanner planner;
    private final int numPartitions;

    private MiniSparkSession(MiniSparkContext sc, int numPartitions) {
        this.sc = sc;
        this.numPartitions = numPartitions;
        this.planner = new SparkPlanner(sc, numPartitions);
    }

    public static MiniSparkSession builder(String appName, String master) {
        MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName(appName).setMaster(master));
        return new MiniSparkSession(sc, sc.defaultParallelism());
    }

    /** Wrap an existing context (so SQL and RDD code share one engine). */
    public static MiniSparkSession on(MiniSparkContext sc) {
        return new MiniSparkSession(sc, sc.defaultParallelism());
    }

    public MiniSparkContext sparkContext() { return sc; }
    public Analyzer analyzer() { return analyzer; }
    public Optimizer optimizer() { return optimizer; }
    public SparkPlanner planner() { return planner; }

    /** Build a DataFrame from in-memory rows + a schema. */
    public DataFrame createDataFrame(List<Row> rows, StructType schema) {
        // Validate values against the declared schema up front (fail fast).
        for (Row r : rows) {
            if (r.size() != schema.size()) {
                throw new IllegalArgumentException(
                        "Row arity " + r.size() + " != schema arity " + schema.size());
            }
            for (int i = 0; i < r.size(); i++) {
                if (!schema.type(i).accepts(r.get(i))) {
                    throw new IllegalArgumentException("Value " + r.get(i)
                            + " not a " + schema.type(i) + " for column '" + schema.name(i) + "'");
                }
            }
        }
        return new DataFrame(this, new LocalRelation(schema, rows));
    }

    /** Register {@code df}'s plan as a temp view, so {@code sql("... FROM name")} resolves it. */
    public void createOrReplaceTempView(String name, DataFrame df) {
        catalog.registerView(name, df.logicalPlan());
    }

    /** Parse and run a SQL string against the registered temp views. */
    public DataFrame sql(String sqlText) {
        com.minispark.sql.plan.LogicalPlan parsed =
                com.minispark.sql.parser.SqlParser.parsePlan(sqlText);
        return new DataFrame(this, parsed);
    }

    @Override public void close() { sc.close(); }
}
