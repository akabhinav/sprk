package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Physical leaf for a {@link com.minispark.sql.plan.LocalRelation}: parallelizes
 * the in-memory rows into an {@code RDD<Row>} on the engine.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.LocalTableScanExec.
 */
public final class LocalTableScanExec implements PhysicalPlan {

    private final StructType schema;
    private final List<Row> rows;
    private final MiniSparkContext sc;
    private final int numPartitions;

    public LocalTableScanExec(StructType schema, List<Row> rows,
                              MiniSparkContext sc, int numPartitions) {
        this.schema = schema;
        this.rows = rows;
        this.sc = sc;
        this.numPartitions = numPartitions;
    }

    @Override public StructType schema() { return schema; }
    @Override public RDD<Row> execute() {
        return sc.parallelize(rows, Math.max(1, Math.min(numPartitions, Math.max(1, rows.size()))));
    }
    @Override public List<PhysicalPlan> children() { return List.of(); }
    @Override public String toString() { return "LocalTableScan " + schema; }
}
