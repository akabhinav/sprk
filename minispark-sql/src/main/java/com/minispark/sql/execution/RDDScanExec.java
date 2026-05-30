package com.minispark.sql.execution;

import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.plan.LogicalRDD;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Physical scan over a {@link LogicalRDD}: just hands back the source RDD. The
 * RDD itself (e.g. a CSV read built on {@code textFile} → parse) carries the
 * distributed work; this node is the plan-tree adapter.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.RDDScanExec / FileSourceScanExec.
 */
public final class RDDScanExec implements PhysicalPlan {

    private final LogicalRDD rdd;

    public RDDScanExec(LogicalRDD rdd) { this.rdd = rdd; }

    @Override public StructType schema() { return rdd.schema(); }
    @Override public RDD<Row> execute() { return rdd.buildRdd(); }
    @Override public List<PhysicalPlan> children() { return List.of(); }
    @Override public String toString() { return "RDDScan " + rdd.schema(); }
}
