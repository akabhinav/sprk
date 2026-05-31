package com.minispark.sql.execution;

import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A leaf physical node that simply returns an RDD the caller has already
 * built (and typically already materialised + cached). Used by
 * {@link AdaptiveJoinExec} to feed the same RDD into a re-planned join
 * operator without re-running its lineage — the cache directive on the
 * inner RDD makes the second read pull from {@code BlockManager} instead.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.RDDScanExec (when
 * pointed at an already-materialised intermediate RDD) — the broader role
 * is what real Spark calls a {@code QueryStageExec}.
 */
public final class MaterializedRDDScanExec implements PhysicalPlan {

    private final RDD<Row> rdd;
    private final StructType schema;

    public MaterializedRDDScanExec(RDD<Row> rdd, StructType schema) {
        this.rdd = rdd;
        this.schema = schema;
    }

    @Override public StructType schema() { return schema; }
    @Override public RDD<Row> execute() { return rdd; }
    @Override public List<PhysicalPlan> children() { return List.of(); }
    @Override public String toString() { return "MaterializedRDDScanExec " + schema; }
}
