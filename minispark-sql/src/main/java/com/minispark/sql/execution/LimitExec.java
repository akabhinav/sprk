package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Physical limit. Takes the first {@code n} rows to the driver via {@code take}
 * (which short-circuits per partition) and re-parallelizes them as a small RDD
 * so a parent operator can keep composing. This is Spark's {@code CollectLimit}
 * shape — fine for the small results LIMIT implies.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.CollectLimitExec.
 */
public final class LimitExec implements PhysicalPlan {

    private final int limit;
    private final PhysicalPlan child;
    private final MiniSparkContext sc;

    public LimitExec(int limit, PhysicalPlan child, MiniSparkContext sc) {
        this.limit = limit;
        this.child = child;
        this.sc = sc;
    }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "LimitExec " + limit; }

    @Override
    public RDD<Row> execute() {
        List<Row> head = child.execute().take(limit);
        return sc.parallelize(head, 1);
    }
}
