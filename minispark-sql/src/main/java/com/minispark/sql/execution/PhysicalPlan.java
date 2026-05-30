package com.minispark.sql.execution;

import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A node in the <i>physical</i> plan: it knows how to {@link #execute} into an
 * {@code RDD<Row>} on the MiniSpark engine. The planner turns each optimized
 * logical operator into one of these; calling {@link #execute} on the root
 * builds the RDD lineage, and an action on that RDD runs the actual job.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.SparkPlan.
 */
public interface PhysicalPlan {

    StructType schema();

    /** Build the RDD that produces this node's rows. */
    RDD<Row> execute();

    List<PhysicalPlan> children();

    default String treeString() {
        StringBuilder sb = new StringBuilder();
        build(sb, 0);
        return sb.toString();
    }

    default void build(StringBuilder sb, int depth) {
        sb.append("  ".repeat(depth)).append(this).append('\n');
        for (PhysicalPlan c : children()) c.build(sb, depth + 1);
    }
}
