package com.minispark.sql.plan;

import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * A node in the <i>logical</i> plan — what the query computes, not how. The
 * DataFrame API builds these; the analyzer resolves their expressions; the
 * optimizer rewrites the tree by rules; the planner turns the optimized logical
 * tree into a {@link com.minispark.sql.execution.PhysicalPlan}.
 *
 * <p>Each node reports the {@link StructType} it outputs (so parents can
 * resolve column references against it) and exposes its children for generic
 * tree rewriting.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.
 */
public interface LogicalPlan extends Serializable {

    /** Schema of the rows this node produces. */
    StructType schema();

    List<LogicalPlan> children();

    /** Rebuild with new children (same order/arity), for the optimizer. */
    LogicalPlan withChildren(List<LogicalPlan> newChildren);

    /** True once this node and all descendants have resolved expressions/schemas. */
    boolean resolved();

    /** Pretty multi-line tree, indented by depth. */
    default String treeString() {
        StringBuilder sb = new StringBuilder();
        buildTree(sb, 0);
        return sb.toString();
    }

    default void buildTree(StringBuilder sb, int depth) {
        sb.append("  ".repeat(depth)).append(this).append('\n');
        for (LogicalPlan c : children()) c.buildTree(sb, depth + 1);
    }
}
