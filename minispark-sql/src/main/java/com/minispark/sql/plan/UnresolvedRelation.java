package com.minispark.sql.plan;

import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A {@code FROM tableName} reference before the catalog has been consulted. The
 * analyzer replaces it with the registered relation (e.g. a {@link
 * LocalRelation}). Until then it has no schema and is unresolved.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.analysis.UnresolvedRelation.
 */
public final class UnresolvedRelation implements LogicalPlan {

    private final String tableName;

    public UnresolvedRelation(String tableName) { this.tableName = tableName; }

    public String tableName() { return tableName; }

    @Override public StructType schema() {
        throw new IllegalStateException("Unresolved relation '" + tableName + "' has no schema yet");
    }
    @Override public List<LogicalPlan> children() { return List.of(); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) { return this; }
    @Override public boolean resolved() { return false; }
    @Override public String toString() { return "UnresolvedRelation " + tableName; }
}
