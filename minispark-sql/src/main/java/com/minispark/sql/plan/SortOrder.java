package com.minispark.sql.plan;

import com.minispark.sql.expr.Expression;

import java.io.Serializable;

/**
 * One ORDER BY term: the expression to sort by and the direction.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.SortOrder.
 */
public record SortOrder(Expression expr, boolean ascending) implements Serializable {
    @Override public String toString() { return expr + (ascending ? " ASC" : " DESC"); }
}
