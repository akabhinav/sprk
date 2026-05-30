package com.minispark.sql.plan;

/**
 * The join variants MiniSQL supports. INNER keeps only matching pairs; LEFT/
 * RIGHT/FULL outer pad the non-matching side with nulls.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.JoinType.
 */
public enum JoinType { INNER, LEFT, RIGHT, FULL }
