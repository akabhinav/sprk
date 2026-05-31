package com.minispark.sql.plan;

/**
 * The join variants MiniSQL supports.
 * <ul>
 *   <li>{@code INNER} — only matching pairs.</li>
 *   <li>{@code LEFT}/{@code RIGHT}/{@code FULL} — outer, padding the
 *       non-matching side with nulls.</li>
 *   <li>{@code LEFT_SEMI} — left rows with <i>at least one</i> right match;
 *       outputs the left columns only (the SQL {@code EXISTS}/{@code IN} shape).</li>
 *   <li>{@code LEFT_ANTI} — left rows with <i>no</i> right match; left columns
 *       only ({@code NOT EXISTS}/{@code NOT IN}).</li>
 *   <li>{@code CROSS} — cartesian product; every left row paired with every
 *       right row (optionally filtered by a non-equi condition).</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.JoinType.
 */
public enum JoinType { INNER, LEFT, RIGHT, FULL, LEFT_SEMI, LEFT_ANTI, CROSS }
