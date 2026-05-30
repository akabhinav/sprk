package com.minispark.sql.optimizer;

import com.minispark.sql.plan.LogicalPlan;

/**
 * A single logical-plan rewrite rule. The {@link Optimizer} applies rules
 * repeatedly to a fixed point. Each rule is a pure {@code LogicalPlan ->
 * LogicalPlan} function and should be idempotent once it has nothing left to do.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.rules.Rule.
 */
public interface Rule {
    String name();
    LogicalPlan apply(LogicalPlan plan);
}
