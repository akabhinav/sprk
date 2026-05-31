package com.minispark.sql.plan;

import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * A logical wrapper that asks the planner to materialise this side of a join
 * as a {@link com.minispark.broadcast.Broadcast} and run the join map-side
 * (no shuffle on the streaming side). Pass-through node: same schema, same
 * resolution state as its child.
 *
 * <p>Set via {@link com.minispark.sql.DataFrame#broadcast()}. The planner
 * looks for it on either input of a {@link Join} and, if present, chooses
 * {@code BroadcastHashJoinExec} instead of {@code ShuffledHashJoinExec}.
 *
 * Real Spark equivalent: ResolvedHint(child, HintInfo(strategy=BROADCAST))
 * / org.apache.spark.sql.catalyst.plans.logical.HintInfo(BROADCAST).
 */
public final class BroadcastHint implements LogicalPlan {

    private final LogicalPlan child;

    public BroadcastHint(LogicalPlan child) {
        this.child = child;
    }

    public LogicalPlan child() { return child; }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) {
        return new BroadcastHint(c.get(0));
    }
    @Override public boolean resolved() { return child.resolved(); }
    @Override public String toString() { return "BroadcastHint"; }
}
