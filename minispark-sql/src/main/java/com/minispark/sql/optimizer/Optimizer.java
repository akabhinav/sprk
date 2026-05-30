package com.minispark.sql.optimizer;

import com.minispark.sql.plan.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Applies a batch of rewrite {@link Rule}s to a logical plan until it stops
 * changing (a fixed point) or a max-iteration guard trips. This is the engine
 * of a rule-based optimizer; the rules themselves (predicate pushdown, etc.)
 * are pluggable.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.optimizer.Optimizer
 *                        (RuleExecutor's fixed-point batch loop).
 */
public final class Optimizer {

    private static final Logger LOG = LoggerFactory.getLogger(Optimizer.class);
    private static final int MAX_ITERATIONS = 100;

    private final List<Rule> rules;

    public Optimizer() {
        this(List.of(new ConstantFolding(), new PushDownFilter(), new CombineFilters()));
    }

    public Optimizer(List<Rule> rules) { this.rules = rules; }

    public LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan current = plan;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            LogicalPlan beforeIteration = current;
            for (Rule rule : rules) {
                LogicalPlan after = rule.apply(current);
                if (after != current && !after.toString().equals(current.toString())) {
                    LOG.debug("Rule {} rewrote the plan", rule.name());
                }
                current = after;
            }
            // Fixed point: a full pass changed nothing.
            if (current.treeString().equals(beforeIteration.treeString())) break;
        }
        return current;
    }
}
