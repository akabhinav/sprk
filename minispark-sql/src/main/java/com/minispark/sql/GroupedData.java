package com.minispark.sql;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.agg.AggregateFunction;
import com.minispark.sql.plan.Aggregate;
import com.minispark.sql.plan.LogicalPlan;

import java.util.List;

/**
 * The intermediate object returned by {@link DataFrame#groupBy}: holds the
 * grouping expressions until {@code .agg(...)} supplies the aggregate functions,
 * at which point it produces a DataFrame over an {@link Aggregate} plan node.
 *
 * Real Spark equivalent: org.apache.spark.sql.RelationalGroupedDataset.
 */
public final class GroupedData {

    private final MiniSparkSession session;
    private final LogicalPlan child;
    private final List<Expression> groupingExprs;

    GroupedData(MiniSparkSession session, LogicalPlan child, List<Expression> groupingExprs) {
        this.session = session;
        this.child = child;
        this.groupingExprs = groupingExprs;
    }

    /** Apply aggregate functions, producing one row per group. */
    public DataFrame agg(AggregateFunction... aggregates) {
        return new DataFrame(session, new Aggregate(groupingExprs, List.of(aggregates), child));
    }

    /** Shorthand for {@code agg(functions.count())}. */
    public DataFrame count() {
        return agg(com.minispark.sql.functions.count());
    }
}
