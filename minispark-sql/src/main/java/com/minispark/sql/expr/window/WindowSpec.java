package com.minispark.sql.expr.window;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.SortOrder;

import java.io.Serializable;
import java.util.List;

/**
 * The {@code PARTITION BY ... ORDER BY ...} clause of a window. Empty
 * partitionBy means "all rows in one logical partition"; empty orderBy means
 * "preserve arbitrary order from the upstream shuffle" (ROW_NUMBER without
 * ORDER BY is allowed and produces stable-but-unspecified numbering).
 *
 * <p>No frame clause yet (ROWS/RANGE BETWEEN). Ranking functions ignore
 * frames anyway; aggregate windows would need them, which is the natural
 * next step.
 *
 * Real Spark equivalent: org.apache.spark.sql.expressions.WindowSpec.
 */
public final class WindowSpec implements Serializable {

    private final List<Expression> partitionBy;
    private final List<SortOrder> orderBy;

    public WindowSpec(List<Expression> partitionBy, List<SortOrder> orderBy) {
        this.partitionBy = List.copyOf(partitionBy);
        this.orderBy = List.copyOf(orderBy);
    }

    public List<Expression> partitionBy() { return partitionBy; }
    public List<SortOrder> orderBy() { return orderBy; }

    @Override public String toString() {
        return "WindowSpec(partitionBy=" + partitionBy + ", orderBy=" + orderBy + ")";
    }
}
