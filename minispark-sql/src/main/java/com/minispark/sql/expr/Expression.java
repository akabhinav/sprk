package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * A node in the expression tree — the {@code col("a") + 1 > 10} kind of thing.
 * Every expression can:
 * <ul>
 *   <li>report the {@link DataType} it produces against an input schema
 *       ({@link #dataType}) — used for analysis/type-checking;</li>
 *   <li>{@link #eval} itself against a concrete {@link Row} at runtime;</li>
 *   <li>expose its {@link #children} so generic tree transforms (the optimizer)
 *       can walk and rewrite it.</li>
 * </ul>
 *
 * <p>Expressions are bound to positions by the analyzer before execution:
 * {@link UnresolvedAttribute} (a name) is rewritten to {@link BoundReference}
 * (an ordinal), so {@link #eval} never does a name lookup on the hot path.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.Expression.
 */
public interface Expression extends Serializable {

    /** Result type given the schema of the rows this expression will see. */
    DataType dataType(StructType inputSchema);

    /** Evaluate against one row. Implementations may assume the tree is resolved. */
    Object eval(Row input);

    /** Child expressions, for generic tree traversal/rewriting. */
    List<Expression> children();

    /** Rebuild this node with new children (same order/arity). For the optimizer. */
    Expression withChildren(List<Expression> newChildren);

    /** True once every {@link UnresolvedAttribute} has become a {@link BoundReference}. */
    default boolean resolved() {
        if (this instanceof UnresolvedAttribute) return false;
        for (Expression c : children()) if (!c.resolved()) return false;
        return true;
    }

    /** A human-readable name for this expression's output column. */
    default String name() { return toString(); }
}
