package com.minispark.sql;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.UnresolvedAttribute;
import com.minispark.sql.expr.window.WindowFunction;
import com.minispark.sql.expr.window.WindowSpec;
import com.minispark.sql.plan.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for {@link WindowSpec}, plus factory helpers for the three
 * ranking functions wrapped as {@link Column}s ready to drop into
 * {@code df.withColumn(name, fn.over(window))}.
 *
 * <pre>
 *   WindowSpec w = Window.partitionBy("dept").orderBy("salary");
 *   df.withColumn("rank", Window.rank().over(w));
 * </pre>
 *
 * Real Spark equivalent: org.apache.spark.sql.expressions.Window /
 * org.apache.spark.sql.functions.{row_number, rank, dense_rank}.
 */
public final class Window {

    private Window() {}

    // ----- spec builders -----

    public static Builder partitionBy(String... cols) {
        Builder b = new Builder();
        for (String c : cols) b.partitionBy.add(new UnresolvedAttribute(c));
        return b;
    }

    public static Builder partitionBy(Column... cols) {
        Builder b = new Builder();
        for (Column c : cols) b.partitionBy.add(c.expr());
        return b;
    }

    public static Builder orderBy(String... cols) {
        Builder b = new Builder();
        for (String c : cols) b.orderBy.add(new SortOrder(new UnresolvedAttribute(c), true));
        return b;
    }

    public static Builder orderBy(Column... cols) {
        Builder b = new Builder();
        for (Column c : cols) b.orderBy.add(new SortOrder(c.expr(), true));
        return b;
    }

    /** Empty spec — equivalent to {@code OVER ()}. Use {@code .orderBy(...)} or similar to extend. */
    public static Builder empty() { return new Builder(); }

    // ----- ranking-function factories -----

    public static WindowFunctionColumn rowNumber() {
        return new WindowFunctionColumn(new com.minispark.sql.expr.window.RankingFunctions.RowNumber());
    }
    public static WindowFunctionColumn rank() {
        return new WindowFunctionColumn(new com.minispark.sql.expr.window.RankingFunctions.Rank());
    }
    public static WindowFunctionColumn denseRank() {
        return new WindowFunctionColumn(new com.minispark.sql.expr.window.RankingFunctions.DenseRank());
    }

    // ----- nested types -----

    /** Mutable spec builder. Calls chain; {@link #build} produces the immutable {@link WindowSpec}. */
    public static final class Builder {
        private final List<Expression> partitionBy = new ArrayList<>();
        private final List<SortOrder> orderBy = new ArrayList<>();

        public Builder partitionBy(String... cols) {
            for (String c : cols) partitionBy.add(new UnresolvedAttribute(c));
            return this;
        }
        public Builder partitionBy(Column... cols) {
            for (Column c : cols) partitionBy.add(c.expr());
            return this;
        }
        public Builder orderBy(String... cols) {
            for (String c : cols) orderBy.add(new SortOrder(new UnresolvedAttribute(c), true));
            return this;
        }
        public Builder orderBy(Column... cols) {
            for (Column c : cols) orderBy.add(new SortOrder(c.expr(), true));
            return this;
        }
        public Builder orderByDesc(String... cols) {
            for (String c : cols) orderBy.add(new SortOrder(new UnresolvedAttribute(c), false));
            return this;
        }

        public WindowSpec build() { return new WindowSpec(partitionBy, orderBy); }
    }

    /** Intermediate object returned by {@link #rowNumber} et al. — pairs with {@code .over(spec)}. */
    public static final class WindowFunctionColumn {
        private final WindowFunction fn;
        WindowFunctionColumn(WindowFunction fn) { this.fn = fn; }
        public Column over(WindowSpec spec) {
            return new Column(new com.minispark.sql.expr.window.WindowExpression(fn, spec));
        }
        public Column over(Builder b) { return over(b.build()); }
    }
}
