package com.minispark.sql.expr.window;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * A function that computes one value <i>per input row</i> within an ordered
 * partition — unlike {@link com.minispark.sql.expr.agg.AggregateFunction},
 * which collapses a group to one value. Driven by
 * {@link com.minispark.sql.execution.WindowExec}: for each ORDER-BY-sorted
 * partition the executor calls {@link #evaluate} with the partition's rows
 * (in order) and gets back a parallel list of N output values.
 *
 * <p>Examples: {@code ROW_NUMBER()}, {@code RANK()}, {@code DENSE_RANK()},
 * and (future) aggregate-over-window forms like {@code SUM(x) OVER (...)}.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.WindowFunction
 * (ours is non-incremental — Spark's interface supports streaming evaluation
 * over a sliding frame; for ranking functions the simpler "see the whole
 * partition" shape is identical in effect).
 */
public interface WindowFunction extends Serializable {

    /** Function display name (used for default output column names). */
    String name();

    /** Output type — invariant across all rows of a partition. */
    DataType resultType(StructType inputSchema);

    /**
     * @param partitionRows the rows of one partition, already sorted by the
     *                      window's ORDER BY (empty ORDER BY = arbitrary order
     *                      preserved from the upstream shuffle)
     * @param orderKeys     the evaluated ORDER BY values per row, in the same
     *                      order — used by rank/dense_rank to detect ties.
     *                      Length matches {@code partitionRows}. Inner arrays
     *                      have length = number of ORDER BY columns. Empty
     *                      inner arrays when there's no ORDER BY (e.g. for
     *                      ROW_NUMBER over an unordered partition).
     * @return one output value per input row, in the same order
     */
    List<Object> evaluate(List<Row> partitionRows, List<Object[]> orderKeys);
}
