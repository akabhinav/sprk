package com.minispark.sql.expr.window;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The three standard SQL ranking window functions. None take arguments — they
 * derive their values purely from the row's position in the ordered partition
 * and (for rank/dense_rank) the tie pattern of the ORDER BY values.
 *
 * Real Spark equivalents:
 *  - {@link RowNumber} → {@code org.apache.spark.sql.catalyst.expressions.RowNumber}
 *  - {@link Rank}      → {@code org.apache.spark.sql.catalyst.expressions.Rank}
 *  - {@link DenseRank} → {@code org.apache.spark.sql.catalyst.expressions.DenseRank}
 */
public final class RankingFunctions {

    private RankingFunctions() {}

    /** Sequential row index within partition: 1, 2, 3, ... (no ties). */
    public static final class RowNumber implements WindowFunction {
        @Override public String name() { return "row_number"; }
        @Override public DataType resultType(StructType inputSchema) { return DataType.LONG; }

        @Override
        public List<Object> evaluate(List<Row> partitionRows, List<Object[]> orderKeys) {
            int n = partitionRows.size();
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) out.add((long) (i + 1));
            return out;
        }
    }

    /** Standard rank with gaps: 1, 1, 3, 4, 4, 6, ... Equal ORDER BY values share a rank. */
    public static final class Rank implements WindowFunction {
        @Override public String name() { return "rank"; }
        @Override public DataType resultType(StructType inputSchema) { return DataType.LONG; }

        @Override
        public List<Object> evaluate(List<Row> partitionRows, List<Object[]> orderKeys) {
            int n = partitionRows.size();
            List<Object> out = new ArrayList<>(n);
            long rank = 1;
            for (int i = 0; i < n; i++) {
                if (i > 0 && !keysEqual(orderKeys.get(i), orderKeys.get(i - 1))) {
                    rank = i + 1;   // gap-on-tie: rank jumps to current position
                }
                out.add(rank);
            }
            return out;
        }
    }

    /** Dense rank: 1, 1, 2, 3, 3, 4, ... No gaps after ties. */
    public static final class DenseRank implements WindowFunction {
        @Override public String name() { return "dense_rank"; }
        @Override public DataType resultType(StructType inputSchema) { return DataType.LONG; }

        @Override
        public List<Object> evaluate(List<Row> partitionRows, List<Object[]> orderKeys) {
            int n = partitionRows.size();
            List<Object> out = new ArrayList<>(n);
            long rank = 1;
            for (int i = 0; i < n; i++) {
                if (i > 0 && !keysEqual(orderKeys.get(i), orderKeys.get(i - 1))) {
                    rank++;   // dense: just step by 1, regardless of tie-group size
                }
                out.add(rank);
            }
            return out;
        }
    }

    /** Two ORDER BY tuples count as ties iff every component is value-equal. */
    private static boolean keysEqual(Object[] a, Object[] b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        // Use Arrays.equals semantics — null == null, otherwise Objects.equals.
        return Arrays.equals(a, b);
    }
}
