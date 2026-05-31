package com.minispark.sql;

import com.minispark.sql.execution.BroadcastNestedLoopJoinExec;
import com.minispark.sql.execution.CartesianProductExec;
import com.minispark.sql.execution.PhysicalPlan;
import com.minispark.sql.execution.ShuffledHashJoinExec;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The non-equi / cross join strategies (CartesianProductExec,
 * BroadcastNestedLoopJoinExec) and the LEFT_SEMI / LEFT_ANTI / CROSS join types.
 */
final class NonEquiJoinTest {

    private MiniSparkSession spark;

    @BeforeEach void setUp() { spark = MiniSparkSession.builder("nonequi-join", "local[3]"); }
    @AfterEach  void tearDown() { if (spark != null) spark.close(); }

    private final StructType BUCKETS = StructType.of(
            StructField.of("lo", DataType.INT),
            StructField.of("hi", DataType.INT),
            StructField.of("label", DataType.STRING));
    private final StructType POINTS = StructType.of(StructField.of("x", DataType.INT));

    private DataFrame buckets() {
        return spark.createDataFrame(List.of(
                Row.of(0, 10, "low"), Row.of(10, 20, "mid"),
                Row.of(20, 30, "high"), Row.of(30, 40, "empty")), BUCKETS);
    }
    private DataFrame points() {
        return spark.createDataFrame(List.of(Row.of(5), Row.of(15), Row.of(25)), POINTS);
    }

    private final StructType PEOPLE = StructType.of(
            StructField.of("id", DataType.INT), StructField.of("name", DataType.STRING));
    private final StructType ORDERS = StructType.of(
            StructField.of("oid", DataType.INT), StructField.of("item", DataType.STRING));

    private DataFrame people() {
        return spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")), PEOPLE);
    }
    private DataFrame orders() {
        return spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp")), ORDERS);
    }

    // ---------- cross join → CartesianProductExec ----------

    @Test
    void cross_join_is_cartesian_product() {
        DataFrame crossed = buckets().crossJoin(points());
        assertThat(findJoin(crossed.compile())).isInstanceOf(CartesianProductExec.class);
        List<Row> rows = crossed.collect();
        // |buckets| × |points| = 4 × 3 = 12, each row has all 4 columns.
        assertThat(rows).hasSize(12);
        assertThat(rows).allSatisfy(r -> assertThat(r.size()).isEqualTo(4));
    }

    // ---------- non-equi inner join → BroadcastNestedLoopJoinExec ----------

    @Test
    void range_inner_join_keeps_only_pairs_satisfying_the_predicate() {
        // x in [lo, hi): a band/range join — no equi-key, so nested-loop.
        Column inBand = col("x").ge(col("lo")).and(col("x").lt(col("hi")));
        DataFrame joined = points().join(buckets(), inBand, JoinType.INNER);
        assertThat(findJoin(joined.compile())).isInstanceOf(BroadcastNestedLoopJoinExec.class);

        List<Row> rows = joined.collect();
        // 5→low, 15→mid, 25→high; the (30,40) bucket has no point. 3 rows.
        assertThat(rows).hasSize(3);
        // schema is points(x) ++ buckets(lo,hi,label): label is column index 3.
        assertThat(rows).extracting(r -> r.getInt(0) + ":" + r.getString(3))
                .containsExactlyInAnyOrder("5:low", "15:mid", "25:high");
    }

    @Test
    void range_left_join_null_pads_unmatched_left_rows() {
        // Keep every bucket; pair with any point inside it. (30,40) has none.
        Column inBand = col("x").ge(col("lo")).and(col("x").lt(col("hi")));
        DataFrame joined = buckets().join(points(), inBand, JoinType.LEFT);
        assertThat(findJoin(joined.compile())).isInstanceOf(BroadcastNestedLoopJoinExec.class);

        List<Row> rows = joined.collect();
        // 3 matched buckets + the empty one null-padded = 4 rows.
        assertThat(rows).hasSize(4);
        Row empty = rows.stream().filter(r -> "empty".equals(r.getString(2))).findFirst().orElseThrow();
        assertThat(empty.isNullAt(3)).isTrue();   // right column x is null
    }

    // ---------- LEFT_SEMI / LEFT_ANTI (equi) → ShuffledHashJoinExec ----------

    @Test
    void left_semi_keeps_left_rows_that_have_a_match_left_columns_only() {
        DataFrame semi = people().join(orders(),
                List.of("id"), List.of("oid"), JoinType.LEFT_SEMI);
        assertThat(findJoin(semi.compile())).isInstanceOf(ShuffledHashJoinExec.class);

        List<Row> rows = semi.collect();
        // alice(1) and bob(2) have orders; carol(3) doesn't. SEMI = {alice, bob}.
        assertThat(rows).hasSize(2);
        assertThat(rows).allSatisfy(r -> assertThat(r.size()).isEqualTo(2));  // left columns only
        assertThat(rows).extracting(r -> r.getString(1))
                .containsExactlyInAnyOrder("alice", "bob");
    }

    @Test
    void left_anti_keeps_left_rows_with_no_match() {
        DataFrame anti = people().join(orders(),
                List.of("id"), List.of("oid"), JoinType.LEFT_ANTI);
        List<Row> rows = anti.collect();
        // Only carol(3) has no order.
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("carol");
        assertThat(rows.get(0).size()).isEqualTo(2);  // left columns only
    }

    // ---------- non-equi semi/anti via nested loop ----------

    @Test
    void non_equi_left_semi_uses_nested_loop() {
        // "points that fall inside SOME bucket" — semi join on a range predicate.
        Column inBand = col("x").ge(col("lo")).and(col("x").lt(col("hi")));
        DataFrame semi = points().join(buckets(), inBand, JoinType.LEFT_SEMI);
        assertThat(findJoin(semi.compile())).isInstanceOf(BroadcastNestedLoopJoinExec.class);

        List<Row> rows = semi.collect();
        // 5,15,25 each fall in a bucket; all three kept, points columns only.
        assertThat(rows).hasSize(3);
        assertThat(rows).allSatisfy(r -> assertThat(r.size()).isEqualTo(1));
        assertThat(rows).extracting(r -> r.getInt(0)).containsExactlyInAnyOrder(5, 15, 25);
    }

    @Test
    void cross_join_with_condition_filters_pairs() {
        // CROSS with an ON predicate behaves like an inner nested-loop filter.
        DataFrame joined = points().join(buckets(),
                col("x").ge(col("lo")).and(col("x").lt(col("hi"))), JoinType.CROSS);
        assertThat(findJoin(joined.compile())).isInstanceOf(BroadcastNestedLoopJoinExec.class);
        assertThat(joined.collect()).hasSize(3);
    }

    // ---------- helpers ----------

    private static PhysicalPlan findJoin(PhysicalPlan p) {
        if (p instanceof CartesianProductExec
                || p instanceof BroadcastNestedLoopJoinExec
                || p instanceof ShuffledHashJoinExec) return p;
        for (PhysicalPlan c : p.children()) {
            PhysicalPlan f = findJoin(c);
            if (f != null) return f;
        }
        return null;
    }
}
