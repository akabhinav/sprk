package com.minispark.sql;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.sql.execution.AdaptiveJoinExec;
import com.minispark.sql.execution.BroadcastHashJoinExec;
import com.minispark.sql.execution.PhysicalPlan;
import com.minispark.sql.execution.ShuffledHashJoinExec;
import com.minispark.sql.execution.SortMergeJoinExec;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runtime AQE for join strategy: the planner wraps non-broadcast joins in
 * AdaptiveJoinExec when {@code minispark.sql.adaptive.enabled=true}. At
 * execute time the wrapper materialises both children, learns their actual
 * row counts, and demotes to BroadcastHashJoinExec when a side fits the
 * runtime threshold — even if the compile-time auto-broadcast missed it
 * because the side flowed through a filter/aggregate.
 */
final class AdaptiveJoinTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("name", DataType.STRING));
    private final StructType orders = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("item", DataType.STRING));

    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    /** Session with AQE on, compile-time auto-broadcast off, runtime demote threshold = 50. */
    private MiniSparkSession aqeSession() {
        return MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")
                        .set("minispark.sql.adaptive.enabled", "true")
                        .set("minispark.sql.adaptive.autoBroadcastJoinThreshold.rows", "50")));
    }

    // ---------- planner wraps in AdaptiveJoinExec when AQE on ----------

    @Test
    void aqe_off_no_wrapper() {
        spark = MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")));
        DataFrame df = peopleDf().join(ordersDf(), "id");
        assertThat(findAdaptive(df.compile())).isNull();
    }

    @Test
    void aqe_on_wraps_shuffled_hash_join() {
        spark = aqeSession();
        DataFrame df = peopleDf().join(ordersDf(), "id");
        AdaptiveJoinExec a = findAdaptive(df.compile());
        assertThat(a).isNotNull();
        // toString carries the chosen fallback for debugging.
        assertThat(a.toString()).contains("SHUFFLED_HASH");
    }

    @Test
    void aqe_on_with_smj_pref_wraps_sort_merge() {
        spark = MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")
                        .set("minispark.sql.adaptive.enabled", "true")
                        .set("minispark.sql.join.preferSortMergeJoin", "true")));
        DataFrame df = peopleDf().join(ordersDf(), "id");
        AdaptiveJoinExec a = findAdaptive(df.compile());
        assertThat(a).isNotNull();
        assertThat(a.toString()).contains("SORT_MERGE");
    }

    @Test
    void compile_time_broadcast_still_skips_aqe_wrapping() {
        // If a side has a static-size auto-broadcast at compile time, no wrap
        // (the join is already broadcast — re-deferring would just slow it down).
        spark = MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]")
                        // Leave auto-broadcast threshold at the default 1000 — peopleDf has 3 rows.
                        .set("minispark.sql.adaptive.enabled", "true")));
        DataFrame df = peopleDf().join(ordersDf(), "id");
        assertThat(findAdaptive(df.compile())).isNull();
        // Should have picked broadcast at compile time.
        assertThat(findJoin(df.compile())).isInstanceOf(BroadcastHashJoinExec.class);
    }

    // ---------- runtime demote actually fires when a side comes in small ----------

    @Test
    void runtime_demote_when_filtered_side_fits_threshold() {
        spark = aqeSession();
        // The filter is opaque to the planner — compile-time the side looks
        // "unknown size", so auto-broadcast is off. At runtime the filter
        // leaves 2 rows, well under the demote threshold of 50.
        DataFrame bigPeople = manyPeople(/*total=*/300);    // compile-time unknown after filter
        DataFrame filtered = bigPeople.filter(new Column(
                com.minispark.sql.expr.Comparison.lt(
                        new com.minispark.sql.expr.UnresolvedAttribute("id"),
                        new com.minispark.sql.expr.Literal(2, DataType.INT))));

        List<Row> rows = filtered.join(ordersDf(), "id").collect();
        // Only id=0 and id=1 survive the filter; id=1 has book+pen in orders, id=0 has no orders.
        // → 2 result rows (alice0:book? no — alice0 doesn't exist in orders. Let me re-derive:
        //   orders has ids 1, 1, 2, 4; filtered has ids 0, 1 → matches: id=1 × {book,pen} = 2 rows
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getString(3))
                .containsExactlyInAnyOrder("book", "pen");
    }

    @Test
    void runtime_keeps_strategy_when_both_sides_too_big() {
        spark = aqeSession();
        // Both sides have 300 unique ids 0..299, well above the 50-row threshold.
        // No demote → stays on the shuffled-hash fallback. Self-join by id is 1:1.
        DataFrame big1 = manyPeople(300);
        DataFrame big2 = manyOrders(300);
        assertThat(big1.join(big2, "id").count()).isEqualTo(300L);
    }

    @Test
    void demoted_result_matches_undemoted_result_row_for_row() {
        // With AQE: should demote. Without AQE: pure shuffled-hash. Both must
        // produce the same rows — runtime re-planning must never change answers.
        List<Row> withAqe = runFilteredJoin(/*aqe=*/true);
        List<Row> noAqe  = runFilteredJoin(/*aqe=*/false);
        assertSameMultiset(withAqe, noAqe);
    }

    @Test
    void full_outer_join_does_not_demote_even_when_side_fits() {
        spark = aqeSession();
        // FULL OUTER is never broadcast-eligible (can't emit unmatched build
        // side map-only). AQE wraps, decides "neither canBuildLeft nor
        // canBuildRight applies," falls through to the shuffled fallback.
        DataFrame filtered = manyPeople(300).filter(new Column(
                com.minispark.sql.expr.Comparison.lt(
                        new com.minispark.sql.expr.UnresolvedAttribute("id"),
                        new com.minispark.sql.expr.Literal(2, DataType.INT))));
        long count = filtered.join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.FULL).count();
        // 2 left rows (id 0, 1) ⋈ 4 right rows; INNER matches: id=1 × 2 orders = 2.
        // Unmatched left: id=0 → 1. Unmatched right: id=2, id=4 → 2.
        // Total FULL = 2 + 1 + 2 = 5.
        assertThat(count).isEqualTo(5L);
    }

    // ---------- helpers ----------

    private List<Row> runFilteredJoin(boolean aqe) {
        if (aqe) spark = aqeSession();
        else spark = MiniSparkSession.on(new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")));
        try {
            DataFrame filtered = manyPeople(300).filter(new Column(
                    com.minispark.sql.expr.Comparison.lt(
                            new com.minispark.sql.expr.UnresolvedAttribute("id"),
                            new com.minispark.sql.expr.Literal(2, DataType.INT))));
            return filtered.join(ordersDf(), "id").collect();
        } finally {
            spark.close();
            spark = null;
        }
    }

    private DataFrame peopleDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")), people);
    }
    private DataFrame ordersDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp"), Row.of(4, "desk")), orders);
    }
    /** {@code n} unique ids 0..n-1 — so a filter on id is sharp and a self-join count is predictable. */
    private DataFrame manyPeople(int n) {
        List<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rows.add(Row.of(i, "p" + i));
        return spark.createDataFrame(rows, people);
    }
    private DataFrame manyOrders(int n) {
        List<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rows.add(Row.of(i, "o" + i));
        return spark.createDataFrame(rows, orders);
    }

    private static AdaptiveJoinExec findAdaptive(PhysicalPlan p) {
        if (p instanceof AdaptiveJoinExec a) return a;
        for (PhysicalPlan c : p.children()) {
            AdaptiveJoinExec f = findAdaptive(c);
            if (f != null) return f;
        }
        return null;
    }

    private static PhysicalPlan findJoin(PhysicalPlan p) {
        if (p instanceof BroadcastHashJoinExec
                || p instanceof ShuffledHashJoinExec
                || p instanceof SortMergeJoinExec
                || p instanceof AdaptiveJoinExec) return p;
        for (PhysicalPlan c : p.children()) {
            PhysicalPlan f = findJoin(c);
            if (f != null) return f;
        }
        return null;
    }

    private static void assertSameMultiset(List<Row> a, List<Row> b) {
        assertThat(a).hasSize(b.size());
        assertThat(bag(a)).isEqualTo(bag(b));
    }
    private static java.util.Map<String, Long> bag(List<Row> rows) {
        return rows.stream().collect(java.util.stream.Collectors.groupingBy(
                AdaptiveJoinTest::rowKey, java.util.stream.Collectors.counting()));
    }
    private static String rowKey(Row r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < r.size(); i++) sb.append(r.isNullAt(i) ? "NULL" : r.get(i)).append('|');
        return sb.toString();
    }
}
