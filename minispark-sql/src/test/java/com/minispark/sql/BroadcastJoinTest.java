package com.minispark.sql;

import com.minispark.sql.execution.BroadcastHashJoinExec;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Broadcast-hash-join correctness + planner-selection coverage. The output of
 * BroadcastHashJoinExec must be row-for-row identical to ShuffledHashJoinExec
 * for the join types it supports — that's the whole point of having two
 * strategies behind one logical plan.
 */
final class BroadcastJoinTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("name", DataType.STRING));
    private final StructType orders = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("item", DataType.STRING));

    @BeforeEach void setUp() { spark = MiniSparkSession.builder("bcast-join-test", "local[3]"); }
    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    private DataFrame peopleDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")), people);
    }
    private DataFrame ordersDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp"), Row.of(4, "desk")), orders);
    }

    // ---------- planner picks the right strategy ----------

    @Test
    void hint_on_right_side_selects_broadcast_join() {
        DataFrame joined = peopleDf().join(ordersDf().broadcast(), "id");
        PhysicalPlan plan = joined.compile();
        assertThat(findJoin(plan)).isInstanceOf(BroadcastHashJoinExec.class);
    }

    @Test
    void hint_on_left_side_selects_broadcast_join() {
        DataFrame joined = peopleDf().broadcast().join(ordersDf(), "id");
        PhysicalPlan plan = joined.compile();
        assertThat(findJoin(plan)).isInstanceOf(BroadcastHashJoinExec.class);
    }

    @Test
    void no_hint_falls_back_to_shuffled_join_when_threshold_low() {
        // With the threshold set to 0, no auto-broadcast even for tiny sides.
        spark.close();
        spark = MiniSparkSession.on(new com.minispark.api.MiniSparkContext(
                new com.minispark.api.MiniSparkConf().setMaster("local[3]")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")));
        DataFrame joined = peopleDf().join(ordersDf(), "id");
        assertThat(findJoin(joined.compile())).isInstanceOf(ShuffledHashJoinExec.class);
    }

    @Test
    void small_local_relation_auto_broadcasts_without_hint() {
        // Default threshold (1000 rows) > our 3+4-row tables, so a plain join
        // of two LocalRelations should pick the broadcast strategy automatically.
        DataFrame joined = peopleDf().join(ordersDf(), "id");
        assertThat(findJoin(joined.compile())).isInstanceOf(BroadcastHashJoinExec.class);
    }

    @Test
    void full_outer_never_broadcasts_even_with_hint() {
        DataFrame joined = peopleDf().broadcast().join(ordersDf().broadcast(),
                List.of("id"), List.of("id"), JoinType.FULL);
        // FULL OUTER can't be evaluated map-only, so the planner must shuffle.
        assertThat(findJoin(joined.compile())).isInstanceOf(ShuffledHashJoinExec.class);
    }

    // ---------- broadcast results match shuffled results ----------

    @Test
    void inner_broadcast_matches_inner_shuffled() {
        List<Row> b = peopleDf().join(ordersDf().broadcast(), "id").collect();
        List<Row> s = noAutoBroadcast(() -> peopleDf().join(ordersDf(), "id").collect());
        assertSameMultiset(b, s);
        assertThat(b).hasSize(3);
    }

    @Test
    void left_outer_broadcast_matches_left_outer_shuffled() {
        // Build side = RIGHT (orders) for a LEFT outer join is the allowed shape.
        List<Row> b = peopleDf().join(ordersDf().broadcast(),
                List.of("id"), List.of("id"), JoinType.LEFT).collect();
        List<Row> s = noAutoBroadcast(() -> peopleDf().join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.LEFT).collect());
        assertSameMultiset(b, s);
        // carol with no match must appear once with null right columns.
        long carolNullPadded = b.stream()
                .filter(r -> "carol".equals(r.getString(1)) && r.isNullAt(3)).count();
        assertThat(carolNullPadded).isEqualTo(1);
    }

    @Test
    void right_outer_broadcast_matches_right_outer_shuffled() {
        // Build side = LEFT (people) for a RIGHT outer join is the allowed shape.
        List<Row> b = peopleDf().broadcast().join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.RIGHT).collect();
        List<Row> s = noAutoBroadcast(() -> peopleDf().join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.RIGHT).collect());
        assertSameMultiset(b, s);
        // order id=4 ("desk") has no person → left columns must be null.
        long deskNullPadded = b.stream()
                .filter(r -> "desk".equals(r.getString(3)) && r.isNullAt(0)).count();
        assertThat(deskNullPadded).isEqualTo(1);
    }

    @Test
    void null_join_keys_never_match_on_either_strategy() {
        DataFrame leftWithNull = spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(null, "ghost")), people);
        DataFrame rightWithNull = spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(null, "void")), orders);
        // SQL null != null in an equi-join: neither side's null-keyed row should match the other's.
        List<Row> b = leftWithNull.join(rightWithNull.broadcast(), "id").collect();
        assertThat(b).hasSize(1);
        assertThat(b.get(0).getString(1)).isEqualTo("alice");
        assertThat(b.get(0).getString(3)).isEqualTo("book");
    }

    // ---------- helpers ----------

    /** Recursively find the first join node (broadcast OR shuffled) in a physical plan. */
    private static PhysicalPlan findJoin(PhysicalPlan p) {
        if (p instanceof BroadcastHashJoinExec || p instanceof ShuffledHashJoinExec) return p;
        for (PhysicalPlan c : p.children()) {
            PhysicalPlan found = findJoin(c);
            if (found != null) return found;
        }
        return null;
    }

    /** Run {@code action} on a session whose auto-broadcast threshold is 0. */
    private List<Row> noAutoBroadcast(java.util.function.Supplier<List<Row>> action) {
        spark.close();
        spark = MiniSparkSession.on(new com.minispark.api.MiniSparkContext(
                new com.minispark.api.MiniSparkConf().setMaster("local[3]")
                        .set("minispark.sql.autoBroadcastJoinThreshold.rows", "0")));
        return action.get();
    }

    /** Equal as multisets — join output ordering is not guaranteed across strategies. */
    private static void assertSameMultiset(List<Row> a, List<Row> b) {
        assertThat(a).hasSize(b.size());
        assertThat(bag(a)).isEqualTo(bag(b));
    }
    private static java.util.Map<String, Long> bag(List<Row> rows) {
        return rows.stream().collect(java.util.stream.Collectors.groupingBy(
                BroadcastJoinTest::rowKey, java.util.stream.Collectors.counting()));
    }
    private static String rowKey(Row r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < r.size(); i++) sb.append(r.isNullAt(i) ? "NULL" : r.get(i)).append('|');
        return sb.toString();
    }
}
