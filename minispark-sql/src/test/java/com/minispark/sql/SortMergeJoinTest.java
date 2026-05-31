package com.minispark.sql;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
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
 * SortMergeJoinExec correctness + planner selection (opt-in via
 * {@code minispark.sql.join.preferSortMergeJoin}). Output must match
 * ShuffledHashJoinExec / BroadcastHashJoinExec row-for-row across all four
 * join types — switching strategies must never change query semantics.
 */
final class SortMergeJoinTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("name", DataType.STRING));
    private final StructType orders = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("item", DataType.STRING));

    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    private MiniSparkSession session(boolean preferSmj, boolean disableAutoBroadcast) {
        MiniSparkConf c = new MiniSparkConf().setMaster("local[3]");
        if (preferSmj) c.set("minispark.sql.join.preferSortMergeJoin", "true");
        if (disableAutoBroadcast) c.set("minispark.sql.autoBroadcastJoinThreshold.rows", "0");
        return MiniSparkSession.on(new MiniSparkContext(c));
    }

    private DataFrame peopleDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")), people);
    }
    private DataFrame ordersDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp"), Row.of(4, "desk")), orders);
    }

    // ---------- planner selection ----------

    @Test
    void prefer_smj_flag_picks_sort_merge_when_no_broadcast() {
        spark = session(/*preferSmj=*/true, /*disableAutoBroadcast=*/true);
        DataFrame joined = peopleDf().join(ordersDf(), "id");
        assertThat(findJoin(joined.compile())).isInstanceOf(SortMergeJoinExec.class);
    }

    @Test
    void broadcast_hint_still_wins_over_smj_flag() {
        spark = session(/*preferSmj=*/true, /*disableAutoBroadcast=*/true);
        DataFrame joined = peopleDf().join(ordersDf().broadcast(), "id");
        // Hint > preferSMJ — broadcast is strictly cheaper when feasible.
        assertThat(findJoin(joined.compile())).isInstanceOf(BroadcastHashJoinExec.class);
    }

    @Test
    void full_outer_with_smj_flag_goes_to_sort_merge_not_shuffled_hash() {
        spark = session(/*preferSmj=*/true, /*disableAutoBroadcast=*/true);
        DataFrame joined = peopleDf().join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.FULL);
        assertThat(findJoin(joined.compile())).isInstanceOf(SortMergeJoinExec.class);
    }

    @Test
    void default_off_means_shuffled_hash_still_wins() {
        spark = session(/*preferSmj=*/false, /*disableAutoBroadcast=*/true);
        DataFrame joined = peopleDf().join(ordersDf(), "id");
        assertThat(findJoin(joined.compile())).isInstanceOf(ShuffledHashJoinExec.class);
    }

    // ---------- output matches shuffled-hash ----------

    @Test
    void inner_smj_matches_inner_shuffled() {
        List<Row> smj = runJoin(/*preferSmj=*/true, JoinType.INNER);
        List<Row> sh  = runJoin(/*preferSmj=*/false, JoinType.INNER);
        assertSameMultiset(smj, sh);
        assertThat(smj).hasSize(3);
    }

    @Test
    void left_smj_matches_left_shuffled() {
        List<Row> smj = runJoin(/*preferSmj=*/true, JoinType.LEFT);
        List<Row> sh  = runJoin(/*preferSmj=*/false, JoinType.LEFT);
        assertSameMultiset(smj, sh);
        long carolNullPadded = smj.stream()
                .filter(r -> "carol".equals(r.getString(1)) && r.isNullAt(3)).count();
        assertThat(carolNullPadded).isEqualTo(1);
    }

    @Test
    void right_smj_matches_right_shuffled() {
        List<Row> smj = runJoin(/*preferSmj=*/true, JoinType.RIGHT);
        List<Row> sh  = runJoin(/*preferSmj=*/false, JoinType.RIGHT);
        assertSameMultiset(smj, sh);
        long deskNullPadded = smj.stream()
                .filter(r -> "desk".equals(r.getString(3)) && r.isNullAt(0)).count();
        assertThat(deskNullPadded).isEqualTo(1);
    }

    @Test
    void full_smj_matches_full_shuffled() {
        List<Row> smj = runJoin(/*preferSmj=*/true, JoinType.FULL);
        List<Row> sh  = runJoin(/*preferSmj=*/false, JoinType.FULL);
        assertSameMultiset(smj, sh);
        // carol (no order) and desk (no person) BOTH appear null-padded.
        long carolPadded = smj.stream()
                .filter(r -> "carol".equals(r.getString(1)) && r.isNullAt(3)).count();
        long deskPadded = smj.stream()
                .filter(r -> "desk".equals(r.getString(3)) && r.isNullAt(0)).count();
        assertThat(carolPadded).isEqualTo(1);
        assertThat(deskPadded).isEqualTo(1);
    }

    @Test
    void smj_handles_many_keys_with_duplicates() {
        // 200 left rows × 200 right rows, key = i % 8, so each side has 25 rows
        // per of 8 keys. Inner join expects 8 × 25 × 25 = 5000 result rows.
        spark = session(/*preferSmj=*/true, /*disableAutoBroadcast=*/true);
        List<Row> leftRows = new ArrayList<>();
        List<Row> rightRows = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            leftRows.add(Row.of(i % 8, "L" + i));
            rightRows.add(Row.of(i % 8, "R" + i));
        }
        DataFrame l = spark.createDataFrame(leftRows, people);
        DataFrame r = spark.createDataFrame(rightRows, orders);
        assertThat(l.join(r, "id").count()).isEqualTo(8L * 25 * 25);
    }

    @Test
    void smj_null_join_keys_never_match() {
        spark = session(/*preferSmj=*/true, /*disableAutoBroadcast=*/true);
        DataFrame leftWithNull = spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(null, "ghost")), people);
        DataFrame rightWithNull = spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(null, "void")), orders);
        List<Row> rows = leftWithNull.join(rightWithNull, "id").collect();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("alice");
        assertThat(rows.get(0).getString(3)).isEqualTo("book");
    }

    // ---------- helpers ----------

    private List<Row> runJoin(boolean preferSmj, JoinType jt) {
        spark = session(preferSmj, /*disableAutoBroadcast=*/true);
        try {
            return peopleDf().join(ordersDf(), List.of("id"), List.of("id"), jt).collect();
        } finally {
            spark.close();
            spark = null;
        }
    }

    private static PhysicalPlan findJoin(PhysicalPlan p) {
        if (p instanceof BroadcastHashJoinExec
                || p instanceof ShuffledHashJoinExec
                || p instanceof SortMergeJoinExec) return p;
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
                SortMergeJoinTest::rowKey, java.util.stream.Collectors.counting()));
    }
    private static String rowKey(Row r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < r.size(); i++) sb.append(r.isNullAt(i) ? "NULL" : r.get(i)).append('|');
        return sb.toString();
    }
}
