package com.minispark.sql;

import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Equi-joins (inner + outer) lowering to cogroup over the shuffle. */
final class JoinTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("name", DataType.STRING));
    private final StructType orders = StructType.of(
            StructField.of("id", DataType.INT),
            StructField.of("item", DataType.STRING));

    @BeforeEach void setUp() { spark = MiniSparkSession.builder("join-test", "local[3]"); }
    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    private DataFrame peopleDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")), people);
    }
    private DataFrame ordersDf() {
        return spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp"), Row.of(4, "desk")), orders);
    }

    @Test
    void inner_join_keeps_only_matches() {
        List<Row> rows = peopleDf().join(ordersDf(), "id").collect();
        // alice has book+pen, bob has lamp; carol(3) and order id 4 drop out.
        assertThat(rows).hasSize(3);
        // schema: id, name, id, item  (both sides' columns)
        assertThat(rows).allSatisfy(r -> assertThat(r.size()).isEqualTo(4));
        assertThat(rows).extracting(r -> r.getString(1) + ":" + r.getString(3))
                .containsExactlyInAnyOrder("alice:book", "alice:pen", "bob:lamp");
    }

    @Test
    void left_outer_join_pads_unmatched_left_rows() {
        List<Row> rows = peopleDf().join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.LEFT).collect();
        // carol(3) has no order → right side null-padded. alice ×2, bob ×1, carol ×1 = 4 rows.
        assertThat(rows).hasSize(4);
        Row carol = rows.stream().filter(r -> "carol".equals(r.getString(1))).findFirst().orElseThrow();
        assertThat(carol.isNullAt(2)).isTrue();   // right id
        assertThat(carol.isNullAt(3)).isTrue();   // right item
    }

    @Test
    void right_outer_join_pads_unmatched_right_rows() {
        List<Row> rows = peopleDf().join(ordersDf(),
                List.of("id"), List.of("id"), JoinType.RIGHT).collect();
        // order id 4 (desk) has no person → left side null-padded.
        Row desk = rows.stream().filter(r -> "desk".equals(r.getString(3))).findFirst().orElseThrow();
        assertThat(desk.isNullAt(0)).isTrue();   // left id
        assertThat(desk.isNullAt(1)).isTrue();   // left name
    }
}
