package com.minispark.sql;

import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** DISTINCT, HAVING, and explicit JOIN ... ON in SQL text. */
final class DistinctHavingJoinTest {

    private MiniSparkSession spark;

    @BeforeEach void setUp() {
        spark = MiniSparkSession.builder("dhj-test", "local[3]");

        spark.createDataFrame(List.of(
                Row.of("east", 10), Row.of("east", 20), Row.of("west", 30),
                Row.of("west", 40), Row.of("east", 10)),
                StructType.of(StructField.of("region", DataType.STRING),
                              StructField.of("amount", DataType.INT)))
            .createOrReplaceTempView("sales");

        spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")),
                StructType.of(StructField.of("id", DataType.INT),
                              StructField.of("name", DataType.STRING)))
            .createOrReplaceTempView("people");

        spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(1, "pen"), Row.of(2, "lamp"), Row.of(4, "desk")),
                StructType.of(StructField.of("pid", DataType.INT),
                              StructField.of("item", DataType.STRING)))
            .createOrReplaceTempView("orders");
    }

    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    @Test
    void select_distinct_dedups_rows() {
        // (east,10) appears twice → distinct collapses to one.
        List<Row> rows = spark.sql("SELECT DISTINCT region, amount FROM sales").collect();
        assertThat(rows).hasSize(4);
    }

    @Test
    void distinct_single_column() {
        List<Row> rows = spark.sql("SELECT DISTINCT region FROM sales").collect();
        assertThat(rows).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("east", "west");
    }

    @Test
    void having_filters_after_aggregation() {
        // sum(amount): east=40, west=70. HAVING sum(amount) > 50 keeps only west.
        List<Row> rows = spark.sql(
                "SELECT region, sum(amount) FROM sales GROUP BY region HAVING sum(amount) > 50")
                .collect();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0)).isEqualTo("west");
        assertThat(rows.get(0).getLong(1)).isEqualTo(70L);
    }

    @Test
    void inner_join_on_explicit_keys() {
        List<Row> rows = spark.sql(
                "SELECT name, item FROM people JOIN orders ON id = pid").collect();
        // alice(1): book,pen; bob(2): lamp; carol(3) and desk(4) drop.
        assertThat(rows).extracting(r -> r.getString(0) + ":" + r.getString(1))
                .containsExactlyInAnyOrder("alice:book", "alice:pen", "bob:lamp");
    }

    @Test
    void left_join_on_pads_unmatched() {
        List<Row> rows = spark.sql(
                "SELECT name, item FROM people LEFT JOIN orders ON id = pid").collect();
        // carol has no order → item null.
        Row carol = rows.stream().filter(r -> "carol".equals(r.getString(0))).findFirst().orElseThrow();
        assertThat(carol.isNullAt(1)).isTrue();
    }

    @Test
    void join_then_groupby_counts_orders_per_person() {
        List<Row> rows = spark.sql(
                "SELECT name, count(*) FROM people JOIN orders ON id = pid GROUP BY name").collect();
        Map<String, Long> byName = new HashMap<>();
        for (Row r : rows) byName.put(r.getString(0), r.getLong(1));
        assertThat(byName).containsEntry("alice", 2L).containsEntry("bob", 1L);
        assertThat(byName).doesNotContainKey("carol");
    }
}
