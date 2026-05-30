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

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the bugs the code review found and we fixed:
 * pushdown-ordinal correctness, aggregate-in-expression and aggregate-in-
 * ORDER-BY routing, GROUP-BY column validity rules, HAVING-not-in-SELECT,
 * and join duplicate-column-name deconfliction.
 */
final class PostReviewFixesTest {

    private MiniSparkSession spark;

    @BeforeEach void setUp() {
        spark = MiniSparkSession.builder("post-review", "local[3]");

        spark.createDataFrame(List.of(
                Row.of("alice", 30, "NYC"), Row.of("bob", 25, "LA"),
                Row.of("carol", 40, "NYC"), Row.of("dave", 19, "SF")),
                StructType.of(StructField.of("name", DataType.STRING),
                              StructField.of("age", DataType.INT),
                              StructField.of("city", DataType.STRING)))
            .createOrReplaceTempView("people");

        spark.createDataFrame(List.of(
                Row.of("east", 10), Row.of("east", 20),
                Row.of("west", 30), Row.of("west", 40)),
                StructType.of(StructField.of("region", DataType.STRING),
                              StructField.of("amount", DataType.INT)))
            .createOrReplaceTempView("sales");

        spark.createDataFrame(List.of(
                Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol")),
                StructType.of(StructField.of("id", DataType.INT),
                              StructField.of("name", DataType.STRING)))
            .createOrReplaceTempView("p");
        spark.createDataFrame(List.of(
                Row.of(1, "book"), Row.of(2, "lamp")),
                StructType.of(StructField.of("id", DataType.INT),
                              StructField.of("item", DataType.STRING)))
            .createOrReplaceTempView("o");
    }
    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    // ----- #1: PushDownFilter must not corrupt ordinals when columns are reordered -----

    @Test
    void pushdown_through_reordering_projection_still_filters_the_right_column() {
        // city,name,age — reordering vs the source (name,age,city). A pushed Filter
        // on 'age' would, if naively kept, read the projection's column 2 (city)
        // from the child row, which is 'city' — silently wrong.
        DataFrame df = spark.sql("SELECT city, name, age FROM people")
                .filter(col("age").ge(30));
        List<Row> rows = df.collect();
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getString(1))   // 'name' is column 1
                .containsExactlyInAnyOrder("alice", "carol");
    }

    @Test
    void pushdown_through_subsetting_projection() {
        // Project drops 'city' entirely; filter on remaining 'age' must still work.
        DataFrame df = spark.sql("SELECT name, age FROM people").filter(col("age").lt(28));
        assertThat(df.collect()).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("bob", "dave");
    }

    // ----- #4 / #5: aggregates inside arithmetic and inside ORDER BY -----

    @Test
    void aggregate_inside_select_expression() {
        // sum(amount) + 1 was previously not detected as an aggregate, so the
        // AggMarker reached ProjectExec and threw at evaluation time.
        Map<String, Long> got = collectMap2(
                spark.sql("SELECT region, sum(amount) + 1 FROM sales GROUP BY region"));
        assertThat(got).containsEntry("east", 31L).containsEntry("west", 71L);  // 30+1, 70+1
    }

    @Test
    void aggregate_in_order_by() {
        // ORDER BY sum(amount) should sort groups by their sum.
        List<Row> rows = spark.sql(
                "SELECT region, sum(amount) FROM sales GROUP BY region ORDER BY sum(amount) DESC")
                .collect();
        assertThat(rows).extracting(r -> r.getString(0)).containsExactly("west", "east");
    }

    // ----- #8: GROUP-BY column validity -----

    @Test
    void non_grouped_non_aggregate_column_with_group_by_is_rejected() {
        // 'name' is in SELECT but neither aggregated nor in GROUP BY — must error,
        // not silently drop the column.
        assertThatThrownBy(() ->
                spark.sql("SELECT region, name FROM people GROUP BY region").collect())
                .hasMessageContaining("GROUP BY");
    }

    @Test
    void aggregate_without_group_by_with_extra_column_is_rejected() {
        // Mixing a bare column with an aggregate and no GROUP BY was silently
        // converted to GROUP BY name. SQL says this should be an error.
        assertThatThrownBy(() ->
                spark.sql("SELECT name, sum(age) FROM people").collect())
                .hasMessageContaining("GROUP BY");
    }

    // ----- #9: HAVING aggregate not in SELECT -----

    @Test
    void having_can_reference_an_aggregate_not_in_select() {
        // Previously HAVING was rewritten to UnresolvedAttribute("count(*)") and
        // the analyzer failed because count(*) wasn't an Aggregate output column.
        // Now SELECT/HAVING/ORDER BY all share one aggregate list.
        List<Row> rows = spark.sql(
                "SELECT region FROM sales GROUP BY region HAVING count(*) > 1").collect();
        assertThat(rows).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("east", "west");
    }

    // ----- #10: Join schema deconflicts duplicate column names -----

    @Test
    void join_with_same_named_columns_keeps_both_accessible_by_name() {
        // Both p and o have 'id' and one has 'name'/'item'. After join, the
        // duplicated 'id' from the right side becomes 'id_2'; the left 'id' stays
        // 'id' and a by-name reference now consistently picks the left.
        StructType s = spark.sql("SELECT * FROM p JOIN o ON id = id").schema();
        assertThat(s.names()).contains("id", "id_2");
    }

    // ----- helpers -----

    private static Map<String, Long> collectMap2(DataFrame df) {
        Map<String, Long> m = new HashMap<>();
        for (Row r : df.collect()) m.put(r.getString(0), r.getLong(1));
        return m;
    }
}
