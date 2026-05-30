package com.minispark.sql;

import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/** ORDER BY, LIMIT, and SELECT * — via both the DSL and SQL text. */
final class SortLimitStarTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("name", DataType.STRING),
            StructField.of("age", DataType.INT));

    @BeforeEach void setUp() {
        spark = MiniSparkSession.builder("sort-test", "local[3]");
        spark.createDataFrame(List.of(
                Row.of("alice", 30),
                Row.of("bob", 25),
                Row.of("carol", 40),
                Row.of("dave", 19),
                Row.of("erin", 30)), people)
            .createOrReplaceTempView("people");
    }

    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    private DataFrame people() {
        return spark.sql("SELECT name, age FROM people");
    }

    @Test
    void order_by_ascending_is_total_across_partitions() {
        List<Row> rows = people().orderBy(col("age")).collect();
        // Ages ascending: 19, 25, 30, 30, 40
        assertThat(rows).extracting(r -> r.getInt(1))
                .containsExactly(19, 25, 30, 30, 40);
    }

    @Test
    void sql_order_by_desc() {
        List<Row> rows = spark.sql("SELECT name, age FROM people ORDER BY age DESC").collect();
        assertThat(rows).extracting(r -> r.getInt(1))
                .containsExactly(40, 30, 30, 25, 19);
    }

    @Test
    void sql_order_by_multi_column() {
        // Sort by age ASC, then name DESC: the two age-30 rows order erin before alice.
        List<Row> rows = spark.sql(
                "SELECT name, age FROM people ORDER BY age ASC, name DESC").collect();
        assertThat(rows).extracting(r -> r.getString(0))
                .containsExactly("dave", "bob", "erin", "alice", "carol");
    }

    @Test
    void limit_caps_row_count() {
        assertThat(people().limit(2).collect()).hasSize(2);
        assertThat(spark.sql("SELECT name, age FROM people LIMIT 3").collect()).hasSize(3);
    }

    @Test
    void order_by_then_limit_is_top_n() {
        List<Row> rows = spark.sql(
                "SELECT name, age FROM people ORDER BY age DESC LIMIT 2").collect();
        assertThat(rows).extracting(r -> r.getInt(1)).containsExactly(40, 30);
    }

    @Test
    void select_star_expands_all_columns() {
        List<Row> rows = spark.sql("SELECT * FROM people WHERE age >= 40").collect();
        assertThat(rows).hasSize(1);
        Row carol = rows.get(0);
        assertThat(carol.size()).isEqualTo(2);            // name + age
        assertThat(carol.getString(0)).isEqualTo("carol");
        assertThat(carol.getInt(1)).isEqualTo(40);
    }

    @Test
    void select_star_schema_matches_source() {
        StructType s = spark.sql("SELECT * FROM people").schema();
        assertThat(s.names()).containsExactly("name", "age");
    }
}
