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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** spark.sql("SELECT ...") end-to-end: parse → analyze → optimize → run. */
final class SqlParserTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("name", DataType.STRING),
            StructField.of("age", DataType.INT),
            StructField.of("city", DataType.STRING));

    @BeforeEach void setUp() {
        spark = MiniSparkSession.builder("sql-parser-test", "local[2]");
        DataFrame df = spark.createDataFrame(List.of(
                Row.of("alice", 30, "NYC"),
                Row.of("bob", 25, "LA"),
                Row.of("carol", 40, "NYC"),
                Row.of("dave", 19, "SF")), people);
        df.createOrReplaceTempView("people");
    }

    @AfterEach void tearDown() { if (spark != null) spark.close(); }

    @Test
    void select_with_where() {
        List<Row> rows = spark.sql(
                "SELECT name, age FROM people WHERE age >= 30").collect();
        assertThat(rows).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("alice", "carol");
        assertThat(rows).allSatisfy(r -> assertThat(r.size()).isEqualTo(2));
    }

    @Test
    void arithmetic_and_alias_in_projection() {
        List<Row> rows = spark.sql(
                "SELECT name, age + 1 AS next FROM people WHERE city = 'NYC'").collect();
        Map<String, Long> nextByName = new HashMap<>();
        for (Row r : rows) nextByName.put(r.getString(0), r.getLong(1));
        assertThat(nextByName).containsEntry("alice", 31L).containsEntry("carol", 41L);
    }

    @Test
    void compound_predicate_precedence() {
        // AND binds tighter than OR: city='SF' OR (city='NYC' AND age>30)
        List<Row> rows = spark.sql(
                "SELECT name FROM people WHERE city = 'SF' OR city = 'NYC' AND age > 30").collect();
        assertThat(rows).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("dave", "carol"); // dave(SF), carol(NYC,40)
    }

    @Test
    void group_by_with_aggregates() {
        List<Row> rows = spark.sql(
                "SELECT city, count(*), sum(age) FROM people GROUP BY city").collect();
        Map<String, Long> countByCity = new HashMap<>();
        Map<String, Long> sumByCity = new HashMap<>();
        for (Row r : rows) {
            countByCity.put(r.getString(0), r.getLong(1));
            sumByCity.put(r.getString(0), r.getLong(2));
        }
        assertThat(countByCity).containsEntry("NYC", 2L).containsEntry("LA", 1L).containsEntry("SF", 1L);
        assertThat(sumByCity).containsEntry("NYC", 70L); // 30 + 40
    }

    @Test
    void unknown_table_fails() {
        assertThatThrownBy(() -> spark.sql("SELECT a FROM nope").collect())
                .hasMessageContaining("no such table");
    }

    @Test
    void unknown_column_fails() {
        assertThatThrownBy(() -> spark.sql("SELECT bogus FROM people").collect())
                .hasMessageContaining("bogus");
    }

    @Test
    void syntax_error_is_reported() {
        assertThatThrownBy(() -> spark.sql("SELECT FROM people"))
                .isInstanceOf(com.minispark.sql.parser.ParseException.class);
    }
}
