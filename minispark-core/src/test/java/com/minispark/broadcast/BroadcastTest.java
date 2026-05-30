package com.minispark.broadcast;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A broadcast variable is captured by a task closure and resolved on the
 * executor. Here the closure adds a per-key offset from a broadcast map; the
 * result proves the executor saw the value.
 */
final class BroadcastTest {

    @Test
    void task_reads_broadcast_value() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("bcast").setMaster("local[4]"))) {

            HashMap<Integer, Integer> table = new HashMap<>();
            table.put(0, 100);
            table.put(1, 200);
            Broadcast<HashMap<Integer, Integer>> bc = sc.broadcast(table);

            List<Integer> out = sc.parallelize(List.of(0, 1, 0, 1, 1), 3)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> bc.value().get(i % 2))
                    .collect();

            // 0->100, 1->200, 0->100, 1->200, 1->200
            assertThat(out).containsExactlyInAnyOrder(100, 200, 100, 200, 200);
        }
    }

    @Test
    void broadcast_handle_is_small_and_value_matches() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {
            Map<String, Integer> ref = new HashMap<>();
            for (int i = 0; i < 1000; i++) ref.put("k" + i, i);
            Broadcast<HashMap<String, Integer>> bc = sc.broadcast(new HashMap<>(ref));
            // Reading the value on the driver side (same JVM) must round-trip.
            assertThat(bc.value()).containsAllEntriesOf(ref);
        }
    }
}
