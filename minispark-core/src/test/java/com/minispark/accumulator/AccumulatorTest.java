package com.minispark.accumulator;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An accumulator captured by a task closure should sum across every task that
 * touches it, with the final total visible on the driver.
 */
final class AccumulatorTest {

    @Test
    void executor_increments_are_summed_on_driver() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]"))) {

            Accumulator<Long> counter = sc.longAccumulator("hits");

            List<Integer> total = sc.parallelize(java.util.stream.IntStream.rangeClosed(1, 100).boxed().toList(), 4)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                        counter.add(1L);
                        return i * i;
                    })
                    .collect();

            assertThat(total).hasSize(100);
            assertThat(counter.value()).isEqualTo(100L);
        }
    }

    @Test
    void double_accumulator_works() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {

            Accumulator<Double> sum = sc.doubleAccumulator("sum");
            sc.parallelize(List.of(1.5, 2.5, 3.0), 2)
                    .foreach(sum::add);
            assertThat(sum.value()).isEqualTo(7.0);
        }
    }
}
