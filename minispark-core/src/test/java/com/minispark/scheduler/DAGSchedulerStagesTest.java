package com.minispark.scheduler;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the DAGScheduler cuts stage boundaries at every ShuffleDependency
 * and only there. This is the core conceptual invariant of Spark.
 */
final class DAGSchedulerStagesTest {

    private static final ch.qos.logback.classic.Logger DAG_LOG =
            (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(DAGScheduler.class);

    @Test
    void narrow_only_pipeline_has_one_stage() {
        StageCounter counter = StageCounter.attach();
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {

            sc.parallelize(List.of(1, 2, 3, 4), 2)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> i + 1)
                    .filter((RDD.SerializablePredicate<Integer>) i -> i > 0)
                    .count();
        } finally {
            counter.detach();
        }
        assertThat(counter.resultStages).isEqualTo(1);
        assertThat(counter.mapStages).isZero();
    }

    @Test
    void one_shuffle_yields_one_map_stage_plus_result_stage() {
        StageCounter counter = StageCounter.attach();
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {

            sc.parallelize(List.of("a", "b", "a"), 2)
                    .mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey(Integer::sum)
                    .collect();
        } finally {
            counter.detach();
        }
        assertThat(counter.resultStages).isEqualTo(1);
        assertThat(counter.mapStages).isEqualTo(1);
    }

    /** Watches DAGScheduler's INFO logs to count submitted stages. */
    private static final class StageCounter extends ch.qos.logback.core.AppenderBase<
            ch.qos.logback.classic.spi.ILoggingEvent> {
        int mapStages = 0;
        int resultStages = 0;

        static StageCounter attach() {
            StageCounter c = new StageCounter();
            c.setContext(DAG_LOG.getLoggerContext());
            c.start();
            DAG_LOG.addAppender(c);
            return c;
        }

        void detach() {
            DAG_LOG.detachAppender(this);
        }

        @Override
        protected void append(ch.qos.logback.classic.spi.ILoggingEvent eventObject) {
            String msg = eventObject.getFormattedMessage();
            if (msg.startsWith("Submitting ShuffleMapStage")) mapStages++;
            else if (msg.startsWith("Submitting ResultStage")) resultStages++;
        }
    }
}
