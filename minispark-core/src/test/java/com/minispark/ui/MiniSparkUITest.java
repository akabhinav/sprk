package com.minispark.ui;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.status.AppStatusStore;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The web UI renders live job/stage/executor state, served over HTTP from the
 * event-sourced {@link AppStatusStore}. We run a two-stage job, then both query
 * the store directly and fetch the rendered HTML to confirm the page reflects
 * what ran.
 */
final class MiniSparkUITest {

    @Test
    void ui_serves_job_and_stage_state() throws Exception {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("ui-demo").setMaster("local[3]")
                        .set("minispark.ui.enabled", "true")
                        .set("minispark.ui.port", "0"))) {   // ephemeral port

            List<String> lines = Arrays.asList("a b a", "b c a", "c c b");
            List<Tuple2<String, Integer>> result = sc.parallelize(lines, 3)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            l -> Arrays.stream(l.split(" ")).iterator())
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();
            assertThat(result).isNotEmpty();

            // Events are dispatched asynchronously; give the bus a moment to drain.
            AppStatusStore store = sc.statusStore();
            waitFor(() -> !store.stages().isEmpty()
                    && store.stages().stream().allMatch(s -> s.status() == AppStatusStore.Status.SUCCEEDED));

            // A reduceByKey job has two stages: a ShuffleMapStage and a ResultStage.
            assertThat(store.stages()).hasSize(2);
            assertThat(store.stages()).anyMatch(s -> s.name().equals("ShuffleMapStage"));
            assertThat(store.stages()).anyMatch(s -> s.name().equals("ResultStage"));
            assertThat(store.jobs()).hasSize(1);
            assertThat(store.executors()).isNotEmpty();

            // Now fetch the actual HTML page.
            int port = sc.uiPort();
            assertThat(port).isGreaterThan(0);
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(resp.statusCode()).isEqualTo(200);
            assertThat(resp.body())
                    .contains("ui-demo")
                    .contains("Stages")
                    .contains("ShuffleMapStage")
                    .contains("ResultStage")
                    .contains("Executors")
                    .contains("DAG visualization");   // nav link to the DAG page

            // The store now records stage-parent edges: the ResultStage depends
            // on the ShuffleMapStage (the shuffle boundary).
            AppStatusStore.StageView resultStage = store.stages().stream()
                    .filter(s -> s.name().equals("ResultStage")).findFirst().orElseThrow();
            AppStatusStore.StageView shuffleStage = store.stages().stream()
                    .filter(s -> s.name().equals("ShuffleMapStage")).findFirst().orElseThrow();
            assertThat(resultStage.parentStageIds()).contains(shuffleStage.stageId());
            assertThat(shuffleStage.parentStageIds()).isEmpty();   // a root stage

            // The /dag page renders an SVG with a node per stage and an edge.
            HttpResponse<String> dag = client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/dag")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(dag.statusCode()).isEqualTo(200);
            assertThat(dag.body())
                    .contains("<svg")
                    .contains("Stage " + shuffleStage.stageId() + " · ShuffleMapStage")
                    .contains("Stage " + resultStage.stageId() + " · ResultStage")
                    .contains("marker-end='url(#arr)'");   // at least one dependency arrow
        }
    }

    @Test
    void dag_page_shows_a_join_as_two_parents_into_the_result_stage() throws Exception {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("ui-join").setMaster("local[3]")
                        .set("minispark.ui.enabled", "true")
                        .set("minispark.ui.port", "0"))) {

            // A join lowers to a cogroup: two ShuffleMapStages feed one ResultStage,
            // so the DAG has three nodes and two edges into the result.
            com.minispark.rdd.RDD<Tuple2<Integer, String>> a =
                    sc.parallelize(List.of(new Tuple2<>(1, "a"), new Tuple2<>(2, "b")), 2);
            com.minispark.rdd.RDD<Tuple2<Integer, String>> b =
                    sc.parallelize(List.of(new Tuple2<>(1, "x"), new Tuple2<>(2, "y")), 2);
            new com.minispark.api.PairRDDFunctions<>(a).join(b).collect();

            AppStatusStore store = sc.statusStore();
            sc.awaitListenerBus(5000);

            AppStatusStore.StageView result = store.stages().stream()
                    .filter(s -> s.name().equals("ResultStage")).findFirst().orElseThrow();
            // The result stage has two shuffle parents (one per joined side).
            assertThat(result.parentStageIds()).hasSize(2);

            int port = sc.uiPort();
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> dag = client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/dag")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(dag.statusCode()).isEqualTo(200);
            // Two ShuffleMapStage nodes + one ResultStage node.
            assertThat(countOccurrences(dag.body(), "· ShuffleMapStage")).isEqualTo(2);
            assertThat(dag.body()).contains("· ResultStage");
            // Two dependency arrows into the result stage.
            assertThat(countOccurrences(dag.body(), "marker-end='url(#arr)'")).isEqualTo(2);
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0, i = 0;
        while ((i = haystack.indexOf(needle, i)) != -1) { count++; i += needle.length(); }
        return count;
    }

    private static void waitFor(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(20);
        }
        throw new AssertionError("condition not met within timeout");
    }
}
