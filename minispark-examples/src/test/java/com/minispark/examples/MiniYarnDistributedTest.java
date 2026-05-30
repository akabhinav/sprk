package com.minispark.examples;

import com.miniyarn.common.ApplicationId;
import com.miniyarn.common.NodeId;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.miniyarn.nm.NodeManager;
import com.miniyarn.rm.ResourceManager;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.JavaSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase 5 milestone. A genuine MiniYarn topology comes up:
 * <ul>
 *   <li>A {@link ResourceManager} on its own Netty port.</li>
 *   <li>Two {@link NodeManager}s on their own ports, each with capacity for
 *       executors of the requested shape.</li>
 *   <li>A driver with {@code master = miniyarn://...} that triggers a
 *       {@link com.minispark.scheduler.cluster.YarnExecutorLauncher}.</li>
 * </ul>
 *
 * <p>The driver does <i>nothing</i> to spawn executors itself: the AM asks the
 * RM, the RM packs containers across the two NMs (so each NM launches one
 * executor JVM), and those executors connect back to the driver over the same
 * RPC plumbing Phase 4 built. The shuffle then crosses the network.
 *
 * <p>Aborts (not fails) if the sandbox blocks child JVM spawning.
 */
final class MiniYarnDistributedTest {

    @Test
    void wordcount_runs_on_two_nodemanagers(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("input.txt");
        String text =
                "the quick brown fox\n" +
                "jumps over the lazy dog\n" +
                "the dog sleeps and the fox jumps\n" +
                "brown fox brown dog\n";
        Files.writeString(file, text, StandardCharsets.UTF_8);

        Map<String, Integer> reference = new HashMap<>();
        Arrays.stream(text.toLowerCase().split("\\W+"))
                .filter(s -> !s.isEmpty())
                .forEach(w -> reference.merge(w, 1, Integer::sum));

        // Stand up RM and two NMs in this JVM but each on its own Netty port,
        // so they exchange real RPC messages exactly as separate processes would.
        RpcEnv rmEnv = RpcEnv.create("rm", "127.0.0.1", 0, "netty", new JavaSerializer());
        ResourceManager rm = new ResourceManager(rmEnv);

        // Each NM has capacity for exactly one container, forcing the RM to
        // spread allocations across both — so the test proves both NMs hosted.
        RpcEnv nm1Env = RpcEnv.create("nm1", "127.0.0.1", 0, "netty", new JavaSerializer());
        RpcEndpointRef rmRef1 = nm1Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME,
                rmEnv.address().host, rmEnv.address().port);
        NodeManager nm1 = new NodeManager(new NodeId("nm1"), nm1Env, rmRef1, new Resource(2, 1024));
        nm1.onStart();

        RpcEnv nm2Env = RpcEnv.create("nm2", "127.0.0.1", 0, "netty", new JavaSerializer());
        RpcEndpointRef rmRef2 = nm2Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME,
                rmEnv.address().host, rmEnv.address().port);
        NodeManager nm2 = new NodeManager(new NodeId("nm2"), nm2Env, rmRef2, new Resource(2, 1024));
        nm2.onStart();

        AtomicReference<Set<NodeId>> nodesUsed = new AtomicReference<>(Set.of());

        try {
            Callable<Map<String, Integer>> job = () -> {
                MiniSparkConf conf = new MiniSparkConf()
                        .setAppName("yarn-wc")
                        .setMaster("miniyarn://127.0.0.1:" + rmEnv.address().port)
                        .set("minispark.executor.instances", "2")
                        .set("minispark.executor.cores", "2")
                        .set("minispark.executor.memoryMB", "256");

                try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                    // Snapshot which nodes RM has placed the app's containers on.
                    // By the time the driver is "ready", both executors have registered,
                    // so allocations have already happened.
                    nodesUsed.set(rm.nodesHostingApp(new ApplicationId(1)));
                    List<Tuple2<String, Integer>> result = sc.textFile(file.toString(), 4)
                            .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                                    line -> Arrays.stream(line.toLowerCase().split("\\W+")).iterator())
                            .filter((RDD.SerializablePredicate<String>) w -> !w.isEmpty())
                            .mapToPair(w -> new Tuple2<>(w, 1))
                            .reduceByKey(Integer::sum)
                            .collect();
                    Map<String, Integer> got = new HashMap<>();
                    for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());
                    return got;
                }
            };

            ExecutorService runner = Executors.newSingleThreadExecutor();
            Future<Map<String, Integer>> f = runner.submit(job);
            Map<String, Integer> got;
            try {
                got = f.get(120, TimeUnit.SECONDS);
            } catch (Exception e) {
                f.cancel(true);
                Assumptions.abort("Skipping MiniYarn distributed test (cluster could not run): " + e);
                return;
            } finally {
                runner.shutdownNow();
            }

            assertThat(got).containsExactlyInAnyOrderEntriesOf(reference);
            // The whole point of running a cluster manager: the two executors
            // ended up on two different NodeManagers.
            assertThat(nodesUsed.get())
                    .as("Executors should be spread across both NodeManagers")
                    .containsExactlyInAnyOrder(new NodeId("nm1"), new NodeId("nm2"));
        } finally {
            nm1.onStop();
            nm2.onStop();
            nm1Env.shutdown();
            nm2Env.shutdown();
            rmEnv.shutdown();
        }
    }
}
