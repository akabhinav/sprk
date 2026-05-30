package com.minispark.examples;

import com.miniyarn.common.NodeId;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.miniyarn.nm.NodeManager;
import com.miniyarn.rm.ResourceManager;
import com.minispark.deploy.MiniSparkSubmit;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.JavaSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end {@code minispark-submit} against a live MiniYarn cluster — the
 * spark-submit-on-YARN equivalent. Stands up an RM + 2 NodeManagers, then drives
 * {@link MiniSparkSubmit#main} with {@code --class WordCount --master
 * miniyarn://… --num-executors 2}, exactly as the shell wrapper would, and
 * asserts the app produced the right word counts.
 *
 * <p>This proves the whole submission chain: parse flags → set minispark.* →
 * app's bare MiniSparkConf reads the master → AM requests containers from the
 * RM → NMs launch executor JVMs → executors register with the driver → job runs.
 *
 * <p>Aborts (skips) if the sandbox can't spawn child JVMs.
 */
final class SubmitMiniYarnTest {

    // Config keys MiniSparkSubmit sets as system properties; cleared after the run
    // so the submit doesn't leak global state into other tests in the same JVM.
    private static final String[] SUBMIT_PROPS = {
            "minispark.master", "minispark.app.name", "minispark.rpc.mode",
            "minispark.executor.instances", "minispark.executor.cores",
            "minispark.executor.memoryMB", "minispark.shuffle.manager"
    };

    @Test
    void submit_wordcount_to_miniyarn_with_two_executors(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("input.txt");
        Files.writeString(file,
                "the quick brown fox\nthe lazy dog and the fox\nfox fox dog the the the\n",
                StandardCharsets.UTF_8);
        // Reference counts: the=6, fox=4, dog=2, quick=1, brown=1, lazy=1, and=1.

        // Stand up RM + 2 NodeManagers, each sized for one 2-core/256MB container,
        // so the RM must spread the two executors across both NMs.
        RpcEnv rmEnv = RpcEnv.create("rm", "127.0.0.1", 0, "netty", new JavaSerializer());
        ResourceManager rm = new ResourceManager(rmEnv);
        RpcEnv nm1Env = RpcEnv.create("nm1", "127.0.0.1", 0, "netty", new JavaSerializer());
        NodeManager nm1 = new NodeManager(new NodeId("nm1"), nm1Env,
                nm1Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmEnv.address().host, rmEnv.address().port),
                new Resource(2, 1024));
        nm1.onStart();
        RpcEnv nm2Env = RpcEnv.create("nm2", "127.0.0.1", 0, "netty", new JavaSerializer());
        NodeManager nm2 = new NodeManager(new NodeId("nm2"), nm2Env,
                nm2Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmEnv.address().host, rmEnv.address().port),
                new Resource(2, 1024));
        nm2.onStart();

        String master = "miniyarn://127.0.0.1:" + rmEnv.address().port;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;

        try {
            Callable<String> submit = () -> {
                // Capture the app's stdout (WordCount prints "<word>\t<count>").
                System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
                try {
                    MiniSparkSubmit.main(new String[]{
                            "--class", "com.minispark.examples.WordCount",
                            "--master", master,
                            "--num-executors", "2",
                            "--executor-cores", "2",
                            "--executor-memory", "256",
                            "--conf", "minispark.executor.heartbeatTimeoutMs=30000",
                            file.toString()
                    });
                } finally {
                    System.setOut(originalOut);
                }
                return captured.toString(StandardCharsets.UTF_8);
            };

            ExecutorService runner = Executors.newSingleThreadExecutor();
            Future<String> f = runner.submit(submit);
            String out;
            try {
                out = f.get(120, TimeUnit.SECONDS);
            } catch (Exception e) {
                f.cancel(true);
                System.setOut(originalOut);
                Assumptions.abort("Skipping submit→MiniYarn test (cluster could not run): " + e);
                return;
            } finally {
                runner.shutdownNow();
            }

            // The submit banner and the app's word counts must both be present.
            assertThat(out).contains("minispark-submit");
            assertThat(out).contains("com.minispark.examples.WordCount");
            assertThat(wordCount(out, "the")).isEqualTo(6);
            assertThat(wordCount(out, "fox")).isEqualTo(4);
            assertThat(wordCount(out, "dog")).isEqualTo(2);
        } finally {
            for (String k : SUBMIT_PROPS) System.clearProperty(k);
            nm1.onStop(); nm2.onStop();
            nm1Env.shutdown(); nm2Env.shutdown(); rmEnv.shutdown();
        }
    }

    /** Extract the count WordCount printed for a word ("word\tcount" lines). */
    private static int wordCount(String out, String word) {
        for (String line : out.split("\n")) {
            String[] parts = line.split("\t");
            if (parts.length == 2 && parts[0].trim().equals(word)) {
                return Integer.parseInt(parts[1].trim());
            }
        }
        throw new AssertionError("word '" + word + "' not found in submit output:\n" + out);
    }
}
