package com.minispark.scheduler.cluster;

import com.minispark.rpc.RpcAddress;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.Serializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Launches executors as in-process objects on the driver's own {@link RpcEnv}.
 * They share the driver's {@link com.minispark.executor.SparkEnv} (one JVM, one
 * static singleton), so shuffle data lives in shared heap and needs no network
 * fetch. This is local mode — but it still flows every task through the full
 * RPC message protocol, so the path is identical to the distributed one.
 */
public final class LocalExecutorLauncher implements ExecutorLauncher {

    private final RpcEnv rpcEnv;
    private final Serializer serializer;
    private final int numExecutors;
    private final int coresPerExecutor;
    private final List<CoarseGrainedExecutorBackend> backends = new ArrayList<>();

    public LocalExecutorLauncher(RpcEnv rpcEnv, Serializer serializer,
                                 int numExecutors, int coresPerExecutor) {
        this.rpcEnv = rpcEnv;
        this.serializer = serializer;
        this.numExecutors = numExecutors;
        this.coresPerExecutor = coresPerExecutor;
    }

    @Override
    public void launchExecutors(RpcAddress driverAddress) {
        RpcEndpointRef driverRef = rpcEnv.endpointRef(
                CoarseGrainedSchedulerBackend.ENDPOINT_NAME, driverAddress.host, driverAddress.port);
        for (int i = 0; i < numExecutors; i++) {
            // Constructing the backend registers its endpoint and (via onStart)
            // registers with the driver synchronously.
            backends.add(new CoarseGrainedExecutorBackend(
                    "local-" + i, rpcEnv, driverRef, coresPerExecutor, serializer, false));
        }
    }

    @Override
    public void stop() {
        backends.forEach(CoarseGrainedExecutorBackend::onStop);
    }
}
