package com.minispark.scheduler.cluster;

import com.minispark.rpc.RpcAddress;

/**
 * Strategy for bringing executors into existence. This is the one piece that
 * genuinely differs between local mode and a real cluster: the backend and the
 * scheduler above it are identical either way.
 *
 * Real Spark equivalent: the executor-launch paths inside the various
 * SchedulerBackends (local vs standalone vs YARN). Phase 5's MiniYarn provides
 * a NodeManager-backed launcher.
 */
public interface ExecutorLauncher {
    /** Start the initial executors, telling them how to reach the driver. */
    void launchExecutors(RpcAddress driverAddress);

    /**
     * Dynamic allocation: launch {@code n} additional executors. Default throws,
     * so a launcher that can't grow at runtime fails loudly if asked. Returns the
     * ids of the executors it started.
     */
    default java.util.List<String> requestExecutors(int n) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support dynamic executor requests");
    }

    /** Dynamic allocation: stop a specific executor. Default no-op. */
    default void killExecutor(String executorId) {}

    /** Whether this launcher supports {@link #requestExecutors}/{@link #killExecutor}. */
    default boolean supportsDynamicAllocation() { return false; }

    /** Tear down any launched processes/threads. */
    void stop();
}
