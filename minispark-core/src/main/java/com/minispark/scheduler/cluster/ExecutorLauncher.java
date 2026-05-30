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
    /** Start executors, telling them how to reach the driver. */
    void launchExecutors(RpcAddress driverAddress);

    /** Tear down any launched processes/threads. */
    void stop();
}
