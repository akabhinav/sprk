package com.minispark.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * Where an executor lives. In Phase 2 there is just one in-process executor,
 * so we use {@code ("local", 0)} sentinel; Phase 5 fills in real host:port.
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockManagerId
 */
public final class ExecutorLocation implements Serializable {
    public static final ExecutorLocation LOCAL = new ExecutorLocation("local", 0);

    public final String host;
    public final int port;

    public ExecutorLocation(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override public boolean equals(Object o) {
        return o instanceof ExecutorLocation e && e.host.equals(host) && e.port == port;
    }
    @Override public int hashCode() { return Objects.hash(host, port); }
    @Override public String toString() { return host + ":" + port; }
}
