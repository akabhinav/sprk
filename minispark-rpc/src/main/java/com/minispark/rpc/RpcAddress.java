package com.minispark.rpc;

import java.io.Serializable;
import java.util.Objects;

/**
 * A network location an {@link RpcEnv} listens on, or that an endpoint
 * reference points at.
 *
 * Real Spark equivalent: org.apache.spark.rpc.RpcAddress
 */
public final class RpcAddress implements Serializable {
    public final String host;
    public final int port;

    public RpcAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override public boolean equals(Object o) {
        return o instanceof RpcAddress a && a.host.equals(host) && a.port == port;
    }
    @Override public int hashCode() { return Objects.hash(host, port); }
    @Override public String toString() { return host + ":" + port; }
}
