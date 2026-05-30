package com.minispark.rpc;

import com.minispark.serializer.JavaSerializer;
import com.minispark.serializer.Serializer;

/**
 * The messaging seam. {@link #setupEndpoint} publishes a local handler;
 * {@link #endpointRef} obtains a handle to a (possibly remote) one. Two
 * implementations:
 * <ul>
 *   <li>{@link LocalRpcEnv} — in-process delivery, used by local mode and tests.</li>
 *   <li>{@link NettyRpcEnv} — length-prefixed framing over TCP, used for real
 *       driver↔executor distribution across JVMs/machines.</li>
 * </ul>
 *
 * <p>Nothing above this interface (scheduler, backend, block manager) knows
 * which transport is in use.
 *
 * Real Spark equivalent: org.apache.spark.rpc.RpcEnv
 */
public abstract class RpcEnv {

    /** Publish {@code endpoint} under {@code name} and return a ref to it. */
    public abstract RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint);

    /** A ref to an endpoint named {@code name} hosted at {@code host:port}. */
    public abstract RpcEndpointRef endpointRef(String name, String host, int port);

    /** This env's listening address. */
    public abstract RpcAddress address();

    public abstract void shutdown();

    /** Blocks until the env has fully stopped. */
    public abstract void awaitTermination();

    /**
     * Factory. {@code mode} is {@code "local"} or {@code "netty"}. For netty,
     * {@code port == 0} binds an ephemeral port (read it back via {@link #address}).
     */
    public static RpcEnv create(String name, String host, int port, String mode, Serializer serializer) {
        Serializer ser = serializer != null ? serializer : new JavaSerializer();
        return switch (mode) {
            case "local" -> new LocalRpcEnv(name);
            case "netty" -> new NettyRpcEnv(name, host, port, ser);
            default -> throw new IllegalArgumentException("Unknown rpc mode: " + mode);
        };
    }
}
