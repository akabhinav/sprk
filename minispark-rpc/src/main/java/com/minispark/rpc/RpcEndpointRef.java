package com.minispark.rpc;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * A handle to a (possibly remote) {@link RpcEndpoint}. The same interface is
 * returned by both transports; callers cannot tell whether the target is in
 * this JVM or across a socket. <b>That is the entire point of the seam.</b>
 *
 * <p>Refs are serializable so they can be passed inside messages — e.g. an
 * executor sends the driver a ref to itself so the driver can call back.
 *
 * Real Spark equivalent: org.apache.spark.rpc.RpcEndpointRef
 */
public interface RpcEndpointRef extends Serializable {

    /** Fire-and-forget. Delivered to the endpoint's {@code receive}. */
    void send(Object message);

    /** Blocking request/response. Delivered to the endpoint's {@code receiveAndReply}. */
    <T> T ask(Object message);

    /** Non-blocking request/response. */
    <T> CompletableFuture<T> askAsync(Object message);

    RpcAddress address();
    String name();
}
