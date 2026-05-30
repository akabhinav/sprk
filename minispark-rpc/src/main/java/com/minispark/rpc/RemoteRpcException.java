package com.minispark.rpc;

/**
 * Thrown when an RPC fails — no such endpoint, a remote handler threw, or the
 * connection dropped. The remote stack trace cannot be reconstructed across
 * the wire, so the message carries the remote exception's class and text.
 *
 * Real Spark equivalent: org.apache.spark.rpc.RpcException family.
 */
public final class RemoteRpcException extends RuntimeException {
    public RemoteRpcException(String message) { super(message); }
    public RemoteRpcException(String message, Throwable cause) { super(message, cause); }
}
