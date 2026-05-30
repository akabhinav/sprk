package com.minispark.rpc;

/**
 * A message handler registered on an {@link RpcEnv} under a name. Other
 * components reach it through an {@link RpcEndpointRef}.
 *
 * <p>Two delivery modes, mirroring real Spark:
 * <ul>
 *   <li>{@link #receive} — fire-and-forget ({@code ref.send}). No reply.</li>
 *   <li>{@link #receiveAndReply} — request/response ({@code ref.ask}). The
 *       returned value is sent back to the caller.</li>
 * </ul>
 *
 * <p>Handlers should be quick and non-blocking where possible; the env runs
 * each delivery on its own (virtual) thread, but an endpoint that blocks for
 * a long time still holds that thread.
 *
 * Real Spark equivalent: org.apache.spark.rpc.RpcEndpoint
 */
public interface RpcEndpoint {

    /** Handle a one-way message. Default: ignore. */
    default void receive(Object message) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not handle one-way message: " + message);
    }

    /** Handle a request and return the reply. Default: unsupported. */
    default Object receiveAndReply(Object message) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not handle ask: " + message);
    }

    /** Called once after the endpoint is registered. */
    default void onStart() {}

    /** Called once when the env shuts down. */
    default void onStop() {}
}
