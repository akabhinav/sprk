package com.minispark.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * In-process {@link RpcEnv}: endpoints live in a name→handler map and messages
 * are delivered by direct method calls (one-way sends are dispatched on a
 * virtual-thread pool so a {@code send} never blocks on the handler, matching
 * the asynchrony callers see over the network).
 *
 * <p>No serialization happens here — local messages are passed by reference,
 * exactly as real Spark optimizes the same-JVM case. Code that must be wire-
 * safe is still exercised by {@link NettyRpcEnv} in tests.
 *
 * Real Spark equivalent: the in-process dispatch path of NettyRpcEnv.
 */
public final class LocalRpcEnv extends RpcEnv {

    private static final Logger LOG = LoggerFactory.getLogger(LocalRpcEnv.class);

    private final String name;
    private final ConcurrentMap<String, RpcEndpoint> endpoints = new ConcurrentHashMap<>();
    private final ExecutorService dispatcher =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("local-rpc-", 0).factory());

    public LocalRpcEnv(String name) { this.name = name; }

    @Override
    public RpcEndpointRef setupEndpoint(String endpointName, RpcEndpoint endpoint) {
        endpoints.put(endpointName, endpoint);
        endpoint.onStart();
        LOG.debug("[{}] registered endpoint '{}'", name, endpointName);
        return new LocalRef(endpointName);
    }

    @Override
    public RpcEndpointRef endpointRef(String endpointName, String host, int port) {
        // Host/port are irrelevant in-process; lookup is by name.
        return new LocalRef(endpointName);
    }

    @Override public RpcAddress address() { return new RpcAddress("local", 0); }

    @Override
    public void shutdown() {
        endpoints.values().forEach(RpcEndpoint::onStop);
        endpoints.clear();
        dispatcher.shutdown();
    }

    @Override public void awaitTermination() { /* nothing to wait for */ }

    private RpcEndpoint lookup(String endpointName) {
        RpcEndpoint e = endpoints.get(endpointName);
        if (e == null) throw new IllegalStateException("No endpoint named '" + endpointName + "'");
        return e;
    }

    /** Serializable so it can travel inside messages, like the netty ref. */
    private final class LocalRef implements RpcEndpointRef {
        private final String endpointName;
        LocalRef(String endpointName) { this.endpointName = endpointName; }

        @Override
        public void send(Object message) {
            dispatcher.submit(() -> {
                try {
                    lookup(endpointName).receive(message);
                } catch (Throwable t) {
                    LOG.warn("[{}] endpoint '{}' failed on send: {}", name, endpointName, t.toString());
                }
            });
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T ask(Object message) {
            // Direct call: synchronous request/response.
            return (T) lookup(endpointName).receiveAndReply(message);
        }

        @Override
        public <T> CompletableFuture<T> askAsync(Object message) {
            return CompletableFuture.supplyAsync(() -> ask(message), dispatcher);
        }

        @Override public RpcAddress address() { return new RpcAddress("local", 0); }
        @Override public String name() { return endpointName; }
    }
}
