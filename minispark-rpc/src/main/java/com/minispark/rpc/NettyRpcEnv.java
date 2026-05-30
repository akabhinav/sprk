package com.minispark.rpc;

import com.minispark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP {@link RpcEnv} with a hand-rolled, length-prefixed framing protocol.
 *
 * <p><b>Why blocking sockets + virtual threads, not a NIO selector.</b> A
 * selector-based reactor scales to tens of thousands of connections on a few
 * OS threads, which is why real Netty uses one. But for a learning engine the
 * blocking model is dramatically easier to read — one virtual thread per
 * connection, straight-line read/write code, no callback soup — and Java 21
 * virtual threads make "thousands of blocked threads" cheap. We trade
 * theoretical scale for clarity and call it out here.
 *
 * <p><b>Framing.</b> Every frame is {@code [int length][length bytes]}, where
 * the bytes are a {@link Serializer}-encoded {@link TransportMessage}.
 *
 * <p><b>Connections.</b> Each env runs a server (accepting inbound) and opens
 * outbound client connections on demand, cached per remote address. A reply
 * travels back on the same connection the request arrived on; an outbound
 * connection has a reader thread that completes pending ask-futures. This is
 * two connections for a bidirectional driver↔executor pair (one each way),
 * simpler than Spark's multiplexed single connection.
 *
 * Real Spark equivalent: org.apache.spark.rpc.netty.NettyRpcEnv
 */
public final class NettyRpcEnv extends RpcEnv {

    private static final Logger LOG = LoggerFactory.getLogger(NettyRpcEnv.class);

    private final String name;
    private final Serializer serializer;
    private final ServerSocket serverSocket;
    private final RpcAddress address;

    private final ConcurrentMap<String, RpcEndpoint> endpoints = new ConcurrentHashMap<>();
    private final ConcurrentMap<RpcAddress, TransportClient> clients = new ConcurrentHashMap<>();
    private final ExecutorService workers =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("netty-rpc-", 0).factory());
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong requestIds = new AtomicLong();

    public NettyRpcEnv(String name, String host, int port, Serializer serializer) {
        this.name = name;
        this.serializer = serializer;
        try {
            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(new InetSocketAddress(host, port));
        } catch (IOException e) {
            throw new RuntimeException("Failed to bind RpcEnv on " + host + ":" + port, e);
        }
        this.address = new RpcAddress(host, serverSocket.getLocalPort());
        workers.submit(this::acceptLoop);
        LOG.info("[{}] NettyRpcEnv listening on {}", name, address);
    }

    @Override
    public RpcEndpointRef setupEndpoint(String endpointName, RpcEndpoint endpoint) {
        endpoints.put(endpointName, endpoint);
        endpoint.onStart();
        LOG.debug("[{}] registered endpoint '{}'", name, endpointName);
        return new NettyRef(endpointName, address);
    }

    @Override
    public RpcEndpointRef endpointRef(String endpointName, String host, int port) {
        return new NettyRef(endpointName, new RpcAddress(host, port));
    }

    @Override public RpcAddress address() { return address; }

    @Override
    public void shutdown() {
        if (!running.compareAndSet(true, false)) return;
        endpoints.values().forEach(RpcEndpoint::onStop);
        clients.values().forEach(TransportClient::close);
        try { serverSocket.close(); } catch (IOException ignored) {}
        workers.shutdownNow();
        LOG.info("[{}] NettyRpcEnv shut down", name);
    }

    @Override public void awaitTermination() {
        try { workers.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ----- server side -----

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket s = serverSocket.accept();
                s.setTcpNoDelay(true);
                workers.submit(() -> serveConnection(s));
            } catch (IOException e) {
                if (running.get()) LOG.warn("[{}] accept failed: {}", name, e.toString());
                return;
            }
        }
    }

    private void serveConnection(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new java.io.BufferedOutputStream(socket.getOutputStream()))) {
            while (running.get()) {
                TransportMessage msg = readFrame(in);
                if (msg == null) return; // peer closed
                // Each request handled on its own vthread so a slow handler
                // doesn't stall pipelined requests on this connection.
                handleInbound(msg, out);
            }
        } catch (EOFException eof) {
            // normal close
        } catch (IOException e) {
            if (running.get()) LOG.debug("[{}] connection closed: {}", name, e.toString());
        }
    }

    private void handleInbound(TransportMessage msg, DataOutputStream out) throws IOException {
        RpcEndpoint endpoint = endpoints.get(msg.endpointName);
        if (endpoint == null) {
            if (msg.expectReply) {
                writeFrame(out, TransportMessage.error(msg.requestId,
                        new RemoteRpcException("No endpoint named '" + msg.endpointName + "'")));
            }
            return;
        }
        if (!msg.expectReply) {
            // one-way: dispatch async, no reply
            workers.submit(() -> {
                try { endpoint.receive(msg.payload); }
                catch (Throwable t) { LOG.warn("[{}] one-way handler failed: {}", name, t.toString()); }
            });
            return;
        }
        // ask: must reply on THIS connection in order, so handle inline.
        try {
            Object reply = endpoint.receiveAndReply(msg.payload);
            writeFrame(out, TransportMessage.reply(msg.requestId, reply));
        } catch (Throwable t) {
            writeFrame(out, TransportMessage.error(msg.requestId,
                    new RemoteRpcException(t.getClass().getName() + ": " + t.getMessage())));
        }
    }

    // ----- client side -----

    private TransportClient clientTo(RpcAddress addr) {
        return clients.computeIfAbsent(addr, a -> new TransportClient(a));
    }

    /** One pooled outbound connection to a remote env, with a reply-reader thread. */
    private final class TransportClient {
        private final RpcAddress addr;
        private final Socket socket;
        private final DataOutputStream out;
        private final ConcurrentMap<Long, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
        private final Object writeLock = new Object();

        TransportClient(RpcAddress addr) {
            this.addr = addr;
            try {
                this.socket = new Socket();
                this.socket.connect(new InetSocketAddress(addr.host, addr.port));
                this.socket.setTcpNoDelay(true);
                this.out = new DataOutputStream(new java.io.BufferedOutputStream(socket.getOutputStream()));
                DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream()));
                workers.submit(() -> readReplies(in));
            } catch (IOException e) {
                throw new RuntimeException("[" + name + "] cannot connect to " + addr, e);
            }
        }

        void sendOneWay(String endpoint, Object payload) {
            write(TransportMessage.oneWay(endpoint, payload));
        }

        CompletableFuture<Object> ask(String endpoint, Object payload) {
            long id = requestIds.incrementAndGet();
            CompletableFuture<Object> f = new CompletableFuture<>();
            pending.put(id, f);
            write(TransportMessage.request(id, endpoint, payload));
            return f;
        }

        private void write(TransportMessage msg) {
            synchronized (writeLock) {
                try {
                    writeFrame(out, msg);
                } catch (IOException e) {
                    throw new RuntimeException("[" + name + "] write to " + addr + " failed", e);
                }
            }
        }

        private void readReplies(DataInputStream in) {
            try {
                while (running.get()) {
                    TransportMessage msg = readFrame(in);
                    if (msg == null) break;
                    CompletableFuture<Object> f = pending.remove(msg.requestId);
                    if (f == null) continue;
                    if (msg.isError) f.completeExceptionally(new RemoteRpcException(String.valueOf(msg.payload)));
                    else f.complete(msg.payload);
                }
            } catch (IOException e) {
                // connection dropped: fail everything outstanding
            } finally {
                pending.values().forEach(f ->
                        f.completeExceptionally(new RemoteRpcException("connection to " + addr + " closed")));
            }
        }

        void close() {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    // ----- framing -----

    private void writeFrame(DataOutputStream out, TransportMessage msg) throws IOException {
        byte[] bytes = serializer.serialize(msg);
        synchronized (out) {
            out.writeInt(bytes.length);
            out.write(bytes);
            out.flush();
        }
    }

    private TransportMessage readFrame(DataInputStream in) throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (EOFException eof) {
            return null;
        }
        if (len < 0 || len > (64 << 20)) throw new IOException("bad frame length " + len);
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return serializer.deserialize(bytes);
    }

    // ----- ref -----

    private final class NettyRef implements RpcEndpointRef {
        private final String endpointName;
        private final RpcAddress addr;

        NettyRef(String endpointName, RpcAddress addr) {
            this.endpointName = endpointName;
            this.addr = addr;
        }

        @Override
        public void send(Object message) {
            if (addr.equals(address)) { localDispatchOneWay(endpointName, message); return; }
            clientTo(addr).sendOneWay(endpointName, message);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T ask(Object message) {
            try {
                return (T) askAsync(message).get();
            } catch (Exception e) {
                Throwable cause = e instanceof java.util.concurrent.ExecutionException ? e.getCause() : e;
                throw new RemoteRpcException("ask to " + endpointName + "@" + addr + " failed: " + cause, cause);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<T> askAsync(Object message) {
            if (addr.equals(address)) return (CompletableFuture<T>) localDispatchAsk(endpointName, message);
            return (CompletableFuture<T>) clientTo(addr).ask(endpointName, message);
        }

        @Override public RpcAddress address() { return addr; }
        @Override public String name() { return endpointName; }
    }

    // Self-addressed messages skip the socket entirely.
    private void localDispatchOneWay(String endpointName, Object message) {
        RpcEndpoint e = endpoints.get(endpointName);
        if (e != null) workers.submit(() -> {
            try { e.receive(message); } catch (Throwable t) { LOG.warn("local one-way failed: {}", t.toString()); }
        });
    }

    private CompletableFuture<Object> localDispatchAsk(String endpointName, Object message) {
        RpcEndpoint e = endpoints.get(endpointName);
        if (e == null) return CompletableFuture.failedFuture(
                new RemoteRpcException("No endpoint named '" + endpointName + "'"));
        return CompletableFuture.supplyAsync(() -> e.receiveAndReply(message), workers);
    }
}
