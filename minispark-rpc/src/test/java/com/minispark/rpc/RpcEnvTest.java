package com.minispark.rpc;

import com.minispark.serializer.JavaSerializer;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The same behavioural contract must hold for both transports. Each test runs
 * against LocalRpcEnv and against two real NettyRpcEnv instances talking over
 * loopback TCP.
 */
final class RpcEnvTest {

    record Ping(String text) implements Serializable {}
    record Pong(String text) implements Serializable {}

    /** Echoes asks; records one-way sends into a latch. */
    static final class EchoEndpoint implements RpcEndpoint {
        final CountDownLatch gotOneWay = new CountDownLatch(1);
        final AtomicReference<Object> lastOneWay = new AtomicReference<>();

        @Override public void receive(Object message) {
            lastOneWay.set(message);
            gotOneWay.countDown();
        }
        @Override public Object receiveAndReply(Object message) {
            if (message instanceof Ping p) return new Pong("re:" + p.text());
            throw new IllegalArgumentException("unexpected " + message);
        }
    }

    @Test
    void local_ask_and_send() throws Exception {
        LocalRpcEnv env = new LocalRpcEnv("local-test");
        try {
            EchoEndpoint echo = new EchoEndpoint();
            env.setupEndpoint("echo", echo);
            RpcEndpointRef ref = env.endpointRef("echo", "local", 0);

            Pong pong = ref.ask(new Ping("hi"));
            assertThat(pong.text()).isEqualTo("re:hi");

            ref.send(new Ping("oneway"));
            assertThat(echo.gotOneWay.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(((Ping) echo.lastOneWay.get()).text()).isEqualTo("oneway");
        } finally {
            env.shutdown();
        }
    }

    @Test
    void netty_ask_and_send_over_real_socket() throws Exception {
        NettyRpcEnv server = new NettyRpcEnv("server", "127.0.0.1", 0, new JavaSerializer());
        NettyRpcEnv client = new NettyRpcEnv("client", "127.0.0.1", 0, new JavaSerializer());
        try {
            EchoEndpoint echo = new EchoEndpoint();
            server.setupEndpoint("echo", echo);

            RpcEndpointRef ref = client.endpointRef("echo", server.address().host, server.address().port);

            Pong pong = ref.ask(new Ping("hi"));
            assertThat(pong.text()).isEqualTo("re:hi");

            ref.send(new Ping("oneway"));
            assertThat(echo.gotOneWay.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(((Ping) echo.lastOneWay.get()).text()).isEqualTo("oneway");
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test
    void netty_ask_unknown_endpoint_fails() {
        NettyRpcEnv server = new NettyRpcEnv("server", "127.0.0.1", 0, new JavaSerializer());
        NettyRpcEnv client = new NettyRpcEnv("client", "127.0.0.1", 0, new JavaSerializer());
        try {
            RpcEndpointRef ref = client.endpointRef("nope", server.address().host, server.address().port);
            assertThatThrownBy(() -> ref.ask(new Ping("x")))
                    .isInstanceOf(RemoteRpcException.class);
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test
    void netty_remote_handler_exception_propagates() {
        NettyRpcEnv server = new NettyRpcEnv("server", "127.0.0.1", 0, new JavaSerializer());
        NettyRpcEnv client = new NettyRpcEnv("client", "127.0.0.1", 0, new JavaSerializer());
        try {
            server.setupEndpoint("echo", new EchoEndpoint());
            RpcEndpointRef ref = client.endpointRef("echo", server.address().host, server.address().port);
            // EchoEndpoint throws on non-Ping asks.
            assertThatThrownBy(() -> ref.ask(new Pong("bad")))
                    .isInstanceOf(RemoteRpcException.class)
                    .hasMessageContaining("IllegalArgumentException");
        } finally {
            client.shutdown();
            server.shutdown();
        }
    }
}
