package com.minispark.rpc;

import java.io.Serializable;

/**
 * Wire envelope for {@link NettyRpcEnv}. One class for both directions:
 * a request (optionally expecting a reply) and a reply are distinguished by
 * {@code isReply}.
 *
 * <ul>
 *   <li>{@code requestId} correlates a reply with its ask.</li>
 *   <li>{@code endpointName} routes a request to the right local endpoint.</li>
 *   <li>{@code payload} is the user message (driver/executor protocol object).</li>
 * </ul>
 */
final class TransportMessage implements Serializable {
    final long requestId;
    final String endpointName;
    final Object payload;
    final boolean expectReply;
    final boolean isReply;
    final boolean isError;

    private TransportMessage(long requestId, String endpointName, Object payload,
                             boolean expectReply, boolean isReply, boolean isError) {
        this.requestId = requestId;
        this.endpointName = endpointName;
        this.payload = payload;
        this.expectReply = expectReply;
        this.isReply = isReply;
        this.isError = isError;
    }

    static TransportMessage oneWay(String endpoint, Object payload) {
        return new TransportMessage(-1, endpoint, payload, false, false, false);
    }
    static TransportMessage request(long id, String endpoint, Object payload) {
        return new TransportMessage(id, endpoint, payload, true, false, false);
    }
    static TransportMessage reply(long id, Object payload) {
        return new TransportMessage(id, null, payload, false, true, false);
    }
    static TransportMessage error(long id, Object error) {
        return new TransportMessage(id, null, error, false, true, true);
    }
}
