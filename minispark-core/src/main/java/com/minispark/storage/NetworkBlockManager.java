package com.minispark.storage;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEnv;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link BlockManager} that can serve and fetch <i>any</i> block over RPC.
 * Each executor (and the driver) runs one, publishing a {@code "BlockManager"}
 * endpoint on its {@link RpcEnv}. A consumer fetching a remote block asks the
 * owning manager's endpoint for the bytes.
 *
 * <p>This is what makes both shuffle and broadcast work across JVMs: data
 * stays where it was produced, and consumers pull only what they need from
 * wherever it lives — addressed purely by {@link BlockId}, so new block kinds
 * (sort-shuffle data blocks, broadcast blocks) need no transport changes.
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockManager + BlockTransferService
 */
public final class NetworkBlockManager implements BlockManager, RpcEndpoint {

    public static final String ENDPOINT_NAME = "BlockManager";

    /** Consumer → owning manager: fetch this block's bytes (null reply = absent). */
    public record FetchBlock(BlockId id) implements Serializable {}

    private final ExecutorLocation location;
    private final RpcEnv rpcEnv;
    private final ConcurrentMap<BlockId, byte[]> blocks = new ConcurrentHashMap<>();

    public NetworkBlockManager(ExecutorLocation location, RpcEnv rpcEnv) {
        this.location = location;
        this.rpcEnv = rpcEnv;
        rpcEnv.setupEndpoint(ENDPOINT_NAME, this);
    }

    @Override public ExecutorLocation location() { return location; }

    @Override public void putBlock(BlockId id, byte[] data) { blocks.put(id, data); }

    @Override public Optional<byte[]> getBlock(BlockId id) {
        return Optional.ofNullable(blocks.get(id));
    }

    @Override
    public Optional<byte[]> getRemoteBlock(BlockId id, ExecutorLocation loc) {
        if (location.equals(loc)) return getBlock(id);
        byte[] bytes = rpcEnv.endpointRef(ENDPOINT_NAME, loc.host, loc.port)
                .ask(new FetchBlock(id));
        return Optional.ofNullable(bytes);
    }

    @Override
    public Object receiveAndReply(Object message) {
        if (message instanceof FetchBlock f) {
            return getBlock(f.id()).orElse(null);
        }
        throw new IllegalArgumentException("BlockManager got unexpected message: " + message);
    }
}
