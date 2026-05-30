package com.minispark.storage;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEnv;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link BlockManager} that can serve and fetch blocks over RPC. Each
 * executor runs one, publishing a {@code "BlockManager"} endpoint on its
 * {@link RpcEnv}. A reducer fetching a remote shuffle block asks the owning
 * executor's endpoint for the bytes.
 *
 * <p>This is what makes shuffle work across executor JVMs: map outputs stay on
 * the executor that produced them, and reducers pull only the buckets they
 * need, from wherever they live.
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockManager + BlockTransferService
 */
public final class NetworkBlockManager implements BlockManager, RpcEndpoint {

    public static final String ENDPOINT_NAME = "BlockManager";

    /** Reducer → owning executor: fetch this block's bytes (null reply = absent). */
    public record FetchBlock(int shuffleId, int mapId, int reduceId) implements Serializable {}

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
        // Pull from the owning executor's block endpoint.
        if (!(id instanceof BlockId.ShuffleBlock sb)) {
            throw new UnsupportedOperationException("Only shuffle blocks are network-fetchable: " + id);
        }
        byte[] bytes = rpcEnv.endpointRef(ENDPOINT_NAME, loc.host, loc.port)
                .ask(new FetchBlock(sb.shuffleId, sb.mapId, sb.reduceId));
        return Optional.ofNullable(bytes);
    }

    @Override
    public Object receiveAndReply(Object message) {
        if (message instanceof FetchBlock f) {
            return getBlock(new BlockId.ShuffleBlock(f.shuffleId(), f.mapId(), f.reduceId()))
                    .orElse(null);
        }
        throw new IllegalArgumentException("BlockManager got unexpected message: " + message);
    }
}
