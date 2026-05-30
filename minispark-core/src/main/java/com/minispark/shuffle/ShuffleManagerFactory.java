package com.minispark.shuffle;

import com.minispark.serializer.Serializer;
import com.minispark.storage.BlockManager;
import com.minispark.storage.MapOutputTracker;

/**
 * Builds the configured {@link ShuffleManager}. The choice must be the same on
 * the driver and on every executor (they exchange blocks in a format only the
 * matching reader understands), so both call this with the same name.
 *
 * <p>Selected by {@code minispark.shuffle.manager}: {@code hash} (default) or
 * {@code sort}.
 */
public final class ShuffleManagerFactory {

    private ShuffleManagerFactory() {}

    public static ShuffleManager create(String name, BlockManager blockManager,
                                        MapOutputTracker tracker, Serializer serializer) {
        return switch (name == null ? "hash" : name.toLowerCase()) {
            case "hash" -> new HashShuffleManager(blockManager, tracker, serializer);
            case "sort" -> new SortShuffleManager(blockManager, tracker, serializer);
            default -> throw new IllegalArgumentException("Unknown shuffle manager: " + name);
        };
    }
}
