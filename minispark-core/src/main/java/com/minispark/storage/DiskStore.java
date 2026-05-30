package com.minispark.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores blocks as files under a per-executor local directory. Used for the
 * disk tier of {@code MEMORY_AND_DISK} / {@code DISK_ONLY} cache levels and for
 * blocks spilled out of the {@link MemoryStore} under pressure.
 *
 * <p>The directory defaults to {@code <java.io.tmpdir>/minispark-<random>} so
 * multiple executors on one host don't collide. Files are named by the block's
 * sanitized id. On JVM exit the directory is best-effort cleaned.
 *
 * Real Spark equivalent: org.apache.spark.storage.DiskStore (+ DiskBlockManager).
 */
public final class DiskStore {

    private static final Logger LOG = LoggerFactory.getLogger(DiskStore.class);

    private final Path baseDir;
    private final ConcurrentMap<BlockId, Path> index = new ConcurrentHashMap<>();

    public DiskStore(String configuredDir) {
        try {
            Path root = (configuredDir != null && !configuredDir.isBlank())
                    ? Path.of(configuredDir)
                    : Path.of(System.getProperty("java.io.tmpdir"));
            this.baseDir = Files.createDirectories(
                    root.resolve("minispark-" + Long.toHexString(System.nanoTime())));
            this.baseDir.toFile().deleteOnExit();
            LOG.info("DiskStore at {}", baseDir);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create disk store dir", e);
        }
    }

    public void put(BlockId id, byte[] data) {
        Path f = baseDir.resolve(sanitize(id.name()));
        try {
            Files.write(f, data);
            f.toFile().deleteOnExit();
            index.put(id, f);
        } catch (IOException e) {
            throw new RuntimeException("Disk write failed for " + id, e);
        }
    }

    public byte[] get(BlockId id) {
        Path f = index.get(id);
        if (f == null) return null;
        try {
            return Files.readAllBytes(f);
        } catch (IOException e) {
            throw new RuntimeException("Disk read failed for " + id, e);
        }
    }

    public boolean contains(BlockId id) { return index.containsKey(id); }

    public void remove(BlockId id) {
        Path f = index.remove(id);
        if (f != null) try { Files.deleteIfExists(f); } catch (IOException ignored) {}
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9_.-]", "_");
    }
}
