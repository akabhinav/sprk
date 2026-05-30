package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.executor.TaskContext;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads a text file as one record per line. The file is split into byte-range
 * partitions of roughly equal size. A line is "owned" by the partition whose
 * range contains the byte offset of the line's <i>first</i> character; a
 * partition that starts mid-line therefore drops its partial first line
 * (the previous partition owns it) and is allowed to read past its nominal
 * end to finish whichever line happens to straddle the boundary.
 *
 * <p>Phase 6 would replace this with a real {@code HadoopRDD}-style input
 * format that streams splits without first stat'ing the file. The seam stays
 * identical.
 *
 * Real Spark equivalent: org.apache.spark.rdd.HadoopRDD (textFile path)
 */
public final class TextFileRDD extends RDD<String> {

    private static final class LinePartition implements Partition, Serializable {
        final int idx;
        final String path;
        final long startInclusive;
        final long endExclusive;
        LinePartition(int idx, String path, long s, long e) {
            this.idx = idx; this.path = path; this.startInclusive = s; this.endExclusive = e;
        }
        public int index() { return idx; }
    }

    private final List<Partition> partitions;

    public TextFileRDD(MiniSparkContext sc, String path, int numPartitions) {
        super(sc);
        try {
            int n = Math.max(1, numPartitions);
            // A directory reads all of its regular files (Spark's textFile does
            // the same), which is also how we read a previously-written output
            // dir of part-NNNNN files. A single file is just the one-element case.
            List<Path> files = listDataFiles(Path.of(path));
            List<Partition> parts = new ArrayList<>();
            int idx = 0;
            for (Path f : files) {
                long size = Files.size(f);
                idx = splitByBytes(f.toString(), size, n, parts, idx);
            }
            this.partitions = parts;
        } catch (IOException e) {
            throw new RuntimeException("Cannot stat " + path, e);
        }
    }

    private static List<Path> listDataFiles(Path p) throws IOException {
        if (!Files.isDirectory(p)) return List.of(p);
        try (var stream = Files.list(p)) {
            return stream.filter(Files::isRegularFile)
                    .filter(f -> { String n = f.getFileName().toString();
                                   return !n.startsWith(".") && !n.startsWith("_"); }) // skip _SUCCESS, hidden
                    .sorted()
                    .toList();
        }
    }

    /** Byte-split one file into up to {@code n} partitions, appended to {@code out}. */
    private static int splitByBytes(String path, long size, int n, List<Partition> out, int startIdx) {
        long stride = Math.max(1L, size / n);
        long cursor = 0;
        int idx = startIdx;
        // Always emit at least one partition per file (even an empty file).
        do {
            long end = (cursor + stride >= size) ? size : cursor + stride;
            out.add(new LinePartition(idx++, path, cursor, end));
            cursor = end;
        } while (cursor < size);
        return idx;
    }

    @Override public List<Partition> getPartitions() { return partitions; }

    @Override
    public Iterator<String> compute(Partition split, TaskContext ctx) {
        LinePartition lp = (LinePartition) split;
        try {
            RandomAccessFile raf = new RandomAccessFile(lp.path, "r");
            // A partition that doesn't start at byte 0 cedes its partial first
            // line to the previous partition — UNLESS we land exactly at the
            // start of a line (the previous byte is '\n'), in which case the
            // line at our start is genuinely ours and skipping it would drop a
            // line entirely. Peek at the byte before our start to decide.
            if (lp.startInclusive == 0) {
                raf.seek(0);
            } else {
                raf.seek(lp.startInclusive - 1);
                int prev = raf.read(); // cursor advances to startInclusive
                if (prev != '\n') {
                    // Mid-line: read through the remainder so the cursor sits
                    // at the first byte of the next line.
                    raf.readLine();
                }
            }
            long firstLineStart = raf.getFilePointer();

            return new Iterator<>() {
                long nextLineStart = firstLineStart;
                String nxt = null;
                boolean done = false;

                @Override public boolean hasNext() {
                    if (done) return false;
                    if (nxt != null) return true;
                    try {
                        // Stop as soon as the next line's first byte is past our
                        // range. Lines that *start* inside our range are owned
                        // by us even if they extend across the boundary.
                        if (nextLineStart >= lp.endExclusive) { close(); return false; }
                        String line = readUtf8Line(raf);
                        if (line == null) { close(); return false; }
                        nxt = line;
                        nextLineStart = raf.getFilePointer();
                        return true;
                    } catch (IOException e) {
                        close();
                        throw new RuntimeException(e);
                    }
                }

                @Override public String next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    String v = nxt; nxt = null; return v;
                }

                private void close() {
                    done = true;
                    try { raf.close(); } catch (IOException ignored) {}
                }
            };
        } catch (IOException e) {
            throw new RuntimeException("Cannot read " + lp.path, e);
        }
    }

    /**
     * Read one UTF-8 line. Reads raw bytes up to '\n', strips an optional
     * trailing '\r'. Returns {@code null} at EOF (no bytes consumed).
     */
    private static String readUtf8Line(RandomAccessFile raf) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        int b;
        boolean any = false;
        while ((b = raf.read()) != -1) {
            any = true;
            if (b == '\n') break;
            baos.write(b);
        }
        if (!any) return null;
        byte[] bytes = baos.toByteArray();
        int len = bytes.length;
        if (len > 0 && bytes[len - 1] == '\r') len--;
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    @Override public List<Dependency<?>> getDependencies() { return List.of(); }
}
