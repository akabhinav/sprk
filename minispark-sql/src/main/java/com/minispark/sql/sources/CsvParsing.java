package com.minispark.sql.sources;

import com.minispark.sql.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV line splitting and value coercion, shared by schema inference and the
 * scan. Splitting handles double-quoted fields (with {@code ""} as an escaped
 * quote) and a configurable delimiter; coercion turns a raw cell string into a
 * typed value matching the column's {@link DataType}, with empty/unparseable
 * cells becoming {@code null}.
 *
 * <p>Serializable so the delimiter config travels with the parse closure to
 * executors.
 */
public final class CsvParsing implements Serializable {

    private final char delimiter;

    public CsvParsing(char delimiter) { this.delimiter = delimiter; }

    /** Split one CSV line into raw cell strings, honoring quotes. */
    public List<String> split(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') { cur.append('"'); i++; }
                    else inQuotes = false;
                } else cur.append(c);
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == delimiter) {
                out.add(cur.toString()); cur.setLength(0);
            } else cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    /** Coerce a raw cell to the target type; null on empty or parse failure. */
    public static Object coerce(String raw, DataType type) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.isEmpty()) return null;
        try {
            return switch (type) {
                case INT -> Integer.valueOf(s);
                case LONG -> Long.valueOf(s);
                case DOUBLE -> Double.valueOf(s);
                case BOOLEAN -> Boolean.valueOf(s);
                case STRING -> s;
            };
        } catch (NumberFormatException e) {
            return null; // permissive: bad cell becomes null (Spark's PERMISSIVE mode)
        }
    }

    /** Infer the narrowest type that fits a raw cell: INT < LONG < DOUBLE < (BOOLEAN) < STRING. */
    public static DataType inferType(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.isEmpty()) return null;
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) return DataType.BOOLEAN;
        try { Integer.parseInt(s); return DataType.INT; } catch (NumberFormatException ignored) {}
        try { Long.parseLong(s); return DataType.LONG; } catch (NumberFormatException ignored) {}
        try { Double.parseDouble(s); return DataType.DOUBLE; } catch (NumberFormatException ignored) {}
        return DataType.STRING;
    }

    /** Widen two inferred types to the one that accepts both. null means "unknown so far". */
    public static DataType widen(DataType a, DataType b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a == b) return a;
        // Numeric widening; anything mixed with STRING/BOOLEAN falls back to STRING.
        if (isNumeric(a) && isNumeric(b)) {
            if (a == DataType.DOUBLE || b == DataType.DOUBLE) return DataType.DOUBLE;
            if (a == DataType.LONG || b == DataType.LONG) return DataType.LONG;
            return DataType.INT;
        }
        return DataType.STRING;
    }

    private static boolean isNumeric(DataType t) {
        return t == DataType.INT || t == DataType.LONG || t == DataType.DOUBLE;
    }
}
