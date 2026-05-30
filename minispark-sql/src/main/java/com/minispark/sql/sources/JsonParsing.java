package com.minispark.sql.sources;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal JSON-lines parser: one flat object per line, primitive values only
 * ({@code {"name":"alice","age":30,"active":true}}). Enough to teach the
 * "semi-structured source → typed DataFrame" path; full JSON (nesting, arrays)
 * is out of scope. Schema is inferred by unioning keys across the sampled lines
 * and widening their value types.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.json (JacksonParser +
 * JsonInferSchema), vastly simplified.
 */
public final class JsonParsing implements Serializable {

    private JsonParsing() {}

    /** Infer a schema by unioning object keys across lines (insertion-ordered). */
    public static StructType inferSchema(List<String> lines) {
        Map<String, DataType> fields = new LinkedHashMap<>();
        for (String line : lines) {
            if (line.isBlank()) continue;
            for (Map.Entry<String, Object> e : parseObject(line).entrySet()) {
                DataType t = typeOf(e.getValue());
                fields.merge(e.getKey(), t, JsonParsing::widen);
            }
        }
        List<StructField> out = new ArrayList<>();
        for (Map.Entry<String, DataType> e : fields.entrySet()) {
            out.add(StructField.of(e.getKey(), e.getValue() == null ? DataType.STRING : e.getValue()));
        }
        return new StructType(out);
    }

    /** Parse one line into a Row whose columns follow {@code schema} (missing keys → null). */
    public static Row parseLine(String line, StructType schema) {
        Map<String, Object> obj = parseObject(line);
        Object[] vals = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            Object v = obj.get(schema.name(i));
            vals[i] = coerce(v, schema.type(i));
        }
        return new Row(vals);
    }

    // ----- a tiny JSON object scanner (flat, primitives) -----

    private static Map<String, Object> parseObject(String line) {
        Map<String, Object> out = new LinkedHashMap<>();
        String s = line.trim();
        if (!s.startsWith("{") || !s.endsWith("}")) return out;
        s = s.substring(1, s.length() - 1);
        int i = 0, n = s.length();
        while (i < n) {
            while (i < n && (Character.isWhitespace(s.charAt(i)) || s.charAt(i) == ',')) i++;
            if (i >= n) break;
            if (s.charAt(i) != '"') break;
            int keyEnd = s.indexOf('"', i + 1);
            if (keyEnd < 0) break;   // unterminated key: tolerate (PERMISSIVE) — stop parsing this line
            String key = s.substring(i + 1, keyEnd);
            i = keyEnd + 1;
            while (i < n && (Character.isWhitespace(s.charAt(i)) || s.charAt(i) == ':')) i++;
            // value: string, number, boolean, or null
            Object val;
            if (i < n && s.charAt(i) == '"') {
                int valEnd = s.indexOf('"', i + 1);
                if (valEnd < 0) { out.put(key, null); break; }  // unterminated value: null, stop
                val = s.substring(i + 1, valEnd);
                i = valEnd + 1;
            } else {
                int start = i;
                while (i < n && s.charAt(i) != ',') i++;
                String token = s.substring(start, i).trim();
                val = parseScalar(token);
            }
            out.put(key, val);
        }
        return out;
    }

    private static Object parseScalar(String token) {
        if (token.equals("null")) return null;
        if (token.equals("true")) return Boolean.TRUE;
        if (token.equals("false")) return Boolean.FALSE;
        try { return Integer.valueOf(token); } catch (NumberFormatException ignored) {}
        try { return Long.valueOf(token); } catch (NumberFormatException ignored) {}
        try { return Double.valueOf(token); } catch (NumberFormatException ignored) {}
        return token;
    }

    private static DataType typeOf(Object v) {
        if (v == null) return null;
        if (v instanceof Integer) return DataType.INT;
        if (v instanceof Long) return DataType.LONG;
        if (v instanceof Double) return DataType.DOUBLE;
        if (v instanceof Boolean) return DataType.BOOLEAN;
        return DataType.STRING;
    }

    private static DataType widen(DataType a, DataType b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a == b) return a;
        boolean an = a == DataType.INT || a == DataType.LONG || a == DataType.DOUBLE;
        boolean bn = b == DataType.INT || b == DataType.LONG || b == DataType.DOUBLE;
        if (an && bn) {
            if (a == DataType.DOUBLE || b == DataType.DOUBLE) return DataType.DOUBLE;
            if (a == DataType.LONG || b == DataType.LONG) return DataType.LONG;
            return DataType.INT;
        }
        return DataType.STRING;
    }

    private static Object coerce(Object v, DataType type) {
        if (v == null) return null;
        return switch (type) {
            case INT -> v instanceof Number ? ((Number) v).intValue() : null;
            case LONG -> v instanceof Number ? ((Number) v).longValue() : null;
            case DOUBLE -> v instanceof Number ? ((Number) v).doubleValue() : null;
            case BOOLEAN -> v instanceof Boolean ? v : null;
            case STRING -> String.valueOf(v);
        };
    }
}
