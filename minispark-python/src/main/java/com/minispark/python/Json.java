package com.minispark.python;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A tiny self-contained JSON reader/writer — just enough for the PySpark
 * gateway wire protocol (null, booleans, numbers, strings, arrays, objects).
 * Numbers decode to {@link Long} when integral, else {@link Double}. No
 * external dependency, on purpose: the bridge protocol stays inspectable.
 */
final class Json {

    private Json() {}

    // ---------- encode ----------

    static String write(Object v) {
        StringBuilder sb = new StringBuilder();
        encode(v, sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    static void encode(Object v, StringBuilder sb) {
        if (v == null) { sb.append("null"); return; }
        if (v instanceof String s) { encodeString(s, sb); return; }
        if (v instanceof Boolean || v instanceof Integer || v instanceof Long) { sb.append(v); return; }
        if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (d == Math.rint(d) && !Double.isInfinite(d)) sb.append((long) d); else sb.append(d);
            return;
        }
        if (v instanceof Number) { sb.append(v); return; }
        if (v instanceof Map<?, ?> m) {
            sb.append('{'); boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(','); first = false;
                encodeString(String.valueOf(e.getKey()), sb); sb.append(':'); encode(e.getValue(), sb);
            }
            sb.append('}'); return;
        }
        if (v instanceof Iterable<?> it) {
            sb.append('['); boolean first = true;
            for (Object o : it) { if (!first) sb.append(','); first = false; encode(o, sb); }
            sb.append(']'); return;
        }
        if (v instanceof Object[] arr) {
            sb.append('['); for (int i = 0; i < arr.length; i++) { if (i > 0) sb.append(','); encode(arr[i], sb); }
            sb.append(']'); return;
        }
        // Fallback: stringify.
        encodeString(String.valueOf(v), sb);
    }

    private static void encodeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> { if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c); }
            }
        }
        sb.append('"');
    }

    // ---------- decode ----------

    static Object read(String s) { return new Parser(s).parseValue(); }

    @SuppressWarnings("unchecked")
    static Map<String, Object> readObject(String s) { return (Map<String, Object>) read(s); }

    private static final class Parser {
        private final String s; private int i;
        Parser(String s) { this.s = s; }

        Object parseValue() {
            skipWs();
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> parseString();
                case 't', 'f' -> parseBool();
                case 'n' -> { i += 4; yield null; }   // null
                default -> parseNumber();
            };
        }

        private Map<String, Object> parseObject() {
            Map<String, Object> m = new LinkedHashMap<>(); i++; skipWs();
            if (s.charAt(i) == '}') { i++; return m; }
            while (true) {
                skipWs(); String key = parseString(); skipWs(); i++; // ':'
                m.put(key, parseValue()); skipWs();
                char c = s.charAt(i++); if (c == '}') break; // else ','
            }
            return m;
        }

        private List<Object> parseArray() {
            List<Object> a = new ArrayList<>(); i++; skipWs();
            if (s.charAt(i) == ']') { i++; return a; }
            while (true) {
                a.add(parseValue()); skipWs();
                char c = s.charAt(i++); if (c == ']') break; // else ','
            }
            return a;
        }

        private String parseString() {
            StringBuilder sb = new StringBuilder(); i++; // opening quote
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') break;
                if (c == '\\') {
                    char e = s.charAt(i++);
                    switch (e) {
                        case '"' -> sb.append('"'); case '\\' -> sb.append('\\'); case '/' -> sb.append('/');
                        case 'n' -> sb.append('\n'); case 'r' -> sb.append('\r'); case 't' -> sb.append('\t');
                        case 'b' -> sb.append('\b'); case 'f' -> sb.append('\f');
                        case 'u' -> { sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; }
                        default -> sb.append(e);
                    }
                } else sb.append(c);
            }
            return sb.toString();
        }

        private Boolean parseBool() {
            if (s.charAt(i) == 't') { i += 4; return Boolean.TRUE; }
            i += 5; return Boolean.FALSE;
        }

        private Object parseNumber() {
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i++;
            String num = s.substring(start, i);
            if (num.indexOf('.') < 0 && num.indexOf('e') < 0 && num.indexOf('E') < 0) {
                try { return Long.parseLong(num); } catch (NumberFormatException ignore) { /* fall through */ }
            }
            return Double.parseDouble(num);
        }

        private void skipWs() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }
    }
}
