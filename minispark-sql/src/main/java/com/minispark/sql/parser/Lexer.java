package com.minispark.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Hand-written lexer: turns a SQL string into a flat list of {@link Token}s.
 * Handles identifiers/keywords, integer and double literals, single-quoted
 * string literals, multi-char operators ({@code <=}, {@code >=}, {@code !=},
 * {@code <>}), and punctuation. Whitespace is skipped.
 *
 * <p>Hand-rolled rather than generated (ANTLR etc.) for the same reason the
 * rest of MiniSpark is hand-rolled: the point is to see every step. Real
 * Spark uses an ANTLR grammar (SqlBase.g4).
 */
public final class Lexer {

    private static final Set<String> KEYWORDS = Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
            "AS", "AND", "OR", "NOT",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON",
            "ASC", "DESC", "LIMIT", "DISTINCT", "TRUE", "FALSE", "NULL");

    private final String src;
    private int i = 0;

    public Lexer(String src) { this.src = src; }

    public List<Token> tokenize() {
        List<Token> out = new ArrayList<>();
        Token t;
        do { t = next(); out.add(t); } while (t.type() != TokenType.EOF);
        return out;
    }

    private Token next() {
        skipWhitespace();
        if (i >= src.length()) return new Token(TokenType.EOF, "", i);
        int start = i;
        char c = src.charAt(i);

        if (Character.isLetter(c) || c == '_') return identifierOrKeyword(start);
        if (Character.isDigit(c)) return number(start);
        if (c == '\'') return stringLiteral(start);

        i++; // consume the operator/punct char
        return switch (c) {
            case '*' -> tok(TokenType.STAR, "*", start);
            case '+' -> tok(TokenType.PLUS, "+", start);
            case '-' -> tok(TokenType.MINUS, "-", start);
            case '/' -> tok(TokenType.SLASH, "/", start);
            case '(' -> tok(TokenType.LPAREN, "(", start);
            case ')' -> tok(TokenType.RPAREN, ")", start);
            case ',' -> tok(TokenType.COMMA, ",", start);
            case '.' -> tok(TokenType.DOT, ".", start);
            case '=' -> tok(TokenType.EQ, "=", start);
            case '<' -> {
                if (peek('=')) { i++; yield tok(TokenType.LE, "<=", start); }
                if (peek('>')) { i++; yield tok(TokenType.NEQ, "<>", start); }
                yield tok(TokenType.LT, "<", start);
            }
            case '>' -> { if (peek('=')) { i++; yield tok(TokenType.GE, ">=", start); }
                          yield tok(TokenType.GT, ">", start); }
            case '!' -> { if (peek('=')) { i++; yield tok(TokenType.NEQ, "!=", start); }
                          throw err("unexpected '!'", start); }
            default -> throw err("unexpected character '" + c + "'", start);
        };
    }

    private Token identifierOrKeyword(int start) {
        while (i < src.length() && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '_')) i++;
        String text = src.substring(start, i);
        String upper = text.toUpperCase();
        if (KEYWORDS.contains(upper)) return new Token(TokenType.KEYWORD, upper, start);
        return new Token(TokenType.IDENTIFIER, text, start);
    }

    private Token number(int start) {
        boolean isDouble = false;
        while (i < src.length() && Character.isDigit(src.charAt(i))) i++;
        if (i < src.length() && src.charAt(i) == '.') {
            isDouble = true; i++;
            while (i < src.length() && Character.isDigit(src.charAt(i))) i++;
        }
        String text = src.substring(start, i);
        return new Token(isDouble ? TokenType.DOUBLE_LITERAL : TokenType.INT_LITERAL, text, start);
    }

    private Token stringLiteral(int start) {
        i++; // opening quote
        StringBuilder sb = new StringBuilder();
        while (i < src.length() && src.charAt(i) != '\'') {
            // '' is an escaped single quote inside a string.
            if (src.charAt(i) == '\\' && i + 1 < src.length()) { sb.append(src.charAt(i + 1)); i += 2; continue; }
            sb.append(src.charAt(i)); i++;
        }
        if (i >= src.length()) throw err("unterminated string literal", start);
        i++; // closing quote
        return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
    }

    private void skipWhitespace() {
        while (i < src.length() && Character.isWhitespace(src.charAt(i))) i++;
    }

    private boolean peek(char c) { return i < src.length() && src.charAt(i) == c; }
    private Token tok(TokenType t, String s, int p) { return new Token(t, s, p); }
    private ParseException err(String msg, int pos) { return new ParseException(msg + " at position " + pos); }
}
