package com.minispark.sql.parser;

/**
 * One lexical token: its {@link TokenType}, the raw text, and the source
 * position (for error messages). Keyword text is stored upper-cased so the
 * parser can compare case-insensitively.
 */
public record Token(TokenType type, String text, int pos) {
    public boolean is(TokenType t) { return type == t; }
    public boolean isKeyword(String kw) { return type == TokenType.KEYWORD && text.equals(kw); }
    @Override public String toString() { return type + "(" + text + ")"; }
}
