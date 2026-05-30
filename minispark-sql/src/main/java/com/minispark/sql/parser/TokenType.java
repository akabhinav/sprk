package com.minispark.sql.parser;

/**
 * Lexical categories the {@link Lexer} emits. Keywords are recognized as a
 * distinct {@link #KEYWORD} (the lexer folds identifiers that match a reserved
 * word) so the parser can branch on them without string compares everywhere.
 */
public enum TokenType {
    IDENTIFIER, KEYWORD,
    INT_LITERAL, DOUBLE_LITERAL, STRING_LITERAL,
    // operators & punctuation
    STAR, PLUS, MINUS, SLASH,
    EQ, NEQ, LT, LE, GT, GE,
    LPAREN, RPAREN, COMMA, DOT,
    EOF
}
