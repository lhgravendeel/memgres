package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;

/**
 * Exception thrown when parsing fails.
 */
public class ParseException extends MemgresException {

    public ParseException(String message, Token token) {
        super(atOrNear(token), "42601");
        if (token.position() >= 0) setPosition(token.position() + 1); // 1-based
    }

    public ParseException(String message, Token token, String sqlState) {
        super(atOrNear(token), sqlState);
        if (token.position() >= 0) setPosition(token.position() + 1); // 1-based
    }

    /**
     * PostgreSQL names the word it stopped on, but when the statement simply ran out there is no
     * word to name and it says so. Quoting the empty string instead — {@code at or near ""} —
     * names nothing and reads as though a blank token were in the text.
     */
    private static String atOrNear(Token token) {
        if (token != null && token.type() == TokenType.EOF) {
            return "syntax error at end of input";
        }
        return "syntax error at or near \"" + (token == null ? "" : token.value()) + "\"";
    }

    private ParseException(String message, Token token, boolean verbatim) {
        super(message, "42601");
        if (token != null && token.position() >= 0) setPosition(token.position() + 1); // 1-based
    }

    /**
     * The syntax error PostgreSQL reports at {@code token}: the word as the statement spelled it,
     * or "at end of input" when there is no word left to point at.
     */
    public static ParseException at(Token token) {
        if (token == null || token.type() == TokenType.EOF || token.type() == TokenType.SEMICOLON) {
            return new ParseException("syntax error at end of input", token, true);
        }
        return new ParseException("syntax error at or near \"" + token.raw() + "\"", token, true);
    }
}
