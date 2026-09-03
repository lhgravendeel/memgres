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
        // The word as the statement spelled it. A keyword's value is folded to upper case for
        // matching, and quoting that named a word nobody had written; a string constant is named
        // with the quotes that made it one, which is how PostgreSQL names it.
        if (token != null && token.type() == TokenType.STRING_LITERAL) {
            return "syntax error at or near \"" + token.sqlText() + "\"";
        }
        return "syntax error at or near \"" + (token == null ? "" : token.raw()) + "\"";
    }

    private ParseException(String message, Token token, String sqlState, boolean verbatim) {
        super(message, sqlState);
        if (token != null && token.position() >= 0) setPosition(token.position() + 1); // 1-based
    }

    /**
     * A complaint PostgreSQL words itself rather than reporting as a syntax error — "no language
     * specified" and its kind. The constructors replace the message with "syntax error at or
     * near ...", which is what PostgreSQL says when the grammar is what went wrong, and not what
     * it says when the statement parses and means something it refuses.
     */
    public static ParseException saying(String message, Token token, String sqlState) {
        return new ParseException(message, token, sqlState, true);
    }

    /**
     * The syntax error PostgreSQL reports at {@code token}: the word as the statement spelled it,
     * or "at end of input" when there is no word left to point at.
     */
    public static ParseException at(Token token) {
        if (token == null || token.type() == TokenType.EOF || token.type() == TokenType.SEMICOLON) {
            return new ParseException("syntax error at end of input", token, "42601", true);
        }
        return new ParseException("syntax error at or near \"" + token.raw() + "\"", token, "42601", true);
    }
}
