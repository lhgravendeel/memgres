package com.memgres.engine.parser;

/**
 * A single token produced by the SQL lexer.
 */
public final class Token {
    public final TokenType type;
    public final String value;
    public final int position;
    /** The word as it was written, before the lexer folded its case. Null when it is unchanged. */
    private final String raw;

    public Token(TokenType type, String value, int position) {
        this(type, value, position, null);
    }

    public Token(TokenType type, String value, int position, String raw) {
        this.type = type;
        this.value = value;
        this.position = position;
        this.raw = raw;
    }

    /**
     * The token's text as the statement spelled it.
     *
     * <p>PostgreSQL quotes the user's own spelling back in a syntax error, so the message points
     * at something findable in the statement rather than at the lexer's folded form.
     */
    public String raw() { return raw != null ? raw : value; }

    /**
     * The token written back as SQL.
     *
     * <p>A clause captured as text and re-parsed later -- an index predicate, a generated
     * column's expression, a routine's body, a trigger's WHEN -- has to say again what it said,
     * and the lexer's value is not that. A string constant has lost the quotes that made it one
     * and so has a quoted identifier, so a column called {@code "c c"} was written back as two
     * words and the re-parse looked for a column called {@code c}.
     */
    public String sqlText() {
        if (type == TokenType.STRING_LITERAL) {
            return "'" + value.replace("'", "''") + "'";
        }
        if (type == TokenType.QUOTED_IDENTIFIER) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        if (type == TokenType.DOLLAR_STRING_LITERAL) {
            String tag = "$$";
            for (int i = 0; value.contains(tag); i++) tag = "$q" + i + "$";
            return tag + value + tag;
        }
        return raw();
    }

    @Override
    public String toString() {
        return type + "(" + value + ")@" + position;
    }

    public TokenType type() { return type; }
    public String value() { return value; }
    public int position() { return position; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Token that = (Token) o;
        return java.util.Objects.equals(type, that.type)
            && java.util.Objects.equals(value, that.value)
            && position == that.position;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(type, value, position);
    }
}
