package com.memgres.engine.parser;

import com.memgres.engine.PgErrors;
import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Index creation parsing (CREATE INDEX), extracted from DdlParser.
 */
class DdlIndexParser {
    private final Parser parser;

    DdlIndexParser(Parser parser) {
        this.parser = parser;
    }

    CreateIndexStmt parseCreateIndex(boolean unique, boolean concurrently) {
        if (!concurrently) concurrently = parser.matchKeyword("CONCURRENTLY");
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");

        String name = null;
        if (!parser.checkKeyword("ON")) {
            name = parser.readIdentifier();
        }

        parser.expectKeyword("ON");
        parser.matchKeyword("ONLY");
        String indexSchema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            indexSchema = table;
            table = parser.readIdentifier();
        }

        String method = null;
        if (parser.matchKeyword("USING")) {
            method = parser.readIdentifier();
        }

        parser.expect(TokenType.LEFT_PAREN);
        List<String> columns = parseIndexColumnList();
        List<String> columnOptions = this.lastColumnOptions;
        parser.expect(TokenType.RIGHT_PAREN);

        List<String> includeColumns = null;
        if (parser.matchKeyword("INCLUDE")) {
            parser.expect(TokenType.LEFT_PAREN);
            includeColumns = parseIncludeColumnList();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        // Parse NULLS NOT DISTINCT (PG 15+) for unique indexes
        boolean nullsNotDistinct = false;
        if (parser.matchKeyword("NULLS")) {
            if (parser.matchKeyword("NOT")) {
                parser.expectKeyword("DISTINCT");
                nullsNotDistinct = true;
            } else {
                // Could be NULLS DISTINCT (default), just consume
                parser.matchKeyword("DISTINCT");
            }
        }

        // WITH (storage_parameter = value, ...) — kept rather than skipped, so the executor can
        // check each one against what the access method accepts.
        java.util.Map<String, String> withOptions = null;
        if (parser.matchKeyword("WITH")) {
            if (parser.match(TokenType.LEFT_PAREN)) {
                withOptions = new java.util.LinkedHashMap<>();
                do {
                    String key = parser.readIdentifier();
                    String val = null;
                    if (parser.match(TokenType.EQUALS)) {
                        StringBuilder sb = new StringBuilder();
                        if (parser.check(TokenType.MINUS)) sb.append(parser.advance().value());
                        sb.append(parser.advance().value());
                        val = sb.toString();
                    }
                    withOptions.put(key.toLowerCase(java.util.Locale.ROOT), val);
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            }
        }

        if (parser.matchKeyword("TABLESPACE")) {
            parser.readIdentifier();
        }

        String whereClause = null;
        if (parser.matchKeyword("WHERE")) {
            StringBuilder sb = new StringBuilder();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                Token wt = parser.advance();
                sb.append(wt.sqlText());
                sb.append(' ');
            }
            whereClause = sb.toString().trim();
        }

        return new CreateIndexStmt(name, indexSchema, table, columns, unique, ifNotExists, concurrently,
                method, includeColumns, whereClause, columnOptions, nullsNotDistinct, withOptions);
    }

    /**
     * INCLUDE accepts only plain column names, but an expression parses far enough that the
     * executor can reject it with PostgreSQL's own "expressions are not supported" message
     * instead of a syntax error.
     */
    private List<String> parseIncludeColumnList() {
        List<String> cols = new ArrayList<>();
        do {
            if (parser.check(TokenType.LEFT_PAREN)) {
                StringBuilder expr = new StringBuilder();
                int depth = 0;
                do {
                    Token t = parser.advance();
                    if (t.type() == TokenType.LEFT_PAREN) depth++;
                    else if (t.type() == TokenType.RIGHT_PAREN) depth--;
                    if (expr.length() > 0) expr.append(' ');
                    expr.append(indexTokenValue(t));
                } while (depth > 0 && !parser.isAtEnd());
                cols.add(expr.toString().trim());
            } else {
                cols.add(parser.readIdentifier());
            }
        } while (parser.match(TokenType.COMMA));
        return cols;
    }

    /** Parsed column options list, populated by parseIndexColumnList. */
    List<String> lastColumnOptions;

    private List<String> parseIndexColumnList() {
        List<String> columns = new ArrayList<>();
        List<String> columnOptions = new ArrayList<>();
        do {
            String col;
            int collateAt = parser.check(TokenType.LEFT_PAREN) ? collateClosingParens() : -1;
            if (collateAt > 0) {
                // A collation written at the top of a parenthesised key belongs to the key rather
                // than sitting above it: PostgreSQL's analysis moves it into the key's own
                // collation, so (b COLLATE "C") is the column b under that collation and the
                // definition reads back as b COLLATE "C" with no parentheses left at all. What
                // stands before the clause is the key, and it keeps whatever parentheses its own
                // shape asks for.
                parser.expect(TokenType.LEFT_PAREN);
                StringBuilder inner = new StringBuilder();
                for (int i = 1; i < collateAt; i++) {
                    Token t = parser.advance();
                    if (inner.length() > 0) inner.append(' ');
                    inner.append(indexTokenValue(t));
                }
                col = inner.toString().trim();
            } else if (parser.check(TokenType.LEFT_PAREN)) {
                // An index key is an ordinary expression; PostgreSQL's grammar has no production
                // for a query here, so a leading SELECT is a syntax error rather than a subquery.
                int parens = parser.countLeadingParensBeforeQuery();
                if (parens > 0) {
                    parser.consumeLeadingParens(parens);
                    throw PgErrors.syntax("syntax error at or near \"" + parser.peek().value() + "\"");
                }
                StringBuilder expr = new StringBuilder();
                int depth = 0;
                do {
                    Token t = parser.advance();
                    if (t.type() == TokenType.LEFT_PAREN) depth++;
                    else if (t.type() == TokenType.RIGHT_PAREN) depth--;
                    if (expr.length() > 0) expr.append(' ');
                    expr.append(indexTokenValue(t));
                } while (depth > 0 && !parser.isAtEnd());
                col = expr.toString().trim();
                if (col.startsWith("(") && col.endsWith(")")) {
                    col = col.substring(1, col.length() - 1).trim();
                }
            } else {
                // An index key is a column name, a call, or a parenthesised expression, and
                // nothing else. A schema written here can therefore only be a function's, which is
                // why PostgreSQL reports (t.a) at the token where the argument list should have
                // begun rather than at the dot: by then it has read t.a as the name of a function.
                Token head = parser.peek();
                boolean callOnly = requireIndexKeyStart(head);
                col = parser.readIdentifier();
                boolean qualified = false;
                while (parser.check(TokenType.DOT)) {
                    parser.advance();
                    col = col + "." + parser.readIdentifier();
                    qualified = true;
                    callOnly = true;
                }
                if (parser.check(TokenType.LEFT_PAREN)) {
                    col = col + readArgumentList();
                } else if (qualified || callOnly) {
                    throw PgErrors.syntax(
                            "syntax error at or near \"" + tokenInMessage(parser.peek()) + "\"");
                }
            }
            columns.add(col);
            StringBuilder opts = new StringBuilder();
            if (parser.matchKeyword("COLLATE")) {
                String collation = parser.readIdentifierOrString();
                if (parser.match(TokenType.DOT)) {
                    // Every collation PostgreSQL ships lives in pg_catalog, which is always on the
                    // search path, so a name written under it is written back without it.
                    String local = parser.readIdentifierOrString();
                    collation = "pg_catalog".equalsIgnoreCase(collation)
                            ? local : collation + "." + local;
                }
                ExpressionParser.validateCollationStatic(collation, parser.peek());
                // Kept so pg_get_indexdef can echo it, as PostgreSQL does.
                opts.append("collate:").append(collation);
            }
            if (collateAt > 0) parser.expect(TokenType.RIGHT_PAREN);
            // Capture opclass name (e.g. text_pattern_ops)
            if (!parser.isAtEnd() && (parser.check(TokenType.IDENTIFIER) || parser.check(TokenType.KEYWORD))
                    && !parser.checkKeyword("ASC") && !parser.checkKeyword("DESC")
                    && !parser.checkKeyword("NULLS") && !parser.checkKeyword("INCLUDE") && !parser.checkKeyword("WHERE")
                    && !parser.checkKeyword("WITH") && !parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                // Any bare name here is an operator class, whatever it is called: PostgreSQL's
                // grammar has nothing else in this position. Requiring an "_ops" suffix turned a
                // misspelt class into a syntax error instead of the 42704 PostgreSQL reports.
                String opclass = parser.advance().value();
                while (parser.check(TokenType.DOT)) {
                    parser.advance();
                    String local = parser.readIdentifier();
                    // A class PostgreSQL ships is in pg_catalog, which is always on the search
                    // path; any other schema stays written down, because it has to be looked up.
                    opclass = "pg_catalog".equalsIgnoreCase(opclass)
                            ? local : opclass + "." + local;
                }
                if (opts.length() > 0) opts.append(' ');
                opts.append("opclass:").append(opclass);
                // An operator class may be given parameters of its own. None of the classes this
                // engine carries takes any, but they have to be read before that can be said.
                if (parser.check(TokenType.LEFT_PAREN)) {
                    readArgumentList();
                    opts.append(" opclassoptions");
                }
            }
            // Capture ASC/DESC. No definition prints ASC back -- it is the direction an index
            // takes anyway -- but it is recorded all the same, because an access method with no
            // ordering of its own refuses the clause for having been written at all.
            if (parser.matchKeyword("ASC")) {
                if (opts.length() > 0) opts.append(' ');
                opts.append("ASC");
            } else if (parser.matchKeyword("DESC")) {
                if (opts.length() > 0) opts.append(' ');
                opts.append("DESC");
            }
            // Capture NULLS FIRST/LAST
            if (parser.matchKeyword("NULLS")) {
                if (parser.matchKeyword("FIRST")) {
                    if (opts.length() > 0) opts.append(' ');
                    opts.append("NULLS FIRST");
                } else if (parser.matchKeyword("LAST")) {
                    if (opts.length() > 0) opts.append(' ');
                    opts.append("NULLS LAST");
                }
            }
            columnOptions.add(opts.toString());
            // Nothing else belongs in the list, so whatever is here is reported where it stands.
            // Leaving it to the closing parenthesis named the token without the quotes a string
            // carries, and INTERVAL '1 day' was reported at 1 day rather than at '1 day'.
            if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                throw PgErrors.syntax(
                        "syntax error at or near \"" + tokenInMessage(parser.peek()) + "\"");
            }
        } while (parser.match(TokenType.COMMA));
        this.lastColumnOptions = columnOptions;
        return columns;
    }

    /**
     * How far ahead a COLLATE clause stands that closes this parenthesised key, or -1 when the key
     * ends in something else. A collation nested inside — {@code upper(b COLLATE "C")} — is an
     * expression over a collated value and stays where it was written, so only a clause the
     * closing parenthesis follows directly is one the key takes for its own.
     */
    private int collateClosingParens() {
        int depth = 1;
        int close = -1;
        for (int i = 1; close < 0; i++) {
            Token t = parser.peekAt(i);
            if (t.type() == TokenType.EOF) return -1;
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            else if (t.type() == TokenType.RIGHT_PAREN && --depth == 0) close = i;
        }
        if (close >= 4 && isNameOrString(parser.peekAt(close - 1))
                && parser.peekAt(close - 2).type() == TokenType.DOT
                && isNameOrString(parser.peekAt(close - 3))
                && isCollate(parser.peekAt(close - 4))) {
            return close - 4;
        }
        if (close >= 2 && isNameOrString(parser.peekAt(close - 1))
                && isCollate(parser.peekAt(close - 2))) {
            return close - 2;
        }
        return -1;
    }

    private static boolean isCollate(Token t) {
        return t.value() != null && "COLLATE".equalsIgnoreCase(t.value())
                && t.type() != TokenType.QUOTED_IDENTIFIER && t.type() != TokenType.STRING_LITERAL;
    }

    private static boolean isNameOrString(Token t) {
        return t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER
                || t.type() == TokenType.STRING_LITERAL;
    }

    /**
     * What a key may begin with, and what PostgreSQL says when it begins with something else.
     *
     * <p>The grammar admits three things here: a name a column may be called, a call, and a
     * parenthesised expression. A word PostgreSQL reserves outright is none of them, so a key that
     * starts with one is reported at that word — {@code CASE}, {@code NOT}, {@code ARRAY},
     * {@code SELECT} all stop there — unless it is one of the calls SQL spells with a keyword
     * instead of a name. A word that may name a function but not a column is only ever the start
     * of a call, which is why the answer for one written alone comes at the token after it; a word
     * that may name a column but never heads a call is read as the column, so an argument list
     * after it is reported at the parenthesis.
     *
     * @return true when the word read may only be the name of a call
     */
    private boolean requireIndexKeyStart(Token head) {
        if (head.type() == TokenType.QUOTED_IDENTIFIER) return false;
        String word = head.value() == null ? "" : head.value().toUpperCase(java.util.Locale.ROOT);
        if (KEY_CONSTRUCTS.contains(word)) return false;
        if (PgKeywords.isReserved(word)) {
            throw PgErrors.syntax("syntax error at or near \"" + tokenInMessage(head) + "\"");
        }
        if (!PgKeywords.canBeColumnName(word)) return true;
        if (PgKeywords.isColumnNameKeyword(word)
                && parser.peekAt(1).type() == TokenType.LEFT_PAREN) {
            throw PgErrors.syntax("syntax error at or near \"(\"");
        }
        return false;
    }

    /**
     * The calls SQL gives a keyword of its own, which stand where a call stands. The value
     * functions are among them, which is why CURRENT_DATE reaches the check that refuses it for
     * being no more than stable rather than being refused as a word out of place.
     */
    private static final java.util.Set<String> KEY_CONSTRUCTS =
            new java.util.HashSet<String>(java.util.Arrays.asList(
                    "CAST", "COALESCE", "COLLATION", "EXTRACT", "GREATEST", "LEAST", "MERGE_ACTION",
                    "NORMALIZE", "NULLIF", "OVERLAY", "POSITION", "SUBSTRING", "TREAT", "TRIM",
                    "XMLCONCAT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST", "XMLPARSE", "XMLPI",
                    "XMLROOT", "XMLSERIALIZE", "JSON", "JSON_ARRAY", "JSON_EXISTS", "JSON_OBJECT",
                    "JSON_QUERY", "JSON_SCALAR", "JSON_SERIALIZE", "JSON_VALUE",
                    "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA",
                    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "LOCALTIME",
                    "LOCALTIMESTAMP", "SESSION_USER", "SYSTEM_USER", "USER"));

    /** A token as PostgreSQL names it in a syntax error: a string keeps the quotes it was written in. */
    private static String tokenInMessage(Token t) {
        return t.type() == TokenType.STRING_LITERAL
                ? "'" + t.value().replace("'", "''") + "'" : t.value();
    }

    /**
     * The argument list of a call written as an index key, kept as the text it was written in.
     *
     * <p>One word is spaced off from the word before it, because a call written in SQL's own syntax
     * -- {@code SUBSTRING(s FROM 1 FOR 2)} -- otherwise runs together into a single name that
     * nothing can read back.
     */
    private String readArgumentList() {
        StringBuilder expr = new StringBuilder();
        int depth = 0;
        Token previous = null;
        do {
            Token t = parser.advance();
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            else if (t.type() == TokenType.RIGHT_PAREN) depth--;
            if (previous != null && isWord(previous) && isWord(t)) expr.append(' ');
            expr.append(indexTokenValue(t));
            previous = t;
        } while (depth > 0 && !parser.isAtEnd());
        return expr.toString();
    }

    /** True for the tokens that would run into one another if written side by side. */
    private static boolean isWord(Token t) {
        switch (t.type()) {
            case IDENTIFIER:
            case QUOTED_IDENTIFIER:
            case KEYWORD:
            case STRING_LITERAL:
            case INTEGER_LITERAL:
            case FLOAT_LITERAL:
                return true;
            default:
                return false;
        }
    }

    private static String indexTokenValue(Token t) {
        if (t.type() == TokenType.STRING_LITERAL) {
            return "'" + t.value().replace("'", "''") + "'";
        }
        // A quoted name is written back with its quotes: they are what say the name is one word,
        // and without them lower("A b") is read as two.
        if (t.type() == TokenType.QUOTED_IDENTIFIER) {
            return "\"" + t.value().replace("\"", "\"\"") + "\"";
        }
        return t.value();
    }
}
