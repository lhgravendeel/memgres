package com.memgres.engine.parser;

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
            includeColumns = parser.parseIdentifierList();
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

        if (parser.matchKeyword("WITH")) {
            if (parser.check(TokenType.LEFT_PAREN)) {
                DdlTableParser.consumeUntilParen(parser);
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
                if (wt.type() == TokenType.STRING_LITERAL) {
                    sb.append('\'').append(wt.value().replace("'", "''")).append('\'');
                } else {
                    sb.append(wt.value());
                }
                sb.append(' ');
            }
            whereClause = sb.toString().trim();
        }

        return new CreateIndexStmt(name, indexSchema, table, columns, unique, ifNotExists, concurrently,
                method, includeColumns, whereClause, columnOptions, nullsNotDistinct);
    }

    /** Parsed column options list, populated by parseIndexColumnList. */
    List<String> lastColumnOptions;

    private List<String> parseIndexColumnList() {
        List<String> columns = new ArrayList<>();
        List<String> columnOptions = new ArrayList<>();
        do {
            String col;
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
                col = expr.toString().trim();
                if (col.startsWith("(") && col.endsWith(")")) {
                    col = col.substring(1, col.length() - 1).trim();
                }
            } else {
                col = parser.readIdentifier();
                if (parser.check(TokenType.LEFT_PAREN)) {
                    StringBuilder expr = new StringBuilder(col);
                    int depth = 0;
                    do {
                        Token t = parser.advance();
                        if (t.type() == TokenType.LEFT_PAREN) depth++;
                        else if (t.type() == TokenType.RIGHT_PAREN) depth--;
                        expr.append(indexTokenValue(t));
                    } while (depth > 0 && !parser.isAtEnd());
                    col = expr.toString();
                }
            }
            columns.add(col);
            StringBuilder opts = new StringBuilder();
            if (parser.matchKeyword("COLLATE")) {
                String collation = parser.readIdentifierOrString();
                if (parser.match(TokenType.DOT)) collation = collation + "." + parser.readIdentifierOrString();
                ExpressionParser.validateCollationStatic(collation, parser.peek());
            }
            // Capture opclass name (e.g. text_pattern_ops)
            if (!parser.isAtEnd() && (parser.check(TokenType.IDENTIFIER) || parser.check(TokenType.KEYWORD))
                    && !parser.checkKeyword("ASC") && !parser.checkKeyword("DESC")
                    && !parser.checkKeyword("NULLS") && !parser.checkKeyword("INCLUDE") && !parser.checkKeyword("WHERE")
                    && !parser.checkKeyword("WITH") && !parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                Token next = parser.peek();
                if (next.value().endsWith("_ops") || next.value().endsWith("_pattern_ops")
                        || next.value().contains("_")) {
                    String opclass = parser.advance().value();
                    if (opts.length() > 0) opts.append(' ');
                    opts.append("opclass:").append(opclass);
                }
            }
            // Capture ASC/DESC
            if (parser.matchKeyword("ASC")) {
                // ASC is default, don't record
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
        } while (parser.match(TokenType.COMMA));
        this.lastColumnOptions = columnOptions;
        return columns;
    }

    private static String indexTokenValue(Token t) {
        if (t.type() == TokenType.STRING_LITERAL) {
            return "'" + t.value().replace("'", "''") + "'";
        }
        return t.value();
    }
}
