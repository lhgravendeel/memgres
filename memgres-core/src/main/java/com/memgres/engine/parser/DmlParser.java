package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DML statement parsing (INSERT, UPDATE, DELETE, MERGE), extracted from Parser to reduce class size.
 */
class DmlParser {
    private final Parser parser;

    DmlParser(Parser parser) {
        this.parser = parser;
    }

    InsertStmt parseInsert() {
        return parseInsert(null);
    }

    InsertStmt parseInsert(List<SelectStmt.CommonTableExpr> withClauses) {
        parser.expectKeyword("INSERT");
        parser.expectKeyword("INTO");

        String schema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = table;
            table = parser.readIdentifier();
        }

        // INSERT INTO t AS alias: PG allows alias for RETURNING/ON CONFLICT references
        String insertAlias = null;
        if (parser.matchKeyword("AS")) {
            insertAlias = parser.readIdentifier();
        }

        // Column list
        List<String> columns = null;
        if (parser.check(TokenType.LEFT_PAREN) && !isNextKeywordSelect()) {
            parser.expect(TokenType.LEFT_PAREN);
            columns = new ArrayList<>();
            do {
                columns.add(parser.readIdentifier());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        }

        // OVERRIDING SYSTEM VALUE / OVERRIDING USER VALUE
        boolean overridingSystemValue = false;
        boolean overridingUserValue = false;
        if (parser.matchKeyword("OVERRIDING")) {
            if (parser.matchKeyword("SYSTEM")) overridingSystemValue = true;
            else if (parser.matchKeyword("USER")) overridingUserValue = true;
            parser.expectKeyword("VALUE");
        }

        // DEFAULT VALUES, VALUES, or SELECT
        List<List<Expression>> values = null;
        Statement selectStmt = null;

        if (parser.matchKeywords("DEFAULT", "VALUES")) {
            // INSERT INTO t DEFAULT VALUES: single row with all defaults
            if (overridingSystemValue || overridingUserValue) {
                throw new ParseException("cannot use DEFAULT VALUES with OVERRIDING clause", parser.peek());
            }
            values = Cols.listOf(Cols.listOf());
        } else if (parser.matchKeyword("VALUES")) {
            values = new ArrayList<>();
            do {
                parser.expect(TokenType.LEFT_PAREN);
                values.add(parser.parseExpressionList());
                parser.expect(TokenType.RIGHT_PAREN);
            } while (parser.match(TokenType.COMMA));
        } else if (parser.checkKeyword("SELECT") || parser.checkKeyword("WITH")) {
            // Parse SELECT which may include UNION/INTERSECT/EXCEPT
            selectStmt = parser.tryParseSetOp(parser.parseSelect());
        } else {
            throw new ParseException("Expected VALUES, DEFAULT VALUES, or SELECT", parser.peek());
        }

        // ON CONFLICT
        InsertStmt.OnConflict onConflict = null;
        if (parser.matchKeywords("ON", "CONFLICT")) {
            onConflict = parseOnConflict();
        }

        // RETURNING
        List<SelectStmt.SelectTarget> returning = null;
        if (parser.matchKeyword("RETURNING")) {
            returning = parser.parseSelectTargets();
            if (returning.isEmpty()) throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"", parser.peek());
            if (parser.checkKeyword("ORDER")) throw new ParseException("syntax error at or near \"ORDER\"", parser.peek());
        }

        return new InsertStmt(schema, table, columns, values, selectStmt, onConflict, returning, withClauses, insertAlias, overridingSystemValue, overridingUserValue);
    }

    boolean isNextKeywordSelect() {
        // Look ahead: is the token after ( a SELECT?
        if (parser.pos + 1 < parser.tokens.size()) {
            Token next = parser.tokens.get(parser.pos + 1);
            return next.type() == TokenType.KEYWORD &&
                    (next.value().equals("SELECT") || next.value().equals("WITH"));
        }
        return false;
    }

    InsertStmt.OnConflict parseOnConflict() {
        List<String> conflictColumns = null;
        List<String> conflictExpressions = null;
        String constraintName = null;
        Expression conflictWhere = null;

        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.expect(TokenType.LEFT_PAREN);
            // Check if the first element is a parenthesized expression like ((lower(email)))
            if (parser.check(TokenType.LEFT_PAREN)) {
                // Expression-based conflict targets
                conflictExpressions = new ArrayList<>();
                do {
                    parser.expect(TokenType.LEFT_PAREN);
                    // Capture the expression text by recording start position and parsing
                    int exprStart = parser.pos;
                    parser.parseExpression(); // consume the expression
                    // Reconstruct expression text from tokens
                    StringBuilder sb = new StringBuilder();
                    for (int i = exprStart; i < parser.pos; i++) {
                        if (i > exprStart) {
                            // Add space unless previous token is ( or current token is ) or ,
                            Token prev = parser.tokens.get(i - 1);
                            Token cur = parser.tokens.get(i);
                            if (prev.type() != TokenType.LEFT_PAREN && cur.type() != TokenType.RIGHT_PAREN
                                    && cur.type() != TokenType.COMMA && prev.type() != TokenType.COMMA) {
                                sb.append(' ');
                            }
                        }
                        sb.append(parser.tokens.get(i).value());
                    }
                    conflictExpressions.add(sb.toString());
                    parser.expect(TokenType.RIGHT_PAREN);
                } while (parser.match(TokenType.COMMA));
            } else {
                conflictColumns = new ArrayList<>();
                do {
                    conflictColumns.add(parser.readIdentifier());
                } while (parser.match(TokenType.COMMA));
            }
            parser.expect(TokenType.RIGHT_PAREN);
            // Optional WHERE clause on conflict target (partial index predicate)
            if (parser.matchKeyword("WHERE")) {
                conflictWhere = parser.parseExpression();
            }
        } else if (parser.matchKeywords("ON", "CONSTRAINT")) {
            constraintName = parser.readIdentifier();
        }

        parser.expectKeyword("DO");
        if (parser.matchKeyword("NOTHING")) {
            return new InsertStmt.OnConflict(conflictColumns, constraintName, true, null, conflictWhere, conflictExpressions);
        }

        parser.expectKeyword("UPDATE");
        parser.expectKeyword("SET");
        List<InsertStmt.SetClause> sets = parseSetClauses();
        // Optional WHERE clause for ON CONFLICT DO UPDATE
        if (parser.matchKeyword("WHERE")) {
            parser.parseExpression(); // consume the WHERE condition (stored but not yet used for filtering)
        }
        return new InsertStmt.OnConflict(conflictColumns, constraintName, false, sets, conflictWhere, conflictExpressions);
    }

    UpdateStmt parseUpdate() {
        return parseUpdate(null);
    }

    UpdateStmt parseUpdate(List<SelectStmt.CommonTableExpr> withClauses) {
        parser.expectKeyword("UPDATE");
        parser.matchKeyword("ONLY"); // optional ONLY keyword

        String schema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = table;
            table = parser.readIdentifier();
        }

        // Optional alias: UPDATE tbl alias SET ... or UPDATE tbl AS alias SET ...
        String alias = null;
        parser.matchKeyword("AS");
        if (!parser.checkKeyword("SET") && (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.KEYWORD)) {
            // Next token is not SET, so it must be an alias
            String nextVal = parser.peek().value();
            if (!nextVal.equalsIgnoreCase("SET")) {
                alias = parser.readIdentifier();
            }
        }

        parser.expectKeyword("SET");
        List<InsertStmt.SetClause> sets = parseSetClauses();

        // FROM
        List<SelectStmt.FromItem> from = null;
        if (parser.matchKeyword("FROM")) {
            from = parser.parseFromList();
        }

        // WHERE
        Expression where = null;
        if (parser.matchKeyword("WHERE")) {
            where = parser.parseExpression();
        }

        // RETURNING
        List<SelectStmt.SelectTarget> returning = null;
        if (parser.matchKeyword("RETURNING")) {
            returning = parser.parseSelectTargets();
            if (parser.checkKeyword("ORDER")) throw new ParseException("syntax error at or near \"ORDER\"", parser.peek());
        }

        return new UpdateStmt(schema, table, alias, sets, from, where, returning, withClauses);
    }

    List<InsertStmt.SetClause> parseSetClauses() {
        List<InsertStmt.SetClause> clauses = new ArrayList<>();
        do {
            String col = parser.readIdentifier();
            parser.expect(TokenType.EQUALS);
            Expression val = parser.parseExpression();
            clauses.add(new InsertStmt.SetClause(col, val));
        } while (parser.match(TokenType.COMMA));
        return clauses;
    }

    DeleteStmt parseDelete() {
        return parseDelete(null);
    }

    DeleteStmt parseDelete(List<SelectStmt.CommonTableExpr> withClauses) {
        parser.expectKeyword("DELETE");
        parser.expectKeyword("FROM");

        parser.matchKeyword("ONLY"); // optional ONLY keyword
        String schema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = table;
            table = parser.readIdentifier();
        }
        // Optional alias: DELETE FROM tbl alias WHERE ... or DELETE FROM tbl AS alias WHERE ...
        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readIdentifier();
        } else if (!parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF) && !parser.isAtEnd()
                && (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                    || parser.isKeywordValidAsBareAlias())) {
            alias = parser.readIdentifier();
        }

        // USING clause
        List<SelectStmt.FromItem> using = null;
        if (parser.matchKeyword("USING")) {
            using = new ArrayList<>();
            using.add(parser.parseFromItem());
            while (parser.match(TokenType.COMMA)) {
                using.add(parser.parseFromItem());
            }
        }

        Expression where = null;
        if (parser.matchKeyword("WHERE")) {
            where = parser.parseExpression();
        }

        List<SelectStmt.SelectTarget> returning = null;
        if (parser.matchKeyword("RETURNING")) {
            returning = parser.parseSelectTargets();
            if (parser.checkKeyword("ORDER")) throw new ParseException("syntax error at or near \"ORDER\"", parser.peek());
        }

        return new DeleteStmt(schema, table, alias, using, where, returning, withClauses);
    }

    MergeStmt parseMerge() {
        parser.expectKeyword("MERGE");
        parser.expectKeyword("INTO");

        // Target table: [schema.]table [AS alias]
        String schema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = table;
            table = parser.readIdentifier();
        }
        String targetAlias = null;
        if (parser.matchKeyword("AS")) {
            targetAlias = parser.readIdentifier();
        } else if (!parser.checkKeyword("USING") && (parser.check(TokenType.IDENTIFIER) || parser.check(TokenType.QUOTED_IDENTIFIER)
                || parser.isKeywordValidAsBareAlias())) {
            targetAlias = parser.readIdentifier();
        }

        // USING source ON condition
        parser.expectKeyword("USING");
        SelectStmt.FromItem source = parser.parseFromItem();
        parser.expectKeyword("ON");
        Expression onCondition = parser.parseExpression();

        // WHEN clauses
        List<MergeStmt.WhenClause> whenClauses = new ArrayList<>();
        while (parser.checkKeyword("WHEN")) {
            parser.advance(); // consume WHEN

            if (parser.matchKeyword("MATCHED")) {
                // WHEN MATCHED [AND condition] THEN UPDATE SET ... | DELETE
                Expression andCondition = null;
                if (parser.matchKeyword("AND")) {
                    andCondition = parser.parseExpression();
                }
                parser.expectKeyword("THEN");

                if (parser.matchKeyword("UPDATE")) {
                    parser.expectKeyword("SET");
                    List<InsertStmt.SetClause> setClauses = parseSetClauses();
                    whenClauses.add(new MergeStmt.WhenMatched(andCondition, false, setClauses));
                } else if (parser.matchKeyword("DELETE")) {
                    whenClauses.add(new MergeStmt.WhenMatched(andCondition, true, null));
                } else if (parser.matchKeywords("DO", "NOTHING")) {
                    whenClauses.add(new MergeStmt.WhenMatched(andCondition, false, Cols.listOf()));
                } else {
                    throw new ParseException("Expected UPDATE, DELETE, or DO NOTHING after WHEN MATCHED THEN", parser.peek());
                }
            } else if (parser.matchKeyword("NOT")) {
                parser.expectKeyword("MATCHED");
                // WHEN NOT MATCHED [AND condition] THEN INSERT ... | DO NOTHING
                Expression andCondition = null;
                if (parser.matchKeyword("AND")) {
                    andCondition = parser.parseExpression();
                }
                parser.expectKeyword("THEN");

                if (parser.matchKeywords("DO", "NOTHING")) {
                    whenClauses.add(new MergeStmt.WhenNotMatched(andCondition, true, null, null));
                } else if (parser.matchKeyword("INSERT")) {
                    List<String> columns = null;
                    if (parser.match(TokenType.LEFT_PAREN)) {
                        columns = new ArrayList<>();
                        do {
                            columns.add(parser.readIdentifier());
                        } while (parser.match(TokenType.COMMA));
                        parser.expect(TokenType.RIGHT_PAREN);
                    }
                    parser.expectKeyword("VALUES");
                    parser.expect(TokenType.LEFT_PAREN);
                    List<Expression> values = new ArrayList<>();
                    do {
                        values.add(parser.parseExpression());
                    } while (parser.match(TokenType.COMMA));
                    parser.expect(TokenType.RIGHT_PAREN);
                    whenClauses.add(new MergeStmt.WhenNotMatched(andCondition, false, columns, values));
                } else {
                    throw new ParseException("Expected INSERT or DO NOTHING after WHEN NOT MATCHED THEN", parser.peek());
                }
            } else {
                throw new ParseException("Expected MATCHED or NOT MATCHED after WHEN", parser.peek());
            }
        }

        if (whenClauses.isEmpty()) {
            throw new ParseException("MERGE statement requires at least one WHEN clause", parser.peek());
        }

        return new MergeStmt(schema, table, targetAlias, source, onCondition, whenClauses);
    }
}
