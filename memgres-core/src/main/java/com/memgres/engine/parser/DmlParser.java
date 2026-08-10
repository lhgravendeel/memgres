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
            insertAlias = parser.readColumnName();
        }

        // Column list — disambiguate from parenthesized SELECT by scanning for query keywords
        List<String> columns = null;
        List<List<SubscriptExpr.Subscript>> columnSubscripts = new ArrayList<>();
        if (parser.check(TokenType.LEFT_PAREN) && parser.countLeadingParensBeforeQuery() < 0) {
            parser.expect(TokenType.LEFT_PAREN);
            columns = new ArrayList<>();
            do {
                columns.add(parser.readIdentifier());
                // A column may be named with brackets after it, which writes part of its value
                // rather than the whole of it, exactly as an UPDATE's assignment does.
                columnSubscripts.add(parseSubscripts());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
            boolean anySubscript = false;
            for (List<SubscriptExpr.Subscript> one : columnSubscripts) {
                if (one != null) anySubscript = true;
            }
            if (!anySubscript) columnSubscripts = null;
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
        } else if (parser.checkKeyword("SELECT") || parser.checkKeyword("WITH")
                || parser.checkKeyword("TABLE")) {
            // Parse SELECT which may include UNION/INTERSECT/EXCEPT
            selectStmt = parser.tryParseSetOp(parser.parseSelect());
        } else if (parser.check(TokenType.LEFT_PAREN)) {
            // A parenthesised source query, which is one arm of whatever set operation follows:
            // INSERT INTO t(id) (SELECT 1) UNION (SELECT 2) inserts both rows. Reading the
            // parentheses off and stopping there left the UNION for the statement level, which
            // made the INSERT itself an arm of a set operation -- so the INSERT ran and the width
            // complaint came afterwards, with one row already written.
            if (parser.countLeadingParensBeforeQuery() > 0) {
                selectStmt = parser.tryParseSetOp(parser.parseSetOpOperandPublic());
            } else {
                throw new ParseException("Expected VALUES, DEFAULT VALUES, or SELECT", parser.peek());
            }
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

        return new InsertStmt(schema, table, columns, values, selectStmt, onConflict, returning,
                withClauses, insertAlias, overridingSystemValue, overridingUserValue)
                .withColumnSubscripts(columnSubscripts);
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
        List<Expression> conflictExpressionAsts = null;
        String constraintName = null;
        Expression conflictWhere = null;

        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.expect(TokenType.LEFT_PAREN);
            // Conflict target list: entries may be bare column names or parenthesized
            // expressions, e.g. ON CONFLICT (queue_name, ((input->>'price_id'))). A mix of
            // both is normalized to conflictExpressions (bare names carried as their own
            // text) so the executor can match structurally against a constraint's
            // expression-column text regardless of which entries are plain identifiers.
            List<String> entries = parser.parseColumnOrExpressionList();
            if (parser.lastColumnListHadExpression) {
                conflictExpressions = entries;
                conflictExpressionAsts = parser.lastColumnListExpressions;
            } else {
                conflictColumns = entries;
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
            return new InsertStmt.OnConflict(conflictColumns, constraintName, true, null,
                    conflictWhere, conflictExpressions, null, conflictExpressionAsts);
        }

        parser.expectKeyword("UPDATE");
        parser.expectKeyword("SET");
        List<InsertStmt.SetClause> sets = parseSetClauses();
        // Optional WHERE clause for ON CONFLICT DO UPDATE
        Expression doUpdateWhere = null;
        if (parser.matchKeyword("WHERE")) {
            doUpdateWhere = parser.parseExpression();
        }
        return new InsertStmt.OnConflict(conflictColumns, constraintName, false, sets,
                conflictWhere, conflictExpressions, doUpdateWhere, conflictExpressionAsts);
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

        // WHERE [CURRENT OF cursor_name | expression]
        Expression where = null;
        if (parser.matchKeyword("WHERE")) {
            if (parser.matchKeyword("CURRENT")) {
                parser.expectKeyword("OF");
                String cursorName = parser.readIdentifier();
                where = new CurrentOfExpr(cursorName);
            } else {
                where = parser.parseExpression();
            }
        }

        // RETURNING
        List<SelectStmt.SelectTarget> returning = null;
        if (parser.matchKeyword("RETURNING")) {
            returning = parser.parseSelectTargets();
            if (parser.checkKeyword("ORDER")) throw new ParseException("syntax error at or near \"ORDER\"", parser.peek());
        }

        return new UpdateStmt(schema, table, alias, sets, from, where, returning, withClauses);
    }

    /** The brackets written after a name, or null when there are none. */
    private List<SubscriptExpr.Subscript> parseSubscripts() {
        List<SubscriptExpr.Subscript> subscripts = null;
        while (parser.check(TokenType.LEFT_BRACKET)) {
            parser.advance();
            Expression lower = null;
            Expression upper = null;
            boolean isSlice = false;
            if (parser.check(TokenType.COLON)) {
                parser.advance();
                isSlice = true;
                if (!parser.check(TokenType.RIGHT_BRACKET)) upper = parser.parseExpression();
            } else {
                lower = parser.parseExpression();
                if (parser.check(TokenType.COLON)) {
                    parser.advance();
                    isSlice = true;
                    if (!parser.check(TokenType.RIGHT_BRACKET)) upper = parser.parseExpression();
                }
            }
            parser.expect(TokenType.RIGHT_BRACKET);
            if (subscripts == null) subscripts = new ArrayList<>();
            subscripts.add(new SubscriptExpr.Subscript(lower, upper, isSlice));
        }
        return subscripts;
    }

    List<InsertStmt.SetClause> parseSetClauses() {
        List<InsertStmt.SetClause> clauses = new ArrayList<>();
        List<String> plainlyAssigned = new ArrayList<>();
        do {
            if (parser.check(TokenType.LEFT_PAREN)) {
                parseMultiColumnSet(clauses, plainlyAssigned);
                continue;
            }
            String col = parser.readIdentifier();
            String subField = null;
            // Check for composite field update: col.field = value
            if (parser.match(TokenType.DOT)) {
                subField = parser.readIdentifier();
            }
            // Subscripted update: col['key'] = value, col[2] = value, or col[1:2] = value.
            // A JSONB key and an array index look the same here; which one is meant depends on
            // the column's declared type, which only the executor knows. Each subscript is a whole
            // expression: reading one token apiece refused col[i], col[$1] and col[-1] outright.
            List<SubscriptExpr.Subscript> subscripts = parseSubscripts();
            parser.expect(TokenType.EQUALS);
            Expression val = parser.parseExpression();
            if (subscripts != null) {
                clauses.add(new InsertStmt.SetClause(col, val, subField, subscripts));
            } else {
                // A field of a composite column writes part of it, and writing several parts is
                // how a statement sets more than one field: "SET pos.x = 1, pos.y = 2" names pos
                // twice and is not two assignments to it.
                if (subField == null) rejectRepeatedAssignment(plainlyAssigned, col);
                clauses.add(new InsertStmt.SetClause(col, val, subField));
            }
        } while (parser.match(TokenType.COMMA));
        return clauses;
    }

    /**
     * One assignment may name several columns and take them from one row:
     * {@code SET (a, b) = (SELECT x, y FROM ...)} and {@code SET (a, b) = (1, 'z')}. The engine
     * stores an assignment per column, so the item is taken apart here -- a row constructor by its
     * elements, a sub-SELECT by a shared node each column reads its own field of.
     *
     * <p>The source has to be one of those two. A parenthesised expression that is not a row --
     * {@code (a) = (9)}, and {@code (a, b) = (9)} with it -- is written the same way but is not a
     * row constructor, and PostgreSQL refuses it as such rather than reading the value into the
     * first column.
     */
    private void parseMultiColumnSet(List<InsertStmt.SetClause> clauses, List<String> plainlyAssigned) {
        Token open = parser.peek();
        parser.expect(TokenType.LEFT_PAREN);
        List<String> columns = new ArrayList<>();
        do {
            columns.add(parser.readIdentifier());
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        parser.expect(TokenType.EQUALS);
        Expression source = parser.parseExpression();

        if (source instanceof SubqueryExpr) {
            int width = SelectStmt.writtenWidth(((SubqueryExpr) source).subquery());
            if (width > 0 && width != columns.size()) {
                throw mismatchedWidth(open);
            }
            for (int i = 0; i < columns.size(); i++) {
                rejectRepeatedAssignment(plainlyAssigned, columns.get(i));
                clauses.add(new InsertStmt.SetClause(columns.get(i),
                        new RowElementExpr(source, i, columns.size()), null));
            }
            return;
        }
        if (source instanceof ArrayExpr && ((ArrayExpr) source).isRow()) {
            List<Expression> elements = ((ArrayExpr) source).elements();
            if (elements.size() != columns.size()) {
                throw mismatchedWidth(open);
            }
            for (int i = 0; i < columns.size(); i++) {
                rejectRepeatedAssignment(plainlyAssigned, columns.get(i));
                clauses.add(new InsertStmt.SetClause(columns.get(i), elements.get(i), null));
            }
            return;
        }
        throw new com.memgres.engine.MemgresException(
                "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression",
                "0A000");
    }

    /**
     * A column takes one value from one statement, so naming it twice among the assignments is a
     * conflict PostgreSQL refuses rather than resolving. Only whole-column assignments count: a
     * subscript and a composite field each write part of a column, and several of those to the
     * same column are how a statement writes several parts of it.
     */
    private com.memgres.engine.MemgresException mismatchedWidth(Token open) {
        com.memgres.engine.MemgresException e = new com.memgres.engine.MemgresException(
                "number of columns does not match number of values", "42601");
        if (open.position() >= 0) e.setPosition(open.position() + 1);
        return e;
    }

    private void rejectRepeatedAssignment(List<String> plainlyAssigned, String column) {
        for (int i = 0; i < plainlyAssigned.size(); i++) {
            if (plainlyAssigned.get(i).equalsIgnoreCase(column)) {
                com.memgres.engine.MemgresException e = new com.memgres.engine.MemgresException(
                        "multiple assignments to same column \"" + column + "\"", "42601");
                throw e;
            }
        }
        plainlyAssigned.add(column);
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
            alias = parser.readColumnName();
        } else if (!parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF) && !parser.isAtEnd()
                && parser.isKeywordValidAsBareAlias()) {
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
            if (parser.matchKeyword("CURRENT")) {
                parser.expectKeyword("OF");
                String cursorName = parser.readIdentifier();
                where = new CurrentOfExpr(cursorName);
            } else {
                where = parser.parseExpression();
            }
        }

        List<SelectStmt.SelectTarget> returning = null;
        if (parser.matchKeyword("RETURNING")) {
            returning = parser.parseSelectTargets();
            if (parser.checkKeyword("ORDER")) throw new ParseException("syntax error at or near \"ORDER\"", parser.peek());
        }

        return new DeleteStmt(schema, table, alias, using, where, returning, withClauses);
    }

    MergeStmt parseMerge() {
        return parseMerge(null);
    }

    MergeStmt parseMerge(List<SelectStmt.CommonTableExpr> withClauses) {
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
            targetAlias = parser.readColumnName();
        } else if (!parser.checkKeyword("USING") && parser.isKeywordValidAsBareAlias()) {
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

                // Check for BY SOURCE / BY TARGET (PG 17+)
                boolean bySource = false;
                if (parser.matchKeyword("BY")) {
                    if (parser.matchIdentifier("SOURCE")) {
                        bySource = true;
                    } else if (parser.matchIdentifier("TARGET")) {
                        // BY TARGET is the default for NOT MATCHED, explicit is allowed
                    } else {
                        throw new ParseException("Expected SOURCE or TARGET after BY", parser.peek());
                    }
                }

                Expression andCondition = null;
                if (parser.matchKeyword("AND")) {
                    andCondition = parser.parseExpression();
                }
                parser.expectKeyword("THEN");

                if (bySource) {
                    // WHEN NOT MATCHED BY SOURCE: UPDATE SET ... / DELETE / DO NOTHING
                    if (parser.matchKeyword("UPDATE")) {
                        parser.expectKeyword("SET");
                        List<InsertStmt.SetClause> setClauses = parseSetClauses();
                        whenClauses.add(new MergeStmt.WhenNotMatchedBySource(andCondition, false, setClauses));
                    } else if (parser.matchKeyword("DELETE")) {
                        whenClauses.add(new MergeStmt.WhenNotMatchedBySource(andCondition, true, null));
                    } else if (parser.matchKeywords("DO", "NOTHING")) {
                        whenClauses.add(new MergeStmt.WhenNotMatchedBySource(andCondition, false, Cols.listOf()));
                    } else {
                        throw new ParseException("Expected UPDATE, DELETE, or DO NOTHING after WHEN NOT MATCHED BY SOURCE THEN", parser.peek());
                    }
                } else {
                    // WHEN NOT MATCHED [BY TARGET]: INSERT ... | DO NOTHING
                    if (parser.matchKeywords("DO", "NOTHING")) {
                        whenClauses.add(new MergeStmt.WhenNotMatched(andCondition, true, null, null));
                    } else if (parser.matchKeyword("INSERT")) {
                        // INSERT DEFAULT VALUES — no columns, no values
                        if (parser.matchKeywords("DEFAULT", "VALUES")) {
                            whenClauses.add(new MergeStmt.WhenNotMatched(andCondition, false, null, null));
                        } else {
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
                        }
                    } else {
                        throw new ParseException("Expected INSERT or DO NOTHING after WHEN NOT MATCHED THEN", parser.peek());
                    }
                }
            } else {
                throw new ParseException("Expected MATCHED or NOT MATCHED after WHEN", parser.peek());
            }
        }

        if (whenClauses.isEmpty()) {
            throw new ParseException("MERGE statement requires at least one WHEN clause", parser.peek());
        }

        // RETURNING
        List<SelectStmt.SelectTarget> returning = null;
        if (parser.matchKeyword("RETURNING")) {
            returning = parser.parseSelectTargets();
            if (returning.isEmpty()) throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"", parser.peek());
        }

        return new MergeStmt(schema, table, targetAlias, source, onCondition, whenClauses, returning, withClauses);
    }
}
