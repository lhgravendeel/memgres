package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SELECT statement parsing, extracted from Parser to reduce class size.
 */
class SelectParser {
    private final Parser parser;

    SelectParser(Parser parser) {
        this.parser = parser;
    }

    Statement tryParseSetOp(Statement left) {
        // INTERSECT has higher precedence than UNION/EXCEPT.
        // UNION and EXCEPT have the same precedence and are left-associative.
        // Grammar: set_expr = intersect_expr ((UNION|EXCEPT) [ALL] intersect_expr)*
        //          intersect_expr = select_term (INTERSECT [ALL] select_term)*

        // First absorb any higher-precedence INTERSECT
        left = parseIntersectChain(left);

        // No UNION/EXCEPT → handle trailing clauses from pure INTERSECT chain
        if (!parser.checkKeyword("UNION") && !parser.checkKeyword("EXCEPT")) {
            if (left instanceof SetOpStmt) {
                left = bubbleUpTrailingClauses(left);
                if (left instanceof SetOpStmt) {
                    SetOpStmt sop = (SetOpStmt) left;
                    List<SelectStmt.OrderByItem> orderBy = sop.orderBy();
                    Expression limit = sop.limit();
                    Expression offset = sop.offset();
                    boolean changed = false;
                    if (parser.matchKeywords("ORDER", "BY")) { orderBy = parser.parseOrderByList(); changed = true; }
                    if (parser.matchKeyword("LIMIT")) { if (!parser.matchKeyword("ALL")) limit = parser.parsePrimary(); changed = true; }
                    if (parser.matchKeyword("OFFSET")) { offset = parser.parsePrimary(); changed = true; }
                    if (changed) {
                        left = new SetOpStmt(sop.left(), sop.op(), sop.all(), sop.right(), orderBy, limit, offset);
                    }
                }
            }
            return left;
        }

        // UNION/EXCEPT loop (left-associative)
        while (parser.checkKeyword("UNION") || parser.checkKeyword("EXCEPT")) {
            SetOpStmt.SetOpType opType;
            if (parser.checkKeyword("UNION")) { parser.advance(); opType = SetOpStmt.SetOpType.UNION; }
            else { parser.advance(); opType = SetOpStmt.SetOpType.EXCEPT; }

            boolean all = parser.matchKeyword("ALL");
            parser.matchKeyword("DISTINCT");

            Statement right;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); right = parser.parseSelect(); parser.expect(TokenType.RIGHT_PAREN);
            } else {
                right = parser.parseSelect();
            }

            // Right side may have higher-precedence INTERSECT
            right = parseIntersectChain(right);

            left = new SetOpStmt(left, opType, all, right, null, null, null);
        }

        // Transfer ORDER BY/LIMIT/OFFSET from rightmost leaf to outermost SetOpStmt
        left = bubbleUpTrailingClauses(left);

        // Also check for explicit ORDER BY/LIMIT/OFFSET tokens after the set ops
        if (left instanceof SetOpStmt) {
            SetOpStmt sop = (SetOpStmt) left;
            List<SelectStmt.OrderByItem> orderBy = sop.orderBy();
            Expression limit = sop.limit();
            Expression offset = sop.offset();
            boolean changed = false;

            if (parser.matchKeywords("ORDER", "BY")) { orderBy = parser.parseOrderByList(); changed = true; }
            if (parser.matchKeyword("LIMIT")) { if (!parser.matchKeyword("ALL")) limit = parser.parsePrimary(); changed = true; }
            if (parser.matchKeyword("OFFSET")) { offset = parser.parsePrimary(); changed = true; }

            if (changed) {
                left = new SetOpStmt(sop.left(), sop.op(), sop.all(), sop.right(), orderBy, limit, offset);
            }
        }

        return left;
    }

    /** Parse a chain of INTERSECT operations (higher precedence, left-associative). */
    Statement parseIntersectChain(Statement left) {
        while (parser.checkKeyword("INTERSECT")) {
            parser.advance();
            boolean all = parser.matchKeyword("ALL");
            parser.matchKeyword("DISTINCT");

            Statement right;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); right = parser.parseSelect(); parser.expect(TokenType.RIGHT_PAREN);
            } else {
                right = parser.parseSelect();
            }

            left = new SetOpStmt(left, SetOpStmt.SetOpType.INTERSECT, all, right, null, null, null);
        }
        return left;
    }

    /**
     * Walk the right spine of a set-op tree and move ORDER BY / LIMIT / OFFSET
     * from the deepest right SelectStmt up to the root SetOpStmt.
     */
    Statement bubbleUpTrailingClauses(Statement stmt) {
        if (!(stmt instanceof SetOpStmt)) return stmt;
        SetOpStmt sop = (SetOpStmt) stmt;

        Statement processedRight = bubbleUpTrailingClauses(sop.right());

        List<SelectStmt.OrderByItem> orderBy = null;
        Expression limit = null;
        Expression offset = null;
        Statement cleanRight = processedRight;

        if (processedRight instanceof SelectStmt) {
            SelectStmt rsel = (SelectStmt) processedRight;
            if (rsel.orderBy() != null || rsel.limit() != null || rsel.offset() != null) {
                orderBy = rsel.orderBy();
                limit = rsel.limit();
                offset = rsel.offset();
                cleanRight = new SelectStmt(rsel.distinct(), rsel.targets(), rsel.from(), rsel.where(),
                        rsel.groupBy(), rsel.having(), null, null, null, rsel.withClauses());
            }
        } else if (processedRight instanceof SetOpStmt) {
            SetOpStmt rightSop = (SetOpStmt) processedRight;
            if (rightSop.orderBy() != null || rightSop.limit() != null || rightSop.offset() != null) {
                orderBy = rightSop.orderBy();
                limit = rightSop.limit();
                offset = rightSop.offset();
                cleanRight = new SetOpStmt(rightSop.left(), rightSop.op(), rightSop.all(), rightSop.right(), null, null, null);
            }
        }

        if (orderBy != null || limit != null || offset != null) {
            return new SetOpStmt(sop.left(), sop.op(), sop.all(), cleanRight, orderBy, limit, offset);
        }

        if (processedRight != sop.right()) {
            return new SetOpStmt(sop.left(), sop.op(), sop.all(), processedRight, sop.orderBy(), sop.limit(), sop.offset());
        }

        return stmt;
    }

    Statement parseSelectFull() {
        // WITH clause (CTEs)
        List<SelectStmt.CommonTableExpr> withClauses = null;
        if (parser.checkKeyword("WITH")) {
            withClauses = parseWithClause();
        }

        Statement body = parseSelectBody();
        if (body instanceof CreateTableAsStmt) {
            CreateTableAsStmt ctas = (CreateTableAsStmt) body;
            // SELECT INTO: the inner query needs WITH clauses attached
            if (withClauses != null && ctas.query() instanceof SelectStmt) {
                SelectStmt sel = (SelectStmt) ctas.query();
                SelectStmt withSel = new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                        sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), sel.orderBy(), sel.limit(), sel.offset(), withClauses, sel.groupingSets(), sel.lockClause());
                return new CreateTableAsStmt(ctas.schema(), ctas.name(), ctas.ifNotExists(), ctas.temporary(), withSel, ctas.withData());
            }
            return ctas;
        }
        SelectStmt sel = (SelectStmt) body;
        return new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), sel.orderBy(), sel.limit(), sel.offset(), withClauses, sel.groupingSets(), sel.lockClause());
    }

    /**
     * Parse the SELECT body (without WITH clause). Used by both parseSelect() and parseWithStatement().
     */
    Statement parseSelectBody() {
        parser.expectKeyword("SELECT");

        boolean distinct = false;
        List<Expression> distinctOn = null;
        if (parser.matchKeyword("DISTINCT")) {
            distinct = true;
            // Check for DISTINCT ON (expr, expr, ...)
            if (parser.matchKeyword("ON")) {
                parser.expect(TokenType.LEFT_PAREN);
                distinctOn = parser.parseExpressionList();
                parser.expect(TokenType.RIGHT_PAREN);
            }
            // DISTINCT DISTINCT is a syntax error
            if (parser.checkKeyword("DISTINCT")) {
                throw new ParseException("syntax error at or near \"DISTINCT\"", parser.peek());
            }
        }
        if (parser.matchKeyword("ALL")) {
            // SELECT ALL is default, just consume it
            // ALL DISTINCT is a syntax error
            if (parser.checkKeyword("DISTINCT")) {
                throw new ParseException("syntax error at or near \"DISTINCT\"", parser.peek());
            }
        }

        // Parse target list (empty for SELECT FROM ... existence checks, or bare SELECT)
        List<SelectStmt.SelectTarget> targets;
        if (parser.isAtEnd() || parser.check(TokenType.SEMICOLON)) {
            // bare SELECT with nothing; PG 18 returns one row with zero columns
            targets = Cols.listOf();
        } else if (parser.checkKeyword("FROM") || parser.checkKeyword("INTO")
                || parser.check(TokenType.RIGHT_PAREN)) {
            targets = Cols.listOf(); // empty target list
        } else {
            targets = parseSelectTargets();
        }

        // SELECT INTO [TEMP|TEMPORARY] table_name -> rewrite as CREATE TABLE AS SELECT
        String selectIntoTable = null;
        String selectIntoSchema = null;
        boolean selectIntoTemp = false;
        if (parser.checkKeyword("INTO") && !parser.checkKeywordAt(1, "STRICT")) {
            int saved = parser.position();
            parser.advance(); // consume INTO
            boolean tempFlag = parser.matchKeyword("TEMPORARY") || parser.matchKeyword("TEMP");
            String tbl = parser.readIdentifier();
            String sch = null;
            if (parser.match(TokenType.DOT)) {
                sch = tbl;
                tbl = parser.readIdentifier();
            }
            // This is a SELECT INTO if followed by FROM, WHERE, GROUP, ORDER, LIMIT, OFFSET, set-ops, HAVING, FETCH, or end
            if (parser.isAtEnd() || parser.checkKeyword("FROM") || parser.checkKeyword("WHERE") || parser.checkKeyword("GROUP")
                    || parser.checkKeyword("ORDER") || parser.checkKeyword("LIMIT") || parser.checkKeyword("OFFSET")
                    || parser.checkKeyword("UNION") || parser.checkKeyword("INTERSECT") || parser.checkKeyword("EXCEPT")
                    || parser.checkKeyword("HAVING") || parser.checkKeyword("FETCH") || parser.check(TokenType.SEMICOLON)) {
                selectIntoTable = tbl;
                selectIntoSchema = sch;
                selectIntoTemp = tempFlag;
            } else {
                parser.resetPosition(saved);
            }
        }

        // FROM
        List<SelectStmt.FromItem> from = null;
        boolean sawFrom = false, sawWhere = false, sawGroupBy = false, sawHaving = false;
        boolean sawOrderBy = false, sawLimit = false;
        if (parser.matchKeyword("FROM")) {
            from = parseFromList();
            sawFrom = true;
        }

        // WHERE
        Expression where = null;
        if (parser.matchKeyword("WHERE")) {
            if (sawGroupBy) throw new ParseException("syntax error at or near \"WHERE\"", parser.peek());
            if (sawOrderBy) throw new ParseException("syntax error at or near \"WHERE\"", parser.peek());
            where = parser.parseExpression();
            sawWhere = true;
            if (parser.checkKeyword("WHERE")) throw new ParseException("Multiple WHERE clauses", parser.peek());
        }

        // GROUP BY
        List<Expression> groupBy = null;
        List<List<Expression>> groupingSets = null;
        if (parser.matchKeywords("GROUP", "BY")) {
            if (sawOrderBy) throw new ParseException("syntax error at or near \"GROUP\"", parser.peek());
            if (sawLimit) throw new ParseException("syntax error at or near \"GROUP\"", parser.peek());
            // Parse potentially multiple GROUP BY elements that may include GROUPING SETS/ROLLUP/CUBE
            groupingSets = parseGroupByClause();
            if (groupingSets != null) {
                // Extract "representative" groupBy columns (all columns appearing in any set, for validation)
                java.util.Set<String> seen = new java.util.LinkedHashSet<>();
                List<Expression> allCols = new ArrayList<>();
                for (List<Expression> gs : groupingSets) {
                    for (Expression e : gs) {
                        String key = e.toString();
                        if (seen.add(key)) allCols.add(e);
                    }
                }
                groupBy = allCols;
            } else {
                groupBy = parseGroupByList();
            }
            sawGroupBy = true;
        }

        // HAVING
        Expression having = null;
        if (parser.matchKeyword("HAVING")) {
            having = parser.parseExpression();
            sawHaving = true;
        }

        // WINDOW clause: WINDOW name AS (window_spec) [, ...]
        List<SelectStmt.WindowDef> windowDefs = null;
        if (parser.matchKeyword("WINDOW")) {
            windowDefs = new ArrayList<>();
            do {
                String winName = parser.readIdentifier();
                parser.expectKeyword("AS");
                parser.expect(TokenType.LEFT_PAREN);
                List<Expression> winPartitionBy = null;
                List<SelectStmt.OrderByItem> winOrderBy = null;
                WindowFuncExpr.FrameClause winFrame = null;
                if (parser.matchKeywords("PARTITION", "BY")) {
                    winPartitionBy = parser.parseExpressionList();
                }
                if (parser.matchKeywords("ORDER", "BY")) {
                    winOrderBy = parser.parseOrderByList();
                }
                if (parser.checkKeyword("ROWS") || parser.checkKeyword("RANGE") || parser.checkKeyword("GROUPS")) {
                    winFrame = parser.parseWindowFrame();
                }
                parser.expect(TokenType.RIGHT_PAREN);
                windowDefs.add(new SelectStmt.WindowDef(winName, winPartitionBy, winOrderBy, winFrame));
            } while (parser.match(TokenType.COMMA));
        }

        // ORDER BY
        List<SelectStmt.OrderByItem> orderBy = null;
        if (parser.checkKeyword("ORDER")) {
            if (!parser.matchKeywords("ORDER", "BY")) {
                throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"", parser.peek());
            }
            orderBy = parser.parseOrderByList();
            sawOrderBy = true;
            if (parser.checkKeyword("ORDER")) throw new ParseException("Multiple ORDER BY clauses", parser.peek());
        }

        // LIMIT
        Expression limit = null;
        if (parser.matchKeyword("LIMIT")) {
            if (parser.matchKeyword("ALL")) {
                limit = null; // LIMIT ALL = no limit
            } else {
                limit = parseLimitOffsetExpr();
            }
            sawLimit = true;
            if (parser.checkKeyword("LIMIT")) throw new ParseException("Multiple LIMIT clauses", parser.peek());
        }

        // OFFSET [n] [ROWS]
        Expression offset = null;
        if (parser.matchKeyword("OFFSET")) {
            offset = parseLimitOffsetExpr();
            parser.matchKeyword("ROW");    // optional ROWS/ROW keyword after offset value
            parser.matchKeyword("ROWS");
        }

        // FETCH FIRST|NEXT [n] ROW|ROWS ONLY (SQL standard equivalent of LIMIT)
        if (parser.matchKeyword("FETCH")) {
            parser.matchKeyword("FIRST");
            parser.matchKeyword("NEXT");
            if (!parser.checkKeyword("ROW") && !parser.checkKeyword("ROWS")) {
                limit = parseLimitOffsetExpr();
            } else {
                limit = Literal.ofInt("1"); // FETCH FIRST ROW ONLY = FETCH FIRST 1 ROW ONLY
            }
            parser.matchKeyword("ROW");
            parser.matchKeyword("ROWS");
            parser.matchKeyword("ONLY");
            // PG also allows OFFSET after FETCH: FETCH FIRST 3 ROWS ONLY OFFSET 2
            if (offset == null && parser.matchKeyword("OFFSET")) {
                offset = parseLimitOffsetExpr();
                parser.matchKeyword("ROW");
                parser.matchKeyword("ROWS");
            }
        }

        // FOR UPDATE / FOR NO KEY UPDATE / FOR SHARE / FOR KEY SHARE
        // PG 18 allows multiple FOR clauses, e.g. FOR UPDATE FOR SHARE
        String lockMode = null;
        boolean nowait = false;
        boolean skipLocked = false;
        while (parser.checkKeyword("FOR")) {
            parser.advance(); // consume FOR
            if (parser.matchKeyword("NO")) {
                parser.matchKeyword("KEY");
                parser.matchKeyword("UPDATE");
                lockMode = "NO KEY UPDATE";
            } else if (parser.matchKeyword("KEY")) {
                parser.matchKeyword("SHARE");
                lockMode = "KEY SHARE";
            } else if (parser.matchKeyword("UPDATE")) {
                lockMode = "UPDATE";
            } else if (parser.matchKeyword("SHARE")) {
                lockMode = "SHARE";
            }
            // Optional: OF table_name [, ...]
            if (parser.matchKeyword("OF")) {
                List<String> forUpdateTables = new ArrayList<>();
                forUpdateTables.add(parser.readIdentifier());
                while (parser.match(TokenType.COMMA)) forUpdateTables.add(parser.readIdentifier());
                // Validate that the table names exist in the FROM clause
                if (from != null) {
                    java.util.Set<String> fromNames = new java.util.HashSet<>();
                    for (SelectStmt.FromItem fi : from) {
                        if (fi instanceof SelectStmt.TableRef) {
                            SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
                            fromNames.add(tr.table().toLowerCase());
                            if (tr.alias() != null) fromNames.add(tr.alias().toLowerCase());
                        } else if (fi instanceof SelectStmt.JoinFrom) {
                            SelectStmt.JoinFrom ji = (SelectStmt.JoinFrom) fi;
                            if (ji.left() instanceof SelectStmt.TableRef) {
                                SelectStmt.TableRef lt = (SelectStmt.TableRef) ji.left();
                                fromNames.add(lt.table().toLowerCase());
                                if (lt.alias() != null) fromNames.add(lt.alias().toLowerCase());
                            }
                            if (ji.right() instanceof SelectStmt.TableRef) {
                                SelectStmt.TableRef rt2 = (SelectStmt.TableRef) ji.right();
                                fromNames.add(rt2.table().toLowerCase());
                                if (rt2.alias() != null) fromNames.add(rt2.alias().toLowerCase());
                            }
                        }
                    }
                    for (String fut : forUpdateTables) {
                        if (!fromNames.contains(fut.toLowerCase())) {
                            throw new ParseException("relation \"" + fut + "\" in FOR UPDATE clause not found in FROM clause", parser.peek(), "42P01");
                        }
                    }
                }
            }
            // Optional: NOWAIT | SKIP LOCKED
            if (parser.matchKeyword("NOWAIT")) {
                nowait = true;
            } else if (parser.matchKeyword("SKIP")) {
                parser.matchKeyword("LOCKED");
                skipLocked = true;
            }
        }
        SelectStmt.LockClause lockClause = lockMode != null ? new SelectStmt.LockClause(lockMode, nowait, skipLocked) : null;

        // PG allows LIMIT/OFFSET after FOR clauses
        if (limit == null && parser.matchKeyword("LIMIT")) {
            if (!parser.matchKeyword("ALL")) limit = parseLimitOffsetExpr();
        }
        if (offset == null && parser.matchKeyword("OFFSET")) {
            offset = parseLimitOffsetExpr();
        }

        SelectStmt select = new SelectStmt(distinct, distinctOn, targets, from, where, groupBy, having, windowDefs, orderBy, limit, offset, null, groupingSets, lockClause);
        if (selectIntoTable != null) {
            return new CreateTableAsStmt(selectIntoSchema, selectIntoTable, false, selectIntoTemp, select, true);
        }
        return select;
    }

    /**
     * Parse VALUES (expr, ...), (expr, ...) as a statement.
     * Converts to UNION ALL of SELECT expressions for multi-row VALUES.
     */
    Statement parseValues() {
        return parseValuesBody();
    }

    /**
     * Core VALUES parsing: VALUES (expr, ...), (expr, ...).
     * Returns SelectStmt for single row, SetOpStmt for multiple rows.
     */
    Statement parseValuesBody() {
        parser.expectKeyword("VALUES");
        List<List<Expression>> rows = new ArrayList<>();
        do {
            parser.expect(TokenType.LEFT_PAREN);
            rows.add(parser.parseExpressionList());
            parser.expect(TokenType.RIGHT_PAREN);
        } while (parser.match(TokenType.COMMA));

        // First row becomes SELECT expr1 AS column1, expr2 AS column2, ...
        List<SelectStmt.SelectTarget> firstTargets = new ArrayList<>();
        List<Expression> firstRow = rows.get(0);
        for (int i = 0; i < firstRow.size(); i++) {
            firstTargets.add(new SelectStmt.SelectTarget(firstRow.get(i), "column" + (i + 1)));
        }
        SelectStmt first = new SelectStmt(false, firstTargets, null, null, null, null, null, null, null);

        if (rows.size() == 1) {
            return first;
        }

        // Additional rows become chained UNION ALL SELECT ...
        Statement result = first;
        for (int r = 1; r < rows.size(); r++) {
            List<SelectStmt.SelectTarget> rowTargets = new ArrayList<>();
            List<Expression> row = rows.get(r);
            for (int i = 0; i < row.size(); i++) {
                rowTargets.add(new SelectStmt.SelectTarget(row.get(i), null));
            }
            SelectStmt rowSelect = new SelectStmt(false, rowTargets, null, null, null, null, null, null, null);
            result = new SetOpStmt(result, SetOpStmt.SetOpType.UNION, true, rowSelect, null, null, null);
        }
        return result;
    }

    /**
     * Parse a WITH statement: WITH ... [SEARCH ... ] [CYCLE ... ] SELECT|INSERT|UPDATE|DELETE.
     * Parses the WITH clause first, then dispatches to the correct DML parser.
     */
    Statement parseWithStatement() {
        List<SelectStmt.CommonTableExpr> ctes = parseWithClause();
        // Consume optional SEARCH BREADTH|DEPTH FIRST BY col SET ordcol
        String searchCol = null;
        boolean searchDepthFirst = false;
        List<String> searchByColumns = null;
        if (parser.checkKeyword("SEARCH")) {
            parser.advance(); // SEARCH
            if (parser.matchKeyword("DEPTH")) {
                searchDepthFirst = true;
            } else {
                parser.matchKeyword("BREADTH");
            }
            parser.matchKeyword("FIRST");
            parser.expectKeyword("BY");
            searchByColumns = parser.parseIdentifierList(); // columns
            parser.expectKeyword("SET");
            searchCol = parser.readIdentifier(); // ordering column name
        }
        // Consume optional CYCLE col SET is_cycle [TO val DEFAULT val] USING path
        String cycleCol = null;
        String cyclePathCol = null;
        if (parser.checkKeyword("CYCLE")) {
            parser.advance(); // CYCLE
            parser.parseIdentifierList(); // tracked columns
            parser.expectKeyword("SET");
            cycleCol = parser.readIdentifier(); // cycle column name
            if (parser.matchKeyword("TO")) {
                parser.parseExpression(); // cycle value
                parser.expectKeyword("DEFAULT");
                parser.parseExpression(); // default value
            }
            parser.expectKeyword("USING");
            cyclePathCol = parser.readIdentifier(); // path column name
        }
        // Attach SEARCH/CYCLE metadata to last CTE
        if ((searchCol != null || cycleCol != null) && !ctes.isEmpty()) {
            int last = ctes.size() - 1;
            SelectStmt.CommonTableExpr origCte = ctes.get(last);
            // SEARCH/CYCLE clauses are only valid on recursive CTEs
            if (!origCte.recursive()) {
                throw new com.memgres.engine.MemgresException(
                    "WITH query \"" + origCte.name() + "\" is not recursive", "42601");
            }
            ctes.set(last, new SelectStmt.CommonTableExpr(origCte.name(), origCte.columnNames(),
                    origCte.query(), origCte.recursive(), searchCol, searchDepthFirst, searchByColumns, cycleCol, cyclePathCol));
        }
        Token next = parser.peek();
        if (next.type() == TokenType.KEYWORD) {
            switch (next.value()) {
                case "SELECT": {
                    // Re-wrap into SELECT with CTEs
                    Statement body = parseSelectBody();
                    if (body instanceof CreateTableAsStmt) {
                        CreateTableAsStmt ctas = (CreateTableAsStmt) body;
                        // WITH ... SELECT INTO
                        if (ctas.query() instanceof SelectStmt) {
                            SelectStmt innerSel = (SelectStmt) ctas.query();
                            SelectStmt withSel = new SelectStmt(innerSel.distinct(), innerSel.distinctOn(), innerSel.targets(), innerSel.from(),
                                    innerSel.where(), innerSel.groupBy(), innerSel.having(), innerSel.windowDefs(), innerSel.orderBy(), innerSel.limit(), innerSel.offset(), ctes, innerSel.groupingSets(), innerSel.lockClause());
                            return new CreateTableAsStmt(ctas.schema(), ctas.name(), ctas.ifNotExists(), ctas.temporary(), withSel, ctas.withData());
                        }
                        return ctas;
                    }
                    SelectStmt sel = (SelectStmt) body;
                    return new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                            sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), sel.orderBy(), sel.limit(), sel.offset(), ctes, sel.groupingSets(), sel.lockClause());
                }
                case "INSERT":
                    return parser.parseInsert(ctes);
                case "UPDATE":
                    return parser.parseUpdate(ctes);
                case "DELETE":
                    return parser.parseDelete(ctes);
                default:
                    throw new ParseException("Expected SELECT, INSERT, UPDATE, or DELETE after WITH clause", next);
            }
        }
        throw new ParseException("Expected SELECT, INSERT, UPDATE, or DELETE after WITH clause", next);
    }

    List<SelectStmt.CommonTableExpr> parseWithClause() {
        parser.expectKeyword("WITH");
        boolean recursive = parser.matchKeyword("RECURSIVE");

        List<SelectStmt.CommonTableExpr> ctes = new ArrayList<>();
        do {
            String name = parser.readIdentifier();

            // Optional column name list
            List<String> columnNames = null;
            if (parser.check(TokenType.LEFT_PAREN) && !isNextKeywordSelect()) {
                parser.expect(TokenType.LEFT_PAREN);
                columnNames = new ArrayList<>();
                do {
                    columnNames.add(parser.readIdentifier());
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            }

            parser.expectKeyword("AS");
            parser.expect(TokenType.LEFT_PAREN);

            // Parse the CTE body: can be SELECT, set operation, or writable (INSERT/UPDATE/DELETE with RETURNING)
            Statement cteBody;
            if (parser.checkKeyword("INSERT")) {
                cteBody = parser.parseInsert();
            } else if (parser.checkKeyword("UPDATE")) {
                cteBody = parser.parseUpdate();
            } else if (parser.checkKeyword("DELETE")) {
                cteBody = parser.parseDelete();
            } else if (parser.checkKeyword("VALUES")) {
                cteBody = parseValues();
                cteBody = tryParseSetOp(cteBody);
            } else {
                cteBody = parser.parseSelect();
                cteBody = tryParseSetOp(cteBody);
            }

            parser.expect(TokenType.RIGHT_PAREN);

            ctes.add(new SelectStmt.CommonTableExpr(name, columnNames, cteBody, recursive));
        } while (parser.match(TokenType.COMMA));

        return ctes;
    }

    /**
     * Parse a GROUP BY list that may include GROUPING SETS, ROLLUP, or CUBE.
     * Returns null if the GROUP BY is a simple expression list (handled separately),
     * or a list-of-lists representing the grouping sets.
     * Also handles mixed: GROUP BY a, GROUPING SETS ((b), ()) -> cross product.
     */
    List<List<Expression>> parseGroupByClause() {
        // Check if the first token is GROUPING SETS / ROLLUP / CUBE
        if (parser.checkKeyword("GROUPING") && parser.checkKeywordAt(1, "SETS")) {
            return parseGroupingSetsOnly();
        }
        if (parser.checkKeyword("ROLLUP") || parser.checkKeyword("CUBE")) {
            return parseRollupOrCube();
        }
        // Check if any element in the comma-separated list is GROUPING SETS/ROLLUP/CUBE
        // by scanning ahead for those keywords at the top level
        int saved = parser.pos;
        boolean hasGroupingSets = false;
        int depth = 0;
        for (int i = parser.pos; i < parser.tokens.size(); i++) {
            Token t = parser.tokens.get(i);
            if (t.type() == TokenType.LEFT_PAREN) { depth++; continue; }
            if (t.type() == TokenType.RIGHT_PAREN) { depth--; if (depth < 0) break; continue; }
            if (depth > 0) continue;
            // Stop at clause keywords
            if (t.type() == TokenType.KEYWORD) {
                String v = t.value().toUpperCase();
                if (v.equals("HAVING") || v.equals("ORDER") || v.equals("LIMIT") || v.equals("OFFSET")
                        || v.equals("FETCH") || v.equals("UNION") || v.equals("INTERSECT")
                        || v.equals("EXCEPT") || v.equals("WINDOW") || v.equals("FOR")) break;
                if ((v.equals("GROUPING") && i + 1 < parser.tokens.size() && parser.tokens.get(i+1).value().equalsIgnoreCase("SETS"))
                        || v.equals("ROLLUP") || v.equals("CUBE")) {
                    hasGroupingSets = true;
                    break;
                }
            }
            if (t.type() == TokenType.SEMICOLON || t.type() == TokenType.EOF) break;
        }
        if (!hasGroupingSets) return null;

        // Mixed GROUP BY: parse comma-separated elements, building cross product of sets
        // e.g. GROUP BY a, GROUPING SETS ((b), ()) -> sets for (a), (b), () cross-producted:
        // each 'plain' expression is a single-element grouping set; GROUPING SETS/ROLLUP/CUBE expand to multiple sets
        // Cross product: combine each "base set" with each entry of grouping sets spec
        List<List<List<Expression>>> parts = new ArrayList<>();
        do {
            if (parser.checkKeyword("GROUPING") && parser.checkKeywordAt(1, "SETS")) {
                parts.add(parseGroupingSetsOnly());
            } else if (parser.checkKeyword("ROLLUP") || parser.checkKeyword("CUBE")) {
                parts.add(parseRollupOrCube());
            } else {
                Expression expr = parser.parseExpression();
                List<List<Expression>> singleColSet = new ArrayList<>();
                singleColSet.add(Cols.listOf(expr));
                parts.add(singleColSet);
            }
        } while (parser.match(TokenType.COMMA) && !parser.checkKeyword("HAVING") && !parser.checkKeyword("ORDER") && !parser.checkKeyword("LIMIT")
                && !parser.checkKeyword("OFFSET") && !parser.checkKeyword("FETCH") && !parser.checkKeyword("WINDOW")
                && !parser.isAtEnd() && !parser.check(TokenType.SEMICOLON));

        // Cross product all parts
        // When any GROUPING SETS part contains the empty set (), we also preserve it
        // in the result (as a grand total row). This matches PG behavior where
        // GROUP BY a, GROUPING SETS ((b), ()) -> GROUPING SETS((a,b), (a), ())
        List<List<Expression>> result = Cols.listOf(Cols.listOf()); // start with one empty set
        boolean hasEmptySetInGroupingSets = false;
        for (List<List<Expression>> part : parts) {
            for (List<Expression> set : part) {
                if (set.isEmpty()) { hasEmptySetInGroupingSets = true; break; }
            }
        }
        for (List<List<Expression>> part : parts) {
            List<List<Expression>> newResult = new ArrayList<>();
            for (List<Expression> existing : result) {
                for (List<Expression> setFromPart : part) {
                    List<Expression> combined = new ArrayList<>(existing);
                    combined.addAll(setFromPart);
                    newResult.add(combined);
                }
            }
            result = newResult;
        }
        // If any GROUPING SETS contained an empty set () and we have plain expressions,
        // PG also includes a grand total row (empty set)
        if (hasEmptySetInGroupingSets) {
            // Check if the result already contains an empty set
            boolean hasEmpty = result.stream().anyMatch(List::isEmpty);
            if (!hasEmpty) {
                List<List<Expression>> resultWithEmpty = new ArrayList<>(result);
                resultWithEmpty.add(new ArrayList<>());
                return resultWithEmpty;
            }
        }
        return result;
    }

    /** Parse GROUPING SETS ((...), ...) and return as list of sets. */
    List<List<Expression>> parseGroupingSetsOnly() {
        parser.expectKeyword("GROUPING");
        parser.expectKeyword("SETS");
        parser.expect(TokenType.LEFT_PAREN);
        List<List<Expression>> sets = new ArrayList<>();
        do {
            parser.expect(TokenType.LEFT_PAREN);
            List<Expression> set = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                // May contain nested tuples e.g. GROUPING SETS ((a,b), (a), ())
                // Or a single expression
                set.addAll(parser.parseExpressionList());
            }
            parser.expect(TokenType.RIGHT_PAREN);
            sets.add(set);
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        return sets;
    }

    /** Parse ROLLUP(...) or CUBE(...) and expand to grouping sets. */
    List<List<Expression>> parseRollupOrCube() {
        boolean isCube = parser.checkKeyword("CUBE");
        parser.advance(); // consume ROLLUP or CUBE
        parser.expect(TokenType.LEFT_PAREN);
        List<Expression> cols = new ArrayList<>();
        if (!parser.check(TokenType.RIGHT_PAREN)) {
            // May contain comma-separated expressions or tuples
            cols.addAll(parser.parseExpressionList());
        }
        parser.expect(TokenType.RIGHT_PAREN);

        if (cols.isEmpty()) {
            // ROLLUP() / CUBE() with no args; PG 18 rejects this as syntax error (42601)
            throw new ParseException("syntax error at or near \")\"", parser.peek());
        }

        if (isCube) {
            // CUBE(a,b) = GROUPING SETS ((a,b),(a),(b),())
            // All subsets (power set) of cols, in order from full to empty
            List<List<Expression>> sets = new ArrayList<>();
            int n = cols.size();
            // Generate all 2^n subsets from full to empty
            for (int mask = (1 << n) - 1; mask >= 0; mask--) {
                List<Expression> subset = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    if ((mask & (1 << i)) != 0) subset.add(cols.get(i));
                }
                sets.add(subset);
            }
            return sets;
        } else {
            // ROLLUP(a,b) = GROUPING SETS ((a,b),(a),())
            List<List<Expression>> sets = new ArrayList<>();
            for (int i = cols.size(); i >= 0; i--) {
                sets.add(new ArrayList<>(cols.subList(0, i)));
            }
            return sets;
        }
    }

    /**
     * Parse a simple GROUP BY expression list (no GROUPING SETS/ROLLUP/CUBE).
     */
    List<Expression> parseGroupByList() {
        return parser.parseExpressionList();
    }

    List<SelectStmt.SelectTarget> parseSelectTargets() {
        List<SelectStmt.SelectTarget> targets = new ArrayList<>();
        // SELECT with no columns: SELECT FROM t or bare SELECT;
        if (parser.checkKeyword("FROM") || parser.checkKeyword("WHERE") || parser.checkKeyword("INTO")
                || parser.isAtEnd() || parser.check(TokenType.SEMICOLON)) {
            return targets; // empty target list
        }
        do {
            // Detect dangling comma: SELECT a, FROM t
            if (!targets.isEmpty() && (parser.checkKeyword("FROM") || parser.checkKeyword("WHERE")
                    || parser.checkKeyword("GROUP") || parser.checkKeyword("ORDER") || parser.checkKeyword("LIMIT")
                    || parser.checkKeyword("OFFSET") || parser.checkKeyword("HAVING") || parser.checkKeyword("UNION")
                    || parser.checkKeyword("INTERSECT") || parser.checkKeyword("EXCEPT") || parser.checkKeyword("FETCH")
                    || parser.checkKeyword("FOR") || parser.checkKeyword("INTO") || parser.checkKeyword("WINDOW")
                    || parser.isAtEnd() || parser.check(TokenType.SEMICOLON) || parser.check(TokenType.RIGHT_PAREN))) {
                throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"", parser.peek());
            }
            targets.add(parseSelectTarget());
        } while (parser.match(TokenType.COMMA));
        return targets;
    }

    SelectStmt.SelectTarget parseSelectTarget() {
        // Handle * wildcard
        if (parser.check(TokenType.STAR)) {
            parser.advance();
            return new SelectStmt.SelectTarget(new WildcardExpr(), null);
        }

        // Handle table.*, but only if it's identifier DOT STAR
        int saved = parser.pos;
        if ((parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER) &&
                parser.pos + 2 < parser.tokens.size()) {
            String name = parser.peek().value();
            parser.advance();
            if (parser.check(TokenType.DOT)) {
                parser.advance();
                if (parser.check(TokenType.STAR)) {
                    parser.advance();
                    return new SelectStmt.SelectTarget(new WildcardExpr(name), null);
                }
            }
            parser.pos = saved; // reset
        }

        Expression expr = parser.parseExpression();

        // AS alias or bare alias
        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readIdentifier();
        } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                || isKeywordValidAsBareAlias()) {
            alias = parser.readIdentifier();
        }

        return new SelectStmt.SelectTarget(expr, alias);
    }

    List<SelectStmt.FromItem> parseFromList() {
        List<SelectStmt.FromItem> items = new ArrayList<>();
        items.add(parseFromItem());
        while (parser.match(TokenType.COMMA)) {
            items.add(parseFromItem());
        }
        return items;
    }

    SelectStmt.FromItem parseFromItem() {
        SelectStmt.FromItem item = parseFromPrimary();

        // Handle JOINs
        while (true) {
            SelectStmt.JoinType joinType = tryParseJoinType();
            if (joinType == null) break;

            SelectStmt.FromItem right = parseFromPrimary();
            Expression on = null;
            List<String> using = null;

            if (joinType != SelectStmt.JoinType.CROSS && joinType != SelectStmt.JoinType.NATURAL
                    && joinType != SelectStmt.JoinType.NATURAL_LEFT && joinType != SelectStmt.JoinType.NATURAL_RIGHT
                    && joinType != SelectStmt.JoinType.NATURAL_FULL) {
                if (parser.matchKeyword("ON")) {
                    on = parser.parseExpression();
                } else if (parser.matchKeyword("USING")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    using = new ArrayList<>();
                    do {
                        using.add(parser.readIdentifier());
                    } while (parser.match(TokenType.COMMA));
                    parser.expect(TokenType.RIGHT_PAREN);
                } else {
                    throw new ParseException("JOIN requires ON or USING clause", parser.peek());
                }
            }

            item = new SelectStmt.JoinFrom(item, joinType, right, on, using);
        }

        return item;
    }

    /** Check if a LEFT_PAREN starts a subquery (SELECT/VALUES/WITH) vs a parenthesized FROM item. */
    boolean isSubqueryStart() {
        // Check if immediately after the opening ( we have SELECT/VALUES/WITH/UPDATE/DELETE/INSERT
        // Only check ONE level to distinguish (SELECT ...) from ((subq) JOIN ...)
        if (parser.pos >= parser.tokens.size()) return false;
        Token first = parser.tokens.get(parser.pos);
        if (first.type() != TokenType.LEFT_PAREN) return false;
        // Check next token after the (
        if (parser.pos + 1 >= parser.tokens.size()) return false;
        Token second = parser.tokens.get(parser.pos + 1);
        if (second.type() == TokenType.KEYWORD) {
            String kw = second.value();
            return kw.equals("SELECT") || kw.equals("VALUES") || kw.equals("WITH")
                    || kw.equals("UPDATE") || kw.equals("DELETE") || kw.equals("INSERT");
        }
        return false;
    }

    SelectStmt.FromItem parseFromPrimary() {
        // ROWS FROM(fn1(...), fn2(...)) [WITH ORDINALITY] AS alias(cols)
        if (parser.checkKeyword("ROWS") && parser.checkKeywordAt(1, "FROM")) {
            parser.advance(); // ROWS
            parser.advance(); // FROM
            parser.expect(TokenType.LEFT_PAREN);
            List<String> funcNames = new ArrayList<>();
            List<List<Expression>> funcArgsList = new ArrayList<>();
            do {
                String fn = parser.readIdentifier();
                funcNames.add(fn);
                parser.expect(TokenType.LEFT_PAREN);
                List<Expression> fnArgs = new ArrayList<>();
                if (!parser.check(TokenType.RIGHT_PAREN)) {
                    fnArgs = parser.parseExpressionList();
                }
                parser.expect(TokenType.RIGHT_PAREN);
                funcArgsList.add(fnArgs);
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
            boolean withOrdinality = parser.matchKeywords("WITH", "ORDINALITY");
            String alias = null;
            if (parser.matchKeyword("AS")) {
                alias = parser.readIdentifier();
            } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                    || isKeywordValidAsBareAlias()) {
                alias = parser.readIdentifier();
            }
            List<String> colAliases = null;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance();
                colAliases = new ArrayList<>();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    colAliases.add(parser.readIdentifier());
                    if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                        parser.parseTypeName(); // optional type
                    }
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            }
            // Encode as FunctionFrom with special name "__rows_from__"
            // args = list of function calls encoded as nested ArrayExpr/FunctionCallExpr
            List<Expression> rowsFromArgs = new ArrayList<>();
            for (int i = 0; i < funcNames.size(); i++) {
                rowsFromArgs.add(new com.memgres.engine.parser.ast.FunctionCallExpr(funcNames.get(i), funcArgsList.get(i), false, false));
            }
            List<String> finalColAliases = colAliases;
            if (withOrdinality && finalColAliases == null) {
                finalColAliases = new ArrayList<>();
                finalColAliases.add("ordinality");
            }
            return new SelectStmt.FunctionFrom("__rows_from__", rowsFromArgs, alias, finalColAliases);
        }

        // LATERAL subquery
        boolean lateral = parser.matchKeyword("LATERAL");

        // [LATERAL] ROWS FROM(fn1(...), fn2(...)) [WITH ORDINALITY] AS alias(cols)
        if (parser.checkKeyword("ROWS") && parser.checkKeywordAt(1, "FROM")) {
            parser.advance(); // ROWS
            parser.advance(); // FROM
            parser.expect(TokenType.LEFT_PAREN);
            List<String> funcNames = new ArrayList<>();
            List<List<Expression>> funcArgsList = new ArrayList<>();
            do {
                String fn = parser.readIdentifier();
                funcNames.add(fn);
                parser.expect(TokenType.LEFT_PAREN);
                List<Expression> fnArgs = new ArrayList<>();
                if (!parser.check(TokenType.RIGHT_PAREN)) {
                    fnArgs = parser.parseExpressionList();
                }
                parser.expect(TokenType.RIGHT_PAREN);
                funcArgsList.add(fnArgs);
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
            boolean withOrdinality = parser.matchKeywords("WITH", "ORDINALITY");
            String alias = null;
            if (parser.matchKeyword("AS")) {
                alias = parser.readIdentifier();
            } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                    || isKeywordValidAsBareAlias()) {
                alias = parser.readIdentifier();
            }
            List<String> colAliases = null;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance();
                colAliases = new ArrayList<>();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    colAliases.add(parser.readIdentifier());
                    if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                        parser.parseTypeName(); // optional type
                    }
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            }
            List<Expression> rowsFromArgs = new ArrayList<>();
            for (int i = 0; i < funcNames.size(); i++) {
                rowsFromArgs.add(new com.memgres.engine.parser.ast.FunctionCallExpr(funcNames.get(i), funcArgsList.get(i), false, false));
            }
            List<String> finalColAliases = colAliases;
            if (withOrdinality && finalColAliases == null) {
                finalColAliases = new ArrayList<>();
                finalColAliases.add("ordinality");
            }
            return new SelectStmt.FunctionFrom("__rows_from__", rowsFromArgs, alias, finalColAliases);
        }

        // Subquery: [LATERAL] (SELECT|VALUES ...) [AS] alias [(col1, col2, ...)]
        // Or parenthesized FROM item: (table_ref JOIN ...), pg_dump style
        if (parser.check(TokenType.LEFT_PAREN)) {
            // Peek ahead to see if this is a subquery or a parenthesized join
            if (isSubqueryStart()) {
                parser.advance();
                // Handle extra nested parens: ((SELECT ...))
                int extraParens = 0;
                while (parser.check(TokenType.LEFT_PAREN) && isSubqueryStart()) {
                    parser.advance();
                    extraParens++;
                }
                Statement subStmt;
                if (parser.checkKeyword("VALUES")) {
                    subStmt = parseValuesBody();
                } else if (parser.checkKeyword("UPDATE")) {
                    subStmt = parser.parseUpdate();
                } else if (parser.checkKeyword("DELETE")) {
                    subStmt = parser.parseDelete();
                } else if (parser.checkKeyword("INSERT")) {
                    subStmt = parser.parseInsert();
                } else {
                    subStmt = parser.parseSelect();
                    subStmt = tryParseSetOp(subStmt);
                }
                for (int ep = 0; ep < extraParens; ep++) parser.expect(TokenType.RIGHT_PAREN);
                parser.expect(TokenType.RIGHT_PAREN);
                parser.matchKeyword("AS");
                // Try to read optional alias
                String alias = null;
                if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                        || isKeywordValidAsBareAlias()) {
                    alias = parser.readIdentifier();
                }
                // Parse optional column aliases: alias(col1, col2, ...)
                List<String> columnAliases = null;
                if (parser.check(TokenType.LEFT_PAREN)) {
                    parser.advance();
                    columnAliases = new ArrayList<>();
                    do {
                        columnAliases.add(parser.readIdentifier());
                    } while (parser.match(TokenType.COMMA));
                    parser.expect(TokenType.RIGHT_PAREN);
                }
                return new SelectStmt.SubqueryFrom(subStmt, alias, lateral, columnAliases);
            } else {
                // Parenthesized FROM item (e.g., pg_dump style: ((a JOIN b ON ...) JOIN c ON ...))
                parser.advance(); // consume (
                SelectStmt.FromItem inner = parseFromItem(); // recursively parse the join tree
                parser.expect(TokenType.RIGHT_PAREN);
                // Check for an optional alias after the parenthesized FROM item
                // Pattern: (SELECT ... JOIN ...) alias or (SELECT ... JOIN ...) alias(col1, col2, ...)
                String parenAlias = null;
                parser.matchKeyword("AS");
                if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                        || isKeywordValidAsBareAlias()) {
                    parenAlias = parser.readIdentifier();
                }
                if (parenAlias != null) {
                    // Wrap the inner FROM item as a subquery-like structure with an alias
                    // Build a SELECT * from the inner result, aliased
                    List<String> columnAliases = null;
                    if (parser.check(TokenType.LEFT_PAREN)) {
                        parser.advance();
                        columnAliases = new ArrayList<>();
                        do {
                            columnAliases.add(parser.readIdentifier());
                        } while (parser.match(TokenType.COMMA));
                        parser.expect(TokenType.RIGHT_PAREN);
                    }
                    // Wrap as a subquery: SELECT * FROM (inner) alias
                    // Build a synthetic SelectStmt that selects everything from the inner FROM item
                    SelectStmt syntheticSelect = new SelectStmt(
                            false, Cols.listOf(new SelectStmt.SelectTarget(new WildcardExpr(), null)),
                            Cols.listOf(inner), null, null, null, null, null, null);
                    return new SelectStmt.SubqueryFrom(syntheticSelect, parenAlias, lateral, columnAliases);
                }
                return inner;
            }
        }

        // JSON_TABLE(expr, path COLUMNS (...)) AS alias
        if (parser.checkKeyword("JSON_TABLE")) {
            return parseJsonTableFromItem(lateral);
        }

        // ONLY table_name
        boolean only = parser.matchKeyword("ONLY");

        // Check for reserved clause keywords used as table names, which indicate syntax errors
        if (parser.peek().type() == TokenType.KEYWORD) {
            String kw = parser.peek().value().toUpperCase();
            if (kw.equals("FROM") || kw.equals("WHERE") || kw.equals("GROUP") || kw.equals("ORDER")
                    || kw.equals("HAVING") || kw.equals("LIMIT") || kw.equals("OFFSET")
                    || kw.equals("UNION") || kw.equals("INTERSECT") || kw.equals("EXCEPT")
                    || kw.equals("JOIN") || kw.equals("INNER") || kw.equals("LEFT") || kw.equals("RIGHT")
                    || kw.equals("FULL") || kw.equals("CROSS") || kw.equals("NATURAL")
                    || kw.equals("ON") || kw.equals("USING") || kw.equals("SET") || kw.equals("INTO")) {
                throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"", parser.peek());
            }
        }

        // Table reference: [schema.]table [AS alias]
        // Or function call: func(args) [AS alias]
        String name1 = parser.readIdentifier();

        // Check for function call in FROM (e.g., generate_series(1, 5))
        if (!only && parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // consume (
            List<Expression> args = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                args = parser.parseExpressionList();
            }
            parser.expect(TokenType.RIGHT_PAREN);
            // Optional WITH ORDINALITY
            parser.matchKeywords("WITH", "ORDINALITY");
            String alias = null;
            if (parser.matchKeyword("AS")) {
                alias = parser.readIdentifier();
            } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                    || isKeywordValidAsBareAlias()) {
                alias = parser.readIdentifier();
            }
            // Optional column definition list: alias(col [type], col [type], ...)
            List<String> colAliases = null;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance();
                colAliases = new ArrayList<>();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    colAliases.add(parser.readIdentifier()); // column name
                    // Optional type name
                    if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                        parser.parseTypeName(); // column type, consumed but not stored
                    }
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            }
            return new SelectStmt.FunctionFrom(name1, args, alias, colAliases);
        }

        String schema = null;
        String tableName = name1;

        if (parser.match(TokenType.DOT)) {
            schema = name1;
            tableName = parser.readIdentifier();

            // Schema-qualified function call: schema.func(args) [AS alias]
            if (!only && parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); // consume (
                List<Expression> args = new ArrayList<>();
                if (!parser.check(TokenType.RIGHT_PAREN)) {
                    args = parser.parseExpressionList();
                }
                parser.expect(TokenType.RIGHT_PAREN);
                parser.matchKeywords("WITH", "ORDINALITY");
                String funcAlias = null;
                if (parser.matchKeyword("AS")) {
                    funcAlias = parser.readIdentifier();
                } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                        || isKeywordValidAsBareAlias()) {
                    funcAlias = parser.readIdentifier();
                }
                List<String> colAliases = null;
                if (parser.check(TokenType.LEFT_PAREN)) {
                    parser.advance();
                    colAliases = new ArrayList<>();
                    while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                        colAliases.add(parser.readIdentifier());
                        if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                            parser.parseTypeName();
                        }
                        parser.match(TokenType.COMMA);
                    }
                    parser.expect(TokenType.RIGHT_PAREN);
                }
                String qualifiedName = schema + "." + tableName;
                return new SelectStmt.FunctionFrom(qualifiedName, args, funcAlias, colAliases);
            }
        }

        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readIdentifier();
        } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                || isKeywordValidAsBareAlias()) {
            alias = parser.readIdentifier();
        }

        // Handle optional secondary alias: table bare_alias AS alias2(col1, col2, ...)
        // PostgreSQL allows this syntax (e.g., CROSS JOIN global_flags gf AS f(flag_key, enabled))
        if (alias != null && parser.matchKeyword("AS")) {
            alias = parser.readIdentifier();
            // Consume optional column alias list
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    parser.readIdentifier();
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            }
        }

        // TABLESAMPLE method (percentage) [REPEATABLE (seed)]
        if (parser.checkKeyword("TABLESAMPLE")) {
            parser.advance(); // consume TABLESAMPLE
            // method name: SYSTEM, BERNOULLI, or identifier
            String method;
            if (parser.peek().type() == TokenType.KEYWORD || parser.peek().type() == TokenType.IDENTIFIER) {
                method = parser.advance().value().toLowerCase();
            } else {
                throw new ParseException("Expected sampling method", parser.peek());
            }
            if (!method.equals("system") && !method.equals("bernoulli")) {
                throw new com.memgres.engine.MemgresException(
                    "tablesample method \"" + method + "\" does not exist", "42704");
            }
            parser.expect(TokenType.LEFT_PAREN);
            Expression pctExpr = parser.parseExpression();
            parser.expect(TokenType.RIGHT_PAREN);
            // Optional REPEATABLE (seed)
            Long seed = null;
            if (parser.matchKeyword("REPEATABLE")) {
                parser.expect(TokenType.LEFT_PAREN);
                Expression seedExpr = parser.parseExpression();
                parser.expect(TokenType.RIGHT_PAREN);
                // Store seed in special FunctionFrom
                // We encode as FunctionFrom with name "__tablesample__"
                // args: [table_alias_expr, percentage, seed, method_literal]
                seed = 0L; // placeholder, actual seed evaluated at runtime
                List<Expression> tsArgs = new ArrayList<>();
                tsArgs.add(Literal.ofString(method));
                tsArgs.add(pctExpr);
                tsArgs.add(seedExpr);
                String finalAlias = alias != null ? alias : tableName;
                // Return a wrapper FunctionFrom that references the table
                return new SelectStmt.FunctionFrom(
                    "__tablesample__:" + (schema != null ? schema + "." : "") + tableName,
                    tsArgs, finalAlias, null);
            }
            // No seed
            List<Expression> tsArgs = new ArrayList<>();
            tsArgs.add(Literal.ofString(method));
            tsArgs.add(pctExpr);
            String finalAlias2 = alias != null ? alias : tableName;
            return new SelectStmt.FunctionFrom(
                "__tablesample__:" + (schema != null ? schema + "." : "") + tableName,
                tsArgs, finalAlias2, null);
        }

        return new SelectStmt.TableRef(schema, tableName, alias, only);
    }

    SelectStmt.JoinType tryParseJoinType() {
        if (parser.matchKeyword("NATURAL")) {
            // NATURAL [LEFT|RIGHT|FULL] [OUTER] JOIN
            if (parser.matchKeyword("LEFT")) { parser.matchKeyword("OUTER"); parser.expectKeyword("JOIN"); return SelectStmt.JoinType.NATURAL_LEFT; }
            if (parser.matchKeyword("RIGHT")) { parser.matchKeyword("OUTER"); parser.expectKeyword("JOIN"); return SelectStmt.JoinType.NATURAL_RIGHT; }
            if (parser.matchKeyword("FULL")) { parser.matchKeyword("OUTER"); parser.expectKeyword("JOIN"); return SelectStmt.JoinType.NATURAL_FULL; }
            parser.expectKeyword("JOIN");
            return SelectStmt.JoinType.NATURAL;
        }
        if (parser.matchKeyword("CROSS")) { parser.expectKeyword("JOIN"); return SelectStmt.JoinType.CROSS; }
        if (parser.matchKeyword("INNER")) { parser.expectKeyword("JOIN"); return SelectStmt.JoinType.INNER; }
        if (parser.matchKeyword("LEFT")) { parser.matchKeyword("OUTER"); parser.expectKeyword("JOIN"); return SelectStmt.JoinType.LEFT; }
        if (parser.matchKeyword("RIGHT")) { parser.matchKeyword("OUTER"); parser.expectKeyword("JOIN"); return SelectStmt.JoinType.RIGHT; }
        if (parser.matchKeyword("FULL")) { parser.matchKeyword("OUTER"); parser.expectKeyword("JOIN"); return SelectStmt.JoinType.FULL; }
        if (parser.matchKeyword("JOIN")) { return SelectStmt.JoinType.INNER; }
        return null;
    }

    boolean isClauseKeyword(String word) {
        switch (word) {
            case "FROM":
            case "WHERE":
            case "GROUP":
            case "HAVING":
            case "ORDER":
            case "LIMIT":
            case "OFFSET":
            case "UNION":
            case "INTERSECT":
            case "EXCEPT":
            case "FETCH":
            case "FOR":
            case "ON":
            case "JOIN":
            case "INNER":
            case "LEFT":
            case "RIGHT":
            case "FULL":
            case "CROSS":
            case "NATURAL":
            case "RETURNING":
            case "INTO":
            case "SET":
            case "VALUES":
                return true;
            default:
                return false;
        }
    }

    /**
     * Check if the current token is a keyword that can be used as a bare alias.
     * In PostgreSQL, most keywords are non-reserved and can be used as identifiers.
     * Only clause-starting and join-related keywords are excluded.
     */
    boolean isKeywordValidAsBareAlias() {
        if (parser.peek().type() != TokenType.KEYWORD) return false;
        String word = parser.peek().value().toUpperCase();
        if (isClauseKeyword(word)) return false;
        return !word.equals("USING") && !word.equals("TABLESAMPLE")
                && !word.equals("WITH") && !word.equals("WINDOW")
                && !word.equals("OVER") && !word.equals("LATERAL");
    }

    Expression parseLimitOffsetExpr() {
        if (parser.match(TokenType.MINUS)) {
            Expression inner = parser.parsePrimary();
            return new UnaryExpr(UnaryExpr.UnaryOp.NEGATE, inner);
        }
        return parser.parsePrimary();
    }

    SelectStmt parseTableCommand() {
        parser.expectKeyword("TABLE");
        // TABLE (SELECT ...): execute the inner subquery and return its results
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // consume (
            Statement subStmt = parser.parseSelect();
            subStmt = tryParseSetOp(subStmt);
            parser.expect(TokenType.RIGHT_PAREN);
            // Build: SELECT * FROM (inner_select) AS __table_subquery__
            List<SelectStmt.SelectTarget> targets = Cols.listOf(
                    new SelectStmt.SelectTarget(new WildcardExpr(), null));
            List<SelectStmt.FromItem> from = Cols.listOf(
                    new SelectStmt.SubqueryFrom(subStmt, "__table_subquery__", false, null));
            return new SelectStmt(false, targets, from, null, null, null, null, null, null, null);
        }
        String schema = null;
        String tableName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { schema = tableName; tableName = parser.readIdentifier(); }
        // Build: SELECT * FROM tablename
        List<SelectStmt.SelectTarget> targets = Cols.listOf(
                new SelectStmt.SelectTarget(new WildcardExpr(), null));
        String fullName = schema != null ? schema + "." + tableName : tableName;
        List<SelectStmt.FromItem> from = Cols.listOf(
                new SelectStmt.TableRef(fullName, null));
        return new SelectStmt(false, targets, from, null, null, null, null, null, null, null);
    }

    /** Look ahead: is the token after ( a SELECT? */
    private boolean isNextKeywordSelect() {
        // Look ahead: is the token after ( a SELECT?
        if (parser.pos + 1 < parser.tokens.size()) {
            Token next = parser.tokens.get(parser.pos + 1);
            return next.type() == TokenType.KEYWORD &&
                    (next.value().equals("SELECT") || next.value().equals("WITH"));
        }
        return false;
    }

    // ---- JSON_TABLE FROM item ----

    private SelectStmt.FromItem parseJsonTableFromItem(boolean lateral) {
        parser.advance(); // consume JSON_TABLE
        parser.expect(TokenType.LEFT_PAREN);
        Expression input = parser.parseExpression();
        parser.expect(TokenType.COMMA);
        Expression path = parser.parseExpression();
        // Optional PASSING
        Map<String, Expression> passing = null;
        if (parser.matchKeyword("PASSING")) {
            passing = parsePassingClause();
        }
        parser.expectKeyword("COLUMNS");
        parser.expect(TokenType.LEFT_PAREN);
        List<JsonTableExpr.JsonTableColumn> columns = parseJsonTableColumns();
        parser.expect(TokenType.RIGHT_PAREN);
        // Optional ERROR ON ERROR
        JsonExistsExpr.OnBehavior onError = null;
        if (parser.matchKeyword("ERROR")) {
            parser.expectKeyword("ON"); parser.expectKeyword("ERROR");
            onError = JsonExistsExpr.OnBehavior.ERROR;
        }
        parser.expect(TokenType.RIGHT_PAREN);
        // Alias
        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readIdentifier();
        } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER) {
            alias = parser.readIdentifier();
        }
        JsonTableExpr jtExpr = new JsonTableExpr(input, path, columns, passing, onError);
        // Store as FunctionFrom with the JsonTableExpr packed into args
        return new SelectStmt.FunctionFrom("__json_table__", Cols.listOf(jtExpr), alias, null);
    }

    private List<JsonTableExpr.JsonTableColumn> parseJsonTableColumns() {
        List<JsonTableExpr.JsonTableColumn> cols = new ArrayList<>();
        do {
            // NESTED PATH ...
            if (parser.matchKeyword("NESTED")) {
                parser.matchKeyword("PATH"); // optional
                Expression nestedPath = parser.parseExpression();
                parser.expectKeyword("COLUMNS");
                parser.expect(TokenType.LEFT_PAREN);
                List<JsonTableExpr.JsonTableColumn> nestedCols = parseJsonTableColumns();
                parser.expect(TokenType.RIGHT_PAREN);
                cols.add(JsonTableExpr.JsonTableColumn.nested(nestedPath, nestedCols));
                continue;
            }
            String colName = parser.readIdentifier();
            // FOR ORDINALITY
            if (parser.matchKeywords("FOR", "ORDINALITY")) {
                cols.add(JsonTableExpr.JsonTableColumn.ordinality(colName));
                continue;
            }
            // type [EXISTS] PATH 'expr'
            String typeName = parser.parseTypeName();
            boolean existsPath = false;
            if (parser.matchKeyword("EXISTS")) {
                existsPath = true;
            }
            Expression pathExpr = null;
            if (parser.matchKeyword("PATH")) {
                pathExpr = parser.parseExpression();
            }
            Expression defaultOnEmpty = null;
            Expression defaultOnError = null;
            // DEFAULT val ON EMPTY / DEFAULT val ON ERROR
            while (parser.checkKeyword("DEFAULT") || parser.checkKeyword("NULL") || parser.checkKeyword("ERROR")) {
                if (parser.matchKeyword("DEFAULT")) {
                    Expression defVal = parser.parseExpression();
                    parser.expectKeyword("ON");
                    if (parser.matchKeyword("EMPTY")) {
                        defaultOnEmpty = defVal;
                    } else {
                        parser.expectKeyword("ERROR");
                        defaultOnError = defVal;
                    }
                } else if (parser.matchKeyword("NULL")) {
                    parser.expectKeyword("ON"); parser.expectKeyword("EMPTY");
                    // null on empty is default, nothing to set
                } else if (parser.matchKeyword("ERROR")) {
                    parser.expectKeyword("ON"); parser.expectKeyword("ERROR");
                    // error on error
                } else {
                    break;
                }
            }
            if (existsPath) {
                cols.add(JsonTableExpr.JsonTableColumn.exists(colName, typeName, pathExpr));
            } else {
                cols.add(JsonTableExpr.JsonTableColumn.typed(colName, typeName, pathExpr, defaultOnEmpty, defaultOnError));
            }
        } while (parser.match(TokenType.COMMA));
        return cols;
    }

    private Map<String, Expression> parsePassingClause() {
        Map<String, Expression> passing = new LinkedHashMap<>();
        do {
            Expression val = parser.parseExpression();
            parser.expectKeyword("AS");
            String name = parser.readIdentifier();
            passing.put(name.toLowerCase(), val);
        } while (parser.match(TokenType.COMMA));
        return passing;
    }
}
