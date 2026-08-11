package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.RecursiveCteCheck;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        rejectSetOpOnModifyingStatement(left);
        rejectSortedLeftArm(left);
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
                    if (parser.matchKeyword("LIMIT")) { limit = parser.matchKeyword("ALL") ? Literal.ofNull() : parser.parseExpression(); changed = true; }
                    if (parser.matchKeyword("OFFSET")) { offset = parser.parseExpression(); changed = true; }
                    if (changed) {
                        left = new SetOpStmt(sop.left(), sop.op(), sop.all(), sop.right(), orderBy, limit, offset,
                        sop.withTies());
                    }
                }
            }
            return left;
        }

        // UNION/EXCEPT loop (left-associative)
        boolean lastRightWasParenthesized = false;
        while (parser.checkKeyword("UNION") || parser.checkKeyword("EXCEPT")) {
            SetOpStmt.SetOpType opType;
            if (parser.checkKeyword("UNION")) { parser.advance(); opType = SetOpStmt.SetOpType.UNION; }
            else { parser.advance(); opType = SetOpStmt.SetOpType.EXCEPT; }

            boolean all = parser.matchKeyword("ALL");
            parser.matchKeyword("DISTINCT");
            // PG 18 does not support CORRESPONDING — reject with syntax error.
            if (parser.checkKeyword("CORRESPONDING") || parser.checkIdentifier("CORRESPONDING")) {
                throw new ParseException("syntax error at or near \"CORRESPONDING\"", parser.peek());
            }

            lastRightWasParenthesized = parser.check(TokenType.LEFT_PAREN);
            Statement right = parseSetOpOperand();

            // Right side may have higher-precedence INTERSECT
            right = parseIntersectChain(right);

            left = new SetOpStmt(left, opType, all, right, null, null, null);
        }

        // Transfer ORDER BY/LIMIT/OFFSET from rightmost leaf to outermost SetOpStmt
        // but NOT if the right side was explicitly parenthesized (the LIMIT belongs to the inner SELECT)
        if (!lastRightWasParenthesized) {
            left = bubbleUpTrailingClauses(left);
        }

        // Also check for explicit ORDER BY/LIMIT/OFFSET tokens after the set ops
        if (left instanceof SetOpStmt) {
            SetOpStmt sop = (SetOpStmt) left;
            List<SelectStmt.OrderByItem> orderBy = sop.orderBy();
            Expression limit = sop.limit();
            Expression offset = sop.offset();
            boolean changed = false;

            if (parser.matchKeywords("ORDER", "BY")) { orderBy = parser.parseOrderByList(); changed = true; }
            if (parser.matchKeyword("LIMIT")) { limit = parser.matchKeyword("ALL") ? Literal.ofNull() : parser.parseExpression(); changed = true; }
            if (parser.matchKeyword("OFFSET")) { offset = parser.parseExpression(); changed = true; }

            if (changed) {
                left = new SetOpStmt(sop.left(), sop.op(), sop.all(), sop.right(), orderBy, limit, offset,
                        sop.withTies());
            }
            rejectLockOnSetOp();
        }

        return left;
    }

    /**
     * A set operation combines queries, and INSERT, UPDATE, DELETE and MERGE are not queries --
     * PostgreSQL's grammar has no production joining one to a SELECT, so a set-operation keyword
     * after one is a syntax error there. Wrapping the statement as the left arm instead ran it and
     * only then complained that the arms had different widths, which left the rows the INSERT had
     * already written behind: {@code INSERT INTO t(id) (SELECT 1) UNION (SELECT 2)} reported an
     * error and inserted one row. Refusing at the parser leaves nothing behind, because nothing
     * has run.
     */
    private void rejectSetOpOnModifyingStatement(Statement left) {
        if (!(left instanceof InsertStmt) && !(left instanceof UpdateStmt)
                && !(left instanceof DeleteStmt) && !(left instanceof MergeStmt)) {
            return;
        }
        if (!parser.checkKeyword("UNION") && !parser.checkKeyword("INTERSECT")
                && !parser.checkKeyword("EXCEPT")) {
            return;
        }
        throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"",
                parser.peek());
    }

    /**
     * A row lock names the base-table row behind an output row, and a set operation has combined
     * rows from different relations by the time it has one to name. PostgreSQL refuses the lock
     * written after the whole set operation just as it refuses one written on an arm.
     */
    private void rejectLockOnSetOp() {
        if (!parser.checkKeyword("FOR")) return;
        String mode = null;
        if (parser.checkKeywordAt(1, "UPDATE")) mode = "UPDATE";
        else if (parser.checkKeywordAt(1, "SHARE")) mode = "SHARE";
        else if (parser.checkKeywordAt(1, "NO") && parser.checkKeywordAt(2, "KEY")) mode = "NO KEY UPDATE";
        else if (parser.checkKeywordAt(1, "KEY") && parser.checkKeywordAt(2, "SHARE")) mode = "KEY SHARE";
        if (mode == null) return;
        throw new MemgresException("FOR " + mode
                + " is not allowed with UNION/INTERSECT/EXCEPT", "0A000");
    }

    /**
     * ORDER BY, LIMIT, OFFSET and FOR UPDATE belong to the set operation as a whole, so
     * PostgreSQL's grammar has no production for one written on an unparenthesised arm:
     * {@code SELECT a FROM t ORDER BY 1 UNION SELECT 5} is a syntax error at UNION, and so is the
     * same with LIMIT, OFFSET or FOR UPDATE. The arms were parsed as ordinary SELECTs, which take
     * those clauses, and the trailing-clause bubbling then moved them to the set operation -- so
     * the ORDER BY silently applied to the union and the LIMIT silently applied to the first arm
     * alone.
     *
     * <p>Parenthesised, all three are legal and mean the arm, which is a difference the AST does
     * not record. The token before the set-operation keyword does record it: a parenthesised arm
     * ends in the closing paren. Reading it there keeps the check to the one shape that can only
     * have come from an unparenthesised arm.
     */
    private void rejectSortedLeftArm(Statement left) {
        if (!(left instanceof SelectStmt)) return;
        SelectStmt sel = (SelectStmt) left;
        if (sel.orderBy() == null && sel.limit() == null && sel.offset() == null
                && sel.lockClause() == null) {
            return;
        }
        if (!parser.checkKeyword("UNION") && !parser.checkKeyword("INTERSECT")
                && !parser.checkKeyword("EXCEPT")) {
            return;
        }
        if (parser.previousTokenIs(TokenType.RIGHT_PAREN)) return;
        throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"",
                parser.peek());
    }

    /**
     * Parse one operand of a set operation: a query, or a parenthesised one.
     *
     * <p>What stands inside the parentheses is a whole query expression and may be a set operation
     * of its own, whose own arms may be parenthesised in turn -- so the parentheses have to be
     * taken off one at a time, each one closed only after everything it opened has been read.
     * Counting the run of opening parentheses first and then expecting that many closing ones at
     * the end assumed every one of them belonged to the same operand: in
     * {@code (SELECT 1) UNION ((SELECT 2) UNION (SELECT 3))} the two leading parentheses of the
     * right operand close in different places, so the second closing parenthesis was looked for
     * where the inner UNION stands and an ordinary set operation was a syntax error.
     */
    private Statement parseSetOpOperand() {
        // Only an arm written bare may not begin with WITH; inside parentheses it is a query
        // expression of its own and a WITH clause belongs to it. The check therefore stands here
        // and not in the recursion, which is what descends through the parentheses.
        if (!parser.check(TokenType.LEFT_PAREN)) rejectWithClauseArm();
        return parseParenthesisedOperand();
    }

    private Statement parseParenthesisedOperand() {
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            Statement inner = parseParenthesisedOperand();
            inner = tryParseSetOp(inner);
            parser.expect(TokenType.RIGHT_PAREN);
            return inner;
        }
        if (parser.checkKeyword("VALUES")) {
            // A bare VALUES list is a query in its own right, so it may be a set-op arm
            Statement values = parseValuesBody();
            if (parser.checkKeyword("FOR")) {
                throw new MemgresException("FOR UPDATE cannot be applied to VALUES", "0A000");
            }
            return values;
        }
        return parser.parseSelect();
    }

    /** Parse a set-op operand from outside this class (an INSERT's source query). */
    Statement parseSetOpOperandPublic() {
        return parseSetOpOperand();
    }

    /**
     * A WITH clause is not one of the productions a set-operation arm may be.
     *
     * <p>PostgreSQL's grammar hangs WITH off a whole query, not off an arm, so {@code SELECT 1
     * UNION ALL WITH x AS (...) SELECT ...} is a syntax error at the WITH — while the same
     * written inside parentheses is a query in its own right and runs. The arms here were parsed
     * by the full SELECT parser, which reads a WITH clause wherever it stands.
     */
    private void rejectWithClauseArm() {
        if (parser.checkKeyword("WITH")) {
            throw new ParseException("syntax error at or near \"WITH\"", parser.peek());
        }
    }

    /** Parse a chain of INTERSECT operations (higher precedence, left-associative). */
    Statement parseIntersectChain(Statement left) {
        rejectSortedLeftArm(left);
        while (parser.checkKeyword("INTERSECT")) {
            parser.advance();
            boolean all = parser.matchKeyword("ALL");
            parser.matchKeyword("DISTINCT");
            // PG: CORRESPONDING [BY (col, ...)]
            // PG 18 does not support CORRESPONDING — reject with syntax error.
            if (parser.checkKeyword("CORRESPONDING") || parser.checkIdentifier("CORRESPONDING")) {
                throw new ParseException("syntax error at or near \"CORRESPONDING\"", parser.peek());
            }

            Statement right = parseSetOpOperand();

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
        boolean withTies = false;
        Statement cleanRight = processedRight;

        if (processedRight instanceof SelectStmt) {
            SelectStmt rsel = (SelectStmt) processedRight;
            if (rsel.orderBy() != null || rsel.limit() != null || rsel.offset() != null) {
                orderBy = rsel.orderBy();
                limit = rsel.limit();
                offset = rsel.offset();
                // Only ORDER BY, LIMIT and OFFSET move up — and WITH TIES with them, since it
                // qualifies the limit it travels with. Everything else stays on the arm:
                // rebuilding it through a short constructor dropped whatever that one has no
                // parameter for, the row lock among them, so FOR UPDATE written after a set
                // operation stopped being refused because the arm no longer carried it.
                withTies = rsel.withTies();
                cleanRight = new SelectStmt(rsel.distinct(), rsel.distinctOn(), rsel.targets(),
                        rsel.from(), rsel.where(), rsel.groupBy(), rsel.having(), rsel.windowDefs(),
                        null, null, null, rsel.withClauses(), rsel.groupingSets(),
                        rsel.lockClause(), false);
            }
        } else if (processedRight instanceof SetOpStmt) {
            SetOpStmt rightSop = (SetOpStmt) processedRight;
            if (rightSop.orderBy() != null || rightSop.limit() != null || rightSop.offset() != null) {
                orderBy = rightSop.orderBy();
                limit = rightSop.limit();
                offset = rightSop.offset();
                withTies = rightSop.withTies();
                cleanRight = new SetOpStmt(rightSop.left(), rightSop.op(), rightSop.all(), rightSop.right(), null, null, null);
            }
        }

        if (orderBy != null || limit != null || offset != null) {
            return new SetOpStmt(sop.left(), sop.op(), sop.all(), cleanRight, orderBy, limit, offset,
                    withTies);
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
            // parseWithClause reads each item's own SEARCH and CYCLE clauses; reading them again
            // here would consume a second set and rebuild the last item from whichever of the two
            // this pass found, discarding the other.
            withClauses = parseWithClause();
        }

        Statement body = parseSelectBody();
        if (body instanceof CreateTableAsStmt) {
            CreateTableAsStmt ctas = (CreateTableAsStmt) body;
            // SELECT INTO: the inner query needs WITH clauses attached
            if (withClauses != null && ctas.query() instanceof SelectStmt) {
                SelectStmt sel = (SelectStmt) ctas.query();
                SelectStmt withSel = new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                        sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), sel.orderBy(), sel.limit(), sel.offset(), withClauses, sel.groupingSets(), sel.lockClause(), sel.withTies());
                return new CreateTableAsStmt(ctas.schema(), ctas.name(), ctas.ifNotExists(), ctas.temporary(), withSel, ctas.withData());
            }
            return ctas;
        }
        SelectStmt sel = (SelectStmt) body;
        return new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), sel.orderBy(), sel.limit(), sel.offset(), withClauses, sel.groupingSets(), sel.lockClause(), sel.withTies());
    }

    /**
     * Parse the SELECT body (without WITH clause). Used by both parseSelect() and parseWithStatement().
     */
    Statement parseSelectBody() {
        // TABLE t is one of PostgreSQL's simple_select productions, alongside SELECT and VALUES,
        // so it is a query wherever a query may stand: a view body, a set-operation arm, a CTE, a
        // sub-select, an INSERT source. Reading it here rather than only as a statement is what
        // makes it one, because every one of those callers arrives through this method.
        if (parser.checkKeyword("TABLE")) return parseTableQuery();
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
            // GROUP BY takes the same set quantifier SELECT does: DISTINCT drops grouping sets
            // the specification produces more than once, ALL (the default) keeps every one.
            boolean groupByDistinct = parser.matchKeyword("DISTINCT");
            if (!groupByDistinct) parser.matchKeyword("ALL");
            // Parse potentially multiple GROUP BY elements that may include GROUPING SETS/ROLLUP/CUBE
            groupingSets = parseGroupByClause();
            if (groupingSets != null) {
                // GROUP BY DISTINCT: deduplicate grouping sets
                if (groupByDistinct) {
                    List<List<Expression>> deduped = new ArrayList<>();
                    java.util.Set<String> seenSets = new java.util.LinkedHashSet<>();
                    for (List<Expression> gs : groupingSets) {
                        // A set is what it contains, so the key ignores the order it was written in.
                        List<String> parts = new ArrayList<>();
                        for (Expression e : gs) parts.add(String.valueOf(e));
                        java.util.Collections.sort(parts);
                        if (seenSets.add(parts.toString())) deduped.add(gs);
                    }
                    groupingSets = deduped;
                }
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
                // Check for base window name reference: w2 AS (w1 ORDER BY ...)
                String winRefName = null;
                if (parser.peek().type() == TokenType.IDENTIFIER
                        && !parser.checkKeyword("PARTITION") && !parser.checkKeyword("ORDER")
                        && !parser.checkKeyword("ROWS") && !parser.checkKeyword("RANGE")
                        && !parser.checkKeyword("GROUPS")) {
                    // Could be a base window name. Peek ahead to disambiguate.
                    int saved = parser.position();
                    String maybeRef = parser.readIdentifier();
                    // If followed by PARTITION BY, ORDER BY, frame, or ), it's a base window ref
                    if (parser.checkKeyword("PARTITION") || parser.checkKeyword("ORDER")
                            || parser.checkKeyword("ROWS") || parser.checkKeyword("RANGE")
                            || parser.checkKeyword("GROUPS") || parser.check(TokenType.RIGHT_PAREN)) {
                        winRefName = maybeRef;
                    } else {
                        parser.resetPosition(saved);
                    }
                }
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
                for (SelectStmt.WindowDef existing : windowDefs) {
                    if (existing.name().equalsIgnoreCase(winName)) {
                        throw new com.memgres.engine.MemgresException(
                                "window \"" + winName + "\" is already defined", "42P20");
                    }
                }
                windowDefs.add(new SelectStmt.WindowDef(winName, winRefName, winPartitionBy, winOrderBy, winFrame));
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
            // LIMIT ALL is LIMIT NULL: no limit, but a LIMIT clause was written, and the rules
            // that turn on whether a query has one have to be able to see it.
            limit = parser.matchKeyword("ALL") ? Literal.ofNull() : parseLimitOffsetExpr();
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

        // FETCH FIRST|NEXT [n] ROW|ROWS {ONLY | WITH TIES} (SQL standard equivalent of LIMIT)
        boolean withTies = false;
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
            if (parser.matchKeyword("WITH")) {
                parser.expectKeyword("TIES");
                // WITH TIES keeps every row equal to the last one returned, which only means
                // something once the query has said what "equal" is.
                if (orderBy == null || orderBy.isEmpty()) {
                    throw new MemgresException(
                            "WITH TIES cannot be specified without ORDER BY clause", "42601");
                }
                withTies = true;
            } else {
                parser.matchKeyword("ONLY");
            }
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
        List<String> forUpdateTables = new ArrayList<>();
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
            } else {
                // FOR opens a locking clause and nothing else here, so a word that names no lock
                // strength is the word PostgreSQL stops on -- swallowing the FOR accepted a
                // statement that trailed off into nothing.
                throw ParseException.at(parser.peek());
            }
            // Optional: OF table_name [, ...]
            if (parser.matchKeyword("OF")) {
                forUpdateTables = new ArrayList<>();
                forUpdateTables.add(readLockTargetName(lockMode));
                while (parser.match(TokenType.COMMA)) forUpdateTables.add(readLockTargetName(lockMode));
                // Whether these names are really in FROM is decided by the executor, which sees
                // the whole FROM tree including aliases, subqueries and outer-join sides.
            }
            // Optional: NOWAIT | SKIP LOCKED
            if (parser.matchKeyword("NOWAIT")) {
                nowait = true;
            } else if (parser.matchKeyword("SKIP")) {
                parser.matchKeyword("LOCKED");
                skipLocked = true;
            }
        }
        SelectStmt.LockClause lockClause = lockMode != null
                ? new SelectStmt.LockClause(lockMode, nowait, skipLocked, forUpdateTables) : null;

        // PG allows LIMIT/OFFSET after FOR clauses
        if (limit == null && parser.matchKeyword("LIMIT")) {
            limit = parser.matchKeyword("ALL") ? Literal.ofNull() : parseLimitOffsetExpr();
        }
        if (offset == null && parser.matchKeyword("OFFSET")) {
            offset = parseLimitOffsetExpr();
        }

        SelectStmt select = new SelectStmt(distinct, distinctOn, targets, from, where, groupBy, having, windowDefs, orderBy, limit, offset, null, groupingSets, lockClause, withTies);
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
        // A bare VALUES list is a query, so it may be the left arm of a set operation too
        Statement stmt = tryParseSetOp(parseValuesBody());
        // ... and a query of its own takes ORDER BY, LIMIT and OFFSET. A list of two rows or more
        // is rewritten as a set operation, whose parsing reads them; a list of one row is one
        // SELECT and nothing read them at all, so VALUES (7) ORDER BY 1 was a syntax error at
        // ORDER while VALUES (7), (8) ORDER BY 1 sorted.
        if (stmt instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) stmt;
            List<SelectStmt.OrderByItem> orderBy = sel.orderBy();
            Expression limit = sel.limit();
            Expression offset = sel.offset();
            boolean changed = false;
            if (parser.matchKeywords("ORDER", "BY")) { orderBy = parser.parseOrderByList(); changed = true; }
            if (parser.matchKeyword("LIMIT")) {
                limit = parser.matchKeyword("ALL") ? Literal.ofNull() : parser.parseExpression();
                changed = true;
            }
            if (parser.matchKeyword("OFFSET")) { offset = parser.parseExpression(); changed = true; }
            // A row lock is taken on a row of a table, and a VALUES list has none: there is
            // nothing for FOR UPDATE to lock. Reading the clause and going on left the query
            // claiming a lock it could never hold.
            if (parser.checkKeyword("FOR")) {
                throw new MemgresException("FOR UPDATE cannot be applied to VALUES", "0A000");
            }
            if (changed) {
                return new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                        sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), orderBy, limit,
                        offset, sel.withClauses(), sel.groupingSets(), sel.lockClause(),
                        sel.withTies()).asValuesList();
            }
        }
        return stmt;
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
        SelectStmt first = new SelectStmt(false, firstTargets, null, null, null, null, null, null, null)
                .asValuesList();

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
            SelectStmt rowSelect = new SelectStmt(false, rowTargets, null, null, null, null, null, null, null)
                    .asValuesList();
            result = new SetOpStmt(result, SetOpStmt.SetOpType.UNION, true, rowSelect, null, null, null);
        }
        return result;
    }

    /**
     * Parse a WITH statement: WITH ... [SEARCH ... ] [CYCLE ... ] SELECT|INSERT|UPDATE|DELETE.
     * Parses the WITH clause first, then dispatches to the correct DML parser.
     */
    /**
     * Read the SEARCH and CYCLE clauses that follow one WITH item and attach them to it.
     *
     * <p>PostgreSQL's grammar allows at most one of each, in that order, so anything left over —
     * a second SEARCH, a second CYCLE, or a SEARCH written after the CYCLE — is a syntax error at
     * the word that starts it, not another clause to read. Saying so here is what stops a second
     * pass over the same words from rebuilding the item with one of the two clauses dropped.
     */
    private void consumeSearchCycleClauses(List<SelectStmt.CommonTableExpr> ctes) {
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
            searchByColumns = parser.parseIdentifierList();
            parser.expectKeyword("SET");
            searchCol = parser.readIdentifier();
        }
        String cycleCol = null;
        String cyclePathCol = null;
        List<String> cycleByColumns = null;
        Expression cycleMarkValue = null;
        Expression cycleMarkDefault = null;
        if (parser.checkKeyword("CYCLE")) {
            parser.advance(); // CYCLE
            cycleByColumns = parser.parseIdentifierList();
            parser.expectKeyword("SET");
            cycleCol = parser.readIdentifier();
            if (parser.matchKeyword("TO")) {
                cycleMarkValue = parseCycleMarkConstant();
                requireKeywordHere("DEFAULT", true);
                cycleMarkDefault = parseCycleMarkConstant();
                requireKeywordHere("USING", false);
            }
            parser.expectKeyword("USING");
            cyclePathCol = parser.readIdentifier();
        }
        if (parser.checkKeyword("SEARCH") || parser.checkKeyword("CYCLE")) {
            throw new ParseException("syntax error at or near \""
                    + parser.peek().value() + "\"", parser.peek());
        }
        if ((searchCol != null || cycleCol != null) && !ctes.isEmpty()) {
            int last = ctes.size() - 1;
            SelectStmt.CommonTableExpr origCte = ctes.get(last);
            // Declaring RECURSIVE does not make an item recursive; naming itself does. SEARCH and
            // CYCLE order and cut a recursion, so an item that never recurses cannot carry them —
            // and the item need not be the one the query goes on to read for PG to say so.
            if (!origCte.recursive() || !RecursiveCteCheck.selfReferencing(origCte)) {
                throw new com.memgres.engine.MemgresException(
                    "WITH query is not recursive", "42601");
            }
            // Every column SEARCH and CYCLE add stands beside the others in the item's column
            // list, so two of them under one name leave the query no way to say which it meant.
            if (searchCol != null && cycleCol != null && searchCol.equals(cycleCol)) {
                throw new com.memgres.engine.MemgresException(
                    "search sequence column name and cycle mark column name are the same", "42601");
            }
            if (searchCol != null && cyclePathCol != null && searchCol.equals(cyclePathCol)) {
                throw new com.memgres.engine.MemgresException(
                    "search sequence column name and cycle path column name are the same", "42601");
            }
            if (cycleCol != null && cyclePathCol != null && cycleCol.equals(cyclePathCol)) {
                throw new com.memgres.engine.MemgresException(
                    "cycle mark column name and cycle path column name are the same", "42601");
            }
            ctes.set(last, new SelectStmt.CommonTableExpr(origCte.name(), origCte.columnNames(),
                    origCte.query(), origCte.recursive(), searchCol, searchDepthFirst, searchByColumns,
                    cycleCol, cyclePathCol, cycleByColumns, cycleMarkValue, cycleMarkDefault));
        }
    }

    /**
     * The value a CYCLE clause may mark a row with: a constant, and nothing else.
     *
     * <p>PostgreSQL's grammar puts an {@code AexprConst} after TO and after DEFAULT, so a sign, an
     * operator, a cast or a function call is a syntax error at the token that broke the constant —
     * {@code TO -1} at the minus, {@code TO 1+1} at the plus, {@code TO 'x'::text} at the cast,
     * {@code TO random()} at the paren that closes an empty argument list. Reading a whole
     * expression here accepted all four and then evaluated them once, per query rather than per
     * row, which is not a meaning PostgreSQL gives them.
     */
    private Expression parseCycleMarkConstant() {
        Token t = parser.peek();
        switch (t.type()) {
            case INTEGER_LITERAL:
            case FLOAT_LITERAL:
            case STRING_LITERAL:
            case DOLLAR_STRING_LITERAL:
            case BIT_STRING_LITERAL:
                return parser.parsePrimary();
            case KEYWORD:
                if (t.value().equals("TRUE") || t.value().equals("FALSE")
                        || t.value().equals("NULL")) {
                    return parser.parsePrimary();
                }
                // A type name followed by a string is a constant too: DATE '2020-01-01'.
                if (tokenAt(1) != null
                        && tokenAt(1).type() == TokenType.STRING_LITERAL) {
                    return parser.parsePrimary();
                }
                break;
            case IDENTIFIER:
                if (tokenAt(1) != null
                        && tokenAt(1).type() == TokenType.STRING_LITERAL) {
                    return parser.parsePrimary();
                }
                // funcname '(' args ')' 'string' is the one other constant form, and the
                // argument list may not be empty; report where PostgreSQL stops reading it.
                if (tokenAt(1) != null
                        && tokenAt(1).type() == TokenType.LEFT_PAREN) {
                    Token stop = tokenAt(2);
                    if (stop != null && stop.type() == TokenType.RIGHT_PAREN) {
                        throw new ParseException("syntax error at or near \")\"", stop);
                    }
                }
                break;
            default:
                break;
        }
        throw new ParseException("syntax error at or near \"" + t.value() + "\"", t);
    }

    /** The token {@code offset} places ahead of the cursor, or null past the end. */
    private Token tokenAt(int offset) {
        int index = parser.pos + offset;
        return index < parser.tokens.size() ? parser.tokens.get(index) : null;
    }

    /** The keyword the CYCLE clause has to have next, said the way PostgreSQL says it is missing. */
    private void requireKeywordHere(String keyword, boolean consume) {
        if (!parser.checkKeyword(keyword)) {
            throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"",
                    parser.peek());
        }
        if (consume) parser.advance();
    }

    Statement parseWithStatement() {
        List<SelectStmt.CommonTableExpr> ctes = parseWithClause();
        Token next = parser.peek();
        if (next.type() == TokenType.KEYWORD) {
            switch (next.value()) {
                // TABLE t is a query wherever a query may stand, so it may be the body a WITH
                // clause hangs off just as SELECT may.
                case "TABLE":
                case "SELECT": {
                    // Re-wrap into SELECT with CTEs
                    Statement body = parseSelectBody();
                    if (body instanceof CreateTableAsStmt) {
                        CreateTableAsStmt ctas = (CreateTableAsStmt) body;
                        // WITH ... SELECT INTO
                        if (ctas.query() instanceof SelectStmt) {
                            SelectStmt innerSel = (SelectStmt) ctas.query();
                            SelectStmt withSel = new SelectStmt(innerSel.distinct(), innerSel.distinctOn(), innerSel.targets(), innerSel.from(),
                                    innerSel.where(), innerSel.groupBy(), innerSel.having(), innerSel.windowDefs(), innerSel.orderBy(), innerSel.limit(), innerSel.offset(), ctes, innerSel.groupingSets(), innerSel.lockClause(), innerSel.withTies());
                            return new CreateTableAsStmt(ctas.schema(), ctas.name(), ctas.ifNotExists(), ctas.temporary(), withSel, ctas.withData());
                        }
                        return ctas;
                    }
                    SelectStmt sel = (SelectStmt) body;
                    return new SelectStmt(sel.distinct(), sel.distinctOn(), sel.targets(), sel.from(),
                            sel.where(), sel.groupBy(), sel.having(), sel.windowDefs(), sel.orderBy(), sel.limit(), sel.offset(), ctes, sel.groupingSets(), sel.lockClause(), sel.withTies());
                }
                case "INSERT":
                    return parser.parseInsert(ctes);
                case "UPDATE":
                    return parser.parseUpdate(ctes);
                case "DELETE":
                    return parser.parseDelete(ctes);
                case "MERGE":
                    return parser.parseMerge(ctes);
                default:
                    throw new ParseException("Expected SELECT, INSERT, UPDATE, DELETE, or MERGE after WITH clause", next);
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
            if (parser.check(TokenType.LEFT_PAREN) && parser.countLeadingParensBeforeQuery() < 0) {
                parser.expect(TokenType.LEFT_PAREN);
                columnNames = new ArrayList<>();
                do {
                    columnNames.add(parser.readIdentifier());
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            }

            parser.expectKeyword("AS");
            // Optional MATERIALIZED / NOT MATERIALIZED inlining hint (PG 12+).
            // Memgres doesn't exploit the hint for planning, but must accept it.
            if (parser.matchKeyword("NOT")) {
                parser.expectKeyword("MATERIALIZED");
            } else {
                parser.matchKeyword("MATERIALIZED");
            }
            parser.expect(TokenType.LEFT_PAREN);

            // Parse the CTE body: can be SELECT, set operation, or writable (INSERT/UPDATE/DELETE with RETURNING)
            // The body may be wrapped in extra parens ((SELECT ...)) or have parenthesized set-op arms
            // (SELECT 1) UNION ALL (SELECT 2). Use parseStatement() for parenthesized content to handle both.
            Statement cteBody;
            if (parser.check(TokenType.LEFT_PAREN)) {
                // Parenthesized content — use parseStatement which handles recursive nesting and set ops
                cteBody = parser.parseStatement();
                cteBody = tryParseSetOp(cteBody);
            } else if (parser.checkKeyword("INSERT")) {
                cteBody = parser.parseInsert();
            } else if (parser.checkKeyword("UPDATE")) {
                cteBody = parser.parseUpdate();
            } else if (parser.checkKeyword("DELETE")) {
                cteBody = parser.parseDelete();
            } else if (parser.checkKeyword("MERGE")) {
                cteBody = parser.parseMerge(null);
            } else if (parser.checkKeyword("VALUES")) {
                cteBody = parseValues();
                cteBody = tryParseSetOp(cteBody);
            } else {
                cteBody = parser.parseSelect();
                cteBody = tryParseSetOp(cteBody);
            }

            parser.expect(TokenType.RIGHT_PAREN);

            ctes.add(new SelectStmt.CommonTableExpr(name, columnNames, cteBody, recursive));
            // SEARCH and CYCLE belong to the item just closed, not to the WITH clause, so they
            // are read here — before the comma that starts the next item, which is why
            // "SEARCH ... SET ord, s AS (...)" parses at all.
            consumeSearchCycleClauses(ctes);
        } while (parser.match(TokenType.COMMA));

        Set<String> seenNames = new HashSet<>();
        for (SelectStmt.CommonTableExpr cte : ctes) {
            if (!seenNames.add(cte.name().toLowerCase())) {
                throw new com.memgres.engine.MemgresException(
                        "WITH query name \"" + cte.name() + "\" specified more than once", "42712");
            }
        }

        return ctes;
    }

    /**
     * Parse a GROUP BY list that may include GROUPING SETS, ROLLUP, CUBE, or the empty
     * grouping set (). Returns null if the GROUP BY is a simple expression list (handled
     * separately), or a list-of-lists representing the grouping sets.
     *
     * <p>Every element of the list contributes a list of grouping sets — one for a plain
     * expression, several for GROUPING SETS / ROLLUP / CUBE — and the query groups by the
     * <em>Cartesian product</em> of those lists, not by the first of them. So
     * {@code GROUP BY ROLLUP(a), ROLLUP(b)} has the four sets {@code (a,b), (a), (b), ()},
     * and {@code GROUP BY a, GROUPING SETS ((b), ())} has {@code (a,b)} and {@code (a)}.
     */
    List<List<Expression>> parseGroupByClause() {
        // Check if any element in the comma-separated list is GROUPING SETS/ROLLUP/CUBE
        // or the empty grouping set (), by scanning ahead at the top level
        boolean hasGroupingSets = false;
        int depth = 0;
        for (int i = parser.pos; i < parser.tokens.size(); i++) {
            Token t = parser.tokens.get(i);
            if (t.type() == TokenType.LEFT_PAREN) {
                // "()" at the top level is the empty grouping set, not a parenthesised expression
                if (depth == 0 && i + 1 < parser.tokens.size()
                        && parser.tokens.get(i + 1).type() == TokenType.RIGHT_PAREN) {
                    hasGroupingSets = true;
                    break;
                }
                depth++;
                continue;
            }
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
            } else if (checkEmptyGroupingSet()) {
                parser.advance(); // (
                parser.advance(); // )
                List<List<Expression>> emptySet = new ArrayList<>();
                emptySet.add(new ArrayList<Expression>());
                parts.add(emptySet);
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
        // e.g. GROUP BY a, GROUPING SETS ((b), ()) -> GROUPING SETS((a,b), (a))
        // The empty set () in GROUPING SETS cross-products with plain col 'a' to produce (a),
        // not a separate grand total row.
        List<List<Expression>> result = Cols.listOf(Cols.listOf()); // start with one empty set
        for (List<List<Expression>> part : parts) {
            List<List<Expression>> newResult = new ArrayList<>();
            for (List<Expression> existing : result) {
                for (List<Expression> setFromPart : part) {
                    List<Expression> combined = new ArrayList<>(existing);
                    combined.addAll(setFromPart);
                    newResult.add(dedupeWithinSet(combined));
                }
            }
            result = newResult;
        }
        return result;
    }

    /**
     * One grouping set with repeated expressions dropped. Grouping by the same expression twice
     * partitions the rows exactly as grouping by it once does, and PostgreSQL folds the repeat
     * away: that is why the six sets of {@code ROLLUP(a), ROLLUP(a,b)} include {@code (a,a,b)}
     * as a plain {@code (a,b)}, and why GROUP BY DISTINCT can then recognise it as a duplicate.
     */
    private static List<Expression> dedupeWithinSet(List<Expression> set) {
        List<Expression> out = new ArrayList<>(set.size());
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Expression e : set) {
            if (seen.add(String.valueOf(e))) out.add(e);
        }
        return out;
    }

    /** True when the parser sits on "()", the empty grouping set. */
    private boolean checkEmptyGroupingSet() {
        return parser.check(TokenType.LEFT_PAREN)
                && parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).type() == TokenType.RIGHT_PAREN;
    }

    /**
     * Parse GROUPING SETS (...) and return as list of sets. An element is either the empty
     * set (), a parenthesised list of expressions, a nested ROLLUP/CUBE/GROUPING SETS that
     * contributes several sets, or — the ordinary spelling — a bare expression standing for
     * a one-element set.
     */
    List<List<Expression>> parseGroupingSetsOnly() {
        parser.expectKeyword("GROUPING");
        parser.expectKeyword("SETS");
        parser.expect(TokenType.LEFT_PAREN);
        if (parser.check(TokenType.RIGHT_PAREN)) {
            // GROUPING SETS () lists no sets at all; PG rejects it as a syntax error
            throw new ParseException("syntax error at or near \")\"", parser.peek());
        }
        List<List<Expression>> sets = new ArrayList<>();
        do {
            if (parser.checkKeyword("GROUPING") && parser.checkKeywordAt(1, "SETS")) {
                sets.addAll(parseGroupingSetsOnly());
            } else if (parser.checkKeyword("ROLLUP") || parser.checkKeyword("CUBE")) {
                sets.addAll(parseRollupOrCube());
            } else {
                List<Expression> set = parseGroupingSetElement();
                sets.add(set);
            }
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        return sets;
    }

    /**
     * One non-nested element of a GROUPING SETS list: "(a, b)" groups on both columns,
     * "()" on none, and a bare "a" on that one expression.
     */
    private List<Expression> parseGroupingSetElement() {
        if (parser.check(TokenType.LEFT_PAREN)) {
            int saved = parser.position();
            parser.advance();
            List<Expression> set = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                set.addAll(parser.parseExpressionList());
            }
            // Only a "(...)" that ends the element is a set; "(a)+1" is an ordinary expression
            if (parser.check(TokenType.RIGHT_PAREN)) {
                parser.advance();
                if (parser.check(TokenType.COMMA) || parser.check(TokenType.RIGHT_PAREN)) {
                    return set;
                }
            }
            parser.resetPosition(saved);
        }
        List<Expression> single = new ArrayList<>();
        single.add(parser.parseExpression());
        return single;
    }

    /** Parse ROLLUP(...) or CUBE(...) and expand to grouping sets. */
    List<List<Expression>> parseRollupOrCube() {
        boolean isCube = parser.checkKeyword("CUBE");
        parser.advance(); // consume ROLLUP or CUBE
        parser.expect(TokenType.LEFT_PAREN);
        if (parser.check(TokenType.RIGHT_PAREN)) {
            // ROLLUP() / CUBE() with no args; PG 18 rejects this as syntax error (42601)
            // and points at the closing paren, so report before consuming it
            throw new ParseException("syntax error at or near \")\"", parser.peek());
        }
        List<Expression> cols = new ArrayList<>(parser.parseExpressionList());
        parser.expect(TokenType.RIGHT_PAREN);

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

        // Handle table.*, but only if it's identifier/keyword DOT STAR
        // Keywords OLD and NEW are valid qualifiers for RETURNING OLD.*/NEW.* (PG 18)
        int saved = parser.pos;
        if ((parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER
                || parser.peek().type() == TokenType.KEYWORD) &&
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
        } else if (isValidBareTargetLabel()) {
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

    /**
     * The word PG's parser chokes on when a data-modifying statement is written where only a
     * query belongs: INTO after INSERT, SET after UPDATE's target, FROM after DELETE.
     */
    private String firstReservedWordAfterDmlVerb() {
        for (int i = parser.pos; i < parser.tokens.size() && i < parser.pos + 8; i++) {
            String word = parser.tokens.get(i).value().toUpperCase();
            if (word.equals("INTO") || word.equals("SET") || word.equals("FROM")) return word;
        }
        return parser.pos + 1 < parser.tokens.size()
                ? parser.tokens.get(parser.pos + 1).value() : parser.peek().value();
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

    /**
     * Check if the current LEFT_PAREN is an extra wrapping paren ((SELECT ...)) vs a
     * parenthesized UNION operand ((SELECT ...) UNION ALL ...). Returns true if it's
     * an extra wrapper (matching ) is followed by another ) or end, not by UNION/INTERSECT/EXCEPT).
     */
    boolean isExtraWrappingParen() {
        // Scan ahead to find the matching ) for the current (, then check what follows
        int depth = 1;
        int lookPos = parser.pos + 1;
        while (lookPos < parser.tokens.size() && depth > 0) {
            if (parser.tokens.get(lookPos).type() == TokenType.LEFT_PAREN) depth++;
            else if (parser.tokens.get(lookPos).type() == TokenType.RIGHT_PAREN) depth--;
            lookPos++;
        }
        // lookPos is now past the matching )
        if (lookPos >= parser.tokens.size()) return true; // end of tokens, treat as extra
        Token afterClose = parser.tokens.get(lookPos);
        if (afterClose.type() == TokenType.KEYWORD) {
            String kw = afterClose.value();
            // If UNION/INTERSECT/EXCEPT follows, this is a set-op operand, not an extra wrapper
            if (kw.equals("UNION") || kw.equals("INTERSECT") || kw.equals("EXCEPT")) {
                return false;
            }
            // If AS follows, the ) ends the subquery and AS introduces an alias — not an extra wrapper
            if (kw.equals("AS")) {
                return false;
            }
        }
        // If an identifier follows, it's an alias for the subquery — not an extra wrapper
        if (afterClose.type() == TokenType.IDENTIFIER || afterClose.type() == TokenType.QUOTED_IDENTIFIER) {
            return false;
        }
        return true;
    }

    /** Check if a LEFT_PAREN starts a subquery (SELECT/VALUES/WITH) vs a parenthesized FROM item. */
    boolean isSubqueryStart() {
        // Check if immediately after the opening ( we have SELECT/VALUES/WITH/UPDATE/DELETE/INSERT
        // Also check through one level of nested parens for ((SELECT ...) UNION ALL ...) patterns
        if (parser.pos >= parser.tokens.size()) return false;
        Token first = parser.tokens.get(parser.pos);
        if (first.type() != TokenType.LEFT_PAREN) return false;
        // Check next token after the (
        if (parser.pos + 1 >= parser.tokens.size()) return false;
        Token second = parser.tokens.get(parser.pos + 1);
        if (second.type() == TokenType.KEYWORD) {
            String kw = second.value();
            return kw.equals("SELECT") || kw.equals("VALUES") || kw.equals("WITH")
                    || kw.equals("TABLE")
                    || kw.equals("UPDATE") || kw.equals("DELETE") || kw.equals("INSERT");
        }
        // Check for nested parens: ((SELECT ...) UNION ALL ...)
        if (second.type() == TokenType.LEFT_PAREN) {
            // Scan ahead to find matching ) for the inner ( and check if UNION/INTERSECT/EXCEPT follows
            int depth = 1;
            int lookPos = parser.pos + 2;
            while (lookPos < parser.tokens.size() && depth > 0) {
                if (parser.tokens.get(lookPos).type() == TokenType.LEFT_PAREN) depth++;
                else if (parser.tokens.get(lookPos).type() == TokenType.RIGHT_PAREN) depth--;
                lookPos++;
            }
            // lookPos is now past the matching ) for the inner (
            if (lookPos < parser.tokens.size()) {
                Token afterInner = parser.tokens.get(lookPos);
                if (afterInner.type() == TokenType.KEYWORD) {
                    String kw = afterInner.value();
                    if (kw.equals("UNION") || kw.equals("INTERSECT") || kw.equals("EXCEPT")) {
                        return true;
                    }
                }
                // If the inner ( matching ) is followed by an identifier or AS, the inner ( is a
                // subquery with an alias, and the outer ( is a parenthesized FROM item — not a subquery start.
                if (afterInner.type() == TokenType.IDENTIFIER || afterInner.type() == TokenType.QUOTED_IDENTIFIER) {
                    return false;
                }
                if (afterInner.type() == TokenType.KEYWORD && afterInner.value().equals("AS")) {
                    return false;
                }
            }
            // Also check if the inner content starts with SELECT/VALUES/WITH
            if (parser.pos + 2 < parser.tokens.size()) {
                Token third = parser.tokens.get(parser.pos + 2);
                if (third.type() == TokenType.KEYWORD) {
                    String kw = third.value();
                    return kw.equals("SELECT") || kw.equals("VALUES") || kw.equals("WITH")
                            || kw.equals("UPDATE") || kw.equals("DELETE") || kw.equals("INSERT");
                }
            }
        }
        return false;
    }

    /**
     * {@code ROWS FROM (f(...) [AS (name type, ...)], g(...)) [WITH ORDINALITY] [AS] alias(cols)}.
     *
     * <p>Each function may carry its own column definition list, which is what distinguishes
     * ROWS FROM from an ordinary function item: the item's alias list renames the columns of all
     * the functions at once and so cannot describe any one of them. Carried through as a
     * {@code __rows_from__} function item whose arguments are {@link RowsFromItem}s.
     */
    private SelectStmt.FromItem parseRowsFrom() {
        parser.advance(); // ROWS
        parser.advance(); // FROM
        parser.expect(TokenType.LEFT_PAREN);
        List<Expression> items = new ArrayList<>();
        do {
            String name = parser.readIdentifier();
            parser.expect(TokenType.LEFT_PAREN);
            List<Expression> args = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                args = parser.parseExpressionList();
            }
            parser.expect(TokenType.RIGHT_PAREN);
            List<String> columnDefs = null;
            int beforeAs = parser.position();
            if (parser.matchKeyword("AS") && parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); // (
                columnDefs = new ArrayList<>();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    String col = parser.readIdentifier();
                    if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                        col = col + " " + parser.parseTypeName();
                    }
                    columnDefs.add(col);
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            } else {
                parser.resetPosition(beforeAs);
            }
            items.add(new RowsFromItem(new FunctionCallExpr(name, args, false, false), columnDefs));
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        boolean withOrdinality = parser.matchKeywords("WITH", "ORDINALITY");
        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readColumnName();
        } else if (isKeywordValidAsBareAlias()) {
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
        return new SelectStmt.FunctionFrom("__rows_from__", items, alias, colAliases, withOrdinality);
    }

    SelectStmt.FromItem parseFromPrimary() {
        // ROWS FROM(fn1(...) [AS (coldef)], ...) [WITH ORDINALITY] [AS] alias(cols)
        if (parser.checkKeyword("ROWS") && parser.checkKeywordAt(1, "FROM")) {
            return parseRowsFrom();
        }

        // LATERAL subquery
        boolean lateral = parser.matchKeyword("LATERAL");

        // [LATERAL] ROWS FROM(...) — same item, reached after the LATERAL keyword
        if (parser.checkKeyword("ROWS") && parser.checkKeywordAt(1, "FROM")) {
            return parseRowsFrom();
        }

        // Subquery: [LATERAL] (SELECT|VALUES ...) [AS] alias [(col1, col2, ...)]
        // Or parenthesized FROM item: (table_ref JOIN ...), pg_dump style
        if (parser.check(TokenType.LEFT_PAREN)) {
            // Peek ahead to see if this is a subquery or a parenthesized join
            if (isSubqueryStart()) {
                parser.advance();
                // Handle extra nested parens: ((SELECT ...))
                // But NOT parenthesized UNION operands: ((SELECT ...) UNION ALL ...)
                int extraParens = 0;
                while (parser.check(TokenType.LEFT_PAREN) && isSubqueryStart() && isExtraWrappingParen()) {
                    parser.advance();
                    extraParens++;
                }
                Statement subStmt;
                if (parser.checkKeyword("VALUES")) {
                    // A VALUES list may be the left arm of a set operation, not only the whole body
                    subStmt = tryParseSetOp(parseValuesBody());
                } else if (parser.checkKeyword("UPDATE") || parser.checkKeyword("DELETE")
                        || parser.checkKeyword("INSERT")) {
                    // Only a top-level CTE may modify data. PG's grammar has no production for a
                    // write here at all, so it reads the verb as a name and fails on the first
                    // reserved word after it; report the same token.
                    throw new com.memgres.engine.MemgresException(
                            "syntax error at or near \"" + firstReservedWordAfterDmlVerb() + "\"", "42601");
                } else if (parser.check(TokenType.LEFT_PAREN)) {
                    // Parenthesized SELECT union: ((SELECT ...) UNION ALL (SELECT ...))
                    // or multi-wrapped arms: ((SELECT ...)) UNION ((SELECT ...))
                    int innerExtra = Math.max(0, parser.countLeadingParensBeforeQuery());
                    if (innerExtra > 1) {
                        // Multi-paren wrapped: ((SELECT ...)) or (((SELECT ...)))
                        // Use parseStatement to handle arbitrary nesting and set ops
                        subStmt = parser.parseStatement();
                        subStmt = tryParseSetOp(subStmt);
                    } else {
                        // Single paren wrapper with potential set ops inside:
                        // (SELECT ...) UNION ALL (SELECT ...) or ((SELECT ...) UNION ALL ...)
                        parser.advance(); // consume inner (
                        subStmt = parser.parseSelect();
                        subStmt = tryParseSetOp(subStmt);
                        parser.expect(TokenType.RIGHT_PAREN); // consume inner )
                        subStmt = tryParseSetOp(subStmt);
                    }
                } else {
                    subStmt = parser.parseSelect();
                    subStmt = tryParseSetOp(subStmt);
                }
                for (int ep = 0; ep < extraParens; ep++) parser.expect(TokenType.RIGHT_PAREN);
                parser.expect(TokenType.RIGHT_PAREN);
                parser.matchKeyword("AS");
                // Try to read optional alias
                String alias = null;
                if (isKeywordValidAsBareAlias()) {
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
                // Check if MERGE is used as a subquery source (not valid SQL)
                if (parser.pos + 1 < parser.tokens.size()) {
                    Token afterParen = parser.tokens.get(parser.pos + 1);
                    if (afterParen.type() == TokenType.KEYWORD && afterParen.value().equals("MERGE")) {
                        throw new ParseException("syntax error", afterParen);
                    }
                }
                // Parenthesized FROM item (e.g., pg_dump style: ((a JOIN b ON ...) JOIN c ON ...))
                // PG rejects bare table names in parens: FROM (tablename) is a syntax error
                parser.advance(); // consume (
                SelectStmt.FromItem inner = parseFromItem(); // recursively parse the join tree
                parser.expect(TokenType.RIGHT_PAREN);
                // Reject bare table references in parens — PG only allows parenthesized joins
                if (inner instanceof SelectStmt.TableRef) {
                    throw new ParseException("syntax error at or near \")\"", parser.tokens.get(parser.pos - 1));
                }
                // Check for an optional alias after the parenthesized FROM item
                // Pattern: (SELECT ... JOIN ...) alias or (SELECT ... JOIN ...) alias(col1, col2, ...)
                String parenAlias = null;
                parser.matchKeyword("AS");
                if (isKeywordValidAsBareAlias()) {
                    parenAlias = parser.readColumnName();
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

        // XMLTABLE(xpath PASSING xml_expr COLUMNS col type PATH xpath, ...) AS alias
        if (parser.checkIdentCI("XMLTABLE")) {
            return parseXmlTableFromItem(lateral);
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
            boolean withOrdinality = parser.matchKeywords("WITH", "ORDINALITY");
            String alias = null;
            if (parser.matchKeyword("AS")) {
                alias = parser.readColumnName();
            } else if (isKeywordValidAsBareAlias()) {
                alias = parser.readIdentifier();
            }
            // Optional column definition list: alias(col [type], col [type], ...)
            List<String> colAliases = null;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance();
                colAliases = new ArrayList<>();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    String colName = parser.readIdentifier(); // column name
                    // Optional type name
                    if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                        String typeName = parser.parseTypeName(); // column type
                        colAliases.add(colName + " " + typeName);
                    } else {
                        colAliases.add(colName);
                    }
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            }
            return new SelectStmt.FunctionFrom(name1, args, alias, colAliases, withOrdinality);
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
                boolean withOrdinality = parser.matchKeywords("WITH", "ORDINALITY");
                String funcAlias = null;
                if (parser.matchKeyword("AS")) {
                    funcAlias = parser.readColumnName();
                } else if (isKeywordValidAsBareAlias()) {
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
                return new SelectStmt.FunctionFrom(qualifiedName, args, funcAlias, colAliases, withOrdinality);
            }
        }

        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readColumnName();
        } else if (isKeywordValidAsBareAlias()) {
            alias = parser.readIdentifier();
        }

        // A relation takes one alias. "FROM t x AS y" is a syntax error at the AS, and reading a
        // second alias over the first silently answered under a name the query never wrote.
        if (alias != null && parser.checkKeyword("AS")) {
            throw ParseException.saying("syntax error at or near \"" + parser.peek().value() + "\"",
                    parser.peek(), "42601");
        }
        // An alias may rename the relation's columns as well as the relation: t AS z(m) calls
        // the first column m. The list renames as far as it reaches, so a shorter one leaves
        // the columns past it under the names the relation gave them.
        List<String> columnAliases = null;
        if (alias != null && parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            columnAliases = new ArrayList<>();
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                columnAliases.add(parser.readIdentifier());
                parser.match(TokenType.COMMA);
            }
            parser.expect(TokenType.RIGHT_PAREN);
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
                // PostgreSQL names the method bare here: what was written is an identifier the
                // grammar resolves, not a string, so it is not quoted back.
                throw new com.memgres.engine.MemgresException(
                    "tablesample method " + method + " does not exist", "42704");
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

        return new SelectStmt.TableRef(schema, tableName, alias, only, columnAliases);
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
     * The words PostgreSQL will not read as an alias written without AS.
     *
     * <p>Most keywords are non-reserved and name a column perfectly well, but a handful cannot
     * stand there because the grammar is still expecting them to continue what came before: after
     * an expression, {@code varying} may be the second half of {@code character varying} and
     * {@code day} the field of an interval, so neither can be a label. PostgreSQL keeps the list
     * itself and reports it through {@code pg_get_keywords().barelabel}; this is that list, read
     * from PostgreSQL 18. It is what makes {@code pg_catalog.character varying} a syntax error
     * pointing at {@code varying} rather than a cast with a column called varying after it.
     */
    private static final java.util.Set<String> NOT_A_BARE_LABEL = new java.util.HashSet<String>(
            java.util.Arrays.asList(
                    "ARRAY", "AS", "CHAR", "CHARACTER", "CREATE", "DAY", "EXCEPT", "FETCH",
                    "FILTER", "FOR", "FROM", "GRANT", "GROUP", "HAVING", "HOUR", "INTERSECT",
                    "INTO", "ISNULL", "LIMIT", "MINUTE", "MONTH", "NOTNULL", "OFFSET", "ON",
                    "ORDER", "OVER", "OVERLAPS", "PRECISION", "RETURNING", "SECOND", "TO",
                    "UNION", "VARYING", "WHERE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "YEAR"));

    /**
     * The words PostgreSQL keeps for itself, which is what an alias may not be.
     *
     * <p>A relation's alias is a plain name, and PostgreSQL takes any keyword for one except the
     * reserved words and the few it reads as a type or function name where a name may stand. It
     * keeps the classification itself and reports it through {@code pg_get_keywords().catcode};
     * this is the R and T rows, read from PostgreSQL 18.
     */
    private static final java.util.Set<String> KEPT_BY_THE_GRAMMAR = new java.util.HashSet<String>(
            java.util.Arrays.asList(
                    "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC",
                    "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE",
                    "COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS",
                    "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA",
                    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE",
                    "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR",
                    "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE", "IN",
                    "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LATERAL",
                    "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL",
                    "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER",
                    "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT",
                    "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "SYSTEM_USER", "TABLE",
                    "TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER",
                    "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"));

    /**
     * Whether the word here may be a relation's alias written without AS.
     *
     * <p>This is the name a FROM item, an UPDATE target and a DELETE target take, which PostgreSQL
     * reads as a plain name rather than as a label. A label is judged by a different and narrower
     * rule -- see {@link #isValidBareTargetLabel()}.
     */
    boolean isKeywordValidAsBareAlias() {
        TokenType type = parser.peek().type();
        if (type == TokenType.QUOTED_IDENTIFIER) return true;
        // A word memgres lexes as an identifier is judged the same way, because PostgreSQL judges
        // the word and not how it was recognised: isnull and overlaps are its own either way.
        if (type != TokenType.IDENTIFIER && type != TokenType.KEYWORD) return false;
        String word = parser.peek().value().toUpperCase();
        if (type == TokenType.KEYWORD && isClauseKeyword(word)) return false;
        return !KEPT_BY_THE_GRAMMAR.contains(word);
    }

    /**
     * Whether the word here may be a select target's label written without AS.
     *
     * <p>Narrower than an alias, and not because a label is more precious: after an expression the
     * grammar may still be expecting the word to continue it, so {@code varying} could be the
     * second half of {@code character varying} and {@code day} the field of an interval. Which
     * words those are is not something to reason out -- it is the list the reference server
     * reports, and a word memgres happens to lex as an identifier rather than as a keyword is
     * judged by it too, because PostgreSQL judges the word and not how it was recognised.
     */
    boolean isValidBareTargetLabel() {
        TokenType type = parser.peek().type();
        if (type == TokenType.QUOTED_IDENTIFIER) return true;
        if (type != TokenType.IDENTIFIER && type != TokenType.KEYWORD) return false;
        String word = parser.peek().value().toUpperCase();
        if (type == TokenType.KEYWORD && isClauseKeyword(word)) return false;
        return !NOT_A_BARE_LABEL.contains(word);
    }

    /**
     * PG takes any value expression here, not only a literal, so a paginating query built with
     * arithmetic ({@code LIMIT :size + 1}) parses the same way it does there.
     */
    Expression parseLimitOffsetExpr() {
        return parser.parseExpression();
    }

    /**
     * {@code TABLE t} with the clauses PostgreSQL's {@code select_no_parens} hangs off a query
     * rather than off a select list: ORDER BY, LIMIT/OFFSET/FETCH and row locking. WHERE, GROUP BY
     * and HAVING are not among them — {@code TABLE t WHERE ...} is a syntax error there too.
     */
    private SelectStmt parseTableQuery() {
        SelectStmt base = parseTableCommand();

        List<SelectStmt.OrderByItem> orderBy = null;
        if (parser.checkKeyword("ORDER")) {
            if (!parser.matchKeywords("ORDER", "BY")) {
                throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"",
                        parser.peek());
            }
            orderBy = parser.parseOrderByList();
        }
        Expression limit = null;
        if (parser.matchKeyword("LIMIT") && !parser.matchKeyword("ALL")) {
            limit = parseLimitOffsetExpr();
        }
        Expression offset = null;
        if (parser.matchKeyword("OFFSET")) {
            offset = parseLimitOffsetExpr();
            parser.matchKeyword("ROW");
            parser.matchKeyword("ROWS");
        }
        boolean withTies = false;
        if (parser.matchKeyword("FETCH")) {
            parser.matchKeyword("FIRST");
            parser.matchKeyword("NEXT");
            limit = parser.checkKeyword("ROW") || parser.checkKeyword("ROWS")
                    ? Literal.ofInt("1") : parseLimitOffsetExpr();
            parser.matchKeyword("ROW");
            parser.matchKeyword("ROWS");
            if (parser.matchKeyword("WITH")) {
                parser.expectKeyword("TIES");
                if (orderBy == null || orderBy.isEmpty()) {
                    throw new com.memgres.engine.MemgresException(
                            "WITH TIES cannot be specified without ORDER BY clause", "42601");
                }
                withTies = true;
            } else {
                parser.matchKeyword("ONLY");
            }
        }
        SelectStmt.LockClause lockClause = parseRowLockClause();

        return new SelectStmt(false, null, base.targets(), base.from(), null, null, null, null,
                orderBy, limit, offset, null, null, lockClause, withTies);
    }

    /** FOR UPDATE / FOR NO KEY UPDATE / FOR SHARE / FOR KEY SHARE, with its options. */
    private SelectStmt.LockClause parseRowLockClause() {
        String lockMode = null;
        boolean nowait = false;
        boolean skipLocked = false;
        List<String> ofTables = new ArrayList<>();
        while (parser.checkKeyword("FOR")) {
            parser.advance();
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
            } else {
                throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"",
                        parser.peek());
            }
            if (parser.matchKeyword("OF")) {
                ofTables = new ArrayList<>();
                ofTables.add(readLockTargetName(lockMode));
                while (parser.match(TokenType.COMMA)) ofTables.add(readLockTargetName(lockMode));
            }
            if (parser.matchKeyword("NOWAIT")) {
                nowait = true;
            } else if (parser.matchKeyword("SKIP")) {
                parser.matchKeyword("LOCKED");
                skipLocked = true;
            }
        }
        return lockMode == null ? null
                : new SelectStmt.LockClause(lockMode, nowait, skipLocked, ofTables);
    }

    /**
     * One relation name in {@code FOR UPDATE OF}.
     *
     * <p>The grammar takes a bare name here: what {@code OF} refers to is a FROM entry, which an
     * alias may have renamed, so a schema qualification could not identify one. PostgreSQL says
     * so rather than reporting a syntax error at the dot.
     */
    private String readLockTargetName(String lockMode) {
        String name = parser.readIdentifier();
        if (parser.check(TokenType.DOT)) {
            throw new com.memgres.engine.MemgresException(
                    "FOR " + (lockMode == null ? "UPDATE" : lockMode)
                            + " must specify unqualified relation names", "42601");
        }
        return name;
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
            alias = parser.readColumnName();
        } else if (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER) {
            alias = parser.readIdentifier();
        }
        JsonTableExpr jtExpr = new JsonTableExpr(input, path, columns, passing, onError);
        // Store as FunctionFrom with the JsonTableExpr packed into args
        return new SelectStmt.FunctionFrom("__json_table__", Cols.listOf(jtExpr), alias, null);
    }

    // ---- XMLTABLE FROM item ----

    private SelectStmt.FromItem parseXmlTableFromItem(boolean lateral) {
        parser.advance(); // consume XMLTABLE
        parser.expect(TokenType.LEFT_PAREN);
        // Parse XPath expression (string literal)
        Expression xpath = parser.parseExpression();
        // PASSING clause
        Expression xmlDoc = null;
        if (parser.matchKeyword("PASSING")) {
            xmlDoc = parser.parseExpression();
        }
        // COLUMNS clause
        parser.expectKeyword("COLUMNS");
        List<String> colNames = new ArrayList<>();
        List<String> colTypes = new ArrayList<>();
        List<Expression> colPaths = new ArrayList<>();
        do {
            String colName = parser.readIdentifier();
            // FOR ORDINALITY
            if (parser.matchKeywords("FOR", "ORDINALITY")) {
                colNames.add(colName);
                colTypes.add("integer");
                colPaths.add(null);
                continue;
            }
            String typeName = parser.parseTypeName();
            Expression pathExpr = null;
            if (parser.matchKeyword("PATH")) {
                pathExpr = parser.parseExpression();
            }
            // Optional DEFAULT and NOT NULL clauses
            if (parser.matchKeyword("DEFAULT")) {
                parser.parseExpression(); // consume default expr
            }
            parser.matchKeywords("NOT", "NULL");
            colNames.add(colName);
            colTypes.add(typeName);
            colPaths.add(pathExpr);
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        // Alias
        String alias = null;
        if (parser.matchKeyword("AS")) {
            alias = parser.readColumnName();
        } else if (!parser.isAtEnd() && (parser.peek().type() == TokenType.IDENTIFIER || parser.peek().type() == TokenType.QUOTED_IDENTIFIER)) {
            alias = parser.readIdentifier();
        }
        // Pack as FunctionFrom with special name "__xmltable__"
        // Encode xpath, xmlDoc, colNames, colTypes, colPaths into args
        List<Expression> args = new ArrayList<>();
        args.add(xpath);
        if (xmlDoc != null) args.add(xmlDoc);
        // Store column metadata as string literals
        for (int i = 0; i < colNames.size(); i++) {
            String pathStr;
            if (colPaths.get(i) != null && colPaths.get(i) instanceof Literal) {
                pathStr = ((Literal) colPaths.get(i)).value();
            } else if (colPaths.get(i) != null) {
                pathStr = colPaths.get(i).toString();
            } else {
                pathStr = colNames.get(i);
            }
            args.add(new Literal(Literal.LiteralType.STRING, colNames.get(i) + ":" + colTypes.get(i) + ":" + pathStr));
        }
        return new SelectStmt.FunctionFrom("__xmltable__", args, alias, null);
    }

    private List<JsonTableExpr.JsonTableColumn> parseJsonTableColumns() {
        List<JsonTableExpr.JsonTableColumn> cols = new ArrayList<>();
        do {
            // NESTED PATH ...
            // Disambiguate: NESTED as a column name vs. NESTED PATH clause.
            // If NESTED is followed by PATH or a string literal, it's a NESTED PATH clause.
            // Otherwise (e.g., "nested jsonb ..."), it's a column named "nested".
            if (parser.checkKeyword("NESTED") &&
                    (parser.checkKeywordAt(1, "PATH") ||
                     (parser.pos + 1 < parser.tokens.size() && parser.tokens.get(parser.pos + 1).type() == TokenType.STRING_LITERAL))) {
                parser.advance(); // consume NESTED
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
            // type [FORMAT JSON] [EXISTS] PATH 'expr'
            String typeName = parser.parseTypeName();
            // Optional FORMAT JSON clause (e.g., nested jsonb FORMAT JSON PATH '$.data')
            if (parser.matchKeyword("FORMAT")) {
                parser.expectKeyword("JSON"); // consume FORMAT JSON, ignore (just a hint)
            }
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
