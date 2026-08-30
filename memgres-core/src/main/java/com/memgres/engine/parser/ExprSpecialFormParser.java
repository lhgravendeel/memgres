package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses special SQL syntax forms: CASE, CAST, ARRAY, SUBSTRING, TRIM, POSITION,
 * OVERLAY, EXTRACT, INTERVAL, XML functions, window functions, and qualified operators.
 * Extracted from ExpressionParser to keep the precedence-climbing core focused.
 */
class ExprSpecialFormParser {

    private final ExpressionParser ep;

    ExprSpecialFormParser(ExpressionParser ep) {
        this.ep = ep;
    }

    // ---- CASE / CAST / ARRAY ----

    Expression parseCaseExpression() {
        ep.expectKeyword("CASE");
        Expression operand = null;
        if (!ep.checkKeyword("WHEN")) {
            // A CASE opens with either its operand or its first WHEN. With neither there is
            // nothing to choose between, and PostgreSQL stops at the word that stands there.
            if (ep.checkKeyword("END") || ep.checkKeyword("ELSE") || ep.checkKeyword("THEN")) {
                throw ParseException.at(ep.peek());
            }
            operand = ep.parseExpression();
        }
        // A CASE has at least one WHEN. With none there is nothing to choose between, and
        // PostgreSQL stops at the word that stands where the first WHEN should.
        if (!ep.checkKeyword("WHEN")) {
            throw ParseException.at(ep.peek());
        }
        List<CaseExpr.WhenClause> whens = new ArrayList<>();
        while (ep.matchKeyword("WHEN")) {
            Expression condition = ep.parseExpression();
            ep.expectKeyword("THEN");
            Expression result = ep.parseExpression();
            whens.add(new CaseExpr.WhenClause(condition, result));
        }
        Expression elseExpr = null;
        if (ep.matchKeyword("ELSE")) {
            elseExpr = ep.parseExpression();
        }
        ep.expectKeyword("END");
        return new CaseExpr(operand, whens, elseExpr);
    }

    Expression parseCastFunction() {
        ep.expectKeyword("CAST");
        ep.expect(TokenType.LEFT_PAREN);
        Expression expr = ep.parseExpression();
        ep.expectKeyword("AS");
        String typeName = ep.parseTypeName();
        ep.expect(TokenType.RIGHT_PAREN);
        return new CastExpr(expr, typeName);
    }

    Expression parseArrayConstructor() {
        ep.expectKeyword("ARRAY");
        if (ep.check(TokenType.LEFT_PAREN)) {
            ep.advance();
            int extraParens = Math.max(0, ep.countLeadingParensBeforeQuery());
            ep.consumeLeadingParens(extraParens);
            Statement subquery = ep.parseSubqueryWithSetOps();
            ep.consumeTrailingParens(extraParens);
            ep.expect(TokenType.RIGHT_PAREN);
            return new ArraySubqueryExpr(subquery);
        }
        Expression arr = parseArrayBracket();
        if (arr instanceof ArrayExpr && ((ArrayExpr) arr).elements().isEmpty() && !ep.check(TokenType.CAST)) {
            // ParseException(message, token, state) reports the token and drops the message.
            // PostgreSQL shows the cast that settles the type rather than only refusing the array.
            throw ParseException.saying("cannot determine type of empty array"
                    + "\n  Hint: Explicitly cast to the desired type, for example"
                    + " ARRAY[]::integer[].", ep.peek(), "42P18");
        }
        return arr;
    }

    private Expression parseArrayBracket() {
        ep.expect(TokenType.LEFT_BRACKET);
        List<Expression> elements = new ArrayList<>();
        if (!ep.check(TokenType.RIGHT_BRACKET)) {
            do {
                if (ep.check(TokenType.LEFT_BRACKET)) {
                    elements.add(parseArrayBracket());
                } else {
                    elements.add(ep.parseExpression());
                }
            } while (ep.match(TokenType.COMMA));
        }
        ep.expect(TokenType.RIGHT_BRACKET);
        return new ArrayExpr(elements);
    }

    /** The comma that separates argument {@code index} from the one before it. */
    private Token commaBeforeArgument(int listStart, int index) {
        int depth = 0;
        int seen = 0;
        for (int i = listStart; i < ep.tokens.size(); i++) {
            Token t = ep.tokens.get(i);
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            else if (t.type() == TokenType.RIGHT_PAREN) {
                if (depth == 0) break;
                depth--;
            } else if (t.type() == TokenType.COMMA && depth == 0) {
                seen++;
                if (seen == index) return t;
            }
        }
        return ep.peek();
    }

    Expression parseBuiltinFunction() {
        String name = ep.advance().value().toLowerCase(java.util.Locale.ROOT);
        ep.expect(TokenType.LEFT_PAREN);
        int listStart = ep.pos;
        List<Expression> args = new ArrayList<>();
        if (!ep.check(TokenType.RIGHT_PAREN)) {
            args = ep.parseExpressionList();
        }
        // These are grammar productions in PostgreSQL, not function calls, so an argument list of
        // the wrong length is a syntax error at the token that should have been a comma. Reading
        // them as ordinary calls let NULLIF(1) through to an array index that was not there.
        // NULLIF takes exactly two; the variadic forms take at least one.
        boolean tooMany = "nullif".equals(name) && args.size() > 2;
        boolean tooFew = "nullif".equals(name) ? args.size() < 2 : args.size() < 1;
        if (tooMany) {
            // PostgreSQL reports the comma that begins the argument it has no room for.
            Token comma = commaBeforeArgument(listStart, 2);
            throw ParseException.saying("syntax error at or near \"" + comma.value() + "\"",
                    comma, "42601");
        }
        if (tooFew) {
            throw ParseException.saying("syntax error at or near \"" + ep.peek().value() + "\"",
                    ep.peek(), "42601");
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr(name, args);
    }

    // ---- Special-syntax keyword functions ----

    Expression parseInterval() {
        // INTERVAL already consumed by caller or we consume it here
        // Check if 'interval' is used as a column name
        if (ep.pos + 1 < ep.tokens.size()) {
            TokenType nextType = ep.tokens.get(ep.pos + 1).type();
            String nextVal = ep.tokens.get(ep.pos + 1).value().toUpperCase(java.util.Locale.ROOT);
            boolean isColumnContext = nextType == TokenType.EQUALS || nextType == TokenType.NOT_EQUALS
                    || nextType == TokenType.LESS_THAN || nextType == TokenType.GREATER_THAN
                    || nextType == TokenType.LESS_EQUALS || nextType == TokenType.GREATER_EQUALS
                    || nextType == TokenType.COMMA || nextType == TokenType.RIGHT_PAREN
                    || nextType == TokenType.DOT || nextType == TokenType.SEMICOLON
                    || nextType == TokenType.EOF
                    || (nextType == TokenType.KEYWORD
                        && ("IS".equals(nextVal) || "AND".equals(nextVal)
                            || "OR".equals(nextVal) || "IN".equals(nextVal)
                            || "FROM".equals(nextVal) || "WHERE".equals(nextVal)
                            || "AS".equals(nextVal) || "THEN".equals(nextVal)
                            || "ELSE".equals(nextVal) || "END".equals(nextVal)
                            || "ORDER".equals(nextVal) || "GROUP".equals(nextVal)
                            || "HAVING".equals(nextVal) || "LIMIT".equals(nextVal)
                            || "OFFSET".equals(nextVal) || "UNION".equals(nextVal)
                            || "INTERSECT".equals(nextVal) || "EXCEPT".equals(nextVal)
                            || "SET".equals(nextVal) || "NOT".equals(nextVal)
                            || "LIKE".equals(nextVal) || "ILIKE".equals(nextVal)
                            || "BETWEEN".equals(nextVal) || "SIMILAR".equals(nextVal)));
            if (isColumnContext) {
                ep.advance();
                return new ColumnRef("interval");
            }
        }
        ep.advance();
        // interval(3) '1.234567 seconds': the precision is written before the literal, the way
        // PG spells a typed literal for any type that takes a modifier.
        String leadingPrecision = parseLeadingPrecision();
        if (ep.check(TokenType.STRING_LITERAL)) {
            String val = ep.advance().value();
            // Check for optional interval qualifier: YEAR TO MONTH, DAY TO SECOND(2), etc.
            String qualifier = parseIntervalQualifier();
            String type = "interval";
            if (qualifier != null) type = type + " " + qualifier;
            if (leadingPrecision != null) type = type + leadingPrecision;
            return new CastExpr(Literal.ofString(val), type);
        }
        return new CastExpr(ep.parsePrimary(), "interval");
    }

    /**
     * Consume a {@code (N)} that stands between INTERVAL and its literal, but only when a literal
     * really follows: {@code interval(x)} with anything else after the parenthesis is a call.
     *
     * @return the precision written as "(N)", or null when there is none
     */
    private String parseLeadingPrecision() {
        if (!ep.check(TokenType.LEFT_PAREN)) return null;
        if (ep.pos + 3 >= ep.tokens.size()) return null;
        if (ep.tokens.get(ep.pos + 1).type() != TokenType.INTEGER_LITERAL) return null;
        if (ep.tokens.get(ep.pos + 2).type() != TokenType.RIGHT_PAREN) return null;
        if (ep.tokens.get(ep.pos + 3).type() != TokenType.STRING_LITERAL) return null;
        ep.advance();
        String digits = ep.advance().value();
        ep.advance();
        return "(" + digits + ")";
    }

    /**
     * Parse optional interval qualifier: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND,
     * or compound forms like YEAR TO MONTH, DAY TO SECOND, plus the fractional-seconds
     * precision the last field may carry -- SECOND(3), DAY TO SECOND(2).
     * Returns null if no qualifier is present.
     */
    private String parseIntervalQualifier() {
        if (ep.checkIntervalField()) {
            String field = ep.advance().value().toLowerCase(java.util.Locale.ROOT);
            if (ep.checkKeyword("TO") || ep.checkIdentCI("TO")) {
                ep.advance(); // consume TO
                String toField = ep.readIdentifier().toLowerCase(java.util.Locale.ROOT);
                return field + " to " + toField + parseFieldPrecision();
            }
            return field + parseFieldPrecision();
        }
        return null;
    }

    /** Consume the {@code (N)} a SECOND field may carry; returns "" when there is none. */
    private String parseFieldPrecision() {
        if (!ep.check(TokenType.LEFT_PAREN)) return "";
        if (ep.pos + 2 >= ep.tokens.size()) return "";
        if (ep.tokens.get(ep.pos + 1).type() != TokenType.INTEGER_LITERAL) return "";
        if (ep.tokens.get(ep.pos + 2).type() != TokenType.RIGHT_PAREN) return "";
        ep.advance();
        String digits = ep.advance().value();
        ep.advance();
        return "(" + digits + ")";
    }

    Expression parseTimestamp() {
        ep.advance(); // consume TIMESTAMP
        String tsType = "timestamp";
        // A typed literal may carry the type's own precision: timestamp(2) '...' keeps two
        // digits of the second, exactly as a column declared that way would.
        String precision = parseFieldPrecision();
        if (ep.checkKeyword("WITH")) {
            ep.advance();
            if (ep.checkKeyword("TIME")) ep.advance();
            if (ep.checkKeyword("ZONE")) ep.advance();
            tsType = "timestamptz";
        } else if (ep.checkKeyword("WITHOUT")) {
            ep.advance();
            if (ep.checkKeyword("TIME")) ep.advance();
            if (ep.checkKeyword("ZONE")) ep.advance();
            tsType = "timestamp";
        }
        if (ep.pos < ep.tokens.size() && ep.tokens.get(ep.pos).type() == TokenType.STRING_LITERAL) {
            String val = ep.advance().value();
            return new CastExpr(Literal.ofString(val), tsType + precision);
        }
        return new ColumnRef("timestamp");
    }

    /**
     * {@code TIME WITH TIME ZONE '10:00+02'} and its WITHOUT form, which are as much a typed
     * literal as {@code TIMESTAMP WITH TIME ZONE '...'} is. Returns null, with the parser left
     * where it started, when what follows TIME is not one of those spellings.
     */
    Expression parseQualifiedTimeLiteral() {
        int saved = ep.pos;
        ep.advance(); // consume TIME
        String precision = parseFieldPrecision();
        String type;
        if (ep.checkKeyword("WITH")) {
            type = "timetz";
            ep.advance();
            if (ep.checkKeyword("TIME")) ep.advance();
            if (ep.checkKeyword("ZONE")) ep.advance();
        } else if (ep.checkKeyword("WITHOUT")) {
            type = "time";
            ep.advance();
            if (ep.checkKeyword("TIME")) ep.advance();
            if (ep.checkKeyword("ZONE")) ep.advance();
        } else if (!precision.isEmpty()) {
            // TIME(2) '...' with no zone spelling after it is the plain time, carrying its
            // precision the way TIMESTAMP(2) does.
            type = "time";
        } else {
            ep.pos = saved;
            return null;
        }
        if (ep.pos < ep.tokens.size() && ep.tokens.get(ep.pos).type() == TokenType.STRING_LITERAL) {
            return new CastExpr(Literal.ofString(ep.advance().value()), type + precision);
        }
        ep.pos = saved;
        return null;
    }

    Expression parseSubstring() {
        ep.advance(); // consume SUBSTRING
        ep.expect(TokenType.LEFT_PAREN);
        Expression str = ep.parseExpression();
        if (ep.matchKeyword("FROM")) {
            Expression from = ep.parseExpression();
            List<Expression> args;
            if (ep.matchKeyword("FOR")) {
                Expression len = ep.parseExpression();
                args = Cols.listOf(str, from, len);
            } else {
                args = Cols.listOf(str, from);
            }
            ep.expect(TokenType.RIGHT_PAREN);
            return grammarCall("substring", args);
        }
        if (ep.matchKeyword("FOR")) {
            Expression len = ep.parseExpression();
            List<Expression> args;
            if (ep.matchKeyword("FROM")) {
                Expression from = ep.parseExpression();
                args = Cols.listOf(str, from, len);
            } else {
                args = Cols.listOf(str, Literal.ofInt("1"), len);
            }
            ep.expect(TokenType.RIGHT_PAREN);
            return grammarCall("substring", args);
        }
        if (ep.matchKeyword("SIMILAR")) {
            Expression pattern = ep.parseExpression();
            ep.expectKeyword("ESCAPE");
            Expression escape = ep.parseExpression();
            ep.expect(TokenType.RIGHT_PAREN);
            return new FunctionCallExpr("substring_similar", Cols.listOf(str, pattern, escape));
        }
        List<Expression> args = new ArrayList<>();
        args.add(str);
        while (ep.match(TokenType.COMMA)) {
            args.add(ep.parseExpression());
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("substring", args);
    }

    Expression parsePosition() {
        ep.advance(); // consume POSITION
        ep.expect(TokenType.LEFT_PAREN);
        // Parse the search operand up to (but not including) IN. parsePostfix absorbs
        // ::type casts (e.g. '\x34'::bytea) while stopping before the IN keyword.
        Expression substring = ep.parsePostfix();
        ep.expectKeyword("IN");
        Expression string = ep.parseExpression();
        ep.expect(TokenType.RIGHT_PAREN);
        return grammarCall("position", Cols.listOf(substring, string));
    }

    Expression parseOverlay() {
        ep.advance(); // consume OVERLAY
        ep.expect(TokenType.LEFT_PAREN);
        Expression str = ep.parseExpression();
        ep.expectKeyword("PLACING");
        Expression replacement = ep.parseExpression();
        ep.expectKeyword("FROM");
        Expression start = ep.parseExpression();
        List<Expression> args;
        if (ep.matchKeyword("FOR")) {
            Expression count = ep.parseExpression();
            args = Cols.listOf(str, replacement, start, count);
        } else {
            args = Cols.listOf(str, replacement, start);
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return grammarCall("overlay", args);
    }

    Expression parseExtract() {
        ep.advance(); // consume EXTRACT
        ep.expect(TokenType.LEFT_PAREN);
        String field;
        if (ep.check(TokenType.STRING_LITERAL)) {
            field = ep.advance().value();
        } else {
            field = ep.readIdentifier();
        }
        ep.expectKeyword("FROM");
        Expression source = ep.parseExpression();
        ep.expect(TokenType.RIGHT_PAREN);
        return grammarCall("extract", Cols.listOf(Literal.ofString(field), source));
    }

    Expression parseTrim() {
        ep.advance(); // consume TRIM
        ep.expect(TokenType.LEFT_PAREN);
        String mode = "BOTH";
        boolean modeExplicit = false;
        if (ep.matchKeyword("LEADING")) { mode = "LEADING"; modeExplicit = true; }
        else if (ep.matchKeyword("TRAILING")) { mode = "TRAILING"; modeExplicit = true; }
        else if (ep.matchKeyword("BOTH")) { mode = "BOTH"; modeExplicit = true; }

        Expression charsExpr = null;
        Expression stringExpr;
        if (modeExplicit && ep.matchKeyword("FROM")) {
            stringExpr = ep.parseExpression();
        } else {
            Expression firstExpr = ep.parseExpression();
            if (ep.matchKeyword("FROM")) {
                charsExpr = firstExpr;
                stringExpr = ep.parseExpression();
            } else if (ep.match(TokenType.COMMA)) {
                // trim(string, characters) is the plain call spelling of the same thing, with
                // the arguments the other way round from the FROM form. It was not accepted at
                // all, so the ordinary way of writing this was a syntax error.
                stringExpr = firstExpr;
                charsExpr = ep.parseExpression();
            } else {
                stringExpr = firstExpr;
            }
        }
        ep.expect(TokenType.RIGHT_PAREN);

        String funcName;
        switch (mode) {
            case "LEADING":
                funcName = "ltrim";
                break;
            case "TRAILING":
                funcName = "rtrim";
                break;
            default:
                funcName = "btrim";
                break;
        }
        if (charsExpr != null) {
            return grammarCall(funcName, Cols.listOf(stringExpr, charsExpr));
        } else {
            return grammarCall(funcName, Cols.listOf(stringExpr));
        }
    }

    // ---- Window function parsing ----

    WindowFuncExpr parseWindowFunction(String name, List<Expression> args, boolean distinct, boolean star) {
        return parseWindowFunction(name, args, distinct, star, false, false, null);
    }

    WindowFuncExpr parseWindowFunction(String name, List<Expression> args, boolean distinct, boolean star, boolean ignoreNulls) {
        return parseWindowFunction(name, args, distinct, star, ignoreNulls, false, null);
    }

    WindowFuncExpr parseWindowFunction(String name, List<Expression> args, boolean distinct, boolean star, boolean ignoreNulls, boolean fromLast, Expression filter) {
        ep.expectKeyword("OVER");

        if (!ep.check(TokenType.LEFT_PAREN)) {
            String windowName = ep.readIdentifier();
            return new WindowFuncExpr(name, args, distinct, star, null, null, null, windowName, ignoreNulls, fromLast, filter, false);
        }

        ep.expect(TokenType.LEFT_PAREN);

        List<Expression> partitionBy = null;
        List<SelectStmt.OrderByItem> orderBy = null;
        WindowFuncExpr.FrameClause frame = null;

        // OVER (w ...) refines an existing named window: the name comes first, and what
        // follows adds to it -- most usefully a frame the named window did not specify
        String baseWindow = null;
        if (!ep.check(TokenType.RIGHT_PAREN)
                && !ep.checkKeyword("PARTITION") && !ep.checkKeyword("ORDER")
                && !ep.checkKeyword("ROWS") && !ep.checkKeyword("RANGE") && !ep.checkKeyword("GROUPS")) {
            baseWindow = ep.readIdentifier();
        }

        if (ep.matchKeywords("PARTITION", "BY")) {
            partitionBy = ep.parseExpressionList();
        }
        if (ep.matchKeywords("ORDER", "BY")) {
            orderBy = ep.parseOrderByList();
        }
        if (ep.checkKeyword("ROWS") || ep.checkKeyword("RANGE") || ep.checkKeyword("GROUPS")) {
            frame = parseWindowFrame();
        }

        ep.expect(TokenType.RIGHT_PAREN);
        return new WindowFuncExpr(name, args, distinct, star, partitionBy, orderBy, frame,
                baseWindow, ignoreNulls, fromLast, filter, baseWindow != null);
    }

    WindowFuncExpr.FrameClause parseWindowFrame() {
        WindowFuncExpr.FrameType frameType;
        if (ep.matchKeyword("ROWS")) frameType = WindowFuncExpr.FrameType.ROWS;
        else if (ep.matchKeyword("RANGE")) frameType = WindowFuncExpr.FrameType.RANGE;
        else { ep.expectKeyword("GROUPS"); frameType = WindowFuncExpr.FrameType.GROUPS; }

        WindowFuncExpr.FrameBound start, end;
        if (ep.matchKeyword("BETWEEN")) {
            start = parseFrameBound();
            ep.expectKeyword("AND");
            end = parseFrameBound();
        } else {
            start = parseFrameBound();
            end = new WindowFuncExpr.FrameBound(
                    WindowFuncExpr.FrameBoundType.CURRENT_ROW, null);
        }

        // Parse optional EXCLUDE clause
        WindowFuncExpr.ExcludeMode excludeMode = null;
        if (ep.matchKeyword("EXCLUDE")) {
            if (ep.matchKeyword("CURRENT")) {
                ep.expectKeyword("ROW");
                excludeMode = WindowFuncExpr.ExcludeMode.CURRENT_ROW;
            } else if (ep.matchKeyword("TIES")) {
                excludeMode = WindowFuncExpr.ExcludeMode.TIES;
            } else if (ep.matchKeyword("GROUP")) {
                excludeMode = WindowFuncExpr.ExcludeMode.GROUP;
            } else if (ep.matchKeyword("NO")) {
                ep.expectKeyword("OTHERS");
                excludeMode = WindowFuncExpr.ExcludeMode.NO_OTHERS;
            } else {
                // What may follow EXCLUDE is four spellings and nothing else, and anything else
                // is a syntax error at the word written. Raised as a bare RuntimeException it
                // reached the client as an internal error instead.
                throw ParseException.at(ep.peek());
            }
        }

        rejectImpossibleFrame(start, end);
        return new WindowFuncExpr.FrameClause(frameType, start, end, excludeMode);
    }

    /**
     * A frame whose start is after its end contains no rows under any reading, so PostgreSQL
     * rejects the shape while parsing rather than computing an answer from it. The order of the
     * checks below is the order PostgreSQL applies them, which is what picks the message.
     */
    private static void rejectImpossibleFrame(WindowFuncExpr.FrameBound start, WindowFuncExpr.FrameBound end) {
        WindowFuncExpr.FrameBoundType s = start.boundType();
        WindowFuncExpr.FrameBoundType e = end.boundType();
        if (s == WindowFuncExpr.FrameBoundType.UNBOUNDED_FOLLOWING) {
            throw windowingError("frame start cannot be UNBOUNDED FOLLOWING");
        }
        if (e == WindowFuncExpr.FrameBoundType.UNBOUNDED_PRECEDING) {
            throw windowingError("frame end cannot be UNBOUNDED PRECEDING");
        }
        if (s == WindowFuncExpr.FrameBoundType.CURRENT_ROW
                && e == WindowFuncExpr.FrameBoundType.PRECEDING) {
            throw windowingError("frame starting from current row cannot have preceding rows");
        }
        if (s == WindowFuncExpr.FrameBoundType.FOLLOWING
                && (e == WindowFuncExpr.FrameBoundType.PRECEDING
                    || e == WindowFuncExpr.FrameBoundType.CURRENT_ROW)) {
            throw windowingError("frame starting from following row cannot have preceding rows");
        }
    }

    /** {@code 42P20} — the window specification itself is invalid. */
    private static MemgresException windowingError(String message) {
        return new MemgresException(message, "42P20");
    }

    private WindowFuncExpr.FrameBound parseFrameBound() {
        if (ep.matchKeyword("UNBOUNDED")) {
            if (ep.matchKeyword("PRECEDING")) return new WindowFuncExpr.FrameBound(WindowFuncExpr.FrameBoundType.UNBOUNDED_PRECEDING, null);
            ep.expectKeyword("FOLLOWING");
            return new WindowFuncExpr.FrameBound(WindowFuncExpr.FrameBoundType.UNBOUNDED_FOLLOWING, null);
        }
        if (ep.matchKeyword("CURRENT")) {
            ep.expectKeyword("ROW");
            return new WindowFuncExpr.FrameBound(WindowFuncExpr.FrameBoundType.CURRENT_ROW, null);
        }
        // A frame offset is an ordinary value expression — "1 + 1 PRECEDING" and
        // "(SELECT max(v) FROM t) / 10 PRECEDING" are both legal — so it is parsed down to the
        // operator level below comparison, which stops on the PRECEDING/FOLLOWING keyword that
        // ends it. A sign is part of the offset: PostgreSQL parses "-1 PRECEDING" and rejects it
        // as a negative size at run time, not as a syntax error.
        Expression offset = ep.parseOtherOps();
        if (ep.matchKeyword("PRECEDING")) return new WindowFuncExpr.FrameBound(WindowFuncExpr.FrameBoundType.PRECEDING, offset);
        ep.expectKeyword("FOLLOWING");
        return new WindowFuncExpr.FrameBound(WindowFuncExpr.FrameBoundType.FOLLOWING, offset);
    }

    // ---- Qualified OPERATOR(...) parsing ----

    /** Shared: read OPERATOR(schema.op) spec after OPERATOR keyword has been consumed. */
        private static final class OperatorSpec {
        public final String schema;
        public final String opSymbol;

        public OperatorSpec(String schema, String opSymbol) {
            this.schema = schema;
            this.opSymbol = opSymbol;
        }

        public String schema() { return schema; }
        public String opSymbol() { return opSymbol; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OperatorSpec that = (OperatorSpec) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(opSymbol, that.opSymbol);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, opSymbol);
        }

        @Override
        public String toString() {
            return "OperatorSpec[schema=" + schema + ", " + "opSymbol=" + opSymbol + "]";
        }
    }

    private OperatorSpec readOperatorSpec() {
        ep.expect(TokenType.LEFT_PAREN);
        String schema = null;
        if (ep.check(TokenType.IDENTIFIER) || ep.check(TokenType.KEYWORD)) {
            schema = ep.advance().value();
        }
        if (!ep.match(TokenType.DOT)) {
            throw new ParseException("Expected '.' in OPERATOR(schema.op)", ep.peek());
        }
        Token opTok = ep.peek();
        if (opTok.type() == TokenType.EOF || opTok.type() == TokenType.RIGHT_PAREN) {
            throw new ParseException("Expected operator symbol", opTok);
        }
        // PG reads exactly ONE operator token (the lexer already applies tokenization
        // rules, e.g. +++ becomes +, +, + but ~~> stays as one token).
        String opSymbol = opTok.value();
        ep.advance();
        ep.expect(TokenType.RIGHT_PAREN);
        return new OperatorSpec(schema, opSymbol);
    }

    /** Parse OPERATOR(schema.op) as an infix binary operator. */
    Expression parseQualifiedOperator(Expression left) {
        ep.advance(); // consume OPERATOR keyword
        OperatorSpec spec = readOperatorSpec();

        BinaryExpr.BinOp binOp;
        switch (spec.opSymbol) {
            case "+":
                binOp = BinaryExpr.BinOp.ADD;
                break;
            case "-":
                binOp = BinaryExpr.BinOp.SUBTRACT;
                break;
            case "*":
                binOp = BinaryExpr.BinOp.MULTIPLY;
                break;
            case "/":
                binOp = BinaryExpr.BinOp.DIVIDE;
                break;
            case "%":
                binOp = BinaryExpr.BinOp.MODULO;
                break;
            case "=":
                binOp = BinaryExpr.BinOp.EQUAL;
                break;
            case "<>":
                binOp = BinaryExpr.BinOp.NOT_EQUAL;
                break;
            case "!=":
                binOp = BinaryExpr.BinOp.NOT_EQUAL;
                break;
            case "<":
                binOp = BinaryExpr.BinOp.LESS_THAN;
                break;
            case ">":
                binOp = BinaryExpr.BinOp.GREATER_THAN;
                break;
            case "<=":
                binOp = BinaryExpr.BinOp.LESS_EQUAL;
                break;
            case ">=":
                binOp = BinaryExpr.BinOp.GREATER_EQUAL;
                break;
            case "||":
                binOp = BinaryExpr.BinOp.CONCAT;
                break;
            case "&&":
                binOp = BinaryExpr.BinOp.OVERLAP;
                break;
            case "@>":
                binOp = BinaryExpr.BinOp.CONTAINS;
                break;
            case "<@":
                binOp = BinaryExpr.BinOp.CONTAINED_BY;
                break;
            case "~~":
                binOp = BinaryExpr.BinOp.LIKE;
                break;
            case "~~*":
                binOp = BinaryExpr.BinOp.ILIKE;
                break;
            case "!~~": {
                Expression rhsNotLike = ep.parseOtherOps();
                Expression likeExpr = new BinaryExpr(left, BinaryExpr.BinOp.LIKE, rhsNotLike);
                Expression notLike = new UnaryExpr(UnaryExpr.UnaryOp.NOT, likeExpr);
                if (spec.schema != null) notLike = new QualifiedOperatorExpr(spec.schema, spec.opSymbol, notLike);
                return notLike;
            }
            case "!~~*": {
                Expression rhsNotIlike = ep.parseOtherOps();
                Expression ilikeExpr = new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, rhsNotIlike);
                Expression notIlike = new UnaryExpr(UnaryExpr.UnaryOp.NOT, ilikeExpr);
                if (spec.schema != null) notIlike = new QualifiedOperatorExpr(spec.schema, spec.opSymbol, notIlike);
                return notIlike;
            }
            case "~":
                binOp = BinaryExpr.BinOp.REGEX_MATCH;
                break;
            case "~*":
                binOp = BinaryExpr.BinOp.REGEX_IMATCH;
                break;
            case "!~":
                binOp = BinaryExpr.BinOp.NOT_REGEX_MATCH;
                break;
            case "!~*":
                binOp = BinaryExpr.BinOp.NOT_REGEX_IMATCH;
                break;
            default:
                // User-defined operator: create CustomOperatorExpr for runtime dispatch
                Expression right = ep.parseOtherOps();
                return new CustomOperatorExpr(spec.schema, spec.opSymbol, left, right);
        }

        Expression rhs = ep.parseOtherOps();
        Expression result = new BinaryExpr(left, binOp, rhs);
        // Wrap in QualifiedOperatorExpr to preserve schema for validation at runtime
        if (spec.schema != null) {
            result = new QualifiedOperatorExpr(spec.schema, spec.opSymbol, result);
        }
        return result;
    }

    /** Parse OPERATOR(schema.op)(arg1[, arg2]) in prefix/function-call position. */
    Expression parsePrefixQualifiedOperator() {
        ep.advance(); // consume OPERATOR keyword
        OperatorSpec spec = readOperatorSpec();

        ep.expect(TokenType.LEFT_PAREN);
        List<Expression> args = ep.parseExpressionList();
        ep.expect(TokenType.RIGHT_PAREN);

        if (args.size() == 2) {
            // Binary operator in function-call style: OPERATOR(schema.op)(a, b)
            return new CustomOperatorExpr(spec.schema, spec.opSymbol, args.get(0), args.get(1));
        }

        if (args.size() == 1) {
            // Known unary built-in operators
            if ("+".equals(spec.opSymbol)) {
                return new QualifiedOperatorExpr(spec.schema, spec.opSymbol,
                    new UnaryExpr(UnaryExpr.UnaryOp.POSITIVE, args.get(0)));
            } else if ("-".equals(spec.opSymbol)) {
                return new QualifiedOperatorExpr(spec.schema, spec.opSymbol,
                    new UnaryExpr(UnaryExpr.UnaryOp.NEGATE, args.get(0)));
            }
            // User-defined unary prefix operator
            return new CustomOperatorExpr(spec.schema, spec.opSymbol, null, args.get(0));
        }
        throw new com.memgres.engine.MemgresException(
            "operator does not exist: " + (spec.schema != null ? spec.schema + "." : "") + spec.opSymbol, "42883");
    }

    private static String getOperandTypeName(Expression expr) {
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case INTEGER:
                    return "integer";
                case FLOAT:
                    return "numeric";
                case STRING:
                    return "text";
                case BOOLEAN:
                    return "boolean";
                default:
                    return "unknown";
            }
        }
        return "unknown";
    }

    // ---- XML syntax parsing ----

    Expression parseXmlparse() {
        ep.advance(); // consume XMLPARSE
        ep.expect(TokenType.LEFT_PAREN);
        String mode;
        if (ep.matchKeyword("DOCUMENT")) {
            mode = "document";
        } else {
            ep.expectKeyword("CONTENT");
            mode = "content";
        }
        Expression value = ep.parseExpression();
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("xmlparse", Cols.listOf(Literal.ofString(mode), value));
    }

    Expression parseXmlserialize() {
        ep.advance(); // consume XMLSERIALIZE
        ep.expect(TokenType.LEFT_PAREN);
        String mode;
        if (ep.matchKeyword("DOCUMENT")) {
            mode = "document";
        } else {
            ep.expectKeyword("CONTENT");
            mode = "content";
        }
        Expression value = ep.parseExpression();
        ep.expectKeyword("AS");
        String typeName = ep.parseTypeName();
        // Both halves of the clause are spelled out in SQL, and a serialisation that says NO
        // INDENT is saying something: it is the answer to the same question INDENT answers, so
        // refusing to read it turned a statement PostgreSQL accepts into a syntax error.
        boolean indent;
        if (ep.matchKeyword("NO") || ep.matchIdentifier("NO")) {
            if (!ep.matchKeyword("INDENT")) ep.matchIdentifier("INDENT");
            indent = false;
        } else {
            indent = ep.matchKeyword("INDENT") || ep.matchIdentifier("INDENT");
        }
        ep.expect(TokenType.RIGHT_PAREN);
        if (indent) {
            return new FunctionCallExpr("xmlserialize", Cols.listOf(Literal.ofString(mode), value, Literal.ofString(typeName), Literal.ofString("indent")));
        }
        return new FunctionCallExpr("xmlserialize", Cols.listOf(Literal.ofString(mode), value, Literal.ofString(typeName)));
    }

    Expression parseXmlelement() {
        ep.advance(); // consume XMLELEMENT
        ep.expect(TokenType.LEFT_PAREN);
        ep.expectKeyword("NAME");
        String tagName = ep.readIdentifier();
        List<Expression> args = new ArrayList<>();
        args.add(Literal.ofString(tagName));

        if (ep.match(TokenType.COMMA)) {
            if (ep.checkKeyword("XMLATTRIBUTES")) {
                ep.advance();
                ep.expect(TokenType.LEFT_PAREN);
                List<Expression> attrArgs = new ArrayList<>();
                do {
                    Expression val = ep.parseExpression();
                    String attrName;
                    if (ep.matchKeyword("AS")) {
                        attrName = ep.readIdentifier();
                    } else {
                        attrName = inferName(val);
                    }
                    attrArgs.add(val);
                    attrArgs.add(Literal.ofString(attrName));
                } while (ep.match(TokenType.COMMA));
                ep.expect(TokenType.RIGHT_PAREN);
                args.add(new FunctionCallExpr("__xmlattributes__", attrArgs));
                while (ep.match(TokenType.COMMA)) {
                    args.add(ep.parseExpression());
                }
            } else {
                args.add(Literal.ofNull());
                args.add(ep.parseExpression());
                while (ep.match(TokenType.COMMA)) {
                    args.add(ep.parseExpression());
                }
            }
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("xmlelement", args);
    }

    Expression parseXmlforest() {
        ep.advance(); // consume XMLFOREST
        ep.expect(TokenType.LEFT_PAREN);
        List<Expression> args = new ArrayList<>();
        do {
            Expression val = ep.parseExpression();
            String name;
            if (ep.matchKeyword("AS")) {
                name = ep.readIdentifier();
            } else {
                name = inferName(val);
            }
            args.add(val);
            args.add(Literal.ofString(name));
        } while (ep.match(TokenType.COMMA));
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("xmlforest", args);
    }

    Expression parseXmlpi() {
        ep.advance(); // consume XMLPI
        ep.expect(TokenType.LEFT_PAREN);
        ep.expectKeyword("NAME");
        String target = ep.readIdentifier();
        List<Expression> args = new ArrayList<>();
        args.add(Literal.ofString(target));
        if (ep.match(TokenType.COMMA)) {
            args.add(ep.parseExpression());
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("xmlpi", args);
    }

    Expression parseXmlroot() {
        ep.advance(); // consume XMLROOT
        ep.expect(TokenType.LEFT_PAREN);
        Expression xml = ep.parseExpression();
        ep.expect(TokenType.COMMA);
        ep.expectKeyword("VERSION");
        Expression version;
        if (ep.matchKeyword("NO")) {
            ep.expectKeyword("VALUE");
            version = Literal.ofString("no value");
        } else {
            version = ep.parseExpression();
        }
        Expression standalone = Literal.ofNull();
        if (ep.match(TokenType.COMMA)) {
            String standaloneTok = ep.readIdentifier();
            if (!standaloneTok.equalsIgnoreCase("STANDALONE")) {
                throw new ParseException("Expected STANDALONE", ep.peek());
            }
            String val = ep.readIdentifier();
            if (val.equalsIgnoreCase("YES")) {
                standalone = Literal.ofString("yes");
            } else if (val.equalsIgnoreCase("NO")) {
                if (ep.checkKeyword("VALUE") || (ep.check(TokenType.IDENTIFIER) && ep.peek().value().equalsIgnoreCase("VALUE"))) {
                    ep.advance();
                    standalone = Literal.ofString("no value");
                } else {
                    standalone = Literal.ofString("no");
                }
            }
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("xmlroot", Cols.listOf(xml, version, standalone));
    }

    Expression parseXmlexists() {
        ep.advance(); // consume XMLEXISTS
        ep.expect(TokenType.LEFT_PAREN);
        Expression xpath = ep.parseExpression();
        ep.expectKeyword("PASSING");
        if (ep.matchKeyword("BY")) {
            if (ep.check(TokenType.IDENTIFIER) || ep.check(TokenType.KEYWORD)) {
                String tok = ep.peek().value().toUpperCase(java.util.Locale.ROOT);
                if (tok.equals("REF") || tok.equals("VALUE")) {
                    ep.advance();
                }
            }
        }
        Expression xml = ep.parseExpression();
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("xmlexists", Cols.listOf(xpath, xml));
    }

    // ---- SQL/JSON standard functions (PG 16+) ----

    Expression parseJsonExists() {
        ep.advance(); // consume JSON_EXISTS
        ep.expect(TokenType.LEFT_PAREN);
        Expression input = ep.parseExpression();
        ep.expect(TokenType.COMMA);
        Expression path = ep.parseExpression();
        Map<String, Expression> passing = null;
        if (ep.matchKeyword("PASSING")) {
            passing = parsePassingVars();
        }
        JsonExistsExpr.OnBehavior[] on = new JsonExistsExpr.OnBehavior[2];
        Expression[] defaults = new Expression[2];
        parseOnClauses(on, defaults, JsonOnClauses.Answers.TRUTH, "JSON_EXISTS()");
        ep.expect(TokenType.RIGHT_PAREN);
        return new JsonExistsExpr(input, path, passing, on[1]);
    }

    Expression parseJsonValue() {
        ep.advance(); // consume JSON_VALUE
        ep.expect(TokenType.LEFT_PAREN);
        Expression input = ep.parseExpression();
        ep.expect(TokenType.COMMA);
        Expression path = ep.parseExpression();
        Map<String, Expression> passing = null;
        if (ep.matchKeyword("PASSING")) {
            passing = parsePassingVars();
        }
        String returningType = null;
        if (ep.matchKeyword("RETURNING")) {
            returningType = ep.parseTypeName();
        }
        JsonExistsExpr.OnBehavior[] on = new JsonExistsExpr.OnBehavior[2];
        Expression[] defaults = new Expression[2];
        parseOnClauses(on, defaults, JsonOnClauses.Answers.SCALAR, "JSON_VALUE()");
        ep.expect(TokenType.RIGHT_PAREN);
        return new JsonValueExpr(input, path, returningType, passing,
                on[0], defaults[0], on[1], defaults[1]);
    }

    Expression parseJsonQuery() {
        ep.advance(); // consume JSON_QUERY
        ep.expect(TokenType.LEFT_PAREN);
        Expression input = ep.parseExpression();
        ep.expect(TokenType.COMMA);
        Expression path = ep.parseExpression();
        Map<String, Expression> passing = null;
        if (ep.matchKeyword("PASSING")) {
            passing = parsePassingVars();
        }
        String returningType = null;
        if (ep.matchKeyword("RETURNING")) {
            returningType = ep.parseTypeName();
        }
        JsonQueryExpr.WrapperBehavior wrapper = JsonQueryExpr.WrapperBehavior.NONE;
        if (ep.matchKeyword("WITH")) {
            if (ep.matchKeyword("CONDITIONAL")) {
                ep.expectKeyword("WRAPPER");
                wrapper = JsonQueryExpr.WrapperBehavior.WITH_CONDITIONAL_WRAPPER;
            } else if (ep.matchKeyword("UNCONDITIONAL")) {
                ep.expectKeyword("WRAPPER");
                wrapper = JsonQueryExpr.WrapperBehavior.WITH_WRAPPER;
            } else {
                ep.expectKeyword("WRAPPER");
                wrapper = JsonQueryExpr.WrapperBehavior.WITH_WRAPPER;
            }
        } else if (ep.matchKeyword("WITHOUT")) {
            ep.expectKeyword("WRAPPER");
            wrapper = JsonQueryExpr.WrapperBehavior.WITHOUT_WRAPPER;
        }
        JsonQueryExpr.QuotesBehavior quotes = JsonQueryExpr.QuotesBehavior.KEEP;
        boolean quotesWritten = true;
        if (ep.matchKeyword("KEEP")) { ep.expectKeyword("QUOTES"); quotes = JsonQueryExpr.QuotesBehavior.KEEP; }
        else if (ep.matchKeyword("OMIT")) { ep.expectKeyword("QUOTES"); quotes = JsonQueryExpr.QuotesBehavior.OMIT; }
        else quotesWritten = false;
        // A wrapper already decides how the items are written, so saying anything about quotes on
        // top of it is a contradiction rather than a refinement, and PostgreSQL refuses it.
        if (quotesWritten && (wrapper == JsonQueryExpr.WrapperBehavior.WITH_WRAPPER
                || wrapper == JsonQueryExpr.WrapperBehavior.WITH_CONDITIONAL_WRAPPER)) {
            throw new MemgresException(
                    "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used",
                    "42601");
        }
        JsonExistsExpr.OnBehavior[] on = new JsonExistsExpr.OnBehavior[2];
        Expression[] defaults = new Expression[2];
        parseOnClauses(on, defaults, JsonOnClauses.Answers.DOCUMENT, "JSON_QUERY()");
        ep.expect(TokenType.RIGHT_PAREN);
        return new JsonQueryExpr(input, path, returningType, passing, wrapper, quotes,
                on[0], defaults[0], on[1], defaults[1]);
    }

    /**
     * The {@code ON EMPTY} and {@code ON ERROR} clauses of a SQL/JSON expression, read into slot
     * 0 (empty) and slot 1 (error) of the two arrays.
     *
     * <p>The two take the same set of answers, so which of them is being written is only known
     * once {@code ON} has been passed. Each may be written once and the empty one comes first:
     * they are a sequence rather than a set, because they say what to do at different moments.
     * A second clause of either kind ends the list as a syntax error at the word that could not
     * follow -- for one written after the ON ERROR, that is the caller's business.
     */
    private void parseOnClauses(JsonExistsExpr.OnBehavior[] on, Expression[] defaults,
                                JsonOnClauses.Answers answers, String whom) {
        boolean sawEmpty = false;
        while (true) {
            Token start = ep.peek();
            JsonExistsExpr.OnBehavior behavior;
            Expression defaultValue = null;
            if (ep.matchKeyword("NULL")) {
                behavior = JsonExistsExpr.OnBehavior.NULL_VAL;
            } else if (ep.matchKeyword("ERROR")) {
                behavior = JsonExistsExpr.OnBehavior.ERROR;
            } else if (ep.matchKeyword("TRUE")) {
                behavior = JsonExistsExpr.OnBehavior.TRUE_VAL;
            } else if (ep.matchKeyword("FALSE")) {
                behavior = JsonExistsExpr.OnBehavior.FALSE_VAL;
            } else if (ep.matchKeyword("UNKNOWN")) {
                behavior = JsonExistsExpr.OnBehavior.UNKNOWN_VAL;
            } else if (ep.matchKeyword("DEFAULT")) {
                behavior = null;
                defaultValue = ep.parseExpression();
            } else if (ep.checkKeyword("EMPTY")
                    && (ep.checkKeywordAt(1, "ARRAY") || ep.checkKeywordAt(1, "OBJECT"))) {
                ep.advance();
                behavior = ep.matchKeyword("ARRAY") ? JsonExistsExpr.OnBehavior.EMPTY_ARRAY
                        : JsonExistsExpr.OnBehavior.EMPTY_OBJECT;
                if (behavior == JsonExistsExpr.OnBehavior.EMPTY_OBJECT) ep.expectKeyword("OBJECT");
            } else {
                return;
            }
            ep.expectKeyword("ON");
            Token which = ep.peek();
            // A truth reading has an ON ERROR clause and no ON EMPTY one, so ON EMPTY is not an
            // answer it refuses but a word its grammar has no place for.
            int slot = !answers.truth() && ep.matchKeyword("EMPTY") ? 0 : 1;
            if (slot == 0 && sawEmpty) {
                throw ParseException.saying("syntax error at or near \"" + which.raw() + "\"",
                        which, "42601");
            }
            if (slot == 1) ep.expectKeyword("ERROR");
            JsonOnClauses.require(answers, whom, null, slot == 0, behavior, defaultValue != null,
                    start);
            on[slot] = behavior;
            defaults[slot] = defaultValue;
            if (slot == 1) return;
            sawEmpty = true;
        }
    }

    Expression parseJsonScalar() {
        ep.advance(); // consume JSON_SCALAR
        ep.expect(TokenType.LEFT_PAREN);
        Expression arg = ep.parseExpression();
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("json_scalar", Cols.listOf(arg));
    }

    Expression parseJsonSerialize() {
        ep.advance(); // consume JSON_SERIALIZE
        ep.expect(TokenType.LEFT_PAREN);
        Expression arg = ep.parseExpression();
        String returningType = null;
        if (ep.matchKeyword("RETURNING")) {
            returningType = ep.parseTypeName();
        }
        ep.expect(TokenType.RIGHT_PAREN);
        // normalize to function call with optional type hint
        if (returningType != null) {
            return new CastExpr(new FunctionCallExpr("json_serialize", Cols.listOf(arg)), returningType);
        }
        return new FunctionCallExpr("json_serialize", Cols.listOf(arg));
    }

    /**
     * The type an SQL/JSON constructor was told to answer with, applied to what it built.
     *
     * <p>The clause used to be read and dropped, so every constructor answered with the characters
     * it had assembled and a text column to hold them. The type settles both: what the column is,
     * and — jsonb being a document rather than the text it was written as — how it prints. Where
     * the clause is left out the type is json, which is what the standard says and not text.
     */
    private static Expression returning(Expression constructed, String type) {
        return new CastExpr(constructed, type == null ? "json" : type);
    }

    Expression parseJsonArray() {
        ep.advance(); // consume JSON_ARRAY
        ep.expect(TokenType.LEFT_PAREN);
        // Check for subquery: JSON_ARRAY(SELECT ...)
        if (ep.checkKeyword("SELECT") || ep.checkKeyword("WITH")) {
            Statement subquery = ep.parseSubqueryWithSetOps();
            String subqueryType = ep.matchKeyword("RETURNING") ? ep.parseTypeName() : null;
            ep.expect(TokenType.RIGHT_PAREN);
            // Wrap as json_array_subquery function call
            return returning(new FunctionCallExpr("json_array_subquery",
                    Cols.listOf(new SubqueryExpr(subquery))), subqueryType);
        }
        // Empty: JSON_ARRAY()
        if (ep.check(TokenType.RIGHT_PAREN)) {
            ep.advance();
            return returning(new FunctionCallExpr("json_array_constructor", Cols.listOf()), null);
        }
        List<Expression> args = new ArrayList<>();
        boolean nullOnNull = false;
        boolean absentOnNull = true; // default
        do {
            args.add(ep.parseExpression());
        } while (ep.match(TokenType.COMMA) && !isNullOnNullLookahead() && !ep.checkKeyword("ABSENT") && !ep.checkKeyword("RETURNING"));
        // NULL ON NULL / ABSENT ON NULL
        if (ep.checkKeyword("NULL") && ep.checkKeywordAt(1, "ON")) {
            ep.advance(); ep.advance(); ep.expectKeyword("NULL"); nullOnNull = true; absentOnNull = false;
        } else if (ep.matchKeyword("ABSENT")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); }
        // RETURNING type
        String type = ep.matchKeyword("RETURNING") ? ep.parseTypeName() : null;
        ep.expect(TokenType.RIGHT_PAREN);
        // Pack nullOnNull flag as an extra Literal arg
        args.add(Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        return returning(new FunctionCallExpr("json_array_constructor", args), type);
    }

    Expression parseJsonObject() {
        ep.advance(); // consume JSON_OBJECT
        ep.expect(TokenType.LEFT_PAREN);
        // Empty: JSON_OBJECT()
        if (ep.check(TokenType.RIGHT_PAREN)) {
            ep.advance();
            return returning(new FunctionCallExpr("json_object_constructor", Cols.listOf()), null);
        }
        List<Expression> args = new ArrayList<>();
        boolean nullOnNull = false;
        // PG 18 uses colon syntax: JSON_OBJECT('key' : value, ...)
        // Also supports VALUE keyword: JSON_OBJECT('key' VALUE value, ...)
        Expression first = ep.parseExpression();
        // The older json_object(text[]) and json_object(text[], text[]) are ordinary function
        // calls that happen to share the keyword's name; neither pairs its arguments with a colon.
        if (!ep.check(TokenType.COLON) && !ep.checkKeyword("VALUE")) {
            List<Expression> callArgs = new ArrayList<>();
            callArgs.add(first);
            while (ep.match(TokenType.COMMA)) callArgs.add(ep.parseExpression());
            ep.expect(TokenType.RIGHT_PAREN);
            return new FunctionCallExpr("json_object", callArgs);
        }
        boolean firstPair = true;
        do {
            Expression key;
            Expression val;
            key = firstPair ? first : ep.parseExpression();
            firstPair = false;
            if (!ep.match(TokenType.COLON)) {
                ep.expectKeyword("VALUE");
            }
            val = ep.parseExpression();
            args.add(key);
            args.add(val);
        } while (ep.match(TokenType.COMMA) && !isNullOnNullLookahead() && !ep.checkKeyword("ABSENT")
                && !ep.checkKeyword("WITH") && !ep.checkKeyword("WITHOUT") && !ep.checkKeyword("RETURNING"));
        // NULL ON NULL / ABSENT ON NULL
        if (ep.checkKeyword("NULL") && ep.checkKeywordAt(1, "ON")) {
            ep.advance(); ep.advance(); ep.expectKeyword("NULL"); nullOnNull = true;
        } else if (ep.matchKeyword("ABSENT")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); }
        // WITH UNIQUE KEYS / WITHOUT UNIQUE KEYS
        boolean uniqueKeys = false;
        if (ep.matchKeyword("WITH")) { ep.expectKeyword("UNIQUE"); ep.expectKeyword("KEYS"); uniqueKeys = true; }
        else if (ep.matchKeyword("WITHOUT")) { ep.expectKeyword("UNIQUE"); ep.expectKeyword("KEYS"); }
        // RETURNING type
        String type = ep.matchKeyword("RETURNING") ? ep.parseTypeName() : null;
        ep.expect(TokenType.RIGHT_PAREN);
        // Pack flags as extra args
        args.add(Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        args.add(Literal.ofString(uniqueKeys ? "unique_keys" : "no_unique_keys"));
        return returning(new FunctionCallExpr("json_object_constructor", args), type);
    }

    Expression parseJsonArrayagg() {
        ep.advance(); // consume JSON_ARRAYAGG
        ep.expect(TokenType.LEFT_PAREN);
        Expression arg = ep.parseExpression();
        List<SelectStmt.OrderByItem> orderBy = null;
        if (ep.matchKeywords("ORDER", "BY")) {
            orderBy = new ArrayList<>();
            do {
                Expression oExpr = ep.parseExpression();
                boolean desc = false;
                Boolean nullsFirst = null;
                if (ep.matchKeyword("DESC")) desc = true;
                else ep.matchKeyword("ASC");
                if (ep.matchKeywords("NULLS", "FIRST")) nullsFirst = true;
                else if (ep.matchKeywords("NULLS", "LAST")) nullsFirst = false;
                orderBy.add(new SelectStmt.OrderByItem(oExpr, desc, nullsFirst));
            } while (ep.match(TokenType.COMMA));
        }
        boolean nullOnNull = false;
        if (ep.matchKeyword("NULL")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); nullOnNull = true; }
        else if (ep.matchKeyword("ABSENT")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); }
        String type = ep.matchKeyword("RETURNING") ? ep.parseTypeName() : null;
        ep.expect(TokenType.RIGHT_PAREN);
        // Create as special aggregate function call
        List<Expression> args = Cols.listOf(arg,
                Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        return returning(new FunctionCallExpr("json_arrayagg", args, false, false, orderBy, null),
                type);
    }

    Expression parseJsonObjectagg() {
        ep.advance(); // consume JSON_OBJECTAGG
        ep.expect(TokenType.LEFT_PAREN);
        Expression key;
        Expression val;
        if (ep.matchKeyword("KEY")) {
            key = ep.parseExpression();
            ep.expectKeyword("VALUE");
            val = ep.parseExpression();
        } else {
            key = ep.parseExpression();
            if (ep.checkKeyword("VALUE")) {
                ep.advance(); // consume VALUE
            } else {
                ep.expect(TokenType.COLON);
            }
            val = ep.parseExpression();
        }
        // A member whose value is null is still a member, so JSON_OBJECTAGG keeps it unless told
        // otherwise -- the default here is NULL ON NULL, the opposite of the array construct's,
        // where an absent element would shift every element after it.
        boolean nullOnNull = true;
        if (ep.matchKeyword("NULL")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); }
        else if (ep.matchKeyword("ABSENT")) {
            ep.expectKeyword("ON");
            ep.expectKeyword("NULL");
            nullOnNull = false;
        }
        boolean uniqueKeys = false;
        if (ep.matchKeyword("WITH")) { ep.expectKeyword("UNIQUE"); ep.expectKeyword("KEYS"); uniqueKeys = true; }
        else if (ep.matchKeyword("WITHOUT")) { ep.expectKeyword("UNIQUE"); ep.expectKeyword("KEYS"); }
        String type = ep.matchKeyword("RETURNING") ? ep.parseTypeName() : null;
        ep.expect(TokenType.RIGHT_PAREN);
        List<Expression> args = new ArrayList<>();
        args.add(key);
        args.add(val);
        args.add(Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        args.add(Literal.ofString(uniqueKeys ? "unique_keys" : "no_unique_keys"));
        return returning(new FunctionCallExpr("json_objectagg", args, false, false, null, null),
                type);
    }

    /** Check if current position is NULL ON (i.e., NULL ON NULL clause, not a null value) */
    private boolean isNullOnNullLookahead() {
        return ep.checkKeyword("NULL") && ep.checkKeywordAt(1, "ON");
    }

    private Map<String, Expression> parsePassingVars() {
        Map<String, Expression> passing = new LinkedHashMap<>();
        do {
            Expression val = ep.parseExpression();
            ep.expectKeyword("AS");
            String name = ep.readIdentifier();
            passing.put(name.toLowerCase(java.util.Locale.ROOT), val);
        } while (ep.match(TokenType.COMMA) && !ep.checkKeyword("RETURNING") && !ep.checkKeyword("TRUE")
                && !ep.checkKeyword("FALSE") && !ep.checkKeyword("ERROR") && !ep.checkKeyword("UNKNOWN")
                && !ep.check(TokenType.RIGHT_PAREN));
        return passing;
    }

    static String inferName(Expression expr) {
        if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            return cr.column();
        }
        return "?column?";
    }

    /** A call the grammar spelled out, which PostgreSQL names schema-qualified when it refuses it. */
    private static FunctionCallExpr grammarCall(String name, List<Expression> args) {
        FunctionCallExpr call = new FunctionCallExpr(name, args);
        call.spelledInGrammar = true;
        return call;
    }
}
