package com.memgres.engine.parser;

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
            operand = ep.parseExpression();
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
            Statement subquery = ep.parseSubqueryWithSetOps();
            ep.expect(TokenType.RIGHT_PAREN);
            return new ArraySubqueryExpr(subquery);
        }
        Expression arr = parseArrayBracket();
        if (arr instanceof ArrayExpr && ((ArrayExpr) arr).elements().isEmpty() && !ep.check(TokenType.CAST)) {
            ArrayExpr ae = (ArrayExpr) arr;
            throw new ParseException("cannot determine type of empty array", ep.peek(), "42P18");
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

    Expression parseBuiltinFunction() {
        String name = ep.advance().value().toLowerCase();
        ep.expect(TokenType.LEFT_PAREN);
        List<Expression> args = new ArrayList<>();
        if (!ep.check(TokenType.RIGHT_PAREN)) {
            args = ep.parseExpressionList();
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
            String nextVal = ep.tokens.get(ep.pos + 1).value().toUpperCase();
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
        if (ep.check(TokenType.STRING_LITERAL)) {
            String val = ep.advance().value();
            return new CastExpr(Literal.ofString(val), "interval");
        }
        return new CastExpr(ep.parsePrimary(), "interval");
    }

    Expression parseTimestamp() {
        ep.advance(); // consume TIMESTAMP
        String tsType = "timestamp";
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
            return new CastExpr(Literal.ofString(val), tsType);
        }
        return new ColumnRef("timestamp");
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
            return new FunctionCallExpr("substring", args);
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
            return new FunctionCallExpr("substring", args);
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
        Expression substring = ep.parsePrimary();
        ep.expectKeyword("IN");
        Expression string = ep.parseExpression();
        ep.expect(TokenType.RIGHT_PAREN);
        return new FunctionCallExpr("position", Cols.listOf(substring, string));
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
        return new FunctionCallExpr("overlay", args);
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
        return new FunctionCallExpr("extract", Cols.listOf(Literal.ofString(field), source));
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
            return new FunctionCallExpr(funcName, Cols.listOf(stringExpr, charsExpr));
        } else {
            return new FunctionCallExpr(funcName, Cols.listOf(stringExpr));
        }
    }

    // ---- Window function parsing ----

    WindowFuncExpr parseWindowFunction(String name, List<Expression> args, boolean distinct, boolean star) {
        ep.expectKeyword("OVER");

        if (!ep.check(TokenType.LEFT_PAREN)) {
            String windowName = ep.readIdentifier();
            return new WindowFuncExpr(name, args, distinct, star, null, null, null, windowName);
        }

        ep.expect(TokenType.LEFT_PAREN);

        List<Expression> partitionBy = null;
        List<SelectStmt.OrderByItem> orderBy = null;
        WindowFuncExpr.FrameClause frame = null;

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
        return new WindowFuncExpr(name, args, distinct, star, partitionBy, orderBy, frame);
    }

    WindowFuncExpr.FrameClause parseWindowFrame() {
        WindowFuncExpr.FrameType frameType;
        if (ep.matchKeyword("ROWS")) frameType = WindowFuncExpr.FrameType.ROWS;
        else if (ep.matchKeyword("RANGE")) frameType = WindowFuncExpr.FrameType.RANGE;
        else { ep.expectKeyword("GROUPS"); frameType = WindowFuncExpr.FrameType.GROUPS; }

        if (ep.matchKeyword("BETWEEN")) {
            WindowFuncExpr.FrameBound start = parseFrameBound();
            ep.expectKeyword("AND");
            WindowFuncExpr.FrameBound end = parseFrameBound();
            return new WindowFuncExpr.FrameClause(frameType, start, end);
        } else {
            WindowFuncExpr.FrameBound start = parseFrameBound();
            WindowFuncExpr.FrameBound end = new WindowFuncExpr.FrameBound(
                    WindowFuncExpr.FrameBoundType.CURRENT_ROW, null);
            return new WindowFuncExpr.FrameClause(frameType, start, end);
        }
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
        Expression offset = ep.parsePrimary();
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
        String opSymbol = opTok.value();
        ep.advance();
        StringBuilder opBuilder = new StringBuilder(opSymbol);
        while (!ep.check(TokenType.RIGHT_PAREN) && !ep.isAtEnd()) {
            opBuilder.append(ep.peek().value());
            ep.advance();
        }
        opSymbol = opBuilder.toString().trim();
        ep.expect(TokenType.RIGHT_PAREN);
        if (schema != null && !schema.equalsIgnoreCase("pg_catalog") && !schema.equalsIgnoreCase("public")) {
            throw new com.memgres.engine.MemgresException("schema \"" + schema + "\" does not exist", "3F000");
        }
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
            default:
                throw new com.memgres.engine.MemgresException(
                "operator does not exist: " + spec.opSymbol, "42883");
        }

        Expression right = ep.parseAddition();
        return new BinaryExpr(left, binOp, right);
    }

    /** Parse OPERATOR(schema.op)(arg) in prefix/function-call position. */
    Expression parsePrefixQualifiedOperator() {
        ep.advance(); // consume OPERATOR keyword
        OperatorSpec spec = readOperatorSpec();

        ep.expect(TokenType.LEFT_PAREN);
        List<Expression> args = ep.parseExpressionList();
        ep.expect(TokenType.RIGHT_PAREN);

        if (args.size() >= 2) {
            throw new com.memgres.engine.MemgresException(
                "operator does not exist: " + (spec.schema != null ? spec.schema + "." : "") + spec.opSymbol + " record", "42883");
        }

        if (args.size() == 1) {
            if ("+".equals(spec.opSymbol)) {
                return new QualifiedOperatorExpr(spec.schema, spec.opSymbol,
                    new UnaryExpr(UnaryExpr.UnaryOp.POSITIVE, args.get(0)));
            } else if ("-".equals(spec.opSymbol)) {
                return new QualifiedOperatorExpr(spec.schema, spec.opSymbol,
                    new UnaryExpr(UnaryExpr.UnaryOp.NEGATE, args.get(0)));
            }
            throw new com.memgres.engine.MemgresException(
                "operator does not exist: " + (spec.schema != null ? spec.schema + "." : "") + spec.opSymbol + " " + getOperandTypeName(args.get(0)), "42883");
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
        ep.expect(TokenType.RIGHT_PAREN);
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
                String tok = ep.peek().value().toUpperCase();
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
        JsonExistsExpr.OnBehavior onError = null;
        if (ep.matchKeyword("TRUE")) { ep.expectKeyword("ON"); ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.TRUE_VAL; }
        else if (ep.matchKeyword("FALSE")) { ep.expectKeyword("ON"); ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.FALSE_VAL; }
        else if (ep.matchKeyword("UNKNOWN")) { ep.expectKeyword("ON"); ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.UNKNOWN_VAL; }
        else if (ep.matchKeyword("ERROR")) { ep.expectKeyword("ON"); ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.ERROR; }
        ep.expect(TokenType.RIGHT_PAREN);
        return new JsonExistsExpr(input, path, passing, onError);
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
        JsonExistsExpr.OnBehavior onEmpty = null;
        Expression defaultOnEmpty = null;
        JsonExistsExpr.OnBehavior onError = null;
        Expression defaultOnError = null;
        // Parse ON EMPTY and ON ERROR clauses (can appear in any order, at most once each)
        for (int i = 0; i < 2; i++) {
            if (ep.checkKeyword("DEFAULT")) {
                ep.advance();
                Expression defVal = ep.parseExpression();
                ep.expectKeyword("ON");
                if (ep.matchKeyword("EMPTY")) { onEmpty = null; defaultOnEmpty = defVal; }
                else { ep.expectKeyword("ERROR"); onError = null; defaultOnError = defVal; }
            } else if (ep.checkKeyword("NULL")) {
                ep.advance();
                ep.expectKeyword("ON");
                if (ep.matchKeyword("EMPTY")) { onEmpty = JsonExistsExpr.OnBehavior.NULL_VAL; }
                else { ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.NULL_VAL; }
            } else if (ep.checkKeyword("ERROR")) {
                ep.advance();
                ep.expectKeyword("ON");
                if (ep.matchKeyword("EMPTY")) { onEmpty = JsonExistsExpr.OnBehavior.ERROR; }
                else { ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.ERROR; }
            } else {
                break;
            }
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new JsonValueExpr(input, path, returningType, passing, onEmpty, defaultOnEmpty, onError, defaultOnError);
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
            wrapper = JsonQueryExpr.WrapperBehavior.NONE;
        }
        JsonQueryExpr.QuotesBehavior quotes = JsonQueryExpr.QuotesBehavior.KEEP;
        if (ep.matchKeyword("KEEP")) { ep.expectKeyword("QUOTES"); quotes = JsonQueryExpr.QuotesBehavior.KEEP; }
        else if (ep.matchKeyword("OMIT")) { ep.expectKeyword("QUOTES"); quotes = JsonQueryExpr.QuotesBehavior.OMIT; }
        JsonExistsExpr.OnBehavior onEmpty = null;
        JsonExistsExpr.OnBehavior onError = null;
        for (int i = 0; i < 2; i++) {
            if (ep.checkKeyword("NULL")) {
                ep.advance(); ep.expectKeyword("ON");
                if (ep.matchKeyword("EMPTY")) onEmpty = JsonExistsExpr.OnBehavior.NULL_VAL;
                else { ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.NULL_VAL; }
            } else if (ep.checkKeyword("ERROR")) {
                ep.advance(); ep.expectKeyword("ON");
                if (ep.matchKeyword("EMPTY")) onEmpty = JsonExistsExpr.OnBehavior.ERROR;
                else { ep.expectKeyword("ERROR"); onError = JsonExistsExpr.OnBehavior.ERROR; }
            } else if (ep.checkKeyword("EMPTY")) {
                ep.advance();
                if (ep.matchKeyword("ARRAY")) { ep.expectKeyword("ON"); ep.expectKeyword("EMPTY"); onEmpty = JsonExistsExpr.OnBehavior.EMPTY_ARRAY; }
                else if (ep.matchKeyword("OBJECT")) { ep.expectKeyword("ON"); ep.expectKeyword("EMPTY"); onEmpty = JsonExistsExpr.OnBehavior.EMPTY_OBJECT; }
            } else {
                break;
            }
        }
        ep.expect(TokenType.RIGHT_PAREN);
        return new JsonQueryExpr(input, path, returningType, passing, wrapper, quotes, onEmpty, onError);
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

    Expression parseJsonArray() {
        ep.advance(); // consume JSON_ARRAY
        ep.expect(TokenType.LEFT_PAREN);
        // Check for subquery: JSON_ARRAY(SELECT ...)
        if (ep.checkKeyword("SELECT") || ep.checkKeyword("WITH")) {
            Statement subquery = ep.parseSubqueryWithSetOps();
            ep.expect(TokenType.RIGHT_PAREN);
            // Wrap as json_array_subquery function call
            return new FunctionCallExpr("json_array_subquery", Cols.listOf(new SubqueryExpr(subquery)));
        }
        // Empty: JSON_ARRAY()
        if (ep.check(TokenType.RIGHT_PAREN)) {
            ep.advance();
            return new FunctionCallExpr("json_array_constructor", Cols.listOf());
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
        if (ep.matchKeyword("RETURNING")) { ep.parseTypeName(); } // consume but ignore for now
        ep.expect(TokenType.RIGHT_PAREN);
        // Pack nullOnNull flag as an extra Literal arg
        args.add(Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        return new FunctionCallExpr("json_array_constructor", args);
    }

    Expression parseJsonObject() {
        ep.advance(); // consume JSON_OBJECT
        ep.expect(TokenType.LEFT_PAREN);
        // Empty: JSON_OBJECT()
        if (ep.check(TokenType.RIGHT_PAREN)) {
            ep.advance();
            return new FunctionCallExpr("json_object_constructor", Cols.listOf());
        }
        List<Expression> args = new ArrayList<>();
        boolean nullOnNull = false;
        boolean keyValueSyntax = ep.checkKeyword("KEY");
        do {
            Expression key;
            Expression val;
            if (ep.matchKeyword("KEY")) {
                key = ep.parseExpression();
                ep.expectKeyword("VALUE");
                val = ep.parseExpression();
            } else {
                key = ep.parseExpression();
                ep.expect(TokenType.COLON);
                val = ep.parseExpression();
            }
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
        if (ep.matchKeyword("RETURNING")) { ep.parseTypeName(); }
        ep.expect(TokenType.RIGHT_PAREN);
        // Pack flags as extra args
        args.add(Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        args.add(Literal.ofString(uniqueKeys ? "unique_keys" : "no_unique_keys"));
        return new FunctionCallExpr("json_object_constructor", args);
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
        if (ep.matchKeyword("RETURNING")) { ep.parseTypeName(); }
        ep.expect(TokenType.RIGHT_PAREN);
        // Create as special aggregate function call
        List<Expression> args = Cols.listOf(arg,
                Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        return new FunctionCallExpr("json_arrayagg", args, false, false, orderBy, null);
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
            ep.expect(TokenType.COLON);
            val = ep.parseExpression();
        }
        boolean nullOnNull = false;
        if (ep.matchKeyword("NULL")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); nullOnNull = true; }
        else if (ep.matchKeyword("ABSENT")) { ep.expectKeyword("ON"); ep.expectKeyword("NULL"); }
        boolean uniqueKeys = false;
        if (ep.matchKeyword("WITH")) { ep.expectKeyword("UNIQUE"); ep.expectKeyword("KEYS"); uniqueKeys = true; }
        else if (ep.matchKeyword("WITHOUT")) { ep.expectKeyword("UNIQUE"); ep.expectKeyword("KEYS"); }
        if (ep.matchKeyword("RETURNING")) { ep.parseTypeName(); }
        ep.expect(TokenType.RIGHT_PAREN);
        List<Expression> args = new ArrayList<>();
        args.add(key);
        args.add(val);
        args.add(Literal.ofString(nullOnNull ? "null_on_null" : "absent_on_null"));
        args.add(Literal.ofString(uniqueKeys ? "unique_keys" : "no_unique_keys"));
        return new FunctionCallExpr("json_objectagg", args, false, false, null, null);
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
            passing.put(name.toLowerCase(), val);
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
}
