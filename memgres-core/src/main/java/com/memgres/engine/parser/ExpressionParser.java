package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive-descent expression parser. Contains token navigation utilities and all expression
 * parsing methods. Parser extends this class and adds statement parsing.
 * Delegates special SQL syntax forms to ExprSpecialFormParser.
 */
public class ExpressionParser {

    protected final List<Token> tokens;
    protected int pos;
    private final ExprSpecialFormParser specialFormParser;

    protected ExpressionParser(List<Token> tokens) {
        this.tokens = tokens;
        this.pos = 0;
        this.specialFormParser = new ExprSpecialFormParser(this);
    }

    // ---- Token navigation ----

    protected Token peek() {
        return tokens.get(pos);
    }

    protected Token advance() {
        Token t = tokens.get(pos);
        pos++;
        return t;
    }

    protected boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    protected void expectEnd() {
        if (!isAtEnd()) {
            Token t = peek();
            throw new ParseException("Unexpected token after statement: " + t.value(), t);
        }
    }

    protected boolean check(TokenType type) {
        return peek().type() == type;
    }

    protected int position() {
        return pos;
    }

    protected void resetPosition(int saved) {
        pos = saved;
    }

    protected boolean checkKeywordAt(int offset, String keyword) {
        int idx = pos + offset;
        if (idx >= tokens.size()) return false;
        Token t = tokens.get(idx);
        return t.type() == TokenType.KEYWORD && t.value().equalsIgnoreCase(keyword);
    }

    protected boolean checkKeyword(String keyword) {
        Token t = peek();
        return t.type() == TokenType.KEYWORD && t.value().equalsIgnoreCase(keyword);
    }

    protected boolean matchKeyword(String keyword) {
        if (checkKeyword(keyword)) {
            advance();
            return true;
        }
        return false;
    }

    /**
     * Returns true if the current token is a SQL clause keyword (FROM, WHERE, GROUP, ORDER, etc.)
     * that should not be consumed as an identifier in contexts like bare COLLATE.
     */
    protected boolean isClauseKeyword() {
        return checkKeyword("FROM") || checkKeyword("WHERE") || checkKeyword("GROUP")
                || checkKeyword("ORDER") || checkKeyword("LIMIT") || checkKeyword("OFFSET")
                || checkKeyword("HAVING") || checkKeyword("UNION") || checkKeyword("INTERSECT")
                || checkKeyword("EXCEPT") || checkKeyword("FETCH") || checkKeyword("FOR")
                || checkKeyword("INTO") || checkKeyword("ON") || checkKeyword("RETURNING")
                || checkKeyword("WINDOW");
    }

    protected boolean checkIdentifier(String name) {
        Token t = peek();
        return t.type() == TokenType.IDENTIFIER && t.value().equalsIgnoreCase(name);
    }

    protected boolean matchIdentifier(String name) {
        if (checkIdentifier(name)) {
            advance();
            return true;
        }
        return false;
    }

    protected boolean matchKeywords(String... keywords) {
        int saved = pos;
        for (String kw : keywords) {
            if (!matchKeyword(kw)) {
                pos = saved;
                return false;
            }
        }
        return true;
    }

    protected boolean match(TokenType type) {
        if (check(type)) {
            advance();
            return true;
        }
        return false;
    }

    protected Token expect(TokenType type) {
        if (check(type)) {
            return advance();
        }
        throw new ParseException("Expected " + type + " but found " + peek().type(), peek());
    }

    protected void expectKeyword(String keyword) {
        if (!matchKeyword(keyword)) {
            throw new ParseException("Expected keyword " + keyword, peek());
        }
    }

    protected void skipSemicolons() {
        while (check(TokenType.SEMICOLON)) advance();
    }

    /** Reject boolean connectives (AND, OR) appearing where an expression is expected. */
    private void checkNotBooleanConnective() {
        if (checkKeyword("AND") || checkKeyword("OR")) {
            throw new ParseException("syntax error at or near \"" + peek().value() + "\"", peek());
        }
    }

    protected String readIdentifier() {
        Token t = peek();
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER) {
            if (t.type() == TokenType.QUOTED_IDENTIFIER && t.value().isEmpty()) {
                throw new ParseException("zero-length delimited identifier", t);
            }
            advance();
            return t.value();
        }
        // Allow keywords as identifiers in many contexts
        if (t.type() == TokenType.KEYWORD) {
            advance();
            return t.value().toLowerCase();
        }
        // Allow single-quoted strings as identifiers (PG accepts 'name' in SET ROLE, CREATE ROLE, etc.)
        if (t.type() == TokenType.STRING_LITERAL) {
            advance();
            return t.value();
        }
        throw new ParseException("Expected identifier", t);
    }

    protected String readIdentifierOrString() {
        Token t = peek();
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER) {
            advance();
            return t.value();
        }
        if (t.type() == TokenType.STRING_LITERAL) {
            advance();
            return t.value();
        }
        if (t.type() == TokenType.KEYWORD) {
            advance();
            return t.value().toLowerCase();
        }
        throw new ParseException("Expected identifier or string", t);
    }

    // ---- Type name parsing ----

    protected String parseTypeName() {
        StringBuilder sb = new StringBuilder();
        String name = readIdentifier();
        // Handle schema-qualified types: schema.typename (strip schema prefix)
        if (check(TokenType.DOT)) {
            advance();
            name = readIdentifier();
        }
        sb.append(name);

        // Handle multi-word types: DOUBLE PRECISION, CHARACTER VARYING, TIMESTAMP WITH/WITHOUT TIME ZONE
        if (name.equalsIgnoreCase("DOUBLE") && checkKeyword("PRECISION")) {
            sb.append(" ").append(advance().value());
        } else if (name.equalsIgnoreCase("CHARACTER") && checkKeyword("VARYING")) {
            sb.append(" ").append(advance().value());
        } else if (name.equalsIgnoreCase("TIMESTAMP") || name.equalsIgnoreCase("TIME")) {
            // Handle optional precision: TIMESTAMP(6) or TIME(3)
            if (check(TokenType.LEFT_PAREN)) {
                advance();
                sb.append("(");
                sb.append(advance().value());
                expect(TokenType.RIGHT_PAREN);
                sb.append(")");
            }
            // Handle WITH/WITHOUT TIME ZONE after optional precision
            if (checkKeyword("WITH") || checkKeyword("WITHOUT")) {
                sb.append(" ").append(advance().value()); // WITH/WITHOUT
                if (matchKeyword("TIME")) sb.append(" TIME");
                if (matchKeyword("ZONE")) sb.append(" ZONE");
            }
        } else if (name.equalsIgnoreCase("BIT") && checkKeyword("VARYING")) {
            sb.append(" ").append(advance().value());
        }

        // Handle precision: (N) or (N,M), for types not already handled above
        if (check(TokenType.LEFT_PAREN)) {
            advance();
            sb.append("(");
            sb.append(advance().value()); // first number
            if (match(TokenType.COMMA)) {
                sb.append(",");
                sb.append(advance().value()); // second number
            }
            expect(TokenType.RIGHT_PAREN);
            sb.append(")");
        }

        // Handle array notation: [], [][], etc.
        while (check(TokenType.LEFT_BRACKET)) {
            advance();
            expect(TokenType.RIGHT_BRACKET);
            sb.append("[]");
        }

        return sb.toString();
    }

    // ---- Order by parsing (used by both expression and statement parsing) ----

    protected List<SelectStmt.OrderByItem> parseOrderByList() {
        List<SelectStmt.OrderByItem> items = new ArrayList<>();
        do {
            Expression expr = parseExpression();
            boolean desc = false;
            Boolean nullsFirst = null;

            if (matchKeyword("ASC")) { desc = false; }
            else if (matchKeyword("DESC")) { desc = true; }

            if (matchKeyword("NULLS")) {
                if (matchKeyword("FIRST")) { nullsFirst = true; }
                else { expectKeyword("LAST"); nullsFirst = false; }
            }

            items.add(new SelectStmt.OrderByItem(expr, desc, nullsFirst));
        } while (match(TokenType.COMMA));
        return items;
    }

    protected List<SelectStmt.OrderByItem> parseOrderByClause() {
        expectKeyword("ORDER");
        expectKeyword("BY");
        return parseOrderByList();
    }

    // ---- Expression list and identifier list ----

    protected List<Expression> parseExpressionList() {
        List<Expression> list = new ArrayList<>();
        do {
            list.add(parseExpression());
        } while (match(TokenType.COMMA));
        return list;
    }

    /**
     * Parse function argument list, supporting named args (name => expr).
     */
    protected List<Expression> parseFunctionArgList() {
        List<Expression> list = new ArrayList<>();
        do {
            // Check for VARIADIC keyword
            if (checkKeyword("VARIADIC")) {
                advance(); // consume VARIADIC
                Expression varExpr = parseExpression();
                // VARIADIC ARRAY[...], expand at call site
                // Wrap as a NamedArgExpr with special name "__variadic__"
                list.add(new NamedArgExpr("__variadic__", varExpr));
                continue;
            }
            // Check for named arg: identifier => expr  or  identifier := expr
            int saved = position();
            if ((peek().type() == TokenType.IDENTIFIER || peek().type() == TokenType.KEYWORD)
                    && pos + 1 < tokens.size()
                    && (tokens.get(pos + 1).type() == TokenType.FAT_ARROW || tokens.get(pos + 1).type() == TokenType.COLON_EQUALS)) {
                // Reject DEFAULT as a named arg name; PG says "DEFAULT is not allowed in this context"
                if (peek().value().equalsIgnoreCase("DEFAULT")
                        && tokens.get(pos + 1).type() == TokenType.FAT_ARROW) {
                    throw new ParseException("DEFAULT is not allowed in this context", peek());
                }
                String argName = readIdentifier();
                advance(); // consume => or :=
                Expression value = parseExpression();
                list.add(new NamedArgExpr(argName.toLowerCase(), value));
            } else {
                resetPosition(saved);
                list.add(parseExpression());
            }
        } while (match(TokenType.COMMA));
        return list;
    }

    protected List<String> parseIdentifierList() {
        List<String> list = new ArrayList<>();
        do {
            list.add(readIdentifier());
        } while (match(TokenType.COMMA));
        return list;
    }

    // ---- Subquery hook (overridden by Parser) ----

    /**
     * Parse a SELECT statement. This is overridden by Parser to provide the real implementation.
     * ExpressionParser needs this for subqueries in expressions (e.g., EXISTS, IN, scalar subqueries).
     */
    protected SelectStmt parseSelect() {
        throw new ParseException("Subquery not supported in this context", peek());
    }

    /**
     * Parse a subquery that may include UNION/INTERSECT/EXCEPT.
     * Returns the parsed Statement (SelectStmt or SetOpStmt).
     * Override in Parser to provide set-operation support.
     */
    protected Statement parseSubqueryWithSetOps() {
        return parseSelect();
    }

    // ---- Expression parsing (Pratt parser / precedence climbing) ----

    public Expression parseExpression() {
        return parseOr();
    }

    private Expression parseOr() {
        Expression left = parseAnd();
        while (matchKeyword("OR")) {
            Expression right = parseAnd();
            left = new BinaryExpr(left, BinaryExpr.BinOp.OR, right);
        }
        return left;
    }

    private Expression parseAnd() {
        Expression left = parseNot();
        while (matchKeyword("AND")) {
            Expression right = parseNot();
            left = new BinaryExpr(left, BinaryExpr.BinOp.AND, right);
        }
        return left;
    }

    private Expression parseNot() {
        if (matchKeyword("NOT")) {
            return new UnaryExpr(UnaryExpr.UnaryOp.NOT, parseNot());
        }
        return parseComparison();
    }

    private Expression parseComparison() {
        Expression left = parseAddition();

        // IS [NOT] NULL
        if (checkKeyword("IS")) {
            advance();
            boolean negated = matchKeyword("NOT");
            if (matchKeyword("NULL")) {
                return new IsNullExpr(left, negated);
            }
            // IS [NOT] DISTINCT FROM: NULL-safe comparison
            if (matchKeywords("DISTINCT", "FROM")) {
                Expression right = parseAddition();
                return negated
                        ? new BinaryExpr(left, BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM, right)
                        : new BinaryExpr(left, BinaryExpr.BinOp.IS_DISTINCT_FROM, right);
            }
            // IS [NOT] TRUE / FALSE / UNKNOWN
            if (matchKeyword("TRUE")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_TRUE : IsBooleanExpr.BooleanTest.IS_TRUE);
            }
            if (matchKeyword("FALSE")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_FALSE : IsBooleanExpr.BooleanTest.IS_FALSE);
            }
            if (matchKeyword("UNKNOWN")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_UNKNOWN : IsBooleanExpr.BooleanTest.IS_UNKNOWN);
            }
            // IS [NOT] DOCUMENT: XML document test
            if (matchKeyword("DOCUMENT")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_DOCUMENT : IsBooleanExpr.BooleanTest.IS_DOCUMENT);
            }
            // IS [NOT] JSON [VALUE | OBJECT | ARRAY | SCALAR] [WITH UNIQUE KEYS]
            if (matchKeyword("JSON")) {
                IsJsonExpr.JsonType jt = null;
                if (matchKeyword("OBJECT")) jt = IsJsonExpr.JsonType.OBJECT;
                else if (matchKeyword("ARRAY")) jt = IsJsonExpr.JsonType.ARRAY;
                else if (matchKeyword("SCALAR")) jt = IsJsonExpr.JsonType.SCALAR;
                else if (matchKeyword("VALUE")) jt = IsJsonExpr.JsonType.VALUE;
                boolean uniqueKeys = false;
                if (matchKeywords("WITH", "UNIQUE")) {
                    expectKeyword("KEYS");
                    uniqueKeys = true;
                }
                return new IsJsonExpr(left, negated, jt, uniqueKeys);
            }
        }

        // [NOT] IN (...)
        boolean negated = false;
        if (checkKeyword("NOT")) {
            int saved = pos;
            advance();
            if (checkKeyword("IN") || checkKeyword("BETWEEN") || checkKeyword("LIKE") || checkKeyword("ILIKE") || checkKeyword("SIMILAR")) {
                negated = true;
            } else {
                pos = saved;
            }
        }

        if (matchKeyword("IN")) {
            expect(TokenType.LEFT_PAREN);
            // Check for subquery: IN (SELECT ... [UNION/INTERSECT/EXCEPT ...])
            if (checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                Statement subquery = parseSubqueryWithSetOps();
                expect(TokenType.RIGHT_PAREN);
                return new InExpr(left, Cols.listOf(new SubqueryExpr(subquery)), negated);
            }
            List<Expression> values = parseExpressionList();
            expect(TokenType.RIGHT_PAREN);
            return new InExpr(left, values, negated);
        }

        // [NOT] BETWEEN [SYMMETRIC] low AND high
        if (matchKeyword("BETWEEN")) {
            boolean symmetric = matchKeyword("SYMMETRIC");
            Expression low = parseAddition();
            expectKeyword("AND");
            Expression high = parseAddition();
            return new BetweenExpr(left, low, high, negated, symmetric);
        }

        // [NOT] LIKE / ILIKE
        if (matchKeyword("LIKE")) {
            Expression right = parseAddition();
            if (matchKeyword("ESCAPE")) {
                String esc = advance().value(); // string literal
                return new LikeExpr(left, right, esc, false, negated);
            }
            Expression result = new BinaryExpr(left, BinaryExpr.BinOp.LIKE, right);
            return negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
        }
        if (matchKeyword("ILIKE")) {
            Expression right = parseAddition();
            if (matchKeyword("ESCAPE")) {
                String esc = advance().value(); // string literal
                return new LikeExpr(left, right, esc, true, negated);
            }
            Expression result = new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, right);
            return negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
        }
        if (matchKeywords("SIMILAR", "TO")) {
            Expression right = parseAddition();
            Expression result = new BinaryExpr(left, BinaryExpr.BinOp.SIMILAR_TO, right);
            return negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
        }

        // Comparison operators
        if (match(TokenType.EQUALS)) {
            // Check for = ANY(...) or = ALL(...)
            if (checkKeyword("ANY") || checkKeyword("ALL")) {
                boolean isAll = checkKeyword("ALL");
                advance(); // consume ANY/ALL
                expect(TokenType.LEFT_PAREN);
                if (checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                    Statement subquery = parseSubqueryWithSetOps();
                    expect(TokenType.RIGHT_PAREN);
                    return new AnyAllExpr(left, BinaryExpr.BinOp.EQUAL, subquery, isAll);
                }
                Expression arrayExpr = parseExpression();
                expect(TokenType.RIGHT_PAREN);
                if (!isAll && arrayExpr instanceof ArrayExpr) {
                    ArrayExpr arr = (ArrayExpr) arrayExpr;
                    return new InExpr(left, arr.elements(), false);
                }
                // ANY/ALL with array expression: evaluate element by element
                if (!isAll) {
                    // = ANY(array_expr) → treated as IN with the array elements
                    return new InExpr(left, Cols.listOf(arrayExpr), false, true);
                }
                // = ALL(array_expr) → all elements must satisfy the comparison
                return new AnyAllArrayExpr(left, BinaryExpr.BinOp.EQUAL, arrayExpr, true);
            }
            checkNotBooleanConnective(); // val = AND → syntax error
            return new BinaryExpr(left, BinaryExpr.BinOp.EQUAL, parseAddition());
        }
        if (match(TokenType.NOT_EQUALS)) return parseComparisonRhs(left, BinaryExpr.BinOp.NOT_EQUAL);
        if (match(TokenType.LESS_THAN)) return parseComparisonRhs(left, BinaryExpr.BinOp.LESS_THAN);
        if (match(TokenType.GREATER_THAN)) return parseComparisonRhs(left, BinaryExpr.BinOp.GREATER_THAN);
        if (match(TokenType.LESS_EQUALS)) return parseComparisonRhs(left, BinaryExpr.BinOp.LESS_EQUAL);
        if (match(TokenType.GREATER_EQUALS)) return parseComparisonRhs(left, BinaryExpr.BinOp.GREATER_EQUAL);

        // Array/JSON operators
        if (match(TokenType.CONTAINS)) return new BinaryExpr(left, BinaryExpr.BinOp.CONTAINS, parseAddition());
        if (match(TokenType.CONTAINED_BY)) return new BinaryExpr(left, BinaryExpr.BinOp.CONTAINED_BY, parseAddition());
        if (match(TokenType.OVERLAP)) return new BinaryExpr(left, BinaryExpr.BinOp.OVERLAP, parseAddition());
        if (match(TokenType.TS_MATCH)) return new BinaryExpr(left, BinaryExpr.BinOp.TS_MATCH, parseAddition());
        if (match(TokenType.JSONB_PATH_EXISTS_OP)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_PATH_EXISTS_OP, parseAddition());

        // JSONB key existence operators
        if (match(TokenType.JSONB_EXISTS)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_EXISTS, parseAddition());
        if (match(TokenType.JSONB_EXISTS_ANY)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_EXISTS_ANY, parseAddition());
        if (match(TokenType.JSONB_EXISTS_ALL)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_EXISTS_ALL, parseAddition());

        // Operator forms of LIKE/ILIKE and NOT LIKE/NOT ILIKE
        if (match(TokenType.DOUBLE_TILDE)) return new BinaryExpr(left, BinaryExpr.BinOp.LIKE, parseAddition());
        if (match(TokenType.DOUBLE_TILDE_STAR)) return new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, parseAddition());
        if (match(TokenType.NOT_DOUBLE_TILDE)) return new UnaryExpr(UnaryExpr.UnaryOp.NOT, new BinaryExpr(left, BinaryExpr.BinOp.LIKE, parseAddition()));
        if (match(TokenType.NOT_DOUBLE_TILDE_STAR)) return new UnaryExpr(UnaryExpr.UnaryOp.NOT, new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, parseAddition()));
        // POSIX regex operators
        if (match(TokenType.TILDE)) return new BinaryExpr(left, BinaryExpr.BinOp.REGEX_MATCH, parseAddition());
        if (match(TokenType.TILDE_STAR)) return new BinaryExpr(left, BinaryExpr.BinOp.REGEX_IMATCH, parseAddition());
        if (match(TokenType.EXCL_TILDE)) return new BinaryExpr(left, BinaryExpr.BinOp.NOT_REGEX_MATCH, parseAddition());
        if (match(TokenType.EXCL_TILDE_STAR)) return new BinaryExpr(left, BinaryExpr.BinOp.NOT_REGEX_IMATCH, parseAddition());

        // Geometric operators
        if (match(TokenType.DISTANCE)) return new BinaryExpr(left, BinaryExpr.BinOp.DISTANCE, parseAddition());
        if (match(TokenType.APPROX_EQUAL)) return new BinaryExpr(left, BinaryExpr.BinOp.APPROX_EQUAL, parseAddition());
        if (match(TokenType.GEO_BELOW)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_BELOW, parseAddition());
        if (match(TokenType.GEO_ABOVE)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_ABOVE, parseAddition());
        if (match(TokenType.GEO_NOT_EXTEND_RIGHT)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_RIGHT, parseAddition());
        if (match(TokenType.GEO_NOT_EXTEND_LEFT)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_LEFT, parseAddition());
        if (match(TokenType.GEO_NOT_EXTEND_ABOVE)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_ABOVE, parseAddition());
        if (match(TokenType.GEO_NOT_EXTEND_BELOW)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_BELOW, parseAddition());

        // Range adjacency operator: -|-
        if (match(TokenType.RANGE_ADJACENT)) return new BinaryExpr(left, BinaryExpr.BinOp.RANGE_ADJACENT, parseAddition());

        // SQL OVERLAPS: (start, end) OVERLAPS (start, end)
        if (matchKeyword("OVERLAPS")) return new BinaryExpr(left, BinaryExpr.BinOp.OVERLAPS, parseAddition());

        // OPERATOR(schema.op) infix syntax: expr OPERATOR(pg_catalog.+) expr
        if (checkKeyword("OPERATOR")) {
            return specialFormParser.parseQualifiedOperator(left);
        }

        return left;
    }

    /**
     * After consuming a comparison operator, check if the next token is ANY or ALL
     * (for subquery comparisons), otherwise parse a regular binary expression.
     */
    private Expression parseComparisonRhs(Expression left, BinaryExpr.BinOp op) {
        if (checkKeyword("ANY") || checkKeyword("ALL")) {
            boolean isAll = checkKeyword("ALL");
            advance(); // consume ANY/ALL
            expect(TokenType.LEFT_PAREN);
            if (checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                Statement subquery = parseSubqueryWithSetOps();
                expect(TokenType.RIGHT_PAREN);
                return new AnyAllExpr(left, op, subquery, isAll);
            }
            // array expression fallback
            Expression arrayExpr = parseExpression();
            expect(TokenType.RIGHT_PAREN);
            return new AnyAllArrayExpr(left, op, arrayExpr, isAll);
        }
        return new BinaryExpr(left, op, parseAddition());
    }

    // Package-private so ExprSpecialFormParser can call it for qualified operator RHS
    Expression parseAddition() {
        Expression left = parseBitOr();
        while (true) {
            if (match(TokenType.PLUS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.ADD, parseBitOr());
            } else if (match(TokenType.MINUS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.SUBTRACT, parseBitOr());
            } else if (match(TokenType.CONCAT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.CONCAT, parseBitOr());
            } else if (check(TokenType.CUSTOM_OPERATOR)) {
                // User-defined multi-char operators: same precedence as addition (left-associative)
                String opSymbol = advance().value();
                left = new CustomOperatorExpr(null, opSymbol, left, parseBitOr());
            } else {
                break;
            }
        }
        return left;
    }

    private Expression parseBitOr() {
        Expression left = parseBitXor();
        while (match(TokenType.PIPE)) {
            left = new BinaryExpr(left, BinaryExpr.BinOp.BIT_OR, parseBitXor());
        }
        return left;
    }

    private Expression parseBitXor() {
        Expression left = parseBitAnd();
        while (match(TokenType.HASH)) {
            left = new BinaryExpr(left, BinaryExpr.BinOp.BIT_XOR, parseBitAnd());
        }
        return left;
    }

    private Expression parseBitAnd() {
        Expression left = parseBitShift();
        while (match(TokenType.AMPERSAND)) {
            left = new BinaryExpr(left, BinaryExpr.BinOp.BIT_AND, parseBitShift());
        }
        return left;
    }

    private Expression parseBitShift() {
        Expression left = parseMultiplication();
        while (true) {
            if (match(TokenType.INET_CONTAINED_BY_EQUALS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.INET_CONTAINED_BY_EQUALS, parseMultiplication());
            } else if (match(TokenType.INET_CONTAINS_EQUALS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.INET_CONTAINS_EQUALS, parseMultiplication());
            } else if (match(TokenType.SHIFT_LEFT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.SHIFT_LEFT, parseMultiplication());
            } else if (match(TokenType.SHIFT_RIGHT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.SHIFT_RIGHT, parseMultiplication());
            } else {
                break;
            }
        }
        return left;
    }

    private Expression parseMultiplication() {
        Expression left = parsePower();
        while (true) {
            if (match(TokenType.STAR)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.MULTIPLY, parsePower());
            } else if (match(TokenType.SLASH)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.DIVIDE, parsePower());
            } else if (match(TokenType.PERCENT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.MODULO, parsePower());
            } else {
                break;
            }
        }
        return left;
    }

    private Expression parsePower() {
        Expression left = parseUnary();
        // Right-associative: 2^3^4 = 2^(3^4)
        if (match(TokenType.CARET)) {
            return new BinaryExpr(left, BinaryExpr.BinOp.POWER, parsePower());
        }
        return left;
    }

    private Expression parseUnary() {
        if (match(TokenType.AT_SIGN)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.ABS, parseUnary());
        }
        if (match(TokenType.MINUS)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.NEGATE, parseUnary());
        }
        if (match(TokenType.PLUS)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.POSITIVE, parseUnary());
        }
        if (match(TokenType.TILDE)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.BIT_NOT, parseUnary());
        }
        // ||/ (cube root) prefix operator; may be lexed as CONCAT+SLASH or CUSTOM_OPERATOR
        if (check(TokenType.CONCAT) && pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.SLASH) {
            advance(); advance(); // consume || /
            return new UnaryExpr(UnaryExpr.UnaryOp.CBRT, parseUnary());
        }
        // |/ (square root) prefix operator; may be lexed as PIPE+SLASH or CUSTOM_OPERATOR
        if (check(TokenType.PIPE) && pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.SLASH) {
            advance(); advance(); // consume | /
            return new UnaryExpr(UnaryExpr.UnaryOp.SQRT, parseUnary());
        }
        // Custom multi-char prefix operator (user-defined): ~~> expr
        // Must check after ||/ and |/ to avoid intercepting built-in prefix operators
        if (check(TokenType.CUSTOM_OPERATOR)) {
            String opSymbol = peek().value();
            if (opSymbol.equals("||/")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.CBRT, parseUnary());
            }
            if (opSymbol.equals("|/")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.SQRT, parseUnary());
            }
            advance();
            Expression right = parseUnary();
            return new CustomOperatorExpr(null, opSymbol, null, right);
        }
        return parsePostfix();
    }

    private Expression parsePostfix() {
        Expression expr = parsePrimary();

        // Handle postfix operators: ::cast, JSON arrows, array subscript
        while (true) {
            if (match(TokenType.CAST)) {
                String typeName = parseTypeName();
                expr = new CastExpr(expr, typeName);
            } else if (match(TokenType.JSON_ARROW)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_ARROW, parsePrimary());
            } else if (match(TokenType.JSON_ARROW_TEXT)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_ARROW_TEXT, parsePrimary());
            } else if (match(TokenType.JSON_HASH_ARROW)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_HASH_ARROW, parsePrimary());
            } else if (match(TokenType.JSON_HASH_ARROW_TEXT)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_HASH_ARROW_TEXT, parsePrimary());
            } else if (match(TokenType.JSON_DELETE_PATH)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_DELETE_PATH, parsePrimary());
            } else if (check(TokenType.LEFT_BRACKET)) {
                // PG rejects ARRAY[...][n] (bare array literal subscript); requires (ARRAY[...])[n]
                if (expr instanceof ArrayExpr && !((ArrayExpr) expr).isRow()
                        && pos >= 1 && tokens.get(pos - 1).type() == TokenType.RIGHT_BRACKET) {
                    ArrayExpr ae = (ArrayExpr) expr;
                    throw new ParseException("syntax error at or near \"[\"", peek());
                }
                advance(); // consume [
                // Check for open-ended slice: [:upper]
                Expression lower = null;
                Expression upper = null;
                boolean isSlice = false;
                if (check(TokenType.COLON)) {
                    // [:upper], no lower bound
                    advance(); // consume :
                    isSlice = true;
                    if (!check(TokenType.RIGHT_BRACKET)) {
                        upper = parseExpression();
                    }
                } else {
                    lower = parseExpression();
                    if (check(TokenType.COLON)) {
                        advance(); // consume :
                        isSlice = true;
                        if (!check(TokenType.RIGHT_BRACKET)) {
                            upper = parseExpression();
                        }
                    }
                }
                expect(TokenType.RIGHT_BRACKET);
                if (isSlice) {
                    expr = new ArraySliceExpr(expr, lower, upper);
                } else {
                    expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_ARROW, lower); // array subscript
                }
            } else if (matchKeywords("AT", "TIME", "ZONE")) {
                Expression zone = parsePrimary();
                expr = new AtTimeZoneExpr(expr, zone);
            } else if (matchKeyword("COLLATE")) {
                // COLLATE postfix: validate collation name and wrap in CollateExpr.
                if (isClauseKeyword()) {
                    pos--; // un-consume COLLATE so it becomes a potential alias
                    break;
                } else {
                    String collation = readIdentifierOrString();
                    if (match(TokenType.DOT)) {
                        collation = collation + "." + readIdentifierOrString();
                    }
                    validateCollation(collation);
                    expr = new CollateExpr(expr, collation.toLowerCase());
                }
            } else if (check(TokenType.DOT) && expr instanceof ArrayExpr && ((ArrayExpr) expr).isRow()) {
                ArrayExpr ae = (ArrayExpr) expr;
                // ROW(...).field: composite field access
                advance(); // consume DOT
                String fieldName = readIdentifier();
                expr = new FieldAccessExpr(expr, fieldName);
            } else {
                break;
            }
        }

        return expr;
    }

    private static final java.util.Set<String> KNOWN_COLLATIONS = Cols.setOf(
            "c", "posix", "default", "ucs_basic", "en_us", "en_us.utf-8", "en_us.utf8",
            "pg_catalog.c", "pg_catalog.posix", "pg_catalog.default",
            "pg_catalog.\"c\"", "pg_catalog.\"posix\"", "pg_catalog.\"default\"",
            "\"c\"", "\"posix\"", "\"default\"", "\"ucs_basic\"",
            "unicode", "icu_root"
    );

    private void validateCollation(String collation) {
        String lower = collation.toLowerCase();
        String unquoted = lower.replace("\"", "");
        if (!KNOWN_COLLATIONS.contains(unquoted) && !KNOWN_COLLATIONS.contains(lower)) {
            throw new ParseException("collation \"" + collation + "\" for encoding \"UTF8\" does not exist",
                    peek(), "42704");
        }
    }

    protected Expression parsePrimary() {
        Token t = peek();

        // Parenthesized expression, subquery, or row constructor
        if (check(TokenType.LEFT_PAREN)) {
            advance();
            // Check if it's a subquery (may include UNION/INTERSECT/EXCEPT)
            if (checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                Statement subquery = parseSubqueryWithSetOps();
                expect(TokenType.RIGHT_PAREN);
                Expression subExpr = new SubqueryExpr(subquery);
                // Check for composite field access: (SELECT ...).field
                if (match(TokenType.DOT)) {
                    String fieldName = readIdentifier();
                    subExpr = new FieldAccessExpr(subExpr, fieldName);
                    while (match(TokenType.DOT)) {
                        fieldName = readIdentifier();
                        subExpr = new FieldAccessExpr(subExpr, fieldName);
                    }
                }
                return subExpr;
            }
            Expression expr = parseExpression();
            if (match(TokenType.COMMA)) {
                // Row constructor: (expr, expr, ...)
                List<Expression> elements = new ArrayList<>();
                elements.add(expr);
                elements.add(parseExpression());
                while (match(TokenType.COMMA)) {
                    elements.add(parseExpression());
                }
                expect(TokenType.RIGHT_PAREN);
                return new ArrayExpr(elements, true); // parenthesized tuple is a row constructor
            }
            expect(TokenType.RIGHT_PAREN);
            // Check for composite field access: (expr).field or (expr).* or (expr).field1.field2
            if (match(TokenType.DOT)) {
                if (check(TokenType.STAR)) {
                    advance(); // consume *
                    return new CompositeStarExpr(expr);
                }
                String fieldName = readIdentifier();
                expr = new FieldAccessExpr(expr, fieldName);
                while (match(TokenType.DOT)) {
                    if (check(TokenType.STAR)) {
                        advance();
                        return new CompositeStarExpr(expr);
                    }
                    fieldName = readIdentifier();
                    expr = new FieldAccessExpr(expr, fieldName);
                }
                return expr;
            }
            return expr;
        }

        // Numeric literals
        if (check(TokenType.INTEGER_LITERAL)) {
            return Literal.ofInt(advance().value());
        }
        if (check(TokenType.FLOAT_LITERAL)) {
            return Literal.ofFloat(advance().value());
        }

        // String literals
        if (check(TokenType.STRING_LITERAL)) {
            return Literal.ofString(advance().value());
        }
        if (check(TokenType.DOLLAR_STRING_LITERAL)) {
            return Literal.ofString(advance().value());
        }
        // Bit string literals: B'1010' or X'1F', stored as string of 0s and 1s
        if (check(TokenType.BIT_STRING_LITERAL)) {
            return Literal.ofBitString(advance().value());
        }

        // Parameter reference
        if (check(TokenType.PARAM)) {
            String param = advance().value();
            return new ParamRef(Integer.parseInt(param.substring(1)));
        }

        // Keywords that are values
        if (t.type() == TokenType.KEYWORD) {
            switch (t.value()) {
                case "TRUE": {
                    advance(); return Literal.ofBoolean(true);
                }
                case "FALSE": {
                    advance(); return Literal.ofBoolean(false);
                }
                case "NULL": {
                    advance(); return Literal.ofNull();
                }
                case "CURRENT_TIMESTAMP":
                case "CURRENT_DATE":
                case "CURRENT_TIME":
                case "LOCALTIME":
                case "LOCALTIMESTAMP":
                case "CURRENT_USER":
                case "SESSION_USER":
                case "CURRENT_ROLE":
                case "CURRENT_CATALOG":
                case "CURRENT_SCHEMA": {
                    advance();
                    // Consume optional empty parentheses (e.g., current_schema() or current_user)
                    if (check(TokenType.LEFT_PAREN)) {
                        int saved = pos;
                        advance(); // (
                        if (check(TokenType.RIGHT_PAREN)) {
                            advance(); // )
                        } else {
                            pos = saved; // restore; there were args after (
                        }
                    }
                    return new FunctionCallExpr(t.value().toLowerCase(), Cols.listOf());
                }
                case "INTERVAL": {
                    return specialFormParser.parseInterval(); 
                }
                case "DATE": {
                    // DATE 'value' literal
                    if (tokens.size() > pos + 1 && tokens.get(pos + 1).type() == TokenType.STRING_LITERAL) {
                        advance(); // consume DATE
                        String val = advance().value();
                        return new CastExpr(Literal.ofString(val), "date");
                    }
                    // else fall through to identifier handling
                    break;
                }
                case "TIME": {
                    if (tokens.size() > pos + 1 && tokens.get(pos + 1).type() == TokenType.STRING_LITERAL) {
                        advance();
                        String val = advance().value();
                        return new CastExpr(Literal.ofString(val), "time");
                    }
                    break;
                }
                case "TIMESTAMP": {
                    return specialFormParser.parseTimestamp(); 
                }
                case "DEFAULT": {
                    advance();
                    return Literal.ofDefault();
                }
                case "OPERATOR": {
                    return specialFormParser.parsePrefixQualifiedOperator(); 
                }
                case "EXISTS": {
                    advance();
                    expect(TokenType.LEFT_PAREN);
                    Statement subquery = parseSubqueryWithSetOps();
                    expect(TokenType.RIGHT_PAREN);
                    return new ExistsExpr(subquery);
                }
                case "CASE": {
                    return specialFormParser.parseCaseExpression(); 
                }
                case "CAST": {
                    return specialFormParser.parseCastFunction(); 
                }
                case "ARRAY": {
                    return specialFormParser.parseArrayConstructor(); 
                }
                case "ROW": {
                    if (pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.LEFT_PAREN) {
                        advance(); // consume ROW
                        expect(TokenType.LEFT_PAREN);
                        if (check(TokenType.RIGHT_PAREN)) {
                            advance();
                            return new ArrayExpr(Cols.listOf(), true);
                        }
                        List<Expression> elements = parseExpressionList();
                        expect(TokenType.RIGHT_PAREN);
                        return new ArrayExpr(elements, true);
                    }
                    advance();
                    return new ColumnRef("row");
                }
                case "NOT": {
                    advance();
                    return new UnaryExpr(UnaryExpr.UnaryOp.NOT, parsePrimary());
                }
                case "SUBSTRING": {
                    return specialFormParser.parseSubstring(); 
                }
                case "POSITION": {
                    return specialFormParser.parsePosition(); 
                }
                case "OVERLAY": {
                    return specialFormParser.parseOverlay(); 
                }
                case "EXTRACT": {
                    return specialFormParser.parseExtract(); 
                }
                case "TRIM": {
                    return specialFormParser.parseTrim(); 
                }
                case "XMLPARSE": {
                    return specialFormParser.parseXmlparse(); 
                }
                case "XMLSERIALIZE": {
                    return specialFormParser.parseXmlserialize(); 
                }
                case "XMLELEMENT": {
                    return specialFormParser.parseXmlelement(); 
                }
                case "XMLFOREST": {
                    return specialFormParser.parseXmlforest(); 
                }
                case "XMLPI": {
                    return specialFormParser.parseXmlpi(); 
                }
                case "XMLROOT": {
                    return specialFormParser.parseXmlroot(); 
                }
                case "XMLCONCAT": {
                    return specialFormParser.parseBuiltinFunction(); 
                }
                case "XMLEXISTS": {
                    return specialFormParser.parseXmlexists(); 
                }
                case "XMLAGG": {
                    return specialFormParser.parseBuiltinFunction(); 
                }
                case "COALESCE":
                case "NULLIF":
                case "GREATEST":
                case "LEAST": {
                    return specialFormParser.parseBuiltinFunction();
                }
                // SQL/JSON standard functions (PG 16+)
                case "JSON_EXISTS": return specialFormParser.parseJsonExists();
                case "JSON_VALUE": return specialFormParser.parseJsonValue();
                case "JSON_QUERY": return specialFormParser.parseJsonQuery();
                case "JSON_SCALAR": return specialFormParser.parseJsonScalar();
                case "JSON_SERIALIZE": return specialFormParser.parseJsonSerialize();
                case "JSON_ARRAY": return specialFormParser.parseJsonArray();
                case "JSON_OBJECT": return specialFormParser.parseJsonObject();
                case "JSON_ARRAYAGG": return specialFormParser.parseJsonArrayagg();
                case "JSON_OBJECTAGG": return specialFormParser.parseJsonObjectagg();
                case "NEW":
                case "OLD": {
                    // Trigger variable: NEW.column or OLD.column
                    String prefix = advance().value().toLowerCase();
                    if (match(TokenType.DOT)) {
                        String col = readIdentifier();
                        return new ColumnRef(null, prefix, col);
                    }
                    return new ColumnRef(prefix);
                }
            }
        }

        // Identifier: could be column ref, function call, type-annotated literal, or qualified name
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER || t.type() == TokenType.KEYWORD) {
            // Reject boolean connectives and COLLATE as identifiers, which indicate a syntax error
            if (t.type() == TokenType.KEYWORD && (t.value().equals("AND") || t.value().equals("OR")
                    || t.value().equals("COLLATE"))) {
                throw new ParseException("syntax error at or near \"" + t.value() + "\"", t);
            }
            String name = readIdentifier();

            // Type-annotated literal: typename 'value' (e.g., point '(1,2)', DATE '2024-01-01', json '{}')
            if (check(TokenType.STRING_LITERAL)) {
                String lower = name.toLowerCase();
                if (lower.equals("point") || lower.equals("line") || lower.equals("lseg")
                        || lower.equals("box") || lower.equals("path") || lower.equals("polygon")
                        || lower.equals("circle")
                        || lower.equals("date") || lower.equals("time") || lower.equals("timestamp")
                        || lower.equals("timestamptz") || lower.equals("timetz") || lower.equals("interval")
                        || lower.equals("json") || lower.equals("jsonb")
                        || lower.equals("boolean") || lower.equals("bool")
                        || lower.equals("inet") || lower.equals("cidr") || lower.equals("macaddr")
                        || lower.equals("xml") || lower.equals("uuid")
                        || lower.equals("bit") || lower.equals("varbit")) {
                    String val = advance().value();
                    String castType;
                    switch (lower) {
                        case "timestamptz":
                            castType = "timestamp with time zone";
                            break;
                        case "timetz":
                            castType = "time with time zone";
                            break;
                        case "bool":
                            castType = "boolean";
                            break;
                        default:
                            castType = lower;
                            break;
                    }
                    return new CastExpr(Literal.ofString(val), castType);
                }
            }

            // Function call: name(...)
            if (check(TokenType.LEFT_PAREN)) {
                return parseFunctionCallExpr(name);
            }

            // Qualified column: table.column or schema.table.column
            if (check(TokenType.DOT)) {
                advance();

                // table.*
                if (check(TokenType.STAR)) {
                    advance();
                    return new WildcardExpr(name);
                }

                String name2 = readIdentifier();

                // Check for schema.table.column
                if (check(TokenType.DOT)) {
                    advance();
                    String name3 = readIdentifier();
                    return new ColumnRef(name, name2, name3);
                }

                // It might be a function call: schema.func(...)
                if (check(TokenType.LEFT_PAREN)) {
                    return parseFunctionCallExpr(name2);
                }

                return new ColumnRef(null, name, name2);
            }

            // Constant type cast: identifier 'string_literal' (e.g., open '...', box '...')
            if (pos < tokens.size() && tokens.get(pos).type() == TokenType.STRING_LITERAL) {
                String litValue = advance().value();
                return new CastExpr(Literal.ofString(litValue), name);
            }

            return new ColumnRef(name);
        }

        // Star (when not handled elsewhere)
        if (check(TokenType.STAR)) {
            advance();
            return new WildcardExpr();
        }

        // Operator-like tokens in expression position indicate an undefined operator (42883)
        if (t.type() == TokenType.EQUALS || t.type() == TokenType.ERROR
                || t.type() == TokenType.TS_MATCH) {
            // Bare $ is a syntax error (unterminated parameter reference), not an operator error
            if (t.type() == TokenType.ERROR && "$".equals(t.value())) {
                throw new ParseException("syntax error at or near \"$\"", t);
            }
            // The ! postfix factorial operator was removed in PG 14; it's a syntax error, not an undefined operator
            if (t.type() == TokenType.ERROR && "!".equals(t.value())) {
                throw new ParseException("syntax error at or near \"!\"", t);
            }
            // !! is the removed factorial operator; report as undefined operator (42883)
            if (t.type() == TokenType.ERROR && "!!".equals(t.value())) {
                throw new ParseException("operator does not exist: !!", t, "42883");
            }
            // For TS_MATCH (@@) followed by another operator char (e.g. @@@), report the combined operator
            if (t.type() == TokenType.TS_MATCH && pos + 1 < tokens.size()) {
                Token next = tokens.get(pos);
                if (next.type() == TokenType.AT_SIGN) {
                    advance(); // consume @@
                    throw new ParseException("operator does not exist: @@@", t, "42883");
                }
            }
            throw new ParseException("operator does not exist: " + t.value(), t, "42883");
        }

        throw new ParseException("Unexpected token in expression", t);
    }

    /**
     * Parse a function call expression: name(...) with optional DISTINCT, ORDER BY,
     * FILTER, WITHIN GROUP, and OVER clauses. DRYs unqualified and schema-qualified function calls.
     */
    private Expression parseFunctionCallExpr(String name) {
        advance(); // consume (

        boolean isStar = false;
        boolean distinct = false;
        List<Expression> args;

        // COUNT(*) special case
        List<SelectStmt.OrderByItem> innerOrderBy = null;
        if (check(TokenType.STAR)) {
            advance();
            expect(TokenType.RIGHT_PAREN);
            isStar = true;
            args = Cols.listOf();
        } else if (check(TokenType.RIGHT_PAREN)) {
            // Empty args
            advance();
            args = Cols.listOf();
        } else {
            // DISTINCT in aggregates
            distinct = matchKeyword("DISTINCT");
            args = parseFunctionArgList();
            // Check for ORDER BY inside aggregate: string_agg(expr, delim ORDER BY ...)
            if (checkKeyword("ORDER")) {
                innerOrderBy = parseOrderByClause();
            }
            expect(TokenType.RIGHT_PAREN);
        }

        // Check for FILTER (WHERE ...) clause on aggregates
        Expression filter = null;
        if (checkKeyword("FILTER")) {
            advance();
            expect(TokenType.LEFT_PAREN);
            expectKeyword("WHERE");
            filter = parseExpression();
            expect(TokenType.RIGHT_PAREN);
        }

        // Check for WITHIN GROUP (ORDER BY ...): ordered-set aggregate
        if (checkKeyword("WITHIN")) {
            advance(); // WITHIN
            expectKeyword("GROUP");
            expect(TokenType.LEFT_PAREN);
            expectKeyword("ORDER");
            expectKeyword("BY");
            List<SelectStmt.OrderByItem> withinOrderBy = parseOrderByList();
            expect(TokenType.RIGHT_PAREN);
            return new OrderedSetAggExpr(name.toLowerCase(), args, withinOrderBy);
        }

        // Check for OVER clause: window function
        if (checkKeyword("OVER")) {
            return specialFormParser.parseWindowFunction(name, args, distinct, isStar);
        }

        if (innerOrderBy != null || filter != null) {
            return new FunctionCallExpr(name, args, distinct, isStar, innerOrderBy, filter);
        }
        if (isStar) return new FunctionCallExpr(name, args, false, true);
        return new FunctionCallExpr(name, args, distinct, false);
    }

    /** Forwarding method for SelectParser; delegates to ExprSpecialFormParser. */
    protected WindowFuncExpr.FrameClause parseWindowFrame() {
        return specialFormParser.parseWindowFrame();
    }
}
