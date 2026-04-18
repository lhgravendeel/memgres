package com.memgres.engine.parser;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Function/procedure creation and CALL parsing, extracted from DdlParser.
 */
class DdlFunctionParser {
    private final Parser parser;

    DdlFunctionParser(Parser parser) {
        this.parser = parser;
    }

    CreateFunctionStmt parseCreateFunction(boolean orReplace, boolean isProcedure) {
        String name = parser.readIdentifier();
        String schema = null;
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        parser.expect(TokenType.LEFT_PAREN);

        StringBuilder rawParams = new StringBuilder();
        List<CreateFunctionStmt.FuncParam> parsedParams = new ArrayList<>();
        if (!parser.check(TokenType.RIGHT_PAREN)) {
            do {
                String mode = "IN";
                if (parser.checkKeyword("VARIADIC")) { parser.advance(); mode = "VARIADIC"; }
                else if (parser.checkKeyword("INOUT") || parser.checkIdentifier("INOUT")) { parser.advance(); mode = "INOUT"; }
                else if (parser.checkKeyword("IN")) {
                    parser.advance(); mode = "IN";
                    if (parser.matchKeyword("OUT") || parser.matchIdentifier("OUT")) mode = "INOUT";
                }
                else if (parser.checkKeyword("OUT") || parser.checkIdentifier("OUT")) { parser.advance(); mode = "OUT"; }

                String paramName = null;
                int saved = parser.pos;
                String firstIdent = parser.readIdentifier();

                boolean isTypeOnly = parser.check(TokenType.COMMA) || parser.check(TokenType.RIGHT_PAREN) ||
                        parser.checkKeyword("DEFAULT") || parser.check(TokenType.COLON_EQUALS) ||
                        parser.check(TokenType.LEFT_BRACKET);
                if (isTypeOnly) {
                    paramName = null;
                    String typeName = firstIdent;
                    typeName = readTypeModifiers(typeName);
                    parsedParams.add(new CreateFunctionStmt.FuncParam(paramName, typeName, mode));
                } else if (parser.checkKeyword("OUT") || parser.checkIdentifier("OUT") ||
                        parser.checkKeyword("INOUT") || parser.checkIdentifier("INOUT")) {
                    paramName = firstIdent;
                    String actualMode = parser.advance().value().toUpperCase();
                    if (actualMode.equals("IN") && (parser.matchKeyword("OUT") || parser.matchIdentifier("OUT"))) actualMode = "INOUT";
                    String typeName = parser.parseTypeName();
                    parsedParams.add(new CreateFunctionStmt.FuncParam(paramName, typeName, actualMode));
                } else {
                    paramName = firstIdent;
                    String typeName = parser.parseTypeName();
                    parsedParams.add(new CreateFunctionStmt.FuncParam(paramName, typeName, mode));
                }

                if (parser.matchKeyword("DEFAULT") || parser.match(TokenType.COLON_EQUALS)) {
                    if (parser.check(TokenType.RIGHT_PAREN) || parser.check(TokenType.COMMA)) {
                        throw new ParseException("syntax error at or near \"" + parser.peek().value() + "\"", parser.peek());
                    }
                    StringBuilder defaultText = new StringBuilder();
                    int depth = 0;
                    while (!parser.isAtEnd()) {
                        if (parser.check(TokenType.LEFT_PAREN)) { depth++; defaultText.append(parser.advance().value()); continue; }
                        if (parser.check(TokenType.RIGHT_PAREN)) {
                            if (depth == 0) break;
                            depth--; defaultText.append(parser.advance().value()); continue;
                        }
                        if (parser.check(TokenType.COMMA) && depth == 0) break;
                        Token dt = parser.advance();
                        if (defaultText.length() > 0) defaultText.append(" ");
                        if (dt.type() == TokenType.STRING_LITERAL) {
                            defaultText.append("'").append(dt.value().replace("'", "''")).append("'");
                        } else {
                            defaultText.append(dt.value());
                        }
                    }
                    if (!parsedParams.isEmpty()) {
                        CreateFunctionStmt.FuncParam last = parsedParams.remove(parsedParams.size() - 1);
                        parsedParams.add(new CreateFunctionStmt.FuncParam(last.name(), last.typeName(), last.mode(), defaultText.toString()));
                    }
                }
            } while (parser.match(TokenType.COMMA));
        }
        parser.expect(TokenType.RIGHT_PAREN);

        String returnType = isProcedure ? "void" : null;
        if (parser.matchKeyword("RETURNS")) {
            if (parser.checkKeyword("SETOF")) {
                parser.advance();
                returnType = "SETOF " + parser.parseTypeName();
            } else if (parser.checkKeyword("TABLE")) {
                parser.advance();
                returnType = "TABLE";
                parser.expect(TokenType.LEFT_PAREN);
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    String colName = parser.readIdentifier();
                    String colType = parser.parseTypeName();
                    parsedParams.add(new CreateFunctionStmt.FuncParam(colName, colType, "OUT"));
                    parser.match(TokenType.COMMA);
                }
                parser.expect(TokenType.RIGHT_PAREN);
            } else {
                returnType = parser.parseTypeName();
            }
        }

        String body = null;
        String language = "sql";
        boolean[] secDefRef = {false};
        boolean[] strictRef = {false};
        boolean[] leakproofRef = {false};
        String[] volatilityRef = {"VOLATILE"};
        String[] parallelRef = {null};
        double[] costRef = {-1};
        double[] rowsRef = {-1};
        String[] supportRef = {null};
        java.util.Map<String, String> setClauses = new java.util.LinkedHashMap<>();
        boolean isAtomicBody = false;

        if (parser.matchKeyword("AS")) {
            body = readFunctionBody();
            parseFunctionAttributes(secDefRef, strictRef, volatilityRef, leakproofRef, setClauses, parallelRef, costRef, rowsRef, supportRef);
            if (parser.matchKeyword("LANGUAGE")) {
                language = parser.readIdentifierOrString();
            }
        } else if (parser.matchKeyword("LANGUAGE")) {
            language = parser.readIdentifierOrString();
            parseFunctionAttributes(secDefRef, strictRef, volatilityRef, leakproofRef, setClauses, parallelRef, costRef, rowsRef, supportRef);
            if (parser.matchKeyword("AS")) {
                body = readFunctionBody();
            }
        }

        parseFunctionAttributes(secDefRef, strictRef, volatilityRef, leakproofRef, setClauses, parallelRef, costRef, rowsRef, supportRef);

        // SQL-standard function body: RETURN expr or BEGIN ATOMIC ... END (PG 14+)
        if (body == null && parser.checkKeyword("RETURN")) {
            // PG: inline SQL function body only valid for language SQL
            if (!"sql".equalsIgnoreCase(language)) {
                throw new ParseException("inline SQL function body only valid for language SQL", parser.peek(), "42P13");
            }
            parser.advance();
            body = readSqlStandardReturn();
            language = "sql";
        } else if (body == null && parser.checkKeyword("BEGIN") && isBeginAtomic()) {
            // PG: inline SQL function body only valid for language SQL
            if (!"sql".equalsIgnoreCase(language)) {
                throw new ParseException("inline SQL function body only valid for language SQL", parser.peek(), "42P13");
            }
            body = readBeginAtomicBody();
            language = "sql";
            isAtomicBody = true;
        }

        CreateFunctionStmt result = new CreateFunctionStmt(name, schema, rawParams.toString().trim(), parsedParams,
                returnType, body != null ? body : "", language, orReplace, isProcedure, secDefRef[0], strictRef[0],
                leakproofRef[0], volatilityRef[0], setClauses.isEmpty() ? null : setClauses,
                parallelRef[0], costRef[0], rowsRef[0]);
        result.atomicBody = isAtomicBody;
        result.supportFunction = supportRef[0];
        return result;
    }

    CallStmt parseCall() {
        parser.expectKeyword("CALL");
        String name = parser.readIdentifier();
        // Support schema-qualified procedure names: schema.procedure(...)
        if (parser.match(TokenType.DOT)) {
            name = name + "." + parser.readIdentifier();
        }
        parser.expect(TokenType.LEFT_PAREN);
        List<Expression> args = new ArrayList<>();
        if (!parser.check(TokenType.RIGHT_PAREN)) {
            do {
                args.add(parser.parseExpression());
            } while (parser.match(TokenType.COMMA));
        }
        parser.expect(TokenType.RIGHT_PAREN);
        return new CallStmt(name, args);
    }

    /**
     * Check if the current BEGIN is followed by ATOMIC (lookahead without consuming).
     */
    private boolean isBeginAtomic() {
        int saved = parser.pos;
        if (parser.checkKeyword("BEGIN")) {
            parser.advance();
            boolean result = parser.checkKeyword("ATOMIC");
            parser.pos = saved;
            return result;
        }
        return false;
    }

    /**
     * SQL-standard RETURN expr — capture everything until semicolon/EOF as SELECT expr.
     */
    private String readSqlStandardReturn() {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (!parser.isAtEnd()) {
            if (parser.check(TokenType.SEMICOLON) && depth == 0) break;
            if (parser.check(TokenType.EOF)) break;
            if (parser.check(TokenType.LEFT_PAREN)) depth++;
            if (parser.check(TokenType.RIGHT_PAREN)) depth--;
            Token t = parser.advance();
            if (sb.length() > 0) sb.append(" ");
            if (t.type() == TokenType.STRING_LITERAL) {
                sb.append("'").append(t.value().replace("'", "''")).append("'");
            } else {
                sb.append(t.value());
            }
        }
        return "SELECT " + sb.toString().trim();
    }

    /**
     * SQL-standard BEGIN ATOMIC ... END — capture the enclosed statements as the body.
     */
    private String readBeginAtomicBody() {
        parser.expectKeyword("BEGIN");
        parser.expectKeyword("ATOMIC");
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        boolean foundEnd = false;
        while (!parser.isAtEnd()) {
            // END at depth 0 terminates the block
            if (parser.checkKeyword("END") && depth == 0) {
                parser.advance(); // consume END
                foundEnd = true;
                break;
            }
            // Track nested BEGIN/END (e.g., CASE ... END)
            if (parser.checkKeyword("CASE")) depth++;
            if (parser.checkKeyword("END") && depth > 0) depth--;
            Token t = parser.advance();
            if (sb.length() > 0) sb.append(" ");
            if (t.type() == TokenType.STRING_LITERAL) {
                sb.append("'").append(t.value().replace("'", "''")).append("'");
            } else {
                sb.append(t.value());
            }
        }
        if (!foundEnd) {
            throw new ParseException("unterminated BEGIN ATOMIC block — missing END", parser.peek());
        }
        return sb.toString().trim();
    }

    private String readFunctionBody() {
        Token bodyToken = parser.peek();
        if (bodyToken.type() == TokenType.DOLLAR_STRING_LITERAL) {
            return parser.advance().value();
        } else if (bodyToken.type() == TokenType.STRING_LITERAL) {
            return parser.advance().value();
        }
        throw new ParseException("Expected function body (dollar-quoted or string)", bodyToken);
    }

    private String readTypeModifiers(String typeName) {
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            StringBuilder sb = new StringBuilder(typeName).append("(");
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                sb.append(parser.advance().value());
            }
            parser.expect(TokenType.RIGHT_PAREN);
            sb.append(")");
            typeName = sb.toString();
        }
        if (parser.check(TokenType.LEFT_BRACKET)) {
            parser.advance();
            parser.expect(TokenType.RIGHT_BRACKET);
            typeName += "[]";
        }
        return typeName;
    }

    private void parseFunctionAttributes(boolean[] securityDefinerRef, boolean[] strictRef,
                                          String[] volatilityRef, boolean[] leakproofRef,
                                          java.util.Map<String, String> setClauses,
                                          String[] parallelRef, double[] costRef, double[] rowsRef,
                                          String[] supportRef) {
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
            Token t = parser.peek();
            if (t.type() == TokenType.KEYWORD) {
                String kw = t.value();
                if (kw.equals("IMMUTABLE") || kw.equals("STABLE") || kw.equals("VOLATILE") ||
                        kw.equals("STRICT") || kw.equals("SECURITY") || kw.equals("COST") ||
                        kw.equals("PARALLEL") || kw.equals("CALLED") || kw.equals("RETURNS") ||
                        kw.equals("ROWS") || kw.equals("LEAKPROOF") || kw.equals("SUPPORT")) {
                    parser.advance();
                    if (kw.equals("IMMUTABLE") || kw.equals("STABLE") || kw.equals("VOLATILE")) {
                        volatilityRef[0] = kw;
                    }
                    if (kw.equals("STRICT")) strictRef[0] = true;
                    if (kw.equals("SECURITY")) {
                        String defOrInvoker = parser.readIdentifier();
                        if ("DEFINER".equalsIgnoreCase(defOrInvoker)) securityDefinerRef[0] = true;
                        else securityDefinerRef[0] = false;
                    }
                    if (kw.equals("COST")) {
                        String costVal = parser.advance().value();
                        try { costRef[0] = Double.parseDouble(costVal); } catch (NumberFormatException e) { /* ignore */ }
                    }
                    if (kw.equals("ROWS")) {
                        String rowsVal = parser.advance().value();
                        try { rowsRef[0] = Double.parseDouble(rowsVal); } catch (NumberFormatException e) { /* ignore */ }
                    }
                    if (kw.equals("PARALLEL")) {
                        String parallelVal = parser.readIdentifier().toUpperCase();
                        parallelRef[0] = parallelVal;
                    }
                    if (kw.equals("SUPPORT")) supportRef[0] = parser.readIdentifier(); // consume support function name
                    if (kw.equals("LEAKPROOF")) { leakproofRef[0] = true; }
                    if (kw.equals("CALLED")) { parser.matchKeyword("ON"); parser.matchKeyword("NULL"); parser.matchKeyword("INPUT"); strictRef[0] = false; }
                    if (kw.equals("RETURNS")) { parser.matchKeyword("NULL"); parser.matchKeyword("ON"); parser.matchKeyword("NULL"); parser.matchKeyword("INPUT"); strictRef[0] = true; }
                    continue;
                }
                // NOT LEAKPROOF — two keywords
                if (kw.equals("NOT") && parser.matchKeywords("NOT", "LEAKPROOF")) {
                    leakproofRef[0] = false;
                    continue;
                }
                if (kw.equals("SET")) {
                    parser.advance();
                    String paramName = parser.readIdentifier();
                    if (parser.matchKeyword("TO") || parser.match(TokenType.EQUALS)) {
                        StringBuilder valBuf = new StringBuilder();
                        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
                            Token next = parser.peek();
                            if (next.type() == TokenType.KEYWORD && isFunctionAttributeKeyword(next.value())) break;
                            if (next.type() == TokenType.KEYWORD && next.value().equals("AS")) break;
                            if (valBuf.length() > 0) valBuf.append(" ");
                            valBuf.append(next.value());
                            parser.advance();
                        }
                        setClauses.put(paramName.toLowerCase(), valBuf.toString().trim());
                    }
                    continue;
                }
            }
            break;
        }
    }

    private static boolean isFunctionAttributeKeyword(String kw) {
        return kw.equals("IMMUTABLE") || kw.equals("STABLE") || kw.equals("VOLATILE") ||
                kw.equals("STRICT") || kw.equals("SECURITY") || kw.equals("COST") ||
                kw.equals("PARALLEL") || kw.equals("CALLED") || kw.equals("RETURNS") ||
                kw.equals("ROWS") || kw.equals("SET") || kw.equals("LANGUAGE") ||
                kw.equals("LEAKPROOF") || kw.equals("SUPPORT") || kw.equals("NOT");
    }
}
