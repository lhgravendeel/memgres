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
                returnType = "SETOF " + parser.readIdentifier();
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
                returnType = parser.readIdentifier();
            }
        }

        String body = null;
        String language = "sql";
        boolean[] secDefRef = {false};
        boolean[] strictRef = {false};
        boolean[] leakproofRef = {false};
        String[] volatilityRef = {"VOLATILE"};
        java.util.Map<String, String> setClauses = new java.util.LinkedHashMap<>();

        if (parser.matchKeyword("AS")) {
            body = readFunctionBody();
            parseFunctionAttributes(secDefRef, strictRef, volatilityRef, leakproofRef, setClauses);
            if (parser.matchKeyword("LANGUAGE")) {
                language = parser.readIdentifierOrString();
            }
        } else if (parser.matchKeyword("LANGUAGE")) {
            language = parser.readIdentifierOrString();
            parseFunctionAttributes(secDefRef, strictRef, volatilityRef, leakproofRef, setClauses);
            if (parser.matchKeyword("AS")) {
                body = readFunctionBody();
            }
        }

        parseFunctionAttributes(secDefRef, strictRef, volatilityRef, leakproofRef, setClauses);

        return new CreateFunctionStmt(name, schema, rawParams.toString().trim(), parsedParams,
                returnType, body != null ? body : "", language, orReplace, isProcedure, secDefRef[0], strictRef[0],
                leakproofRef[0], volatilityRef[0], setClauses.isEmpty() ? null : setClauses);
    }

    CallStmt parseCall() {
        parser.expectKeyword("CALL");
        String name = parser.readIdentifier();
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
                                          java.util.Map<String, String> setClauses) {
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
                    if (kw.equals("COST")) parser.advance();
                    if (kw.equals("ROWS")) parser.advance();
                    if (kw.equals("PARALLEL")) parser.readIdentifier();
                    if (kw.equals("SUPPORT")) parser.readIdentifier(); // consume support function name
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
