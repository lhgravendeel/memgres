package com.memgres.engine.parser;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Function/procedure creation and CALL parsing, extracted from DdlParser.
 */
class DdlFunctionParser {
    private final Parser parser;

    // Keywords that PG treats as reserved in function parameter name positions.
    // These are "type_func_name" keywords that PG's parser interprets as expression starts
    // (e.g., OVERLAY(...), POSITION(...), TRIM(...)) rather than bare identifiers.
    private static final Set<String> RESERVED_PARAM_KEYWORDS = Cols.setOf(
        "OVERLAY", "POSITION", "SUBSTRING", "TREAT", "TRIM",
        "XMLELEMENT", "XMLFOREST", "XMLPARSE", "XMLPI", "XMLROOT", "XMLSERIALIZE",
        "XMLEXISTS", "XMLTABLE", "NORMALIZE", "JSON_ARRAY", "JSON_OBJECT",
        "JSON_ARRAYAGG", "JSON_OBJECTAGG"
    );

    /**
     * True when {@code firstIdent} is the first word of a two-word type name and the next token is
     * the word that completes it — so what was read as a parameter name is really half a type.
     */
    private static boolean continuesMultiWordType(Parser parser, String firstIdent) {
        String head = firstIdent == null ? "" : firstIdent.toLowerCase(java.util.Locale.ROOT);
        switch (head) {
            case "double":
                return parser.checkKeyword("PRECISION") || parser.checkIdentifier("precision");
            case "character":
            case "bit":
                return parser.checkKeyword("VARYING") || parser.checkIdentifier("varying");
            case "time":
            case "timestamp":
                return parser.checkKeyword("WITH") || parser.checkIdentifier("with")
                        || parser.checkKeyword("WITHOUT") || parser.checkIdentifier("without");
            default:
                return false;
        }
    }

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
                // Check if the next token is a reserved keyword that can't be used as a parameter name
                Token nextTok = parser.peek();
                if (nextTok.type() == TokenType.KEYWORD && RESERVED_PARAM_KEYWORDS.contains(nextTok.value())) {
                    throw new ParseException("syntax error at or near \"" + nextTok.value().toLowerCase(java.util.Locale.ROOT) + "\"", nextTok, "42601");
                }
                String firstIdent = parser.readIdentifier();

                boolean isTypeOnly = parser.check(TokenType.COMMA) || parser.check(TokenType.RIGHT_PAREN) ||
                        parser.checkKeyword("DEFAULT") || parser.check(TokenType.COLON_EQUALS) ||
                        parser.check(TokenType.LEFT_BRACKET);
                // A type whose name is two words has no parameter name in front of it: what looks
                // like the name is the first half of the type. Reading "double" as the name left
                // "precision" as the type, so the parameter could not be declared at all.
                if (!isTypeOnly && continuesMultiWordType(parser, firstIdent)) {
                    parser.pos = saved;
                    String typeName = parser.parseTypeName();
                    parsedParams.add(new CreateFunctionStmt.FuncParam(null, typeName, mode));
                } else if (isTypeOnly) {
                    paramName = null;
                    String typeName = firstIdent;
                    typeName = readTypeModifiers(typeName);
                    parsedParams.add(new CreateFunctionStmt.FuncParam(paramName, typeName, mode));
                } else if (parser.checkKeyword("OUT") || parser.checkIdentifier("OUT") ||
                        parser.checkKeyword("INOUT") || parser.checkIdentifier("INOUT")) {
                    paramName = firstIdent;
                    String actualMode = parser.advance().value().toUpperCase(java.util.Locale.ROOT);
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
                        defaultText.append(dt.sqlText());
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
        // A procedure answers with nothing, so its grammar has no RETURNS: PostgreSQL stops at
        // the type that follows one. Accepting it let a procedure be declared to return rows it
        // has no way of producing.
        if (isProcedure && parser.checkKeyword("RETURNS")) {
            Token after = parser.peekAt(1);
            throw ParseException.saying("syntax error at or near \"" + after.raw() + "\"",
                    after, "42601");
        }
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
        String language = null;
        boolean[] secDefRef = {false};
        boolean[] strictRef = {false};
        boolean[] leakproofRef = {false};
        String[] volatilityRef = {"VOLATILE"};
        String[] parallelRef = {null};
        double[] costRef = {-1};
        double[] rowsRef = {-1};
        String[] supportRef = {null};
        boolean[] windowRef = {false};
        java.util.Map<String, String> setClauses = new java.util.LinkedHashMap<>();
        boolean isAtomicBody = false;

        // The options are one list PostgreSQL reads in whatever order they were written, not an
        // AS clause and a LANGUAGE clause in either of two arrangements. Reading them as the
        // latter left anything spelled a third way — WINDOW ahead of LANGUAGE, say — sitting
        // unread after the statement, and the routine was created from what had been read so far.
        RoutineOptions opts = new RoutineOptions(isProcedure);
        int asItemCount = 0;
        boolean sqlStandardBody = false;
        Token languageToken = null;
        Token sqlBodyToken = null;
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
            Token at = parser.peek();
            if (parser.matchKeyword("AS")) {
                opts.take(RoutineOptions.AS, at);
                if (sqlStandardBody) {
                    throw ParseException.saying("duplicate function body specified", at, "42P13");
                }
                body = readFunctionBody();
                asItemCount = 1;
                // A C function names an object file and a link symbol; nothing else takes two.
                while (parser.match(TokenType.COMMA)) {
                    readFunctionBody();
                    asItemCount++;
                }
                continue;
            }
            if (parser.matchKeyword("LANGUAGE")) {
                opts.take(RoutineOptions.LANGUAGE, at);
                languageToken = at;
                language = parser.readIdentifierOrString();
                continue;
            }
            // A SQL-standard body — RETURN expr, or BEGIN ATOMIC ... END (PG 14+). It says by
            // itself that the language is SQL, which is why it needs no LANGUAGE clause.
            if (parser.checkKeyword("RETURN") || (parser.checkKeyword("BEGIN") && isBeginAtomic())) {
                if (body != null) {
                    throw ParseException.saying("duplicate function body specified", at, "42P13");
                }
                sqlBodyToken = at;
                if (parser.matchKeyword("RETURN")) {
                    body = readSqlStandardReturn();
                } else {
                    body = readBeginAtomicBody();
                    isAtomicBody = true;
                }
                sqlStandardBody = true;
                continue;
            }
            if (!parseOneAttribute(opts, secDefRef, strictRef, volatilityRef, leakproofRef,
                    setClauses, parallelRef, costRef, rowsRef, supportRef, windowRef)) {
                break;
            }
        }

        // What a definition must settle before it is one, in the order PostgreSQL settles it.
        if (language == null && !sqlStandardBody) {
            throw ParseException.saying("no language specified", parser.peek(), "42P13");
        }
        if (sqlStandardBody && language != null && !"sql".equalsIgnoreCase(language)) {
            throw ParseException.saying("inline SQL function body only valid for language SQL",
                    sqlBodyToken, "42P13");
        }
        if (sqlStandardBody) language = "sql";
        // Only the C language reads a second AS item, as the symbol inside the named object file.
        if (asItemCount > 1 && !"c".equalsIgnoreCase(language) && !"internal".equalsIgnoreCase(language)) {
            throw ParseException.saying("only one AS item needed for language \""
                    + language.toLowerCase(java.util.Locale.ROOT) + "\"", languageToken, "42P13");
        }

        CreateFunctionStmt result = new CreateFunctionStmt(name, schema, rawParams.toString().trim(), parsedParams,
                returnType, body != null ? body : "", language, orReplace, isProcedure, secDefRef[0], strictRef[0],
                leakproofRef[0], volatilityRef[0], setClauses.isEmpty() ? null : setClauses,
                parallelRef[0], costRef[0], rowsRef[0]);
        result.atomicBody = isAtomicBody;
        result.sqlStandardBody = sqlStandardBody;
        result.supportFunction = supportRef[0];
        result.bodyGiven = body != null;
        result.windowFunction = windowRef[0];
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
            // A CALL takes its arguments the way any routine call does: by position, by name with
            // => or :=, or as one array after VARIADIC. Reading them as plain expressions made
            // every named argument a syntax error at the arrow.
            args = parser.parseFunctionArgList();
        }
        parser.expect(TokenType.RIGHT_PAREN);
        // A CALL takes no clause after its arguments: no RETURNING, and nothing else either.
        parser.expectEndOfStatement();
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
            sb.append(t.sqlText());
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
            sb.append(t.sqlText());
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

    /**
     * Reads the one attribute the statement is sitting on, or answers false when what follows is
     * not an attribute at all. Each is recorded against {@code opts}, which is what refuses a
     * repeat and what refuses an attribute a procedure may not carry.
     */
    private boolean parseOneAttribute(RoutineOptions opts,
                                      boolean[] securityDefinerRef, boolean[] strictRef,
                                      String[] volatilityRef, boolean[] leakproofRef,
                                      java.util.Map<String, String> setClauses,
                                      String[] parallelRef, double[] costRef, double[] rowsRef,
                                      String[] supportRef, boolean[] windowRef) {
        Token t = parser.peek();
        if (t.type() != TokenType.KEYWORD) return false;
        String kw = t.value();
        if (kw.equals("IMMUTABLE") || kw.equals("STABLE") || kw.equals("VOLATILE")) {
            opts.take(RoutineOptions.VOLATILITY, t);
            parser.advance();
            volatilityRef[0] = kw;
            return true;
        }
        if (kw.equals("STRICT")) {
            opts.take(RoutineOptions.STRICT, t);
            parser.advance();
            strictRef[0] = true;
            return true;
        }
        if (kw.equals("CALLED")) {
            opts.take(RoutineOptions.STRICT, t);
            parser.advance();
            parser.matchKeyword("ON"); parser.matchKeyword("NULL"); parser.matchKeyword("INPUT");
            strictRef[0] = false;
            return true;
        }
        if (kw.equals("RETURNS")) {
            opts.take(RoutineOptions.STRICT, t);
            parser.advance();
            parser.matchKeyword("NULL"); parser.matchKeyword("ON");
            parser.matchKeyword("NULL"); parser.matchKeyword("INPUT");
            strictRef[0] = true;
            return true;
        }
        if (kw.equals("SECURITY")) {
            opts.take(RoutineOptions.SECURITY, t);
            parser.advance();
            securityDefinerRef[0] = "DEFINER".equalsIgnoreCase(parser.readIdentifier());
            return true;
        }
        if (kw.equals("LEAKPROOF")) {
            opts.take(RoutineOptions.LEAKPROOF, t);
            parser.advance();
            leakproofRef[0] = true;
            return true;
        }
        if (kw.equals("NOT") && parser.checkKeywordAt(1, "LEAKPROOF")) {
            opts.take(RoutineOptions.LEAKPROOF, t);
            parser.matchKeywords("NOT", "LEAKPROOF");
            leakproofRef[0] = false;
            return true;
        }
        if (kw.equals("COST")) {
            opts.take(RoutineOptions.COST, t);
            parser.advance();
            String costVal = parser.advance().value();
            try { costRef[0] = Double.parseDouble(costVal); } catch (NumberFormatException e) { /* ignore */ }
            return true;
        }
        if (kw.equals("ROWS")) {
            opts.take(RoutineOptions.ROWS, t);
            parser.advance();
            String rowsVal = parser.advance().value();
            try { rowsRef[0] = Double.parseDouble(rowsVal); } catch (NumberFormatException e) { /* ignore */ }
            return true;
        }
        if (kw.equals("PARALLEL")) {
            opts.take(RoutineOptions.PARALLEL, t);
            parser.advance();
            parallelRef[0] = parser.readIdentifier().toUpperCase(java.util.Locale.ROOT);
            return true;
        }
        if (kw.equals("SUPPORT")) {
            opts.take(RoutineOptions.SUPPORT, t);
            parser.advance();
            supportRef[0] = parser.readIdentifier();
            return true;
        }
        if (kw.equals("WINDOW")) {
            opts.take(RoutineOptions.WINDOW, t);
            parser.advance();
            windowRef[0] = true;
            return true;
        }
        if (kw.equals("TRANSFORM")) {
            opts.take(RoutineOptions.TRANSFORM, t);
            parser.advance();
            do {
                parser.matchKeyword("FOR"); parser.matchKeyword("TYPE");
                parser.parseTypeName();
            } while (parser.match(TokenType.COMMA));
            return true;
        }
        // SET names a parameter apiece rather than being one option, so it is the one that repeats.
        if (kw.equals("SET")) {
            parser.advance();
            String paramName = parser.readIdentifier();
            if (parser.matchKeyword("TO") || parser.match(TokenType.EQUALS)) {
                StringBuilder valBuf = new StringBuilder();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
                    Token next = parser.peek();
                    if (next.type() == TokenType.KEYWORD && isFunctionAttributeKeyword(next.value())) break;
                    if (next.type() == TokenType.KEYWORD && next.value().equals("AS")) break;
                    // A list separator belongs to the item before it: a search_path of two
                    // schemas is written "public, pg_temp", never "public , pg_temp".
                    if (valBuf.length() > 0 && next.type() != TokenType.COMMA) valBuf.append(" ");
                    valBuf.append(next.value());
                    parser.advance();
                }
                setClauses.put(paramName.toLowerCase(java.util.Locale.ROOT), valBuf.toString().trim());
            } else if (parser.matchKeywords("FROM", "CURRENT")) {
                setClauses.put(paramName.toLowerCase(java.util.Locale.ROOT), "FROM CURRENT");
            }
            return true;
        }
        return false;
    }

    private static boolean isFunctionAttributeKeyword(String kw) {
        return kw.equals("IMMUTABLE") || kw.equals("STABLE") || kw.equals("VOLATILE") ||
                kw.equals("STRICT") || kw.equals("SECURITY") || kw.equals("COST") ||
                kw.equals("PARALLEL") || kw.equals("CALLED") || kw.equals("RETURNS") ||
                kw.equals("ROWS") || kw.equals("SET") || kw.equals("LANGUAGE") ||
                kw.equals("LEAKPROOF") || kw.equals("SUPPORT") || kw.equals("NOT") ||
                kw.equals("WINDOW") || kw.equals("TRANSFORM");
    }
}
