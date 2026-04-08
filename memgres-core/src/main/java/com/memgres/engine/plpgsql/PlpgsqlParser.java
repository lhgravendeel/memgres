package com.memgres.engine.plpgsql;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.Lexer;
import com.memgres.engine.parser.Token;
import com.memgres.engine.parser.TokenType;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses PL/pgSQL function bodies into PlpgsqlStatement AST nodes.
 * Expressions are kept as raw text strings for variable substitution at runtime.
 */
public class PlpgsqlParser {

    private final List<Token> tokens;
    private int pos;

    public PlpgsqlParser(String body) {
        this.tokens = new Lexer(body).tokenize();
        this.pos = 0;
    }

    public static PlpgsqlStatement.Block parse(String body) {
        return new PlpgsqlParser(body).parseBlock();
    }

    // ---- Token navigation ----

    private Token peek() {
        return pos < tokens.size() ? tokens.get(pos) : new Token(TokenType.EOF, "", 0);
    }

    private Token advance() {
        return tokens.get(pos++);
    }

    private boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    private boolean check(TokenType type) {
        return peek().type() == type;
    }

    private boolean checkKw(String keyword) {
        Token t = peek();
        return (t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER)
                && t.value().equalsIgnoreCase(keyword);
    }

    private boolean matchKw(String keyword) {
        if (checkKw(keyword)) { advance(); return true; }
        return false;
    }

    private boolean match(TokenType type) {
        if (check(type)) { advance(); return true; }
        return false;
    }

    private String readIdent() {
        Token t = peek();
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER
                || t.type() == TokenType.KEYWORD) {
            advance();
            return t.value();
        }
        throw new RuntimeException("Expected identifier at position " + t.position() + ", found: " + t.value());
    }

    // ---- Block parsing ----

    public PlpgsqlStatement.Block parseBlock() {
        List<PlpgsqlStatement.VarDeclaration> declarations = new ArrayList<>();
        List<PlpgsqlStatement> body = new ArrayList<>();
        List<PlpgsqlStatement.ExceptionHandler> handlers = new ArrayList<>();

        // Skip optional label: <<label_name>>
        if (check(TokenType.SHIFT_LEFT)) {
            advance(); // consume <<
            readIdent(); // consume label name
            if (check(TokenType.SHIFT_RIGHT)) advance(); // consume >>
        }

        if (matchKw("DECLARE")) {
            declarations = parseDeclarations();
        }

        if (!matchKw("BEGIN")) {
            // If no BEGIN, try to parse statements until END or EOF
            body = parseStatements("END");
            return new PlpgsqlStatement.Block(declarations, body, handlers);
        }

        body = parseStatements("END", "EXCEPTION");

        if (matchKw("EXCEPTION")) {
            handlers = parseExceptionHandlers();
        }

        matchKw("END");
        // Consume optional label name after END (e.g., END my_block;)
        if (!check(TokenType.SEMICOLON) && !check(TokenType.EOF) && !isAtEnd()) {
            advance(); // consume label name
        }
        match(TokenType.SEMICOLON);

        return new PlpgsqlStatement.Block(declarations, body, handlers);
    }

    private List<PlpgsqlStatement.VarDeclaration> parseDeclarations() {
        List<PlpgsqlStatement.VarDeclaration> decls = new ArrayList<>();
        while (!isAtEnd() && !checkKw("BEGIN")) {
            if (check(TokenType.SEMICOLON)) { advance(); continue; }
            // Allow multiple DECLARE keywords (some codebases use repeated DECLARE sections)
            if (checkKw("DECLARE")) { advance(); continue; }

            String name = readIdent();

            if (checkKw("CURSOR")) {
                advance();
                matchKw("FOR");
                String cursorSql = collectUntilSemicolon();
                decls.add(new PlpgsqlStatement.VarDeclaration(name, "REFCURSOR", false, false, null, true, cursorSql));
                match(TokenType.SEMICOLON);
                continue;
            }

            boolean constant = matchKw("CONSTANT");
            String typeName = readTypeName();
            boolean notNull = false;
            if (matchKw("NOT")) { matchKw("NULL"); notNull = true; }

            String defaultExpr = null;
            if (matchKw("DEFAULT") || match(TokenType.COLON_EQUALS)) {
                defaultExpr = collectUntilSemicolon();
            }

            decls.add(new PlpgsqlStatement.VarDeclaration(name, typeName, constant, notNull, defaultExpr, false, null));
            if (!match(TokenType.SEMICOLON)) {
                // Missing semicolon after variable declaration
                if (!isAtEnd() && !checkKw("BEGIN")) {
                    throw new RuntimeException("missing semicolon after variable declaration at position " + peek().position());
                }
                if (checkKw("BEGIN")) {
                    throw new RuntimeException("missing semicolon after variable declaration before BEGIN");
                }
            }
        }
        return decls;
    }

    private String readTypeName() {
        StringBuilder sb = new StringBuilder();
        sb.append(readIdent());
        while (!isAtEnd()) {
            if (checkKw("VARYING")) { sb.append(" ").append(advance().value()); continue; }
            if (check(TokenType.LEFT_PAREN)) {
                sb.append("("); advance();
                while (!check(TokenType.RIGHT_PAREN) && !isAtEnd()) sb.append(advance().value());
                if (match(TokenType.RIGHT_PAREN)) sb.append(")");
                continue;
            }
            if (check(TokenType.LEFT_BRACKET)) {
                advance(); match(TokenType.RIGHT_BRACKET); sb.append("[]"); continue;
            }
            if (check(TokenType.DOT)) {
                advance(); sb.append(".").append(readIdent()); continue;
            }
            if (check(TokenType.PERCENT)) {
                advance(); sb.append("%").append(readIdent()); continue;
            }
            break;
        }
        return sb.toString();
    }

    // ---- Statement parsing ----

    private List<PlpgsqlStatement> parseStatements(String... terminators) {
        List<PlpgsqlStatement> stmts = new ArrayList<>();
        outer:
        while (!isAtEnd()) {
            for (String term : terminators) {
                if (checkKw(term)) break outer;
            }
            if (check(TokenType.SEMICOLON)) { advance(); continue; }
            PlpgsqlStatement stmt = parseOneStatement();
            if (stmt != null) stmts.add(stmt);
        }
        return stmts;
    }

    private PlpgsqlStatement parseOneStatement() {
        Token t = peek();

        // Label: <<label>>, can be SHIFT_LEFT token or two LESS_THAN tokens
        if (t.type() == TokenType.SHIFT_LEFT
                || (t.type() == TokenType.LESS_THAN && pos + 1 < tokens.size()
                && tokens.get(pos + 1).type() == TokenType.LESS_THAN)) {
            if (t.type() == TokenType.SHIFT_LEFT) { advance(); }
            else { advance(); advance(); }
            String label = readIdent();
            if (check(TokenType.SHIFT_RIGHT)) advance();
            else { if (check(TokenType.GREATER_THAN)) advance(); if (check(TokenType.GREATER_THAN)) advance(); }
            t = peek();
            if (t.type() == TokenType.KEYWORD) {
                switch (t.value().toUpperCase()) {
                    case "LOOP":
                        return parseLoop(label);
                    case "WHILE":
                        return parseWhile(label);
                    case "FOR":
                        return parseFor(label);
                    case "FOREACH":
                        return parseForeach(label);
                    case "DECLARE":
                    case "BEGIN":
                        return parseBlock();
                    default:
                        return parseAssignmentOrSql();
                }
            }
            return parseAssignmentOrSql();
        }

        if (t.type() == TokenType.KEYWORD) {
            switch (t.value().toUpperCase()) {
                case "IF":
                    return parseIf();
                case "LOOP":
                    return parseLoop(null);
                case "WHILE":
                    return parseWhile(null);
                case "FOR":
                    return parseFor(null);
                case "FOREACH":
                    return parseForeach(null);
                case "EXIT":
                    return parseExit();
                case "CONTINUE":
                    return parseContinue();
                case "RETURN":
                    return parseReturn();
                case "RAISE":
                    return parseRaise();
                case "PERFORM":
                    return parsePerform();
                case "EXECUTE":
                    return parseExecute();
                case "NULL": {
                    advance(); match(TokenType.SEMICOLON); return new PlpgsqlStatement.NullStmt(); 
                }
                case "BEGIN":
                    return parseBlock();
                case "DECLARE":
                    return parseBlock();
                case "GET":
                    return parseGetDiagnostics();
                case "OPEN":
                    return parseOpenCursor();
                case "FETCH":
                    return parseFetch();
                case "CLOSE":
                    return parseCloseCursor();
                case "SELECT":
                case "INSERT":
                case "UPDATE":
                case "DELETE":
                case "WITH":
                    return parseSqlStmt();
                default:
                    return parseAssignmentOrSql();
            }
        }

        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER) {
            return parseAssignmentOrSql();
        }

        advance();
        return null;
    }

    // ---- Control flow ----

    private PlpgsqlStatement parseIf() {
        matchKw("IF");
        String condition = collectUntilKeyword("THEN");
        matchKw("THEN");
        List<PlpgsqlStatement> thenBody = parseStatements("ELSIF", "ELSEIF", "ELSE", "END");
        List<PlpgsqlStatement.ElsifClause> elsifs = new ArrayList<>();

        while (checkKw("ELSIF") || checkKw("ELSEIF")) {
            advance();
            String elsifCond = collectUntilKeyword("THEN");
            matchKw("THEN");
            List<PlpgsqlStatement> elsifBody = parseStatements("ELSIF", "ELSEIF", "ELSE", "END");
            elsifs.add(new PlpgsqlStatement.ElsifClause(elsifCond, elsifBody));
        }

        List<PlpgsqlStatement> elseBody = Cols.listOf();
        if (matchKw("ELSE")) {
            elseBody = parseStatements("END");
        }

        matchKw("END");
        matchKw("IF");
        match(TokenType.SEMICOLON);

        return new PlpgsqlStatement.IfStmt(condition, thenBody, elsifs, elseBody);
    }

    private PlpgsqlStatement parseLoop(String label) {
        matchKw("LOOP");
        List<PlpgsqlStatement> body = parseStatements("END");
        matchKw("END");
        matchKw("LOOP");
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.LoopStmt(label, body);
    }

    private PlpgsqlStatement parseWhile(String label) {
        matchKw("WHILE");
        String condition = collectUntilKeyword("LOOP");
        matchKw("LOOP");
        List<PlpgsqlStatement> body = parseStatements("END");
        matchKw("END");
        matchKw("LOOP");
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.WhileStmt(label, condition, body);
    }

    private PlpgsqlStatement parseFor(String label) {
        matchKw("FOR");
        String varName = readIdent();
        matchKw("IN");

        boolean reverse = matchKw("REVERSE");

        // Check if this is a range FOR (look for ..) or query FOR
        if (isRangeFor()) {
            String lower = collectUntilDotDot();
            expect2Dots();
            String upper = collectUntilMulti("LOOP", "BY");
            String step = null;
            if (matchKw("BY")) {
                step = collectUntilKeyword("LOOP");
            }
            matchKw("LOOP");
            List<PlpgsqlStatement> body = parseStatements("END");
            matchKw("END");
            matchKw("LOOP");
            match(TokenType.SEMICOLON);
            return new PlpgsqlStatement.ForStmt(label, varName, lower, upper, step, reverse, body);
        } else {
            String sql = collectUntilKeyword("LOOP");
            matchKw("LOOP");
            List<PlpgsqlStatement> body = parseStatements("END");
            matchKw("END");
            matchKw("LOOP");
            match(TokenType.SEMICOLON);
            return new PlpgsqlStatement.ForQueryStmt(label, varName, sql, body);
        }
    }

    private boolean isRangeFor() {
        int saved = pos;
        int depth = 0;
        for (int i = saved; i < tokens.size(); i++) {
            Token t = tokens.get(i);
            if (t.type() == TokenType.EOF) break;
            if (t.type() == TokenType.KEYWORD && t.value().equals("LOOP") && depth == 0) break;
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) depth--;
            // Check for .. pattern (two consecutive dots)
            if (t.type() == TokenType.DOT && i + 1 < tokens.size()
                    && tokens.get(i + 1).type() == TokenType.DOT) {
                return true;
            }
        }
        return false;
    }

    private void expect2Dots() {
        if (check(TokenType.DOT)) {
            advance();
            if (check(TokenType.DOT)) {
                advance();
                return;
            }
        }
        throw new RuntimeException("Expected '..' in FOR range");
    }

    private PlpgsqlStatement parseForeach(String label) {
        matchKw("FOREACH");
        String varName = readIdent();
        if (matchKw("SLICE")) advance(); // skip number
        matchKw("IN");
        matchKw("ARRAY");
        String arrayExpr = collectUntilKeyword("LOOP");
        matchKw("LOOP");
        List<PlpgsqlStatement> body = parseStatements("END");
        matchKw("END");
        matchKw("LOOP");
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.ForeachStmt(label, varName, arrayExpr, body);
    }

    private PlpgsqlStatement parseExit() {
        matchKw("EXIT");
        String label = null;
        String whenCond = null;
        if (!check(TokenType.SEMICOLON) && !checkKw("WHEN") && !isAtEnd()) {
            if (peek().type() == TokenType.IDENTIFIER) label = readIdent();
        }
        if (matchKw("WHEN")) {
            whenCond = collectUntilSemicolon();
        }
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.ExitStmt(label, whenCond);
    }

    private PlpgsqlStatement parseContinue() {
        matchKw("CONTINUE");
        String label = null;
        String whenCond = null;
        if (!check(TokenType.SEMICOLON) && !checkKw("WHEN") && !isAtEnd()) {
            if (peek().type() == TokenType.IDENTIFIER) label = readIdent();
        }
        if (matchKw("WHEN")) {
            whenCond = collectUntilSemicolon();
        }
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.ContinueStmt(label, whenCond);
    }

    // ---- RETURN ----

    private PlpgsqlStatement parseReturn() {
        matchKw("RETURN");
        if (matchKw("NEXT")) {
            String value = collectUntilSemicolon();
            match(TokenType.SEMICOLON);
            return new PlpgsqlStatement.ReturnNextStmt(value);
        }
        if (matchKw("QUERY")) {
            String sql = collectUntilSemicolon();
            match(TokenType.SEMICOLON);
            return new PlpgsqlStatement.ReturnQueryStmt(sql);
        }
        if (check(TokenType.SEMICOLON)) {
            advance();
            return new PlpgsqlStatement.ReturnStmt(null);
        }
        String value = collectUntilSemicolon();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.ReturnStmt(value);
    }

    // ---- RAISE ----

    private PlpgsqlStatement parseRaise() {
        matchKw("RAISE");
        String level = "EXCEPTION";
        if (checkKw("NOTICE") || checkKw("WARNING") || checkKw("EXCEPTION")
                || checkKw("INFO") || checkKw("LOG") || checkKw("DEBUG")) {
            level = advance().value().toUpperCase();
        } else if (!check(TokenType.STRING_LITERAL) && !check(TokenType.SEMICOLON) && !checkKw("USING")) {
            // Named condition: RAISE division_by_zero; or RAISE unique_violation USING MESSAGE = '...'
            level = readIdent();
        }

        String format = null;
        List<String> args = new ArrayList<>();
        String errcode = null;
        String hint = null;

        if (check(TokenType.STRING_LITERAL)) {
            format = advance().value();
            while (match(TokenType.COMMA)) {
                if (checkKw("USING")) break;
                args.add(collectUntilMulti(",", ";", "USING"));
            }
        }

        if (matchKw("USING")) {
            while (!check(TokenType.SEMICOLON) && !isAtEnd()) {
                String key = readIdent();
                if (match(TokenType.EQUALS) || match(TokenType.COLON_EQUALS)) {
                    if (key.equalsIgnoreCase("ERRCODE")) {
                        if (check(TokenType.STRING_LITERAL)) {
                            errcode = advance().value();
                        } else {
                            errcode = readIdent();
                        }
                    } else if (key.equalsIgnoreCase("HINT")) {
                        if (check(TokenType.STRING_LITERAL)) {
                            hint = advance().value();
                        } else {
                            hint = collectUntilMulti(",", ";");
                        }
                    } else if (key.equalsIgnoreCase("MESSAGE")) {
                        if (check(TokenType.STRING_LITERAL)) {
                            format = advance().value();
                        } else {
                            format = collectUntilMulti(",", ";");
                        }
                    } else {
                        advance(); // skip value for other keys (DETAIL, COLUMN, etc.)
                    }
                }
                match(TokenType.COMMA);
            }
        }

        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.RaiseStmt(level, format, args, errcode, hint);
    }

    // ---- PERFORM ----

    private PlpgsqlStatement parsePerform() {
        matchKw("PERFORM");
        String sql = "SELECT " + collectUntilSemicolon();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.PerformStmt(sql);
    }

    // ---- EXECUTE dynamic SQL ----

    private PlpgsqlStatement parseExecute() {
        matchKw("EXECUTE");
        String sqlExpr = collectUntilMulti(";", "INTO", "USING");
        List<String> intoVars = null;
        boolean strict = false;
        List<String> usingExprs = new ArrayList<>();

        if (matchKw("INTO")) {
            if (matchKw("STRICT")) strict = true;
            intoVars = new ArrayList<>();
            intoVars.add(readIdent());
            while (match(TokenType.COMMA)) {
                intoVars.add(readIdent());
            }
        }
        if (matchKw("USING")) {
            do {
                usingExprs.add(collectUntilMulti(",", ";"));
            } while (match(TokenType.COMMA));
        }

        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.ExecuteStmt(sqlExpr, usingExprs, intoVars, strict);
    }

    // ---- SQL statements ----

    private PlpgsqlStatement parseSqlStmt() {
        String sql = collectUntilSemicolon();
        match(TokenType.SEMICOLON);

        List<String> intoVars = null;
        boolean strict = false;

        // Detect SELECT ... INTO [STRICT] var1[, var2, ...] ... FROM
        // Normalize whitespace for detection (newlines before INTO)
        String upper = sql.toUpperCase();
        if (upper.startsWith("SELECT")) {
            int intoIdx = -1;
            int intoEnd = -1;
            java.util.regex.Matcher intoMatcher = java.util.regex.Pattern.compile("\\sINTO\\s", java.util.regex.Pattern.CASE_INSENSITIVE).matcher(sql);
            if (intoMatcher.find()) { intoIdx = intoMatcher.start(); intoEnd = intoMatcher.end(); }
            if (intoIdx >= 0) {
                String afterInto = sql.substring(intoEnd).trim();
                if (afterInto.toUpperCase().startsWith("STRICT ")) {
                    strict = true;
                    afterInto = afterInto.substring(7).trim();
                }
                // Find FROM with any whitespace prefix
                java.util.regex.Matcher fromMatcher = java.util.regex.Pattern.compile("\\sFROM\\s", java.util.regex.Pattern.CASE_INSENSITIVE).matcher(afterInto);
                int fromIdx = fromMatcher.find() ? fromMatcher.start() : -1;
                if (fromIdx >= 0) {
                    String beforeFrom = afterInto.substring(0, fromIdx).trim();
                    // Parse comma-separated variable list
                    // Check if this looks like a variable list (identifiers separated by commas)
                    String[] parts = beforeFrom.split(",");
                    boolean allIdents = true;
                    List<String> varNames = new ArrayList<>();
                    for (String part : parts) {
                        String trimmed = part.trim();
                        if (trimmed.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                            varNames.add(trimmed);
                        } else {
                            allIdents = false;
                            break;
                        }
                    }
                    if (allIdents && !varNames.isEmpty()) {
                        intoVars = varNames;
                        sql = sql.substring(0, intoIdx) + " " + afterInto.substring(fromIdx);
                    } else {
                        // Single variable followed by extra expressions (old behavior)
                        int spaceIdx = beforeFrom.indexOf(' ');
                        if (spaceIdx > 0) {
                            intoVars = Cols.listOf(beforeFrom.substring(0, spaceIdx).trim());
                            String restExpr = beforeFrom.substring(spaceIdx).trim();
                            sql = sql.substring(0, intoIdx) + " " + restExpr + afterInto.substring(fromIdx);
                        } else {
                            intoVars = Cols.listOf(beforeFrom);
                            sql = sql.substring(0, intoIdx) + " " + afterInto.substring(fromIdx);
                        }
                    }
                } else {
                    // SELECT expr INTO var (no FROM)
                    String trimmedAfter = afterInto.trim();
                    // Parse comma-separated variable list for no-FROM case too
                    String[] parts = trimmedAfter.split(",");
                    boolean allIdents = true;
                    List<String> varNames = new ArrayList<>();
                    for (String part : parts) {
                        String trimmed = part.trim();
                        if (trimmed.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                            varNames.add(trimmed);
                        } else {
                            allIdents = false;
                            break;
                        }
                    }
                    if (allIdents && !varNames.isEmpty()) {
                        intoVars = varNames;
                        sql = sql.substring(0, intoIdx);
                    } else {
                        int spaceIdx = trimmedAfter.indexOf(' ');
                        if (spaceIdx > 0) {
                            intoVars = Cols.listOf(trimmedAfter.substring(0, spaceIdx).trim());
                            sql = sql.substring(0, intoIdx) + " " + trimmedAfter.substring(spaceIdx).trim();
                        } else {
                            intoVars = Cols.listOf(trimmedAfter);
                            sql = sql.substring(0, intoIdx);
                        }
                    }
                }
            }
        }

        // Handle INSERT/UPDATE/DELETE ... RETURNING col1[, col2] INTO var1[, var2]
        if (intoVars == null) {
            String upperSql = sql.toUpperCase();
            if (upperSql.startsWith("INSERT") || upperSql.startsWith("UPDATE") || upperSql.startsWith("DELETE")
                    || upperSql.startsWith("WITH")) {
                // Look for RETURNING ... INTO pattern
                java.util.regex.Matcher retIntoMatcher = java.util.regex.Pattern.compile(
                        "\\bRETURNING\\b(.+?)\\bINTO\\b(.+)$",
                        java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.DOTALL).matcher(sql);
                if (retIntoMatcher.find()) {
                    String returningCols = retIntoMatcher.group(1).trim();
                    String intoTargets = retIntoMatcher.group(2).trim();
                    // Check for STRICT
                    if (intoTargets.toUpperCase().startsWith("STRICT ")) {
                        strict = true;
                        intoTargets = intoTargets.substring(7).trim();
                    }
                    // Parse into variable list
                    String[] parts = intoTargets.split(",");
                    List<String> varNames = new ArrayList<>();
                    for (String part : parts) {
                        String trimmed = part.trim();
                        if (trimmed.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                            varNames.add(trimmed);
                        }
                    }
                    if (!varNames.isEmpty()) {
                        intoVars = varNames;
                        // Remove the INTO ... part, keep RETURNING clause in SQL
                        sql = sql.substring(0, retIntoMatcher.start()) + "RETURNING " + returningCols;
                    }
                }
            }
        }

        return new PlpgsqlStatement.SqlStmt(sql, intoVars, strict);
    }

    // ---- GET DIAGNOSTICS ----

    private PlpgsqlStatement parseGetDiagnostics() {
        matchKw("GET");
        matchKw("DIAGNOSTICS");
        List<PlpgsqlStatement.DiagItem> items = new ArrayList<>();
        do {
            String varName = readIdent();
            if (!match(TokenType.EQUALS)) match(TokenType.COLON_EQUALS);
            String itemName = readIdent();
            if (peek().type() == TokenType.IDENTIFIER || peek().type() == TokenType.KEYWORD) {
                // Handle multi-word like ROW_COUNT, but it's typically one token: ROW_COUNT
                // Actually ROW is a keyword and COUNT is a keyword, so this might be two tokens
                if (!check(TokenType.COMMA) && !check(TokenType.SEMICOLON) && !isAtEnd()) {
                    itemName += "_" + readIdent();
                }
            }
            items.add(new PlpgsqlStatement.DiagItem(varName, itemName.toUpperCase()));
        } while (match(TokenType.COMMA));
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.GetDiagnosticsStmt(items);
    }

    // ---- Cursors ----

    private PlpgsqlStatement parseOpenCursor() {
        matchKw("OPEN");
        String cursorName = readIdent();
        String sql = null;
        if (matchKw("FOR")) sql = collectUntilSemicolon();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.OpenCursorStmt(cursorName, sql);
    }

    private PlpgsqlStatement parseFetch() {
        matchKw("FETCH");
        matchKw("NEXT"); matchKw("FROM");
        String cursorName = readIdent();
        List<String> intoVars = null;
        if (matchKw("INTO")) {
            intoVars = new ArrayList<>();
            intoVars.add(readIdent());
            while (match(TokenType.COMMA)) {
                intoVars.add(readIdent());
            }
        }
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.FetchStmt(cursorName, intoVars);
    }

    private PlpgsqlStatement parseCloseCursor() {
        matchKw("CLOSE");
        String cursorName = readIdent();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.CloseCursorStmt(cursorName);
    }

    // ---- Assignment ----

    private PlpgsqlStatement parseAssignmentOrSql() {
        int saved = pos;
        StringBuilder target = new StringBuilder();
        target.append(readIdent());

        boolean qualified = false;
        while (check(TokenType.DOT)) {
            advance();
            target.append(".").append(readIdent());
            qualified = true;
        }

        if (match(TokenType.COLON_EQUALS)) {
            String value = collectUntilSemicolon();
            match(TokenType.SEMICOLON);
            return new PlpgsqlStatement.Assignment(target.toString(), value);
        }

        // Accept = for assignment (PG allows both := and = in PL/pgSQL)
        if (match(TokenType.EQUALS)) {
            String value = collectUntilSemicolon();
            match(TokenType.SEMICOLON);
            return new PlpgsqlStatement.Assignment(target.toString(), value);
        }

        pos = saved;
        return parseSqlStmt();
    }

    // ---- Exception handlers ----

    private List<PlpgsqlStatement.ExceptionHandler> parseExceptionHandlers() {
        List<PlpgsqlStatement.ExceptionHandler> handlers = new ArrayList<>();
        while (matchKw("WHEN")) {
            List<String> conditions = new ArrayList<>();
            conditions.add(readConditionName());
            while (matchKw("OR")) conditions.add(readConditionName());
            matchKw("THEN");
            List<PlpgsqlStatement> body = parseStatements("WHEN", "END");
            handlers.add(new PlpgsqlStatement.ExceptionHandler(conditions, body));
        }
        return handlers;
    }

    private String readConditionName() {
        StringBuilder sb = new StringBuilder();
        sb.append(readIdent());
        if (sb.toString().equalsIgnoreCase("SQLSTATE") && check(TokenType.STRING_LITERAL)) {
            sb.append(" ").append(advance().value());
        }
        return sb.toString();
    }

    // ---- Token collecting helpers ----

    private String collectUntilSemicolon() {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (!isAtEnd()) {
            Token t = peek();
            if (t.type() == TokenType.SEMICOLON && depth == 0) break;
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) depth--;
            appendToken(sb, t);
            advance();
        }
        return sb.toString().trim();
    }

    private String collectUntilKeyword(String keyword) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (!isAtEnd()) {
            Token t = peek();
            if (t.type() == TokenType.KEYWORD && t.value().equalsIgnoreCase(keyword) && depth == 0) break;
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) depth--;
            appendToken(sb, t);
            advance();
        }
        return sb.toString().trim();
    }

    private String collectUntilMulti(String... terminators) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (!isAtEnd()) {
            Token t = peek();
            if (depth == 0) {
                for (String term : terminators) {
                    if (term.equals(";") && t.type() == TokenType.SEMICOLON) return sb.toString().trim();
                    if (term.equals(",") && t.type() == TokenType.COMMA) return sb.toString().trim();
                    if (t.type() == TokenType.KEYWORD && t.value().equalsIgnoreCase(term)) return sb.toString().trim();
                }
            }
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) depth--;
            appendToken(sb, t);
            advance();
        }
        return sb.toString().trim();
    }

    private String collectUntilDotDot() {
        StringBuilder sb = new StringBuilder();
        while (!isAtEnd()) {
            Token t = peek();
            if (t.type() == TokenType.DOT && pos + 1 < tokens.size()
                    && tokens.get(pos + 1).type() == TokenType.DOT) break;
            appendToken(sb, t);
            advance();
        }
        return sb.toString().trim();
    }

    private void appendToken(StringBuilder sb, Token t) {
        if (!sb.isEmpty()) sb.append(" ");
        if (t.type() == TokenType.STRING_LITERAL) {
            sb.append("'").append(t.value().replace("'", "''")).append("'");
        } else if (t.type() == TokenType.DOLLAR_STRING_LITERAL) {
            // Preserve dollar-quoted strings as string literals
            sb.append("'").append(t.value().replace("'", "''")).append("'");
        } else {
            sb.append(t.value());
        }
    }
}
