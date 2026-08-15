package com.memgres.engine.plpgsql;

import com.memgres.engine.MemgresException;
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
    /** Parameters of each cursor declared so far, so an OPEN can be checked against them. */
    private final java.util.Map<String, List<String>> cursorParams = new java.util.HashMap<>();

    public PlpgsqlParser(String body) {
        this.tokens = new Lexer(body).tokenize();
        this.pos = 0;
    }

    public static PlpgsqlStatement.Block parse(String body) {
        PlpgsqlParser parser = new PlpgsqlParser(body);
        try {
            return parser.parseBlock();
        } catch (MemgresException e) {
            throw e;
        } catch (RuntimeException e) {
            // Whatever way the parser ran off the rails, the client sent a body that does not
            // parse. Reporting that as a syntax error at the token it stopped on says the same
            // thing PostgreSQL does; reporting it as an internal fault does not.
            throw syntaxError(parser.peek());
        }
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

    /**
     * A loop or block label is an ordinary name, so it may be spelled with a word the lexer also
     * treats as a keyword — {@code outer} and {@code end} are common choices. Anything that reads
     * as a name is a label here; EXIT and CONTINUE have already ruled out WHEN and the semicolon.
     */
    private boolean isLabelToken(Token t) {
        return t.type() == TokenType.IDENTIFIER
                || t.type() == TokenType.QUOTED_IDENTIFIER
                || t.type() == TokenType.KEYWORD;
    }

    private String readIdent() {
        Token t = peek();
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER
                || t.type() == TokenType.KEYWORD) {
            advance();
            // A keyword's token carries its upper-case form, which is not the name of anything:
            // the column in "v t.name%TYPE" is called name, not NAME.
            return t.type() == TokenType.KEYWORD ? t.value().toLowerCase() : t.value();
        }
        throw syntaxError(t);
    }

    /**
     * The same, but the spelling as the author wrote it. The lexer folds an unquoted word, and a
     * message about a word PostgreSQL did not recognise quotes it back the way it was typed — so
     * the reader can find it in their own source.
     */
    private String readIdentAsWritten() {
        Token t = peek();
        String raw = t.raw();
        readIdent();
        return raw;
    }

    /**
     * A body that does not parse is the writer's mistake, not the engine's. PostgreSQL reports it
     * as a syntax error naming the token it stopped at; reporting it as an internal error instead
     * tells the caller nothing they can act on and hides a plain typo behind a fault report.
     */
    static MemgresException syntaxError(Token t) {
        if (t == null || t.type() == TokenType.EOF) {
            return new MemgresException("syntax error at end of input", "42601");
        }
        return new MemgresException("syntax error at or near \"" + t.raw() + "\"", "42601");
    }

    // ---- Block parsing ----

    public PlpgsqlStatement.Block parseBlock() {
        return parseBlock(null);
    }

    /**
     * @param outerLabel label already consumed by the caller, when the {@code <<label>>} was read
     *                   before we knew a block rather than a loop followed it
     */
    public PlpgsqlStatement.Block parseBlock(String outerLabel) {
        List<PlpgsqlStatement.VarDeclaration> declarations = new ArrayList<>();
        List<PlpgsqlStatement> body = new ArrayList<>();
        List<PlpgsqlStatement.ExceptionHandler> handlers = new ArrayList<>();

        String variableConflict = parsePragmas();

        String label = outerLabel;
        // Optional label: <<label_name>>
        if (check(TokenType.SHIFT_LEFT)) {
            advance(); // consume <<
            label = readIdent();
            if (check(TokenType.SHIFT_RIGHT)) advance(); // consume >>
        }

        if (matchKw("DECLARE")) {
            declarations = parseDeclarations();
        }

        if (!matchKw("BEGIN")) {
            // If no BEGIN, try to parse statements until END or EOF
            body = parseStatements("END");
            PlpgsqlStatement.Block noBegin = new PlpgsqlStatement.Block(label, declarations, body, handlers);
            noBegin.variableConflict = variableConflict;
            return noBegin;
        }

        body = parseStatements("END", "EXCEPTION");

        if (matchKw("EXCEPTION")) {
            handlers = parseExceptionHandlers();
        }

        if (matchKw("END")) {
            // Optional label name after END (e.g., END my_block;), which PG checks against the
            // block's own label rather than skipping.
            if (isLabelToken(peek())) {
                checkEndLabel(readIdent(), label);
            }
        }
        match(TokenType.SEMICOLON);

        PlpgsqlStatement.Block block = new PlpgsqlStatement.Block(label, declarations, body, handlers);
        block.variableConflict = variableConflict;
        return block;
    }

    /**
     * Read the {@code #option value} pragmas a body may open with. They are compiler directives
     * rather than statements, so they precede even the block label.
     */
    private String parsePragmas() {
        String variableConflict = null;
        while (check(TokenType.HASH)) {
            advance();
            String option = readIdent();
            if ("variable_conflict".equalsIgnoreCase(option)) {
                String value = peek().value();
                if (!"error".equalsIgnoreCase(value) && !"use_variable".equalsIgnoreCase(value)
                        && !"use_column".equalsIgnoreCase(value)) {
                    throw new MemgresException("syntax error at or near \"" + value + "\"", "42601");
                }
                advance();
                variableConflict = value.toLowerCase();
            } else if ("print_strict_params".equalsIgnoreCase(option)) {
                String value = peek().value();
                if (!"on".equalsIgnoreCase(value) && !"off".equalsIgnoreCase(value)) {
                    throw new MemgresException("syntax error at or near \"" + value + "\"", "42601");
                }
                advance();
            } else {
                throw new MemgresException("syntax error at or near \"" + option + "\"", "42601");
            }
        }
        return variableConflict;
    }

    /**
     * PG matches the name after END against the construct's own label; a name on an unlabeled
     * construct is an error rather than something to skip.
     */
    private void checkEndLabel(String endLabel, String openLabel) {
        if (openLabel == null) {
            throw new MemgresException(
                    "end label \"" + endLabel + "\" specified for unlabeled block", "42601");
        }
        if (!endLabel.equalsIgnoreCase(openLabel)) {
            throw new MemgresException("end label \"" + endLabel
                    + "\" differs from block's label \"" + openLabel + "\"", "42601");
        }
    }

    /** Consume {@code END LOOP [label]} and check the label against the loop's own. */
    private void endLoop(String label) {
        matchKw("END");
        matchKw("LOOP");
        if (isLabelToken(peek())) {
            checkEndLabel(readIdent(), label);
        }
        match(TokenType.SEMICOLON);
    }

    private List<PlpgsqlStatement.VarDeclaration> parseDeclarations() {
        List<PlpgsqlStatement.VarDeclaration> decls = new ArrayList<>();
        while (!isAtEnd() && !checkKw("BEGIN")) {
            if (check(TokenType.SEMICOLON)) { advance(); continue; }
            // Allow multiple DECLARE keywords (some codebases use repeated DECLARE sections)
            if (checkKw("DECLARE")) { advance(); continue; }

            String name = readIdent();

            // PL/pgSQL: reject sqlstate and sqlerrm as user variable names (they are implicit CONSTANT variables)
            if ("sqlstate".equalsIgnoreCase(name) || "sqlerrm".equalsIgnoreCase(name)) {
                throw new MemgresException(
                        "variable \"" + name + "\" is declared CONSTANT", "42601");
            }

            // SCROLL and NO SCROLL only say whether the cursor may be read backwards; every
            // memgres cursor materializes its rows, so both spellings are simply recognized.
            boolean sawScroll = false;
            if (checkKw("SCROLL")) { advance(); sawScroll = true; }
            else if (checkKw("NO") && pos + 1 < tokens.size()
                    && "SCROLL".equalsIgnoreCase(tokens.get(pos + 1).value())) {
                advance(); advance(); sawScroll = true;
            }

            if (checkKw("CURSOR")) {
                advance();
                // PG 18: CURSOR WITH HOLD is not valid in PL/pgSQL
                if (checkKw("WITH")) {
                    throw new MemgresException("syntax error at or near \"WITH\"", "42601");
                }
                List<String> cursorNames = new ArrayList<>();
                if (check(TokenType.LEFT_PAREN)) {
                    advance();
                    while (!check(TokenType.RIGHT_PAREN) && !isAtEnd()) {
                        cursorNames.add(readIdent());
                        readTypeName();
                        if (!match(TokenType.COMMA)) break;
                    }
                    match(TokenType.RIGHT_PAREN);
                }
                matchKw("FOR");
                String cursorSql = collectUntilSemicolon();
                decls.add(new PlpgsqlStatement.VarDeclaration(name, "REFCURSOR", false, false, null,
                        true, cursorSql, cursorNames));
                this.cursorParams.put(name.toLowerCase(), cursorNames);
                match(TokenType.SEMICOLON);
                continue;
            }
            if (sawScroll) {
                throw new MemgresException("syntax error at or near \"" + peek().value() + "\"", "42601");
            }

            // ALIAS FOR gives an existing variable — usually a positional parameter — a second
            // name, and declares nothing of its own
            if (checkKw("ALIAS")) {
                advance();
                if (!matchKw("FOR")) {
                    throw new MemgresException(
                            "syntax error at or near \"" + peek().value() + "\"", "42601");
                }
                String target = check(TokenType.PARAM) ? advance().value() : readIdent();
                PlpgsqlStatement.VarDeclaration alias = new PlpgsqlStatement.VarDeclaration(
                        name, null, false, false, null, false, null);
                alias.aliasFor = target;
                decls.add(alias);
                if (!match(TokenType.SEMICOLON) && !isAtEnd()) {
                    throw new MemgresException(
                            "syntax error at or near \"" + peek().value() + "\"", "42601");
                }
                continue;
            }

            boolean constant = matchKw("CONSTANT");
            String typeName = readTypeName();
            boolean notNull = false;
            if (matchKw("NOT")) { matchKw("NULL"); notNull = true; }

            String defaultExpr = null;
            // PL/pgSQL accepts =, := and DEFAULT interchangeably as the initialiser
            if (matchKw("DEFAULT") || match(TokenType.COLON_EQUALS) || match(TokenType.EQUALS)) {
                defaultExpr = collectUntilSemicolon();
            }

            decls.add(new PlpgsqlStatement.VarDeclaration(name, typeName, constant, notNull, defaultExpr, false, null));
            if (!match(TokenType.SEMICOLON) && !isAtEnd()) {
                throw new MemgresException(
                        "syntax error at or near \"" + peek().value() + "\"", "42601");
            }
        }
        return decls;
    }

    private String readTypeName() {
        StringBuilder sb = new StringBuilder();
        String first = readIdent();
        sb.append(first);
        // A declared interval may carry a field qualifier, exactly as one in a column definition,
        // a cast, a domain or a parameter list may. Stopping at the field word left the qualifier
        // standing where the parser expected a semicolon, so DECLARE v interval day to second(2)
        // was a syntax error in the one place the type could not be written.
        if ("interval".equalsIgnoreCase(first) && checkIntervalFieldWord()) {
            readIntervalQualifier(sb);
            // A qualifier takes the whole modifier, so nothing but an array suffix may follow.
            while (check(TokenType.LEFT_BRACKET)) {
                advance();
                match(TokenType.RIGHT_BRACKET);
                sb.append("[]");
            }
            return sb.toString();
        }
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

    /** True when the next word names an interval field, so it belongs to the type and not after it. */
    private boolean checkIntervalFieldWord() {
        Token t = peek();
        if (t.type() != TokenType.KEYWORD && t.type() != TokenType.IDENTIFIER) return false;
        String v = t.value().toUpperCase(java.util.Locale.ROOT);
        return "YEAR".equals(v) || "MONTH".equals(v) || "DAY".equals(v)
                || "HOUR".equals(v) || "MINUTE".equals(v) || "SECOND".equals(v);
    }

    /**
     * The field qualifier written after {@code interval}, in the one spelling the rest of the
     * engine reads it back in — {@code interval day to second(2)}, lower case, with the precision
     * SECOND alone may carry.
     */
    private void readIntervalQualifier(StringBuilder sb) {
        String first = advance().value().toLowerCase(java.util.Locale.ROOT);
        sb.append(' ').append(first);
        if ("second".equals(first)) { readIntervalPrecision(sb); return; }
        if (!checkKw("TO")) return;
        String[] allowed;
        if ("year".equals(first)) allowed = new String[]{"month"};
        else if ("day".equals(first)) allowed = new String[]{"hour", "minute", "second"};
        else if ("hour".equals(first)) allowed = new String[]{"minute", "second"};
        else if ("minute".equals(first)) allowed = new String[]{"second"};
        else allowed = new String[0];
        if (allowed.length == 0) throw syntaxError(peek());
        advance();  // TO
        Token endTok = peek();
        String end = checkIntervalFieldWord() ? endTok.value().toLowerCase(java.util.Locale.ROOT) : null;
        boolean ok = false;
        for (int i = 0; i < allowed.length; i++) {
            if (allowed[i].equals(end)) ok = true;
        }
        if (!ok) throw syntaxError(endTok);
        advance();
        sb.append(" to ").append(end);
        if ("second".equals(end)) readIntervalPrecision(sb);
    }

    /** The optional precision SECOND takes. */
    private void readIntervalPrecision(StringBuilder sb) {
        if (!check(TokenType.LEFT_PAREN)) return;
        advance();
        sb.append('(').append(advance().value());
        if (!match(TokenType.RIGHT_PAREN)) throw syntaxError(peek());
        sb.append(')');
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
                        return parseBlock(label);
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
                case "CASE":
                    return parseCase();
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
                    advance(); expectSemicolon(); return new PlpgsqlStatement.NullStmt(); 
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
                case "MOVE":
                    return parseFetch();
                case "CLOSE":
                    return parseCloseCursor();
                case "COMMIT":
                    return parseCommit();
                case "ROLLBACK":
                    return parseRollback();
                case "ABORT":
                    return parseAbort();
                case "SAVEPOINT":
                    // SAVEPOINT reads as an ordinary SQL statement and is refused when it runs,
                    // the way every other transaction command a body cannot give is. Refusing it
                    // at compile time named it a syntax error, which is not what it is.
                    return parseSqlStmt();
                case "ASSERT":
                    return parseAssert();
                case "CALL":
                    return parseSqlStmt();
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
        // An IF is closed by END IF. A bare END is the enclosing block's, and letting the IF take
        // it left the block with no end of its own and the body after it read as part of the IF.
        if (!matchKw("IF")) throw syntaxError(peek());
        match(TokenType.SEMICOLON);

        return new PlpgsqlStatement.IfStmt(condition, thenBody, elsifs, elseBody);
    }

    private PlpgsqlStatement parseCase() {
        matchKw("CASE");
        // Determine if this is a simple CASE (has search expression) or searched CASE
        // Simple CASE: CASE expr WHEN val THEN ... END CASE;
        // Searched CASE: CASE WHEN bool_expr THEN ... END CASE;
        String searchExpr = null;
        if (!checkKw("WHEN")) {
            searchExpr = collectUntilKeyword("WHEN");
        }

        List<PlpgsqlStatement.CaseWhenClause> whenClauses = new ArrayList<>();
        while (checkKw("WHEN")) {
            advance(); // consume WHEN
            String whenExpr = collectUntilKeyword("THEN");
            matchKw("THEN");
            List<PlpgsqlStatement> body = parseStatements("WHEN", "ELSE", "END");
            whenClauses.add(new PlpgsqlStatement.CaseWhenClause(whenExpr, body));
        }

        List<PlpgsqlStatement> elseBody = Cols.listOf();
        if (matchKw("ELSE")) {
            elseBody = parseStatements("END");
        }

        matchKw("END");
        matchKw("CASE");
        match(TokenType.SEMICOLON);

        return new PlpgsqlStatement.CaseStmt(searchExpr, whenClauses, elseBody);
    }

    private PlpgsqlStatement parseLoop(String label) {
        matchKw("LOOP");
        List<PlpgsqlStatement> body = parseStatements("END");
        endLoop(label);
        return new PlpgsqlStatement.LoopStmt(label, body);
    }

    private PlpgsqlStatement parseWhile(String label) {
        matchKw("WHILE");
        String condition = collectUntilKeyword("LOOP");
        matchKw("LOOP");
        List<PlpgsqlStatement> body = parseStatements("END");
        endLoop(label);
        return new PlpgsqlStatement.WhileStmt(label, condition, body);
    }

    private PlpgsqlStatement parseFor(String label) {
        matchKw("FOR");
        // Read comma-separated loop variable names (PG supports FOR k, v IN ...)
        List<String> varNames = new ArrayList<>();
        varNames.add(readIdent());
        while (match(TokenType.COMMA)) {
            varNames.add(readIdent());
        }
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
            endLoop(label);
            return new PlpgsqlStatement.ForStmt(label, varNames.get(0), lower, upper, step, reverse, body);
        } else if (checkKw("EXECUTE")) {
            // FOR rec IN EXECUTE 'sql' [USING expr, ...] LOOP ... END LOOP
            matchKw("EXECUTE");
            String sqlExpr = collectUntilMulti("LOOP", "USING");
            List<String> usingExprs = new ArrayList<>();
            if (matchKw("USING")) {
                do {
                    usingExprs.add(collectUntilMulti(",", "LOOP"));
                } while (match(TokenType.COMMA));
            }
            matchKw("LOOP");
            List<PlpgsqlStatement> body = parseStatements("END");
            endLoop(label);
            return new PlpgsqlStatement.ForExecuteStmt(label, varNames, sqlExpr, usingExprs, body);
        } else {
            String sql = collectUntilKeyword("LOOP");
            matchKw("LOOP");
            List<PlpgsqlStatement> body = parseStatements("END");
            endLoop(label);
            return new PlpgsqlStatement.ForQueryStmt(label, varNames, sql, body);
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
        throw syntaxError(peek());
    }

    private PlpgsqlStatement parseForeach(String label) {
        matchKw("FOREACH");
        String varName = readIdent();
        int sliceDepth = 0;
        if (matchKw("SLICE")) {
            String sliceVal = advance().value();
            try {
                sliceDepth = Integer.parseInt(sliceVal);
                if (sliceDepth < 0) {
                    throw new MemgresException("FOREACH SLICE depth must not be negative", "42601");
                }
            } catch (NumberFormatException e) {
                throw new MemgresException("syntax error at or near \"SLICE\"", "42601");
            }
        }
        matchKw("IN");
        matchKw("ARRAY");
        String arrayExpr = collectUntilKeyword("LOOP");
        matchKw("LOOP");
        List<PlpgsqlStatement> body = parseStatements("END");
        endLoop(label);
        return new PlpgsqlStatement.ForeachStmt(label, varName, sliceDepth, arrayExpr, body);
    }

    private PlpgsqlStatement parseExit() {
        matchKw("EXIT");
        String label = null;
        String whenCond = null;
        if (!check(TokenType.SEMICOLON) && !checkKw("WHEN") && !isAtEnd()) {
            if (isLabelToken(peek())) label = readIdent();
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
            if (isLabelToken(peek())) label = readIdent();
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
            if (matchKw("EXECUTE")) {
                String sqlExpr = collectUntilMulti(";", "USING");
                List<String> usingExprs = new ArrayList<>();
                if (matchKw("USING")) {
                    do {
                        usingExprs.add(collectUntilMulti(",", ";"));
                    } while (match(TokenType.COMMA));
                }
                match(TokenType.SEMICOLON);
                return new PlpgsqlStatement.ReturnQueryExecuteStmt(sqlExpr, usingExprs);
            }
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

    // ---- ASSERT ----

    private PlpgsqlStatement parseAssert() {
        matchKw("ASSERT");
        String condition = collectUntilMulti(";", ",");
        String message = null;
        if (match(TokenType.COMMA)) {
            message = collectUntilSemicolon();
        }
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.AssertStmt(condition, message);
    }

    // ---- RAISE ----

    /**
     * Whether the next token is a RAISE format string. Dollar quoting is just another way to
     * write one — {@code raise notice $x$a % b$x$, 1} says exactly what the quoted form says —
     * so a body that uses it must not be read as naming a condition instead.
     */
    private boolean isFormatLiteral() {
        return check(TokenType.STRING_LITERAL) || check(TokenType.DOLLAR_STRING_LITERAL);
    }

    private PlpgsqlStatement parseRaise() {
        matchKw("RAISE");
        String level = "EXCEPTION";
        String condition = null;
        String errcode = null;
        boolean levelGiven = false;
        if (checkKw("NOTICE") || checkKw("WARNING") || checkKw("EXCEPTION")
                || checkKw("INFO") || checkKw("LOG") || checkKw("DEBUG")) {
            level = advance().value().toUpperCase();
            levelGiven = true;
        }
        if (checkKw("SQLSTATE")) {
            // RAISE SQLSTATE 'XXXXX' ...
            advance(); // consume SQLSTATE
            if (check(TokenType.STRING_LITERAL)) {
                errcode = "'" + advance().value().replace("'", "''") + "'";
            }
        } else if (!isFormatLiteral() && !check(TokenType.SEMICOLON) && !checkKw("USING")) {
            // Named condition, with or without a level of its own:
            //   RAISE division_by_zero;  RAISE NOTICE unique_violation;
            if (levelGiven) condition = readIdent();
            else level = readIdent();
        }

        // A level says how loud the message is, not what it says, so a format string, a condition,
        // an SQLSTATE or a USING clause still has to follow it. Only a bare RAISE stands alone,
        // and that one re-raises whatever an exception handler caught.
        if (levelGiven && condition == null && errcode == null && check(TokenType.SEMICOLON)) {
            throw syntaxError(peek());
        }

        String format = null;
        String messageExpr = null;
        List<String> args = new ArrayList<>();
        String hint = null;
        String detail = null;
        String column = null;
        String constraint = null;
        String datatype = null;
        String table = null;
        String schema = null;

        if (isFormatLiteral()) {
            format = advance().value();
            while (match(TokenType.COMMA)) {
                if (checkKw("USING")) break;
                args.add(collectUntilMulti(",", ";", "USING"));
            }
        }

        String duplicateOption = null;
        if (matchKw("USING")) {
            java.util.Set<String> seen = new java.util.HashSet<>();
            // A format string is itself the message, so USING MESSAGE would be a second one
            if (format != null) seen.add("MESSAGE");
            while (!check(TokenType.SEMICOLON) && !isAtEnd()) {
                String key = readIdentAsWritten();
                if (match(TokenType.EQUALS) || match(TokenType.COLON_EQUALS)) {
                    // The value is an ordinary expression, so it is kept as text and evaluated
                    // when the RAISE runs rather than being read as a bare literal.
                    String val = collectUntilMulti(",", ";");
                    // PG reads the whole statement before it complains that an option was given
                    // twice, so the complaint belongs to the run rather than to the compile
                    if (!seen.add(key.toUpperCase()) && duplicateOption == null) {
                        duplicateOption = key.toUpperCase();
                    }
                    switch (key.toUpperCase()) {
                        case "ERRCODE": errcode = val; break;
                        case "MESSAGE": messageExpr = val; break;
                        case "HINT": hint = val; break;
                        case "DETAIL": detail = val; break;
                        case "COLUMN": column = val; break;
                        case "CONSTRAINT": constraint = val; break;
                        case "DATATYPE": datatype = val; break;
                        case "TABLE": table = val; break;
                        case "SCHEMA": schema = val; break;
                        default:
                            // PostgreSQL echoes the word as it was written, which is what a
                            // reader is looking for in their own source.
                            throw new MemgresException("unrecognized RAISE statement option at or"
                                    + " near \"" + key + "\"", "42601");
                    }
                }
                match(TokenType.COMMA);
            }
        }

        match(TokenType.SEMICOLON);
        PlpgsqlStatement.RaiseStmt raise = new PlpgsqlStatement.RaiseStmt(level, format, args,
                errcode, hint, detail, column, constraint, datatype, table, schema);
        raise.messageExpr = messageExpr;
        raise.condition = condition;
        raise.duplicateOption = duplicateOption;
        return raise;
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

        // PG accepts the INTO and USING clauses in either order:
        //   EXECUTE sql INTO target USING args
        //   EXECUTE sql USING args INTO target
        for (int guard = 0; guard < 2; guard++) {
            if (intoVars == null && matchKw("INTO")) {
                if (matchKw("STRICT")) strict = true;
                intoVars = new ArrayList<>();
                intoVars.add(readIntoTarget());
                while (match(TokenType.COMMA)) {
                    intoVars.add(readIntoTarget());
                }
            } else if (usingExprs.isEmpty() && matchKw("USING")) {
                // Stop each USING expression at a following INTO so the reversed order works.
                do {
                    usingExprs.add(collectUntilMulti(",", ";", "INTO"));
                } while (match(TokenType.COMMA));
            } else {
                break;
            }
        }

        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.ExecuteStmt(sqlExpr, usingExprs, intoVars, strict);
    }

    /**
     * What an INTO clause writes to, which may be a field of a record rather than a variable of
     * its own: PostgreSQL takes {@code INTO r.a} wherever it takes {@code INTO r}, and a trigger
     * routine writing {@code INTO NEW.i} has rewritten the row exactly as an assignment would.
     */
    private String readIntoTarget() {
        StringBuilder target = new StringBuilder(readIdent());
        while (check(TokenType.DOT)) {
            advance();
            target.append('.').append(readIdent());
        }
        return target.toString();
    }

    // ---- SQL statements ----

    /**
     * Close up the spaces the statement text was rebuilt with around a dot, so that an INTO
     * target naming a field — {@code INTO r.a} — is read as one name rather than a name followed
     * by something else.
     */
    private static String joinFieldPaths(String text) {
        return text.replaceAll("\\s*\\.\\s*", ".");
    }

    private PlpgsqlStatement parseSqlStmt() {
        CollectedSql collected = collectSqlUntilSemicolon();
        String sql = collected.text;
        match(TokenType.SEMICOLON);

        List<String> intoVars = null;
        boolean strict = false;

        // PL/pgSQL owns the INTO clause: it has to be taken out of the statement before the SQL
        // parser is given it. Where the clause is was settled while the text was rebuilt, token
        // by token, because the finished text cannot tell the keyword INTO from the same letters
        // inside a string literal -- SELECT ' into me ' INTO v holds two and means one.
        String upper = sql.toUpperCase();
        // In a CTE query the clause belongs to the final SELECT, so the search starts there and
        // the INTO of a WITH ... INSERT INTO is left where it stands.
        int selectStart = upper.startsWith("WITH") ? collected.lastSelect : 0;
        if (upper.startsWith("SELECT") || (upper.startsWith("WITH") && selectStart > 0)) {
            int[] into = collected.firstIntoAfter(selectStart);
            if (into != null) {
                int fromIdx = collected.firstFromAfter(into[1]);
                String targets = (fromIdx >= 0 ? sql.substring(into[1], fromIdx)
                        : sql.substring(into[1])).trim();
                if (targets.toUpperCase().startsWith("STRICT ")) {
                    strict = true;
                    targets = targets.substring(7).trim();
                }
                targets = joinFieldPaths(targets);
                // What follows the target list belongs to the statement and goes back into it.
                String tail = fromIdx >= 0 ? sql.substring(fromIdx) : "";
                boolean allIdents = true;
                List<String> varNames = new ArrayList<>();
                for (String part : targets.split(",")) {
                    String trimmed = part.trim();
                    // A target may be a field of a composite variable as well as a plain name
                    if (trimmed.matches("[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*")) {
                        varNames.add(trimmed);
                    } else {
                        allIdents = false;
                        break;
                    }
                }
                if (allIdents && !varNames.isEmpty()) {
                    intoVars = varNames;
                    sql = sql.substring(0, into[0]) + tail;
                } else {
                    // Single variable followed by extra expressions (old behavior)
                    int spaceIdx = targets.indexOf(' ');
                    if (spaceIdx > 0) {
                        intoVars = Cols.listOf(targets.substring(0, spaceIdx).trim());
                        sql = sql.substring(0, into[0]) + " " + targets.substring(spaceIdx).trim() + tail;
                    } else {
                        intoVars = Cols.listOf(targets);
                        sql = sql.substring(0, into[0]) + tail;
                    }
                }
            }
        }

        // Handle SHOW param INTO var -- the clause is the last INTO in the statement, and one
        // variable name is all that may follow it.
        if (intoVars == null && upper.startsWith("SHOW")) {
            int[] into = collected.lastInto();
            if (into != null) {
                String target = sql.substring(into[1]).trim();
                if (target.matches("\\w+")) {
                    intoVars = Cols.listOf(target);
                    sql = sql.substring(0, into[0]).trim();
                }
            }
        }

        // Handle INSERT/UPDATE/DELETE ... RETURNING col1[, col2] INTO var1[, var2]
        if (intoVars == null) {
            String upperSql = sql.toUpperCase();
            if (upperSql.startsWith("INSERT") || upperSql.startsWith("UPDATE") || upperSql.startsWith("DELETE")
                    || upperSql.startsWith("WITH")) {
                // The clause is the first INTO token after RETURNING. Taking the first INTO in
                // the text instead ended the list at a literal that spelled it, and the statement
                // then went to the SQL parser still carrying an INTO it cannot read.
                int[] into = collected.returning == null ? null
                        : collected.firstIntoAfter(collected.returning[1]);
                if (into != null) {
                    String returningCols = sql.substring(collected.returning[1], into[0]).trim();
                    String intoTargets = sql.substring(into[1]).trim();
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
                        // A target may be a field of a composite variable as well as a plain name
                        if (trimmed.matches("[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*")) {
                            varNames.add(trimmed);
                        }
                    }
                    if (!varNames.isEmpty()) {
                        intoVars = varNames;
                        // Remove the INTO ... part, keep RETURNING clause in SQL
                        sql = sql.substring(0, collected.returning[0]) + " RETURNING " + returningCols;
                    }
                }
            }
        }

        return new PlpgsqlStatement.SqlStmt(sql, intoVars, strict);
    }

    // ---- GET DIAGNOSTICS ----

    private PlpgsqlStatement parseGetDiagnostics() {
        matchKw("GET");
        boolean stacked = matchKw("STACKED");
        // GET CURRENT DIAGNOSTICS spells out what plain GET DIAGNOSTICS already means
        if (!stacked) matchKw("CURRENT");
        matchKw("DIAGNOSTICS");
        List<PlpgsqlStatement.DiagItem> items = new ArrayList<>();
        do {
            String varName = readIdent();
            if (!match(TokenType.EQUALS)) match(TokenType.COLON_EQUALS);
            String itemName = readIdentAsWritten();
            if (peek().type() == TokenType.IDENTIFIER || peek().type() == TokenType.KEYWORD) {
                // Handle multi-word like ROW_COUNT, PG_EXCEPTION_DETAIL, etc.
                if (!check(TokenType.COMMA) && !check(TokenType.SEMICOLON) && !isAtEnd()) {
                    itemName += "_" + readIdentAsWritten();
                    // Handle triple-word items like PG_EXCEPTION_DETAIL -> already two-word after underscore join
                    // But PG_EXCEPTION_CONTEXT is also possible via PG + EXCEPTION + CONTEXT
                    if (peek().type() == TokenType.IDENTIFIER || peek().type() == TokenType.KEYWORD) {
                        if (!check(TokenType.COMMA) && !check(TokenType.SEMICOLON) && !isAtEnd()) {
                            itemName += "_" + readIdentAsWritten();
                        }
                    }
                }
            }
            // Kept as written: PostgreSQL echoes the word back in the message about it, and a
            // reader is looking for what they typed rather than for a normalised spelling.
            items.add(new PlpgsqlStatement.DiagItem(varName, itemName));
        } while (match(TokenType.COMMA));
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.GetDiagnosticsStmt(items, stacked);
    }

    // ---- Cursors ----

    private PlpgsqlStatement parseOpenCursor() {
        matchKw("OPEN");
        String cursorName = readIdent();
        List<String> argExprs = new ArrayList<>();
        List<String> argNames = new ArrayList<>();
        // OPEN c(args) binds a bound cursor's parameters, positionally or by name
        if (check(TokenType.LEFT_PAREN)) {
            advance();
            if (!check(TokenType.RIGHT_PAREN)) {
                do {
                    String argName = null;
                    if ((peek().type() == TokenType.IDENTIFIER || peek().type() == TokenType.KEYWORD)
                            && pos + 1 < tokens.size()
                            && tokens.get(pos + 1).type() == TokenType.COLON_EQUALS) {
                        argName = readIdent();
                        advance(); // consume :=
                    }
                    argNames.add(argName);
                    argExprs.add(collectArgument());
                } while (match(TokenType.COMMA));
            }
            match(TokenType.RIGHT_PAREN);
        }
        String sql = null;
        if (matchKw("FOR")) sql = collectUntilSemicolon();
        match(TokenType.SEMICOLON);
        checkCursorArgs(cursorName, argNames);
        return new PlpgsqlStatement.OpenCursorStmt(cursorName, sql, argExprs, argNames);
    }

    /**
     * PG matches an OPEN's arguments against the cursor's declared parameters while compiling the
     * body, so a bad argument list keeps the function from being created at all.
     */
    private void checkCursorArgs(String cursorName, List<String> argNames) {
        List<String> params = cursorParams.get(cursorName.toLowerCase());
        if (params == null) return;
        if (params.isEmpty()) {
            if (!argNames.isEmpty()) {
                throw new MemgresException(
                        "cursor \"" + cursorName + "\" has no arguments", "42601");
            }
            return;
        }
        boolean[] bound = new boolean[params.size()];
        for (int i = 0; i < argNames.size(); i++) {
            String name = argNames.get(i);
            int slot;
            if (name != null) {
                slot = -1;
                for (int p = 0; p < params.size(); p++) {
                    if (params.get(p).equalsIgnoreCase(name)) { slot = p; break; }
                }
                if (slot < 0) {
                    throw new MemgresException("cursor \"" + cursorName
                            + "\" has no argument named \"" + name + "\"", "42601");
                }
            } else {
                slot = i;
                if (slot >= params.size()) {
                    throw new MemgresException(
                            "too many arguments for cursor \"" + cursorName + "\"", "42601");
                }
            }
            if (bound[slot]) {
                throw new MemgresException("value for parameter \"" + params.get(slot)
                        + "\" of cursor \"" + cursorName + "\" specified more than once", "42601");
            }
            bound[slot] = true;
        }
        for (boolean b : bound) {
            if (!b) {
                throw new MemgresException(
                        "not enough arguments for cursor \"" + cursorName + "\"", "42601");
            }
        }
    }

    /** Collect one argument of an OPEN, stopping at the comma or paren that ends it. */
    private String collectArgument() {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (!isAtEnd()) {
            Token t = peek();
            if (depth == 0 && (t.type() == TokenType.COMMA || t.type() == TokenType.RIGHT_PAREN
                    || t.type() == TokenType.SEMICOLON)) break;
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) depth--;
            appendToken(sb, t);
            advance();
        }
        return sb.toString().trim();
    }

    /** Directions a FETCH or MOVE may name; NEXT is the default when none is written. */
    private static final java.util.Set<String> FETCH_DIRECTIONS = new java.util.HashSet<>(
            java.util.Arrays.asList("NEXT", "PRIOR", "FIRST", "LAST", "ABSOLUTE", "RELATIVE",
                    "FORWARD", "BACKWARD"));

    private PlpgsqlStatement parseFetch() {
        boolean move = checkKw("MOVE");
        advance(); // consume FETCH or MOVE

        String direction = "NEXT";
        String countExpr = null;
        if (isLabelToken(peek()) && FETCH_DIRECTIONS.contains(peek().value().toUpperCase())) {
            direction = advance().value().toUpperCase();
        } else if (checkKw("ALL")) {
            // A bare ALL is FORWARD ALL, the same count written the shorter way
            direction = "FORWARD";
            countExpr = advance().value().toUpperCase();
        }
        if ("ABSOLUTE".equals(direction) || "RELATIVE".equals(direction)
                || "FORWARD".equals(direction) || "BACKWARD".equals(direction)) {
            if (!checkKw("FROM") && !checkKw("IN") && !check(TokenType.SEMICOLON)) {
                countExpr = collectUntilMulti("FROM", "IN", ";");
            }
        } else if ("NEXT".equals(direction) && !checkKw("FROM") && !checkKw("IN")
                && (check(TokenType.INTEGER_LITERAL) || check(TokenType.MINUS))) {
            // A bare count is FORWARD count
            direction = "FORWARD";
            countExpr = collectUntilMulti("FROM", "IN", ";");
        }
        if (!matchKw("FROM")) matchKw("IN");

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
        return new PlpgsqlStatement.FetchStmt(cursorName, intoVars, direction, countExpr, move);
    }

    private PlpgsqlStatement parseCloseCursor() {
        matchKw("CLOSE");
        String cursorName = readIdent();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.CloseCursorStmt(cursorName);
    }

    // ---- Transaction control (PG 11+ procedures) ----

    private PlpgsqlStatement parseCommit() {
        advance(); // consume COMMIT
        boolean chain = parseAndChain();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.CommitStmt(chain);
    }

    private PlpgsqlStatement parseRollback() {
        advance(); // consume ROLLBACK
        // PG 18: ROLLBACK TO [SAVEPOINT] is not valid in PL/pgSQL — reject at creation time
        if (checkKw("TO")) {
            throw new MemgresException("syntax error at or near \"TO\"", "42601");
        }
        boolean chain = parseAndChain();
        match(TokenType.SEMICOLON);
        return new PlpgsqlStatement.RollbackStmt(chain);
    }

    private PlpgsqlStatement parseAbort() {
        advance(); // consume ABORT
        // Consume the rest until semicolon
        while (pos < tokens.size() && !check(TokenType.SEMICOLON)) advance();
        if (check(TokenType.SEMICOLON)) advance();
        return new PlpgsqlStatement.AbortStmt();
    }

    private boolean parseAndChain() {
        if (checkKw("AND")) {
            advance();
            if (checkKw("CHAIN")) {
                advance();
                return true;
            }
            // AND NO CHAIN is the default — PG accepts it explicitly
            if (checkKw("NO")) {
                advance();
                matchKw("CHAIN");
            }
        }
        // Skip optional WORK/TRANSACTION keyword
        if (checkKw("WORK") || checkKw("TRANSACTION")) advance();
        return false;
    }

    // ---- Assignment ----

    private PlpgsqlStatement parseAssignmentOrSql() {
        int saved = pos;
        StringBuilder target = new StringBuilder();
        String baseName = readIdent();
        target.append(baseName);

        // Steps past the variable name: field selections and subscripts, in any order, so that
        // a[1].x and v.f[2] both reach the element they name.
        List<PlpgsqlStatement.TargetStep> steps = new ArrayList<>();
        boolean sawSubscript = false;
        while (true) {
            if (check(TokenType.DOT)) {
                advance();
                String field = readIdent();
                target.append(".").append(field);
                steps.add(PlpgsqlStatement.TargetStep.field(field));
                continue;
            }
            if (check(TokenType.LEFT_BRACKET)) {
                advance();
                String lower = collectSubscript();
                String upper = null;
                if (match(TokenType.COLON)) {
                    upper = collectSubscript();
                }
                if (!match(TokenType.RIGHT_BRACKET)) { pos = saved; return parseSqlStmt(); }
                steps.add(upper != null
                        ? PlpgsqlStatement.TargetStep.slice(lower, upper)
                        : PlpgsqlStatement.TargetStep.subscript(lower));
                sawSubscript = true;
                continue;
            }
            break;
        }

        // PG accepts = as a synonym for := everywhere an assignment is written
        if (match(TokenType.COLON_EQUALS) || match(TokenType.EQUALS)) {
            String value = collectUntilSemicolon();
            expectSemicolon();
            if (sawSubscript) {
                return new PlpgsqlStatement.SubscriptAssignment(baseName, steps, value);
            }
            return new PlpgsqlStatement.Assignment(target.toString(), value);
        }

        pos = saved;
        return parseSqlStmt();
    }

    /** Collect one subscript expression, stopping at the bracket or colon that closes it. */
    private String collectSubscript() {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (!isAtEnd()) {
            Token t = peek();
            if (depth == 0 && (t.type() == TokenType.RIGHT_BRACKET || t.type() == TokenType.COLON)) break;
            if (t.type() == TokenType.SEMICOLON && depth == 0) break;
            if (t.type() == TokenType.LEFT_PAREN || t.type() == TokenType.LEFT_BRACKET) depth++;
            if (t.type() == TokenType.RIGHT_PAREN || t.type() == TokenType.RIGHT_BRACKET) depth--;
            appendToken(sb, t);
            advance();
        }
        return sb.toString().trim();
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

    /**
     * Every statement in a PL/pgSQL body ends with a semicolon, and the one that does not is a
     * syntax error where it runs out — at the word that could not follow it, or at the end of the
     * input when nothing did. Treating the terminator as optional let a block missing one compile
     * and run, so text PostgreSQL refuses did whatever the reader guessed it meant.
     */
    private void expectSemicolon() {
        if (match(TokenType.SEMICOLON)) return;
        Token t = peek();
        if (isAtEnd() || t.type() == TokenType.EOF) {
            throw new MemgresException("syntax error at end of input", "42601");
        }
        throw new MemgresException("syntax error at or near \"" + t.raw() + "\"", "42601");
    }

    private String collectUntilSemicolon() {
        return collectSqlUntilSemicolon().text.trim();
    }

    /**
     * A statement's text rebuilt from its tokens, with the offsets of the clause keywords in it.
     *
     * <p>PL/pgSQL has to find INTO, FROM and RETURNING in a statement it otherwise passes on
     * untouched, and a search over the finished text cannot tell a keyword from the same letters
     * inside a string literal. The offsets are recorded as the text is built, where a literal is
     * one token, and only at the top level, so the FROM of {@code extract(year FROM d)} is not
     * one of them. Each is the offset of the space the token was written after, which is where a
     * text search would have matched, so what is cut and what is kept is unchanged.
     */
    private static final class CollectedSql {
        final String text;
        /** {start, end} of every top-level INTO. */
        final List<int[]> intos;
        /** The start of every top-level FROM. */
        final List<Integer> froms;
        /** The start of the last top-level SELECT, or -1 when the statement has none. */
        final int lastSelect;
        /** {start, end} of the first top-level RETURNING, or null. */
        final int[] returning;

        CollectedSql(String text, List<int[]> intos, List<Integer> froms, int lastSelect,
                     int[] returning) {
            this.text = text;
            this.intos = intos;
            this.froms = froms;
            this.lastSelect = lastSelect;
            this.returning = returning;
        }

        int[] firstIntoAfter(int offset) {
            for (int[] into : intos) {
                if (into[0] >= offset) return into;
            }
            return null;
        }

        int[] lastInto() {
            return intos.isEmpty() ? null : intos.get(intos.size() - 1);
        }

        int firstFromAfter(int offset) {
            for (Integer from : froms) {
                if (from.intValue() >= offset) return from.intValue();
            }
            return -1;
        }
    }

    private CollectedSql collectSqlUntilSemicolon() {
        StringBuilder sb = new StringBuilder();
        List<int[]> intos = new ArrayList<>();
        List<Integer> froms = new ArrayList<>();
        int lastSelect = -1;
        int[] returning = null;
        int depth = 0;
        while (!isAtEnd()) {
            Token t = peek();
            if (t.type() == TokenType.SEMICOLON && depth == 0) break;
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) depth--;
            int start = sb.length();
            appendToken(sb, t);
            if (depth == 0 && t.type() == TokenType.KEYWORD) {
                if (t.value().equalsIgnoreCase("INTO")) {
                    intos.add(new int[]{start, sb.length()});
                } else if (t.value().equalsIgnoreCase("FROM")) {
                    froms.add(Integer.valueOf(start));
                } else if (t.value().equalsIgnoreCase("SELECT")) {
                    lastSelect = start;
                } else if (returning == null && t.value().equalsIgnoreCase("RETURNING")) {
                    returning = new int[]{start, sb.length()};
                }
            }
            advance();
        }
        return new CollectedSql(sb.toString(), intos, froms, lastSelect, returning);
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

    /**
     * Collect one expression's worth of tokens. A terminator only ends it at the top level:
     * a comma inside {@code ARRAY[1,2]} belongs to the array, exactly as one inside
     * {@code coalesce(a,b)} belongs to the call, so brackets nest the same way parentheses do.
     */
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
            if (t.type() == TokenType.LEFT_PAREN || t.type() == TokenType.LEFT_BRACKET) depth++;
            if (t.type() == TokenType.RIGHT_PAREN || t.type() == TokenType.RIGHT_BRACKET) depth--;
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
        if (sb.length() > 0) sb.append(" ");
        if (t.type() == TokenType.STRING_LITERAL) {
            sb.append("'").append(t.value().replace("'", "''")).append("'");
        } else if (t.type() == TokenType.DOLLAR_STRING_LITERAL) {
            // Preserve dollar-quoted strings as string literals
            sb.append("'").append(t.value().replace("'", "''")).append("'");
        } else if (t.type() == TokenType.BIT_STRING_LITERAL) {
            sb.append("B'").append(t.value()).append("'");
        } else {
            sb.append(t.value());
        }
    }
}
