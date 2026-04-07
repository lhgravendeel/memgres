package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility statement parsing (SET, SHOW, COPY, transactions, EXPLAIN, etc.),
 * extracted from Parser to reduce class size.
 */
class UtilityParser {
    private final Parser parser;

    UtilityParser(Parser parser) {
        this.parser = parser;
    }

    // ---- COPY ----

    CopyStmt parseCopy() {
        parser.expectKeyword("COPY");
        // COPY (SELECT ...) TO: subquery form
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // consume (
            Statement subquery = parser.parseStatement();
            parser.expect(TokenType.RIGHT_PAREN);
            parser.expectKeyword("TO");
            String source;
            if (parser.check(TokenType.STRING_LITERAL)) {
                source = parser.advance().value();
            } else {
                source = parser.readIdentifier().toUpperCase();
            }
            // Parse options for subquery COPY TO
            String format = "text";
            String delimiter = null;
            String nullString = null;
            boolean header = false;
            if (parser.matchKeyword("WITH") || parser.check(TokenType.LEFT_PAREN)) {
                if (parser.check(TokenType.LEFT_PAREN)) parser.advance();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    String opt = parser.readIdentifier().toUpperCase();
                    switch (opt) {
                        case "FORMAT":
                            format = parser.readIdentifier().toLowerCase();
                            break;
                        case "DELIMITER":
                            delimiter = parser.advance().value();
                            break;
                        case "NULL":
                            nullString = parser.advance().value();
                            break;
                        case "HEADER": {
                            header = true; parser.matchKeyword("TRUE"); 
                            break;
                        }
                        case "CSV":
                            format = "csv";
                            break;
                        default: {
                            if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd())
                                parser.advance();
                            break;
                        }
                    }
                    parser.match(TokenType.COMMA);
                }
                parser.match(TokenType.RIGHT_PAREN);
            }
            if (parser.matchKeyword("CSV")) {
                format = "csv";
                if (parser.matchKeyword("HEADER")) header = true;
            }
            if (delimiter == null) delimiter = "csv".equals(format) ? "," : "\t";
            if (nullString == null) nullString = "csv".equals(format) ? "" : "\\N";
            return new CopyStmt(null, null, false, source, format, delimiter, nullString, header, null, subquery);
        }

        // Table name: handle schema-qualified names (schema.table)
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            // schema.table: preserve schema-qualified name
            table = table + "." + parser.readIdentifier();
        }

        // Optional column list
        List<String> columns = null;
        if (parser.match(TokenType.LEFT_PAREN)) {
            columns = new java.util.ArrayList<>();
            do {
                columns.add(parser.readIdentifier());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        }

        boolean isFrom;
        if (parser.matchKeyword("FROM")) {
            isFrom = true;
        } else {
            parser.expectKeyword("TO");
            isFrom = false;
        }

        // Source: filename string, STDIN/STDOUT, or PROGRAM 'command'
        String source;
        if (parser.check(TokenType.STRING_LITERAL)) {
            source = parser.advance().value();
        } else {
            source = parser.readIdentifier().toUpperCase();
        }
        // Handle PROGRAM keyword: consume the program command string
        if ("PROGRAM".equals(source)) {
            if (parser.check(TokenType.STRING_LITERAL)) {
                parser.advance(); // consume command string, keep source as "PROGRAM"
            }
        }

        // Optional WITH (options)
        String format = "text";
        String delimiter = null; // null = use format default later
        String nullString = null; // null = use format default later
        boolean header = false;
        String quote = "\"";
        String escape = null; // null = same as quote char
        List<String> forceQuote = null;
        List<String> forceNotNull = null;
        boolean freeze = false;
        String encoding = null;
        String onError = null;
        String defaultString = null;

        if (parser.matchKeyword("WITH") || parser.check(TokenType.LEFT_PAREN)) {
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); // consume '('
            }
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                String opt = parser.readIdentifier().toUpperCase();
                switch (opt) {
                    case "FORMAT":
                        format = parser.readIdentifier().toLowerCase();
                        break;
                    case "DELIMITER":
                        delimiter = parser.advance().value();
                        break;
                    case "NULL":
                        nullString = parser.advance().value();
                        break;
                    case "HEADER": {
                        header = true;
                        if (parser.matchKeyword("TRUE")) { /* already set */ }
                        else if (parser.matchKeyword("FALSE")) { header = false; }
                        else if (parser.matchKeyword("MATCH")) { /* HEADER MATCH for FROM */ }
                        break;
                    }
                    case "QUOTE":
                        quote = parser.advance().value();
                        break;
                    case "ESCAPE":
                        escape = parser.advance().value();
                        break;
                    case "FORCE_QUOTE":
                        forceQuote = parseColumnListOption();
                        break;
                    case "FORCE_NOT_NULL":
                        forceNotNull = parseColumnListOption();
                        break;
                    case "FORCE_NULL":
                        parseColumnListOption();
                        break;
                    case "FREEZE": {
                        freeze = true;
                        parser.matchKeyword("TRUE");
                        break;
                    }
                    case "ENCODING":
                        encoding = parser.advance().value();
                        break;
                    case "ON_ERROR":
                        onError = parser.readIdentifier().toLowerCase();
                        break;
                    case "LOG_VERBOSITY":
                        parser.readIdentifier();
                        break;
                    case "DEFAULT":
                        defaultString = parser.advance().value();
                        break;
                    case "CSV":
                        format = "csv";
                        break;
                    default: {
                        // Skip unknown option values
                        if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                            parser.advance();
                        }
                        break;
                    }
                }
                parser.match(TokenType.COMMA);
            }
            parser.match(TokenType.RIGHT_PAREN);
        }

        // Handle CSV keyword without WITH (old-style syntax)
        if (parser.matchKeyword("CSV")) {
            format = "csv";
            if (parser.matchKeyword("HEADER")) header = true;
        }

        // Handle BINARY keyword without WITH
        if (parser.matchKeyword("BINARY")) {
            format = "binary";
        }

        // Set format-dependent defaults
        if (delimiter == null) delimiter = "csv".equals(format) ? "," : "\t";
        if (nullString == null) nullString = "csv".equals(format) ? "" : "\\N";
        if (escape == null) escape = quote;

        // Handle WHERE clause for COPY TO
        String whereClause = null;
        if (!isFrom && parser.matchKeyword("WHERE")) {
            // Capture the rest as WHERE clause text
            StringBuilder wb = new StringBuilder();
            while (!parser.isAtEnd() && parser.peek().type() != TokenType.SEMICOLON) {
                Token t = parser.advance();
                if (wb.length() > 0) wb.append(' ');
                if (t.type() == TokenType.STRING_LITERAL) {
                    wb.append("'").append(t.value()).append("'");
                } else {
                    wb.append(t.value());
                }
            }
            whereClause = wb.toString();
        }

        return new CopyStmt(table, columns, isFrom, source, format, delimiter, nullString, header, null, null,
                quote, escape, forceQuote, forceNotNull, freeze, encoding, whereClause, onError, defaultString);
    }

    /** Parse a column list option like FORCE_QUOTE (col1, col2) or FORCE_QUOTE *. */
    private List<String> parseColumnListOption() {
        List<String> cols = new ArrayList<>();
        if (parser.match(TokenType.LEFT_PAREN)) {
            do {
                if (parser.check(TokenType.STAR)) {
                    parser.advance();
                    cols.add("*");
                } else {
                    cols.add(parser.readIdentifier());
                }
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        } else if (parser.check(TokenType.STAR)) {
            parser.advance();
            cols.add("*");
        }
        return cols;
    }

    // ---- SET ----

    SetStmt parseSet() {
        parser.expectKeyword("SET");

        // SET SESSION AUTHORIZATION name | DEFAULT
        if (parser.checkKeyword("SESSION") && parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).type() == TokenType.KEYWORD
                && parser.tokens.get(parser.pos + 1).value().equals("AUTHORIZATION")) {
            parser.advance(); // SESSION
            parser.advance(); // AUTHORIZATION
            String authValue = parser.readIdentifier();
            return new SetStmt("session_authorization", authValue);
        }

        // SET ROLE name | NONE | DEFAULT
        if (parser.checkKeyword("ROLE")) {
            parser.advance();
            String roleVal = parser.readIdentifier();
            return new SetStmt("role", roleVal);
        }

        boolean isSession = parser.matchKeyword("SESSION");
        boolean isLocal = !isSession && parser.matchKeyword("LOCAL");
        if (isSession && (parser.checkKeyword("CHARACTERISTICS") || (parser.peek().type() == TokenType.IDENTIFIER && parser.peek().value().equalsIgnoreCase("CHARACTERISTICS")))) {
            parser.advance(); // CHARACTERISTICS
            parser.matchKeyword("AS");
            parser.matchKeyword("TRANSACTION");
            if (parser.checkKeyword("ISOLATION")) {
                parser.advance(); // ISOLATION
                parser.matchKeyword("LEVEL");
                String isolLevel = null;
                if (parser.matchKeyword("READ")) {
                    if (parser.matchKeyword("COMMITTED")) isolLevel = "read committed";
                    else if (parser.matchKeyword("UNCOMMITTED")) isolLevel = "read uncommitted";
                } else if (parser.matchKeyword("REPEATABLE")) {
                    parser.matchKeyword("READ");
                    isolLevel = "repeatable read";
                } else if (parser.matchKeyword("SERIALIZABLE")) {
                    isolLevel = "serializable";
                }
                // consume remaining (READ ONLY/WRITE, DEFERRABLE, etc.)
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                if (isolLevel != null) {
                    return new SetStmt("default_transaction_isolation", isolLevel);
                }
                return new SetStmt("default_transaction_isolation", "");
            }
            if (parser.checkKeyword("READ")) {
                parser.advance(); // READ
                if (parser.matchKeyword("ONLY")) {
                    // consume remaining
                    while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                    return new SetStmt("default_transaction_read_only", "on");
                } else if (parser.matchKeyword("WRITE")) {
                    // consume remaining
                    while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                    return new SetStmt("default_transaction_read_only", "off");
                }
            }
            // consume remaining
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("default_transaction_isolation", "");
        }
        // SET XML OPTION { DOCUMENT | CONTENT }
        if (parser.checkKeyword("XML")) {
            parser.advance(); // XML
            if (parser.matchKeyword("OPTION")) {
                String mode = parser.readIdentifier(); // DOCUMENT or CONTENT
                return new SetStmt("xmloption", mode.toLowerCase());
            }
            // Otherwise fall through; treat as SET xml = ...
            if (parser.match(TokenType.EQUALS) || parser.matchKeyword("TO")) {
                StringBuilder val = new StringBuilder();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                    val.append(parser.advance().value()).append(" ");
                }
                return new SetStmt("xml", val.toString().trim());
            }
        }

        // SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }
        if (parser.checkKeyword("CONSTRAINTS")) {
            parser.advance();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("constraints", "");
        }

        // SET TRANSACTION [ISOLATION LEVEL ...] [READ ONLY|WRITE] [NOT DEFERRABLE|DEFERRABLE]
        if (parser.checkKeyword("TRANSACTION")) {
            parser.advance();
            String isolLevel = null;
            if (parser.matchKeyword("ISOLATION")) {
                parser.matchKeyword("LEVEL");
                if (parser.matchKeyword("READ")) {
                    if (parser.matchKeyword("COMMITTED")) isolLevel = "read committed";
                    else if (parser.matchKeyword("UNCOMMITTED")) isolLevel = "read uncommitted";
                } else if (parser.matchKeyword("REPEATABLE")) {
                    parser.matchKeyword("READ");
                    isolLevel = "repeatable read";
                } else if (parser.matchKeyword("SERIALIZABLE")) {
                    isolLevel = "serializable";
                }
            }
            // consume remaining (READ ONLY/WRITE, DEFERRABLE, etc.)
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            if (isolLevel != null) {
                return new SetStmt("transaction_isolation", isolLevel);
            }
            return new SetStmt("transaction", "");
        }

        String name = parser.readIdentifier();
        // Handle schema-qualified GUC names: myapp.tenant_id
        if (parser.match(TokenType.DOT)) {
            name = name + "." + parser.readIdentifier();
        }
        // Handle SET TIME ZONE value → treat as SET timezone value
        if (name.equalsIgnoreCase("TIME") && parser.checkKeyword("ZONE")) {
            parser.advance(); // consume ZONE
            name = "TimeZone";
            // Continue to parse the value below
        }
        // SET name = value | SET name TO value
        if (parser.match(TokenType.EQUALS) || parser.matchKeyword("TO")) {
            StringBuilder val = new StringBuilder();
            boolean hasTokens = false;
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                Token tok = parser.advance();
                hasTokens = true;
                if (tok.type() == TokenType.COMMA) {
                    if (!val.isEmpty() && val.charAt(val.length() - 1) == ' ') {
                        val.setLength(val.length() - 1);
                    }
                    val.append(", ");
                } else {
                    val.append(tok.value()).append(" ");
                }
            }
            String trimmed = val.toString().trim();
            if (!hasTokens) {
                throw new ParseException("syntax error at or near end of input", parser.peek());
            }
            return new SetStmt(name, trimmed, isLocal);
        }
        // SET name value (e.g., SET TIMEZONE 'UTC')
        StringBuilder val = new StringBuilder();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            val.append(parser.advance().value()).append(" ");
        }
        return new SetStmt(name, val.toString().trim(), isLocal);
    }

    // ---- DISCARD ----

    DiscardStmt parseDiscard() {
        parser.expectKeyword("DISCARD");
        String target = parser.readIdentifier();
        return new DiscardStmt(target);
    }

    // ---- Transaction statements ----

    TransactionStmt parseTransactionBegin() {
        if (parser.matchKeyword("START")) {
            parser.matchKeyword("TRANSACTION");
        } else {
            parser.expectKeyword("BEGIN");
            parser.matchKeyword("TRANSACTION");
            parser.matchKeyword("WORK");
        }
        String isolationLevel = null;
        Boolean readOnly = null;
        // Consume transaction options (ISOLATION LEVEL, READ ONLY/WRITE, [NOT] DEFERRABLE)
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
            if (parser.matchKeyword("ISOLATION")) {
                parser.matchKeyword("LEVEL");
                // READ COMMITTED / UNCOMMITTED / REPEATABLE READ / SERIALIZABLE
                if (parser.matchKeyword("READ")) {
                    if (parser.matchKeyword("COMMITTED")) isolationLevel = "read committed";
                    else if (parser.matchKeyword("UNCOMMITTED")) isolationLevel = "read uncommitted";
                } else if (parser.matchKeyword("REPEATABLE")) {
                    parser.matchKeyword("READ");
                    isolationLevel = "repeatable read";
                } else if (parser.matchKeyword("SERIALIZABLE")) {
                    isolationLevel = "serializable";
                }
            } else if (parser.matchKeyword("READ")) {
                if (parser.matchKeyword("ONLY")) readOnly = true;
                else if (parser.matchKeyword("WRITE")) readOnly = false;
            } else if (parser.matchKeyword("NOT")) {
                parser.matchKeyword("DEFERRABLE");
            } else if (parser.matchKeyword("DEFERRABLE")) {
                // consumed
            } else {
                break;
            }
            parser.match(TokenType.COMMA); // options can be comma-separated
        }
        return new TransactionStmt(TransactionStmt.TransactionAction.BEGIN, null, isolationLevel, readOnly);
    }

    TransactionStmt parseTransactionCommit() {
        if (parser.matchKeyword("COMMIT") || parser.matchKeyword("END")) {
            // COMMIT PREPARED 'gid'
            if (parser.matchKeyword("PREPARED")) {
                String gid = parser.advance().value(); // string literal
                return new TransactionStmt(TransactionStmt.TransactionAction.COMMIT_PREPARED, gid);
            }
            parser.matchKeyword("TRANSACTION");
            parser.matchKeyword("WORK");
        }
        return new TransactionStmt(TransactionStmt.TransactionAction.COMMIT, null);
    }

    TransactionStmt parseTransactionRollback() {
        parser.advance(); // ROLLBACK or ABORT
        // ROLLBACK PREPARED 'gid'
        if (parser.matchKeyword("PREPARED")) {
            String gid = parser.advance().value(); // string literal
            return new TransactionStmt(TransactionStmt.TransactionAction.ROLLBACK_PREPARED, gid);
        }
        if (parser.matchKeyword("TO")) {
            parser.matchKeyword("SAVEPOINT");
            String name = parser.readIdentifier();
            return new TransactionStmt(TransactionStmt.TransactionAction.ROLLBACK_TO_SAVEPOINT, name);
        }
        parser.matchKeyword("TRANSACTION");
        parser.matchKeyword("WORK");
        return new TransactionStmt(TransactionStmt.TransactionAction.ROLLBACK, null);
    }

    TransactionStmt parseSavepoint() {
        parser.expectKeyword("SAVEPOINT");
        String name = parser.readIdentifier();
        return new TransactionStmt(TransactionStmt.TransactionAction.SAVEPOINT, name);
    }

    TransactionStmt parseReleaseSavepoint() {
        parser.expectKeyword("RELEASE");
        parser.matchKeyword("SAVEPOINT");
        String name = parser.readIdentifier();
        return new TransactionStmt(TransactionStmt.TransactionAction.RELEASE_SAVEPOINT, name);
    }

    // ---- EXPLAIN ----

    ExplainStmt parseExplain() {
        parser.expectKeyword("EXPLAIN");
        boolean analyze = parser.matchKeyword("ANALYZE");
        boolean verbose = parser.matchKeyword("VERBOSE");
        String format = "TEXT";
        boolean costs = true;
        // Deferred option error: collected here so that table-existence checks
        // in the executor run first (matching PostgreSQL's error priority).
        String deferredOptionError = null;
        // SQLSTATE for the deferred error: 42601 for unrecognized option names,
        // 22023 for invalid option values (bad FORMAT, non-boolean value, etc.)
        String deferredOptionSqlState = null;

        // Handle EXPLAIN (options) format
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                parser.match(TokenType.COMMA); // options separated by commas
                if (parser.check(TokenType.RIGHT_PAREN)) break;
                // EXPLAIN options can be keywords or identifiers (e.g., COSTS is an identifier)
                Token optToken = parser.peek();
                String optName = optToken.value().toUpperCase();
                if (optName.equals("ANALYZE") || optName.equals("ANALYSE")) {
                    parser.advance();
                    try {
                        analyze = !consumeExplainBooleanOption();
                    } catch (ParseException e) {
                        if (deferredOptionError == null) { deferredOptionError = e.getMessage(); deferredOptionSqlState = "22023"; }
                    }
                } else if (optName.equals("VERBOSE")) {
                    parser.advance();
                    try {
                        verbose = !consumeExplainBooleanOption();
                    } catch (ParseException e) {
                        if (deferredOptionError == null) { deferredOptionError = e.getMessage(); deferredOptionSqlState = "22023"; }
                    }
                } else if (optName.equals("FORMAT")) {
                    parser.advance();
                    Token f = parser.advance();
                    format = f.value().toUpperCase();
                    if (!format.equals("TEXT") && !format.equals("XML") && !format.equals("JSON") && !format.equals("YAML")) {
                        if (deferredOptionError == null) {
                            deferredOptionError = "unrecognized value for EXPLAIN option \"FORMAT\": \"" + f.value() + "\"" +
                                    " at position " + f.position() + " near '" + f.value() + "'";
                            deferredOptionSqlState = "22023";
                        }
                    }
                } else if (optName.equals("COSTS")) {
                    parser.advance();
                    try {
                        costs = !consumeExplainBooleanOption();
                    } catch (ParseException e) {
                        if (deferredOptionError == null) { deferredOptionError = e.getMessage(); deferredOptionSqlState = "22023"; }
                    }
                } else if (optName.equals("BUFFERS")
                        || optName.equals("TIMING") || optName.equals("SUMMARY")
                        || optName.equals("WAL") || optName.equals("SETTINGS")) {
                    parser.advance();
                    try {
                        consumeExplainBooleanOption();
                    } catch (ParseException e) {
                        if (deferredOptionError == null) { deferredOptionError = e.getMessage(); deferredOptionSqlState = "22023"; }
                    }
                } else {
                    Token unknown = parser.advance();
                    if (deferredOptionError == null) {
                        deferredOptionError = "unrecognized EXPLAIN option \"" + unknown.value() + "\"" +
                                " at position " + unknown.position() + " near '" + unknown.value() + "'";
                        deferredOptionSqlState = "42601";
                    }
                }
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }

        Statement stmt = parser.parseStatement();
        return new ExplainStmt(stmt, analyze, verbose, format, costs, deferredOptionError, deferredOptionSqlState);
    }

    /** Consume an optional TRUE/FALSE/ON/OFF value after an EXPLAIN option. Returns true if value was FALSE/OFF.
     *  Rejects non-boolean values like integers or unrecognized identifiers. */
    boolean consumeExplainBooleanOption() {
        if (parser.checkKeyword("FALSE") || parser.checkKeyword("OFF")) { parser.advance(); return true; }
        if (parser.checkKeyword("TRUE") || parser.checkKeyword("ON")) { parser.advance(); return false; }
        // If there's a non-boolean value (integer, string, unrecognized identifier), reject it
        if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
            Token t = parser.peek();
            if (t.type() == TokenType.INTEGER_LITERAL || t.type() == TokenType.FLOAT_LITERAL
                    || t.type() == TokenType.STRING_LITERAL
                    || t.type() == TokenType.IDENTIFIER || t.type() == TokenType.KEYWORD) {
                throw new ParseException("\"" + t.value() + "\" is not a valid boolean value", t, "22023");
            }
        }
        return false; // no value = default (true)
    }

    // ---- LISTEN / NOTIFY / UNLISTEN ----

    ListenStmt parseListen() {
        parser.expectKeyword("LISTEN");
        String channel = parser.readIdentifier();
        return new ListenStmt(channel);
    }

    NotifyStmt parseNotify() {
        parser.expectKeyword("NOTIFY");
        String channel = parser.readIdentifier();
        String payload = null;
        if (parser.match(TokenType.COMMA)) {
            Token payloadToken = parser.peek();
            if (payloadToken.type() != TokenType.STRING_LITERAL) {
                throw new ParseException("syntax error at or near \"" + payloadToken.value() + "\"", payloadToken);
            }
            payload = parser.advance().value();
        }
        return new NotifyStmt(channel, payload);
    }

    UnlistenStmt parseUnlisten() {
        parser.expectKeyword("UNLISTEN");
        if (parser.match(TokenType.STAR)) {
            return new UnlistenStmt(null);
        }
        String channel = parser.readIdentifier();
        // PG only accepts a single channel, not a comma-separated list
        if (parser.check(TokenType.COMMA)) {
            throw new ParseException("syntax error at or near \",\"", parser.peek());
        }
        return new UnlistenStmt(channel);
    }

    // ---- PREPARE / EXECUTE / DEALLOCATE ----

    Statement parsePrepare() {
        parser.expectKeyword("PREPARE");
        // PREPARE TRANSACTION 'gid': two-phase commit
        if (parser.matchKeyword("TRANSACTION")) {
            String gid = parser.advance().value(); // string literal
            return new TransactionStmt(TransactionStmt.TransactionAction.PREPARE_TRANSACTION, gid);
        }
        String name = parser.readIdentifier();
        // Optional parameter types: (type, type, ...)
        List<String> paramTypes = new ArrayList<>();
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                do {
                    paramTypes.add(parser.parseTypeName());
                } while (parser.match(TokenType.COMMA));
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }
        parser.expectKeyword("AS");
        Statement body = parser.parseStatement();
        return new PrepareStmt(name, paramTypes, body);
    }

    ExecuteStmt parseExecuteStmt() {
        parser.expectKeyword("EXECUTE");
        String name = parser.readIdentifier();
        List<Expression> params = new ArrayList<>();
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                params = parser.parseExpressionList();
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }
        return new ExecuteStmt(name, params);
    }

    DeallocateStmt parseDeallocate() {
        parser.expectKeyword("DEALLOCATE");
        parser.matchKeyword("PREPARE"); // optional PREPARE keyword
        if (parser.matchKeyword("ALL")) {
            return new DeallocateStmt(null, true);
        }
        String name = parser.readIdentifier();
        return new DeallocateStmt(name, false);
    }

    // ---- DECLARE CURSOR / FETCH / MOVE / CLOSE ----

    DeclareCursorStmt parseDeclareCursor() {
        parser.expectKeyword("DECLARE");
        String name = parser.readIdentifier();
        // Optional: BINARY, INSENSITIVE, [NO] SCROLL
        parser.matchKeyword("BINARY");
        parser.matchKeyword("INSENSITIVE");
        boolean scroll = false;
        if (parser.matchKeyword("NO")) {
            parser.matchKeyword("SCROLL");
        } else if (parser.matchKeyword("SCROLL")) {
            scroll = true;
        }
        parser.expectKeyword("CURSOR");
        // Optional: WITH HOLD / WITHOUT HOLD
        boolean withHold = false;
        if (parser.matchKeyword("WITH")) {
            parser.matchKeyword("HOLD");
            withHold = true;
        } else if (parser.matchKeyword("WITHOUT")) {
            parser.matchKeyword("HOLD");
        }
        parser.expectKeyword("FOR");
        SelectStmt query = parser.parseSelect();
        return new DeclareCursorStmt(name, query, scroll, withHold);
    }

    FetchStmt parseFetchOrMove(boolean isMove) {
        parser.advance(); // consume FETCH or MOVE
        FetchStmt.Direction direction = FetchStmt.Direction.NEXT;
        int count = 1;

        // Parse direction
        if (parser.matchKeyword("NEXT")) {
            direction = FetchStmt.Direction.NEXT;
        } else if (parser.matchKeyword("PRIOR")) {
            direction = FetchStmt.Direction.PRIOR;
        } else if (parser.matchKeyword("FIRST")) {
            direction = FetchStmt.Direction.FIRST;
        } else if (parser.matchKeyword("LAST")) {
            direction = FetchStmt.Direction.LAST;
        } else if (parser.matchKeyword("ABSOLUTE")) {
            direction = FetchStmt.Direction.ABSOLUTE;
            count = parseFetchCount();
        } else if (parser.matchKeyword("RELATIVE")) {
            direction = FetchStmt.Direction.RELATIVE;
            count = parseFetchCount();
        } else if (parser.matchKeyword("FORWARD")) {
            if (parser.matchKeyword("ALL")) {
                direction = FetchStmt.Direction.FORWARD_ALL;
            } else if (parser.peek().type() == TokenType.INTEGER_LITERAL) {
                direction = FetchStmt.Direction.FORWARD;
                count = Integer.parseInt(parser.advance().value());
            } else {
                direction = FetchStmt.Direction.FORWARD;
            }
        } else if (parser.matchKeyword("BACKWARD")) {
            if (parser.matchKeyword("ALL")) {
                direction = FetchStmt.Direction.BACKWARD_ALL;
            } else if (parser.peek().type() == TokenType.INTEGER_LITERAL) {
                direction = FetchStmt.Direction.BACKWARD;
                count = Integer.parseInt(parser.advance().value());
            } else {
                direction = FetchStmt.Direction.BACKWARD;
            }
        } else if (parser.matchKeyword("ALL")) {
            direction = FetchStmt.Direction.ALL;
        } else if (parser.peek().type() == TokenType.INTEGER_LITERAL || (parser.peek().type() == TokenType.MINUS)) {
            // FETCH count [IN|FROM] cursor
            boolean negative = parser.match(TokenType.MINUS);
            count = Integer.parseInt(parser.advance().value());
            if (negative) count = -count;
            direction = count >= 0 ? FetchStmt.Direction.FORWARD : FetchStmt.Direction.BACKWARD;
            if (count < 0) count = -count;
        }

        parser.matchKeyword("FROM");
        parser.matchKeyword("IN");
        String cursorName = parser.readIdentifier();
        return new FetchStmt(direction, count, cursorName, isMove);
    }

    int parseFetchCount() {
        boolean negative = parser.match(TokenType.MINUS);
        int val = Integer.parseInt(parser.advance().value());
        return negative ? -val : val;
    }

    CloseStmt parseClose() {
        parser.expectKeyword("CLOSE");
        if (parser.matchKeyword("ALL")) {
            return new CloseStmt(null, true);
        }
        String name = parser.readIdentifier();
        return new CloseStmt(name, false);
    }

    // ---- LOCK TABLE ----

    LockStmt parseLock() {
        parser.expectKeyword("LOCK");
        parser.matchKeyword("TABLE");
        parser.matchKeyword("ONLY");
        String tableName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            tableName = tableName + "." + parser.readIdentifier(); // schema.table
        }
        String lockMode = "ACCESS EXCLUSIVE"; // default
        if (parser.matchKeyword("IN")) {
            StringBuilder mode = new StringBuilder();
            // Read lock mode keywords until MODE keyword
            while (!parser.checkKeyword("MODE") && !parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                if (!mode.isEmpty()) mode.append(" ");
                mode.append(parser.advance().value());
            }
            parser.matchKeyword("MODE");
            lockMode = mode.toString();
        }
        boolean nowait = parser.matchKeyword("NOWAIT");
        return new LockStmt(tableName, lockMode, nowait);
    }

    // ---- GRANT ----

    GrantStmt parseGrant() {
        parser.expectKeyword("GRANT");
        return parseGrantInner();
    }

    GrantStmt parseGrantInner() {
        List<String> privileges = new ArrayList<>();
        List<String> columns = null;

        // Read privileges: ALL [PRIVILEGES], SELECT, INSERT, UPDATE, DELETE, etc.
        do {
            String priv = parser.readIdentifier().toUpperCase();
            if (priv.equals("ALL")) {
                parser.matchKeyword("PRIVILEGES");
                privileges.add("ALL");
            } else {
                privileges.add(priv);
            }
            // Column-level privileges: UPDATE (col1, col2)
            if (parser.check(TokenType.LEFT_PAREN) && !parser.checkKeyword("ON")) {
                parser.advance(); // (
                columns = new ArrayList<>();
                do { columns.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            }
        } while (parser.match(TokenType.COMMA));

        // GRANT role TO role (role membership grant)
        if (parser.matchKeyword("TO")) {
            List<String> grantees = new ArrayList<>();
            do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
            boolean withAdmin = false;
            if (parser.matchKeywords("WITH", "ADMIN")) {
                parser.expectKeyword("OPTION");
                withAdmin = true;
            }
            return new GrantStmt(privileges, null, null, grantees, false, withAdmin, true, null);
        }

        // GRANT privileges ON object TO roles
        parser.expectKeyword("ON");
        String objectType = "TABLE"; // default
        String objectName;

        // GRANT SET ON PARAMETER param_name TO role (PG 15+), no-op
        if (parser.matchIdentifier("PARAMETER")) {
            // Consume remaining until semicolon
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new GrantStmt(privileges, "PARAMETER", "param", Cols.listOf(), false, false, false, null);
        }

        if (parser.matchKeyword("ALL")) {
            // ALL TABLES IN SCHEMA, ALL SEQUENCES IN SCHEMA, ALL FUNCTIONS IN SCHEMA
            String what = parser.readIdentifier().toUpperCase(); // TABLES, SEQUENCES, FUNCTIONS
            parser.expectKeyword("IN");
            parser.expectKeyword("SCHEMA");
            objectName = parser.readIdentifier();
            objectType = "ALL " + what + " IN SCHEMA";
        } else if (parser.matchKeyword("TABLE")) {
            objectName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("SEQUENCE")) {
            objectType = "SEQUENCE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("FUNCTION") || parser.matchKeyword("PROCEDURE")) {
            objectType = "FUNCTION";
            objectName = parser.readIdentifier();
            if (parser.check(TokenType.LEFT_PAREN)) parser.consumeUntilParen();
        } else if (parser.matchKeyword("SCHEMA")) {
            objectType = "SCHEMA";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("DATABASE")) {
            objectType = "DATABASE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("DOMAIN")) {
            objectType = "DOMAIN";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("LANGUAGE")) {
            objectType = "LANGUAGE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("TYPE")) {
            objectType = "TYPE";
            objectName = parser.readIdentifier();
        } else {
            objectName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) objectName = parser.readIdentifier();
        }

        parser.expectKeyword("TO");
        List<String> grantees = new ArrayList<>();
        do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));

        boolean withGrantOption = false;
        if (parser.matchKeywords("WITH", "GRANT")) {
            parser.expectKeyword("OPTION");
            withGrantOption = true;
        }

        return new GrantStmt(privileges, objectType, objectName, grantees, withGrantOption, false, false, columns);
    }

    // ---- REVOKE ----

    RevokeStmt parseRevoke() {
        parser.expectKeyword("REVOKE");

        boolean grantOptionFor = false;
        if (parser.matchKeywords("GRANT", "OPTION")) {
            parser.expectKeyword("FOR");
            grantOptionFor = true;
        }
        boolean adminOptionFor = false;
        if (parser.matchKeywords("ADMIN", "OPTION")) {
            parser.expectKeyword("FOR");
            adminOptionFor = true;
        }

        List<String> privileges = new ArrayList<>();
        do {
            String priv = parser.readIdentifier().toUpperCase();
            if (priv.equals("ALL")) {
                parser.matchKeyword("PRIVILEGES");
                privileges.add("ALL");
            } else {
                privileges.add(priv);
            }
            // Skip column-level parens
            if (parser.check(TokenType.LEFT_PAREN) && !parser.checkKeyword("ON") && !parser.checkKeyword("FROM")) {
                parser.consumeUntilParen();
            }
        } while (parser.match(TokenType.COMMA));

        // REVOKE role FROM role (role membership revoke)
        if (parser.matchKeyword("FROM")) {
            List<String> grantees = new ArrayList<>();
            do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
            boolean cascade = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new RevokeStmt(privileges, null, null, grantees, adminOptionFor, true, cascade);
        }

        // REVOKE privileges ON object FROM roles
        parser.expectKeyword("ON");
        String objectType = "TABLE";
        String objectName;

        if (parser.matchKeyword("ALL")) {
            String what = parser.readIdentifier().toUpperCase();
            parser.expectKeyword("IN");
            parser.expectKeyword("SCHEMA");
            objectName = parser.readIdentifier();
            objectType = "ALL " + what + " IN SCHEMA";
        } else if (parser.matchKeyword("TABLE")) {
            objectName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("SEQUENCE")) {
            objectType = "SEQUENCE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("FUNCTION") || parser.matchKeyword("PROCEDURE")) {
            objectType = "FUNCTION";
            objectName = parser.readIdentifier();
            if (parser.check(TokenType.LEFT_PAREN)) parser.consumeUntilParen();
        } else if (parser.matchKeyword("SCHEMA")) {
            objectType = "SCHEMA";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("DATABASE")) {
            objectType = "DATABASE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("DOMAIN")) {
            objectType = "DOMAIN";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("LANGUAGE")) {
            objectType = "LANGUAGE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("TYPE")) {
            objectType = "TYPE";
            objectName = parser.readIdentifier();
        } else {
            objectName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) objectName = parser.readIdentifier();
        }

        parser.expectKeyword("FROM");
        List<String> grantees = new ArrayList<>();
        do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));

        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");

        return new RevokeStmt(privileges, objectType, objectName, grantees, grantOptionFor, false, cascade);
    }

    // ---- REASSIGN OWNED ----

    Statement parseReassign() {
        parser.expectKeyword("REASSIGN");
        parser.expectKeyword("OWNED");
        parser.expectKeyword("BY");
        String oldRole = parser.readIdentifier();
        parser.expectKeyword("TO");
        String newRole = parser.readIdentifier();
        return new ReassignOwnedStmt(oldRole, newRole);
    }

    // ---- DO block ----

    SetStmt parseDo() {
        parser.expectKeyword("DO");
        // DO [LANGUAGE lang] body [LANGUAGE lang]
        if (parser.matchKeyword("LANGUAGE")) {
            parser.readIdentifierOrString(); // plpgsql or 'plpgsql'
        }
        // Get the dollar-quoted or string literal body
        Token bodyToken = parser.advance();
        String body = bodyToken.value();
        // Consume optional trailing LANGUAGE
        if (parser.matchKeyword("LANGUAGE")) {
            parser.readIdentifierOrString(); // plpgsql or 'plpgsql'
        }
        return new SetStmt("do_block", body);
    }

    // ---- RESET ----

    SetStmt parseReset() {
        parser.expectKeyword("RESET");
        if (parser.matchKeyword("ALL")) {
            return new SetStmt("reset", "ALL");
        }
        String param = parser.readIdentifier();
        return new SetStmt("reset", param);
    }

    // ---- SHOW ----

    SetStmt parseShow() {
        parser.expectKeyword("SHOW");
        if (parser.matchKeyword("ALL")) {
            return new SetStmt("show", "ALL");
        }
        String param = parser.readIdentifier();
        // Handle dotted GUC names: SHOW myapp.tenant_id
        if (parser.match(TokenType.DOT)) {
            param = param + "." + parser.readIdentifier();
        }
        // SHOW TRANSACTION ISOLATION LEVEL
        if (param.equalsIgnoreCase("TRANSACTION")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("show", "transaction_isolation");
        }
        return new SetStmt("show", param);
    }

    // ---- COMMENT ON ----

    SetStmt parseComment() {
        parser.expectKeyword("COMMENT");
        parser.expectKeyword("ON");
        // COMMENT ON {TABLE|COLUMN|FOREIGN DATA WRAPPER|...} name IS 'text'|NULL
        // Collect tokens before IS, building object type and name properly
        List<String> tokenValues = new ArrayList<>();
        while (!parser.isAtEnd() && !parser.checkKeyword("IS")) {
            Token tok = parser.advance();
            // Merge dot-separated identifiers into single dotted names
            if (tok.type() == TokenType.DOT && !tokenValues.isEmpty()) {
                String prev = tokenValues.remove(tokenValues.size() - 1);
                if (!parser.isAtEnd() && !parser.checkKeyword("IS")) {
                    Token next = parser.advance();
                    tokenValues.add(prev + "." + next.value());
                } else {
                    tokenValues.add(prev + ".");
                }
            } else {
                tokenValues.add(tok.value());
            }
        }
        // Object type is everything except the last token, object name is the last
        String objectType = tokenValues.size() > 1
                ? String.join(" ", tokenValues.subList(0, tokenValues.size() - 1))
                : "TABLE";
        String objectName = !tokenValues.isEmpty()
                ? tokenValues.get(tokenValues.size() - 1) : "";
        parser.expectKeyword("IS");
        String comment = null;
        if (parser.matchKeyword("NULL")) {
            comment = null;
        } else if (parser.check(TokenType.STRING_LITERAL)) {
            comment = parser.advance().value();
        } else {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        }
        // Store the comment
        return new SetStmt("comment:" + objectType + ":" + objectName, comment != null ? comment : "");
    }

    // ---- SECURITY LABEL ----

    SetStmt parseSecurityLabel() {
        parser.expectKeyword("SECURITY");
        parser.expectKeyword("LABEL");
        // SECURITY LABEL [FOR provider] ON object IS 'label'
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("security_label", "ok");
    }

    // ---- ANALYZE ----

    SetStmt parseAnalyze() {
        parser.advance(); // ANALYZE or ANALYSE
        // ANALYZE [VERBOSE] [table [(column, ...)]]
        parser.matchKeyword("VERBOSE");
        String tableName = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            tableName = parser.readIdentifier(); // table name
            if (parser.match(TokenType.DOT)) tableName = parser.readIdentifier(); // schema.table
            // Optional column list
            if (parser.check(TokenType.LEFT_PAREN)) parser.consumeUntilParen();
        }
        return new SetStmt("analyze", tableName != null ? "table:" + tableName : "ok");
    }

    // ---- VACUUM ----

    private static final Set<String> VALID_VACUUM_OPTIONS = Cols.setOf(
            "FULL", "FREEZE", "VERBOSE", "ANALYZE", "ANALYSE",
            "DISABLE_PAGE_SKIPPING", "SKIP_LOCKED", "PROCESS_TOAST", "PROCESS_MAIN",
            "TRUNCATE", "PARALLEL", "INDEX_CLEANUP", "BUFFER_USAGE_LIMIT", "SKIP_DATABASE_STATS");

    SetStmt parseVacuum() {
        parser.expectKeyword("VACUUM");
        // VACUUM [(options)] [table [(column, ...)]]
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // (
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                parser.match(TokenType.COMMA);
                if (parser.check(TokenType.RIGHT_PAREN)) break;
                Token optToken = parser.advance();
                String opt = optToken.value().toUpperCase();
                if (!VALID_VACUUM_OPTIONS.contains(opt)) {
                    throw new ParseException("unrecognized VACUUM option \"" + optToken.value() + "\"", optToken);
                }
                // Some options take a value (PARALLEL n, BUFFER_USAGE_LIMIT n, INDEX_CLEANUP bool)
                if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    parser.advance(); // consume value
                }
            }
            parser.expect(TokenType.RIGHT_PAREN);
        } else {
            // Bare options before table name
            parser.matchKeyword("FULL");
            parser.matchKeyword("FREEZE");
            parser.matchKeyword("VERBOSE");
            parser.matchKeyword("ANALYZE");
            parser.matchKeyword("ANALYSE");
        }
        // Optional table name
        String vacuumTable = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            vacuumTable = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) vacuumTable = parser.readIdentifier();
            if (parser.check(TokenType.LEFT_PAREN)) parser.consumeUntilParen();
        }
        return new SetStmt("vacuum", vacuumTable != null ? "table:" + vacuumTable : "ok");
    }

    // ---- REINDEX ----

    SetStmt parseReindex() {
        parser.expectKeyword("REINDEX");
        // REINDEX [(options)] { INDEX | TABLE | SCHEMA | DATABASE | SYSTEM } name
        if (parser.check(TokenType.LEFT_PAREN)) parser.consumeUntilParen();
        // Target type
        String targetType = null;
        String targetName = null;
        if (parser.checkKeyword("INDEX")) { parser.advance(); targetType = "INDEX"; }
        else if (parser.checkKeyword("TABLE")) { parser.advance(); targetType = "TABLE"; }
        else if (parser.checkKeyword("SCHEMA")) { parser.advance(); targetType = "SCHEMA"; }
        else if (parser.checkKeyword("DATABASE")) { parser.advance(); targetType = "DATABASE"; }
        else if (parser.checkKeyword("SYSTEM")) { parser.advance(); targetType = "SYSTEM"; }
        if (targetType != null) {
            // optional CONCURRENTLY
            parser.matchKeyword("CONCURRENTLY");
            if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                targetName = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) targetName = parser.readIdentifier(); // schema.name -> keep name
            }
        }
        String value = targetType != null && targetName != null ? targetType + ":" + targetName : "ok";
        return new SetStmt("reindex", value);
    }

    // ---- CLUSTER ----

    SetStmt parseCluster() {
        parser.expectKeyword("CLUSTER");
        // CLUSTER [VERBOSE] table [USING index]
        parser.matchKeyword("VERBOSE");
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            parser.readIdentifier(); // table name
            if (parser.matchKeyword("USING")) {
                parser.readIdentifier(); // index name
            }
        }
        return new SetStmt("cluster", "ok");
    }

    // ---- CHECKPOINT ----

    SetStmt parseCheckpoint() {
        parser.expectKeyword("CHECKPOINT");
        return new SetStmt("checkpoint", "ok");
    }

    // ---- LOAD ----

    SetStmt parseLoad() {
        parser.expectKeyword("LOAD");
        // LOAD 'library_name': consume the string literal
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("load", "ok");
    }

    SetStmt parseImport() {
        // IMPORT FOREIGN SCHEMA ... INTO ..., no-op
        parser.expectKeyword("IMPORT");
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("create_noop", "ok");
    }
}
