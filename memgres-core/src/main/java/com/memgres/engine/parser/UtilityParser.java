package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
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

    /** The opening of every "syntax error at or near" message. */
    private static final String SYNTAX_AT = "syntax error at or near \"";
    private static final String Q = "\"";

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
        List<String> forceNull = null;
        boolean headerMatch = false;
        boolean freeze = false;
        String encoding = null;
        String onError = null;
        String rejectLimit = null;
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
                        else if (parser.matchKeyword("MATCH")) { headerMatch = true; }
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
                        forceNull = parseColumnListOption();
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
                    case "REJECT_LIMIT":
                        rejectLimit = parser.advance().value();
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

        // A limit on how many rows may be rejected only means something where rows are allowed
        // to be rejected at all, so PostgreSQL refuses the one written without the other.
        if (rejectLimit != null && !"ignore".equalsIgnoreCase(onError)) {
            throw new MemgresException("COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
                    "22023");
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

        // Handle WHERE clause for COPY FROM/TO
        String whereClause = null;
        if (parser.matchKeyword("WHERE")) {
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
                quote, escape, forceQuote, forceNotNull, forceNull, headerMatch, freeze, encoding, whereClause, onError, defaultString);
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

    /**
     * The name a token spells. A keyword's token carries its upper-case form, which is not what
     * an object called name is called: COMMENT ON COLUMN t.name has to reach the column name.
     */
    private static String identifierSpelling(Token token) {
        return token.type() == TokenType.KEYWORD ? token.value().toLowerCase() : token.value();
    }

    /**
     * The role named by SET ROLE or SET SESSION AUTHORIZATION.
     *
     * <p>Both are written with an optional {@code TO} or {@code =} before the name, which was not
     * read, so {@code SET ROLE TO alice} set the role to the word "to". {@code NONE} gives the
     * role up; only SESSION AUTHORIZATION also accepts {@code DEFAULT}, and SET ROLE DEFAULT is
     * a syntax error rather than a way of clearing it.
     */
    private String readRoleTarget(boolean allowDefault) {
        if (parser.checkKeyword("TO")) parser.advance();
        else parser.match(TokenType.EQUALS);
        if (parser.checkKeyword("NONE")) { parser.advance(); return "none"; }
        if (parser.checkKeyword("DEFAULT")) {
            Token t = parser.peek();
            if (!allowDefault) throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
            parser.advance();
            return "default";
        }
        Token t = parser.peek();
        if (t.type() == TokenType.STRING_LITERAL) { parser.advance(); return t.value(); }
        return parser.readIdentifier();
    }

    SetStmt parseSet() {
        parser.expectKeyword("SET");

        // SET SESSION AUTHORIZATION name | DEFAULT
        if (parser.checkKeyword("SESSION") && parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).type() == TokenType.KEYWORD
                && parser.tokens.get(parser.pos + 1).value().equals("AUTHORIZATION")) {
            parser.advance(); // SESSION
            parser.advance(); // AUTHORIZATION
            return new SetStmt("session_authorization", readRoleTarget(true));
        }

        // SET ROLE [TO|=] name | NONE
        if (parser.checkKeyword("ROLE")) {
            parser.advance();
            return new SetStmt("role", readRoleTarget(false));
        }

        boolean isSession = parser.matchKeyword("SESSION");
        boolean isLocal = !isSession && parser.matchKeyword("LOCAL");
        if (isSession && (parser.checkKeyword("CHARACTERISTICS") || (parser.peek().type() == TokenType.IDENTIFIER && parser.peek().value().equalsIgnoreCase("CHARACTERISTICS")))) {
            parser.advance(); // CHARACTERISTICS
            parser.matchKeyword("AS");
            parser.matchKeyword("TRANSACTION");
            return new SetStmt("session_characteristics", encodeModes(requireTransactionModes()));
        }
        // SET LOCAL SESSION AUTHORIZATION name | DEFAULT
        if (isLocal && parser.checkKeyword("SESSION") && parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).type() == TokenType.KEYWORD
                && parser.tokens.get(parser.pos + 1).value().equals("AUTHORIZATION")) {
            parser.advance(); // SESSION
            parser.advance(); // AUTHORIZATION
            return new SetStmt("session_authorization", readRoleTarget(true), true);
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
            List<String> constraintNames = new ArrayList<>();
            if (parser.matchKeyword("ALL")) {
                constraintNames.add("ALL");
            } else {
                constraintNames.add(readQualifiedConstraintName());
                while (parser.match(TokenType.COMMA)) {
                    constraintNames.add(readQualifiedConstraintName());
                }
            }
            String mode = "IMMEDIATE";
            if (parser.matchKeyword("DEFERRED")) {
                mode = "DEFERRED";
            } else {
                parser.matchKeyword("IMMEDIATE");
            }
            // Encode as "constraints:ALL:DEFERRED" or "constraints:name1,name2:IMMEDIATE"
            return new SetStmt("constraints", String.join(",", constraintNames) + ":" + mode);
        }

        // SET TRANSACTION transaction_mode [, ...] | SET TRANSACTION SNAPSHOT 'id'
        if (parser.checkKeyword("TRANSACTION")) {
            parser.advance();
            if (parser.matchKeyword("SNAPSHOT") || parser.matchIdentifier("SNAPSHOT")) {
                String snapshotId;
                if (parser.check(TokenType.STRING_LITERAL)) {
                    snapshotId = parser.advance().value();
                } else {
                    snapshotId = parser.readIdentifier();
                }
                return new SetStmt("transaction_snapshot", snapshotId);
            }
            return new SetStmt("set_transaction", encodeModes(requireTransactionModes()));
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
                // A setting's value is a list of names and constants, so a stray token is a
                // syntax error rather than a word to store. "SET search_path = $user" has to be
                // written with the quotes PostgreSQL needs; taking it verbatim stored a search
                // path of "$ USER, public" that named no schema at all.
                if (tok.type() == TokenType.ERROR) {
                    throw ParseException.saying(
                            "syntax error at or near \"" + tok.value() + "\"", tok, "42601");
                }
                if (tok.type() == TokenType.COMMA) {
                    if (val.length() > 0 && val.charAt(val.length() - 1) == ' ') {
                        val.setLength(val.length() - 1);
                    }
                    val.append(", ");
                } else if (val.length() == 0
                        && (tok.type() == TokenType.MINUS || tok.type() == TokenType.PLUS)) {
                    // The sign of a number belongs to it. Appending a space after every token
                    // turned "= -15" into "- 15", which is not a number any setting will take.
                    val.append(tok.value());
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
            Token tok = parser.advance();
            if (val.length() == 0
                    && (tok.type() == TokenType.MINUS || tok.type() == TokenType.PLUS)) {
                val.append(tok.value());
            } else {
                val.append(tok.value()).append(" ");
            }
        }
        return new SetStmt(name, val.toString().trim(), isLocal);
    }

    // ---- DISCARD ----

    /** The things DISCARD knows how to throw away. */
    private static final java.util.Set<String> DISCARD_TARGETS = Cols.setOf(
            "all", "plans", "sequences", "temp", "temporary");

    DiscardStmt parseDiscard() {
        parser.expectKeyword("DISCARD");
        Token targetToken = parser.peek();
        String target = parser.readIdentifier();
        // DISCARD names one of these, and anything else is a syntax error where it stands rather
        // than a discard of nothing at all.
        if (target == null || !DISCARD_TARGETS.contains(target.toLowerCase())) {
            throw ParseException.saying(SYNTAX_AT + targetToken.raw() + Q, targetToken, "42601");
        }
        parser.expectEndOfStatement();
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
        TransactionModes modes = parseTransactionModes();
        return new TransactionStmt(TransactionStmt.TransactionAction.BEGIN, null,
                modes.isolationLevel, modes.readOnly, false, modes.deferrable);
    }

    /** Render a mode list as "iso=...;ro=on;def=off", listing only the modes that were written. */
    static String encodeModes(TransactionModes modes) {
        StringBuilder sb = new StringBuilder();
        if (modes.isolationLevel != null) sb.append("iso=").append(modes.isolationLevel).append(';');
        if (modes.readOnly != null) sb.append("ro=").append(modes.readOnly ? "on" : "off").append(';');
        if (modes.deferrable != null) sb.append("def=").append(modes.deferrable ? "on" : "off").append(';');
        return sb.toString();
    }

    /** The transaction_mode list shared by BEGIN, START TRANSACTION and SET TRANSACTION. */
    static final class TransactionModes {
        String isolationLevel;
        Boolean readOnly;
        Boolean deferrable;
        boolean any;
    }

    /**
     * Parse {@code transaction_mode [, ...]}.
     *
     * <p>An isolation level PostgreSQL does not have is a syntax error, not a level to remember:
     * accepting it would leave the session claiming an isolation it is not providing.
     */
    TransactionModes parseTransactionModes() {
        TransactionModes modes = new TransactionModes();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
            if (matchWord("ISOLATION")) {
                matchWord("LEVEL");
                if (matchWord("READ")) {
                    if (matchWord("COMMITTED")) modes.isolationLevel = "read committed";
                    else if (matchWord("UNCOMMITTED")) modes.isolationLevel = "read uncommitted";
                    else throw syntaxErrorHere();
                } else if (matchWord("REPEATABLE")) {
                    if (!matchWord("READ")) throw syntaxErrorHere();
                    modes.isolationLevel = "repeatable read";
                } else if (matchWord("SERIALIZABLE")) {
                    modes.isolationLevel = "serializable";
                } else {
                    throw syntaxErrorHere();
                }
                modes.any = true;
            } else if (matchWord("READ")) {
                if (matchWord("ONLY")) modes.readOnly = true;
                else if (matchWord("WRITE")) modes.readOnly = false;
                else throw syntaxErrorHere();
                modes.any = true;
            } else if (matchWord("NOT")) {
                if (!matchWord("DEFERRABLE")) throw syntaxErrorHere();
                modes.deferrable = false;
                modes.any = true;
            } else if (matchWord("DEFERRABLE")) {
                modes.deferrable = true;
                modes.any = true;
            } else {
                break;
            }
            parser.match(TokenType.COMMA); // options can be comma-separated
        }
        // Anything left over is not a transaction mode. Stopping quietly would let BEGIN
        // garbage start a transaction, discarding a word the caller expected to be honoured.
        if (!atStatementEnd()) throw syntaxErrorHere();
        return modes;
    }

    /**
     * A constraint name in SET CONSTRAINTS, which PostgreSQL allows to be schema-qualified.
     * The qualification is kept so the lookup can honour it rather than searching for a schema.
     */
    private String readQualifiedConstraintName() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            name = name + "." + parser.readIdentifier();
        }
        return name;
    }

    /**
     * The mode list of a statement that is nothing but its modes.
     *
     * <p>{@code BEGIN} on its own is a statement; {@code SET TRANSACTION} on its own is not, and
     * PostgreSQL's grammar requires at least one mode after it.
     */
    private TransactionModes requireTransactionModes() {
        TransactionModes modes = parseTransactionModes();
        if (!modes.any) throw syntaxErrorHere();
        return modes;
    }

    /** True when nothing but the end of the statement is left. */
    private boolean atStatementEnd() {
        return parser.isAtEnd() || parser.check(TokenType.SEMICOLON) || parser.check(TokenType.EOF);
    }

    /** Reject anything the statement did not consume, the way PostgreSQL's grammar does. */
    private void expectStatementEnd() {
        if (!atStatementEnd()) throw syntaxErrorHere();
    }

    /** Match a bare word whether the lexer classified it as a keyword or an identifier. */
    private boolean matchWord(String word) {
        Token t = parser.peek();
        if ((t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER)
                && t.value().equalsIgnoreCase(word)) {
            parser.advance();
            return true;
        }
        return false;
    }

    private ParseException syntaxErrorHere() {
        return ParseException.at(parser.peek());
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
        boolean chain = parseAndChain();
        expectStatementEnd();
        return new TransactionStmt(TransactionStmt.TransactionAction.COMMIT, null, null, null, chain);
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
        boolean chain = parseAndChain();
        expectStatementEnd();
        return new TransactionStmt(TransactionStmt.TransactionAction.ROLLBACK, null, null, null, chain);
    }

    /** Parse optional AND [NO] CHAIN clause (PG 11+). */
    private boolean parseAndChain() {
        if (parser.matchKeyword("AND")) {
            if (parser.matchKeyword("CHAIN")) return true;
            if (parser.matchKeyword("NO")) parser.matchKeyword("CHAIN");
        }
        return false;
    }

    TransactionStmt parseSavepoint() {
        parser.expectKeyword("SAVEPOINT");
        // A savepoint is named the way a column is, so a reserved word is not a name for one:
        // SAVEPOINT ALL and SAVEPOINT select are syntax errors rather than savepoints.
        String name = parser.readColumnName();
        parser.expectEndOfStatement();
        return new TransactionStmt(TransactionStmt.TransactionAction.SAVEPOINT, name);
    }

    TransactionStmt parseReleaseSavepoint() {
        parser.expectKeyword("RELEASE");
        parser.matchKeyword("SAVEPOINT");
        String name = parser.readColumnName();
        return new TransactionStmt(TransactionStmt.TransactionAction.RELEASE_SAVEPOINT, name);
    }

    // ---- EXPLAIN ----

    /**
     * {@code EXPLAIN [ANALYZE [VERBOSE]] [VERBOSE] [(option, ...)] statement}.
     *
     * <p>The options are a list of name/value pairs, not a fixed sequence of keywords: a name may
     * be any word, a value may be missing, and what a value means is the option's business. Reading
     * them with one branch per keyword accepted {@code EXPLAIN ANALYZE (COSTS OFF)} — which is two
     * grammars at once — refused {@code COSTS 1} and {@code COSTS 'off'}, which are ordinary
     * boolean spellings, and let an empty list and a trailing comma through.
     *
     * <p>Only a statement that has a plan can be explained. Anything else is a syntax error at the
     * word that begins it, so {@code EXPLAIN (ANALYZE) TRUNCATE t} is refused rather than emptying
     * the table to find out how long it took.
     */
    ExplainStmt parseExplain() {
        parser.expectKeyword("EXPLAIN");
        List<ExplainOption> options = new ArrayList<>();
        boolean legacyForm = false;
        if (parser.checkKeyword("ANALYZE") || parser.checkKeyword("ANALYSE")) {
            parser.advance();
            options.add(new ExplainOption("analyze", null, false));
            legacyForm = true;
            if (parser.checkKeyword("VERBOSE")) {
                parser.advance();
                options.add(new ExplainOption("verbose", null, false));
            }
        } else if (parser.checkKeyword("VERBOSE")) {
            parser.advance();
            options.add(new ExplainOption("verbose", null, false));
            legacyForm = true;
        }
        // A parenthesis after the legacy spelling begins the query, never an option list.
        if (!legacyForm && parser.check(TokenType.LEFT_PAREN)
                && Math.max(0, parser.countLeadingParensBeforeQuery()) == 0) {
            parseExplainOptions(options);
        }

        Statement stmt = parseExplainableStatement();
        return buildExplain(stmt, options);
    }

    /** One {@code name [value]} pair from an EXPLAIN option list. */
    private static final class ExplainOption {
        final String name;
        final String value;
        /** True when the value was written as a number, which only 0 and 1 mean anything as. */
        final boolean numeric;
        ExplainOption(String name, String value, boolean numeric) {
            this.name = name;
            this.value = value;
            this.numeric = numeric;
        }
    }

    /** A token as the reader wrote it: a string constant keeps the quotes that made it one. */
    private static String asWritten(Token token) {
        if (token.type() == TokenType.STRING_LITERAL) return "'" + token.value() + "'";
        return token.raw();
    }

    private void parseExplainOptions(List<ExplainOption> options) {
        parser.expect(TokenType.LEFT_PAREN);
        while (true) {
            Token nameToken = parser.peek();
            if (nameToken.type() != TokenType.IDENTIFIER && nameToken.type() != TokenType.KEYWORD
                    && nameToken.type() != TokenType.QUOTED_IDENTIFIER) {
                throw ParseException.saying(SYNTAX_AT + asWritten(nameToken) + Q, nameToken, "42601");
            }
            parser.advance();
            // An unquoted word is folded to lower case, the way every identifier is; a quoted one
            // keeps what was written, so it matches no option and is reported as written.
            String name = nameToken.type() == TokenType.QUOTED_IDENTIFIER
                    ? nameToken.value() : nameToken.value().toLowerCase();
            String value = null;
            boolean numeric = false;
            if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                Token valueToken = parser.peek();
                if (valueToken.type() == TokenType.INTEGER_LITERAL
                        || valueToken.type() == TokenType.FLOAT_LITERAL) {
                    parser.advance();
                    value = valueToken.value();
                    numeric = true;
                } else if (valueToken.type() == TokenType.MINUS || valueToken.type() == TokenType.PLUS) {
                    parser.advance();
                    Token number = parser.advance();
                    value = (valueToken.type() == TokenType.MINUS ? "-" : "") + number.value();
                    numeric = true;
                } else if (valueToken.type() == TokenType.IDENTIFIER
                        || valueToken.type() == TokenType.KEYWORD) {
                    parser.advance();
                    value = valueToken.value().toLowerCase();
                } else if (valueToken.type() == TokenType.STRING_LITERAL
                        || valueToken.type() == TokenType.QUOTED_IDENTIFIER) {
                    parser.advance();
                    value = valueToken.value();
                } else {
                    throw ParseException.saying(SYNTAX_AT + asWritten(valueToken) + Q, valueToken, "42601");
                }
            }
            options.add(new ExplainOption(name, value, numeric));
            if (parser.match(TokenType.COMMA)) continue;
            break;
        }
        Token closing = parser.peek();
        if (!parser.check(TokenType.RIGHT_PAREN)) {
            throw ParseException.saying(SYNTAX_AT + closing.raw() + Q, closing, "42601");
        }
        parser.advance();
    }

    /**
     * The statements that have a plan. PostgreSQL's grammar admits exactly these after EXPLAIN;
     * every other statement is a syntax error at its first word.
     */
    private Statement parseExplainableStatement() {
        Token first = parser.peek();
        if (parser.isAtEnd() || first.type() == TokenType.EOF) {
            throw ParseException.saying("syntax error at end of input", first, "42601");
        }
        if (parser.check(TokenType.LEFT_PAREN)) return parser.parseStatement();
        String word = first.value() == null ? "" : first.value().toUpperCase();
        if (word.equals("SELECT") || word.equals("VALUES") || word.equals("TABLE")
                || word.equals("WITH") || word.equals("INSERT") || word.equals("UPDATE")
                || word.equals("DELETE") || word.equals("MERGE") || word.equals("DECLARE")
                || word.equals("EXECUTE")) {
            return parser.parseStatement();
        }
        if (word.equals("CREATE")) {
            // CREATE TABLE ... AS and CREATE MATERIALIZED VIEW ... AS carry a query; every other
            // CREATE does not, and fails at the word that follows CREATE.
            int offset = 1;
            while (parser.checkKeywordAt(offset, "TEMP") || parser.checkKeywordAt(offset, "TEMPORARY")
                    || parser.checkKeywordAt(offset, "UNLOGGED") || parser.checkKeywordAt(offset, "GLOBAL")
                    || parser.checkKeywordAt(offset, "LOCAL")) {
                offset++;
            }
            if (parser.checkKeywordAt(offset, "TABLE")) {
                requireCreateAsShape(offset + 1);
                return parser.parseStatement();
            }
            if (parser.checkKeywordAt(offset, "MATERIALIZED")) {
                return parser.parseStatement();
            }
            Token bad = parser.peekAt(offset);
            throw ParseException.saying(SYNTAX_AT + bad.raw() + Q, bad, "42601");
        }
        if (word.equals("REFRESH")) return parser.parseStatement();
        throw ParseException.saying(SYNTAX_AT + first.raw() + Q, first, "42601");
    }

    /**
     * Only {@code CREATE TABLE name [(col, ...)] AS query} can be explained, so the parenthesised
     * list after the name holds column names alone. A column definition there — a name with a type
     * after it — is the point at which PostgreSQL stops reading.
     */
    private void requireCreateAsShape(int offset) {
        int at = offset;
        // qualified name: ident [. ident]*
        at++;
        while (parser.peekAt(at).type() == TokenType.DOT) at += 2;
        if (parser.peekAt(at).type() == TokenType.LEFT_PAREN) {
            at++;
            while (true) {
                Token name = parser.peekAt(at);
                if (name.type() == TokenType.RIGHT_PAREN) { at++; break; }
                at++;
                Token next = parser.peekAt(at);
                if (next.type() == TokenType.COMMA) { at++; continue; }
                if (next.type() == TokenType.RIGHT_PAREN) { at++; break; }
                throw ParseException.saying(SYNTAX_AT + next.raw() + Q, next, "42601");
            }
        }
        if (!parser.checkKeywordAt(at, "AS")) {
            Token bad = parser.peekAt(at);
            throw ParseException.saying(SYNTAX_AT + bad.raw() + Q, bad, "42601");
        }
    }

    /**
     * Apply the option list. A name that is not an option, and a value an option cannot use, are
     * reported only after the statement itself has been analysed: PostgreSQL reads the query first,
     * so a missing table is what a reader hears about before a misspelled option.
     */
    private ExplainStmt buildExplain(Statement stmt, List<ExplainOption> options) {
        boolean analyze = false;
        boolean verbose = false;
        boolean costs = true;
        boolean buffers = false;
        boolean wal = false;
        boolean settings = false;
        boolean memory = false;
        boolean genericPlan = false;
        boolean timing = false;
        boolean summary = false;
        boolean timingSet = false;
        boolean summarySet = false;
        String serializeMode = "none";
        String format = "TEXT";
        String deferredError = null;
        String deferredSqlState = null;

        for (ExplainOption opt : options) {
            try {
                if (opt.name.equals("analyze") || opt.name.equals("analyse")) analyze = booleanOption(opt);
                else if (opt.name.equals("verbose")) verbose = booleanOption(opt);
                else if (opt.name.equals("costs")) costs = booleanOption(opt);
                else if (opt.name.equals("buffers")) buffers = booleanOption(opt);
                else if (opt.name.equals("wal")) wal = booleanOption(opt);
                else if (opt.name.equals("settings")) settings = booleanOption(opt);
                else if (opt.name.equals("memory")) memory = booleanOption(opt);
                else if (opt.name.equals("generic_plan")) genericPlan = booleanOption(opt);
                else if (opt.name.equals("timing")) { timing = booleanOption(opt); timingSet = true; }
                else if (opt.name.equals("summary")) { summary = booleanOption(opt); summarySet = true; }
                else if (opt.name.equals("serialize")) serializeMode = serializeOption(opt);
                else if (opt.name.equals("format")) format = formatOption(opt);
                else {
                    throw new ExplainOptionError(
                            "unrecognized EXPLAIN option \"" + opt.name + "\"", "42601");
                }
            } catch (ExplainOptionError e) {
                if (deferredError == null) {
                    deferredError = e.getMessage();
                    deferredSqlState = e.sqlState;
                }
            }
        }
        if (deferredError == null) {
            if (wal && !analyze) {
                deferredError = "EXPLAIN option WAL requires ANALYZE";
                deferredSqlState = "22023";
            } else if (!serializeMode.equals("none") && !analyze) {
                deferredError = "EXPLAIN option SERIALIZE requires ANALYZE";
                deferredSqlState = "22023";
            } else if (timing && !analyze) {
                deferredError = "EXPLAIN option TIMING requires ANALYZE";
                deferredSqlState = "22023";
            } else if (genericPlan && analyze) {
                deferredError = "EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together";
                deferredSqlState = "22023";
            }
        }
        if (!timingSet) timing = analyze;
        if (!summarySet) summary = analyze;

        ExplainStmt explain = new ExplainStmt(stmt, analyze, verbose, format, costs,
                deferredError, deferredSqlState, memory, !serializeMode.equals("none"),
                genericPlan, buffers, wal);
        explain.settings = settings;
        explain.timing = timing;
        explain.summary = summary;
        explain.serializeMode = serializeMode;
        return explain;
    }

    /** An option error, carried until the statement itself has been read. */
    private static final class ExplainOptionError extends RuntimeException {
        final String sqlState;
        ExplainOptionError(String message, String sqlState) {
            super(message);
            this.sqlState = sqlState;
        }
    }

    /**
     * The values a boolean option takes: nothing at all, the numbers 0 and 1, and the four words
     * {@code true}, {@code false}, {@code on} and {@code off} however they were quoted. Anything
     * else — {@code yes}, {@code t}, {@code 2} — is not a spelling PostgreSQL reads as a boolean.
     */
    private boolean booleanOption(ExplainOption opt) {
        if (opt.value == null) return true;
        if (opt.numeric) {
            if (opt.value.equals("0")) return false;
            if (opt.value.equals("1")) return true;
        } else {
            if (opt.value.equalsIgnoreCase("true")) return true;
            if (opt.value.equalsIgnoreCase("false")) return false;
            if (opt.value.equalsIgnoreCase("on")) return true;
            if (opt.value.equalsIgnoreCase("off")) return false;
        }
        throw new ExplainOptionError(opt.name + " requires a Boolean value", "42601");
    }

    private String serializeOption(ExplainOption opt) {
        if (opt.value == null) return "text";
        String v = opt.value;
        if (v.equals("off") || v.equals("none")) return "none";
        if (v.equals("text")) return "text";
        if (v.equals("binary")) return "binary";
        throw new ExplainOptionError("unrecognized value for EXPLAIN option \"serialize\": \""
                + opt.value + "\"", "22023");
    }

    private String formatOption(ExplainOption opt) {
        if (opt.value == null) {
            throw new ExplainOptionError("format requires a parameter", "42601");
        }
        String v = opt.value;
        if (v.equals("text") || v.equals("xml") || v.equals("json") || v.equals("yaml")) {
            return v.toUpperCase();
        }
        throw new ExplainOptionError("unrecognized value for EXPLAIN option \"format\": \""
                + opt.value + "\"", "22023");
    }

    // ---- LISTEN / NOTIFY / UNLISTEN ----

    ListenStmt parseListen() {
        parser.expectKeyword("LISTEN");
        // A channel is named the way a column is: one plain identifier, no list, no literal.
        String channel = parser.readObjectName();
        parser.expectEndOfStatement();
        return new ListenStmt(channel);
    }

    NotifyStmt parseNotify() {
        parser.expectKeyword("NOTIFY");
        String channel = parser.readObjectName();
        String payload = null;
        if (parser.match(TokenType.COMMA)) {
            Token payloadToken = parser.peek();
            // The payload is one string constant, however it is quoted; it is not an expression.
            if (payloadToken.type() != TokenType.STRING_LITERAL
                    && payloadToken.type() != TokenType.DOLLAR_STRING_LITERAL) {
                throw new ParseException(SYNTAX_AT + asWritten(payloadToken) + Q, payloadToken);
            }
            payload = parser.advance().value();
        }
        parser.expectEndOfStatement();
        return new NotifyStmt(channel, payload);
    }

    UnlistenStmt parseUnlisten() {
        parser.expectKeyword("UNLISTEN");
        if (parser.match(TokenType.STAR)) {
            parser.expectEndOfStatement();
            return new UnlistenStmt(null);
        }
        String channel = parser.readObjectName();
        parser.expectEndOfStatement();
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
            // PREPARE p () AS ... has an empty list where a type belongs; PostgreSQL's grammar
            // has no production for it, so it fails at the parenthesis that closes nothing.
            if (parser.check(TokenType.RIGHT_PAREN)) {
                Token t = parser.peek();
                throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
            }
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
        String name = parser.readObjectName();
        // The options are a list, in any order and repeatable, as PostgreSQL's cursor_options is:
        // reading them as a fixed sequence made "SCROLL BINARY" and "SCROLL INSENSITIVE" syntax
        // errors, and let "SCROLL NO SCROLL" through as a plain no-scroll cursor.
        boolean binary = false;
        boolean scroll = false;
        boolean explicitNoScroll = false;
        while (true) {
            if (parser.matchWord("BINARY")) { binary = true; continue; }
            if (parser.matchWord("INSENSITIVE")) { continue; }
            if (parser.matchWord("ASENSITIVE")) { continue; }
            if (parser.matchWord("SCROLL")) { scroll = true; continue; }
            if (parser.checkKeyword("NO")) {
                parser.advance();
                parser.expectKeyword("SCROLL");
                explicitNoScroll = true;
                continue;
            }
            break;
        }
        if (scroll && explicitNoScroll) {
            throw new MemgresException("cannot specify both SCROLL and NO SCROLL", "42P11");
        }
        parser.expectKeyword("CURSOR");
        // Optional: WITH HOLD / WITHOUT HOLD. HOLD is not optional after either word.
        boolean withHold = false;
        if (parser.matchKeyword("WITH")) {
            parser.expectKeyword("HOLD");
            withHold = true;
        } else if (parser.matchKeyword("WITHOUT")) {
            parser.expectKeyword("HOLD");
        }
        parser.expectKeyword("FOR");
        int cursorExtraParens = Math.max(0, parser.countLeadingParensBeforeQuery());
        parser.consumeLeadingParens(cursorExtraParens);
        // A cursor may be declared for any query, VALUES included
        Statement query = parser.parseSubqueryWithSetOpsPublic();
        parser.consumeTrailingParens(cursorExtraParens);
        return new DeclareCursorStmt(name, query, scroll, withHold, binary, explicitNoScroll);
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
            } else if (isSignedCount()) {
                direction = FetchStmt.Direction.FORWARD;
                count = parseFetchCount();
            } else {
                direction = FetchStmt.Direction.FORWARD;
            }
        } else if (parser.matchKeyword("BACKWARD")) {
            if (parser.matchKeyword("ALL")) {
                direction = FetchStmt.Direction.BACKWARD_ALL;
            } else if (isSignedCount()) {
                direction = FetchStmt.Direction.BACKWARD;
                count = parseFetchCount();
            } else {
                direction = FetchStmt.Direction.BACKWARD;
            }
        } else if (parser.matchKeyword("ALL")) {
            direction = FetchStmt.Direction.ALL;
        } else if (isSignedCount()) {
            // FETCH count [IN|FROM] cursor
            count = parseFetchCount();
            direction = count >= 0 ? FetchStmt.Direction.FORWARD : FetchStmt.Direction.BACKWARD;
            if (count < 0) count = -count;
        }

        parser.matchKeyword("FROM");
        parser.matchKeyword("IN");
        String cursorName = parser.readObjectName();
        parser.expectEndOfStatement();
        return new FetchStmt(direction, count, cursorName, isMove);
    }

    /** Whether a signed integer count stands here. */
    private boolean isSignedCount() {
        TokenType t = parser.peek().type();
        if (t == TokenType.INTEGER_LITERAL) return true;
        if (t != TokenType.MINUS && t != TokenType.PLUS) return false;
        return parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).type() == TokenType.INTEGER_LITERAL;
    }

    /**
     * A fetch count, which PostgreSQL's grammar takes as a signed integer constant. A number too
     * large to be one is a syntax error at the number rather than a NumberFormatException reaching
     * the client as an internal error, and a leading plus is part of the constant.
     */
    int parseFetchCount() {
        boolean negative = parser.match(TokenType.MINUS);
        if (!negative) parser.match(TokenType.PLUS);
        Token tok = parser.advance();
        long val;
        try {
            val = Long.parseLong(tok.value());
        } catch (NumberFormatException e) {
            throw ParseException.saying(SYNTAX_AT + tok.value() + Q, tok, "42601");
        }
        if (negative) val = -val;
        if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE) {
            throw ParseException.saying(SYNTAX_AT + tok.value() + Q, tok, "42601");
        }
        return (int) val;
    }

    CloseStmt parseClose() {
        parser.expectKeyword("CLOSE");
        if (parser.matchKeyword("ALL")) {
            parser.expectEndOfStatement();
            return new CloseStmt(null, true);
        }
        String name = parser.readObjectName();
        parser.expectEndOfStatement();
        return new CloseStmt(name, false);
    }

    // ---- LOCK TABLE ----

    LockStmt parseLock() {
        parser.expectKeyword("LOCK");
        parser.matchKeyword("TABLE");
        parser.matchKeyword("ONLY");
        // Read comma-separated list of table names:
        // LOCK TABLE t1, schema.t2, t3 IN ACCESS SHARE MODE NOWAIT
        java.util.List<String> tableNames = new java.util.ArrayList<>();
        do {
            parser.matchKeyword("ONLY"); // ONLY can appear before each table
            String tableName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) {
                tableName = tableName + "." + parser.readIdentifier(); // schema.table
            }
            tableNames.add(tableName);
        } while (parser.match(TokenType.COMMA));
        String lockMode = "ACCESS EXCLUSIVE"; // default
        if (parser.matchKeyword("IN")) {
            StringBuilder mode = new StringBuilder();
            // Read lock mode keywords until MODE keyword
            while (!parser.checkKeyword("MODE") && !parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                if (mode.length() > 0) mode.append(" ");
                mode.append(parser.advance().value());
            }
            parser.matchKeyword("MODE");
            lockMode = mode.toString();
        }
        boolean nowait = parser.matchKeyword("NOWAIT");
        return new LockStmt(tableNames, lockMode, nowait);
    }

    // ---- GRANT ----

    GrantStmt parseGrant() {
        parser.expectKeyword("GRANT");
        return parseGrantInner();
    }

    GrantStmt parseGrantInner() {
        List<String> privileges = new ArrayList<>();
        List<String> columns = null;

        // Read privileges: ALL [PRIVILEGES], SELECT, INSERT, UPDATE, DELETE, ALTER SYSTEM, etc.
        do {
            String priv = parser.readIdentifier().toUpperCase();
            if (priv.equals("ALL")) {
                parser.matchKeyword("PRIVILEGES");
                privileges.add("ALL");
            } else if (priv.equals("ALTER") && parser.checkKeyword("SYSTEM")) {
                parser.advance(); // consume SYSTEM
                privileges.add("ALTER SYSTEM");
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

        // GRANT SET/ALTER SYSTEM ON PARAMETER param_name[, ...] TO role (PG 15+)
        if (parser.matchIdentifier("PARAMETER")) {
            String paramName = parser.readIdentifier();
            // Consume additional parameter names separated by commas
            while (parser.match(TokenType.COMMA)) {
                paramName = paramName + "," + parser.readIdentifier();
            }
            parser.expectKeyword("TO");
            java.util.List<String> paramGrantees = new java.util.ArrayList<>();
            paramGrantees.add(readGrantee());
            while (parser.match(TokenType.COMMA)) {
                paramGrantees.add(readGrantee());
            }
            return new GrantStmt(privileges, "PARAMETER", paramName, paramGrantees, false, false, false, null);
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
            // M10: Preserve schema prefix for schema-qualified tables
            if (parser.match(TokenType.DOT)) objectName = objectName + "." + parser.readIdentifier();
        } else if (parser.matchKeyword("SEQUENCE")) {
            objectType = "SEQUENCE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("FUNCTION") || parser.matchKeyword("PROCEDURE") || parser.matchKeyword("ROUTINE")) {
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
        } else if (parser.matchKeyword("FOREIGN")) {
            // FOREIGN DATA WRAPPER name or FOREIGN SERVER name
            if (parser.matchKeyword("DATA")) {
                parser.expectKeyword("WRAPPER");
                objectType = "FOREIGN DATA WRAPPER";
                objectName = parser.readIdentifier();
            } else {
                parser.expectKeyword("SERVER");
                objectType = "FOREIGN SERVER";
                objectName = parser.readIdentifier();
            }
        } else if (parser.matchKeyword("LARGE")) {
            parser.expectKeyword("OBJECT");
            objectType = "LARGE OBJECT";
            objectName = parser.advance().value(); // OID is a number, not an identifier
        } else {
            objectName = parser.readIdentifier();
            // M10: Preserve schema prefix for default TABLE path
            if (parser.match(TokenType.DOT)) objectName = objectName + "." + parser.readIdentifier();
        }

        parser.expectKeyword("TO");
        List<String> grantees = new ArrayList<>();
        do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));

        boolean withGrantOption = false;
        if (parser.matchKeywords("WITH", "GRANT")) {
            parser.expectKeyword("OPTION");
            withGrantOption = true;
        }

        // Consume optional GRANTED BY role and capture the grantor name
        // Note: GRANTED is not a reserved keyword, so it is tokenized as an IDENTIFIER
        String grantor = null;
        if (parser.matchIdentifier("GRANTED")) {
            parser.expectKeyword("BY");
            grantor = parser.readIdentifier();
        }

        return new GrantStmt(privileges, objectType, objectName, grantees, withGrantOption, false, false, columns, grantor);
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
        List<String> revokeColumns = null;
        do {
            String priv = parser.readIdentifier().toUpperCase();
            if (priv.equals("ALL")) {
                parser.matchKeyword("PRIVILEGES");
                privileges.add("ALL");
            } else {
                privileges.add(priv);
            }
            // Capture column-level parens
            if (parser.check(TokenType.LEFT_PAREN) && !parser.checkKeyword("ON") && !parser.checkKeyword("FROM")) {
                revokeColumns = new ArrayList<>();
                parser.advance(); // consume '('
                do {
                    revokeColumns.add(parser.readIdentifier());
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
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
            // M10: Preserve schema prefix
            if (parser.match(TokenType.DOT)) objectName = objectName + "." + parser.readIdentifier();
        } else if (parser.matchKeyword("SEQUENCE")) {
            objectType = "SEQUENCE";
            objectName = parser.readIdentifier();
        } else if (parser.matchKeyword("FUNCTION") || parser.matchKeyword("PROCEDURE") || parser.matchKeyword("ROUTINE")) {
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
        } else if (parser.matchKeyword("FOREIGN")) {
            if (parser.matchKeyword("DATA")) {
                parser.expectKeyword("WRAPPER");
                objectType = "FOREIGN DATA WRAPPER";
                objectName = parser.readIdentifier();
            } else {
                parser.expectKeyword("SERVER");
                objectType = "FOREIGN SERVER";
                objectName = parser.readIdentifier();
            }
        } else if (parser.matchKeyword("LARGE")) {
            parser.expectKeyword("OBJECT");
            objectType = "LARGE OBJECT";
            objectName = parser.advance().value(); // OID is a number, not an identifier
        } else {
            objectName = parser.readIdentifier();
            // M10: Preserve schema prefix
            if (parser.match(TokenType.DOT)) objectName = objectName + "." + parser.readIdentifier();
        }

        parser.expectKeyword("FROM");
        List<String> grantees = new ArrayList<>();
        do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));

        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");

        return new RevokeStmt(privileges, objectType, objectName, grantees, grantOptionFor, false, cascade, revokeColumns);
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

    /**
     * {@code DO [LANGUAGE lang] body [LANGUAGE lang]}.
     *
     * <p>PostgreSQL reads this as a list of items — a body and at most one language — so both
     * orders are allowed and naming the language twice is a redundant option. Reading two tokens
     * and discarding them accepted a language that does not exist, a language that has no inline
     * form, a body that is not a string at all, and anything written after the block.
     */
    SetStmt parseDo() {
        parser.expectKeyword("DO");
        String language = null;
        String body = null;
        while (true) {
            if (parser.checkKeyword("LANGUAGE")) {
                parser.advance();
                if (language != null) {
                    throw ParseException.saying("conflicting or redundant options", parser.peek(), "42601");
                }
                language = parser.readIdentifierOrString();
                continue;
            }
            Token t = parser.peek();
            if (t.type() == TokenType.STRING_LITERAL || t.type() == TokenType.DOLLAR_STRING_LITERAL) {
                if (body != null) {
                    throw ParseException.saying("conflicting or redundant options", t, "42601");
                }
                body = parser.advance().value();
                continue;
            }
            break;
        }
        if (body == null) {
            Token t = parser.peek();
            if (parser.isAtEnd() || t.type() == TokenType.SEMICOLON || t.type() == TokenType.EOF) {
                throw ParseException.saying("syntax error at end of input", t, "42601");
            }
            throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
        }
        parser.expectEndOfStatement();
        // The language travels with the body so the executor can refuse one that has no inline
        // form; a body with nothing in it has no block to compile and PostgreSQL says so.
        if (body.trim().isEmpty()) {
            throw ParseException.saying("syntax error at end of input", parser.peek(), "42601");
        }
        SetStmt doStmt = new SetStmt("do_block", body);
        doStmt.setAuxiliary(language);
        return doStmt;
    }

    // ---- RESET ----

    SetStmt parseReset() {
        parser.expectKeyword("RESET");
        if (parser.matchKeyword("ALL")) {
            return new SetStmt("reset", "ALL");
        }
        String param = parser.readIdentifier();
        // RESET SESSION AUTHORIZATION → combine into "session_authorization"
        if (param.equalsIgnoreCase("session") && parser.checkKeyword("AUTHORIZATION")) {
            parser.advance(); // consume AUTHORIZATION
            param = "session_authorization";
        }
        // RESET TIME ZONE undoes SET TIME ZONE, and names the same setting.
        if (param.equalsIgnoreCase("TIME") && matchZone()) {
            return new SetStmt("reset", "TimeZone");
        }
        if (parser.match(TokenType.DOT)) {
            param = param + "." + parser.readIdentifier();
        }
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
        // SHOW TIME ZONE is how SET TIME ZONE is read back; the setting itself is TimeZone.
        if (param.equalsIgnoreCase("TIME") && matchZone()) {
            return new SetStmt("show", "TimeZone");
        }
        return new SetStmt("show", param);
    }

    /** ZONE is not a reserved word everywhere, so match it on the word rather than the kind. */
    private boolean matchZone() {
        if (parser.isAtEnd()) return false;
        if (!"ZONE".equalsIgnoreCase(parser.peek().value())) return false;
        parser.advance();
        return true;
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
                    tokenValues.add(prev + "." + identifierSpelling(next));
                } else {
                    tokenValues.add(prev + ".");
                }
            } else {
                tokenValues.add(identifierSpelling(tok));
            }
        }
        // A routine's argument list says which routine of that name this is, so it travels with
        // the name. Dropping it meant a comment on one overload was a comment on whichever
        // overload happened to be found, and a signature that matches none was not noticed.
        int paren = tokenValues.indexOf("(");
        String argumentList = null;
        if (paren > 0) {
            StringBuilder args = new StringBuilder();
            for (int i = paren; i < tokenValues.size(); i++) {
                String piece = tokenValues.get(i);
                if (piece.equals("(") || piece.equals(")")) args.append(piece);
                else if (piece.equals(",")) args.append(", ");
                else args.append(args.length() > 0 && args.charAt(args.length() - 1) != '('
                        && !args.toString().endsWith(", ") ? " " : "").append(piece);
            }
            argumentList = args.toString();
            tokenValues = new ArrayList<>(tokenValues.subList(0, paren));
        }
        String objectType;
        String objectName;
        // CONSTRAINT c ON t / TRIGGER t ON r / RULE r ON t / POLICY p ON t name the object
        // relative to a relation, so key them as "<relation>.<object>"
        int onIdx = -1;
        for (int i = 0; i < tokenValues.size(); i++) {
            if ("ON".equalsIgnoreCase(tokenValues.get(i))) { onIdx = i; break; }
        }
        if (onIdx == 2 && tokenValues.size() == 4) {
            objectType = tokenValues.get(0);
            // The relation keeps whatever qualifier it was written with: COMMENT ON CONSTRAINT c
            // ON a.t names a's t, and dropping the "a" filed the comment under whichever t the
            // search path happened to reach.
            objectName = tokenValues.get(3) + "." + tokenValues.get(1);
        } else {
            objectType = tokenValues.size() > 1
                    ? String.join(" ", tokenValues.subList(0, tokenValues.size() - 1))
                    : "TABLE";
            objectName = !tokenValues.isEmpty()
                    ? tokenValues.get(tokenValues.size() - 1) : "";
            if (argumentList != null) objectName = objectName + argumentList;
        }
        parser.expectKeyword("IS");
        String comment = null;
        if (parser.matchKeyword("NULL")) {
            comment = null;
        } else if (parser.check(TokenType.STRING_LITERAL)
                || parser.check(TokenType.DOLLAR_STRING_LITERAL)) {
            comment = parser.advance().value();
        } else {
            // A comment is one string constant or NULL, never an expression. Swallowing the rest
            // of the line instead accepted 'a' || 'b', 42 and current_user, and filed nothing.
            Token text = parser.peek();
            throw ParseException.saying(SYNTAX_AT + asWritten(text) + Q, text, "42601");
        }
        parser.expectEndOfStatement();
        // Store the comment (null for IS NULL to trigger removal)
        return new SetStmt("comment:" + objectType + ":" + objectName, comment);
    }

    // ---- SECURITY LABEL ----

    SetStmt parseSecurityLabel() {
        parser.expectKeyword("SECURITY");
        parser.expectKeyword("LABEL");
        // SECURITY LABEL [FOR provider] ON object IS 'label'
        String provider = null;
        if (parser.matchKeyword("FOR")) {
            provider = parser.readIdentifier();
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        String value = provider != null ? "provider:" + provider : "ok";
        return new SetStmt("security_label", value);
    }

    /**
     * PG 18 lets VACUUM and ANALYZE name a relation as {@code ONLY t}, meaning the relation itself
     * and not its inheritance children. Nothing here descends into children anyway, so the word is
     * read and discarded — but it has to be read, or it is taken for the relation's name. Only an
     * unquoted ONLY that is followed by something counts: {@code VACUUM "only"} still names a
     * table, since the lexer hands a quoted word over as an identifier rather than a keyword.
     */
    private void matchOnlyBeforeRelation() {
        if (!parser.checkKeyword("ONLY")) return;
        int next = parser.pos + 1;
        boolean nothingFollows = next >= parser.tokens.size();
        if (!nothingFollows) {
            TokenType t = parser.tokens.get(next).type();
            nothingFollows = t == TokenType.SEMICOLON || t == TokenType.EOF;
        }
        // ONLY is a reserved word, so it can never be the relation it was meant to qualify.
        if (nothingFollows) {
            throw ParseException.at(next < parser.tokens.size() ? parser.tokens.get(next) : null);
        }
        parser.advance();
    }

    // ---- ANALYZE ----

    SetStmt parseAnalyze() {
        parser.advance(); // ANALYZE or ANALYSE
        // ANALYZE [VERBOSE] [table [(column, ...)]]
        parser.matchKeyword("VERBOSE");
        String tableName = null;
        String columnList = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            matchOnlyBeforeRelation();
            tableName = parser.readIdentifier(); // table name
            if (parser.match(TokenType.DOT)) tableName = tableName + "." + parser.readIdentifier();
            // Optional column list
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); // (
                StringBuilder cols = new StringBuilder();
                while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                    if (cols.length() > 0) { parser.expect(TokenType.COMMA); cols.append(","); }
                    cols.append(parser.readIdentifier());
                }
                parser.expect(TokenType.RIGHT_PAREN);
                columnList = cols.toString();
            }
        }
        String value = tableName != null ? "table:" + tableName : "ok";
        if (columnList != null) value += ",columns:" + columnList;
        return new SetStmt("analyze", value);
    }

    // ---- VACUUM ----

    private static final Set<String> VALID_VACUUM_OPTIONS = Cols.setOf(
            "FULL", "FREEZE", "VERBOSE", "ANALYZE", "ANALYSE",
            "DISABLE_PAGE_SKIPPING", "SKIP_LOCKED", "PROCESS_TOAST", "PROCESS_MAIN",
            "TRUNCATE", "PARALLEL", "INDEX_CLEANUP", "BUFFER_USAGE_LIMIT", "SKIP_DATABASE_STATS");

    SetStmt parseVacuum() {
        parser.expectKeyword("VACUUM");
        // VACUUM [(options)] [table [(column, ...)]]
        boolean hasAnalyze = false;
        boolean hasVerbose = false;
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // (
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                parser.match(TokenType.COMMA);
                if (parser.check(TokenType.RIGHT_PAREN)) break;
                Token optToken = parser.advance();
                String opt = optToken.value().toUpperCase();
                if (!VALID_VACUUM_OPTIONS.contains(opt)) {
                    // ParseException(message, token) reports the token and drops the message, so
                    // the option that was not recognised was reported as a plain syntax error.
                    // An option name is folded like any other unquoted word.
                    throw ParseException.saying("unrecognized VACUUM option \""
                            + optToken.value().toLowerCase(java.util.Locale.ROOT) + "\"",
                            optToken, "42601");
                }
                if (opt.equals("ANALYZE") || opt.equals("ANALYSE")) hasAnalyze = true;
                if (opt.equals("VERBOSE")) hasVerbose = true;
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
            if (parser.matchKeyword("VERBOSE")) hasVerbose = true;
            if (parser.matchKeyword("ANALYZE") || parser.matchKeyword("ANALYSE")) hasAnalyze = true;
        }
        // Optional table name
        String vacuumTable = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            matchOnlyBeforeRelation();
            vacuumTable = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) vacuumTable = vacuumTable + "." + parser.readIdentifier();
            if (parser.check(TokenType.LEFT_PAREN)) parser.consumeUntilParen();
        }
        // Encode flags + table into the value string
        String value = vacuumTable != null ? "table:" + vacuumTable : "ok";
        if (hasAnalyze) value = "analyze," + value;
        if (hasVerbose) value = "verbose," + value;
        return new SetStmt("vacuum", value);
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
                if (parser.match(TokenType.DOT)) targetName = targetName + "." + parser.readIdentifier();
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
        String tableName = null;
        String indexName = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            tableName = parser.readIdentifier(); // table name
            if (parser.match(TokenType.DOT)) tableName = tableName + "." + parser.readIdentifier();
            if (parser.matchKeyword("USING")) {
                indexName = parser.readIdentifier(); // index name
            }
        }
        String value = "ok";
        if (tableName != null && indexName != null) {
            value = "table:" + tableName + ",index:" + indexName;
        } else if (tableName != null) {
            value = "table:" + tableName;
        }
        return new SetStmt("cluster", value);
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
        // IMPORT FOREIGN SCHEMA <schema_name> [LIMIT TO (...) | EXCEPT (...)] FROM SERVER <server_name> INTO <local_schema>
        parser.expectKeyword("IMPORT");
        parser.expectKeyword("FOREIGN");
        parser.expectKeyword("SCHEMA");
        // Skip the remote schema name
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)
                && !parser.checkKeyword("FROM")) {
            parser.advance();
        }
        // Extract server name from FROM SERVER <name>
        String serverName = null;
        if (parser.matchKeyword("FROM")) {
            parser.expectKeyword("SERVER");
            serverName = parser.readIdentifier();
        }
        // Consume the rest of the statement
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("import_foreign_schema", serverName != null ? serverName : "");
    }

    /** Read a grantee name, which may be an identifier or a keyword like CURRENT_USER, SESSION_USER, PUBLIC. */
    private String readGrantee() {
        if (parser.matchKeyword("CURRENT_USER")) return "current_user";
        if (parser.matchKeyword("SESSION_USER")) return "session_user";
        if (parser.matchKeyword("CURRENT_ROLE")) return "current_user";
        if (parser.matchKeyword("PUBLIC")) return "public";
        return parser.readIdentifier();
    }
}
