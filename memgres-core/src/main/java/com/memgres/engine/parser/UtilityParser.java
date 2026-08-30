package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.Arrays;
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
        // The target is either a query in parentheses or a relation with an optional column
        // list. Both take the same options and the same trailing clauses, so both are read here:
        // a second, smaller loop is a second set of rules for the same syntax, and the query form
        // was accepting options and a trailing WHERE that PostgreSQL refuses.
        Statement subquery = null;
        String table = null;
        List<String> columns = null;
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // consume (
            subquery = parser.parseStatement();
            parser.expect(TokenType.RIGHT_PAREN);
        } else {
            // Table name: handle schema-qualified names (schema.table)
            table = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) {
                // schema.table: preserve schema-qualified name
                table = table + "." + parser.readIdentifier();
            }
            // Optional column list
            if (parser.match(TokenType.LEFT_PAREN)) {
                columns = new ArrayList<>();
                do {
                    columns.add(parser.readIdentifier());
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            }
        }

        // A query is a source of rows and nothing else, so only the relation form reads FROM.
        boolean isFrom = false;
        if (subquery == null && parser.matchKeyword("FROM")) {
            isFrom = true;
        } else {
            parser.expectKeyword("TO");
        }

        // Source: filename string, STDIN/STDOUT, or PROGRAM 'command'
        String source;
        if (parser.check(TokenType.STRING_LITERAL)) {
            source = parser.advance().value();
        } else {
            source = parser.readIdentifier().toUpperCase(java.util.Locale.ROOT);
        }
        // Handle PROGRAM keyword: consume the program command string
        if ("PROGRAM".equals(source)) {
            if (parser.check(TokenType.STRING_LITERAL)) {
                parser.advance(); // consume command string, keep source as "PROGRAM"
            }
        }

        // Optional WITH (options)
        CopyOptions opts = new CopyOptions();
        if (parser.matchKeyword("WITH") || parser.check(TokenType.LEFT_PAREN)) {
            boolean parenthesised = parser.check(TokenType.LEFT_PAREN);
            if (parenthesised) {
                parser.advance(); // consume '('
            }
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                try {
                    readCopyOption(opts, isFrom, parenthesised);
                } catch (MemgresException e) {
                    // The list is still read to its end: this is a complaint about the options,
                    // and PostgreSQL does not make it until the relation is open.
                    opts.defer(e);
                    skipRestOfOptionList();
                    break;
                }
                parser.match(TokenType.COMMA);
            }
            parser.match(TokenType.RIGHT_PAREN);
        }

        // Handle CSV keyword without WITH (old-style syntax)
        if (parser.matchKeyword("CSV")) {
            opts.format = "csv";
            if (parser.matchKeyword("HEADER")) opts.header = true;
        }

        // Handle BINARY keyword without WITH
        if (parser.matchKeyword("BINARY")) {
            opts.format = "binary";
        }

        // PostgreSQL settles the options against each other and against the direction before it
        // reads or writes anything, so a statement it will not run moves no data at all -- but
        // after it has opened the relation, so the refusal waits with the rest of them.
        try {
            opts.validate(isFrom);
        } catch (MemgresException e) {
            opts.defer(e);
        }

        // WHERE picks the rows a COPY FROM loads. On the way out there is nothing left to pick
        // from, so PostgreSQL refuses a WHERE on COPY TO, and the query form has no grammar for
        // one at all — there it is a syntax error rather than a clause nobody reads.
        String whereClause = null;
        if (parser.checkKeyword("WHERE")) {
            if (subquery != null) {
                throw new MemgresException(SYNTAX_AT + parser.peek().value() + Q, "42601");
            }
            if (!isFrom) {
                throw new MemgresException("WHERE clause not allowed with COPY TO", "42601");
            }
            parser.advance();
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

        // Only now do the format's defaults fill in what was omitted: every check above turns on
        // what was written, and a default is not something anybody wrote.
        boolean csv = "csv".equals(opts.format);
        String delimiter = opts.delimiter != null ? opts.delimiter : (csv ? "," : "\t");
        String nullString = opts.nullString != null ? opts.nullString : (csv ? "" : "\\N");
        String quote = opts.quote != null ? opts.quote : "\"";
        String escape = opts.escape != null ? opts.escape : quote;

        CopyStmt stmt = new CopyStmt(table, columns, isFrom, source, opts.format, delimiter,
                nullString, opts.header, subquery, quote, escape, opts.forceQuote,
                opts.forceNotNull, opts.forceNull, opts.headerMatch, opts.freeze, opts.encoding,
                whereClause, opts.onError, opts.defaultString, opts.rejectLimit,
                opts.logVerbosity);
        stmt.setOptionError(opts.deferred);
        return stmt;
    }

    /**
     * Step over what is left of an option list after one of its entries was refused. The list may
     * hold parenthesised column lists of its own, so the depth is counted rather than the first
     * closing parenthesis taken.
     */
    private void skipRestOfOptionList() {
        int depth = 0;
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.check(TokenType.LEFT_PAREN)) {
                depth++;
            } else if (parser.check(TokenType.RIGHT_PAREN)) {
                if (depth == 0) return;
                depth--;
            }
            parser.advance();
        }
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

    /** Read one entry of a COPY option list, refusing what PostgreSQL's own option loop refuses. */
    private void readCopyOption(CopyOptions opts, boolean isFrom, boolean parenthesised) {
        String opt = parser.readIdentifier().toUpperCase(java.util.Locale.ROOT);
        switch (opt) {
            case "FORMAT":
                opts.format = parser.readIdentifier().toLowerCase(java.util.Locale.ROOT);
                if (!"text".equals(opts.format) && !"csv".equals(opts.format)
                        && !"binary".equals(opts.format)) {
                    throw new MemgresException(
                            "COPY format \"" + opts.format + "\" not recognized", "22023");
                }
                break;
            case "DELIMITER":
                opts.delimiter = parser.advance().value();
                break;
            case "NULL":
                opts.nullString = parser.advance().value();
                break;
            case "DEFAULT":
                opts.defaultString = parser.advance().value();
                break;
            case "HEADER": {
                // HEADER on its own means true. Otherwise PostgreSQL reads a Boolean or the word
                // "match", which only a COPY FROM has anything to match against, and it refuses
                // anything else rather than quietly taking the option as given.
                String value = readOptionValue();
                if (value == null) {
                    opts.header = true;
                } else if ("MATCH".equalsIgnoreCase(value)) {
                    if (!isFrom) {
                        throw new MemgresException(
                                "cannot use \"match\" with HEADER in COPY TO", "0A000");
                    }
                    opts.header = true;
                    opts.headerMatch = true;
                } else {
                    Boolean flag = booleanValue(value);
                    if (flag == null) {
                        throw new MemgresException(
                                "header requires a Boolean value or \"match\"", "42601");
                    }
                    opts.header = flag.booleanValue();
                }
                break;
            }
            case "QUOTE":
                opts.quote = parser.advance().value();
                break;
            case "ESCAPE":
                opts.escape = parser.advance().value();
                break;
            case "FORCE_QUOTE":
                opts.forceQuote = parseColumnListOption();
                break;
            case "FORCE_NOT_NULL":
                opts.forceNotNull = parseColumnListOption();
                break;
            case "FORCE_NULL":
                opts.forceNull = parseColumnListOption();
                break;
            case "FREEZE": {
                String value = readOptionValue();
                Boolean flag = value == null ? Boolean.TRUE : booleanValue(value);
                if (flag == null) {
                    throw new MemgresException("freeze requires a Boolean value", "42601");
                }
                opts.freeze = flag.booleanValue();
                break;
            }
            case "ENCODING":
                opts.encoding = parser.advance().value();
                if (!isEncodingName(opts.encoding)) {
                    throw new MemgresException(
                            "argument to option \"encoding\" must be a valid encoding name", "22023");
                }
                break;
            case "ON_ERROR":
                opts.onError = parser.readIdentifier().toLowerCase(java.util.Locale.ROOT);
                if (!"stop".equals(opts.onError) && !"ignore".equals(opts.onError)) {
                    throw new MemgresException(
                            "COPY ON_ERROR \"" + opts.onError + "\" not recognized", "22023");
                }
                break;
            case "LOG_VERBOSITY":
                opts.logVerbosity = parser.readIdentifier().toLowerCase(java.util.Locale.ROOT);
                if (!"default".equals(opts.logVerbosity) && !"verbose".equals(opts.logVerbosity)
                        && !"silent".equals(opts.logVerbosity)) {
                    throw new MemgresException(
                            "COPY LOG_VERBOSITY \"" + opts.logVerbosity + "\" not recognized", "22023");
                }
                break;
            case "REJECT_LIMIT": {
                String raw = parser.advance().value();
                long limit;
                try {
                    limit = Long.parseLong(raw.trim());
                } catch (NumberFormatException e) {
                    limit = 0;
                }
                if (limit <= 0) {
                    throw new MemgresException(
                            "REJECT_LIMIT (" + raw + ") must be greater than zero", "22023");
                }
                opts.rejectLimit = Long.valueOf(limit);
                break;
            }
            case "CSV":
            case "BINARY":
                // The pre-9.0 spelling names the format as a bare word after WITH. Inside a
                // parenthesised list PostgreSQL knows no option of that name.
                if (parenthesised) throw unknownCopyOption(opt);
                opts.format = "CSV".equals(opt) ? "csv" : "binary";
                break;
            default:
                // PostgreSQL reads a COPY option list against a fixed set of names and refuses one
                // it does not know. Stepping over the unknown one left the statement running under
                // options nobody had looked at, including ones that change how the data is read.
                throw unknownCopyOption(opt);
        }
    }

    private static MemgresException unknownCopyOption(String opt) {
        return new MemgresException("option \"" + opt.toLowerCase(java.util.Locale.ROOT) + "\" not recognized", "42601");
    }

    /**
     * The value an option carries, or null when it carries none. Only a literal or one of the
     * words PostgreSQL's grammar accepts here counts, so the clause after a bare {@code HEADER}
     * is not read as the header's value.
     */
    private String readOptionValue() {
        Token t = parser.peek();
        if (t.type() == TokenType.STRING_LITERAL || t.type() == TokenType.INTEGER_LITERAL) {
            parser.advance();
            return t.value();
        }
        if ((t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER)
                && BOOLEAN_WORDS.contains(t.value().toUpperCase(java.util.Locale.ROOT))) {
            parser.advance();
            return t.value();
        }
        return null;
    }

    /** PostgreSQL's Boolean spellings, or null when the word is not one of them. */
    private static Boolean booleanValue(String value) {
        String v = value.trim().toLowerCase(java.util.Locale.ROOT);
        if ("true".equals(v) || "t".equals(v) || "yes".equals(v) || "y".equals(v)
                || "on".equals(v) || "1".equals(v)) {
            return Boolean.TRUE;
        }
        if ("false".equals(v) || "f".equals(v) || "no".equals(v) || "n".equals(v)
                || "off".equals(v) || "0".equals(v)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static final Set<String> BOOLEAN_WORDS = Cols.setOf(
            "TRUE", "FALSE", "T", "F", "YES", "NO", "Y", "N", "ON", "OFF", "MATCH");

    /**
     * True when the name is one PostgreSQL knows an encoding by. Its own alias table is what
     * decides: {@code Charset.isSupported} is a different set, which would take names the server
     * refuses and refuse LATIN1 and WIN1252, which the server takes. Names are compared with the
     * punctuation dropped, so ISO-8859-1 and iso88591 are the one encoding.
     */
    private static boolean isEncodingName(String name) {
        if (name == null) return false;
        StringBuilder key = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isLetterOrDigit(c)) key.append(Character.toLowerCase(c));
        }
        return PG_ENCODINGS.contains(key.toString());
    }

    private static final Set<String> PG_ENCODINGS = Cols.setOf(
            "abc", "alt", "big5", "bigfive", "ceuc", "chinese",
            "cp1250", "cp1251", "cp1252", "cp1253", "cp1254", "cp1255", "cp1256", "cp1257",
            "cp1258", "cp866", "cp874", "cp932", "cp936", "cp949", "cp950",
            "eucchinese", "euccn", "eucjis2004", "eucjp", "euckr", "euctaiwan", "euctw",
            "gb18030", "gbk",
            "iso88591", "iso885910", "iso885913", "iso885914", "iso885915", "iso885916",
            "iso88592", "iso88593", "iso88594", "iso88595", "iso88596", "iso88597", "iso88598",
            "iso88599", "johab", "koi8", "koi8r", "koi8u",
            "latin1", "latin10", "latin2", "latin3", "latin4", "latin5", "latin6", "latin7",
            "latin8", "latin9", "mskanji", "muleinternal", "shiftjis", "shiftjis2004", "sjis",
            "sqlascii", "tcvn", "tcvn5712", "thai", "tis620", "uhc", "unicode", "utf8",
            "vscii", "win", "win1250", "win1251", "win1252", "win1253", "win1254", "win1255",
            "win1256", "win1257", "win1258", "win866", "win874", "win932", "win936", "win949",
            "win950", "windows1250", "windows1251", "windows1252", "windows1253", "windows1254",
            "windows1255", "windows1256", "windows1257", "windows1258", "windows866",
            "windows874", "windows932", "windows936", "windows949", "windows950");

    /**
     * The COPY options as written, before the format's defaults fill in what was omitted.
     *
     * <p>Several of PostgreSQL's checks turn on the difference between "not given" and "given the
     * value the default would have had": QUOTE outside CSV mode is refused even when it names the
     * very character CSV would have used, so the defaults cannot be applied before the checks.
     */
    private static final class CopyOptions {
        String format = "text";
        String delimiter;
        String nullString;
        String quote;
        String escape;
        String defaultString;
        String encoding;
        String onError;
        String logVerbosity;
        Long rejectLimit;
        List<String> forceQuote;
        List<String> forceNotNull;
        List<String> forceNull;
        boolean header;
        boolean headerMatch;
        boolean freeze;
        /** The first refusal the options earned, kept for the executor to raise in its turn. */
        MemgresException deferred;

        void defer(MemgresException e) {
            if (deferred == null) deferred = e;
        }

        /**
         * Outside CSV a backslash introduces an escape and the other characters here occur inside
         * escaped data, so a delimiter spelled with one of them could not be told from the data
         * it is meant to separate.
         */
        private static final String UNSAFE_TEXT_DELIMITERS =
                "\\.abcdefghijklmnopqrstuvwxyz0123456789";

        void validate(boolean isFrom) {
            boolean binary = "binary".equals(format);
            boolean csv = "csv".equals(format);

            // Asked before the defaults are filled in, because they ask what was written.
            if (binary && delimiter != null) throw inBinaryMode("DELIMITER");
            if (binary && nullString != null) throw inBinaryMode("NULL");
            if (binary && defaultString != null) throw inBinaryMode("DEFAULT");

            String delim = delimiter != null ? delimiter : (csv ? "," : "\t");
            String nulls = nullString != null ? nullString : (csv ? "" : "\\N");
            String quoted = quote != null ? quote : "\"";

            if (delim.length() != 1) {
                throw new MemgresException(
                        "COPY delimiter must be a single one-byte character", "0A000");
            }
            if (delim.indexOf('\r') >= 0 || delim.indexOf('\n') >= 0) {
                throw new MemgresException(
                        "COPY delimiter cannot be newline or carriage return", "22023");
            }
            if (nulls.indexOf('\r') >= 0 || nulls.indexOf('\n') >= 0) {
                throw new MemgresException(
                        "COPY null representation cannot use newline or carriage return", "22023");
            }
            if (defaultString != null
                    && (defaultString.indexOf('\r') >= 0 || defaultString.indexOf('\n') >= 0)) {
                throw new MemgresException(
                        "COPY default representation cannot use newline or carriage return", "22023");
            }
            if (!csv && UNSAFE_TEXT_DELIMITERS.indexOf(delim.charAt(0)) >= 0) {
                throw new MemgresException("COPY delimiter cannot be \"" + delim + "\"", "22023");
            }
            if (binary && header) {
                throw new MemgresException("cannot specify HEADER in BINARY mode", "0A000");
            }
            if (!csv && quote != null) throw requiresCsv("QUOTE");
            if (csv && quoted.length() != 1) {
                throw new MemgresException("COPY quote must be a single one-byte character", "0A000");
            }
            if (csv && delim.charAt(0) == quoted.charAt(0)) {
                throw new MemgresException("COPY delimiter and quote must be different", "22023");
            }
            if (!csv && escape != null) throw requiresCsv("ESCAPE");
            if (csv && escape != null && escape.length() != 1) {
                throw new MemgresException("COPY escape must be a single one-byte character", "0A000");
            }
            if (!csv && forceQuote != null) throw requiresCsv("FORCE_QUOTE");
            if (forceQuote != null && isFrom) {
                throw new MemgresException("COPY FORCE_QUOTE cannot be used with COPY FROM", "0A000");
            }
            if (!csv && forceNotNull != null) throw requiresCsv("FORCE_NOT_NULL");
            if (forceNotNull != null && !isFrom) throw notWithCopyTo("FORCE_NOT_NULL");
            if (!csv && forceNull != null) throw requiresCsv("FORCE_NULL");
            if (forceNull != null && !isFrom) throw notWithCopyTo("FORCE_NULL");
            if (nulls.indexOf(delim.charAt(0)) >= 0) {
                throw new MemgresException(
                        "COPY delimiter character must not appear in the NULL specification", "22023");
            }
            if (csv && nulls.indexOf(quoted.charAt(0)) >= 0) {
                throw new MemgresException(
                        "CSV quote character must not appear in the NULL specification", "22023");
            }
            if (freeze && !isFrom) throw notWithCopyTo("FREEZE");
            if (defaultString != null) {
                if (!isFrom) {
                    throw new MemgresException("COPY DEFAULT cannot be used with COPY TO", "0A000");
                }
                if (defaultString.indexOf(delim.charAt(0)) >= 0) {
                    throw new MemgresException(
                            "COPY delimiter must not appear in the DEFAULT specification", "0A000");
                }
                if (csv && defaultString.indexOf(quoted.charAt(0)) >= 0) {
                    throw new MemgresException(
                            "CSV quote character must not appear in the DEFAULT specification", "0A000");
                }
                // A field matching both markers would have to mean two things at once.
                if (defaultString.equals(nulls)) {
                    throw new MemgresException(
                            "NULL specification and DEFAULT specification cannot be the same", "0A000");
                }
            }
            if (onError != null && !"stop".equals(onError) && !isFrom) throw notWithCopyTo("ON_ERROR");
            // A limit on how many rows may be rejected only means something where rows are allowed
            // to be rejected at all, so PostgreSQL refuses the one written without the other.
            if (rejectLimit != null && !"ignore".equals(onError)) {
                throw new MemgresException(
                        "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE", "22023");
            }
        }

        private static MemgresException inBinaryMode(String option) {
            return new MemgresException("cannot specify " + option + " in BINARY mode", "42601");
        }

        private static MemgresException requiresCsv(String option) {
            return new MemgresException("COPY " + option + " requires CSV mode", "0A000");
        }

        private static MemgresException notWithCopyTo(String option) {
            return new MemgresException("COPY " + option + " cannot be used with COPY TO", "22023");
        }
    }

    // ---- SET ----

    /**
     * The name a token spells. A keyword's token carries its upper-case form, which is not what
     * an object called name is called: COMMENT ON COLUMN t.name has to reach the column name.
     */
    private static String identifierSpelling(Token token) {
        return token.type() == TokenType.KEYWORD ? token.value().toLowerCase(java.util.Locale.ROOT) : token.value();
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
        // SET SESSION ROLE and SET LOCAL ROLE name the role the same way SET ROLE does, and
        // differ only in how long the choice lasts.
        if ((isSession || isLocal) && parser.checkKeyword("ROLE")) {
            parser.advance();
            return new SetStmt("role", readRoleTarget(false), isLocal);
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
                return new SetStmt("xmloption", mode.toLowerCase(java.util.Locale.ROOT));
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
            // The mode is not optional: SET CONSTRAINTS says which way to set them, and with
            // nothing written the statement asks for nothing. Defaulted to IMMEDIATE, a
            // statement meant to defer them quietly did the opposite of what it said.
            String mode;
            if (parser.matchKeyword("DEFERRED")) {
                mode = "DEFERRED";
            } else if (parser.matchKeyword("IMMEDIATE")) {
                mode = "IMMEDIATE";
            } else {
                throw ParseException.at(parser.peek());
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
        // The forms PostgreSQL's grammar spells out by name, each with a value of its own shape.
        if (name.equalsIgnoreCase("TIME") && parser.checkKeyword("ZONE")) {
            parser.advance(); // consume ZONE
            return new SetStmt("TimeZone", readZoneValue(), isLocal);
        }
        if (name.equalsIgnoreCase("NAMES")) {
            // SET NAMES is the standard's spelling of SET client_encoding.
            String encoding = "DEFAULT";
            if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                Token tok = parser.advance();
                encoding = tok.value();
            }
            parser.expectEndOfStatement();
            SetStmt names = new SetStmt("client_encoding", encoding, isLocal);
            if ("DEFAULT".equalsIgnoreCase(encoding)) names.setToDefault(true);
            return names;
        }
        if ((name.equalsIgnoreCase("SCHEMA") || name.equalsIgnoreCase("CATALOG"))
                && parser.check(TokenType.STRING_LITERAL)) {
            String only = parser.advance().value();
            parser.expectEndOfStatement();
            if (name.equalsIgnoreCase("CATALOG")) {
                throw new MemgresException("cross-database references are not implemented: \""
                        + only + "\"", "0A000");
            }
            return new SetStmt("search_path", quotedListName(only), isLocal);
        }
        // SET name = value | SET name TO value. The value is a list of constants and bare
        // names, which is a grammar of its own: two bare words in a row are two values, not one
        // value with a space in it, and a comma between them is what makes them a list.
        if (parser.match(TokenType.EQUALS) || parser.matchKeyword("TO")) {
            if (parser.checkKeyword("DEFAULT")) {
                parser.advance();
                parser.expectEndOfStatement();
                SetStmt toDefault = new SetStmt(name, "DEFAULT", isLocal);
                toDefault.setToDefault(true);
                return toDefault;
            }
            return new SetStmt(name, readSettingValue(name), isLocal);
        }
        // Everything else PostgreSQL's grammar spells out by name, and a value written after a
        // parameter with neither = nor TO between them is a value the grammar has no place for.
        Token stray = parser.peek();
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            throw ParseException.saying(SYNTAX_AT + stray.raw() + Q, stray, "42601");
        }
        throw ParseException.saying("syntax error at end of input", stray, "42601");
    }

    /**
     * What may stand for a zone after SET TIME ZONE: a name, a number of hours, an interval, or
     * one of the two words that mean "put it back". A number is read as an offset in hours, and
     * PostgreSQL keeps the sign the POSIX way round when it writes the zone back.
     */
    private String readZoneValue() {
        if (parser.checkKeyword("DEFAULT") || parser.checkKeyword("LOCAL")) {
            String word = parser.advance().value();
            parser.expectEndOfStatement();
            return word.toUpperCase(java.util.Locale.ROOT);
        }
        if (parser.checkKeyword("INTERVAL")) {
            parser.advance();
            Token literal = parser.advance();
            StringBuilder written = new StringBuilder(literal.value());
            // The field qualifier an interval may carry says which unit the number counts.
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                written.append(' ').append(parser.advance().value());
            }
            return "INTERVAL:" + written;
        }
        boolean negative = parser.match(TokenType.MINUS);
        if (!negative) parser.match(TokenType.PLUS);
        Token tok = parser.advance();
        parser.expectEndOfStatement();
        if (tok.type() == TokenType.INTEGER_LITERAL || tok.type() == TokenType.FLOAT_LITERAL) {
            return "HOURS:" + (negative ? "-" : "") + tok.value();
        }
        if (negative) {
            throw ParseException.saying(SYNTAX_AT + tok.raw() + Q, tok, "42601");
        }
        return tok.value();
    }

    /**
     * The list of values a SET may be given, written back the way PostgreSQL writes it.
     *
     * <p>Each item is a string constant, a signed number, or a bare word; the items are separated
     * by commas and nothing else. An item that would not read back as itself is quoted, which is
     * how {@code SET search_path = "$user", public} comes back out with its quotes and
     * {@code SET search_path = 'a, b'} comes back as one quoted name rather than as two.
     */
    private String readSettingValue(String name) {
        List<String> written = new ArrayList<>();
        boolean listValued = isListValuedSetting(name);
        do {
            Token tok = parser.peek();
            if (parser.isAtEnd() || tok.type() == TokenType.SEMICOLON
                    || tok.type() == TokenType.EOF) {
                throw ParseException.saying("syntax error at end of input", tok, "42601");
            }
            if (tok.type() == TokenType.ERROR) {
                throw ParseException.saying(SYNTAX_AT + tok.value() + Q, tok, "42601");
            }
            written.add(readOneSettingValue(listValued));
        } while (parser.match(TokenType.COMMA));
        Token after = parser.peek();
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)
                && after.type() != TokenType.EOF) {
            throw ParseException.saying(SYNTAX_AT + after.raw() + Q, after, "42601");
        }
        // A parameter the reader made up holds one value; PostgreSQL says so by name.
        if (written.size() > 1 && name.indexOf('.') >= 0) {
            throw new MemgresException("SET " + name + " takes only one argument", "22023");
        }
        return String.join(", ", written);
    }

    /** One item of a SET's value list. */
    private String readOneSettingValue(boolean listValued) {
        boolean negative = parser.match(TokenType.MINUS);
        if (!negative) parser.match(TokenType.PLUS);
        Token tok = parser.advance();
        if (tok.type() == TokenType.STRING_LITERAL) {
            // A quoted item is the text it holds, whatever that text says.
            return listValued ? quotedListName(tok.value()) : tok.value();
        }
        if (tok.type() == TokenType.INTEGER_LITERAL || tok.type() == TokenType.FLOAT_LITERAL) {
            return (negative ? "-" : "") + tok.value();
        }
        if (negative) {
            throw ParseException.saying(SYNTAX_AT + tok.raw() + Q, tok, "42601");
        }
        if (tok.type() == TokenType.QUOTED_IDENTIFIER) {
            return listValued ? quotedListName(tok.value()) : tok.value();
        }
        if (tok.type() == TokenType.IDENTIFIER || tok.type() == TokenType.KEYWORD) {
            return listValued ? quotedListName(tok.raw()) : tok.raw();
        }
        throw ParseException.saying(SYNTAX_AT + tok.raw() + Q, tok, "42601");
    }

    /** The settings whose value is a list of names, and so is written back name by name. */
    private static final Set<String> LIST_VALUED_SETTINGS = Cols.setOf(
            "search_path", "local_preload_libraries", "session_preload_libraries",
            "shared_preload_libraries", "temp_tablespaces", "synchronous_standby_names");

    private static boolean isListValuedSetting(String name) {
        return LIST_VALUED_SETTINGS.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * A name in a settings list, quoted where it needs to be. PostgreSQL writes a list back the
     * way it would have to be written to mean the same thing again, so a name that is not a plain
     * lower-case word comes back between quotes.
     */
    private static String quotedListName(String name) {
        boolean plain = !name.isEmpty();
        for (int i = 0; plain && i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || c == '_'
                    || (i > 0 && ((c >= '0' && c <= '9') || c == '$'));
            if (!ok) plain = false;
        }
        if (plain) return name;
        return "\"" + name.replace("\"", "\"\"") + "\"";
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
        if (target == null || !DISCARD_TARGETS.contains(target.toLowerCase(java.util.Locale.ROOT))) {
            throw ParseException.saying(SYNTAX_AT + targetToken.raw() + Q, targetToken, "42601");
        }
        parser.expectEndOfStatement();
        return new DiscardStmt(target);
    }

    // ---- Transaction statements ----

    TransactionStmt parseTransactionBegin() {
        // Which of the two spellings opened the block is kept, because it is the one PostgreSQL
        // answers with: START TRANSACTION completes as START TRANSACTION, not as BEGIN.
        boolean started = parser.matchKeyword("START");
        if (started) {
            parser.matchKeyword("TRANSACTION");
        } else {
            parser.expectKeyword("BEGIN");
            parser.matchKeyword("TRANSACTION");
            parser.matchKeyword("WORK");
        }
        TransactionModes modes = parseTransactionModes();
        return new TransactionStmt(TransactionStmt.TransactionAction.BEGIN, null,
                modes.isolationLevel, modes.readOnly, false, modes.deferrable, started);
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
                    ? nameToken.value() : nameToken.value().toLowerCase(java.util.Locale.ROOT);
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
                    value = valueToken.value().toLowerCase(java.util.Locale.ROOT);
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
        String word = first.value() == null ? "" : first.value().toUpperCase(java.util.Locale.ROOT);
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
            return v.toUpperCase(java.util.Locale.ROOT);
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
            // An argument list is written only when there are arguments in it. Accepting an empty
            // one made EXECUTE p() a way of saying EXECUTE p, which is not a spelling PostgreSQL
            // has: it is the closing bracket that is out of place.
            params = parser.parseExpressionList();
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
        // Each privilege carries the column list written after it, which need not be the same
        // list as the one after the privilege before it.
        java.util.Map<String, List<String>> columnsByPrivilege = new java.util.LinkedHashMap<>();

        // The same list is read whether these turn out to be privilege names or role names, and
        // a role keeps the case it was created with. Upper-cased for both, GRANT r TO r named a
        // role called R and the message quoted a name nobody wrote.
        List<String> written = new ArrayList<>();
        // Read privileges: ALL [PRIVILEGES], SELECT, INSERT, UPDATE, DELETE, ALTER SYSTEM, etc.
        do {
            String spelled = parser.readIdentifier();
            written.add(spelled);
            String priv = spelled.toUpperCase(java.util.Locale.ROOT);
            if (priv.equals("ALL")) {
                parser.matchKeyword("PRIVILEGES");
                priv = "ALL";
            } else if (priv.equals("ALTER") && parser.checkKeyword("SYSTEM")) {
                parser.advance(); // consume SYSTEM
                priv = "ALTER SYSTEM";
            }
            privileges.add(priv);
            // Column-level privileges: UPDATE (col1, col2)
            if (parser.check(TokenType.LEFT_PAREN) && !parser.checkKeyword("ON")) {
                parser.advance(); // (
                List<String> own = new ArrayList<>();
                do { own.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
                columnsByPrivilege.put(priv, own);
                if (columns == null) columns = new ArrayList<>();
                for (String col : own) {
                    if (!columns.contains(col)) columns.add(col);
                }
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
            // A membership grant carries GRANTED BY too, and dropping the clause here meant the
            // grantor was never a name the statement had to answer for.
            String roleGrantor = null;
            if (parser.matchIdentifier("GRANTED")) {
                parser.expectKeyword("BY");
                roleGrantor = parser.readIdentifier();
            }
            return new GrantStmt(written, null, null, grantees, false, withAdmin, true, null, roleGrantor);
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
            String what = parser.readIdentifier().toUpperCase(java.util.Locale.ROOT); // TABLES, SEQUENCES, FUNCTIONS
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
            // The argument list says which routine of that name this is, so it travels with the
            // name. Consumed and thrown away, a grant on one overload was a grant on whichever
            // overload happened to be found, and a signature matching none was not noticed.
            if (parser.check(TokenType.LEFT_PAREN)) {
                objectName = objectName + writtenArgumentList();
            }
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
        } else if (parser.checkKeyword("WITH")) {
            // A privilege grant carries WITH GRANT OPTION and nothing else. ADMIN OPTION belongs
            // to a membership grant, and read as though it were the same thing a statement asking
            // for one on a table was accepted and did neither.
            Token with = parser.peek();
            parser.advance();
            throw ParseException.at(parser.isAtEnd() ? with : parser.peek());
        }

        // Consume optional GRANTED BY role and capture the grantor name
        // Note: GRANTED is not a reserved keyword, so it is tokenized as an IDENTIFIER
        String grantor = null;
        if (parser.matchIdentifier("GRANTED")) {
            parser.expectKeyword("BY");
            grantor = parser.readIdentifier();
        }

        return new GrantStmt(privileges, objectType, objectName, grantees, withGrantOption, false,
                false, columns, grantor).withPerPrivilegeColumns(columnsByPrivilege);
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
        // As for GRANT: the same list is read whether these are privilege names or role names,
        // and a role keeps the case it was created with.
        List<String> revokeWritten = new ArrayList<>();
        List<String> revokeColumns = null;
        do {
            String spelledRevoke = parser.readIdentifier();
            revokeWritten.add(spelledRevoke);
            String priv = spelledRevoke.toUpperCase(java.util.Locale.ROOT);
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
            return new RevokeStmt(revokeWritten, null, null, grantees, adminOptionFor, true, cascade);
        }

        // REVOKE privileges ON object FROM roles
        parser.expectKeyword("ON");
        String objectType = "TABLE";
        String objectName;

        if (parser.matchKeyword("ALL")) {
            String what = parser.readIdentifier().toUpperCase(java.util.Locale.ROOT);
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
            // The argument list says which routine of that name this is, so it travels with the
            // name. Consumed and thrown away, a grant on one overload was a grant on whichever
            // overload happened to be found, and a signature matching none was not noticed.
            if (parser.check(TokenType.LEFT_PAREN)) {
                objectName = objectName + writtenArgumentList();
            }
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
        List<String> oldRoles = new ArrayList<>();
        do { oldRoles.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
        parser.expectKeyword("TO");
        String newRole = parser.readIdentifier();
        parser.expectEndOfStatement();
        return new ReassignOwnedStmt(oldRoles, newRole);
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
        // A quoted name is the name it holds, letter for letter: PostgreSQL names the column
        // after the parameter the way it was asked for.
        boolean quoted = parser.check(TokenType.QUOTED_IDENTIFIER);
        String param = parser.readIdentifier();
        // Handle dotted GUC names: SHOW myapp.tenant_id
        if (parser.match(TokenType.DOT)) {
            quoted = quoted || parser.check(TokenType.QUOTED_IDENTIFIER);
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
        // SHOW asks about one parameter. A comma after it starts nothing the grammar has.
        parser.expectEndOfStatement();
        SetStmt show = new SetStmt("show", param);
        if (quoted) show.setAuxiliary(param);
        return show;
    }

    /** ZONE is not a reserved word everywhere, so match it on the word rather than the kind. */
    private boolean matchZone() {
        if (parser.isAtEnd()) return false;
        if (!"ZONE".equalsIgnoreCase(parser.peek().value())) return false;
        parser.advance();
        return true;
    }

    // ---- COMMENT ON ----

    /**
     * The object kinds COMMENT names. A kind spelled in more than one word stands before the
     * kinds that share its first word, so OPERATOR CLASS is read whole rather than as OPERATOR
     * followed by a name called "class".
     */
    private static final String[][] COMMENT_KINDS = {
            {"ACCESS", "METHOD"},
            {"EVENT", "TRIGGER"},
            {"FOREIGN", "DATA", "WRAPPER"},
            {"FOREIGN", "TABLE"},
            {"LARGE", "OBJECT"},
            {"MATERIALIZED", "VIEW"},
            {"OPERATOR", "CLASS"},
            {"OPERATOR", "FAMILY"},
            {"PROCEDURAL", "LANGUAGE"},
            {"TEXT", "SEARCH", "CONFIGURATION"},
            {"TEXT", "SEARCH", "DICTIONARY"},
            {"TEXT", "SEARCH", "PARSER"},
            {"TEXT", "SEARCH", "TEMPLATE"},
            {"USER", "MAPPING"},
            {"AGGREGATE"}, {"CAST"}, {"COLLATION"}, {"COLUMN"}, {"CONSTRAINT"}, {"CONVERSION"},
            {"DATABASE"}, {"DOMAIN"}, {"EXTENSION"}, {"FUNCTION"}, {"INDEX"}, {"LANGUAGE"},
            {"OPERATOR"}, {"POLICY"}, {"PROCEDURE"}, {"PUBLICATION"}, {"ROLE"}, {"ROUTINE"},
            {"RULE"}, {"SCHEMA"}, {"SEQUENCE"}, {"SERVER"}, {"STATISTICS"}, {"SUBSCRIPTION"},
            {"TABLE"}, {"TABLESPACE"}, {"TRANSFORM"}, {"TRIGGER"}, {"TYPE"}, {"VIEW"},
    };

    /**
     * The kinds SECURITY LABEL names. It reaches fewer of them than COMMENT does: there is no
     * label on a trigger, a rule, a cast or an operator, and the word is a syntax error there.
     */
    private static final Set<String> LABEL_KINDS = new java.util.HashSet<>(Arrays.asList(
            "TABLE", "COLUMN", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE", "SEQUENCE",
            "AGGREGATE", "FUNCTION", "PROCEDURE", "ROUTINE", "LARGE OBJECT", "TYPE", "DOMAIN",
            "SCHEMA", "DATABASE", "ROLE", "TABLESPACE", "EVENT TRIGGER", "LANGUAGE",
            "PROCEDURAL LANGUAGE", "PUBLICATION", "SUBSCRIPTION"));

    /** The kinds whose object is named against a relation: {@code ... name ON relation}. */
    private static final Set<String> RELATION_SCOPED_KINDS = new java.util.HashSet<>(
            Arrays.asList("CONSTRAINT", "TRIGGER", "RULE", "POLICY"));

    /** The kinds written with a routine's parenthesised argument list. */
    private static final Set<String> ROUTINE_KINDS = new java.util.HashSet<>(
            Arrays.asList("AGGREGATE", "FUNCTION", "PROCEDURE", "ROUTINE"));

    // ---- COMMENT ON ----

    CommentStmt parseComment() {
        parser.expectKeyword("COMMENT");
        parser.expectKeyword("ON");
        return parseCommentBody(false, null);
    }

    // ---- SECURITY LABEL ----

    CommentStmt parseSecurityLabel() {
        parser.expectKeyword("SECURITY");
        parser.expectKeyword("LABEL");
        String provider = null;
        if (parser.matchKeyword("FOR")) {
            Token t = parser.peek();
            if (t.type() != TokenType.IDENTIFIER && t.type() != TokenType.QUOTED_IDENTIFIER
                    && t.type() != TokenType.STRING_LITERAL) {
                throw commentSyntaxError(t);
            }
            provider = parser.advance().value();
        }
        parser.expectKeyword("ON");
        return parseCommentBody(true, provider);
    }

    /**
     * Everything after the ON, which COMMENT and SECURITY LABEL write the same way: a kind, a name
     * written the way that kind is written, and one string constant or NULL.
     */
    private CommentStmt parseCommentBody(boolean label, String provider) {
        Token kindToken = parser.peek();
        String kind = matchCommentKind();
        if (kind == null || (label && !LABEL_KINDS.contains(kind))) {
            throw commentSyntaxError(kindToken);
        }
        String schema = null;
        String relation = null;
        String name = null;
        List<String> args = null;
        String using = null;

        if (kind.equals("CAST")) {
            // A cast is named by the two types it converts between, and both have to be there.
            parser.expect(TokenType.LEFT_PAREN);
            String source = readCommentTypeName();
            expectWordOrSyntaxError("AS");
            String target = readCommentTypeName();
            expectRightParen();
            args = new ArrayList<>();
            args.add(source);
            args.add(target);
            name = "(" + source + " AS " + target + ")";
        } else if (kind.equals("TRANSFORM")) {
            expectWordOrSyntaxError("FOR");
            name = readCommentTypeName();
            expectWordOrSyntaxError("LANGUAGE");
            using = parser.readIdentifier();
        } else if (kind.equals("OPERATOR")) {
            String[] qualified = readOperatorSpelling();
            schema = qualified[0];
            name = qualified[1];
            args = readOperatorOperands();
        } else if (kind.equals("OPERATOR CLASS") || kind.equals("OPERATOR FAMILY")) {
            String[] qualified = readQualifiedCommentName();
            schema = qualified[0];
            name = qualified[1];
            expectWordOrSyntaxError("USING");
            using = parser.readIdentifier();
        } else if (kind.equals("LARGE OBJECT")) {
            Token t = parser.peek();
            if (t.type() != TokenType.INTEGER_LITERAL && t.type() != TokenType.FLOAT_LITERAL) {
                throw commentSyntaxError(t);
            }
            name = parser.advance().value();
        } else if (kind.equals("COLUMN")) {
            List<String> parts = readDottedCommentName();
            if (parts.size() < 2) {
                // A label goes to a provider before anything about the name is looked at, so a
                // server with none loaded never reaches this complaint.
                if (!label) {
                    throw ParseException.saying(
                            "column name must be qualified", parser.peek(), "42601");
                }
                name = parts.get(0);
            } else {
                name = parts.get(parts.size() - 1);
                relation = parts.get(parts.size() - 2);
                if (parts.size() > 2) schema = parts.get(parts.size() - 3);
            }
        } else if (RELATION_SCOPED_KINDS.contains(kind)) {
            name = parser.readIdentifier();
            expectWordOrSyntaxError("ON");
            String[] qualified = readQualifiedCommentName();
            schema = qualified[0];
            relation = qualified[1];
        } else {
            String[] qualified = readQualifiedCommentName();
            schema = qualified[0];
            name = qualified[1];
            if (ROUTINE_KINDS.contains(kind)) {
                // A routine's argument list says which routine of that name this is. An aggregate
                // is always written with one; the others may leave it off when it settles nothing.
                if (parser.check(TokenType.LEFT_PAREN)) {
                    args = readRoutineArgumentTypes();
                } else if (kind.equals("AGGREGATE")) {
                    throw commentSyntaxError(parser.peek());
                }
            }
        }

        // One statement names one object. A comma here was read as a second name and quietly
        // dropped, so COMMENT ON TABLE a, b commented on a alone.
        if (parser.check(TokenType.COMMA)) {
            throw commentSyntaxError(parser.peek());
        }
        expectWordOrSyntaxError("IS");

        String comment = null;
        if (parser.matchKeyword("NULL")) {
            comment = null;
        } else if (parser.check(TokenType.STRING_LITERAL)
                || parser.check(TokenType.DOLLAR_STRING_LITERAL)) {
            comment = parser.advance().value();
        } else {
            // A comment is one string constant or NULL, never an expression. Swallowing the rest
            // of the line instead accepted 'a' || 'b', 42 and current_user, and filed nothing.
            throw commentSyntaxError(parser.peek());
        }
        parser.expectEndOfStatement();
        return new CommentStmt(kind, schema, relation, name, args, using, comment, label, provider);
    }

    /** The kind written here, read whole, or null when the word names no kind at all. */
    private String matchCommentKind() {
        for (String[] words : COMMENT_KINDS) {
            int saved = parser.pos;
            boolean all = true;
            for (String word : words) {
                if (!parser.matchWord(word)) { all = false; break; }
            }
            if (all) return String.join(" ", words);
            parser.pos = saved;
        }
        return null;
    }

    /**
     * The parenthesised argument list as it was written, for a name that carries its signature.
     * A routine is told apart by its arguments, so the list is part of what a statement named.
     */
    private String writtenArgumentList() {
        StringBuilder out = new StringBuilder("(");
        parser.expect(TokenType.LEFT_PAREN);
        boolean first = true;
        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            if (parser.match(TokenType.COMMA)) { out.append(", "); first = true; continue; }
            if (!first) out.append(' ');
            out.append(identifierSpelling(parser.advance()));
            first = false;
        }
        parser.match(TokenType.RIGHT_PAREN);
        return out.append(')').toString();
    }

    /** A word the grammar requires, reported where it is missing rather than where it ends. */
    private void expectWordOrSyntaxError(String word) {
        if (parser.matchWord(word)) return;
        throw commentSyntaxError(parser.peek());
    }

    private void expectRightParen() {
        if (parser.match(TokenType.RIGHT_PAREN)) return;
        throw commentSyntaxError(parser.peek());
    }

    /**
     * The syntax error at this token, which is "at end of input" when the statement simply ran
     * out and names a string constant with the quotes that made it one.
     */
    private static ParseException commentSyntaxError(Token token) {
        if (token == null || token.type() == TokenType.EOF
                || token.type() == TokenType.SEMICOLON) {
            return ParseException.saying("syntax error at end of input", token, "42601");
        }
        return ParseException.saying(SYNTAX_AT + asWritten(token) + Q, token, "42601");
    }

    /** The dot-separated words of a name, each spelled the way it was written. */
    private List<String> readDottedCommentName() {
        List<String> parts = new ArrayList<>();
        parts.add(parser.readColumnName());
        while (parser.match(TokenType.DOT)) {
            parts.add(parser.readColumnName());
        }
        return parts;
    }

    /** A name that may carry a schema, as {schema-or-null, name}. */
    private String[] readQualifiedCommentName() {
        List<String> parts = readDottedCommentName();
        if (parts.size() == 1) return new String[]{null, parts.get(0)};
        return new String[]{parts.get(parts.size() - 2), parts.get(parts.size() - 1)};
    }

    /**
     * An operator's spelling, which is symbols rather than a word, and may carry a schema written
     * as {@code OPERATOR(schema.+)} would write it — here plainly, as {@code schema.+}.
     */
    private String[] readOperatorSpelling() {
        String schema = null;
        StringBuilder spelling = new StringBuilder();
        while (!parser.isAtEnd() && !parser.check(TokenType.LEFT_PAREN)
                && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
            Token t = parser.peek();
            if (t.type() == TokenType.DOT && spelling.length() > 0 && schema == null) {
                parser.advance();
                schema = spelling.toString();
                spelling.setLength(0);
                continue;
            }
            if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER) {
                // Only a schema may be a word here; the operator itself is symbols.
                if (spelling.length() > 0) break;
                parser.advance();
                spelling.append(t.value());
                continue;
            }
            if (t.type() == TokenType.KEYWORD) break;
            parser.advance();
            spelling.append(t.value());
        }
        if (spelling.length() == 0) {
            throw commentSyntaxError(parser.peek());
        }
        return new String[]{schema, spelling.toString()};
    }

    /**
     * The two operands an operator is named by. NONE stands where an operand is not there, and a
     * unary operator written NONE on the right is a postfix operator, which PostgreSQL dropped.
     */
    private List<String> readOperatorOperands() {
        if (!parser.check(TokenType.LEFT_PAREN)) {
            throw commentSyntaxError(parser.peek());
        }
        parser.advance();
        List<String> operands = new ArrayList<>();
        operands.add(readOperandType());
        if (parser.match(TokenType.COMMA)) operands.add(readOperandType());
        expectRightParen();
        if (operands.size() == 2 && operands.get(1) == null) {
            throw new MemgresException("postfix operators are not supported", "42601");
        }
        return operands;
    }

    /** One operand of an operator: a type, or NONE where the operator takes nothing. */
    private String readOperandType() {
        if (parser.checkWord("NONE")) { parser.advance(); return null; }
        return readCommentTypeName();
    }

    /** The types of a routine's arguments, with the mode words that may stand before them. */
    private List<String> readRoutineArgumentTypes() {
        parser.expect(TokenType.LEFT_PAREN);
        List<String> types = new ArrayList<>();
        if (parser.match(TokenType.RIGHT_PAREN)) return types;
        while (true) {
            if (parser.checkWord("IN") || parser.checkWord("OUT") || parser.checkWord("INOUT")
                    || parser.checkWord("VARIADIC")) {
                parser.advance();
            }
            // An argument may be written "name type" as well as "type"; the name settles nothing
            // about which routine this is, so what is read is the type at the end.
            int saved = parser.pos;
            String first = readCommentTypeName();
            if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                parser.pos = saved;
                parser.readIdentifier();
                first = readCommentTypeName();
            }
            types.add(first);
            if (parser.match(TokenType.COMMA)) continue;
            break;
        }
        expectRightParen();
        return types;
    }

    /** A type name as written, including the several PostgreSQL spells in more than one word. */
    private String readCommentTypeName() {
        Token t = parser.peek();
        if (t.type() != TokenType.IDENTIFIER && t.type() != TokenType.QUOTED_IDENTIFIER
                && t.type() != TokenType.KEYWORD) {
            throw commentSyntaxError(t);
        }
        StringBuilder sb = new StringBuilder(identifierSpelling(parser.advance()));
        while (parser.check(TokenType.DOT)) {
            parser.advance();
            sb.append('.').append(identifierSpelling(parser.advance()));
        }
        if (parser.checkWord("PRECISION") || parser.checkWord("VARYING")) {
            sb.append(' ').append(parser.advance().value().toLowerCase(java.util.Locale.ROOT));
        } else if (parser.checkWord("WITH") || parser.checkWord("WITHOUT")) {
            sb.append(' ').append(parser.advance().value().toLowerCase(java.util.Locale.ROOT));
            if (parser.checkWord("TIME")) sb.append(' ').append(parser.advance().value().toLowerCase(java.util.Locale.ROOT));
            if (parser.checkWord("ZONE")) sb.append(' ').append(parser.advance().value().toLowerCase(java.util.Locale.ROOT));
        }
        if (parser.check(TokenType.LEFT_PAREN)) {
            // A length or precision belongs to the written type but not to which type it is.
            int depth = 0;
            do {
                Token p = parser.advance();
                if (p.type() == TokenType.LEFT_PAREN) depth++;
                else if (p.type() == TokenType.RIGHT_PAREN) depth--;
            } while (depth > 0 && !parser.isAtEnd());
        }
        while (parser.check(TokenType.LEFT_BRACKET)) {
            parser.advance();
            sb.append("[]");
            if (parser.check(TokenType.INTEGER_LITERAL) || parser.check(TokenType.FLOAT_LITERAL)) {
                parser.advance();
            }
            parser.match(TokenType.RIGHT_BRACKET);
        }
        return sb.toString();
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

    // ---- VACUUM, ANALYZE, REINDEX, CLUSTER, CHECKPOINT ----

    /** The options ANALYZE takes, and whether each carries a value. */
    private static final Set<String> ANALYZE_OPTIONS = Cols.setOf(
            "VERBOSE", "SKIP_LOCKED", "BUFFER_USAGE_LIMIT");

    /** The options VACUUM takes. */
    private static final Set<String> VACUUM_OPTIONS = Cols.setOf(
            "FULL", "FREEZE", "VERBOSE", "ANALYZE", "ANALYSE",
            "DISABLE_PAGE_SKIPPING", "SKIP_LOCKED", "PROCESS_TOAST", "PROCESS_MAIN",
            "TRUNCATE", "PARALLEL", "INDEX_CLEANUP", "BUFFER_USAGE_LIMIT",
            "SKIP_DATABASE_STATS", "ONLY_DATABASE_STATS");

    /** The options REINDEX takes. */
    private static final Set<String> REINDEX_OPTIONS = Cols.setOf(
            "VERBOSE", "CONCURRENTLY", "TABLESPACE");

    /** The options CLUSTER takes. */
    private static final Set<String> CLUSTER_OPTIONS = Cols.setOf("VERBOSE");

    /**
     * The parenthesised option list these statements share. PostgreSQL names the statement in the
     * complaint about an option it does not know, and an empty list is a syntax error at the
     * closing bracket rather than a list of nothing.
     */
    private List<MaintenanceStmt.Option> parseMaintenanceOptions(String verb, Set<String> known) {
        List<MaintenanceStmt.Option> options = new ArrayList<>();
        if (!parser.check(TokenType.LEFT_PAREN)) return options;
        parser.advance();
        if (parser.check(TokenType.RIGHT_PAREN)) {
            Token t = parser.peek();
            throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
        }
        do {
            Token nameTok = parser.advance();
            String name = nameTok.value().toUpperCase(java.util.Locale.ROOT);
            if (!known.contains(name)) {
                throw ParseException.saying("unrecognized " + verb + " option \""
                        + nameTok.value().toLowerCase(java.util.Locale.ROOT) + "\"",
                        nameTok, "42601");
            }
            String value = null;
            if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)
                    && !parser.isAtEnd()) {
                boolean negative = parser.match(TokenType.MINUS);
                value = (negative ? "-" : "") + parser.advance().value();
            }
            options.add(new MaintenanceStmt.Option(name, value));
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        return options;
    }

    /** The relation a maintenance statement names, or null when it names none. */
    private String[] parseMaintenanceRelation() {
        matchOnlyBeforeRelation();
        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        return new String[]{schema, name};
    }

    /** The column list an ANALYZE or a VACUUM may name, which has at least one column in it. */
    private List<String> parseMaintenanceColumns() {
        if (!parser.check(TokenType.LEFT_PAREN)) return null;
        parser.advance();
        if (parser.check(TokenType.RIGHT_PAREN)) {
            Token t = parser.peek();
            throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
        }
        List<String> columns = new ArrayList<>();
        do {
            columns.add(parser.readColumnName());
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        return columns;
    }

    /**
     * Nothing may follow a maintenance statement's target. A word left standing there is a word
     * the grammar had no place for, and PostgreSQL reports it where it stands rather than reading
     * it as the relation.
     */
    private void expectNothingFurther() {
        if (parser.isAtEnd() || parser.check(TokenType.SEMICOLON)) return;
        Token t = parser.peek();
        throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
    }

    MaintenanceStmt parseAnalyze() {
        parser.advance(); // ANALYZE or ANALYSE
        List<MaintenanceStmt.Option> options =
                parseMaintenanceOptions("ANALYZE", ANALYZE_OPTIONS);
        // The bare spelling takes VERBOSE and nothing else in front of the relation.
        if (options.isEmpty() && parser.matchKeyword("VERBOSE")) {
            options.add(new MaintenanceStmt.Option("VERBOSE", null));
        }
        String schema = null;
        String name = null;
        List<String> columns = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            String[] relation = parseMaintenanceRelation();
            schema = relation[0];
            name = relation[1];
            columns = parseMaintenanceColumns();
        }
        expectNothingFurther();
        return new MaintenanceStmt(MaintenanceStmt.Verb.ANALYZE, options, null, schema, name,
                columns, null, false);
    }

    MaintenanceStmt parseVacuum() {
        parser.expectKeyword("VACUUM");
        List<MaintenanceStmt.Option> options = parseMaintenanceOptions("VACUUM", VACUUM_OPTIONS);
        if (options.isEmpty()) {
            // The bare spelling takes its options in one fixed order, which is the order
            // PostgreSQL's grammar writes them in: FULL, FREEZE, VERBOSE, ANALYZE.
            if (parser.matchKeyword("FULL")) options.add(new MaintenanceStmt.Option("FULL", null));
            if (parser.matchKeyword("FREEZE")) {
                options.add(new MaintenanceStmt.Option("FREEZE", null));
            }
            if (parser.matchKeyword("VERBOSE")) {
                options.add(new MaintenanceStmt.Option("VERBOSE", null));
            }
            if (parser.matchKeyword("ANALYZE") || parser.matchKeyword("ANALYSE")) {
                options.add(new MaintenanceStmt.Option("ANALYZE", null));
            }
            // A second option word after them is one the order had no place for.
            Token next = parser.peek();
            if (next.type() == TokenType.KEYWORD && BARE_VACUUM_WORDS.contains(
                    next.value().toUpperCase(java.util.Locale.ROOT))) {
                throw ParseException.saying(SYNTAX_AT + next.raw() + Q, next, "42601");
            }
        }
        checkVacuumOptions(options);
        String schema = null;
        String name = null;
        List<String> columns = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            String[] relation = parseMaintenanceRelation();
            schema = relation[0];
            name = relation[1];
            columns = parseMaintenanceColumns();
            if (columns != null && !isVacuumAnalysing(options)) {
                throw new MemgresException(
                        "ANALYZE option must be specified when a column list is provided", "0A000");
            }
        }
        expectNothingFurther();
        if (name != null) {
            for (MaintenanceStmt.Option o : options) {
                if ("ONLY_DATABASE_STATS".equals(o.name)) {
                    throw new MemgresException(
                            "ONLY_DATABASE_STATS cannot be specified with a list of tables",
                            "0A000");
                }
            }
        }
        return new MaintenanceStmt(MaintenanceStmt.Verb.VACUUM, options, null, schema, name,
                columns, null, false);
    }

    /** The words the bare VACUUM spelling reads, so a leftover one is reported as one. */
    private static final Set<String> BARE_VACUUM_WORDS =
            Cols.setOf("FULL", "FREEZE", "VERBOSE", "ANALYZE", "ANALYSE");

    private static boolean isVacuumAnalysing(List<MaintenanceStmt.Option> options) {
        for (MaintenanceStmt.Option o : options) {
            if (!"ANALYZE".equals(o.name) && !"ANALYSE".equals(o.name)) continue;
            if (o.value == null) return true;
            return !("false".equalsIgnoreCase(o.value) || "off".equalsIgnoreCase(o.value)
                    || "0".equals(o.value));
        }
        return false;
    }

    /**
     * What a VACUUM option list has to say for itself. A boolean option takes a boolean, the
     * parallel degree is a small number, the buffer limit is a size PostgreSQL will work in, and
     * a full vacuum has no parallel workers to give.
     */
    private void checkVacuumOptions(List<MaintenanceStmt.Option> options) {
        boolean full = false;
        boolean parallel = false;
        for (MaintenanceStmt.Option o : options) {
            if ("FULL".equals(o.name) && (o.value == null || !"false".equalsIgnoreCase(o.value))) {
                full = true;
            }
            if ("INDEX_CLEANUP".equals(o.name) && o.value != null
                    && !isBooleanWord(o.value) && !"auto".equalsIgnoreCase(o.value)) {
                throw new MemgresException("index_cleanup requires a Boolean value", "42601");
            }
            if ("PARALLEL".equals(o.name)) {
                if (o.value == null) {
                    throw new MemgresException(
                            "parallel option requires a value between 0 and 1024", "42601");
                }
                int workers;
                try {
                    workers = Integer.parseInt(o.value.trim());
                } catch (NumberFormatException e) {
                    throw new MemgresException(
                            "parallel option requires a value between 0 and 1024", "42601");
                }
                if (workers < 0 || workers > 1024) {
                    throw new MemgresException(
                            "parallel workers for vacuum must be between 0 and 1024", "42601");
                }
                parallel = workers > 0;
            }
            if ("BUFFER_USAGE_LIMIT".equals(o.name)) checkBufferUsageLimit(o.value);
        }
        if (full && parallel) {
            throw new MemgresException("VACUUM FULL cannot be performed in parallel", "0A000");
        }
    }

    private static boolean isBooleanWord(String value) {
        String v = value.trim();
        return v.equalsIgnoreCase("true") || v.equalsIgnoreCase("false")
                || v.equalsIgnoreCase("on") || v.equalsIgnoreCase("off")
                || v.equalsIgnoreCase("yes") || v.equalsIgnoreCase("no")
                || v.equals("1") || v.equals("0")
                || v.equalsIgnoreCase("t") || v.equalsIgnoreCase("f");
    }

    /** The ring buffer a vacuum may use is nothing at all, or a size between 128 kB and 16 GB. */
    private void checkBufferUsageLimit(String written) {
        if (written == null) return;
        long kilobytes;
        try {
            kilobytes = sizeInKilobytes(written.trim());
        } catch (NumberFormatException e) {
            throw new MemgresException("BUFFER_USAGE_LIMIT option must be 0 or between 128 kB"
                    + " and 16777216 kB", "22023");
        }
        if (kilobytes == 0) return;
        if (kilobytes < 128 || kilobytes > 16777216) {
            MemgresException bad = new MemgresException("BUFFER_USAGE_LIMIT option must be 0 or"
                    + " between 128 kB and 16777216 kB", "22023");
            if (kilobytes > Integer.MAX_VALUE) bad.setHint("Value exceeds integer range.");
            throw bad;
        }
    }

    /** A size written the way a storage parameter is written, read as kilobytes. */
    private static long sizeInKilobytes(String written) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(\\d+)\\s*(B|kB|MB|GB|TB)?", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(written);
        if (!m.matches()) throw new NumberFormatException(written);
        long value = Long.parseLong(m.group(1));
        String unit = m.group(2) == null ? "kB" : m.group(2).toUpperCase(java.util.Locale.ROOT);
        switch (unit) {
            case "B": return value / 1024;
            case "KB": return value;
            case "MB": return value * 1024;
            case "GB": return value * 1024 * 1024;
            case "TB": return value * 1024 * 1024 * 1024;
            default: return value;
        }
    }

    MaintenanceStmt parseReindex() {
        parser.expectKeyword("REINDEX");
        List<MaintenanceStmt.Option> options = parseMaintenanceOptions("REINDEX", REINDEX_OPTIONS);
        MaintenanceStmt.Target target;
        if (parser.matchKeyword("INDEX")) target = MaintenanceStmt.Target.INDEX;
        else if (parser.matchKeyword("TABLE")) target = MaintenanceStmt.Target.TABLE;
        else if (parser.matchKeyword("SCHEMA")) target = MaintenanceStmt.Target.SCHEMA;
        else if (parser.matchKeyword("DATABASE")) target = MaintenanceStmt.Target.DATABASE;
        else if (parser.matchKeyword("SYSTEM")) target = MaintenanceStmt.Target.SYSTEM;
        else {
            // REINDEX names what kind of thing it is reindexing; a bare name is not one.
            Token t = parser.peek();
            if (parser.isAtEnd() || t.type() == TokenType.EOF) {
                throw ParseException.saying("syntax error at end of input", t, "42601");
            }
            throw ParseException.saying(SYNTAX_AT + t.raw() + Q, t, "42601");
        }
        boolean concurrently = parser.matchKeyword("CONCURRENTLY");
        if (parser.isAtEnd() || parser.check(TokenType.SEMICOLON)) {
            throw ParseException.saying("syntax error at end of input", parser.peek(), "42601");
        }
        // REINDEX reaches every partition of what it names; there is no ONLY to write.
        Token first = parser.peek();
        if (first.type() == TokenType.KEYWORD && "ONLY".equalsIgnoreCase(first.value())) {
            throw ParseException.saying(SYNTAX_AT + first.raw() + Q, first, "42601");
        }
        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        expectNothingFurther();
        checkReindexTablespace(options);
        return new MaintenanceStmt(MaintenanceStmt.Verb.REINDEX, options, target, schema, name,
                null, null, concurrently);
    }

    /** A TABLESPACE named by a REINDEX has to be one the server has. */
    private void checkReindexTablespace(List<MaintenanceStmt.Option> options) {
        for (MaintenanceStmt.Option o : options) {
            if (!"TABLESPACE".equals(o.name) || o.value == null) continue;
            if (!"pg_default".equalsIgnoreCase(o.value) && !"pg_global".equalsIgnoreCase(o.value)) {
                throw new MemgresException(
                        "tablespace \"" + o.value + "\" does not exist", "42704");
            }
        }
    }

    MaintenanceStmt parseCluster() {
        parser.expectKeyword("CLUSTER");
        List<MaintenanceStmt.Option> options = parseMaintenanceOptions("CLUSTER", CLUSTER_OPTIONS);
        if (options.isEmpty() && parser.matchKeyword("VERBOSE")) {
            options.add(new MaintenanceStmt.Option("VERBOSE", null));
        }
        String schema = null;
        String name = null;
        String indexName = null;
        if (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            Token first = parser.peek();
            if (first.type() == TokenType.KEYWORD
                    && "USING".equalsIgnoreCase(first.value())) {
                throw ParseException.saying(SYNTAX_AT + first.raw() + Q, first, "42601");
            }
            String[] relation = parseMaintenanceRelation();
            schema = relation[0];
            name = relation[1];
            if (parser.matchKeyword("USING")) {
                indexName = parser.readIdentifier();
            } else if (parser.matchKeyword("ON")) {
                // The spelling PostgreSQL kept from before version 8.3 names the index first.
                indexName = name;
                String[] onRelation = parseMaintenanceRelation();
                schema = onRelation[0];
                name = onRelation[1];
            }
        }
        expectNothingFurther();
        return new MaintenanceStmt(MaintenanceStmt.Verb.CLUSTER, options, null, schema, name,
                null, indexName, false);
    }

    MaintenanceStmt parseCheckpoint() {
        parser.expectKeyword("CHECKPOINT");
        // CHECKPOINT is the whole statement: nothing follows it.
        expectNothingFurther();
        return new MaintenanceStmt(MaintenanceStmt.Verb.CHECKPOINT,
                new ArrayList<MaintenanceStmt.Option>(), null, null, null, null, null, false);
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
