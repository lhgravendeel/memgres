package com.memgres.engine.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SQL Lexer: tokenizes SQL text into a stream of tokens.
 * Handles PostgreSQL-specific syntax: dollar-quoting, :: casts,
 * JSON operators, E'...' strings, $N parameters, etc.
 */
public class Lexer {

    private static final Set<String> KEYWORDS = new HashSet<>(java.util.Arrays.asList(
            // DDL
            "CREATE", "ALTER", "DROP", "TABLE", "INDEX", "VIEW", "SCHEMA", "TYPE",
            "SEQUENCE", "FUNCTION", "PROCEDURE", "TRIGGER", "EXTENSION", "DATABASE",
            "IF", "EXISTS", "NOT", "CASCADE", "CASCADED", "RESTRICT", "TEMPORARY", "TEMP", "UNLOGGED", "DATA",
            "INHERITS", "INHERIT", "PARTITION", "MATERIALIZED", "REPLACE", "LIST", "HASH",
            "MODULUS", "REMAINDER",
            // Column constraints
            "PRIMARY", "KEY", "UNIQUE", "CHECK", "REFERENCES", "CONSTRAINT", "CONSTRAINTS", "FOREIGN",
            "DEFAULT", "NULL", "GENERATED", "ALWAYS", "STORED", "VIRTUAL", "IDENTITY",
            // Data types (keywords, not all; many are identifiers)
            "INTEGER", "INT", "SMALLINT", "BIGINT", "REAL", "FLOAT", "DOUBLE", "PRECISION",
            "NUMERIC", "DECIMAL", "BOOLEAN", "BOOL", "SERIAL", "BIGSERIAL", "SMALLSERIAL",
            "VARCHAR", "CHAR", "CHARACTER", "TEXT", "BYTEA", "UUID", "JSON", "JSONB",
            "DATE", "TIME", "TIMESTAMP", "INTERVAL", "TIMESTAMPTZ",
            "INET", "CIDR", "MACADDR", "MONEY", "XML", "BIT", "VARYING",
            // DML
            "SELECT", "INSERT", "UPDATE", "DELETE", "INTO", "FROM", "WHERE", "SET",
            "VALUES", "RETURNING", "ON", "CONFLICT", "DO", "NOTHING",
            // Clauses
            "AS", "AND", "OR", "IN", "BETWEEN", "LIKE", "ILIKE", "SIMILAR", "TO",
            "IS", "ANY", "ALL", "SOME", "ARRAY", "SUBSTRING",
            "ORDER", "BY", "ASC", "DESC", "NULLS", "FIRST", "LAST",
            "GROUP", "HAVING", "LIMIT", "OFFSET", "FETCH", "NEXT", "ROWS", "ONLY",
            "DISTINCT", "UNION", "INTERSECT", "EXCEPT",
            // Joins
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL",
            "LATERAL", "USING",
            // Subquery
            "WITH", "RECURSIVE",
            // CASE
            "CASE", "WHEN", "THEN", "ELSE", "END",
            // Window
            "OVER", "WINDOW", "RANGE", "ROW", "ORDINALITY", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "GROUPS",
            // Transactions
            "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "START", "TRANSACTION",
            "ISOLATION", "LEVEL", "READ", "COMMITTED", "UNCOMMITTED", "REPEATABLE",
            "SERIALIZABLE", "ABORT", "WORK",
            // Prepared statements & Cursors
            "PREPARE", "DEALLOCATE", "CURSOR", "OPEN", "MOVE", "CLOSE",
            "SCROLL", "HOLD", "INSENSITIVE", "PRIOR", "FORWARD", "BACKWARD", "ABSOLUTE", "RELATIVE",
            // Locking
            "NOWAIT", "SKIP", "LOCKED", "SHARE", "ACCESS", "EXCLUSIVE", "MODE",
            // SET
            "DISCARD", "RESET", "SHOW", "LOCAL", "SESSION",
            // Functions/Triggers
            "RETURNS", "LANGUAGE", "BEFORE", "AFTER", "INSTEAD", "OF",
            "FOR", "EACH", "EXECUTE",
            "NEW", "OLD", "RETURN",
            "DECLARE", "RAISE", "NOTICE", "EXCEPTION", "WARNING",
            // Misc
            "ENUM", "ADD", "COLUMN", "RENAME", "OWNER", "GRANT", "REVOKE", "ATTRIBUTE",
            "TRUE", "FALSE", "COALESCE", "NULLIF", "GREATEST", "LEAST",
            "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_USER", "SESSION_USER", "CURRENT_ROLE", "CURRENT_CATALOG", "CURRENT_SCHEMA",
            "POSITION", "OVERLAY", "PLACING",
            "CAST", "COLLATE", "VERBOSE", "ANALYZE", "EXPLAIN", "FORMAT",
            "TRUNCATE", "COPY", "LOCK", "VACUUM", "CLUSTER", "REINDEX", "REFRESH",
            "NOTIFY", "LISTEN", "UNLISTEN",
            "PERFORM", "GET", "DIAGNOSTICS", "FOUND",
            "LOOP", "WHILE", "EXIT", "CONTINUE", "FOREACH", "ELSIF", "ELSEIF",
            "AT", "ZONE", "WITHOUT", "FORCE",
            "LOCALTIME", "LOCALTIMESTAMP",
            "POLICY", "ENABLE", "DISABLE",
            "CONCURRENTLY", "INCLUDE", "INCREMENT", "MINVALUE", "MAXVALUE",
            "CACHE", "CYCLE", "OWNED",
            "DOMAIN", "EXTRACT", "VALUE", "FILTER", "ESCAPE",
            "EXCLUDE", "NO", "ACTION",
            "DEFERRABLE", "INITIALLY", "DEFERRED", "IMMEDIATE", "ENFORCED",
            "VALID", "VALIDATE", "HANDLER", "INLINE", "TRUSTED", "BINARY", "INSENSITIVE",
            "COMMENT", "SECURITY", "DEFINER", "INVOKER",
            "CALLED", "INPUT", "STRICT", "IMMUTABLE", "STABLE", "VOLATILE",
            "PARALLEL", "SAFE", "UNSAFE", "RESTRICTED",
            "COST", "SUPPORT", "LEAKPROOF",
            "WORK", "ABORT",
            "PROCEDURE",  // used in EXECUTE PROCEDURE
            "CALL", "SETOF", "INOUT", "VARIADIC", "SLICE", "ROUTINE", "ASSERT",
            "TIES", "OTHERS",
            "ATOMIC", "CHAIN",
            "ATTACH", "DETACH", "STATEMENT", "REFERENCING",
            "MERGE", "MATCHED", "RESTART", "MATCH", "PARTIAL", "SIMPLE",
            "TRIM", "LEADING", "TRAILING", "BOTH",
            "UNKNOWN", "SYMMETRIC", "OVERRIDING",
            "AUTHORIZATION", "INCLUDING", "EXCLUDING",
            "RULE", "INSTEAD", "ALSO",
            "COLLATION", "CONVERSION", "AGGREGATE", "OPERATOR", "CLASS", "FAMILY",
            "SFUNC", "STYPE", "FINALFUNC", "INITCOND", "COMBINEFUNC", "SORTOP",
            "SEARCH", "PROVIDER", "DETERMINISTIC", "LOCALE",
            "WITHIN", "GROUPING", "SETS", "ROLLUP", "CUBE",
            "TABLESAMPLE", "BERNOULLI", "DEPTH", "BREADTH",
            "SUPERUSER", "NOSUPERUSER", "CREATEDB", "NOCREATEDB",
            "CREATEROLE", "NOCREATEROLE", "LOGIN", "NOLOGIN",
            "REPLICATION", "NOREPLICATION", "BYPASSRLS", "NOBYPASSRLS",
            "ENCRYPTED", "CONNECTION", "NOINHERIT",
            "PERMISSIVE", "RESTRICTIVE", "ADMIN", "PRIVILEGES",
            "REASSIGN", "OPTION",
            "ROLE", "USER", "PASSWORD", "SYSID", "UNTIL",
            "ANALYSE", "LABEL", "CHECKPOINT", "LOAD",
            "FREEZE", "FULL", "OFF",
            "BUFFERS", "TIMING", "SUMMARY", "WAL", "SETTINGS", "SYSTEM",
            "XMLPARSE", "XMLSERIALIZE", "XMLELEMENT", "XMLFOREST", "XMLPI",
            "XMLROOT", "XMLCONCAT", "XMLEXISTS", "XMLAGG", "XMLATTRIBUTES",
            "XMLTEXT", "XMLCOMMENT", "DOCUMENT", "CONTENT", "STANDALONE",
            "PASSING", "NAME", "VERSION",
            // SQL/JSON standard keywords
            "JSON_TABLE", "JSON_EXISTS", "JSON_VALUE", "JSON_QUERY",
            "JSON_ARRAY", "JSON_OBJECT", "JSON_ARRAYAGG", "JSON_OBJECTAGG",
            "JSON_SCALAR", "JSON_SERIALIZE",
            "SCALAR", "WRAPPER", "CONDITIONAL", "UNCONDITIONAL",
            "KEEP", "OMIT", "QUOTES", "KEYS", "EMPTY",
            "COLUMNS", "ERROR", "PATH", "NESTED", "ABSENT",
            // No-op DDL targets
            "SERVER", "MAPPING", "IMPORT", "OPTIONS",
            "PUBLICATION", "SUBSCRIPTION", "TABLES",
            "TABLESPACE",
            "TRANSFORM",
            "STATISTICS", "METHOD",
            "EVENT", "OBJECT", "LARGE",
            "PROCEDURAL", "PREPARED"
    ));

    private final String sql;
    private int pos;
    private final int length;

    public Lexer(String sql) {
        this.sql = sql;
        this.pos = 0;
        this.length = sql.length();
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (pos < length) {
            skipWhitespaceAndComments();
            if (pos >= length) break;
            Token token = nextToken();
            if (token != null) {
                tokens.add(token);
            }
        }
        tokens.add(new Token(TokenType.EOF, "", pos));
        return tokens;
    }

    private void skipWhitespaceAndComments() {
        while (pos < length) {
            char c = sql.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
            } else if (c == '-' && pos + 1 < length && sql.charAt(pos + 1) == '-') {
                // Line comment
                pos += 2;
                while (pos < length && sql.charAt(pos) != '\n') pos++;
            } else if (c == '/' && pos + 1 < length && sql.charAt(pos + 1) == '*') {
                // Block comment
                pos += 2;
                int depth = 1;
                while (pos < length - 1 && depth > 0) {
                    if (sql.charAt(pos) == '/' && sql.charAt(pos + 1) == '*') {
                        depth++;
                        pos += 2;
                    } else if (sql.charAt(pos) == '*' && sql.charAt(pos + 1) == '/') {
                        depth--;
                        pos += 2;
                    } else {
                        pos++;
                    }
                }
            } else {
                break;
            }
        }
    }

    private Token nextToken() {
        int start = pos;
        char c = sql.charAt(pos);

        // String literal: 'text' or E'text'
        if (c == '\'') {
            return readStringLiteral(start);
        }
        if ((c == 'E' || c == 'e') && pos + 1 < length && sql.charAt(pos + 1) == '\'') {
            pos++; // skip E
            return readEscapeStringLiteral(start);
        }
        // Bit string literals: B'101' (binary) or X'1FF' (hex)
        if ((c == 'B' || c == 'b' || c == 'X' || c == 'x') && pos + 1 < length && sql.charAt(pos + 1) == '\'') {
            boolean isHex = (c == 'X' || c == 'x');
            pos++; // skip B or X
            Token inner = readStringLiteral(start);
            String val = inner.value();
            if (isHex) {
                // Convert hex to binary string
                StringBuilder bits = new StringBuilder();
                for (char hc : val.toCharArray()) {
                    int d = Character.digit(hc, 16);
                    if (d < 0) return new Token(TokenType.ERROR, val, start);
                    bits.append(String.format("%4s", Integer.toBinaryString(d)).replace(' ', '0'));
                }
                val = bits.toString();
            }
            return new Token(TokenType.BIT_STRING_LITERAL, val, start);
        }

        // Dollar-quoted string: $$...$$ or $tag$...$tag$
        if (c == '$') {
            Token dollarStr = tryReadDollarString(start);
            if (dollarStr != null) return dollarStr;

            // Positional parameter: $1, $2, etc.
            if (pos + 1 < length && Character.isDigit(sql.charAt(pos + 1))) {
                return readParameter(start);
            }

            // Bare $ with no following digit or dollar-string: syntax error
            pos++;
            return new Token(TokenType.ERROR, "$", start);
        }

        // Quoted identifier: "name"
        if (c == '"') {
            return readQuotedIdentifier(start);
        }

        // Numbers (but don't start a number with '.' if preceded by another '.', as that's the .. range operator)
        if (Character.isDigit(c) || (c == '.' && pos + 1 < length && Character.isDigit(sql.charAt(pos + 1))
                && !(pos > 0 && sql.charAt(pos - 1) == '.'))) {
            return readNumber(start);
        }

        // A national-character literal holds the same characters an ordinary one does — there is
        // only the one character set here — but it is a value of the national character type, so
        // it is kept apart from a plain string. It used to be read as a type name followed by a
        // string, so N'abc' was a cast to a type called n.
        if ((c == 'N' || c == 'n') && pos + 1 < length && sql.charAt(pos + 1) == '\'') {
            String marker = String.valueOf(c);
            pos += 1;
            Token literal = readStringLiteral(start);
            // The marker is kept as the token's written form: a clause that takes a plain string
            // and not this stops on the letter, which is the letter PostgreSQL names.
            return new Token(TokenType.NATIONAL_STRING_LITERAL, literal.value(), start, marker);
        }

        // Unicode escape strings: U&'...'
        if ((c == 'U' || c == 'u') && pos + 1 < length && sql.charAt(pos + 1) == '&'
                && pos + 2 < length && sql.charAt(pos + 2) == '\'') {
            pos += 2; // skip U&
            return readUnicodeStringLiteral(start);
        }

        // Unicode escape identifiers: U&"..."
        if ((c == 'U' || c == 'u') && pos + 1 < length && sql.charAt(pos + 1) == '&'
                && pos + 2 < length && sql.charAt(pos + 2) == '"') {
            pos += 2; // skip U&
            return readUnicodeIdentifier(start);
        }

        // Identifiers and keywords
        if (Character.isLetter(c) || c == '_') {
            return readIdentifierOrKeyword(start);
        }

        // Multi-character operators (order matters; check longer patterns first)
        if (c == ':' && pos + 1 < length) {
            if (sql.charAt(pos + 1) == ':') {
                pos += 2;
                return new Token(TokenType.CAST, "::", start);
            }
            if (sql.charAt(pos + 1) == '=') {
                pos += 2;
                return new Token(TokenType.COLON_EQUALS, ":=", start);
            }
            pos++;
            return new Token(TokenType.COLON, ":", start);
        }

        // Operator characters: PG-compatible greedy scanning
        // PG operator chars: + - * / < > = ~ ! @ # % ^ & | ?
        if (isOperatorChar(c)) {
            return scanAndClassifyOperator(start);
        }

        // Non-operator punctuation
        pos++;
        switch (c) {
            case '(':
                return new Token(TokenType.LEFT_PAREN, "(", start);
            case ')':
                return new Token(TokenType.RIGHT_PAREN, ")", start);
            case '[':
                return new Token(TokenType.LEFT_BRACKET, "[", start);
            case ']':
                return new Token(TokenType.RIGHT_BRACKET, "]", start);
            case ',':
                return new Token(TokenType.COMMA, ",", start);
            case ';':
                return new Token(TokenType.SEMICOLON, ";", start);
            case '.':
                // Two dots together are one token, as they are in PostgreSQL, where they write
                // a range in PL/pgSQL. Nothing in SQL accepts one, so a run of dots is reported
                // as ".." rather than as the first single dot of it.
                if (pos < sql.length() && sql.charAt(pos) == '.') {
                    pos++;
                    return new Token(TokenType.DOT_DOT, "..", start);
                }
                return new Token(TokenType.DOT, ".", start);
            default:
                return new Token(TokenType.ERROR, String.valueOf(c), start);
        }
    }

    /**
     * Scans an operator token using PG-compatible greedy scanning.
     * 1. Greedily consume all consecutive operator characters
     * 2. Truncate if the sequence contains -- or /* (comment syntax)
     * 3. Apply PG trailing +/- rule: if the operator ends with + or -,
     *    and does NOT also contain ~ ! @ # % ^ & | ?, give back trailing +/- chars
     * 4. Map to known token type or CUSTOM_OPERATOR
     */
    private Token scanAndClassifyOperator(int start) {
        // Step 1: greedily consume operator characters
        pos++; // consume first char
        while (pos < length && isOperatorChar(sql.charAt(pos))) {
            pos++;
        }
        String op = sql.substring(start, pos);

        // Step 2: truncate at -- or /* (comment syntax takes precedence)
        int dashDash = op.indexOf("--");
        int slashStar = op.indexOf("/*");
        int truncAt = -1;
        if (dashDash >= 0) truncAt = dashDash;
        if (slashStar >= 0 && (truncAt < 0 || slashStar < truncAt)) truncAt = slashStar;
        if (truncAt > 0) {
            pos = start + truncAt;
            op = op.substring(0, truncAt);
        } else if (truncAt == 0) {
            // Operator starts with -- or /* — should not happen (comments are stripped earlier)
            // but handle defensively: return just the first char
            pos = start + 1;
            op = op.substring(0, 1);
        }

        // Step 3: PG trailing +/- rule
        // If operator ends with + or - and does NOT contain ~ ! @ # % ^ & | ?,
        // give back trailing +/- chars until it no longer ends with them or is 1 char.
        while (op.length() > 1) {
            char last = op.charAt(op.length() - 1);
            if (last != '+' && last != '-') break;
            boolean hasSpecial = false;
            for (int i = 0; i < op.length(); i++) {
                char ch = op.charAt(i);
                if (ch == '~' || ch == '!' || ch == '@' || ch == '#' || ch == '%'
                        || ch == '^' || ch == '&' || ch == '|' || ch == '?') {
                    hasSpecial = true;
                    break;
                }
            }
            if (hasSpecial) break;
            // Give back trailing +/- chars one at a time
            op = op.substring(0, op.length() - 1);
            pos--;
        }

        // Step 4: classify
        return classifyOperator(op, start);
    }

    /**
     * Maps an operator string to the appropriate token type.
     * Known operators get their specific token types; unknown multi-char sequences
     * get CUSTOM_OPERATOR.
     */
    private Token classifyOperator(String op, int start) {
        switch (op) {
            // Single-char operators
            case "+": return new Token(TokenType.PLUS, "+", start);
            case "-": return new Token(TokenType.MINUS, "-", start);
            case "*": return new Token(TokenType.STAR, "*", start);
            case "/": return new Token(TokenType.SLASH, "/", start);
            case "%": return new Token(TokenType.PERCENT, "%", start);
            case "^": return new Token(TokenType.CARET, "^", start);
            case "&": return new Token(TokenType.AMPERSAND, "&", start);
            case "|": return new Token(TokenType.PIPE, "|", start);
            case "~": return new Token(TokenType.TILDE, "~", start);
            case "#": return new Token(TokenType.HASH, "#", start);
            case "?": return new Token(TokenType.JSONB_EXISTS, "?", start);
            case "<": return new Token(TokenType.LESS_THAN, "<", start);
            case ">": return new Token(TokenType.GREATER_THAN, ">", start);
            case "=": return new Token(TokenType.EQUALS, "=", start);
            case "@": return new Token(TokenType.AT_SIGN, "@", start);
            case "!": return new Token(TokenType.ERROR, "!", start);

            // 2-char operators
            case "||": return new Token(TokenType.CONCAT, "||", start);
            case "->": return new Token(TokenType.JSON_ARROW, "->", start);
            case "<<": return new Token(TokenType.SHIFT_LEFT, "<<", start);
            case ">>": return new Token(TokenType.SHIFT_RIGHT, ">>", start);
            case "<=": return new Token(TokenType.LESS_EQUALS, "<=", start);
            case ">=": return new Token(TokenType.GREATER_EQUALS, ">=", start);
            case "<>": return new Token(TokenType.NOT_EQUALS, "<>", start);
            case "!=": return new Token(TokenType.NOT_EQUALS, "!=", start);
            case "<@": return new Token(TokenType.CONTAINED_BY, "<@", start);
            case "@>": return new Token(TokenType.CONTAINS, "@>", start);
            case "@@": return new Token(TokenType.TS_MATCH, "@@", start);
            case "@?": return new Token(TokenType.JSONB_PATH_EXISTS_OP, "@?", start);
            case "&&": return new Token(TokenType.OVERLAP, "&&", start);
            case "?|": return new Token(TokenType.JSONB_EXISTS_ANY, "?|", start);
            case "?&": return new Token(TokenType.JSONB_EXISTS_ALL, "?&", start);
            case "?#": return new Token(TokenType.GEO_INTERSECTS, "?#", start);
            case "##": return new Token(TokenType.GEO_CLOSEST_POINT, "##", start);
            case "?-": return new Token(TokenType.GEO_IS_HORIZONTAL, "?-", start);
            case "#>": return new Token(TokenType.JSON_HASH_ARROW, "#>", start);
            case "#-": return new Token(TokenType.JSON_DELETE_PATH, "#-", start);
            case "<^": return new Token(TokenType.GEO_BELOW_EQ, "<^", start);
            case ">^": return new Token(TokenType.GEO_ABOVE_EQ, ">^", start);
            case "&<": return new Token(TokenType.GEO_NOT_EXTEND_RIGHT, "&<", start);
            case "&>": return new Token(TokenType.GEO_NOT_EXTEND_LEFT, "&>", start);
            case "!~": return new Token(TokenType.EXCL_TILDE, "!~", start);
            case "~~": return new Token(TokenType.DOUBLE_TILDE, "~~", start);
            case "~*": return new Token(TokenType.TILDE_STAR, "~*", start);
            case "~=": return new Token(TokenType.APPROX_EQUAL, "~=", start);
            case "!!": return new Token(TokenType.ERROR, "!!", start);
            case "=>": return new Token(TokenType.FAT_ARROW, "=>", start);

            // 3-char operators
            case "->>": return new Token(TokenType.JSON_ARROW_TEXT, "->>", start);
            case "-|-": return new Token(TokenType.RANGE_ADJACENT, "-|-", start);
            case ">>=": return new Token(TokenType.INET_CONTAINS_EQUALS, ">>=", start);
            case "<<=": return new Token(TokenType.INET_CONTAINED_BY_EQUALS, "<<=", start);
            case "<<|": return new Token(TokenType.GEO_BELOW, "<<|", start);
            case "|>>": return new Token(TokenType.GEO_ABOVE, "|>>", start);
            case "|&>": return new Token(TokenType.GEO_NOT_EXTEND_BELOW, "|&>", start);
            case "#>>": return new Token(TokenType.JSON_HASH_ARROW_TEXT, "#>>", start);
            // @@@ is PostgreSQL's older spelling of @@ and means exactly the same thing.
            case "@@@": return new Token(TokenType.TS_MATCH, "@@@", start);
            case "!~~": return new Token(TokenType.NOT_DOUBLE_TILDE, "!~~", start);
            case "!~*": return new Token(TokenType.EXCL_TILDE_STAR, "!~*", start);
            case "?||": return new Token(TokenType.GEO_PARALLEL, "?||", start);
            case "?-|": return new Token(TokenType.GEO_PERPENDICULAR, "?-|", start);
            case "&<|": return new Token(TokenType.GEO_NOT_EXTEND_ABOVE, "&<|", start);
            case "~~*": return new Token(TokenType.DOUBLE_TILDE_STAR, "~~*", start);
            case "<->": return new Token(TokenType.DISTANCE, "<->", start);

            // 4-char operators
            case "!~~*": return new Token(TokenType.NOT_DOUBLE_TILDE_STAR, "!~~*", start);

            default:
                if (op.length() > 1) {
                    return new Token(TokenType.CUSTOM_OPERATOR, op, start);
                }
                return new Token(TokenType.ERROR, op, start);
        }
    }

    /**
     * Returns true if the character is a valid PostgreSQL operator character.
     * PG operator chars: + - * / < > = ~ ! @ # % ^ & | ?
     */
    private static boolean isOperatorChar(char c) {
        switch (c) {
            case '+': case '-': case '*': case '/': case '<': case '>':
            case '=': case '~': case '!': case '@': case '#': case '%':
            case '^': case '&': case '|': case '?':
                return true;
            default:
                return false;
        }
    }

    private Token readEscapeStringLiteral(int start) {
        pos++; // skip opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < length) {
            char c = sql.charAt(pos);
            if (c == '\'') {
                if (pos + 1 < length && sql.charAt(pos + 1) == '\'') {
                    sb.append('\'');
                    pos += 2;
                } else {
                    pos++; // skip closing quote
                    return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
                }
            } else if (c == '\\' && pos + 1 < length) {
                char next = sql.charAt(pos + 1);
                switch (next) {
                    case 'n': {
                        sb.append('\n'); pos += 2; 
                        break;
                    }
                    case 't': {
                        sb.append('\t'); pos += 2; 
                        break;
                    }
                    case 'r': {
                        sb.append('\r'); pos += 2; 
                        break;
                    }
                    case '\\': {
                        sb.append('\\'); pos += 2; 
                        break;
                    }
                    case '\'': {
                        sb.append('\''); pos += 2; 
                        break;
                    }
                    default: {
                        // Octal escape: \ooo (1-3 octal digits)
                        if (next >= '0' && next <= '7') {
                            int escStart = pos;
                            int val = next - '0';
                            pos += 2;
                            for (int oi = 0; oi < 2 && pos < length; oi++) {
                                char oc = sql.charAt(pos);
                                if (oc >= '0' && oc <= '7') { val = val * 8 + (oc - '0'); pos++; }
                                else break;
                            }
                            // A string is a run of characters and the zero byte is not one of
                            // them: it ends a string rather than standing in it, so it cannot be
                            // written into one.
                            if (val == 0) {
                                throw ParseException.saying(
                                        "invalid byte sequence for encoding \"UTF8\": 0x00",
                                        new Token(TokenType.ERROR, sql.substring(escStart, pos),
                                                escStart), "22021");
                            }
                            sb.append((char) val);
                        } else if (next == 'x' || next == 'X') {
                            // Hex escape: \xHH (1-2 hex digits)
                            int escStart = pos;
                            pos += 2;
                            int val = 0;
                            int hexCount = 0;
                            while (pos < length && hexCount < 2) {
                                char hc = sql.charAt(pos);
                                int hv = Character.digit(hc, 16);
                                if (hv < 0) break;
                                val = val * 16 + hv;
                                pos++;
                                hexCount++;
                            }
                            if (val == 0 && hexCount > 0) {
                                throw ParseException.saying(
                                        "invalid byte sequence for encoding \"UTF8\": 0x00",
                                        new Token(TokenType.ERROR, sql.substring(escStart, pos),
                                                escStart), "22021");
                            }
                            sb.append((char) val);
                        } else if (next == 'u' || next == 'U') {
                            // Unicode escape: backslash-u XXXX or backslash-U XXXXXXXX
                            int escapeStart = pos;
                            int digits = (next == 'u') ? 4 : 8;
                            pos += 2;
                            int val = 0;
                            for (int ui = 0; ui < digits && pos < length; ui++) {
                                char uc = sql.charAt(pos);
                                int uv = Character.digit(uc, 16);
                                if (uv < 0) break;
                                val = val * 16 + uv;
                                pos++;
                            }
                            // Eight hex digits reach well past the last code point there is, so a
                            // number naming no character is the escape's fault and PostgreSQL
                            // quotes the escape back as the statement wrote it.
                            if (!Character.isValidCodePoint(val) || val == 0) {
                                String written = sql.substring(escapeStart, pos);
                                throw ParseException.saying(
                                        "invalid Unicode escape value at or near \"" + written + "\"",
                                        new Token(TokenType.ERROR, written, escapeStart), "42601");
                            }
                            // Half of a surrogate pair is not a character: it only names one
                            // together with its other half, and written alone it names nothing.
                            if (Character.isSurrogate((char) val)) {
                                int low = Character.isHighSurrogate((char) val)
                                        ? followingLowSurrogateEscape() : -1;
                                if (low < 0) {
                                    // A low surrogate is wrong where it stands and is quoted back;
                                    // a high one is only wrong once what follows it is read, so
                                    // what follows is what the fault is reported at.
                                    String at = Character.isHighSurrogate((char) val)
                                            ? whatFollowsAnEscape()
                                            : sql.substring(escapeStart, pos);
                                    throw ParseException.saying(
                                            "invalid Unicode surrogate pair at or near \"" + at + "\"",
                                            new Token(TokenType.ERROR, at, pos), "42601");
                                }
                                sb.append((char) val).append((char) low);
                                break;
                            }
                            sb.appendCodePoint(val);
                        } else {
                            // A backslash in front of anything else escapes that character and
                            // is itself dropped: E'\z' is one letter, not two.
                            sb.append(next);
                            pos += 2;
                        }
                        break;
                    }
                }
            } else {
                sb.append(c);
                pos++;
            }
        }
        return new Token(TokenType.ERROR, sb.toString(), start);
    }

    private Token readUnicodeStringLiteral(int start) {
        // Read the raw string content first (pos is on the opening quote)
        Token raw = readStringLiteral(start);
        return new Token(TokenType.STRING_LITERAL,
                decodeUnicodeEscapes(raw.value(), readUescape(), start), start);
    }

    /**
     * The character that introduces an escape in the {@code U&} form just read.
     *
     * <p>A {@code UESCAPE 'x'} clause after the literal names it, and without one it is the
     * backslash. The clause was not read at all, so the escape character could never be changed
     * and every literal that named one was a syntax error at the clause.
     */
    /** The escape written at the read point, or the one character there if it is not one. */
    private String whatFollowsAnEscape() {
        if (pos >= length) return "";
        if (sql.charAt(pos) == '\\' && pos + 1 < length) {
            char marker = sql.charAt(pos + 1);
            if (marker == 'u' || marker == 'U') {
                int digits = marker == 'u' ? 4 : 8;
                int end = pos + 2;
                while (end < length && end < pos + 2 + digits
                        && Character.digit(sql.charAt(end), 16) >= 0) {
                    end++;
                }
                return sql.substring(pos, end);
            }
        }
        return sql.substring(pos, pos + 1);
    }

    /**
     * The low surrogate written straight after the escape just read, or -1 if there is none. The
     * escape is consumed when there is one: the two halves are read together as one character.
     */
    private int followingLowSurrogateEscape() {
        if (pos + 2 >= length || sql.charAt(pos) != '\\') return -1;
        char marker = sql.charAt(pos + 1);
        if (marker != 'u' && marker != 'U') return -1;
        int digits = marker == 'u' ? 4 : 8;
        int val = 0;
        for (int i = 0; i < digits; i++) {
            if (pos + 2 + i >= length) return -1;
            int uv = Character.digit(sql.charAt(pos + 2 + i), 16);
            if (uv < 0) return -1;
            val = val * 16 + uv;
        }
        if (!Character.isLowSurrogate((char) val)) return -1;
        pos += 2 + digits;
        return val;
    }

    private char readUescape() {
        int mark = pos;
        while (pos < length && Character.isWhitespace(sql.charAt(pos))) pos++;
        if (pos + 7 <= length && sql.regionMatches(true, pos, "UESCAPE", 0, 7)) {
            int after = pos + 7;
            while (after < length && Character.isWhitespace(sql.charAt(after))) after++;
            if (after + 2 < length && sql.charAt(after) == '\''
                    && sql.charAt(after + 2) == '\'') {
                char escape = sql.charAt(after + 1);
                pos = after + 3;
                return escape;
            }
        }
        pos = mark;
        return '\\';
    }

    /**
     * Read the escapes in a {@code U&} literal: four hexadecimal digits, or a plus and six of
     * them, or the escape character twice for one of itself.
     */
    private String decodeUnicodeEscapes(String val, char escape, int start) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < val.length(); i++) {
            if (val.charAt(i) != escape || i + 1 >= val.length()) {
                sb.append(val.charAt(i));
                continue;
            }
            // A doubled escape character stands for one of itself, so U&'a\\b' is three
            // characters. Reading it as the start of a codepoint left too few digits behind it,
            // and a string PostgreSQL accepts was refused outright.
            if (val.charAt(i + 1) == escape) {
                sb.append(escape);
                i++;
                continue;
            }
            int digits = val.charAt(i + 1) == '+' ? 6 : 4;
            int from = val.charAt(i + 1) == '+' ? i + 2 : i + 1;
            if (from + digits > val.length()) {
                throw ParseException.saying("invalid Unicode escape value",
                        new Token(TokenType.ERROR, val, start), "42601");
            }
            int cp = 0;
            for (int k = from; k < from + digits; k++) {
                int digit = Character.digit(val.charAt(k), 16);
                if (digit < 0) {
                    throw ParseException.saying("invalid Unicode escape value",
                            new Token(TokenType.ERROR, val, start), "42601");
                }
                cp = cp * 16 + digit;
            }
            // The zero byte is not a character a string may hold, and a number past the last code
            // point names no character at all.
            if (cp > Character.MAX_CODE_POINT || cp == 0) {
                throw ParseException.saying("invalid Unicode escape value",
                        new Token(TokenType.ERROR, val, start), "42601");
            }
            // Half of a surrogate pair names a character only together with its other half, which
            // has to be written as the very next escape; alone it names nothing at all.
            if (Character.isSurrogate((char) cp)) {
                int[] low = Character.isHighSurrogate((char) cp)
                        ? escapeAt(val, escape, from + digits) : null;
                if (low == null || !Character.isLowSurrogate((char) low[0])) {
                    throw ParseException.saying("invalid Unicode surrogate pair",
                            new Token(TokenType.ERROR, val, start), "42601");
                }
                sb.append((char) cp).append((char) low[0]);
                i = low[1] - 1;
                continue;
            }
            sb.appendCodePoint(cp);
            i = from + digits - 1;
        }
        return sb.toString();
    }

    /**
     * The escape written at {@code i}, as its value and the index just past it, or null if no
     * whole escape is written there.
     */
    private int[] escapeAt(String val, char escape, int i) {
        if (i >= val.length() || val.charAt(i) != escape || i + 1 >= val.length()) return null;
        if (val.charAt(i + 1) == escape) return null;
        int digits = val.charAt(i + 1) == '+' ? 6 : 4;
        int from = val.charAt(i + 1) == '+' ? i + 2 : i + 1;
        if (from + digits > val.length()) return null;
        int cp = 0;
        for (int k = from; k < from + digits; k++) {
            int digit = Character.digit(val.charAt(k), 16);
            if (digit < 0) return null;
            cp = cp * 16 + digit;
        }
        return new int[] {cp, from + digits};
    }

    private Token readUnicodeIdentifier(int start) {
        // Read the raw quoted identifier content first (pos is on the opening double-quote)
        Token raw = readQuotedIdentifier(start);
        return new Token(TokenType.QUOTED_IDENTIFIER,
                decodeUnicodeEscapes(raw.value(), readUescape(), start), start);
    }

    private Token readStringLiteral(int start) {
        pos++; // skip opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < length) {
            char c = sql.charAt(pos);
            if (c == '\'') {
                if (pos + 1 < length && sql.charAt(pos + 1) == '\'') {
                    sb.append('\'');
                    pos += 2;
                } else {
                    pos++; // skip closing quote
                    // Two literals with a line break between them are one literal. PostgreSQL
                    // requires the break -- on one line they are two tokens and a syntax error
                    // -- which is what makes this safe to read here.
                    if (continuesOnNextLine()) continue;
                    return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
                }
            } else {
                sb.append(c);
                pos++;
            }
        }
        return new Token(TokenType.ERROR, sb.toString(), start);
    }

    private Token tryReadDollarString(int start) {
        // Try to read a dollar-quote tag: $$ or $tag$
        int tagStart = pos;
        pos++; // skip first $
        StringBuilder tag = new StringBuilder("$");

        while (pos < length && (Character.isLetterOrDigit(sql.charAt(pos)) || sql.charAt(pos) == '_')) {
            tag.append(sql.charAt(pos));
            pos++;
        }

        // A dollar quote's tag is an identifier: PostgreSQL has no bare "$ ... $" form, so
        // "SELECT $ 'hello' $" is a syntax error at the dollar rather than the string between.
        if (tag.length() > 1 && Character.isDigit(tag.charAt(1))) {
            pos = tagStart;
            return null;   // $1$ is the parameter $1, not a quote
        }

        if (pos < length && sql.charAt(pos) == '$') {
            tag.append('$');
            pos++;
            String delimiter = tag.toString();

            // Read until closing delimiter
            StringBuilder body = new StringBuilder();
            while (pos <= length - delimiter.length()) {
                if (sql.substring(pos, pos + delimiter.length()).equals(delimiter)) {
                    pos += delimiter.length();
                    return new Token(TokenType.DOLLAR_STRING_LITERAL, body.toString(), start);
                }
                body.append(sql.charAt(pos));
                pos++;
            }
        }

        // Not a dollar string; reset
        pos = tagStart;
        return null;
    }

    private Token readParameter(int start) {
        pos++; // skip $
        StringBuilder sb = new StringBuilder();
        while (pos < length && Character.isDigit(sql.charAt(pos))) {
            sb.append(sql.charAt(pos));
            pos++;
        }
        return new Token(TokenType.PARAM, "$" + sb, start);
    }

    private Token readQuotedIdentifier(int start) {
        pos++; // skip opening "
        StringBuilder sb = new StringBuilder();
        while (pos < length) {
            char c = sql.charAt(pos);
            if (c == '"') {
                if (pos + 1 < length && sql.charAt(pos + 1) == '"') {
                    sb.append('"');
                    pos += 2;
                } else {
                    pos++; // skip closing "
                    // PG truncates identifiers to NAMEDATALEN-1 = 63 bytes
                    String qid = sb.toString();
                    // An identifier written with nothing between its quotes names nothing, and
                    // PostgreSQL says so at the quotes rather than letting an empty name through.
                    if (qid.isEmpty()) {
                        throw ParseException.saying(
                                "zero-length delimited identifier at or near \"\"\"\"",
                                new Token(TokenType.ERROR, "\"\"", start), "42601");
                    }
                    if (qid.length() > 63) qid = qid.substring(0, 63);
                    return new Token(TokenType.QUOTED_IDENTIFIER, qid, start);
                }
            } else {
                sb.append(c);
                pos++;
            }
        }
        return new Token(TokenType.ERROR, sb.toString(), start);
    }

    /** The base a 0x / 0o / 0b marker introduces, or 0 when the character is not one. */
    private static int radixOf(char marker) {
        switch (marker) {
            case 'x': case 'X': return 16;
            case 'o': case 'O': return 8;
            case 'b': case 'B': return 2;
            default: return 0;
        }
    }

    /** What PostgreSQL calls a base when it names one in a complaint. */
    private static String baseName(int radix) {
        return radix == 16 ? "hexadecimal" : radix == 8 ? "octal" : "binary";
    }

    private Token readNumber(int start) {
        StringBuilder sb = new StringBuilder();
        boolean hasDecimal = false;

        // PostgreSQL 16 added non-decimal integer literals. Without them 0x10 lexed as the number
        // 0 followed by the identifier x10, which is trailing junk.
        if (sql.charAt(pos) == '0' && pos + 1 < length) {
            int radix = radixOf(sql.charAt(pos + 1));
            // A marker with no digit of its own base behind it is a number of that base with
            // nothing in it, which PostgreSQL names by the base rather than calling the marker
            // junk after a zero.
            if (radix > 0
                    && (pos + 2 >= length || Character.digit(sql.charAt(pos + 2), radix) < 0)
                    && (pos + 2 >= length || !Character.isLetterOrDigit(sql.charAt(pos + 2)))) {
                String written = sql.substring(pos, pos + 2);
                throw ParseException.saying("invalid " + baseName(radix)
                        + " integer at or near \"" + written + "\"",
                        new Token(TokenType.ERROR, written, start), "42601");
            }
            if (radix > 0 && pos + 2 < length
                    && Character.digit(sql.charAt(pos + 2), radix) >= 0) {
                pos += 2;
                StringBuilder digits = new StringBuilder();
                while (pos < length) {
                    char d = sql.charAt(pos);
                    if (d == '_' && digits.length() > 0) { pos++; continue; }
                    if (Character.digit(d, radix) < 0) break;
                    digits.append(d);
                    pos++;
                }
                java.math.BigInteger value = new java.math.BigInteger(digits.toString(), radix);
                return new Token(TokenType.INTEGER_LITERAL, value.toString(), start);
            }
        }

        while (pos < length) {
            char c = sql.charAt(pos);
            if (Character.isDigit(c)) {
                sb.append(c);
                pos++;
            } else if (c == '_' && sb.length() > 0 && pos + 1 < length && Character.isDigit(sql.charAt(pos + 1))) {
                // PG 16+ numeric underscore separator — skip it
                pos++;
            } else if (c == '.' && !hasDecimal) {
                // Check it's not a dot-separator (e.g., schema.table)
                if (pos + 1 < length && Character.isDigit(sql.charAt(pos + 1))) {
                    hasDecimal = true;
                    sb.append(c);
                    pos++;
                } else if (sb.length() == 0) {
                    // Leading dot like .5
                    hasDecimal = true;
                    sb.append(c);
                    pos++;
                } else {
                    break;
                }
            } else if ((c == 'e' || c == 'E') && sb.length() > 0) {
                // Scientific notation. An exponent marker with no digits behind it is not a
                // number at all: taking it for one produced a token BigDecimal could not read,
                // and the NumberFormatException reached the client as an internal error.
                int marker = pos;
                StringBuilder exponent = new StringBuilder();
                exponent.append(c);
                pos++;
                if (pos < length && (sql.charAt(pos) == '+' || sql.charAt(pos) == '-')) {
                    exponent.append(sql.charAt(pos));
                    pos++;
                }
                if (pos >= length || !Character.isDigit(sql.charAt(pos))) {
                    pos = marker;
                    throw ParseException.saying("trailing junk after numeric literal at or near \""
                            + sb + exponent + "\"", new Token(TokenType.ERROR, sb.toString(), start),
                            "42601");
                }
                hasDecimal = true;
                sb.append(exponent);
            } else {
                break;
            }
        }

        // PG rejects number immediately followed by letter (e.g., 123abc)
        if (pos < length && Character.isLetter(sql.charAt(pos)) && sql.charAt(pos) != 'e' && sql.charAt(pos) != 'E') {
            // The whole word is named, not just the letter the number ran into: PostgreSQL
            // points at "123abc" rather than at "123a".
            int end = pos;
            while (end < length && (Character.isLetterOrDigit(sql.charAt(end))
                    || sql.charAt(end) == '_')) {
                end++;
            }
            throw ParseException.saying("trailing junk after numeric literal at or near \""
                    + sb + sql.substring(pos, end) + "\"",
                    new Token(TokenType.ERROR, sb.toString(), start), "42601");
        }
        return new Token(hasDecimal ? TokenType.FLOAT_LITERAL : TokenType.INTEGER_LITERAL,
                sb.toString(), start);
    }

    private Token readIdentifierOrKeyword(int start) {
        StringBuilder sb = new StringBuilder();
        while (pos < length) {
            char c = sql.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '$') {
                sb.append(c);
                pos++;
            } else {
                break;
            }
        }
        String word = sb.toString();
        // Which word this is, and what an unquoted name folds to, are properties of the language
        // and not of the machine it runs on, so the fold is Locale.ROOT's. Under the default
        // locale a Turkish JVM reads IN as "İN", which is no keyword, and MIN as "mın", which is
        // no aggregate.
        String upper = word.toUpperCase(java.util.Locale.ROOT);

        if (KEYWORDS.contains(upper)) {

            return new Token(TokenType.KEYWORD, upper, start, word);
        }
        // PG truncates identifiers to NAMEDATALEN-1 = 63 bytes
        // Only the twenty-six letters are folded. PostgreSQL leaves everything else as written,
        // so MÜLLER is the column mÜller and müller names nothing; folding the whole word made
        // the two the same name and the one PostgreSQL rejects was found.
        String id = foldAsciiOnly(word);
        if (id.length() > 63) id = id.substring(0, 63);
        return new Token(TokenType.IDENTIFIER, id, start, word);
    }

    /**
     * Whether the literal just closed is continued by another one on a later line.
     *
     * <p>Consumes the whitespace and the opening quote when it is, so that reading carries on
     * into the second literal's characters.
     */
    private boolean continuesOnNextLine() {
        int mark = pos;
        boolean sawNewline = false;
        while (pos < length && Character.isWhitespace(sql.charAt(pos))) {
            if (sql.charAt(pos) == '\n') sawNewline = true;
            pos++;
        }
        if (sawNewline && pos < length && sql.charAt(pos) == '\'') {
            pos++;
            return true;
        }
        pos = mark;
        return false;
    }

    /** Lower-case the twenty-six letters and leave every other character alone. */
    static String foldAsciiOnly(String word) {
        StringBuilder sb = new StringBuilder(word.length());
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            sb.append(c >= 'A' && c <= 'Z' ? (char) (c + 32) : c);
        }
        return sb.toString();
    }
}
