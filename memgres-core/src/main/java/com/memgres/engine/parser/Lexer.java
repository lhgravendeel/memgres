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
            "ENUM", "ADD", "COLUMN", "RENAME", "OWNER", "GRANT", "REVOKE",
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
            "VALID", "VALIDATE", "HANDLER", "INLINE", "TRUSTED",
            "COMMENT", "SECURITY", "DEFINER", "INVOKER",
            "CALLED", "INPUT", "STRICT", "IMMUTABLE", "STABLE", "VOLATILE",
            "PARALLEL", "SAFE", "UNSAFE", "RESTRICTED",
            "COST", "SUPPORT", "LEAKPROOF",
            "WORK", "ABORT",
            "PROCEDURE",  // used in EXECUTE PROCEDURE
            "CALL", "SETOF", "INOUT", "VARIADIC", "SLICE", "ROUTINE",
            "ATOMIC",
            "ATTACH", "DETACH", "STATEMENT", "REFERENCING",
            "MERGE", "MATCHED", "RESTART",
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
            "SERVER", "MAPPING", "IMPORT",
            "PUBLICATION", "SUBSCRIPTION",
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

        // Unicode escape strings: U&'...'
        if ((c == 'U' || c == 'u') && pos + 1 < length && sql.charAt(pos + 1) == '&'
                && pos + 2 < length && sql.charAt(pos + 2) == '\'') {
            pos += 2; // skip U&
            return readUnicodeStringLiteral(start);
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
            case "#>": return new Token(TokenType.JSON_HASH_ARROW, "#>", start);
            case "#-": return new Token(TokenType.JSON_DELETE_PATH, "#-", start);
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
            case "!~~": return new Token(TokenType.NOT_DOUBLE_TILDE, "!~~", start);
            case "!~*": return new Token(TokenType.EXCL_TILDE_STAR, "!~*", start);
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
                        sb.append(c); pos++; 
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
        // Process Unicode escapes: \XXXX (4-digit) or \+NNNNNN (6-digit) → character
        String val = raw.value();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < val.length(); i++) {
            if (val.charAt(i) == '\\' && i + 1 < val.length()) {
                // \+NNNNNN form (6-digit codepoint)
                if (val.charAt(i + 1) == '+' && i + 7 < val.length()) {
                    String hex = val.substring(i + 2, i + 8);
                    // Validate all characters are hex digits
                    boolean valid = true;
                    for (char hc : hex.toCharArray()) {
                        if (Character.digit(hc, 16) < 0) { valid = false; break; }
                    }
                    if (!valid) {
                        throw new ParseException("invalid Unicode escape value",
                                new Token(TokenType.ERROR, val, start));
                    }
                    int cp = Integer.parseInt(hex, 16);
                    sb.appendCodePoint(cp);
                    i += 7; // skip \+NNNNNN
                    continue;
                }
                // \XXXX form (4-digit codepoint)
                if (i + 4 < val.length()) {
                    String hex = val.substring(i + 1, i + 5);
                    // Validate all characters are hex digits
                    boolean valid = true;
                    for (char hc : hex.toCharArray()) {
                        if (Character.digit(hc, 16) < 0) { valid = false; break; }
                    }
                    if (!valid) {
                        throw new ParseException("invalid Unicode escape value",
                                new Token(TokenType.ERROR, val, start));
                    }
                    int cp = Integer.parseInt(hex, 16);
                    sb.appendCodePoint(cp);
                    i += 4; // skip \XXXX
                    continue;
                }
                // Invalid: not enough characters for escape
                throw new ParseException("invalid Unicode escape value",
                        new Token(TokenType.ERROR, val, start));
            }
            sb.append(val.charAt(i));
        }
        return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
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

        // Check for bare $ ($ followed by whitespace/newline - PostgreSQL treats as $$)
        if (pos == tagStart + 1 && pos < length && Character.isWhitespace(sql.charAt(pos))) {
            // Bare $ delimiter
            StringBuilder body = new StringBuilder();
            while (pos < length) {
                // Look for closing bare $ ($ not followed by alphanumeric/underscore/$)
                if (sql.charAt(pos) == '$') {
                    int next = pos + 1;
                    if (next >= length || (!Character.isLetterOrDigit(sql.charAt(next)) && sql.charAt(next) != '_' && sql.charAt(next) != '$')) {
                        pos++; // skip closing $
                        return new Token(TokenType.DOLLAR_STRING_LITERAL, body.toString(), start);
                    }
                }
                body.append(sql.charAt(pos));
                pos++;
            }
            // No closing $ found; reset
            pos = tagStart;
            return null;
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
                    return new Token(TokenType.QUOTED_IDENTIFIER, sb.toString(), start);
                }
            } else {
                sb.append(c);
                pos++;
            }
        }
        return new Token(TokenType.ERROR, sb.toString(), start);
    }

    private Token readNumber(int start) {
        StringBuilder sb = new StringBuilder();
        boolean hasDecimal = false;

        while (pos < length) {
            char c = sql.charAt(pos);
            if (Character.isDigit(c)) {
                sb.append(c);
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
                // Scientific notation
                hasDecimal = true;
                sb.append(c);
                pos++;
                if (pos < length && (sql.charAt(pos) == '+' || sql.charAt(pos) == '-')) {
                    sb.append(sql.charAt(pos));
                    pos++;
                }
            } else {
                break;
            }
        }

        // PG rejects number immediately followed by letter (e.g., 123abc)
        if (pos < length && Character.isLetter(sql.charAt(pos)) && sql.charAt(pos) != 'e' && sql.charAt(pos) != 'E') {
            throw new ParseException("trailing junk after numeric literal at or near \"" + sb + sql.charAt(pos) + "\"",
                    new Token(TokenType.ERROR, sb.toString(), start));
        }
        return new Token(hasDecimal ? TokenType.FLOAT_LITERAL : TokenType.INTEGER_LITERAL,
                sb.toString(), start);
    }

    private Token readIdentifierOrKeyword(int start) {
        StringBuilder sb = new StringBuilder();
        while (pos < length) {
            char c = sql.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(c);
                pos++;
            } else {
                break;
            }
        }
        String word = sb.toString();
        String upper = word.toUpperCase();

        if (KEYWORDS.contains(upper)) {
            return new Token(TokenType.KEYWORD, upper, start);
        }
        return new Token(TokenType.IDENTIFIER, word.toLowerCase(), start);
    }
}
