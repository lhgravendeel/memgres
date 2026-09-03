package com.memgres.engine.parser;

import com.memgres.engine.BuiltinFunctionSignatures;
import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive-descent expression parser. Contains token navigation utilities and all expression
 * parsing methods. Parser extends this class and adds statement parsing.
 * Delegates special SQL syntax forms to ExprSpecialFormParser.
 */
public class ExpressionParser {

    protected final List<Token> tokens;
    protected int pos;
    private final ExprSpecialFormParser specialFormParser;

    /**
     * The schema every qualified type name in this statement was written under, in the order the
     * statement writes them.
     *
     * <p>A type name is the one place a schema qualifier is read and thrown away: {@code int4} is
     * what the engine goes on to work with whether the statement wrote {@code int4},
     * {@code pg_catalog.int4} or {@code nosuch.int4}. PostgreSQL resolves the qualifier first and
     * refuses the statement when it names no schema, so the qualifiers are kept here for whoever
     * runs the statement to resolve, rather than being widened into every type name the engine
     * carries around.
     */
    private final List<String> typeSchemaQualifiers = new ArrayList<>();

    protected ExpressionParser(List<Token> tokens) {
        this.tokens = tokens;
        this.pos = 0;
        this.specialFormParser = new ExprSpecialFormParser(this);
    }

    /** Every schema a type name in this statement was qualified with, leftmost first. */
    public List<String> typeSchemaQualifiers() {
        return typeSchemaQualifiers;
    }

    // ---- Token navigation ----

    protected Token peek() {
        return tokens.get(pos);
    }

    /** The token {@code offset} places ahead, or the last one when the input ends before that. */
    protected Token peekAt(int offset) {
        int index = Math.min(pos + offset, tokens.size() - 1);
        return tokens.get(index);
    }

    protected Token advance() {
        Token t = tokens.get(pos);
        pos++;
        return t;
    }

    protected boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    protected void expectEnd() {
        if (!isAtEnd()) {
            Token t = peek();
            throw new ParseException("Unexpected token after statement: " + t.value(), t);
        }
    }

    protected boolean check(TokenType type) {
        return peek().type() == type;
    }

    protected int position() {
        return pos;
    }

    /** True when the token just consumed is of {@code type}. False at the start of the input. */
    protected boolean previousTokenIs(TokenType type) {
        return pos > 0 && tokens.get(pos - 1).type() == type;
    }

    protected void resetPosition(int saved) {
        pos = saved;
    }

    protected boolean checkKeywordAt(int offset, String keyword) {
        int idx = pos + offset;
        if (idx >= tokens.size()) return false;
        Token t = tokens.get(idx);
        return t.type() == TokenType.KEYWORD && t.value().equalsIgnoreCase(keyword);
    }

    protected boolean checkKeyword(String keyword) {
        Token t = peek();
        return t.type() == TokenType.KEYWORD && t.value().equalsIgnoreCase(keyword);
    }

    /**
     * Match a word that is not reserved, whatever kind of token it arrived as.
     *
     * <p>An unreserved word may be spelled by a KEYWORD token or by an IDENTIFIER one depending on
     * whether the lexer happens to know it, and a grammar that only matched keywords refused
     * ASENSITIVE — which is an ordinary word PostgreSQL's cursor options accept.
     */
    protected boolean matchWord(String word) {
        Token t = peek();
        if ((t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER)
                && t.value().equalsIgnoreCase(word)) {
            advance();
            return true;
        }
        return false;
    }

    /** The same question asked of the token {@code offset} places ahead. */
    protected boolean checkWordAt(int offset, String word) {
        int idx = pos + offset;
        if (idx >= tokens.size()) return false;
        Token t = tokens.get(idx);
        return (t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER)
                && t.value().equalsIgnoreCase(word);
    }

    protected boolean checkWord(String word) {
        Token t = peek();
        return (t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER)
                && t.value().equalsIgnoreCase(word);
    }

    protected boolean matchKeyword(String keyword) {
        if (checkKeyword(keyword)) {
            advance();
            return true;
        }
        return false;
    }

    /** Check if current token is an interval field name (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND). */
    protected boolean checkIntervalField() {
        Token t = peek();
        if (t.type() == TokenType.EOF) return false;
        String v = t.value().toUpperCase(java.util.Locale.ROOT);
        return (t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER) &&
                ("YEAR".equals(v) || "MONTH".equals(v) || "DAY".equals(v) ||
                 "HOUR".equals(v) || "MINUTE".equals(v) || "SECOND".equals(v));
    }

    /** Check if current token is an identifier with the given value (case-insensitive). */
    protected boolean checkIdentCI(String val) {
        Token t = peek();
        return (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.KEYWORD) &&
                t.value().equalsIgnoreCase(val);
    }

    /**
     * Returns true if the current token is a SQL clause keyword (FROM, WHERE, GROUP, ORDER, etc.)
     * that should not be consumed as an identifier in contexts like bare COLLATE.
     */
    protected boolean isClauseKeyword() {
        return checkKeyword("FROM") || checkKeyword("WHERE") || checkKeyword("GROUP")
                || checkKeyword("ORDER") || checkKeyword("LIMIT") || checkKeyword("OFFSET")
                || checkKeyword("HAVING") || checkKeyword("UNION") || checkKeyword("INTERSECT")
                || checkKeyword("EXCEPT") || checkKeyword("FETCH") || checkKeyword("FOR")
                || checkKeyword("INTO") || checkKeyword("ON") || checkKeyword("RETURNING")
                || checkKeyword("WINDOW");
    }

    private static final java.util.Set<String> QUERY_START_KEYWORDS = Cols.setOf(
            "SELECT", "WITH", "VALUES", "TABLE", "INSERT", "UPDATE", "DELETE", "MERGE");

    /**
     * Scans ahead through consecutive LEFT_PAREN tokens to check if a query keyword
     * (SELECT, WITH, VALUES, etc.) follows. Does NOT consume any tokens.
     * Returns the number of extra LEFT_PAREN tokens before the keyword, or -1 if
     * no query keyword is found. A return of 0 means the current token is already
     * a query keyword (no extra parens).
     */
    protected int countLeadingParensBeforeQuery() {
        int look = pos;
        int count = 0;
        while (look < tokens.size() && tokens.get(look).type() == TokenType.LEFT_PAREN) {
            count++;
            look++;
        }
        if (count == 0) return -1;
        if (look < tokens.size()) {
            Token t = tokens.get(look);
            if (t.type() == TokenType.KEYWORD && QUERY_START_KEYWORDS.contains(t.value())) {
                return count;
            }
        }
        return -1;
    }

    /**
     * Consumes N LEFT_PAREN tokens. Used with countLeadingParensBeforeQuery().
     */
    protected int consumeLeadingParens(int count) {
        for (int i = 0; i < count; i++) {
            expect(TokenType.LEFT_PAREN);
        }
        return count;
    }

    /**
     * Consumes N RIGHT_PAREN tokens (the closing parens matching consumeLeadingParens).
     */
    protected void consumeTrailingParens(int count) {
        for (int i = 0; i < count; i++) {
            expect(TokenType.RIGHT_PAREN);
        }
    }

    protected boolean checkIdentifier(String name) {
        Token t = peek();
        return t.type() == TokenType.IDENTIFIER && t.value().equalsIgnoreCase(name);
    }

    protected boolean matchIdentifier(String name) {
        if (checkIdentifier(name)) {
            advance();
            return true;
        }
        return false;
    }

    protected boolean matchKeywords(String... keywords) {
        int saved = pos;
        for (String kw : keywords) {
            if (!matchKeyword(kw)) {
                pos = saved;
                return false;
            }
        }
        return true;
    }

    protected boolean match(TokenType type) {
        if (check(type)) {
            advance();
            return true;
        }
        return false;
    }

    protected Token expect(TokenType type) {
        if (check(type)) {
            return advance();
        }
        throw new ParseException("Expected " + type + " but found " + peek().type(), peek());
    }

    protected void expectKeyword(String keyword) {
        if (!matchKeyword(keyword)) {
            throw new ParseException("Expected keyword " + keyword, peek());
        }
    }

    protected void skipSemicolons() {
        while (check(TokenType.SEMICOLON)) advance();
    }

    /** Reject boolean connectives (AND, OR) appearing where an expression is expected. */
    private void checkNotBooleanConnective() {
        if (checkKeyword("AND") || checkKeyword("OR")) {
            throw new ParseException("syntax error at or near \"" + peek().value() + "\"", peek());
        }
    }

    /** The tokens after which a keyword can only have been a column name, not a construct. */
    private static final java.util.Set<String> FOLLOWS_A_COLUMN = java.util.Collections
            .unmodifiableSet(new java.util.HashSet<String>(java.util.Arrays.asList(
                    "FROM", "AS", "WHERE", "ORDER", "GROUP", "HAVING", "LIMIT", "OFFSET", "INTO",
                    "UNION", "INTERSECT", "EXCEPT", "ON", "AND", "OR", "WHEN", "THEN", "ELSE",
                    "END", "IS", "RETURNING", "USING", "FOR", "WINDOW", "FETCH")));

    /** Whether the token after this keyword rules out every construct the keyword could begin. */
    private boolean keywordStandsAlone() {
        if (pos + 1 >= tokens.size()) return true;
        Token next = tokens.get(pos + 1);
        switch (next.type()) {
            case COMMA: case RIGHT_PAREN: case SEMICOLON: case EOF:
            case EQUALS: case NOT_EQUALS: case LESS_THAN: case GREATER_THAN:
            case LESS_EQUALS: case GREATER_EQUALS:
                return true;
            case KEYWORD:
                return FOLLOWS_A_COLUMN.contains(next.value().toUpperCase(java.util.Locale.ROOT));
            default:
                return false;
        }
    }

    /**
     * A name that has to be usable as a column, a table alias or a parameter. PostgreSQL's
     * reserved and type/function-name keywords are not, which is why {@code CREATE TABLE t (order
     * int)} and {@code FROM t AS left} are syntax errors and were accepted here.
     */
    /** The type names PostgreSQL spells in two words, and the typname each one carries. */
    private static final String[][] TWO_WORD_TYPES = {
            {"DOUBLE", "PRECISION", "float8"},
            {"CHARACTER", "VARYING", "varchar"},
            {"CHAR", "VARYING", "varchar"},
            {"BIT", "VARYING", "varbit"},
            {"NATIONAL", "CHARACTER", "bpchar"},
    };

    /** A constant written as a two-word type name and a string, or null when this is not one. */
    private Expression parseTwoWordTypeLiteral() {
        if (pos + 2 >= tokens.size()) return null;
        if (tokens.get(pos + 2).type() != TokenType.STRING_LITERAL) return null;
        String first = tokens.get(pos).value();
        String second = tokens.get(pos + 1).value();
        if (first == null || second == null) return null;
        for (String[] type : TWO_WORD_TYPES) {
            if (type[0].equalsIgnoreCase(first) && type[1].equalsIgnoreCase(second)) {
                advance();
                advance();
                String text = advance().value();
                return new CastExpr(Literal.ofString(text), type[2]);
            }
        }
        return null;
    }

    /**
     * A name that has to be a plain identifier: not a string literal, and not a keyword
     * PostgreSQL reserves. A channel, a cursor and a prepared statement are all named this way,
     * and reading them with the general identifier reader accepted {@code LISTEN 'ch'} and
     * {@code DECLARE select CURSOR} alike.
     */
    protected String readObjectName() {
        Token t = peek();
        if (t.type() == TokenType.STRING_LITERAL || t.type() == TokenType.DOLLAR_STRING_LITERAL) {
            throw ParseException.saying("syntax error at or near \"'" + t.value() + "'\"", t, "42601");
        }
        return readColumnName();
    }

    /** Nothing may follow a statement but its semicolon. */
    protected void expectEndOfStatement() {
        if (isAtEnd()) return;
        Token t = peek();
        if (t.type() == TokenType.SEMICOLON || t.type() == TokenType.EOF) return;
        throw ParseException.saying("syntax error at or near \"" + tokenAsWritten(t) + "\"", t, "42601");
    }

    /** A token as the reader wrote it: a string constant keeps the quotes that made it one. */
    protected static String tokenAsWritten(Token token) {
        if (token.type() == TokenType.STRING_LITERAL) return "'" + token.value() + "'";
        return token.raw();
    }



    protected String readColumnName() {
        Token t = peek();
        if (t.type() == TokenType.KEYWORD && !PgKeywords.canBeColumnName(t.value())) {
            // PostgreSQL names the word as it was written, not folded: SAVEPOINT ALL stops at
            // "ALL" and SAVEPOINT select at "select".
            throw ParseException.saying(
                    "syntax error at or near \"" + t.raw() + "\"", t, "42601");
        }
        return readIdentifier();
    }

    protected String readIdentifier() {
        Token t = peek();
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER) {
            if (t.type() == TokenType.QUOTED_IDENTIFIER && t.value().isEmpty()) {
                throw new ParseException("zero-length delimited identifier", t);
            }
            advance();
            return t.value();
        }
        // Allow keywords as identifiers in many contexts
        if (t.type() == TokenType.KEYWORD) {
            advance();
            return t.value().toLowerCase(java.util.Locale.ROOT);
        }
        // Allow single-quoted strings as identifiers (PG accepts 'name' in SET ROLE, CREATE ROLE, etc.)
        if (t.type() == TokenType.STRING_LITERAL) {
            advance();
            return t.value();
        }
        throw new ParseException("Expected identifier", t);
    }

    protected String readIdentifierOrString() {
        Token t = peek();
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER) {
            advance();
            return t.value();
        }
        if (t.type() == TokenType.STRING_LITERAL) {
            advance();
            return t.value();
        }
        if (t.type() == TokenType.KEYWORD) {
            advance();
            return t.value().toLowerCase(java.util.Locale.ROOT);
        }
        throw new ParseException("Expected identifier or string", t);
    }

    // ---- Type name parsing ----

    protected String parseTypeName() {
        StringBuilder sb = new StringBuilder();
        boolean qualified = false;  // an interval field qualifier has already taken its precision
        boolean schemaWritten = false;
        // "char" is the one type PostgreSQL names with quotes, and the quotes are what tell it
        // apart from char, which is the blank-padded string type. Reading the name without them
        // made ::"char" a bpchar, so a type the catalogs are written in could not be written at all.
        boolean quotedName = peek().type() == TokenType.QUOTED_IDENTIFIER;
        String name = readIdentifier();
        if (quotedName && "char".equals(name)) name = "\"char\"";
        // A schema-qualified type is written schema.typename, and the qualifier is carried: two
        // schemas may each hold a type of the same name, so a column declared a.e has to record
        // which e it was declared with. pg_catalog is the exception — it holds the types this
        // engine ships and nothing a schema map knows about, so pg_catalog.int4 is just int4.
        if (check(TokenType.DOT)) {
            advance();
            String qualifier = name;
            name = readIdentifier();
            schemaWritten = true;
            // Recorded whole, because both halves are read: the schema has to be one that exists,
            // and the type has to be one that schema holds.
            typeSchemaQualifiers.add(qualifier + "." + name);
            if (!"pg_catalog".equalsIgnoreCase(qualifier)) {
                name = qualifier.toLowerCase(java.util.Locale.ROOT) + "." + name.toLowerCase(java.util.Locale.ROOT);
            }
        }
        sb.append(name);

        // A multi-word type is a spelling PostgreSQL's grammar rewrites, and the grammar reads one
        // only where no schema was written: after a qualifier it takes a single name and the next
        // word is unexpected, which is what "pg_catalog.character varying" reports.
        if (schemaWritten) {
            return finishTypeName(sb, false);
        }

        // Handle multi-word types: DOUBLE PRECISION, CHARACTER VARYING, TIMESTAMP WITH/WITHOUT TIME ZONE
        if (name.equalsIgnoreCase("DOUBLE") && checkKeyword("PRECISION")) {
            sb.append(" ").append(advance().value());
        } else if (name.equalsIgnoreCase("CHARACTER") && checkKeyword("VARYING")) {
            sb.append(" ").append(advance().value());
        } else if (name.equalsIgnoreCase("TIMESTAMP") || name.equalsIgnoreCase("TIME")) {
            // Handle optional precision: TIMESTAMP(6) or TIME(3)
            if (check(TokenType.LEFT_PAREN)) {
                advance();
                sb.append("(");
                // A fractional-seconds precision is a number in the grammar, so one too large
                // for an integer is not a number the grammar has -- and neither is a signed one.
                if (check(TokenType.MINUS)) throw ParseException.at(peek());
                sb.append(requireTypeModifierNumber(sb.toString()));
                expect(TokenType.RIGHT_PAREN);
                sb.append(")");
            }
            // Handle WITH/WITHOUT TIME ZONE after optional precision
            if (checkKeyword("WITH") || checkKeyword("WITHOUT")) {
                sb.append(" ").append(advance().value()); // WITH/WITHOUT
                if (matchKeyword("TIME")) sb.append(" TIME");
                if (matchKeyword("ZONE")) sb.append(" ZONE");
            }
        } else if (name.equalsIgnoreCase("INTERVAL")) {
            // Handle INTERVAL qualifiers: INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, etc.
            // Also INTERVAL YEAR, INTERVAL HOUR, etc. (single field)
            // The qualifier is part of the type's identity, so it is spelled one way regardless
            // of how it was written: "interval day to second", never "interval day TO second".
            if (checkIntervalField()) {
                parseIntervalQualifier(sb);
                qualified = true;
            }
        } else if (name.equalsIgnoreCase("BIT") && checkKeyword("VARYING")) {
            sb.append(" ").append(advance().value());
        }

        return finishTypeName(sb, qualified);
    }

    /**
     * The precision and the array suffix, which every type name may carry however it was written.
     *
     * @param qualified whether an interval field list has already taken this name's precision.
     */
    /**
     * The types PostgreSQL's grammar has a production of its own for, and which take no modifier.
     * That production has no place for a parenthesis, so the parenthesis itself is the word the
     * grammar stopped on: {@code int(5)} is a syntax error rather than a modifier it turned down.
     */
    private static final java.util.Set<String> UNMODIFIABLE_GRAMMAR_TYPES = Cols.setOf(
            "int", "integer", "bigint", "smallint", "boolean", "real", "double precision", "json");

    /** The written type names that carry a modifier. Every other type this engine knows has none. */
    private static final java.util.Set<String> MODIFIABLE_TYPES = Cols.setOf(
            "numeric", "decimal", "float", "varchar", "character varying", "char", "character",
            "bpchar", "bit", "bit varying", "varbit",
            "timestamp", "timestamptz", "time", "timetz", "interval");

    /** The clock functions that take how many digits of the second to keep. */
    private static final java.util.Set<String> PRECISION_TAKING_VALUE_FUNCTIONS =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "CURRENT_TIMESTAMP", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP"));

    /** The typed literals whose type carries a precision, written in front of the value. */
    private static final java.util.Set<String> PRECISION_TAKING_LITERALS =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "timestamp", "timestamptz", "time", "timetz", "interval"));

    /**
     * Refuse a modifier written after a type that has none. A type read as a plain name is looked
     * up and found to have no modifier input function, which PostgreSQL reports in words rather
     * than as a syntax error. Names this engine does not know are left alone: a domain or an enum
     * is the executor's to resolve, and only it can say whether the name means anything at all.
     */
    /**
     * The types whose modifier PostgreSQL's grammar reads as an expression rather than as a plain
     * number, so a sign may stand in it and the type's own bounds are what refuse it.
     */
    private static final java.util.Set<String> SIGNED_MODIFIER_TYPES =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "numeric", "decimal", "dec", "bit", "bit varying", "varbit"));

    private void rejectTypeModifier(String written) {
        // The one type whose name is written with its quotes is "char", and the quotes are how it
        // is told apart from char, so a name spelled that way is left to the executor.
        if (written.startsWith("\"")) return;
        String name = written.toLowerCase(java.util.Locale.ROOT);
        if (UNMODIFIABLE_GRAMMAR_TYPES.contains(name)) throw ParseException.at(peek());
        if (MODIFIABLE_TYPES.contains(name)) return;
        if (com.memgres.engine.DataType.fromPgName(name) == null) return;
        throw ParseException.saying("type modifier is not allowed for type \"" + name + "\"",
                peek(), "42601");
    }

    /**
     * One number of a type modifier, which has to be one a modifier can hold.
     *
     * <p>A modifier is a 32-bit number, so PostgreSQL refuses float(9999999999) where the grammar
     * reads it. Taken as written, it reached the code that turns a modifier into a width and
     * escaped from there as an internal error.
     */
    private String requireTypeModifierNumber(String typeSoFar) {
        Token t = peek();
        String text = t.value();
        // A modifier may hold a word rather than a number — geometry(Point, 4326) is a type
        // name PostgreSQL's grammar takes — so only a number is checked for fitting in one.
        boolean fits = true;
        if (t.type() == TokenType.INTEGER_LITERAL) {
            try {
                long v = Long.parseLong(text);
                fits = v <= Integer.MAX_VALUE && v >= Integer.MIN_VALUE;
            } catch (NumberFormatException tooLong) {
                fits = false;
            }
        }
        if (!fits) {
            // Some types read their modifier as an integer value — a bit string's width, a
            // numeric's precision — so one too large for an integer is out of range. The rest
            // take a number in the grammar itself, where one too large is not a number at all.
            // The buffer already carries the opening parenthesis, and may carry a minus sign.
            String type = typeSoFar.toLowerCase(java.util.Locale.ROOT).trim();
            int paren = type.indexOf('(');
            if (paren >= 0) type = type.substring(0, paren).trim();
            if (type.equals("bit") || type.equals("varbit") || type.equals("bit varying")
                    || type.equals("numeric") || type.equals("decimal")) {
                throw new com.memgres.engine.MemgresException(
                        "value \"" + text + "\" is out of range for type integer", "22003");
            }
            throw ParseException.at(t);
        }
        advance();
        return text;
    }

    private String finishTypeName(StringBuilder sb, boolean qualified) {
        // Handle precision: (N) or (N,M), for types not already handled above
        // PG allows negative scale in numeric(p,s), e.g. numeric(10,-2) rounds to hundreds
        if (qualified && check(TokenType.LEFT_PAREN)) {
            // Only SECOND carries a precision, and parseIntervalQualifier has already taken it,
            // so a parenthesis here is the one PostgreSQL points at.
            throw ParseException.at(peek());
        }
        if (check(TokenType.LEFT_PAREN)) {
            // A modifier belongs to the types that have one, and PostgreSQL settles that where the
            // type name is read rather than storing a width nothing would ever consult.
            rejectTypeModifier(sb.toString());
            advance();
            sb.append("(");
            // Some types read their modifier as a value and some as a number in the grammar, and
            // only the first kind has anywhere to put a sign: numeric(-1) is a precision out of
            // range, where varchar(-1) and interval(-1) are statements that will not parse.
            if (check(TokenType.MINUS)) {
                if (!SIGNED_MODIFIER_TYPES.contains(
                        sb.substring(0, sb.length() - 1).trim()
                                .toLowerCase(java.util.Locale.ROOT))) {
                    throw ParseException.at(peek());
                }
                advance();
                sb.append("-");
            }
            sb.append(requireTypeModifierNumber(sb.toString())); // first number
            if (match(TokenType.COMMA)) {
                sb.append(",");
                // Handle optional minus sign for negative scale
                if (check(TokenType.MINUS)) {
                    advance();
                    sb.append("-");
                }
                sb.append(advance().value()); // second number
            }
            expect(TokenType.RIGHT_PAREN);
            sb.append(")");
        }

        // The standard spells an array type with the word ARRAY, and lets one length follow it.
        // PostgreSQL reads that as the same type its own brackets name -- text ARRAY and
        // text ARRAY[4] are both text[] -- because an array column carries no length at all.
        if (checkKeyword("ARRAY")) {
            advance();
            if (check(TokenType.LEFT_BRACKET)) {
                advance();
                expect(TokenType.INTEGER_LITERAL);
                expect(TokenType.RIGHT_BRACKET);
            }
            sb.append("[]");
            return sb.toString();
        }

        // Handle array notation: [], [3], [][], etc. A dimension length is written where the
        // standard allows one but names no other type, so int[3] is the same int[] that int[] is.
        while (check(TokenType.LEFT_BRACKET)) {
            advance();
            match(TokenType.INTEGER_LITERAL);
            expect(TokenType.RIGHT_BRACKET);
            sb.append("[]");
        }

        return sb.toString();
    }

    /**
     * The field qualifier written after {@code interval}, appended to {@code sb} in the one
     * spelling information_schema reports.
     *
     * <p>SQL names the pairs a qualifier may span, and PostgreSQL's grammar names them one by
     * one: a range runs from the larger field to a smaller one, never the other way and never
     * across a gap, so {@code interval hour to year} and {@code interval second to day} are
     * syntax errors rather than types that do not exist. A precision belongs to SECOND alone,
     * which is why {@code interval year(2)} is a syntax error at the parenthesis.
     */
    private void parseIntervalQualifier(StringBuilder sb) {
        String first = advance().value().toLowerCase(java.util.Locale.ROOT);
        sb.append(" ").append(first);
        if (first.equals("second")) {
            appendIntervalPrecision(sb);
            return;
        }
        if (!checkKeyword("TO") && !checkIdentCI("TO")) return;
        Token to = peek();
        String[] allowed;
        if (first.equals("year")) allowed = new String[]{"month"};
        else if (first.equals("day")) allowed = new String[]{"hour", "minute", "second"};
        else if (first.equals("hour")) allowed = new String[]{"minute", "second"};
        else if (first.equals("minute")) allowed = new String[]{"second"};
        else allowed = new String[0];
        if (allowed.length == 0) throw ParseException.at(to);
        advance();  // consume TO
        Token endTok = peek();
        String end = checkIntervalField() ? endTok.value().toLowerCase(java.util.Locale.ROOT) : null;
        boolean ok = false;
        for (int i = 0; i < allowed.length; i++) {
            if (allowed[i].equals(end)) ok = true;
        }
        if (!ok) throw ParseException.at(endTok);
        advance();
        sb.append(" to ").append(end);
        if ("second".equals(end)) appendIntervalPrecision(sb);
    }

    /** The optional precision SECOND takes, appended to {@code sb} when it is written. */
    private void appendIntervalPrecision(StringBuilder sb) {
        if (!check(TokenType.LEFT_PAREN)) return;
        advance();
        sb.append("(").append(advance().value());
        expect(TokenType.RIGHT_PAREN);
        sb.append(")");
    }

    // ---- Order by parsing (used by both expression and statement parsing) ----

    protected List<SelectStmt.OrderByItem> parseOrderByList() {
        List<SelectStmt.OrderByItem> items = new ArrayList<>();
        do {
            Expression expr = parseExpression();
            boolean desc = false;
            Boolean nullsFirst = null;
            BinaryExpr.BinOp usingOp = null;

            if (matchKeyword("ASC")) { desc = false; }
            else if (matchKeyword("DESC")) { desc = true; }
            else if (matchKeyword("USING")) {
                // ORDER BY col USING <operator>: consume operator token(s)
                // The operator determines sort direction: < means ASC, > means DESC
                Token opTok = advance();
                if (opTok.type() == TokenType.GREATER_THAN) {
                    desc = true;
                }
                rejectNonOrderingOperator(opTok);
                // Carried so that the reader that knows what is being sorted can look the operator
                // up for that type: whether an operator exists at all is a question about the two
                // operands, which this parser has not got.
                usingOp = writtenOperator(opTok);
                // else default ASC for <, and other operators
            }

            if (matchKeyword("NULLS")) {
                if (matchKeyword("FIRST")) { nullsFirst = true; }
                else { expectKeyword("LAST"); nullsFirst = false; }
            }

            items.add(new SelectStmt.OrderByItem(expr, desc, nullsFirst, usingOp));
        } while (match(TokenType.COMMA));
        return items;
    }

    /**
     * The operator a symbol stands for, where the symbol is one this engine knows how to look up
     * for a pair of operand types; null for anything else, which leaves the sort unjudged rather
     * than refused.
     */
    private static BinaryExpr.BinOp writtenOperator(Token opTok) {
        switch (opTok.type()) {
            case PLUS: return BinaryExpr.BinOp.ADD;
            case MINUS: return BinaryExpr.BinOp.SUBTRACT;
            case STAR: return BinaryExpr.BinOp.MULTIPLY;
            case SLASH: return BinaryExpr.BinOp.DIVIDE;
            case PERCENT: return BinaryExpr.BinOp.MODULO;
            case CARET: return BinaryExpr.BinOp.POWER;
            case AMPERSAND: return BinaryExpr.BinOp.BIT_AND;
            case PIPE: return BinaryExpr.BinOp.BIT_OR;
            case HASH: return BinaryExpr.BinOp.BIT_XOR;
            case SHIFT_LEFT: return BinaryExpr.BinOp.SHIFT_LEFT;
            case SHIFT_RIGHT: return BinaryExpr.BinOp.SHIFT_RIGHT;
            case TS_MATCH: return BinaryExpr.BinOp.TS_MATCH;
            case CONTAINS: return BinaryExpr.BinOp.CONTAINS;
            case CONTAINED_BY: return BinaryExpr.BinOp.CONTAINED_BY;
            case OVERLAP: return BinaryExpr.BinOp.OVERLAP;
            case CONCAT: return BinaryExpr.BinOp.CONCAT;
            case TILDE: return BinaryExpr.BinOp.REGEX_MATCH;
            case TILDE_STAR: return BinaryExpr.BinOp.REGEX_IMATCH;
            case EXCL_TILDE: return BinaryExpr.BinOp.NOT_REGEX_MATCH;
            case EXCL_TILDE_STAR: return BinaryExpr.BinOp.NOT_REGEX_IMATCH;
            case DOUBLE_TILDE: return BinaryExpr.BinOp.LIKE;
            case DOUBLE_TILDE_STAR: return BinaryExpr.BinOp.ILIKE;
            default: return null;
        }
    }

    /**
     * ORDER BY ... USING takes an ordering operator, which PostgreSQL defines as a "&lt;" or "&gt;"
     * member of a btree operator family. Anything else was consumed and the sort ran ascending, so
     * ORDER BY a USING = quietly answered in whatever order the rows arrived.
     *
     * <p>Only the operators that are provably never ordering operators are refused here, which is
     * the four comparisons every btree type has and none of which orders: {@code =}, {@code <>},
     * {@code <=} and {@code >=}. An operator the type may not have at all -- {@code ~~} against an
     * integer -- is a different complaint (the operator does not exist) and telling the two apart
     * needs the sort column's type, which this parser does not have.
     */
    private void rejectNonOrderingOperator(Token opTok) {
        String name;
        switch (opTok.type()) {
            case EQUALS: name = "="; break;
            case NOT_EQUALS: name = opTok.value(); break;
            case LESS_EQUALS: name = "<="; break;
            case GREATER_EQUALS: name = ">="; break;
            default: return;
        }
        MemgresException e = new MemgresException(
                "operator " + name + " is not a valid ordering operator", "42809");
        e.setHint("Ordering operators must be \"<\" or \">\" members of btree operator families.");
        throw e;
    }

    protected List<SelectStmt.OrderByItem> parseOrderByClause() {
        expectKeyword("ORDER");
        expectKeyword("BY");
        return parseOrderByList();
    }

    // ---- Expression list and identifier list ----

    protected List<Expression> parseExpressionList() {
        List<Expression> list = new ArrayList<>();
        do {
            list.add(parseExpression());
        } while (match(TokenType.COMMA));
        return list;
    }

    /**
     * Parse function argument list, supporting named args (name => expr).
     */
    protected List<Expression> parseFunctionArgList() {
        List<Expression> list = new ArrayList<>();
        do {
            // Check for VARIADIC keyword
            if (checkKeyword("VARIADIC")) {
                advance(); // consume VARIADIC
                Expression varExpr = parseExpression();
                // VARIADIC ARRAY[...], expand at call site
                // Wrap as a NamedArgExpr with special name "__variadic__"
                list.add(new NamedArgExpr("__variadic__", varExpr));
                continue;
            }
            // Check for named arg: identifier => expr  or  identifier := expr
            int saved = position();
            if ((peek().type() == TokenType.IDENTIFIER || peek().type() == TokenType.KEYWORD)
                    && pos + 1 < tokens.size()
                    && (tokens.get(pos + 1).type() == TokenType.FAT_ARROW || tokens.get(pos + 1).type() == TokenType.COLON_EQUALS)) {
                // Reject DEFAULT as a named arg name; PG says "DEFAULT is not allowed in this context"
                if (peek().value().equalsIgnoreCase("DEFAULT")
                        && tokens.get(pos + 1).type() == TokenType.FAT_ARROW) {
                    throw new ParseException("DEFAULT is not allowed in this context", peek());
                }
                String argName = readIdentifier();
                advance(); // consume => or :=
                Expression value = parseExpression();
                list.add(new NamedArgExpr(argName.toLowerCase(java.util.Locale.ROOT), value));
            } else {
                resetPosition(saved);
                list.add(parseExpression());
            }
        } while (match(TokenType.COMMA));
        return list;
    }

    protected List<String> parseIdentifierList() {
        List<String> list = new ArrayList<>();
        do {
            list.add(readIdentifier());
        } while (match(TokenType.COMMA));
        return list;
    }

    /**
     * Parses a comma-separated column list where any entry may instead be a parenthesized
     * expression, e.g. {@code UNIQUE (id, (data->>'k'))} or an ON CONFLICT target list.
     * Plain entries are captured as bare identifier text; expression entries are captured as
     * their reconstructed source text (with exactly one layer of wrapping parens stripped).
     * Sets {@link #lastColumnListHadExpression} so callers can tell whether any entry was an
     * expression (vs. a plain identifier).
     */
    protected boolean lastColumnListHadExpression;

    /**
     * The parsed form of each expression entry of the last {@link #parseColumnOrExpressionList()},
     * so a caller that has to judge what was written — an aggregate in an ON CONFLICT index
     * expression, say — can read the tree instead of the reconstructed text.
     */
    protected List<Expression> lastColumnListExpressions;

    /** Whether the entry at this point is a bare column name and nothing more. */
    private boolean namesAColumnOnItsOwn() {
        int after = pos + 1;
        if (after >= tokens.size()) return true;
        TokenType next = tokens.get(after).type();
        if (next == TokenType.COMMA || next == TokenType.RIGHT_PAREN) return true;
        // Where the list is a list of index keys, what follows a key may be the options that key
        // was indexed under rather than more of an expression: the name of an operator class is a
        // bare word, and reading it as part of the key left the parenthesis unclosed.
        if (!columnListTakesIndexOptions) return false;
        if (next != TokenType.IDENTIFIER && next != TokenType.KEYWORD) return false;
        // A collation is written between the key and its class and belongs to the key itself, so
        // the key is read as the expression it is and this reads what may follow that.
        return !"COLLATE".equalsIgnoreCase(tokens.get(after).raw());
    }

    /**
     * Whether an entry of the next column list may carry what an index key carries. A conflict
     * target names the keys of an index, so an operator class may be written after one -- and a
     * direction may not, the arbiter being an index that has one already.
     */
    protected boolean columnListTakesIndexOptions;

    /** The operator class written after each entry of the last such list, null where none was. */
    protected List<String> lastColumnListOpclasses;

    /**
     * Read the options written after one key of a conflict target: an operator class, and nothing
     * else. Whether the class is one that exists, and whether it is the one the index was built
     * with, are questions for the reader that knows which relation is being written to.
     */
    private String readConflictTargetOptions() {
        if (checkKeyword("ASC") || checkKeyword("DESC")) {
            throw new MemgresException("ASC/DESC is not allowed in ON CONFLICT clause", "42P10");
        }
        if (checkKeyword("NULLS")) {
            throw new MemgresException(
                    "NULLS FIRST/LAST is not allowed in ON CONFLICT clause", "42P10");
        }
        if (check(TokenType.COMMA) || check(TokenType.RIGHT_PAREN) || isAtEnd()) return null;
        if (!check(TokenType.IDENTIFIER) && !check(TokenType.KEYWORD)) return null;
        String opclass = advance().value();
        while (check(TokenType.DOT)) {
            advance();
            opclass = opclass + "." + readIdentifier();
        }
        return opclass;
    }

    protected List<String> parseColumnOrExpressionList() {
        List<String> list = new ArrayList<>();
        List<Expression> parsed = new ArrayList<>();
        List<String> opclasses = new ArrayList<>();
        boolean anyExpr = false;
        do {
            if (check(TokenType.LEFT_PAREN)) {
                anyExpr = true;
                expect(TokenType.LEFT_PAREN);
                int exprStart = pos;
                parsed.add(parseExpression());
                StringBuilder sb = new StringBuilder();
                for (int i = exprStart; i < pos; i++) {
                    if (i > exprStart) {
                        Token prev = tokens.get(i - 1);
                        Token cur = tokens.get(i);
                        if (prev.type() != TokenType.LEFT_PAREN && cur.type() != TokenType.RIGHT_PAREN
                                && cur.type() != TokenType.COMMA && prev.type() != TokenType.COMMA) {
                            sb.append(' ');
                        }
                    }
                    sb.append(columnListTokenValue(tokens.get(i)));
                }
                list.add(sb.toString());
                expect(TokenType.RIGHT_PAREN);
            } else if (namesAColumnOnItsOwn()) {
                list.add(readIdentifier());
            } else {
                // An entry that is not a bare name is an expression, whether or not it was
                // written in its own parentheses: ON CONFLICT (lower(t)) names the expression an
                // index was built on, and reading only the name stopped at the parenthesis.
                anyExpr = true;
                int exprStart = pos;
                parsed.add(parseExpression());
                StringBuilder sb = new StringBuilder();
                for (int i = exprStart; i < pos; i++) {
                    if (i > exprStart) {
                        Token prev = tokens.get(i - 1);
                        Token cur = tokens.get(i);
                        if (prev.type() != TokenType.LEFT_PAREN && cur.type() != TokenType.RIGHT_PAREN
                                && cur.type() != TokenType.COMMA && prev.type() != TokenType.COMMA) {
                            sb.append(' ');
                        }
                    }
                    sb.append(columnListTokenValue(tokens.get(i)));
                }
                list.add(sb.toString());
            }
            opclasses.add(columnListTakesIndexOptions ? readConflictTargetOptions() : null);
        } while (match(TokenType.COMMA));
        lastColumnListHadExpression = anyExpr;
        lastColumnListExpressions = parsed;
        lastColumnListOpclasses = opclasses;
        return list;
    }

    /**
     * Renders a token's source text for reconstruction in {@link #parseColumnOrExpressionList()}.
     * String literal tokens store their unquoted content (see {@code Lexer}), so they must be
     * re-quoted here or the reconstructed expression text silently turns e.g. {@code 'k'} into
     * the bare identifier {@code k} — which then fails to parse/evaluate as a string constant.
     */
    private static String columnListTokenValue(Token t) {
        if (t.type() == TokenType.STRING_LITERAL) {
            return "'" + t.value().replace("'", "''") + "'";
        }
        return t.value();
    }

    // ---- Subquery hook (overridden by Parser) ----

    /**
     * Parse a SELECT statement. This is overridden by Parser to provide the real implementation.
     * ExpressionParser needs this for subqueries in expressions (e.g., EXISTS, IN, scalar subqueries).
     */
    protected SelectStmt parseSelect() {
        throw new ParseException("Subquery not supported in this context", peek());
    }

    /**
     * Parse a subquery that may include UNION/INTERSECT/EXCEPT.
     * Returns the parsed Statement (SelectStmt or SetOpStmt).
     * Override in Parser to provide set-operation support.
     */
    protected Statement parseSubqueryWithSetOps() {
        return parseSelect();
    }

    // ---- Expression parsing (Pratt parser / precedence climbing) ----

    public Expression parseExpression() {
        return parseOr();
    }

    private Expression parseOr() {
        Expression left = parseAnd();
        while (matchKeyword("OR")) {
            Expression right = parseAnd();
            left = new BinaryExpr(left, BinaryExpr.BinOp.OR, right);
        }
        return left;
    }

    private Expression parseAnd() {
        Expression left = parseNot();
        while (matchKeyword("AND")) {
            Expression right = parseNot();
            left = new BinaryExpr(left, BinaryExpr.BinOp.AND, right);
        }
        return left;
    }

    private Expression parseNot() {
        if (matchKeyword("NOT")) {
            return new UnaryExpr(UnaryExpr.UnaryOp.NOT, parseNot());
        }
        return parseComparison();
    }

    /**
     * A comparison, followed by the postfix null tests {@code ISNULL} and {@code NOTNULL}.
     *
     * <p>These are PostgreSQL's postfix spellings of IS NULL and IS NOT NULL, and they bind looser
     * than every comparison operator, so {@code n = 5 ISNULL} asks whether the comparison came out
     * NULL rather than whether 5 did. Only a bare identifier counts: a quoted {@code "isnull"} is a
     * name, and an identifier anywhere else is still free to be a column called isnull.
     */
    private Expression parseComparison() {
        Expression left = parseIsPostfix(parseComparisonOperand());
        if (checkIdentifier("ISNULL")) {
            advance();
            return new IsNullExpr(left, false);
        }
        if (checkIdentifier("NOTNULL")) {
            advance();
            return new IsNullExpr(left, true);
        }
        return left;
    }

    /**
     * The IS forms, which bind looser than a comparison operator: "1 = 1 IS NULL" asks whether the
     * comparison came out NULL, not whether the right-hand 1 did. Reading them as part of an
     * operand made every one of them a syntax error after a comparison.
     */
    private Expression parseIsPostfix(Expression left) {
        int saved = pos;
        if (checkKeyword("IS")) {
            advance();
            boolean negated = matchKeyword("NOT");
            if (matchKeyword("NULL")) {
                return new IsNullExpr(left, negated);
            }
            // IS [NOT] DISTINCT FROM: NULL-safe comparison
            if (matchKeywords("DISTINCT", "FROM")) {
                Expression right = parseOtherOps();
                return negated
                        ? new BinaryExpr(left, BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM, right)
                        : new BinaryExpr(left, BinaryExpr.BinOp.IS_DISTINCT_FROM, right);
            }
            // IS [NOT] TRUE / FALSE / UNKNOWN
            if (matchKeyword("TRUE")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_TRUE : IsBooleanExpr.BooleanTest.IS_TRUE);
            }
            if (matchKeyword("FALSE")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_FALSE : IsBooleanExpr.BooleanTest.IS_FALSE);
            }
            if (matchKeyword("UNKNOWN")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_UNKNOWN : IsBooleanExpr.BooleanTest.IS_UNKNOWN);
            }
            // IS [NOT] DOCUMENT: XML document test
            if (matchKeyword("DOCUMENT")) {
                return new IsBooleanExpr(left, negated ? IsBooleanExpr.BooleanTest.IS_NOT_DOCUMENT : IsBooleanExpr.BooleanTest.IS_DOCUMENT);
            }
            // IS [NOT] JSON [VALUE | OBJECT | ARRAY | SCALAR] [WITH UNIQUE KEYS]
            if (matchKeyword("JSON")) {
                IsJsonExpr.JsonType jt = null;
                if (matchKeyword("OBJECT")) jt = IsJsonExpr.JsonType.OBJECT;
                else if (matchKeyword("ARRAY")) jt = IsJsonExpr.JsonType.ARRAY;
                else if (matchKeyword("SCALAR")) jt = IsJsonExpr.JsonType.SCALAR;
                else if (matchKeyword("VALUE")) jt = IsJsonExpr.JsonType.VALUE;
                else if (checkKeyword("BOOLEAN")) throw new ParseException("syntax error at or near \"BOOLEAN\"", peek());
                else if (checkKeyword("NULL")) throw new ParseException("syntax error at or near \"NULL\"", peek());
                else if (checkIdentCI("STRING")) throw new ParseException("syntax error at or near \"STRING\"", peek());
                else if (checkIdentCI("NUMBER")) throw new ParseException("syntax error at or near \"NUMBER\"", peek());
                boolean uniqueKeys = false;
                if (matchKeywords("WITH", "UNIQUE")) {
                    expectKeyword("KEYS");
                    uniqueKeys = true;
                }
                return new IsJsonExpr(left, negated, jt, uniqueKeys);
            }
            // IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED
            String normForm = "NFC"; // default
            if (checkIdentCI("NFC") || checkIdentCI("NFD") || checkIdentCI("NFKC") || checkIdentCI("NFKD")) {
                normForm = advance().value().toUpperCase(java.util.Locale.ROOT);
            }
            if (checkIdentCI("NORMALIZED")) {
                advance(); // consume NORMALIZED
                // Encode as function call: __is_normalized__(expr, form, negated)
                return new FunctionCallExpr("__is_normalized__",
                        java.util.Arrays.asList(left, Literal.ofString(normForm), Literal.ofBoolean(!negated)));
            }
            // If we consumed a normForm token but didn't find NORMALIZED, that's an error
            // but in practice this won't happen with well-formed SQL
        }

        pos = saved;   // an IS this does not know is not ours to consume
        return left;
    }

    private Expression parseComparisonOperand() {
        Expression left = parsePatternTests(parseOtherOps());
        return parseComparisonTail(left);
    }

    /**
     * The IN, BETWEEN, LIKE, ILIKE and SIMILAR TO tests, which bind tighter than a comparison
     * operator: {@code 1 = 1 IN (1,2)} asks whether 1 equals the answer to {@code 1 IN (1,2)},
     * which is a boolean, and PostgreSQL refuses it for want of an integer = boolean operator.
     * Reading them at the same level as = read the comparison first and left the test dangling,
     * where the select list took the word IN for an output name and threw the test away.
     */
    private Expression parsePatternTests(Expression left) {
        // An IN test ends in a closing parenthesis, so another test may follow it and read its
        // answer; every other test ends in an expression, and a second test there would be
        // ambiguous, so PostgreSQL refuses it where the word is written.
        boolean mayChain = true;
        while (startsPatternTest()) {
            if (!mayChain) {
                throw ParseException.saying("syntax error at or near \"" + peek().value() + "\"",
                        peek(), "42601");
            }
            mayChain = startsInTest();
            left = parseOnePatternTest(left);
        }
        return left;
    }

    /** Whether an IN, BETWEEN, LIKE, ILIKE, SIMILAR TO or LIKE_REGEX test begins here. */
    private boolean startsPatternTest() {
        int at = checkKeyword("NOT") ? 1 : 0;
        if (checkKeywordAt(at, "IN") || checkKeywordAt(at, "BETWEEN") || checkKeywordAt(at, "LIKE")
                || checkKeywordAt(at, "ILIKE")) {
            return true;
        }
        if (checkKeywordAt(at, "SIMILAR") && checkKeywordAt(at + 1, "TO")) return true;
        return at == 0 && checkIdentCI("LIKE_REGEX");
    }

    /** Whether the test beginning here is an IN or a NOT IN. */
    private boolean startsInTest() {
        return checkKeywordAt(checkKeyword("NOT") ? 1 : 0, "IN");
    }

    private Expression parseOnePatternTest(Expression left) {
        boolean negated = false;
        if (checkKeyword("NOT")) {
            int saved = pos;
            advance();
            if (checkKeyword("IN") || checkKeyword("BETWEEN") || checkKeyword("LIKE") || checkKeyword("ILIKE") || checkKeyword("SIMILAR")) {
                negated = true;
            } else {
                pos = saved;
            }
        }

        if (matchKeyword("IN")) {
            expect(TokenType.LEFT_PAREN);
            // Check for subquery: IN (SELECT ... [UNION/INTERSECT/EXCEPT ...])
            if (checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                Statement subquery = parseSubqueryWithSetOps();
                expect(TokenType.RIGHT_PAREN);
                left = new InExpr(left, Cols.listOf(new SubqueryExpr(subquery)), negated);
            } else {
                List<Expression> values = parseExpressionList();
                expect(TokenType.RIGHT_PAREN);
                left = new InExpr(left, values, negated);
            }
            // Fall through to check for trailing comparison operator (e.g. "IN (1,2) = false")
        } else if (matchKeyword("BETWEEN")) {
            // [NOT] BETWEEN [SYMMETRIC] low AND high
            boolean symmetric = matchKeyword("SYMMETRIC");
            Expression low = parseOtherOps();
            expectKeyword("AND");
            Expression high = parseOtherOps();
            left = new BetweenExpr(left, low, high, negated, symmetric);
            // Fall through
        } else if (matchKeyword("LIKE")) {
            // [NOT] LIKE / ILIKE, either against one pattern or against ANY/ALL of a set
            if (checkKeyword("ANY") || checkKeyword("SOME") || checkKeyword("ALL")) {
                Expression any = parseComparisonRhs(left, BinaryExpr.BinOp.LIKE);
                return negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, any) : any;
            }
            Expression right = parseOtherOps();
            if (matchKeyword("ESCAPE")) {
                // The escape is an operand, so it is kept as one: written as NULL it is
                // nothing, and reading the token's text made an escape spelled "NULL".
                Expression esc = parseOtherOps();
                left = new LikeExpr(left, right, esc, false, negated);
            } else {
                Expression result = new BinaryExpr(left, BinaryExpr.BinOp.LIKE, right);
                left = negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
            }
            // Fall through
        } else if (matchKeyword("ILIKE")) {
            if (checkKeyword("ANY") || checkKeyword("SOME") || checkKeyword("ALL")) {
                Expression any = parseComparisonRhs(left, BinaryExpr.BinOp.ILIKE);
                return negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, any) : any;
            }
            Expression right = parseOtherOps();
            if (matchKeyword("ESCAPE")) {
                Expression esc = parseOtherOps();
                left = new LikeExpr(left, right, esc, true, negated);
            } else {
                Expression result = new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, right);
                left = negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
            }
            // Fall through
        } else if (matchKeywords("SIMILAR", "TO")) {
            Expression right = parseOtherOps();
            if (matchKeyword("ESCAPE")) {
                String esc = advance().value(); // string literal (may be empty)
                Expression result = new FunctionCallExpr("__similar_to_escape__",
                        java.util.Arrays.asList(left, right, Literal.ofString(esc)), false, false);
                left = negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
            } else {
                Expression result = new BinaryExpr(left, BinaryExpr.BinOp.SIMILAR_TO, right);
                left = negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
            }
            // Fall through
        } else if (matchIdentifier("LIKE_REGEX")) {
            // SQL:2008 LIKE_REGEX (equivalent to POSIX ~ operator)
            Expression right = parseOtherOps();
            Expression result = new BinaryExpr(left, BinaryExpr.BinOp.REGEX_MATCH, right);
            left = negated ? new UnaryExpr(UnaryExpr.UnaryOp.NOT, result) : result;
            // Fall through
        }
        return left;
    }

    private Expression parseComparisonTail(Expression left) {
        // Comparison operators
        if (match(TokenType.EQUALS)) {
            // Check for = ANY/SOME(...) or = ALL(...)
            if (checkKeyword("ANY") || checkKeyword("SOME") || checkKeyword("ALL")) {
                boolean isAll = checkKeyword("ALL");
                advance(); // consume ANY/SOME/ALL
                expect(TokenType.LEFT_PAREN);
                int extraParens = Math.max(0, countLeadingParensBeforeQuery());
                if (extraParens > 0 || checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                    consumeLeadingParens(extraParens);
                    Statement subquery = parseSubqueryWithSetOps();
                    consumeTrailingParens(extraParens);
                    expect(TokenType.RIGHT_PAREN);
                    // An ANY or ALL ends in a closing parenthesis, so a comparison may follow it
                    // and read its answer. Stopping here instead left "= true" for the select list
                    // to make of what it could, which lost the output name the query gave.
                    return parseComparisonTail(
                            new AnyAllExpr(left, BinaryExpr.BinOp.EQUAL, subquery, isAll));
                }
                Expression arrayExpr = parseExpression();
                expect(TokenType.RIGHT_PAREN);
                if (!isAll && arrayExpr instanceof ArrayExpr
                        && ((ArrayExpr) arrayExpr).elements().size() != 1) {
                    // Marked as the ANY spelling even though the elements are written out, so that
                    // it is not mistaken for a written IN afterwards: the two are the same
                    // comparison but not the same construct, and a one-element IN forbids things
                    // a one-element ANY allows. An array of one element is kept whole, because
                    // written out it would be indistinguishable from a value that is not an array
                    // at all -- and what ANY takes is an array.
                    ArrayExpr arr = (ArrayExpr) arrayExpr;
                    return parseComparisonTail(new InExpr(left, arr.elements(), false, true));
                }
                // ANY/ALL with array expression: evaluate element by element
                if (!isAll) {
                    // = ANY(array_expr) → treated as IN with the array elements
                    return parseComparisonTail(
                            new InExpr(left, Cols.listOf(arrayExpr), false, true));
                }
                // = ALL(array_expr) → all elements must satisfy the comparison
                return parseComparisonTail(
                        new AnyAllArrayExpr(left, BinaryExpr.BinOp.EQUAL, arrayExpr, true));
            }
            checkNotBooleanConnective(); // val = AND → syntax error
            return nonAssociative(new BinaryExpr(left, BinaryExpr.BinOp.EQUAL,
                    parsePatternTests(parseOtherOps())));
        }
        if (match(TokenType.NOT_EQUALS)) return nonAssociative(parseComparisonRhs(left, BinaryExpr.BinOp.NOT_EQUAL, true));
        if (match(TokenType.LESS_THAN)) return nonAssociative(parseComparisonRhs(left, BinaryExpr.BinOp.LESS_THAN, true));
        if (match(TokenType.GREATER_THAN)) return nonAssociative(parseComparisonRhs(left, BinaryExpr.BinOp.GREATER_THAN, true));
        if (match(TokenType.LESS_EQUALS)) return nonAssociative(parseComparisonRhs(left, BinaryExpr.BinOp.LESS_EQUAL, true));
        if (match(TokenType.GREATER_EQUALS)) return nonAssociative(parseComparisonRhs(left, BinaryExpr.BinOp.GREATER_EQUAL, true));

        // Array/JSON operators
        if (match(TokenType.CONTAINS)) return new BinaryExpr(left, BinaryExpr.BinOp.CONTAINS, parseOtherOps());
        if (match(TokenType.CONTAINED_BY)) return new BinaryExpr(left, BinaryExpr.BinOp.CONTAINED_BY, parseOtherOps());
        if (match(TokenType.OVERLAP)) return new BinaryExpr(left, BinaryExpr.BinOp.OVERLAP, parseOtherOps());
        if (match(TokenType.TS_MATCH)) return new BinaryExpr(left, BinaryExpr.BinOp.TS_MATCH, parseOtherOps());
        if (match(TokenType.JSONB_PATH_EXISTS_OP)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_PATH_EXISTS_OP, parseOtherOps());

        // JSONB key existence operators
        if (match(TokenType.JSONB_EXISTS)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_EXISTS, parseOtherOps());
        if (match(TokenType.JSONB_EXISTS_ANY)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_EXISTS_ANY, parseOtherOps());
        if (match(TokenType.JSONB_EXISTS_ALL)) return new BinaryExpr(left, BinaryExpr.BinOp.JSONB_EXISTS_ALL, parseOtherOps());

        // Operator forms of LIKE/ILIKE and NOT LIKE/NOT ILIKE
        if (match(TokenType.DOUBLE_TILDE)) return parseComparisonRhs(left, BinaryExpr.BinOp.LIKE);
        if (match(TokenType.DOUBLE_TILDE_STAR)) return parseComparisonRhs(left, BinaryExpr.BinOp.ILIKE);
        if (match(TokenType.NOT_DOUBLE_TILDE)) return new UnaryExpr(UnaryExpr.UnaryOp.NOT, new BinaryExpr(left, BinaryExpr.BinOp.LIKE, parseOtherOps()));
        if (match(TokenType.NOT_DOUBLE_TILDE_STAR)) return new UnaryExpr(UnaryExpr.UnaryOp.NOT, new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, parseOtherOps()));
        // An operator the reader declared, written in front of ANY or ALL: the set is read the
        // way it is read for a built-in operator, and each member compared with this one. Left
        // to the level below, the word ANY was read as a call and the subquery in it as its
        // arguments, which is a syntax error where PostgreSQL has a comparison.
        if (check(TokenType.CUSTOM_OPERATOR)
                && (checkKeywordAt(1, "ANY") || checkKeywordAt(1, "SOME")
                    || checkKeywordAt(1, "ALL"))) {
            return parseUserOperatorRhs(left, advance().value());
        }
        // POSIX regex operators
        if (match(TokenType.TILDE)) return parseComparisonRhs(left, BinaryExpr.BinOp.REGEX_MATCH);
        if (match(TokenType.TILDE_STAR)) return parseComparisonRhs(left, BinaryExpr.BinOp.REGEX_IMATCH);
        if (match(TokenType.EXCL_TILDE)) return parseComparisonRhs(left, BinaryExpr.BinOp.NOT_REGEX_MATCH);
        if (match(TokenType.EXCL_TILDE_STAR)) return parseComparisonRhs(left, BinaryExpr.BinOp.NOT_REGEX_IMATCH);

        // Geometric operators (DISTANCE is handled in parseOtherOps for correct precedence)
        if (match(TokenType.APPROX_EQUAL)) return new BinaryExpr(left, BinaryExpr.BinOp.APPROX_EQUAL, parseOtherOps());
        if (match(TokenType.GEO_BELOW)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_BELOW, parseOtherOps());
        if (match(TokenType.GEO_BELOW_EQ)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_BELOW_EQ, parseOtherOps());
        if (match(TokenType.GEO_ABOVE_EQ)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_ABOVE_EQ, parseOtherOps());
        if (match(TokenType.GEO_ABOVE)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_ABOVE, parseOtherOps());
        if (match(TokenType.GEO_NOT_EXTEND_RIGHT)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_RIGHT, parseOtherOps());
        if (match(TokenType.GEO_NOT_EXTEND_LEFT)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_LEFT, parseOtherOps());
        if (match(TokenType.GEO_NOT_EXTEND_ABOVE)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_ABOVE, parseOtherOps());
        if (match(TokenType.GEO_NOT_EXTEND_BELOW)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_NOT_EXTEND_BELOW, parseOtherOps());
        if (match(TokenType.GEO_INTERSECTS)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_INTERSECTS, parseOtherOps());
        if (match(TokenType.GEO_CLOSEST_POINT)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_CLOSEST_POINT, parseOtherOps());
        if (match(TokenType.GEO_PARALLEL)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_PARALLEL, parseOtherOps());
        if (match(TokenType.GEO_PERPENDICULAR)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_PERPENDICULAR, parseOtherOps());
        // Binary point alignment: point ?- point (horizontally aligned) -> boolean.
        // (?| shares its token with JSONB_EXISTS_ANY and is disambiguated at eval time.)
        if (match(TokenType.GEO_IS_HORIZONTAL)) return new BinaryExpr(left, BinaryExpr.BinOp.GEO_HORIZONTAL, parseOtherOps());

        // Range adjacency operator: -|-
        if (match(TokenType.RANGE_ADJACENT)) return new BinaryExpr(left, BinaryExpr.BinOp.RANGE_ADJACENT, parseOtherOps());

        // OPERATOR(schema.op) infix syntax: expr OPERATOR(pg_catalog.+) expr
        if (checkKeyword("OPERATOR")) {
            return specialFormParser.parseQualifiedOperator(left);
        }

        // SQL OVERLAPS syntax: (start, end) OVERLAPS (start, end) -> boolean
        if (checkIdentifier("overlaps")) {
            advance(); // consume OVERLAPS
            Expression right = parseOtherOps();
            return new FunctionCallExpr("overlaps", java.util.Arrays.asList(left, right), false, false);
        }

        return left;
    }

    /**
     * After consuming a comparison operator, check if the next token is ANY or ALL
     * (for subquery comparisons), otherwise parse a regular binary expression.
     */
    /** The comparison operators, which cannot be chained. */
    private static final java.util.Set<TokenType> COMPARISON_TOKENS = java.util.Collections
            .unmodifiableSet(new java.util.HashSet<TokenType>(java.util.Arrays.asList(
                    TokenType.EQUALS, TokenType.NOT_EQUALS, TokenType.LESS_THAN,
                    TokenType.GREATER_THAN, TokenType.LESS_EQUALS, TokenType.GREATER_EQUALS)));

    /**
     * PostgreSQL's comparison operators are non-associative, so {@code 1 < 0 < 5} is a syntax
     * error rather than a comparison of a boolean with a number. Chaining them read as a
     * left-associative pair and answered f, which is what the same text means in Python and in
     * nothing else: a query written that way is a mistake and has to be reported as one.
     */
    private Expression nonAssociative(Expression comparison) {
        if (!isAtEnd() && COMPARISON_TOKENS.contains(peek().type())) {
            throw ParseException.saying("syntax error at or near \"" + peek().value() + "\"",
                    peek(), "42601");
        }
        return comparison;
    }

    /** The right-hand side of an operator the reader declared, standing in front of ANY or ALL. */
    private Expression parseUserOperatorRhs(Expression left, String opSymbol) {
        boolean isAll = checkKeyword("ALL");
        advance(); // consume ANY/SOME/ALL
        expect(TokenType.LEFT_PAREN);
        int extraParens = Math.max(0, countLeadingParensBeforeQuery());
        if (extraParens > 0 || checkKeyword("SELECT") || checkKeyword("WITH")
                || checkKeyword("VALUES")) {
            consumeLeadingParens(extraParens);
            Statement subquery = parseSubqueryWithSetOps();
            consumeTrailingParens(extraParens);
            expect(TokenType.RIGHT_PAREN);
            return new AnyAllExpr(left, BinaryExpr.BinOp.EQUAL, subquery, isAll, opSymbol);
        }
        Expression arrayExpr = parseExpression();
        expect(TokenType.RIGHT_PAREN);
        return new AnyAllArrayExpr(left, BinaryExpr.BinOp.EQUAL, arrayExpr, isAll, opSymbol);
    }

    private Expression parseComparisonRhs(Expression left, BinaryExpr.BinOp op) {
        return parseComparisonRhs(left, op, false);
    }

    /**
     * @param patternTests whether the right-hand side may itself be an IN, BETWEEN or LIKE test.
     *     It may after a comparison operator, which binds looser than those; it may not after one
     *     of the operator-level spellings such as ~ or ~~, which bind tighter.
     */
    private Expression parseComparisonRhs(Expression left, BinaryExpr.BinOp op, boolean patternTests) {
        if (checkKeyword("ANY") || checkKeyword("SOME") || checkKeyword("ALL")) {
            boolean isAll = checkKeyword("ALL");
            advance(); // consume ANY/SOME/ALL
            expect(TokenType.LEFT_PAREN);
            int extraParens = Math.max(0, countLeadingParensBeforeQuery());
            if (extraParens > 0 || checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                consumeLeadingParens(extraParens);
                Statement subquery = parseSubqueryWithSetOps();
                consumeTrailingParens(extraParens);
                expect(TokenType.RIGHT_PAREN);
                return new AnyAllExpr(left, op, subquery, isAll);
            }
            // array expression fallback
            Expression arrayExpr = parseExpression();
            expect(TokenType.RIGHT_PAREN);
            return new AnyAllArrayExpr(left, op, arrayExpr, isAll);
        }
        Expression right = parseOtherOps();
        return new BinaryExpr(left, op, patternTests ? parsePatternTests(right) : right);
    }

    /**
     * PG "other operators" level: ||, |, #, &, <<, >>, <->, custom operators.
     * All sit BELOW +/- and ABOVE comparison in PG's precedence table.
     * They are all left-associative at the same flat precedence level.
     * Package-private so ExprSpecialFormParser can call it for qualified operator RHS.
     */
    Expression parseOtherOps() {
        Expression left = parseOtherOpsOperand();
        while (true) {
            if (match(TokenType.CONCAT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.CONCAT, parseOtherOpsOperand());
            } else if (match(TokenType.PIPE)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.BIT_OR, parseOtherOpsOperand());
            } else if (match(TokenType.HASH)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.BIT_XOR, parseOtherOpsOperand());
            } else if (match(TokenType.AMPERSAND)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.BIT_AND, parseOtherOpsOperand());
            } else if (match(TokenType.SHIFT_LEFT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.SHIFT_LEFT, parseOtherOpsOperand());
            } else if (match(TokenType.SHIFT_RIGHT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.SHIFT_RIGHT, parseOtherOpsOperand());
            } else if (match(TokenType.INET_CONTAINED_BY_EQUALS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.INET_CONTAINED_BY_EQUALS, parseOtherOpsOperand());
            } else if (match(TokenType.INET_CONTAINS_EQUALS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.INET_CONTAINS_EQUALS, parseOtherOpsOperand());
            } else if (match(TokenType.DISTANCE)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.DISTANCE, parseOtherOpsOperand());
            } else if (aTildeOperatorFollows()) {
                TokenType written = advance().type();
                left = tildeComparison(left, written, parseOtherOpsOperand());
            } else if (check(TokenType.CUSTOM_OPERATOR)
                    && !checkKeywordAt(1, "ANY") && !checkKeywordAt(1, "SOME")
                    && !checkKeywordAt(1, "ALL")) {
                String opSymbol = advance().value();
                left = new CustomOperatorExpr(null, opSymbol, left, parseOtherOpsOperand());
            } else {
                break;
            }
        }
        return left;
    }


    /**
     * Whether one of the operators written with a tilde stands here, at the level PostgreSQL puts
     * it: {@code ~} and {@code ~~} are operators like any other, not comparisons, so {@code x ~ 'a'
     * || 'b'} matches x against a and puts a b after the answer. Read at the comparison level they
     * took the whole concatenation as their pattern, and a chain of two of them was a syntax error
     * where PostgreSQL parses one and then complains about the types.
     *
     * <p>An ANY or ALL after the operator is not an operand of it: that spelling is read where the
     * rest of the ANY forms are read, and it is left alone here.
     */
    private boolean aTildeOperatorFollows() {
        if (!check(TokenType.TILDE) && !check(TokenType.TILDE_STAR)
                && !check(TokenType.EXCL_TILDE) && !check(TokenType.EXCL_TILDE_STAR)
                && !check(TokenType.DOUBLE_TILDE) && !check(TokenType.DOUBLE_TILDE_STAR)
                && !check(TokenType.NOT_DOUBLE_TILDE)
                && !check(TokenType.NOT_DOUBLE_TILDE_STAR)) {
            return false;
        }
        return !checkKeywordAt(1, "ANY") && !checkKeywordAt(1, "SOME")
                && !checkKeywordAt(1, "ALL");
    }

    /** The test one of the tilde operators makes. */
    private static Expression tildeComparison(Expression left, TokenType written,
                                              Expression right) {
        switch (written) {
            case TILDE: return new BinaryExpr(left, BinaryExpr.BinOp.REGEX_MATCH, right);
            case TILDE_STAR: return new BinaryExpr(left, BinaryExpr.BinOp.REGEX_IMATCH, right);
            case EXCL_TILDE: return new BinaryExpr(left, BinaryExpr.BinOp.NOT_REGEX_MATCH, right);
            case EXCL_TILDE_STAR:
                return new BinaryExpr(left, BinaryExpr.BinOp.NOT_REGEX_IMATCH, right);
            case DOUBLE_TILDE: return new BinaryExpr(left, BinaryExpr.BinOp.LIKE, right);
            case DOUBLE_TILDE_STAR: return new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, right);
            case NOT_DOUBLE_TILDE: return new UnaryExpr(UnaryExpr.UnaryOp.NOT,
                    new BinaryExpr(left, BinaryExpr.BinOp.LIKE, right));
            default: return new UnaryExpr(UnaryExpr.UnaryOp.NOT,
                    new BinaryExpr(left, BinaryExpr.BinOp.ILIKE, right));
        }
    }

    /**
     * An operand of the "other operators" level, prefix forms included. PostgreSQL puts every
     * symbolic prefix operator at this precedence — below {@code + -} and {@code * /} — so
     * {@code ~ 2 + 1} complements the sum and answers -4, and {@code @ -3 + 1} is the absolute
     * value of -2. Reading them with the unary sign bound them one level too tightly.
     */
    private Expression parseOtherOpsOperand() {
        if (match(TokenType.AT_SIGN)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.ABS, parseOtherOpsOperand());
        }
        if (match(TokenType.TILDE)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.BIT_NOT, parseOtherOpsOperand());
        }
        if (check(TokenType.CONCAT) && pos + 1 < tokens.size()
                && tokens.get(pos + 1).type() == TokenType.SLASH) {
            advance(); advance();
            return new UnaryExpr(UnaryExpr.UnaryOp.CBRT, parseOtherOpsOperand());
        }
        if (check(TokenType.PIPE) && pos + 1 < tokens.size()
                && tokens.get(pos + 1).type() == TokenType.SLASH) {
            advance(); advance();
            return new UnaryExpr(UnaryExpr.UnaryOp.SQRT, parseOtherOpsOperand());
        }
        return parseAddition();
    }

    private Expression parseAddition() {
        Expression left = parseMultiplication();
        while (true) {
            if (match(TokenType.PLUS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.ADD, parseMultiplication());
            } else if (match(TokenType.MINUS)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.SUBTRACT, parseMultiplication());
            } else {
                break;
            }
        }
        return left;
    }

    private Expression parseMultiplication() {
        Expression left = parsePower();
        while (true) {
            if (match(TokenType.STAR)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.MULTIPLY, parsePower());
            } else if (match(TokenType.SLASH)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.DIVIDE, parsePower());
            } else if (match(TokenType.PERCENT)) {
                left = new BinaryExpr(left, BinaryExpr.BinOp.MODULO, parsePower());
            } else {
                break;
            }
        }
        return left;
    }

    private Expression parsePower() {
        Expression left = parseUnary();
        // Left-associative (PG): 2^3^2 = (2^3)^2 = 64
        while (match(TokenType.CARET)) {
            left = new BinaryExpr(left, BinaryExpr.BinOp.POWER, parseUnary());
        }
        return left;
    }

    private Expression parseUnary() {
        if (match(TokenType.AT_SIGN)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.ABS, parseUnary());
        }
        if (match(TokenType.MINUS)) {
            Expression operand = parseUnary();
            // PostgreSQL's grammar folds a sign into the numeric literal behind it, so the whole
            // of -2147483648 is one integer literal. Negating the literal afterwards instead read
            // 2147483648 first, which is a bigint, and the expression was one width too wide:
            // (-2147483648) - 1 answered -2147483649 where PostgreSQL raises integer out of range.
            if (operand instanceof Literal) {
                Literal lit = (Literal) operand;
                if (lit.literalType() == Literal.LiteralType.INTEGER
                        || lit.literalType() == Literal.LiteralType.FLOAT) {
                    String text = lit.value();
                    if (text != null && !text.isEmpty()) {
                        // The sign is flipped either way, as PostgreSQL's doNegate does, so
                        // -(-2147483648) folds to the bigint 2147483648 rather than overflowing
                        // an integer negation.
                        String negated = text.startsWith("-") ? text.substring(1) : "-" + text;
                        return lit.literalType() == Literal.LiteralType.INTEGER
                                ? Literal.ofInt(negated) : Literal.ofFloat(negated);
                    }
                }
            }
            return new UnaryExpr(UnaryExpr.UnaryOp.NEGATE, operand);
        }
        if (match(TokenType.PLUS)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.POSITIVE, parseUnary());
        }
        if (match(TokenType.TILDE)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.BIT_NOT, parseUnary());
        }
        // ||/ (cube root) prefix operator; may be lexed as CONCAT+SLASH or CUSTOM_OPERATOR
        if (check(TokenType.CONCAT) && pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.SLASH) {
            advance(); advance(); // consume || /
            return new UnaryExpr(UnaryExpr.UnaryOp.CBRT, parseUnary());
        }
        // |/ (square root) prefix operator; may be lexed as PIPE+SLASH or CUSTOM_OPERATOR
        if (check(TokenType.PIPE) && pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.SLASH) {
            advance(); advance(); // consume | /
            return new UnaryExpr(UnaryExpr.UnaryOp.SQRT, parseUnary());
        }
        // ?- (geometric is-horizontal prefix operator)
        if (match(TokenType.GEO_IS_HORIZONTAL)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.GEO_IS_HORIZONTAL, parseUnary());
        }
        // @@ (geometric center prefix operator) — shares token with binary TS_MATCH,
        // but a leading @@ can only be the unary center-of-object operator. @@@ shares the
        // token too and has no prefix form at all, so it is refused rather than read as @@.
        if (check(TokenType.TS_MATCH)) {
            if ("@@@".equals(peek().value())) {
                throw new ParseException("operator does not exist: @@@", peek(), "42883");
            }
            advance();
            return new UnaryExpr(UnaryExpr.UnaryOp.GEO_CENTER, parseUnary());
        }
        // # (geometric npoints prefix operator) — shares token with binary HASH.
        if (match(TokenType.HASH)) {
            return new UnaryExpr(UnaryExpr.UnaryOp.GEO_NPOINTS, parseUnary());
        }
        // ?| (geometric is-vertical prefix operator) — shares token with JSONB_EXISTS_ANY
        if (check(TokenType.JSONB_EXISTS_ANY)) {
            // In prefix position (no left operand), treat as geometric is-vertical
            advance();
            return new UnaryExpr(UnaryExpr.UnaryOp.GEO_IS_VERTICAL, parseUnary());
        }
        // !~ as unary prefix operator (user-defined): !~ expr
        // EXCL_TILDE is normally a binary NOT-regex-match operator, but when used
        // in prefix position it must be treated as a custom unary operator.
        if (check(TokenType.EXCL_TILDE)) {
            advance();
            Expression right = parseUnary();
            return new CustomOperatorExpr(null, "!~", null, right);
        }
        // Custom multi-char prefix operator (user-defined): ~~> expr
        // Must check after ||/ and |/ to avoid intercepting built-in prefix operators
        if (check(TokenType.CUSTOM_OPERATOR)) {
            String opSymbol = peek().value();
            if (opSymbol.equals("||/")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.CBRT, parseUnary());
            }
            if (opSymbol.equals("|/")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.SQRT, parseUnary());
            }
            // @-@ (geometric length/perimeter) prefix operator
            if (opSymbol.equals("@-@")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.GEO_LENGTH, parseUnary());
            }
            // %% (hstore to array) and %# (hstore to matrix) prefix operators
            if (opSymbol.equals("%%")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.HSTORE_TO_ARRAY, parseUnary());
            }
            if (opSymbol.equals("%#")) {
                advance();
                return new UnaryExpr(UnaryExpr.UnaryOp.HSTORE_TO_MATRIX, parseUnary());
            }
            advance();
            Expression right = parseUnary();
            return new CustomOperatorExpr(null, opSymbol, null, right);
        }
        return parsePostfix();
    }

    /**
     * Parse a primary expression and eagerly absorb any immediately-following
     * {@code ::type} casts. Used for the right-hand operand of JSON postfix
     * operators ({@code ->}, {@code ->>}, {@code #>}, {@code #>>}, {@code #-})
     * so that {@code a #- '{b,c}'::text[]} parses as {@code a #- ('{b,c}'::text[])}
     * rather than {@code (a #- '{b,c}')::text[]}. PG's type-cast operator binds
     * tighter than any binary operator.
     */
    private Expression parsePrimaryWithCasts() {
        // A JSON subscript may be negative — jsonb -> -1 is the last element — so the sign binds
        // to the subscript here rather than being left for a binary minus that has no left side.
        boolean negated = false;
        if (check(TokenType.MINUS)) {
            advance();
            negated = true;
        }
        Expression expr = parsePrimary();
        if (negated) expr = new UnaryExpr(UnaryExpr.UnaryOp.NEGATE, expr);
        while (match(TokenType.CAST)) {
            String typeName = parseTypeName();
            expr = new CastExpr(expr, typeName);
        }
        return expr;
    }

    Expression parsePostfix() {
        Expression expr = parsePrimary();

        // Handle postfix operators: ::cast, JSON arrows, array subscript
        while (true) {
            if (match(TokenType.CAST)) {
                String typeName = parseTypeName();
                expr = new CastExpr(expr, typeName);
            } else if (match(TokenType.JSON_ARROW)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_ARROW, parsePrimaryWithCasts());
            } else if (match(TokenType.JSON_ARROW_TEXT)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_ARROW_TEXT, parsePrimaryWithCasts());
            } else if (match(TokenType.JSON_HASH_ARROW)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_HASH_ARROW, parsePrimaryWithCasts());
            } else if (match(TokenType.JSON_HASH_ARROW_TEXT)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_HASH_ARROW_TEXT, parsePrimaryWithCasts());
            } else if (match(TokenType.JSON_DELETE_PATH)) {
                expr = new BinaryExpr(expr, BinaryExpr.BinOp.JSON_DELETE_PATH, parsePrimaryWithCasts());
            } else if (check(TokenType.LEFT_BRACKET)) {
                // PG rejects ARRAY[...][n] (bare array literal subscript); requires (ARRAY[...])[n]
                if (expr instanceof ArrayExpr && !((ArrayExpr) expr).isRow()
                        && pos >= 1 && tokens.get(pos - 1).type() == TokenType.RIGHT_BRACKET) {
                    ArrayExpr ae = (ArrayExpr) expr;
                    throw new ParseException("syntax error at or near \"[\"", peek());
                }
                // Every pair of brackets that follows belongs to the same reference: PostgreSQL
                // reads a[1][2] as one subscript into a two-dimensional array, not as a subscript
                // of a subscript.
                java.util.List<SubscriptExpr.Subscript> subscripts =
                        new java.util.ArrayList<SubscriptExpr.Subscript>();
                while (check(TokenType.LEFT_BRACKET)) {
                    advance(); // consume [
                    Expression lower = null;
                    Expression upper = null;
                    boolean isSlice = false;
                    if (check(TokenType.COLON)) {
                        // [:upper], no lower bound
                        advance(); // consume :
                        isSlice = true;
                        if (!check(TokenType.RIGHT_BRACKET)) {
                            upper = parseExpression();
                        }
                    } else {
                        lower = parseExpression();
                        if (check(TokenType.COLON)) {
                            advance(); // consume :
                            isSlice = true;
                            if (!check(TokenType.RIGHT_BRACKET)) {
                                upper = parseExpression();
                            }
                        }
                    }
                    expect(TokenType.RIGHT_BRACKET);
                    subscripts.add(new SubscriptExpr.Subscript(lower, upper, isSlice));
                }
                expr = new SubscriptExpr(expr, subscripts);
            } else if (matchKeywords("AT", "TIME", "ZONE")) {
                Expression zone = parsePrimary();
                expr = new AtTimeZoneExpr(expr, zone);
            } else if (matchKeywords("AT", "LOCAL")) {
                // PG 17's AT LOCAL is AT TIME ZONE with the session's TimeZone, which the
                // evaluator reads when the zone expression is absent.
                expr = new AtTimeZoneExpr(expr, null);
            } else if (matchKeyword("COLLATE")) {
                // COLLATE postfix: validate collation name and wrap in CollateExpr.
                if (isClauseKeyword()) {
                    pos--; // un-consume COLLATE so it becomes a potential alias
                    break;
                } else {
                    String collation = readIdentifierOrString();
                    if (match(TokenType.DOT)) {
                        collation = collation + "." + readIdentifierOrString();
                    }
                    validateCollation(collation);
                    // A collation name is an identifier and keeps the case it was written with:
                    // COLLATE "C" names the collation C. Everything that reads it back matches
                    // case-insensitively, so only the name reported to the client changes.
                    expr = new CollateExpr(expr, collation);
                }
            } else if (check(TokenType.DOT) && expr instanceof ArrayExpr && ((ArrayExpr) expr).isRow()) {
                ArrayExpr ae = (ArrayExpr) expr;
                // ROW(...).field: composite field access
                advance(); // consume DOT
                String fieldName = readIdentifier();
                expr = new FieldAccessExpr(expr, fieldName);
            } else {
                break;
            }
        }

        // PG rejects FILTER on ordered-set aggregates after a cast as a syntax error (42601).
        // e.g. percentile_cont(0.5) WITHIN GROUP (ORDER BY val)::integer FILTER (WHERE ...)
        if (checkKeyword("FILTER") && containsOrderedSetAgg(expr)) {
            throw new ParseException("FILTER is not implemented for ordered-set aggregates", peek());
        }

        return expr;
    }

    /** Check if the expression is or wraps an OrderedSetAggExpr (e.g. through CastExpr). */
    private boolean containsOrderedSetAgg(Expression expr) {
        if (expr instanceof OrderedSetAggExpr) return true;
        if (expr instanceof CastExpr) return containsOrderedSetAgg(((CastExpr) expr).expr());
        return false;
    }

    private static final java.util.Set<String> KNOWN_COLLATIONS = Cols.setOf(
            "c", "posix", "default", "ucs_basic",
            "pg_catalog.c", "pg_catalog.posix", "pg_catalog.default",
            "pg_catalog.\"c\"", "pg_catalog.\"posix\"", "pg_catalog.\"default\"",
            "\"c\"", "\"posix\"", "\"default\"", "\"ucs_basic\"",
            "unicode", "icu_root", "pg_c_utf8", "\"pg_c_utf8\"", "pg_catalog.pg_c_utf8"
    );

    private void validateCollation(String collation) {
        validateCollationStatic(collation, peek());
    }

    static void validateCollationStatic(String collation, Token errorToken) {
        // Accept all collation names at parse time; unknown collations are validated
        // at runtime by ExprEvaluator.validateCollationAtRuntime which has access
        // to user-defined collations from the database catalog.
        // However, reject OS-level locale collations (e.g., en_US.utf8) that
        // memgres cannot support, to match PG behavior for CREATE INDEX COLLATE.
        if (collation == null) return;
        String lower = collation.toLowerCase(java.util.Locale.ROOT).replace("\"", "");
        if (KNOWN_COLLATIONS.contains(lower)) return;
        if (lower.startsWith("pg_catalog.")) return;
        // Locale-like names with dots (en_US.utf8) are OS collations not available in memgres
        if (lower.contains(".") && !lower.startsWith("c.")) {
            com.memgres.engine.MemgresException ex = new com.memgres.engine.MemgresException(
                    "collation \"" + collation + "\" for encoding \"UTF8\" does not exist", "42704");
            if (errorToken != null && errorToken.position() > 0) ex.setPosition(errorToken.position());
            throw ex;
        }
        // Everything else (could be user-defined) is accepted at parse time
    }

    protected Expression parsePrimary() {
        Token t = peek();

        // Parenthesized expression, subquery, or row constructor
        if (check(TokenType.LEFT_PAREN)) {
            advance();
            // Check if it's a subquery (may include UNION/INTERSECT/EXCEPT)
            if (checkKeyword("SELECT") || checkKeyword("WITH") || checkKeyword("VALUES")) {
                Statement subquery = parseSubqueryWithSetOps();
                expect(TokenType.RIGHT_PAREN);
                Expression subExpr = new SubqueryExpr(subquery);
                // Check for composite field access: (SELECT ...).field
                if (match(TokenType.DOT)) {
                    String fieldName = readIdentifier();
                    subExpr = new FieldAccessExpr(subExpr, fieldName);
                    while (match(TokenType.DOT)) {
                        fieldName = readIdentifier();
                        subExpr = new FieldAccessExpr(subExpr, fieldName);
                    }
                }
                return subExpr;
            }
            Expression expr = parseExpression();
            if (match(TokenType.COMMA)) {
                // Row constructor: (expr, expr, ...)
                List<Expression> elements = new ArrayList<>();
                elements.add(expr);
                elements.add(parseExpression());
                while (match(TokenType.COMMA)) {
                    elements.add(parseExpression());
                }
                expect(TokenType.RIGHT_PAREN);
                return new ArrayExpr(elements, true); // parenthesized tuple is a row constructor
            }
            expect(TokenType.RIGHT_PAREN);
            // Check for composite field access: (expr).field or (expr).* or (expr).field1.field2
            if (match(TokenType.DOT)) {
                if (check(TokenType.STAR)) {
                    advance(); // consume *
                    return new CompositeStarExpr(expr);
                }
                String fieldName = readIdentifier();
                expr = new FieldAccessExpr(expr, fieldName);
                while (match(TokenType.DOT)) {
                    if (check(TokenType.STAR)) {
                        advance();
                        return new CompositeStarExpr(expr);
                    }
                    fieldName = readIdentifier();
                    expr = new FieldAccessExpr(expr, fieldName);
                }
                return expr;
            }
            return expr;
        }

        // Numeric literals
        if (check(TokenType.INTEGER_LITERAL)) {
            return Literal.ofInt(advance().value());
        }
        if (check(TokenType.FLOAT_LITERAL)) {
            return Literal.ofFloat(advance().value());
        }

        // String literals
        if (check(TokenType.STRING_LITERAL)) {
            return Literal.ofString(advance().value());
        }
        // The national character type is bpchar, and a literal of it is written as a cast to one,
        // which is also how PostgreSQL labels the column it comes back in.
        if (check(TokenType.NATIONAL_STRING_LITERAL)) {
            return new CastExpr(Literal.ofString(advance().value()), "bpchar");
        }
        if (check(TokenType.DOLLAR_STRING_LITERAL)) {
            return Literal.ofString(advance().value());
        }
        // Bit string literals: B'1010' or X'1F', stored as string of 0s and 1s
        if (check(TokenType.BIT_STRING_LITERAL)) {
            return Literal.ofBitString(advance().value());
        }

        // Parameter reference
        if (check(TokenType.PARAM)) {
            Token paramToken = peek();
            String param = advance().value();
            // A parameter number is an int, and a client may write anything. Parsing it without a
            // guard let the NumberFormatException out as an internal error.
            try {
                return new ParamRef(Integer.parseInt(param.substring(1)));
            } catch (NumberFormatException e) {
                throw ParseException.saying("parameter number too large at or near \"" + param
                        + "\"", paramToken, "42601");
            }
        }

        // A keyword that names a construct of the grammar is still an ordinary column name where
        // no construct can follow it: "SELECT trim FROM t" reads the column trim, and reading it
        // as the start of a trim() call made a perfectly ordinary query a syntax error.
        if (t.type() == TokenType.KEYWORD && PgKeywords.isColumnNameKeyword(t.value())
                && keywordStandsAlone()) {
            advance();
            return new ColumnRef(null, null, null, t.value().toLowerCase(java.util.Locale.ROOT));
        }

        // Keywords that are values
        if (t.type() == TokenType.KEYWORD) {
            switch (t.value()) {
                case "COLLATION": {
                    // COLLATION FOR (expr) is how pg_collation_for is written.
                    if (pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.KEYWORD
                            && "FOR".equals(tokens.get(pos + 1).value())) {
                        advance();
                        advance();
                        expect(TokenType.LEFT_PAREN);
                        Expression arg = parseExpression();
                        expect(TokenType.RIGHT_PAREN);
                        return new FunctionCallExpr("pg_collation_for",
                                java.util.Collections.singletonList(arg));
                    }
                    break;
                }
                case "USER": {
                    // PostgreSQL's USER is a value function, the same as CURRENT_USER -- but it
                    // is written USER, and a column takes the name of the function that filled
                    // it, so rewriting it here labelled the column current_user.
                    advance();
                    return new FunctionCallExpr("user", new ArrayList<Expression>());
                }
                case "TRUE": {
                    advance(); return Literal.ofBoolean(true);
                }
                case "FALSE": {
                    advance(); return Literal.ofBoolean(false);
                }
                case "NULL": {
                    advance(); return Literal.ofNull();
                }
                case "CURRENT_TIMESTAMP":
                case "CURRENT_DATE":
                case "CURRENT_TIME":
                case "LOCALTIME":
                case "LOCALTIMESTAMP":
                case "CURRENT_USER":
                case "SESSION_USER":
                case "CURRENT_ROLE":
                case "CURRENT_CATALOG":
                case "CURRENT_SCHEMA": {
                    advance();
                    List<Expression> valueArgs = Cols.listOf();
                    // Consume optional empty parentheses (e.g., current_schema() or current_user)
                    if (check(TokenType.LEFT_PAREN)) {
                        int saved = pos;
                        advance(); // (
                        if (check(TokenType.RIGHT_PAREN)) {
                            advance(); // )
                        } else if (PRECISION_TAKING_VALUE_FUNCTIONS.contains(t.value())
                                && check(TokenType.INTEGER_LITERAL)
                                && pos + 1 < tokens.size()
                                && tokens.get(pos + 1).type() == TokenType.RIGHT_PAREN) {
                            // The four clock functions take how many digits of the second to
                            // keep, which is a precision and not an argument to resolve against.
                            valueArgs = Cols.listOf(Literal.ofInt(advance().value()));
                            advance(); // )
                        } else {
                            pos = saved; // restore; there were args after (
                        }
                    }
                    return new FunctionCallExpr(t.value().toLowerCase(java.util.Locale.ROOT), valueArgs);
                }
                case "INTERVAL": {
                    return specialFormParser.parseInterval(); 
                }
                case "DATE": {
                    // DATE 'value' literal
                    if (tokens.size() > pos + 1 && tokens.get(pos + 1).type() == TokenType.STRING_LITERAL) {
                        advance(); // consume DATE
                        String val = advance().value();
                        return new CastExpr(Literal.ofString(val), "date");
                    }
                    // else fall through to identifier handling
                    break;
                }
                case "TIME": {
                    if (tokens.size() > pos + 1 && tokens.get(pos + 1).type() == TokenType.STRING_LITERAL) {
                        advance();
                        String val = advance().value();
                        return new CastExpr(Literal.ofString(val), "time");
                    }
                    // TIME WITH/WITHOUT TIME ZONE 'value', the spelling TIMESTAMP already accepts
                    Expression timeLiteral = specialFormParser.parseQualifiedTimeLiteral();
                    if (timeLiteral != null) return timeLiteral;
                    break;
                }
                case "TIMESTAMP": {
                    return specialFormParser.parseTimestamp(); 
                }
                case "DEFAULT": {
                    // The tree carries no offsets of its own, and a misplaced DEFAULT is reported
                    // against the keyword, so where this one was written is kept with the node.
                    return Literal.ofDefault(advance().position());
                }
                case "OPERATOR": {
                    return specialFormParser.parsePrefixQualifiedOperator(); 
                }
                case "EXISTS": {
                    advance();
                    expect(TokenType.LEFT_PAREN);
                    int extraParens = Math.max(0, countLeadingParensBeforeQuery());
                    consumeLeadingParens(extraParens);
                    Statement subquery = parseSubqueryWithSetOps();
                    consumeTrailingParens(extraParens);
                    expect(TokenType.RIGHT_PAREN);
                    return new ExistsExpr(subquery);
                }
                case "CASE": {
                    return specialFormParser.parseCaseExpression(); 
                }
                case "CAST": {
                    return specialFormParser.parseCastFunction(); 
                }
                case "ARRAY": {
                    return specialFormParser.parseArrayConstructor(); 
                }
                case "ROW": {
                    if (pos + 1 < tokens.size() && tokens.get(pos + 1).type() == TokenType.LEFT_PAREN) {
                        advance(); // consume ROW
                        expect(TokenType.LEFT_PAREN);
                        if (check(TokenType.RIGHT_PAREN)) {
                            advance();
                            return new ArrayExpr(Cols.listOf(), true, true);
                        }
                        List<Expression> elements = parseExpressionList();
                        expect(TokenType.RIGHT_PAREN);
                        return new ArrayExpr(elements, true, true);
                    }
                    advance();
                    return new ColumnRef("row");
                }
                case "NOT": {
                    advance();
                    return new UnaryExpr(UnaryExpr.UnaryOp.NOT, parsePrimary());
                }
                case "SUBSTRING": {
                    return specialFormParser.parseSubstring(); 
                }
                case "POSITION": {
                    return specialFormParser.parsePosition(); 
                }
                case "OVERLAY": {
                    return specialFormParser.parseOverlay(); 
                }
                case "EXTRACT": {
                    return specialFormParser.parseExtract(); 
                }
                case "TRIM": {
                    return specialFormParser.parseTrim(); 
                }
                case "XMLPARSE": {
                    return specialFormParser.parseXmlparse(); 
                }
                case "XMLSERIALIZE": {
                    return specialFormParser.parseXmlserialize(); 
                }
                case "XMLELEMENT": {
                    return specialFormParser.parseXmlelement(); 
                }
                case "XMLFOREST": {
                    return specialFormParser.parseXmlforest(); 
                }
                case "XMLPI": {
                    return specialFormParser.parseXmlpi(); 
                }
                case "XMLROOT": {
                    return specialFormParser.parseXmlroot(); 
                }
                case "XMLCONCAT": {
                    return specialFormParser.parseBuiltinFunction(); 
                }
                case "XMLEXISTS": {
                    return specialFormParser.parseXmlexists(); 
                }
                case "XMLAGG": {
                    // xmlagg is an aggregate that supports ORDER BY inside
                    advance(); // consume XMLAGG keyword
                    return parseFunctionCallExpr("xmlagg");
                }
                case "COALESCE":
                case "NULLIF":
                case "GREATEST":
                case "LEAST": {
                    return specialFormParser.parseBuiltinFunction();
                }
                // SQL/JSON standard functions (PG 16+)
                case "JSON_EXISTS": return specialFormParser.parseJsonExists();
                case "JSON_VALUE": return specialFormParser.parseJsonValue();
                case "JSON_QUERY": return specialFormParser.parseJsonQuery();
                case "JSON_SCALAR": return specialFormParser.parseJsonScalar();
                case "JSON_SERIALIZE": return specialFormParser.parseJsonSerialize();
                case "JSON_ARRAY": return specialFormParser.parseJsonArray();
                case "JSON_OBJECT": return specialFormParser.parseJsonObject();
                case "JSON_ARRAYAGG": return specialFormParser.parseJsonArrayagg();
                case "JSON_OBJECTAGG": return specialFormParser.parseJsonObjectagg();
                case "NEW":
                case "OLD": {
                    // Trigger variable: NEW.column or OLD.column or NEW.* / OLD.*
                    String prefix = advance().value().toLowerCase(java.util.Locale.ROOT);
                    if (match(TokenType.DOT)) {
                        if (check(TokenType.STAR)) {
                            advance();
                            return new WildcardExpr(prefix);
                        }
                        String col = readIdentifier();
                        return new ColumnRef(null, prefix, col);
                    }
                    return new ColumnRef(prefix);
                }
            }
        }

        // A type name spelt in two words introduces a constant just as a one-word name does:
        // double precision '1.5' is a float8. Reading only the first word left the second one
        // standing where nothing could follow it, and the whole constant was a syntax error.
        Expression twoWordConstant = parseTwoWordTypeLiteral();
        if (twoWordConstant != null) return twoWordConstant;

        // Identifier: could be column ref, function call, type-annotated literal, or qualified name
        if (t.type() == TokenType.IDENTIFIER || t.type() == TokenType.QUOTED_IDENTIFIER || t.type() == TokenType.KEYWORD) {
            // Reject boolean connectives and COLLATE as identifiers, which indicate a syntax error
            if (t.type() == TokenType.KEYWORD && (t.value().equals("AND") || t.value().equals("OR")
                    || t.value().equals("COLLATE"))) {
                throw new ParseException("syntax error at or near \"" + t.value() + "\"", t);
            }
            String name = readIdentifier();

            // A typed literal may carry the type's own precision in front of it, the way a column
            // declaration does: timestamptz(3) '...' keeps three digits of the second.
            String literalPrecision = "";
            if (check(TokenType.LEFT_PAREN) && pos + 3 < tokens.size()
                    && tokens.get(pos + 1).type() == TokenType.INTEGER_LITERAL
                    && tokens.get(pos + 2).type() == TokenType.RIGHT_PAREN
                    && tokens.get(pos + 3).type() == TokenType.STRING_LITERAL
                    && PRECISION_TAKING_LITERALS.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                advance();
                literalPrecision = "(" + advance().value() + ")";
                advance();
            }

            // Type-annotated literal: typename 'value' (e.g., point '(1,2)', DATE '2024-01-01', json '{}')
            if (check(TokenType.STRING_LITERAL)) {
                String lower = name.toLowerCase(java.util.Locale.ROOT);
                if (lower.equals("point") || lower.equals("line") || lower.equals("lseg")
                        || lower.equals("box") || lower.equals("path") || lower.equals("polygon")
                        || lower.equals("circle")
                        || lower.equals("date") || lower.equals("time") || lower.equals("timestamp")
                        || lower.equals("timestamptz") || lower.equals("timetz") || lower.equals("interval")
                        || lower.equals("json") || lower.equals("jsonb")
                        || lower.equals("boolean") || lower.equals("bool")
                        || lower.equals("inet") || lower.equals("cidr") || lower.equals("macaddr")
                        || lower.equals("xml") || lower.equals("uuid")
                        || lower.equals("bit") || lower.equals("varbit")) {
                    String val = advance().value();
                    String castType;
                    switch (lower) {
                        case "timestamptz":
                            castType = "timestamp with time zone";
                            break;
                        case "timetz":
                            castType = "time with time zone";
                            break;
                        case "bool":
                            castType = "boolean";
                            break;
                        default:
                            castType = lower;
                            break;
                    }
                    return new CastExpr(Literal.ofString(val), castType + literalPrecision);
                }
            }

            // Function call: name(...)
            if (check(TokenType.LEFT_PAREN)) {
                return parseFunctionCallExpr(name);
            }

            // Qualified column: table.column or schema.table.column
            if (check(TokenType.DOT)) {
                advance();

                // table.*
                if (check(TokenType.STAR)) {
                    advance();
                    return new WildcardExpr(name);
                }

                String name2 = readIdentifier();

                // Check for schema.table.column (or schema.table.*)
                if (check(TokenType.DOT)) {
                    advance();
                    if (check(TokenType.STAR)) {
                        advance();
                        return new WildcardExpr(name, name2);
                    }
                    String name3 = readIdentifier();
                    // ... and for catalog.schema.table.column, which SQL allows and PostgreSQL
                    // accepts as long as the catalog is the database being queried. A fourth part
                    // was a syntax error at the dot, so a name written the way a tool writes it
                    // would not parse at all.
                    if (check(TokenType.DOT)) {
                        advance();
                        if (check(TokenType.STAR)) {
                            advance();
                            return new WildcardExpr(name, name2, name3);
                        }
                        String name4 = readIdentifier();
                        return new ColumnRef(name, name2, name3, name4);
                    }
                    return new ColumnRef(name, name2, name3);
                }

                // It might be a function call: schema.func(...)
                if (check(TokenType.LEFT_PAREN)) {
                    return parseFunctionCallExpr(name + "." + name2);
                }

                return new ColumnRef(null, name, name2);
            }

            // Constant type cast: identifier 'string_literal' (e.g., open '...', box '...')
            if (pos < tokens.size() && tokens.get(pos).type() == TokenType.STRING_LITERAL) {
                String litValue = advance().value();
                return new CastExpr(Literal.ofString(litValue), name);
            }

            return new ColumnRef(name);
        }

        // Star (when not handled elsewhere)
        if (check(TokenType.STAR)) {
            advance();
            return new WildcardExpr();
        }

        // Operator-like tokens in expression position indicate an undefined operator (42883)
        if (t.type() == TokenType.EQUALS || t.type() == TokenType.ERROR
                || t.type() == TokenType.TS_MATCH) {
            // Bare $ is a syntax error (unterminated parameter reference), not an operator error
            if (t.type() == TokenType.ERROR && "$".equals(t.value())) {
                throw new ParseException("syntax error at or near \"$\"", t);
            }
            // The ! postfix factorial operator was removed in PG 14; it's a syntax error, not an undefined operator
            if (t.type() == TokenType.ERROR && "!".equals(t.value())) {
                throw new ParseException("syntax error at or near \"!\"", t);
            }
            // !! is the tsquery NOT prefix operator
            if (t.type() == TokenType.ERROR && "!!".equals(t.value())) {
                advance(); // consume the !! token
                // Parse the operand expression following !!
                Expression operand = parseUnary();
                return new FunctionCallExpr("__tsquery_not__", java.util.Collections.singletonList(operand));
            }
            // For TS_MATCH (@@) followed by another operator char (e.g. @@@), report the combined operator
            if (t.type() == TokenType.TS_MATCH && pos + 1 < tokens.size()) {
                Token next = tokens.get(pos);
                if (next.type() == TokenType.AT_SIGN) {
                    advance(); // consume @@
                    throw new ParseException("operator does not exist: @@@", t, "42883");
                }
            }
            throw new ParseException("operator does not exist: " + t.value(), t, "42883");
        }

        throw new ParseException("Unexpected token in expression", t);
    }

    /**
     * PostgreSQL's {@code FUNC_MAX_ARGS}: a pg_proc entry has room for this many argument types,
     * so no function call — variadic ones included — may carry more.
     */
    private static final int MAX_FUNCTION_ARGS = 100;

    /**
     * Refuse a function call with more arguments than PostgreSQL can pass. This is deliberately
     * confined to real function calls: {@code COALESCE}, {@code GREATEST}, {@code LEAST},
     * {@code NULLIF} and the XML forms are grammar productions of their own in PostgreSQL rather
     * than {@code FuncCall} nodes, they never reach {@code ParseFuncOrColumn}, and a hundred-odd
     * arguments to those is accepted. They are parsed elsewhere here for the same reason.
     */
    private static void checkFunctionArgCount(int argCount) {
        if (argCount > MAX_FUNCTION_ARGS) {
            throw new com.memgres.engine.MemgresException("cannot pass more than "
                    + MAX_FUNCTION_ARGS + " arguments to a function", "54023");
        }
    }

    /**
     * Parse a function call expression: name(...) with optional DISTINCT, ORDER BY,
     * FILTER, WITHIN GROUP, and OVER clauses. DRYs unqualified and schema-qualified function calls.
     */
    private Expression parseFunctionCallExpr(String name) {
        advance(); // consume (

        if ("grouping".equalsIgnoreCase(name)) return parseGroupingExpr(name);

        boolean isStar = false;
        boolean distinct = false;
        boolean ignoreNulls = false;
        List<Expression> args;

        // COUNT(*) special case
        List<SelectStmt.OrderByItem> innerOrderBy = null;
        if (check(TokenType.STAR)) {
            advance();
            expect(TokenType.RIGHT_PAREN);
            isStar = true;
            args = Cols.listOf();
        } else if (check(TokenType.RIGHT_PAREN)) {
            // Empty args
            advance();
            args = Cols.listOf();
        } else {
            // ALL is the other half of DISTINCT and belongs to every call, aggregate or not:
            // the grammar writes an argument list as "[ ALL | DISTINCT ] expr, ...", so
            // abs(ALL -1) is abs(-1) and count(ALL v) is count(v). Reading only DISTINCT left ALL
            // to be parsed as an expression, which made it a column of that name.
            distinct = matchKeyword("DISTINCT");
            boolean all = !distinct && matchKeyword("ALL");
            if (distinct || all) {
                // One of the two, and then arguments: a star is not an argument list, and the
                // other word is not the start of one either.
                if (check(TokenType.STAR)) {
                    throw new ParseException("syntax error at or near \"*\"", peek());
                }
                if (checkKeyword("DISTINCT") || checkKeyword("ALL")) {
                    throw new ParseException(
                            "syntax error at or near \"" + peek().value() + "\"", peek());
                }
            }
            args = parseFunctionArgList();
            // Check for ORDER BY inside aggregate: string_agg(expr, delim ORDER BY ...)
            if (checkKeyword("ORDER")) {
                innerOrderBy = parseOrderByClause();
            }
            expect(TokenType.RIGHT_PAREN);
        }
        checkFunctionArgCount(args.size());

        // IGNORE NULLS / RESPECT NULLS: PG 18 does not support this syntax.
        // Reject with syntax error to match PG 18 behavior.
        if (checkIdentifier("IGNORE") || checkIdentifier("RESPECT")) {
            int saved = pos;
            boolean isIgnore = matchIdentifier("IGNORE");
            if (!isIgnore) matchIdentifier("RESPECT");
            if (checkKeyword("NULLS")) {
                throw new ParseException("syntax error at or near \"NULLS\"", peek());
            } else {
                // Not followed by NULLS — restore position
                pos = saved;
            }
        }

        // FROM FIRST / FROM LAST: PG 18 does not support this syntax.
        // Reject with syntax error to match PG 18 behavior.
        boolean fromLast = false;
        if (checkKeyword("FROM")) {
            int saved = pos;
            advance();
            if (checkKeyword("LAST") || checkKeyword("FIRST")) {
                // PG 18: syntax error at or near "ORDER" (the OVER keyword that follows)
                throw new ParseException("syntax error at or near \"ORDER\"", peek());
            } else {
                pos = saved;
            }
        }

        // What may follow a call is WITHIN GROUP, then FILTER, then OVER — in that order, because
        // that is the order the grammar writes them in. Reading FILTER first made the FILTER an
        // ordered-set aggregate is entitled to, written where it goes, a syntax error.
        List<SelectStmt.OrderByItem> withinOrderBy = null;
        if (checkKeyword("WITHIN")) {
            advance(); // WITHIN
            expectKeyword("GROUP");
            expect(TokenType.LEFT_PAREN);
            expectKeyword("ORDER");
            expectKeyword("BY");
            withinOrderBy = parseOrderByList();
            expect(TokenType.RIGHT_PAREN);
        }

        // Check for FILTER (WHERE ...) clause on aggregates
        Expression filter = null;
        if (checkKeyword("FILTER")) {
            advance();
            expect(TokenType.LEFT_PAREN);
            expectKeyword("WHERE");
            filter = parseExpression();
            expect(TokenType.RIGHT_PAREN);
        }

        if (withinOrderBy != null) {
            if (checkKeyword("OVER")) {
                throw new MemgresException("OVER is not supported for ordered-set aggregate "
                        + name.toLowerCase(java.util.Locale.ROOT), "0A000");
            }
            return new OrderedSetAggExpr(name.toLowerCase(java.util.Locale.ROOT), args, withinOrderBy, filter);
        }

        // Check for OVER clause: window function
        if (checkKeyword("OVER")) {
            // An aggregate's own ORDER BY decides the order it accumulates its input in, which a
            // window frame already fixes; PostgreSQL has never implemented the combination.
            //
            // It resolves the call first, though, and says so about the combination only once the
            // call is a function at all: row_number takes no arguments, so
            // row_number(v ORDER BY v) OVER () is "function row_number(integer) does not exist"
            // and never reaches this. The call is left to be resolved where the register and the
            // signature table are -- but only where the written argument list settles that it
            // cannot resolve, so every combination that does resolve is still refused here.
            if (innerOrderBy != null
                    && !BuiltinFunctionSignatures.windowCallCannotResolve(name, args.size())) {
                throw new MemgresException(
                        "aggregate ORDER BY is not implemented for window functions", "0A000");
            }
            return specialFormParser.parseWindowFunction(name, args, distinct, isStar, ignoreNulls, fromLast, filter);
        }

        if (innerOrderBy != null || filter != null) {
            return new FunctionCallExpr(name, args, distinct, isStar, innerOrderBy, filter);
        }
        if (isStar) return new FunctionCallExpr(name, args, false, true);
        return new FunctionCallExpr(name, args, distinct, false);
    }

    /**
     * GROUPING is a production of the grammar in its own right — {@code GROUPING ( expr, ... )} —
     * rather than a function that happens to be spelled that way. It takes at least one argument,
     * takes neither DISTINCT nor ALL, and admits nothing after its closing parenthesis. Reading it
     * as an ordinary call let {@code grouping()} answer 0 and let OVER, FILTER and WITHIN GROUP be
     * written after it, none of which the grammar has anywhere to put.
     */
    private Expression parseGroupingExpr(String name) {
        if (check(TokenType.RIGHT_PAREN) || check(TokenType.STAR)
                || checkKeyword("DISTINCT") || checkKeyword("ALL")) {
            throw new ParseException("syntax error at or near \"" + peek().value() + "\"", peek());
        }
        List<Expression> args = parseFunctionArgList();
        expect(TokenType.RIGHT_PAREN);
        if (checkKeyword("WITHIN") || checkKeyword("FILTER") || checkKeyword("OVER")) {
            throw new ParseException("syntax error at or near \"" + peek().value() + "\"", peek());
        }
        return new FunctionCallExpr(name, args, false, false);
    }

    /** Forwarding method for SelectParser; delegates to ExprSpecialFormParser. */
    protected WindowFuncExpr.FrameClause parseWindowFrame() {
        return specialFormParser.parseWindowFrame();
    }
}
