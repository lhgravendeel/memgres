package com.memgres.engine;

/**
 * Exception thrown by the Memgres engine for SQL errors.
 * Supports SQLSTATE error codes per the PostgreSQL specification.
 */
public class MemgresException extends RuntimeException {

    private final String sqlState;
    private String detail;
    private String hint;
    private String column;
    private String constraint;
    private String datatype;
    private String table;
    private String schema;
    private int position; // 1-based character position in the query (P field)
    private String positionToken; // the token in the query text the position points at
    private boolean positionSuppressed; // PostgreSQL sends no P field for this error
    private String pgContext; // PL/pgSQL exception context (function name + line)
    private String copyField; // the field of a COPY line this was raised in

    public MemgresException(String message) {
        super(primaryOf(message));
        this.sqlState = inferSqlState(message);
        this.detail = sectionOf(message, DETAIL_MARK);
        this.hint = sectionOf(message, HINT_MARK);
        if (this.hint == null) this.hint = standingHint(getMessage(), this.sqlState);
    }

    public MemgresException(String message, String sqlState) {
        super(primaryOf(message));
        this.sqlState = sqlState;
        this.detail = sectionOf(message, DETAIL_MARK);
        this.hint = sectionOf(message, HINT_MARK);
        if (this.hint == null) this.hint = standingHint(getMessage(), sqlState);
    }

    /**
     * The advice PostgreSQL attaches to an error by what the error says. It is the same sentence
     * every time, and it depends only on what the message says could not be found, so saying it
     * here says it once rather than at each of the forty places that raise the error.
     *
     * <p>Three things the sentence turns on are readable in the message itself. Two candidates
     * that fit a call equally well are advised about differently from none at all. An operator
     * written in front of its one operand has one argument to cast, and PostgreSQL says so in the
     * singular. And a routine named by a signature written between quotes was looked up as
     * written rather than resolved from arguments, so there is nothing a cast could change and
     * PostgreSQL offers nothing.
     */
    private static String standingHint(String message, String sqlState) {
        if (message == null) return null;
        if ("42725".equals(sqlState)) {
            if (message.startsWith("operator is not unique")) {
                return "Could not choose a best candidate operator."
                        + " You might need to add explicit type casts.";
            }
            if (message.startsWith("function ") && message.endsWith("is not unique")) {
                return "Could not choose a best candidate function."
                        + " You might need to add explicit type casts.";
            }
            return null;
        }
        if (!"42883".equals(sqlState) || message.indexOf("does not exist") < 0) return null;
        if (message.startsWith("operator does not exist")) {
            return prefixOperator(message)
                    ? "No operator matches the given name and argument type."
                            + " You might need to add an explicit type cast."
                    : "No operator matches the given name and argument types."
                            + " You might need to add explicit type casts.";
        }
        String kind = null;
        if (message.startsWith("function ")) kind = "function";
        else if (message.startsWith("procedure ")) kind = "procedure";
        if (kind == null || message.startsWith(kind + " \"")) return null;
        return "No " + kind + " matches the given name and argument types."
                + " You might need to add explicit type casts.";
    }

    /**
     * True when a missing operator was written in front of its one operand. The left operand's
     * type comes first when there is one, and a type name begins with a letter or a quote, so a
     * message that goes straight on to the spelling is the one-argument form.
     */
    private static boolean prefixOperator(String message) {
        int colon = message.indexOf(": ");
        if (colon < 0 || colon + 2 >= message.length()) return false;
        char first = message.charAt(colon + 2);
        return !Character.isLetter(first) && first != '"';
    }

    /**
     * A detail and a hint are fields of an error, not sentences inside its message. They are
     * written here the way PostgreSQL prints them — on their own indented lines — because that is
     * how a throw site reads, and they are taken apart again so the protocol can send each in the
     * field it belongs to. Left in the message they reached a client as one run-on sentence with
     * nothing in the DETAIL or HINT the client asked for.
     */
    private static final String DETAIL_MARK = "\n  Detail: ";
    private static final String HINT_MARK = "\n  Hint: ";

    /** The message with any detail and hint taken out of it. */
    private static String primaryOf(String message) {
        if (message == null) return null;
        int cut = message.length();
        int detail = message.indexOf(DETAIL_MARK);
        int hint = message.indexOf(HINT_MARK);
        if (detail >= 0) cut = Math.min(cut, detail);
        if (hint >= 0) cut = Math.min(cut, hint);
        return cut == message.length() ? message : message.substring(0, cut);
    }

    /** One labelled section of a message, or null when it carries none. */
    private static String sectionOf(String message, String mark) {
        if (message == null) return null;
        int at = message.indexOf(mark);
        if (at < 0) return null;
        int from = at + mark.length();
        // A section runs to the next labelled one, whichever that is.
        int end = message.length();
        int nextDetail = message.indexOf(DETAIL_MARK, from);
        int nextHint = message.indexOf(HINT_MARK, from);
        if (nextDetail >= 0) end = Math.min(end, nextDetail);
        if (nextHint >= 0) end = Math.min(end, nextHint);
        String section = message.substring(from, end).trim();
        return section.isEmpty() ? null : section;
    }

    public String getSqlState() {
        return sqlState;
    }

    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public String getHint() { return hint; }
    public void setHint(String hint) { this.hint = hint; }
    public String getColumn() { return column; }
    public void setColumn(String column) { this.column = column; }
    public String getConstraint() { return constraint; }
    public void setConstraint(String constraint) { this.constraint = constraint; }
    public String getDatatype() { return datatype; }
    public void setDatatype(String datatype) { this.datatype = datatype; }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    /**
     * The token in the statement text the position should point at.
     *
     * <p>The engine analyses an AST that carries no source offsets, so where PostgreSQL reports a
     * parse-analysis error against the exact character it read, this names the word to look for
     * instead and the protocol layer finds it in the statement text. Naming the token is what
     * separates {@code LIMIT generate_series(1,1)} — where PostgreSQL points at the call and not
     * at the clause — from a message whose quoted name happens to occur elsewhere.
     */
    public String getPositionToken() { return positionToken; }
    public void setPositionToken(String positionToken) { this.positionToken = positionToken; }

    /**
     * True when PostgreSQL sends no Position field for this error at all.
     *
     * <p>Not every error PostgreSQL raises has a parse location: the ones raised while a query's
     * range table is being built — a FROM name given twice, a USING column named twice or missing
     * — carry none, because the check runs over the built namespace rather than over a node the
     * parser located. A message with a quoted name in it otherwise looks exactly like one that
     * does, so the throw site is what has to say so.
     */
    public boolean isPositionSuppressed() { return positionSuppressed; }
    public MemgresException suppressPosition() { this.positionSuppressed = true; return this; }

    /**
     * Drop the standing advice from this error.
     *
     * <p>A statement that writes a routine's argument types out — ALTER FUNCTION, COMMENT ON, a
     * trigger's EXECUTE FUNCTION, an operator's FUNCTION — has already said which routine it
     * means, so PostgreSQL says only that there is no such routine: no cast the writer could add
     * would find another one. The message is word for word the one a call that resolved to
     * nothing produces, so only the statement doing the looking knows the difference.
     */
    public MemgresException withoutHint() { this.hint = null; return this; }
    public String getPgContext() { return pgContext; }
    public void setPgContext(String pgContext) { this.pgContext = pgContext; }

    /**
     * The field of a COPY line this error was raised in, named the way PostgreSQL names it.
     *
     * <p>PostgreSQL holds on to the column and the text it is handing a type's reader for as long
     * as that reader is running, and reports them in place of the line when it fails: the value
     * that could not be read says more than the line it sat on. The relation and the line number
     * are the copy's own to add, so what is recorded here is only what the reader knew.
     */
    public String getCopyField() { return copyField; }
    public void setCopyField(String copyField) { this.copyField = copyField; }

    /**
     * Infer a SQLSTATE code from common error message patterns.
     * This provides reasonable defaults when callers don't specify an explicit code.
     */
    private static String inferSqlState(String message) {
        if (message == null) return "42000";
        String lower = message.toLowerCase(java.util.Locale.ROOT);

        // 42P01: undefined table/relation
        if (lower.contains("table not found") || (lower.contains("relation") && lower.contains("does not exist"))
                || lower.contains("table reference not found"))
            return "42P01";

        // 42703: undefined column
        if (lower.contains("column not found") || (lower.contains("column") && lower.contains("does not exist")))
            return "42703";

        // 42704: undefined type/object
        if ((lower.contains("type") && lower.contains("does not exist"))
                || (lower.contains("role") && lower.contains("does not exist")))
            return "42704";

        // 22012: division by zero
        if (lower.contains("division by zero"))
            return "22012";

        // 22003: numeric value out of range
        if (lower.contains("out of range") || lower.contains("overflow"))
            return "22003";

        // 42P02: undefined parameter
        if (lower.contains("parameter") && lower.contains("does not exist"))
            return "42P02";

        // 42883: undefined function/operator
        if (lower.contains("unknown function") || (lower.contains("function") && lower.contains("does not exist"))
                || lower.contains("operator does not exist"))
            return "42883";

        // 42804: datatype mismatch
        if (lower.contains("type mismatch") || lower.contains("datatype mismatch")
                || lower.contains("array subscript must have type"))
            return "42804";

        // 42723: duplicate function
        if (lower.contains("function") && lower.contains("already exists"))
            return "42723";

        // 42P06: duplicate schema
        if (lower.contains("schema") && lower.contains("already exists"))
            return "42P06";

        // 42P07: duplicate table
        if ((lower.contains("table") && lower.contains("already exists"))
                || (lower.contains("relation") && lower.contains("already exists")))
            return "42P07";

        // 42710: duplicate object
        if (lower.contains("already exists"))
            return "42710";

        // 25P02: in failed transaction
        if (lower.contains("current transaction is aborted"))
            return "25P02";

        // 22P02: invalid text representation
        if (lower.contains("invalid input syntax"))
            return "22P02";

        // Default
        return "42000";
    }
}
