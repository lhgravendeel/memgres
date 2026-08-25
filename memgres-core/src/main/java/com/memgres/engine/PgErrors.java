package com.memgres.engine;

/**
 * Central construction and translation of PostgreSQL-shaped errors.
 *
 * <p>Two distinct jobs live here. {@link #translate} is the safety net: a Java throwable that
 * escapes evaluation must never reach a client as {@code XX000 Internal error: <java type>}, which
 * tells the client the database is broken rather than that the statement was wrong. Where the
 * throwable has a documented PostgreSQL equivalent — a blown stack is {@code 54001}, exhausted
 * memory is {@code 53200} — it is reported as that.
 *
 * <p>The remaining methods are the vocabulary used by definition-time checks, so that a rejection
 * carries the same SQLSTATE and the same wording PostgreSQL would use.
 */
public final class PgErrors {

    private PgErrors() {
    }

    /**
     * Nesting limit for routine bodies — functions, procedures and DO blocks.
     *
     * <p>PostgreSQL bounds recursion by stack bytes rather than by frames, so its own limit moves
     * with {@code max_stack_depth} and with how much stack each frame happens to take; a default
     * installation manages a few hundred frames of plain PL/pgSQL recursion. This is the
     * equivalent bound expressed in frames, chosen so that ordinary recursive traversals finish
     * and runaway recursion is reported as {@code 54001} rather than blowing the Java stack.
     */
    public static final int MAX_ROUTINE_DEPTH = 1000;

    /**
     * Nesting limit for trigger firings. Far lower than {@link #MAX_ROUTINE_DEPTH} because each
     * level carries a whole statement and its trigger body, so a level costs many times what a
     * plain recursive call does. A trigger that writes to its own table recurses without bound and
     * has to be stopped well before the stack is gone.
     */
    public static final int MAX_TRIGGER_DEPTH = 100;

    /**
     * Map a throwable that escaped evaluation onto a PostgreSQL error.
     *
     * <p>A {@link MemgresException} already carries its SQLSTATE and is returned unchanged.
     * Everything else is a defect in this engine; the ones with a documented PostgreSQL
     * counterpart are reported as that counterpart, and the rest stay {@code XX000} so they remain
     * visible as internal failures rather than being disguised as user errors.
     */
    public static MemgresException translate(Throwable t) {
        if (t instanceof MemgresException) {
            return (MemgresException) t;
        }
        if (t instanceof StackOverflowError) {
            return stackDepthExceeded();
        }
        if (t instanceof OutOfMemoryError) {
            return new MemgresException("out of memory", "53200");
        }
        Throwable cause = t.getCause();
        if (cause != null && cause != t && cause instanceof MemgresException) {
            return (MemgresException) cause;
        }
        String detail = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        return new MemgresException("Internal error: " + detail, "XX000");
    }

    /** {@code 54001} — recursion went deeper than the engine will follow. */
    public static MemgresException stackDepthExceeded() {
        MemgresException e = new MemgresException("stack depth limit exceeded", "54001");
        // Worded exactly as PostgreSQL words it, down to the quoted parameter name and the
        // current value, so a client cannot tell the two engines apart from the error alone.
        e.setHint("Increase the configuration parameter \"max_stack_depth\" (currently 2048kB), "
                + "after ensuring the platform's stack depth limit is adequate.");
        return e;
    }

    /** {@code 22015} — a field of an interval, or the interval itself, is too large to represent. */
    public static MemgresException intervalFieldOutOfRange(String literal) {
        return new MemgresException("interval field value out of range: \"" + literal + "\"", "22015");
    }

    /** {@code 42601} — the statement is syntactically wrong. */
    public static MemgresException syntax(String message) {
        return new MemgresException(message, "42601");
    }

    /** {@code 42P13} — a routine's definition is not self-consistent. */
    public static MemgresException invalidObjectDefinition(String message) {
        return new MemgresException(message, "42P13");
    }

    /** {@code 42809} — the object named is of the wrong kind for this statement. */
    public static MemgresException wrongObjectType(String message) {
        return new MemgresException(message, "42809");
    }

    /** {@code 42710} — an object of this name already exists. */
    public static MemgresException duplicateObject(String kind, String name) {
        return new MemgresException(kind + " \"" + name + "\" already exists", "42710");
    }

    /** {@code 42704} — the object named does not exist. */
    public static MemgresException undefinedObject(String kind, String name) {
        return new MemgresException(kind + " \"" + name + "\" does not exist", "42704");
    }

    /** {@code 42701} — a name appears more than once where it must be unique. */
    public static MemgresException duplicateColumn(String name) {
        return new MemgresException("column \"" + name + "\" specified more than once", "42701");
    }

    /** {@code 42804} — an expression has the wrong type for the context it appears in. */
    public static MemgresException datatypeMismatch(String message) {
        return new MemgresException(message, "42804");
    }

    /** {@code 42P17} — the object's definition is not valid, though each part parses. */
    public static MemgresException invalidObjectState(String message) {
        return new MemgresException(message, "42P17");
    }

    /**
     * {@code 42P07} — the statement would make a table a child of one of its own descendants.
     * PostgreSQL rejects the statement that closes the loop rather than storing a cyclic
     * hierarchy that no later walk of it could finish.
     */
    public static MemgresException circularInheritance(String child, String parent) {
        MemgresException e = new MemgresException("circular inheritance not allowed", "42P07");
        e.setDetail("\"" + parent + "\" is already a child of \"" + child + "\".");
        return e;
    }

    /**
     * {@code 42P17} — expanding this relation's rules (a view body is one) leads back to the
     * relation itself.
     */
    public static MemgresException infiniteRecursionInRules(String relation) {
        return new MemgresException(
                "infinite recursion detected in rules for relation \"" + relation + "\"", "42P17");
    }

    /** {@code 22023} — an argument is outside the range the routine accepts. */
    public static MemgresException invalidParameter(String message) {
        return new MemgresException(message, "22023");
    }

    /** {@code 0A000} — valid SQL that this engine deliberately does not implement. */
    public static MemgresException notImplemented(String message) {
        return new MemgresException(message, "0A000");
    }

    /**
     * {@code 23502} — a rule forbidding nulls is being declared over a column that already holds
     * one. PostgreSQL names the column in the error's own column field as well as in the sentence,
     * so a client reading the fields rather than the prose still learns what has to be put right.
     *
     * @param noun how the statement refers to what holds the column: a statement about the table
     *     says "relation", while ALTER DOMAIN, which reaches the column through its type, says
     *     "table"
     */
    public static MemgresException columnContainsNulls(String column, String noun, String relation) {
        MemgresException e = new MemgresException("column \"" + column + "\" of " + noun
                + " \"" + relation + "\" contains null values", "23502");
        e.setColumn(column);
        return e;
    }
}
