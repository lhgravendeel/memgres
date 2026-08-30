package com.memgres.engine;

/**
 * The eleven functions the SQL grammar spells as keywords rather than as calls.
 *
 * <p>PostgreSQL keeps {@code CURRENT_DATE} and its kin as a node of their own rather than as a
 * function call, so a stored definition writes them back the way the grammar spells them: in
 * capitals and with no parentheses. memgres's parser reads each of them into an ordinary call of
 * no arguments, which is why the spelling has to be put back where a definition is written rather
 * than read off the tree.
 *
 * <p>Each of them also answers a type of its own, and that type is what settles a constant written
 * beside one: {@code CURRENT_DATE > '2020-01-02'} stores a date constant, not a text one.
 *
 * <p>{@code USER} is listed because it is one of the eleven, but the parser reads it as
 * {@code CURRENT_USER}, so a definition that was written with it comes back written the other way.
 */
final class SqlValueFunctions {

    private SqlValueFunctions() {
    }

    /** The names the grammar spells as keywords, lower case, in the order PostgreSQL lists them. */
    private static final String[] NAMES = {
            "current_catalog", "current_date", "current_role", "current_schema", "current_time",
            "current_timestamp", "current_user", "localtime", "localtimestamp", "session_user",
            "user",
    };

    /**
     * The keyword a call of this name is written back as, or null where the call is an ordinary
     * function. A call carrying arguments is one of those: {@code current_schema()} is the
     * function of that name, which PostgreSQL prints as a call and not as the keyword.
     */
    static String keywordOf(String name, boolean hasArguments) {
        if (name == null || hasArguments) return null;
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        for (int i = 0; i < NAMES.length; i++) {
            if (NAMES[i].equals(lower)) return NAMES[i].toUpperCase(java.util.Locale.ROOT);
        }
        return null;
    }

    /** The type one of these answers with, or null for a name that is not one of them. */
    static DataType typeOf(String name) {
        if (name == null) return null;
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if ("current_date".equals(lower)) return DataType.DATE;
        if ("current_time".equals(lower)) return DataType.TIMETZ;
        if ("current_timestamp".equals(lower)) return DataType.TIMESTAMPTZ;
        if ("localtime".equals(lower)) return DataType.TIME;
        if ("localtimestamp".equals(lower)) return DataType.TIMESTAMP;
        // The rest name a role, a database or a schema, and PostgreSQL names all of those with
        // the type it keeps its own identifiers in.
        return keywordOf(lower, false) == null ? null : DataType.NAME;
    }
}
