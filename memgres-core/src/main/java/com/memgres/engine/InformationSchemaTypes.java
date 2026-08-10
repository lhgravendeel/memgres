package com.memgres.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The domains information_schema is written in terms of.
 *
 * <p>The SQL standard describes its own catalog in five named domains rather than in the base
 * types underneath them, and PostgreSQL declares all five in the information_schema namespace. They
 * are ordinary domains: {@code cardinal_number} is a non-negative integer, {@code yes_or_no} a
 * three-character string that must read YES or NO, and a value cast to one is checked the way any
 * domain's is.
 *
 * <p>They answer only under that qualifier, which is why the parser keeps it: {@code
 * cardinal_number} on its own is no type, and neither is {@code public.cardinal_number}. That is
 * PostgreSQL's own reading, and it is what makes these worth modelling separately rather than
 * registering as ordinary domains of this database — a domain here answers to its bare name
 * everywhere, and these must not.
 */
public final class InformationSchemaTypes {

    private InformationSchemaTypes() {
    }

    /** Each domain's name, and the type its values really are. */
    private static final Map<String, DataType> DOMAINS;

    static {
        Map<String, DataType> m = new LinkedHashMap<String, DataType>();
        m.put("cardinal_number", DataType.INTEGER);
        m.put("character_data", DataType.TEXT);
        m.put("sql_identifier", DataType.NAME);
        m.put("yes_or_no", DataType.VARCHAR);
        m.put("time_stamp", DataType.TIMESTAMPTZ);
        DOMAINS = Collections.unmodifiableMap(m);
    }

    static final String SCHEMA = "information_schema";

    /** Whether a written qualifier names the schema these belong to. */
    public static boolean isTheSchema(String qualifier) {
        return SCHEMA.equalsIgnoreCase(qualifier);
    }

    /** The names, without their qualifier. */
    static Set<String> names() {
        return DOMAINS.keySet();
    }

    /** Whether this name, written under this schema, is one of them. */
    static boolean holds(String qualifier, String bare) {
        return isTheSchema(qualifier) && bare != null
                && DOMAINS.containsKey(bare.toLowerCase(Locale.ROOT));
    }

    /**
     * The type a written name stands for, or null when the name is not one of these. The name is
     * the qualified one the parser kept, {@code information_schema.cardinal_number}.
     */
    static DataType baseTypeOf(String written) {
        return DOMAINS.get(bareOf(written));
    }

    /** Whether this written name is one of these domains. */
    static boolean isOne(String written) {
        return bareOf(written) != null && DOMAINS.containsKey(bareOf(written));
    }

    /** The name without its qualifier, or null when the name carries a different one. */
    private static String bareOf(String written) {
        if (written == null) return null;
        String lower = written.trim().toLowerCase(Locale.ROOT);
        int dot = lower.indexOf('.');
        if (dot < 0) return null;
        return isTheSchema(lower.substring(0, dot)) ? lower.substring(dot + 1) : null;
    }

    /**
     * Applies the domain's own rule to a value already coerced to its base type.
     *
     * <p>{@code yes_or_no} is the only one that carries a constraint, and PostgreSQL reports a
     * domain's constraint by the domain's qualified name and the constraint's own.
     */
    static Object check(String written, Object value) {
        String bare = bareOf(written);
        if (value == null || !"yes_or_no".equals(bare)) return value;
        String text = value.toString();
        if ("YES".equals(text) || "NO".equals(text)) return value;
        MemgresException ex = new MemgresException("value for domain " + SCHEMA + ".yes_or_no"
                + " violates check constraint \"yes_or_no_check\"", "23514");
        ex.setConstraint("yes_or_no_check");
        // The qualifier belongs to the sentence, which has to say which yes_or_no; the field is
        // already about one type and carries the bare name.
        ex.setDatatype("yes_or_no");
        throw ex;
    }
}
