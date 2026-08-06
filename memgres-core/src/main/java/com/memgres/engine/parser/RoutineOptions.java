package com.memgres.engine.parser;

import java.util.HashSet;
import java.util.Set;

/**
 * The options a routine definition may carry, and the two ways a list of them can be wrong.
 *
 * <p>PostgreSQL reads the options left to right and refuses at the first one it cannot take: an
 * option a procedure has no room for, or an option that was already given. Both checks are per
 * option and in that order, so a procedure written {@code STRICT STRICT} is refused for being a
 * procedure rather than for the repeat.
 *
 * <p>Each option belongs to a group, and it is the group that may appear only once — which is why
 * {@code IMMUTABLE STABLE} and {@code STRICT CALLED ON NULL INPUT} are refused as readily as the
 * same word twice. {@code SET} is the exception PostgreSQL makes: it names a parameter apiece and
 * accumulates.
 */
class RoutineOptions {

    static final String AS = "as";
    static final String LANGUAGE = "language";
    static final String TRANSFORM = "transform";
    static final String WINDOW = "window";
    static final String VOLATILITY = "volatility";
    static final String STRICT = "strict";
    static final String SECURITY = "security";
    static final String LEAKPROOF = "leakproof";
    static final String COST = "cost";
    static final String ROWS = "rows";
    static final String SUPPORT = "support";
    static final String PARALLEL = "parallel";

    /**
     * What only a function may be. A procedure is called for its effect rather than its value, so
     * nothing describing how a value is computed — how often, from what, at what price — applies.
     * LANGUAGE, AS, SECURITY, SET and TRANSFORM are not on this list: a procedure takes those.
     */
    private static final Set<String> FUNCTION_ONLY = new HashSet<String>();

    static {
        FUNCTION_ONLY.add(WINDOW);
        FUNCTION_ONLY.add(VOLATILITY);
        FUNCTION_ONLY.add(STRICT);
        FUNCTION_ONLY.add(LEAKPROOF);
        FUNCTION_ONLY.add(COST);
        FUNCTION_ONLY.add(ROWS);
        FUNCTION_ONLY.add(SUPPORT);
        FUNCTION_ONLY.add(PARALLEL);
    }

    private final boolean isProcedure;
    private final Set<String> given = new HashSet<String>();

    RoutineOptions(boolean isProcedure) {
        this.isProcedure = isProcedure;
    }

    /** Records that {@code group} was given at {@code at}, refusing it if it may not be. */
    void take(String group, Token at) {
        if (isProcedure && FUNCTION_ONLY.contains(group)) {
            throw ParseException.saying("invalid attribute in procedure definition", at, "42P13");
        }
        if (!given.add(group)) {
            throw ParseException.saying("conflicting or redundant options", at, "42601");
        }
    }

    boolean has(String group) {
        return given.contains(group);
    }
}
