package com.memgres.engine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The index access methods, and how many strategies and support functions each of them has.
 *
 * <p>An operator class tells one access method which operator answers which of its strategies, so
 * a strategy number outside the method's range names nothing. btree has five strategies and six
 * support functions; hash has one and three. The methods whose strategies are defined by the
 * operator class itself — gist, gin, spgist, brin — put no bound on the number.
 */
final class AccessMethods {
    private AccessMethods() {}

    private static final Set<String> KNOWN = new HashSet<String>(Arrays.asList(
            "btree", "hash", "gist", "gin", "spgist", "brin"));

    /** Method to {strategies, support functions}, absent where the method sets no bound. */
    private static final Map<String, int[]> BOUNDS = bounds();

    private static Map<String, int[]> bounds() {
        Map<String, int[]> m = new HashMap<String, int[]>();
        m.put("btree", new int[]{5, 6});
        m.put("hash", new int[]{1, 3});
        return m;
    }

    /**
     * What each index access method can do, as PostgreSQL 18 reports it.
     *
     * <p>Each entry is the method's OID and the properties it has, of the five an access method
     * is asked about. Answering the same for every method said a hash index could order its rows
     * and enforce uniqueness, which is the whole of what a hash index cannot do.
     */
    private static final Map<Integer, Set<String>> PROPERTIES = properties();

    private static Map<Integer, Set<String>> properties() {
        Map<Integer, Set<String>> m = new HashMap<Integer, Set<String>>();
        m.put(403, new HashSet<String>(Arrays.asList(   // btree
                "can_order", "can_unique", "can_multi_col", "can_exclude", "can_include")));
        m.put(405, new HashSet<String>(Arrays.asList("can_exclude")));           // hash
        m.put(783, new HashSet<String>(Arrays.asList(                            // gist
                "can_multi_col", "can_exclude", "can_include")));
        m.put(2742, new HashSet<String>(Arrays.asList("can_multi_col")));        // gin
        m.put(3580, new HashSet<String>(Arrays.asList("can_multi_col")));        // brin
        m.put(4000, new HashSet<String>(Arrays.asList(                           // spgist
                "can_exclude", "can_include")));
        return m;
    }

    /** The five properties an index access method is asked about. */
    private static final Set<String> ASKABLE = new HashSet<String>(Arrays.asList(
            "can_order", "can_unique", "can_multi_col", "can_exclude", "can_include"));

    /**
     * Whether the access method has the property, or null when there is no such method or no
     * such property — neither of which is a question about an access method at all.
     */
    static Boolean hasProperty(int amOid, String property) {
        Set<String> held = PROPERTIES.get(Integer.valueOf(amOid));
        if (held == null || property == null || !ASKABLE.contains(property)) return null;
        return held.contains(property);
    }

    static boolean exists(String method) {
        return method != null && KNOWN.contains(method.toLowerCase(Locale.ROOT));
    }

    /** Refuse an access method nothing implements. */
    static void require(String method) {
        if (!exists(method)) {
            throw new MemgresException("access method \"" + method + "\" does not exist", "42704");
        }
    }

    /**
     * Refuse a strategy or support-function number the method does not have.
     *
     * @param isOperator true for a strategy number, false for a support function number
     */
    static void requireNumberInRange(String method, boolean isOperator, int number) {
        int[] b = BOUNDS.get(method == null ? "" : method.toLowerCase(Locale.ROOT));
        if (b == null) return;
        int max = isOperator ? b[0] : b[1];
        if (number < 1 || number > max) {
            throw new MemgresException("invalid " + (isOperator ? "operator" : "function")
                    + " number " + number + ", must be between 1 and " + max, "42P17");
        }
    }
}
