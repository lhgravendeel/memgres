package com.memgres.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Which built-in a call means, worked out from the types it was written with.
 *
 * <p>PostgreSQL resolves an overloaded name before it runs anything: it keeps the signatures every
 * written argument reaches, prefers exact matches and then the preferred type of each argument's
 * category, and only then reads the arguments whose type is not written yet — an untyped literal,
 * or a parameter the client left unspecified. Those it settles by category: the string category
 * if any candidate takes one there, otherwise the single category the candidates agree on, and
 * within it the category's preferred type. A name that still has more than one candidate after
 * all of that is not a call PostgreSQL can choose, and it says so rather than picking one.
 *
 * <p>That last step is the whole point of this class. {@code sum} is declared over eight types in
 * two categories, so {@code sum(?)} names no particular one of them; {@code abs} is declared over
 * six numeric types, so {@code abs(?)} means the preferred one, {@code float8}. Guessing text for
 * both told a client to send a string to a function that has no string form, and answered a call
 * PostgreSQL refuses.
 *
 * <p>The signatures are the ones already recorded for the catalog — {@link
 * BuiltinFunctionSignatures}, its window table, and {@link BuiltinAggregateSignatures} — read as
 * what each name accepts. A name none of them records is one this class says nothing about.
 */
public final class BuiltinCallTypes {

    private BuiltinCallTypes() {
    }

    /** The OID an argument has when nothing has said yet: an untyped literal, or a parameter. */
    public static final int UNKNOWN = 0;

    // ---- Type categories, as pg_type records them ----

    private static final char NUMERIC = 'N';
    private static final char STRING = 'S';
    private static final char DATETIME = 'D';
    private static final char TIMESPAN = 'T';
    private static final char BOOLEAN = 'B';
    private static final char NETWORK = 'I';
    private static final char BITSTRING = 'V';
    private static final char GEOMETRIC = 'G';
    private static final char ARRAY = 'A';
    private static final char PSEUDO = 'P';
    private static final char USER = 'U';

    private static final Map<Integer, Character> CATEGORY = buildCategories();
    /** The type a category resolves an otherwise undecided argument to. */
    private static final Map<Character, Integer> PREFERRED = buildPreferred();
    /** The pseudo-types a signature uses to say "whatever was passed", which settle nothing. */
    private static final Set<Integer> POLYMORPHIC = buildPolymorphic();

    private static Map<Integer, Character> buildCategories() {
        Map<Integer, Character> m = new HashMap<Integer, Character>();
        int[] numeric = {20, 21, 23, 26, 700, 701, 790, 1700, 2202, 2203, 2204, 2205, 2206, 3734, 3769, 4096, 4089};
        for (int i = 0; i < numeric.length; i++) m.put(Integer.valueOf(numeric[i]), Character.valueOf(NUMERIC));
        int[] string = {18, 19, 25, 1042, 1043};
        for (int i = 0; i < string.length; i++) m.put(Integer.valueOf(string[i]), Character.valueOf(STRING));
        int[] datetime = {702, 1082, 1083, 1114, 1184, 1266};
        for (int i = 0; i < datetime.length; i++) m.put(Integer.valueOf(datetime[i]), Character.valueOf(DATETIME));
        int[] timespan = {703, 1186};
        for (int i = 0; i < timespan.length; i++) m.put(Integer.valueOf(timespan[i]), Character.valueOf(TIMESPAN));
        m.put(Integer.valueOf(16), Character.valueOf(BOOLEAN));
        int[] network = {650, 869};
        for (int i = 0; i < network.length; i++) m.put(Integer.valueOf(network[i]), Character.valueOf(NETWORK));
        int[] bits = {1560, 1562};
        for (int i = 0; i < bits.length; i++) m.put(Integer.valueOf(bits[i]), Character.valueOf(BITSTRING));
        int[] geometric = {600, 601, 602, 603, 604, 628, 718};
        for (int i = 0; i < geometric.length; i++) m.put(Integer.valueOf(geometric[i]), Character.valueOf(GEOMETRIC));
        int[] user = {17, 114, 142, 774, 829, 2950, 3220, 3614, 3615, 3802, 3904, 3906, 3908, 3910, 3912, 3926};
        for (int i = 0; i < user.length; i++) m.put(Integer.valueOf(user[i]), Character.valueOf(USER));
        int[] pseudo = {705, 2249, 2276, 2277, 2278, 2283, 2776, 5077, 5078, 5079, 5086};
        for (int i = 0; i < pseudo.length; i++) m.put(Integer.valueOf(pseudo[i]), Character.valueOf(PSEUDO));
        return m;
    }

    private static Map<Character, Integer> buildPreferred() {
        Map<Character, Integer> m = new HashMap<Character, Integer>();
        m.put(Character.valueOf(NUMERIC), Integer.valueOf(701));      // float8
        m.put(Character.valueOf(STRING), Integer.valueOf(25));        // text
        m.put(Character.valueOf(DATETIME), Integer.valueOf(1184));    // timestamptz
        m.put(Character.valueOf(TIMESPAN), Integer.valueOf(1186));    // interval
        m.put(Character.valueOf(BOOLEAN), Integer.valueOf(16));       // bool
        m.put(Character.valueOf(NETWORK), Integer.valueOf(869));      // inet
        m.put(Character.valueOf(BITSTRING), Integer.valueOf(1562));   // varbit
        m.put(Character.valueOf(GEOMETRIC), Integer.valueOf(603));    // box
        return m;
    }

    private static Set<Integer> buildPolymorphic() {
        Set<Integer> s = new HashSet<Integer>();
        int[] oids = {2276, 2277, 2283, 2776, 5077, 5078, 5079, 5086, 2249, 2278, 3500, 3831, 4537};
        for (int i = 0; i < oids.length; i++) s.add(Integer.valueOf(oids[i]));
        return s;
    }

    // ---- The signatures, indexed by name ----

    /** One declared signature: the types it takes, the type it gives back, and whether it is variadic. */
    static final class Signature {
        final int[] args;
        final int result;
        final boolean variadic;
        /**
         * The fewest arguments this signature accepts, which is its parameter count less the ones
         * carrying a default. A call passing fewer than every parameter is still this signature's
         * call, and reading arity as an exact count left every such call unjudged.
         */
        final int minArgs;

        Signature(int[] args, int result, boolean variadic, int minArgs) {
            this.args = args;
            this.result = result;
            this.variadic = variadic;
            this.minArgs = minArgs;
        }
    }

    /** Built lazily: the signature tables are initialised further down their own files. */
    private static final class Index {
        static final Map<String, List<Signature>> BY_NAME = build();

        static Map<String, List<Signature>> build() {
            Map<String, List<Signature>> m = new HashMap<String, List<Signature>>();
            for (String[] row : BuiltinFunctionSignatures.SIGNATURES) {
                if (!BuiltinFunctionSignatures.isPostgresSignature(row)) continue;
                add(m, row[0], row[2], row[1], BuiltinFunctionSignatures.isVariadic(row),
                        BuiltinFunctionSignatures.fewestArguments(row));
            }
            for (String[] row : BuiltinFunctionSignatures.WINDOW_FUNCTIONS) {
                add(m, row[0], row[2], row[1], false, -1);
            }
            for (String[] row : BuiltinAggregateSignatures.AGGREGATES) {
                add(m, row[0], row[2], row[1], false, -1);
            }
            return m;
        }

        static void add(Map<String, List<Signature>> m, String name, String argOids, String resultOid,
                        boolean variadic, int fewest) {
            int[] args = parseOids(argOids);
            if (args == null) return;
            int minArgs = fewest >= 0 && fewest <= args.length ? fewest : args.length;
            int result = parseOid(resultOid);
            String key = name.toLowerCase(Locale.ROOT);
            List<Signature> list = m.get(key);
            if (list == null) {
                list = new ArrayList<Signature>();
                m.put(key, list);
            }
            list.add(new Signature(args, result, variadic, minArgs));
        }
    }

    private static int[] parseOids(String spec) {
        if (spec == null) return null;
        String trimmed = spec.trim();
        if (trimmed.isEmpty()) return new int[0];
        String[] parts = trimmed.split("\\s+");
        int[] oids = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            oids[i] = parseOid(parts[i]);
            if (oids[i] < 0) return null;
        }
        return oids;
    }

    private static int parseOid(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (RuntimeException e) {
            return -1;
        }
    }

    /** Whether any of the signature tables records this name at all. */
    public static boolean records(String name) {
        return name != null && Index.BY_NAME.containsKey(name.toLowerCase(Locale.ROOT));
    }

    // ---- Resolution ----

    /**
     * The types {@code name} takes when called with {@code argOids}, where a 0 stands for an
     * argument whose type nothing has settled yet. Returns null when this class has no opinion —
     * an unrecorded name, an arity none of its signatures accepts, or a signature written over a
     * polymorphic type, which says only "whatever was passed".
     *
     * @throws MemgresException 42725 when the arguments leave more than one candidate and at least
     *                          one argument is unsettled, which is the case PostgreSQL refuses.
     */
    public static int[] argumentTypes(String name, int[] argOids) {
        List<Signature> candidates = resolve(name, argOids, true);
        if (candidates == null || candidates.size() != 1) return null;
        Signature chosen = candidates.get(0);
        int[] resolved = new int[argOids.length];
        for (int i = 0; i < argOids.length; i++) {
            resolved[i] = declaredAt(chosen, i);
        }
        return resolved;
    }

    /**
     * Whether a type is one a signature writes to mean "whatever was passed". A parameter reaching
     * such a position has nothing to take its type from, which is the one case PostgreSQL reports
     * rather than leaving the parameter unresolved.
     */
    public static boolean isPolymorphic(int oid) {
        return POLYMORPHIC.contains(Integer.valueOf(oid));
    }

    /** The category pg_type puts a type in, or 0 when this class does not record one. */
    public static char categoryOf(int oid) {
        Character c = CATEGORY.get(Integer.valueOf(oid));
        if (c != null) return c.charValue();
        // Every array is of the array category, and there are as many of them as there are types;
        // recording each one by hand would leave the ones nobody thought of looking unclassified.
        DataType type = DataType.fromOid(oid);
        if (type == null) return 0;
        // The categories above are the ones PostgreSQL groups its own types into; a type it
        // records that is in none of them is a type of its own, which is what user-defined means
        // here. Leaving those uncategorised made age(unknown) look decidable between a timestamp
        // and a transaction id, which are as unalike as two types get.
        return DataType.elementOf(type) != null ? ARRAY : USER;
    }

    /**
     * Refuses a call whose written arguments are of a kind no declared signature takes there.
     *
     * <p>Only the kind is judged, never the exact type: a signature table that is complete about
     * which categories a name accepts can still be read too strictly about which type within a
     * category reaches which, and refusing a call PostgreSQL runs is the worse mistake. An
     * argument of a category the name accepts nowhere at that position is not that kind of doubt —
     * {@code overlaps} is declared over instants, and no number is one.
     */
    public static void requireCallable(String name, int[] argOids) {
        requireCallable(name, name, argOids);
    }

    /** As above, reporting the call by the name it was written with. */
    public static void requireCallable(String name, String writtenName, int[] argOids) {
        if (name == null || argOids == null) return;
        List<Signature> declared = Index.BY_NAME.get(name.toLowerCase(Locale.ROOT));
        if (declared == null || declared.isEmpty()) return;
        List<Signature> sameArity = new ArrayList<Signature>();
        for (int i = 0; i < declared.size(); i++) {
            if (acceptsArity(declared.get(i), argOids.length)) sameArity.add(declared.get(i));
        }
        if (sameArity.isEmpty()) return;   // arity is judged elsewhere, and worded differently
        for (int i = 0; i < argOids.length; i++) {
            if (argOids[i] == UNKNOWN) continue;
            char written = categoryOf(argOids[i]);
            if (written == 0 || written == PSEUDO) continue;
            boolean accepted = false;
            for (int c = 0; c < sameArity.size() && !accepted; c++) {
                int at = declaredAt(sameArity.get(c), i);
                accepted = POLYMORPHIC.contains(Integer.valueOf(at)) || categoryOf(at) == 0
                        || categoryOf(at) == written;
            }
            if (!accepted) throw missing(writtenName, argOids);
        }
    }

    /**
     * Refuses a call no declared signature can take, given what its arguments were written as.
     *
     * <p>{@link #requireCallable} judges the category alone, which lets a {@code numeric} through
     * to a {@code bigint} parameter: the category is right and the type is not. PostgreSQL
     * resolves on the type, so {@code pg_advisory_lock(1.5)} is a function that does not exist —
     * there is a cast from numeric to bigint, but only for an assignment, and a call is not one.
     *
     * <p>What makes this safe to refuse on, where the category check was not, is that
     * {@link #convertible} now carries PostgreSQL's own list of the casts it applies unasked. A
     * call still gets the benefit of the doubt wherever there is doubt: an argument whose type the
     * statement does not settle is skipped, and a name declaring nothing of that arity is left to
     * whatever judges arity.
     */
    public static void requireReachable(String name, String writtenName, int[] argOids) {
        if (name == null || argOids == null) return;
        List<Signature> declared = Index.BY_NAME.get(name.toLowerCase(Locale.ROOT));
        if (declared == null || declared.isEmpty()) return;
        boolean anySettled = false;
        for (int i = 0; i < argOids.length; i++) {
            if (argOids[i] == UNKNOWN) continue;
            // A type PostgreSQL does not have is one its signatures say nothing about. memgres
            // declares hstore and extends jsonb_set to it, and a table of PostgreSQL's signatures
            // is not evidence that the call has nowhere to go.
            if (categoryOf(argOids[i]) == USER
                    && CATEGORY.get(Integer.valueOf(argOids[i])) == null) return;
            if (categoryOf(argOids[i]) != PSEUDO) anySettled = true;
        }
        if (!anySettled) return;
        boolean anyOfThisArity = false;
        for (int i = 0; i < declared.size(); i++) {
            Signature sig = declared.get(i);
            if (!acceptsArity(sig, argOids.length)) continue;
            anyOfThisArity = true;
            if (reachable(sig, argOids)) return;
        }
        if (anyOfThisArity) throw missing(writtenName, argOids);
    }

    /**
     * PostgreSQL prints the name a call was written with, except where the call was not written
     * as a call at all: {@code (a, b) OVERLAPS (c, d)} is spelled out in the grammar and reported
     * against the function the grammar means, qualified as PostgreSQL qualifies it.
     */
    private static MemgresException missing(String name, int[] argOids) {
        String printed = name.toLowerCase(Locale.ROOT);
        if (printed.equals("overlaps")) printed = "pg_catalog." + printed;
        return new MemgresException("function " + printed
                + "(" + written(argOids) + ") does not exist"
                + "\n  Hint: No function matches the given name and argument types."
                + " You might need to add explicit type casts.", "42883");
    }

    /**
     * The type {@code name} returns when called with {@code argOids}, or 0 when the call does not
     * settle on one signature. Never throws: a result type is read to describe a call, not to
     * decide whether it may run.
     */
    public static int resultType(String name, int[] argOids) {
        List<Signature> candidates;
        try {
            candidates = resolve(name, argOids, false);
        } catch (MemgresException e) {
            return 0;
        }
        if (candidates == null || candidates.size() != 1) return 0;
        int result = candidates.get(0).result;
        return POLYMORPHIC.contains(Integer.valueOf(result)) ? 0 : result;
    }

    /**
     * Whether a call PostgreSQL cannot choose between would be refused, given the arguments whose
     * types are written. Reports the error PostgreSQL reports, so a caller that has an unsettled
     * argument can refuse the call instead of picking a signature for it.
     */
    public static void requireResolvable(String name, int[] argOids) {
        requireResolvable(name, name, argOids);
    }

    /**
     * As above, reporting the call by the name it was written with. PostgreSQL names what the
     * statement said: a call written {@code pg_catalog.age(NULL)} is reported qualified, and the
     * same call written {@code age(NULL)} is not.
     */
    public static void requireResolvable(String name, String writtenName, int[] argOids) {
        resolve(name, writtenName, argOids, true);
    }

    private static List<Signature> resolve(String name, int[] argOids, boolean strict) {
        return resolve(name, name, argOids, strict);
    }

    private static List<Signature> resolve(String name, String writtenName, int[] argOids,
                                           boolean strict) {
        if (name == null || argOids == null) return null;
        List<Signature> declared = Index.BY_NAME.get(name.toLowerCase(Locale.ROOT));
        if (declared == null || declared.isEmpty()) return null;

        List<Signature> candidates = new ArrayList<Signature>();
        for (int i = 0; i < declared.size(); i++) {
            Signature sig = declared.get(i);
            if (!acceptsArity(sig, argOids.length)) continue;
            if (reachable(sig, argOids)) candidates.add(sig);
        }
        if (candidates.size() <= 1) return candidates;

        candidates = keepBest(candidates, argOids, true);
        if (candidates.size() == 1) return candidates;
        candidates = keepBest(candidates, argOids, false);
        if (candidates.size() == 1) return candidates;

        boolean everyKindKnown = true;
        for (int i = 0; i < argOids.length; i++) {
            // Whether a name is unchoosable is only safe to report where every candidate's kind
            // is one this class records, whatever the argument at that position said.
            everyKindKnown &= everyCategoryKnown(candidates, i);
            if (argOids[i] != UNKNOWN) continue;
            // A signature written over "whatever was passed" cannot take an argument that has not
            // said what it is, so PostgreSQL drops those candidates before it looks at categories
            // — which is why quote_literal(text) answers a call quote_literal(anyelement) also
            // spells, and why dropping neither made the call look like a choice between the two.
            candidates = discardPolymorphic(candidates, i);
            if (candidates.size() == 1) return candidates;
            candidates = narrowUnsettled(candidates, i);
            if (candidates.size() == 1) return candidates;
        }
        // Candidates that ask for the same types are not a choice: format is declared both with
        // and without a variadic tail, and both take text first, so format('') is not a call
        // PostgreSQL has to choose between -- whichever it runs, the argument is the same.
        if (candidates.size() > 1 && agreeOnEveryArgument(candidates, argOids.length)) {
            return candidates.subList(0, 1);
        }
        // Reporting a name as unchoosable is only safe where every candidate's kind is one this
        // class records. One it does not could be the very candidate PostgreSQL chose.
        //
        // A call whose arguments were all written with a type is judged the same way: to_hex(int2)
        // reaches both to_hex(int4) and to_hex(int8) and is neither of them, so it is not a call
        // PostgreSQL can choose either. Asking only where an argument was untyped let every such
        // call through and picked one of the candidates for it.
        if (candidates.size() > 1 && strict && everyKindKnown
                && noneWrittenOverAnything(candidates, argOids.length)) {
            throw ambiguous(writtenName, argOids);
        }
        return candidates;
    }

    /**
     * Whether no candidate is written over "whatever was passed" at any position.
     *
     * <p>A polymorphic signature takes an argument of any type, so two of them are not a choice
     * PostgreSQL has to make between: {@code array_agg} is declared over both anyarray and
     * anynonarray, and which one a call means follows from whether the argument is an array
     * rather than from anything a preferred type could settle. Only where every candidate names
     * real types is more than one of them a name that cannot be chosen.
     */
    private static boolean noneWrittenOverAnything(List<Signature> candidates, int arity) {
        for (int c = 0; c < candidates.size(); c++) {
            for (int i = 0; i < arity; i++) {
                if (POLYMORPHIC.contains(Integer.valueOf(declaredAt(candidates.get(c), i)))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean acceptsArity(Signature sig, int count) {
        if (sig.variadic) return count >= sig.args.length - 1 && sig.args.length > 0;
        return count >= sig.minArgs && count <= sig.args.length;
    }

    /** Whether every written argument can reach the type the signature declares for its position. */
    private static boolean reachable(Signature sig, int[] argOids) {
        for (int i = 0; i < argOids.length; i++) {
            if (argOids[i] == UNKNOWN) continue;
            int declared = declaredAt(sig, i);
            if (POLYMORPHIC.contains(Integer.valueOf(declared))) continue;
            if (!convertible(argOids[i], declared)) return false;
        }
        return true;
    }

    /**
     * The type a signature declares at one argument position. A VARIADIC signature declares its
     * last parameter as the array, and what a call passes there are its elements — so
     * {@code jsonb_extract_path(jsonb, VARIADIC text[])} takes texts, however many of them.
     */
    private static int declaredAt(Signature sig, int position) {
        if (!sig.variadic) {
            if (position < sig.args.length) return sig.args[position];
            return sig.args.length > 0 ? sig.args[sig.args.length - 1] : 0;
        }
        int last = sig.args.length - 1;
        if (position < last) return sig.args[position];
        if (last < 0) return 0;
        int element = BuiltinFunctionSignatures.variadicElementType(sig.args[last]);
        return element > 0 ? element : sig.args[last];
    }

    /**
     * The casts PostgreSQL applies on its own when it resolves a call — pg_cast's implicit ones,
     * read off the reference server. An argument reaches a parameter it casts to implicitly and
     * no other, which is why {@code numeric} does not reach a {@code bigint} parameter: that cast
     * exists, but only for an assignment, and a call is not one.
     *
     * <p>Pairs of source and target OID. The categories tell most of the same story, but not all
     * of it: nothing about a category says a date reaches a timestamp, a cidr an inet, or a bit a
     * bit varying, and the rank the numeric types are ordered by has no counterpart elsewhere.
     */
    private static final int[] IMPLICIT_CASTS = {
        18, 25,
        19, 25,
        20, 24, 20, 26, 20, 700, 20, 701, 20, 1700, 20, 2202, 20, 2203, 20, 2204, 20, 2205,
        20, 2206, 20, 3734, 20, 3769, 20, 4089, 20, 4096, 20, 4191,
        21, 20, 21, 23, 21, 24, 21, 26, 21, 700, 21, 701, 21, 1700, 21, 2202, 21, 2203, 21, 2204,
        21, 2205, 21, 2206, 21, 3734, 21, 3769, 21, 4089, 21, 4096, 21, 4191,
        23, 20, 23, 24, 23, 26, 23, 700, 23, 701, 23, 1700, 23, 2202, 23, 2203, 23, 2204, 23, 2205,
        23, 2206, 23, 3734, 23, 3769, 23, 4089, 23, 4096, 23, 4191,
        24, 26, 24, 2202,
        25, 19, 25, 1042, 25, 1043, 25, 2205,
        26, 24, 26, 2202, 26, 2203, 26, 2204, 26, 2205, 26, 2206, 26, 3734, 26, 3769, 26, 4089,
        26, 4096, 26, 4191,
        194, 25,
        650, 869,
        700, 701,
        774, 829, 829, 774,
        1042, 19, 1042, 25, 1042, 1043,
        1043, 19, 1043, 25, 1043, 1042, 1043, 2205,
        1082, 1114, 1082, 1184,
        1083, 1186, 1083, 1266,
        1114, 1184,
        1560, 1562, 1562, 1560,
        1700, 700, 1700, 701,
        2202, 24, 2202, 26,
        2203, 26, 2203, 2204,
        2204, 26, 2204, 2203,
        2205, 26,
        2206, 26,
        3361, 17, 3361, 25,
        3402, 17, 3402, 25,
        3734, 26, 3769, 26, 4089, 26, 4096, 26, 4191, 26,
        5017, 17, 5017, 25,
    };

    private static final Set<Long> IMPLICIT = buildImplicit();

    private static Set<Long> buildImplicit() {
        Set<Long> m = new HashSet<Long>();
        for (int i = 0; i + 1 < IMPLICIT_CASTS.length; i += 2) {
            m.add(Long.valueOf(castKey(IMPLICIT_CASTS[i], IMPLICIT_CASTS[i + 1])));
        }
        return m;
    }

    private static long castKey(int from, int to) {
        return (((long) from) << 32) | (to & 0xFFFFFFFFL);
    }

    /** An argument reaches a parameter of its own type, or of one it casts to on its own. */
    private static boolean convertible(int from, int to) {
        if (from == to) return true;
        if (IMPLICIT.contains(Long.valueOf(castKey(from, to)))) return true;
        Character fc = CATEGORY.get(Integer.valueOf(from));
        Character tc = CATEGORY.get(Integer.valueOf(to));
        if (fc == null || tc == null || fc.charValue() != tc.charValue()) return false;
        if (fc.charValue() == NUMERIC) return numericRank(from) <= numericRank(to);
        return fc.charValue() == STRING;   // the string types all reach each other
    }

    private static int numericRank(int oid) {
        switch (oid) {
            case 21: return 1;    // int2
            case 23: return 2;    // int4
            case 20: return 3;    // int8
            case 1700: return 4;  // numeric
            case 700: return 5;   // float4
            case 701: return 6;   // float8
            default: return 7;
        }
    }

    /**
     * Keeps the candidates matching the most written arguments exactly, or — when {@code exact} is
     * false — on the preferred type of the argument's category.
     */
    private static List<Signature> keepBest(List<Signature> candidates, int[] argOids, boolean exact) {
        int bestScore = -1;
        List<Signature> best = new ArrayList<Signature>();
        for (int c = 0; c < candidates.size(); c++) {
            Signature sig = candidates.get(c);
            int score = 0;
            for (int i = 0; i < argOids.length; i++) {
                if (argOids[i] == UNKNOWN) continue;
                int declared = declaredAt(sig, i);
                if (exact) {
                    if (argOids[i] == declared) score++;
                } else {
                    Character cat = CATEGORY.get(Integer.valueOf(argOids[i]));
                    Integer preferred = cat == null ? null : PREFERRED.get(cat);
                    if (preferred != null && preferred.intValue() == declared) score++;
                }
            }
            if (score > bestScore) {
                bestScore = score;
                best.clear();
                best.add(sig);
            } else if (score == bestScore) {
                best.add(sig);
            }
        }
        return best;
    }

    /** Whether every candidate declares the same type at every position the call passes one. */
    private static boolean agreeOnEveryArgument(List<Signature> candidates, int arity) {
        for (int i = 0; i < arity; i++) {
            int first = declaredAt(candidates.get(0), i);
            for (int c = 1; c < candidates.size(); c++) {
                if (declaredAt(candidates.get(c), i) != first) return false;
            }
        }
        return true;
    }

    private static List<Signature> discardPolymorphic(List<Signature> candidates, int position) {
        List<Signature> kept = new ArrayList<Signature>();
        for (int c = 0; c < candidates.size(); c++) {
            if (!POLYMORPHIC.contains(Integer.valueOf(declaredAt(candidates.get(c), position)))) {
                kept.add(candidates.get(c));
            }
        }
        return kept.isEmpty() ? candidates : kept;
    }

    private static boolean everyCategoryKnown(List<Signature> candidates, int position) {
        for (int c = 0; c < candidates.size(); c++) {
            if (categoryOf(declaredAt(candidates.get(c), position)) == 0) return false;
        }
        return true;
    }

    /**
     * Settles one unwritten argument the way PostgreSQL does: the string category if any candidate
     * takes one there, otherwise the one category they agree on, and within it the category's
     * preferred type when a candidate offers it.
     */
    private static List<Signature> narrowUnsettled(List<Signature> candidates, int position) {
        Character selected = null;
        boolean mixed = false;
        for (int c = 0; c < candidates.size(); c++) {
            int declared = declaredAt(candidates.get(c), position);
            char kind = categoryOf(declared);
            if (kind == 0 || kind == PSEUDO) return candidates;   // says nothing
            Character cat = Character.valueOf(kind);
            if (cat.charValue() == STRING) {
                selected = cat;
                mixed = false;
                break;
            }
            if (selected == null) selected = cat;
            else if (selected.charValue() != cat.charValue()) mixed = true;
        }
        if (selected == null || mixed) return candidates;

        List<Signature> kept = new ArrayList<Signature>();
        for (int c = 0; c < candidates.size(); c++) {
            if (categoryOf(declaredAt(candidates.get(c), position)) == selected.charValue()) {
                kept.add(candidates.get(c));
            }
        }
        if (kept.isEmpty()) return candidates;

        Integer preferred = PREFERRED.get(selected);
        if (preferred == null) return kept;
        List<Signature> onPreferred = new ArrayList<Signature>();
        for (int c = 0; c < kept.size(); c++) {
            if (declaredAt(kept.get(c), position) == preferred.intValue()) onPreferred.add(kept.get(c));
        }
        return onPreferred.isEmpty() ? kept : onPreferred;
    }

    /** The argument list as PostgreSQL prints it in an error, an unsettled one named "unknown". */
    private static String written(int[] argOids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < argOids.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(argOids[i] == UNKNOWN ? "unknown" : typeName(argOids[i]));
        }
        return sb.toString();
    }

    private static MemgresException ambiguous(String name, int[] argOids) {
        String sb = written(argOids);
        return new MemgresException("function " + name + "(" + sb + ") is not unique"
                + "\n  Hint: Could not choose a best candidate function."
                + " You might need to add explicit type casts.", "42725");
    }

    /**
     * The name PostgreSQL prints a type with in an error message, which is the name SQL gives it
     * rather than the short one pg_type records: {@code integer}, not {@code int4}.
     */
    public static String typeName(int oid) {
        DataType type = DataType.fromOid(oid);
        if (type == null) return "unknown";
        // An array is named after its element: PostgreSQL writes numeric[] where its catalog
        // holds _numeric, and a client told a function of _numeric does not exist is being told
        // about a type it did not write.
        DataType element = DataType.elementOf(type);
        if (element != null && element != type) return typeName(element.getOid()) + "[]";
        String recorded = type.getPgName();
        if (recorded == null) return "unknown";
        String lower = recorded.toLowerCase(Locale.ROOT);
        if (lower.equals("int4") || lower.equals("int")) return "integer";
        if (lower.equals("int2")) return "smallint";
        if (lower.equals("int8")) return "bigint";
        if (lower.equals("float8")) return "double precision";
        if (lower.equals("float4")) return "real";
        if (lower.equals("varchar")) return "character varying";
        if (lower.equals("bpchar") || lower.equals("char")) return "character";
        if (lower.equals("bool")) return "boolean";
        if (lower.equals("timestamp")) return "timestamp without time zone";
        if (lower.equals("timestamptz")) return "timestamp with time zone";
        if (lower.equals("time")) return "time without time zone";
        if (lower.equals("timetz")) return "time with time zone";
        return lower;
    }
}
