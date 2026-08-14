package com.memgres.engine;

/**
 * Whether a name PostgreSQL declares routines under has one the written argument types reach.
 *
 * <p>{@link BuiltinCallTypes#requireReachable} answers the same question for a call that is being
 * run, and lets an argument past wherever the signature says "whatever was passed". Most of those
 * really do take anything. Three do not: {@code anyrange}, {@code anymultirange} and
 * {@code anyarray} stand for a type of that kind and for no other — which is the whole of why
 * {@code lower(integer)} is a function that does not exist. lower is declared over text and over
 * the two kinds of range, and an integer reaches none of the three.
 *
 * <p>A call that is being run has its argument in hand and is answered by whatever implements the
 * name; a stored expression has no row to be run against, so the same question is settled from the
 * types the definition itself writes down. That is why the rule is read here rather than folded
 * into the one the running call goes through.
 *
 * <p>Every doubt is answered in the call's favour. An argument nothing has typed yet, one of a
 * type PostgreSQL does not have, a name whose signatures memgres wrote for itself and a name
 * PostgreSQL declares nothing of that arity for all leave the call alone: a definition refused on
 * a guess is worse than one stored on one.
 */
final class StoredCallSignature {

    /** The polymorphic types that stand for one kind of type rather than for any type at all. */
    private static final int ANYARRAY = 2277;
    private static final int ANYRANGE = 3831;
    private static final int ANYMULTIRANGE = 4537;
    private static final int ANYCOMPATIBLEARRAY = 5078;
    private static final int ANYCOMPATIBLERANGE = 5080;
    private static final int ANYCOMPATIBLEMULTIRANGE = 5086;

    /** The categories pg_type puts the ranges and the arrays in. */
    private static final char RANGE = 'R';
    private static final char ARRAY = 'A';
    private static final char PSEUDO = 'P';
    private static final char USER = 'U';

    private StoredCallSignature() {
    }

    /**
     * Whether some signature of {@code name} takes arguments of these types — with a 0 standing
     * for an argument nothing has typed yet — or whether nothing certain can be said about it.
     */
    static boolean mayTake(String name, int[] argOids) {
        if (name == null || argOids == null || argOids.length == 0) return true;
        for (int i = 0; i < argOids.length; i++) {
            if (argOids[i] == BuiltinCallTypes.UNKNOWN) return true;
            char category = BuiltinCallTypes.categoryOf(argOids[i]);
            if (category == 0 || category == PSEUDO) return true;
            // A type PostgreSQL does not have is one its signatures say nothing about: memgres
            // declares hstore and extends the jsonb routines to it, and a table of PostgreSQL's
            // signatures is not evidence that such a call has nowhere to go.
            if (category == USER && !BuiltinCallTypes.recordsCategoryFor(argOids[i])) return true;
        }
        boolean anyOfThisArity = false;
        for (int i = 0; i < BuiltinFunctionSignatures.SIGNATURES.length; i++) {
            String[] signature = BuiltinFunctionSignatures.SIGNATURES[i];
            if (!signature[0].equalsIgnoreCase(name)) continue;
            // A row memgres wrote for itself names the types it happened to write down rather than
            // the ones its implementation takes, so nothing about the name may be read from it.
            if (!BuiltinFunctionSignatures.isPostgresSignature(signature)) return true;
            int[] params = parameterTypes(signature);
            if (params == null) return true;
            if (!acceptsArity(signature, params, argOids.length)) continue;
            anyOfThisArity = true;
            if (reached(signature, params, argOids)) return true;
        }
        return !anyOfThisArity;
    }

    /** The types a signature declares, or null where the table's spelling cannot be read. */
    private static int[] parameterTypes(String[] signature) {
        String spec = signature[2] == null ? "" : signature[2].trim();
        if (spec.isEmpty()) return new int[0];
        String[] parts = spec.split("\\s+");
        int[] oids = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                oids[i] = Integer.parseInt(parts[i]);
            } catch (NumberFormatException notAnOid) {
                return null;
            }
        }
        return oids;
    }

    /**
     * Whether this signature takes this many arguments. It takes anything from its fewest — its
     * parameter count less the ones carrying a default — up to its parameter count, and one that
     * collects a tail takes any number from its fewest upwards.
     */
    private static boolean acceptsArity(String[] signature, int[] params, int count) {
        if (BuiltinFunctionSignatures.isVariadic(signature)) {
            return params.length > 0 && count >= params.length - 1;
        }
        return count >= BuiltinFunctionSignatures.fewestArguments(signature)
                && count <= params.length;
    }

    /**
     * The type a signature declares at one argument position. A signature that collects a tail
     * declares its last parameter as the array, and what a call passes there are its elements.
     */
    private static int declaredAt(String[] signature, int[] params, int position) {
        if (!BuiltinFunctionSignatures.isVariadic(signature)) {
            if (position < params.length) return params[position];
            return params.length > 0 ? params[params.length - 1] : 0;
        }
        int last = params.length - 1;
        if (position < last) return params[position];
        if (last < 0) return 0;
        int element = BuiltinFunctionSignatures.variadicElementType(params[last]);
        return element > 0 ? element : params[last];
    }

    /** Whether every written argument reaches the type this signature declares for its position. */
    private static boolean reached(String[] signature, int[] params, int[] argOids) {
        for (int i = 0; i < argOids.length; i++) {
            int declared = declaredAt(signature, params, i);
            if (declared == ANYRANGE || declared == ANYMULTIRANGE
                    || declared == ANYCOMPATIBLERANGE || declared == ANYCOMPATIBLEMULTIRANGE) {
                if (BuiltinCallTypes.categoryOf(argOids[i]) != RANGE) return false;
                continue;
            }
            if (declared == ANYARRAY || declared == ANYCOMPATIBLEARRAY) {
                if (BuiltinCallTypes.categoryOf(argOids[i]) != ARRAY) return false;
                continue;
            }
            if (BuiltinCallTypes.isPolymorphic(declared)) continue;
            if (!BuiltinCallTypes.reaches(argOids[i], declared)) return false;
        }
        return true;
    }
}
