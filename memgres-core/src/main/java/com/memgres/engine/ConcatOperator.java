package com.memgres.engine;

/**
 * Which concatenation a {@code ||} means, worked out from the types it was written with.
 *
 * <p>PostgreSQL declares eleven of them and no more. Two of those take a text on one side and
 * anything that is not an array on the other, which is what makes {@code 'x' || 42} a string and
 * {@code 42 || now()} nothing at all: neither side of the second is a text, so there is no
 * operator to reach. memgres resolved nothing — it read the two values, decided from their shapes
 * whether they looked like arrays or JSON, and ran them together as strings otherwise. So it
 * answered for 772 pairs of types PostgreSQL has no operator for, refused a few it does, and
 * chose one for itself where PostgreSQL says it cannot choose.
 *
 * <p>The choice follows PostgreSQL's own rule for an operator's arguments: keep the candidates
 * both operands reach, then those matching the most types exactly, then those holding the
 * preferred type of an operand's category. One survivor is the operator; more than one is a call
 * that cannot be chosen; none is a call that does not exist.
 */
final class ConcatOperator {

    private ConcatOperator() {
    }

    // The polymorphic types the declarations are written over.
    private static final int ANYNONARRAY = 2776;
    private static final int ANYCOMPATIBLE = 5077;
    private static final int ANYCOMPATIBLEARRAY = 5078;

    private static final int TEXT = 25;
    private static final int BYTEA = 17;
    private static final int VARBIT = 1562;
    private static final int TSVECTOR = 3614;
    private static final int TSQUERY = 3615;
    private static final int JSONB = 3802;

    /**
     * Every {@code ||} PostgreSQL declares, as left, right and result type. Read off the reference
     * server; there are eleven and the list is closed.
     */
    private static final int[][] DECLARED = {
        {BYTEA, BYTEA, BYTEA},
        {TEXT, TEXT, TEXT},
        {TEXT, ANYNONARRAY, TEXT},
        {VARBIT, VARBIT, VARBIT},
        {ANYNONARRAY, TEXT, TEXT},
        {TSVECTOR, TSVECTOR, TSVECTOR},
        {TSQUERY, TSQUERY, TSQUERY},
        {JSONB, JSONB, JSONB},
        {ANYCOMPATIBLE, ANYCOMPATIBLEARRAY, ANYCOMPATIBLEARRAY},
        {ANYCOMPATIBLEARRAY, ANYCOMPATIBLE, ANYCOMPATIBLEARRAY},
        {ANYCOMPATIBLEARRAY, ANYCOMPATIBLEARRAY, ANYCOMPATIBLEARRAY},
    };

    /** What resolving a written pair of types came to. */
    enum Outcome {
        /** Both operands are read as text and run together. */
        TEXT_CONCAT,
        /** Both operands are of one type that concatenates with itself. */
        SAME_TYPE,
        /** One or both operands are arrays, and the result is an array. */
        ARRAY,
        /** More than one operator survives, which PostgreSQL refuses rather than choosing. */
        AMBIGUOUS,
        /** No operator takes this pair. */
        NONE,
        /** A type this rule does not record, so it says nothing about the pair. */
        UNDECIDED,
    }

    /**
     * Which operator a {@code ||} between these two types means. An operand whose type the
     * statement has not settled is 0, and a pair with one of those is left undecided — the same
     * benefit of the doubt every other resolution rule here gives.
     */
    static Resolution resolve(int leftOid, int rightOid) {
        if (leftOid == 0 && rightOid == 0) return UNDECIDED;
        // A type neither PostgreSQL's categories nor its arrays account for is one memgres added,
        // and a table of PostgreSQL's operators is no evidence about it.
        if (!recorded(leftOid) || !recorded(rightOid)) return UNDECIDED;

        java.util.List<int[]> candidates = new java.util.ArrayList<int[]>();
        for (int i = 0; i < DECLARED.length; i++) {
            int[] op = DECLARED[i];
            if (reaches(leftOid, op[0], rightOid, op[1]) && reaches(rightOid, op[1], leftOid, op[0])) {
                candidates.add(op);
            }
        }
        if (candidates.isEmpty()) return NONE;
        if (candidates.size() > 1) candidates = keepMostExact(candidates, leftOid, rightOid);
        if (candidates.size() > 1) candidates = keepMostPreferred(candidates, leftOid, rightOid);
        // An operand the statement has not typed is read last, once the ones it has typed have
        // narrowed the field. A candidate written over "whatever was passed" has nothing to take
        // its type from there, so PostgreSQL drops those first.
        if (candidates.size() > 1 && leftOid == 0) candidates = withoutPolymorphicAt(candidates, 0);
        if (candidates.size() > 1 && rightOid == 0) candidates = withoutPolymorphicAt(candidates, 1);
        // An untyped operand beside an array is read as the array, and both array forms that take
        // one give back that same array -- so whichever runs, the answer is the same and there is
        // nothing to choose. Two candidates that both answer with a text are a different matter:
        // there the choice is real, which is what makes text || "char" a call PostgreSQL refuses.
        if (candidates.size() > 1 && agreeOnResult(candidates)
                && candidates.get(0)[2] == ANYCOMPATIBLEARRAY) {
            candidates = candidates.subList(0, 1);
        }
        if (candidates.size() > 1) return AMBIGUOUS;

        int[] chosen = candidates.get(0);
        if (chosen[2] == ANYCOMPATIBLEARRAY) return new Resolution(Outcome.ARRAY, null);
        if (chosen[2] == TEXT) return new Resolution(Outcome.TEXT_CONCAT, null);
        return new Resolution(Outcome.SAME_TYPE, sameTypeName(chosen[2]));
    }

    /** The name of the type a concatenation of one type reads both its operands as. */
    private static String sameTypeName(int oid) {
        switch (oid) {
            case BYTEA: return "bytea";
            case VARBIT: return "varbit";
            case TSVECTOR: return "tsvector";
            case TSQUERY: return "tsquery";
            case JSONB: return "jsonb";
            default: return null;
        }
    }

    /** What a written pair of types resolved to, and the type it reads both operands as. */
    static final class Resolution {
        final Outcome outcome;
        /** The type both operands are read as, where the operator takes one type; else null. */
        final String sameType;

        Resolution(Outcome outcome, String sameType) {
            this.outcome = outcome;
            this.sameType = sameType;
        }
    }

    private static final Resolution UNDECIDED = new Resolution(Outcome.UNDECIDED, null);
    private static final Resolution NONE = new Resolution(Outcome.NONE, null);
    private static final Resolution AMBIGUOUS = new Resolution(Outcome.AMBIGUOUS, null);

    /**
     * Whether an operand of {@code actual} reaches a parameter declared {@code declared}. The
     * polymorphic ones are decided against the other operand: an array pairs with its own element
     * type, and two arrays with each other.
     */
    private static boolean reaches(int actual, int declared, int otherActual, int otherDeclared) {
        if (actual == 0) return true;   // an operand the statement has not typed reaches anything
        if (declared == ANYNONARRAY) return !isArray(actual) && !isPseudo(actual);
        if (declared == ANYCOMPATIBLEARRAY) return isArray(actual);
        if (declared == ANYCOMPATIBLE) {
            // The one that is not the array must share a common type with the array's elements,
            // which two types of different categories never do.
            if (otherDeclared != ANYCOMPATIBLEARRAY || otherActual == 0) return !isArray(actual);
            return sharesACommonType(actual, elementOf(otherActual));
        }
        return BuiltinCallTypes.reaches(actual, declared);
    }

    /** Candidates matching the most operands exactly; an untyped operand matches nothing. */
    private static java.util.List<int[]> keepMostExact(java.util.List<int[]> candidates,
                                                      int leftOid, int rightOid) {
        return keepBest(candidates, leftOid, rightOid, false);
    }

    /**
     * Candidates matching the most operands exactly or on the preferred type of that operand's
     * category. A category with no preferred type of its own — the one PostgreSQL keeps for its
     * internal types, which is where {@code "char"} lives — settles nothing here, which is why a
     * {@code "char"} beside a string is a choice PostgreSQL will not make.
     */
    private static java.util.List<int[]> keepMostPreferred(java.util.List<int[]> candidates,
                                                           int leftOid, int rightOid) {
        return keepBest(candidates, leftOid, rightOid, true);
    }

    private static java.util.List<int[]> keepBest(java.util.List<int[]> candidates,
                                                  int leftOid, int rightOid, boolean orPreferred) {
        int best = -1;
        java.util.List<int[]> kept = new java.util.ArrayList<int[]>();
        for (int c = 0; c < candidates.size(); c++) {
            int[] op = candidates.get(c);
            int score = scoreOf(leftOid, op[0], orPreferred) + scoreOf(rightOid, op[1], orPreferred);
            if (score > best) {
                best = score;
                kept.clear();
            }
            if (score == best) kept.add(op);
        }
        return kept;
    }

    /**
     * The candidates naming a real type at {@code position}, or all of them when none does — a
     * position every candidate leaves open is not one that can narrow the field.
     */
    private static java.util.List<int[]> withoutPolymorphicAt(java.util.List<int[]> candidates,
                                                              int position) {
        java.util.List<int[]> named = new java.util.ArrayList<int[]>();
        for (int c = 0; c < candidates.size(); c++) {
            int declared = candidates.get(c)[position];
            if (declared != ANYNONARRAY && declared != ANYCOMPATIBLE
                    && declared != ANYCOMPATIBLEARRAY) {
                named.add(candidates.get(c));
            }
        }
        return named.isEmpty() ? candidates : named;
    }

    private static boolean agreeOnResult(java.util.List<int[]> candidates) {
        int result = candidates.get(0)[2];
        for (int c = 1; c < candidates.size(); c++) {
            if (candidates.get(c)[2] != result) return false;
        }
        return true;
    }

    private static int scoreOf(int actual, int declared, boolean orPreferred) {
        if (actual == 0) return 0;
        if (actual == declared) return 1;
        if (!orPreferred) return 0;
        return BuiltinCallTypes.preferredOf(BuiltinCallTypes.categoryOf(actual)) == declared ? 1 : 0;
    }

    /**
     * Whether two types have a common type to be read as, which PostgreSQL looks for only within
     * one category: a {@code "char"} and a text are of different categories however freely one
     * casts to the other, so an array of one takes no element of the other.
     */
    private static boolean sharesACommonType(int a, int b) {
        if (a == 0 || b == 0) return true;
        if (a == b) return true;
        if (BuiltinCallTypes.categoryOf(a) != BuiltinCallTypes.categoryOf(b)) return false;
        return BuiltinCallTypes.reaches(a, b) || BuiltinCallTypes.reaches(b, a);
    }

    /**
     * Whether this rule records the type at all.
     *
     * <p>A type PostgreSQL has no category for is one memgres added, and a table of PostgreSQL's
     * operators is no evidence about it: hstore declares a concatenation of its own, and refusing
     * it because eleven other operators do not take one would refuse SQL that runs.
     */
    private static boolean recorded(int oid) {
        if (oid == 0) return true;
        char category = BuiltinCallTypes.categoryOf(oid);
        if (category == 'A') return BuiltinCallTypes.categoryOf(elementOf(oid)) != 0;
        return BuiltinCallTypes.recordsCategoryFor(oid);
    }

    private static boolean isArray(int oid) {
        return BuiltinCallTypes.categoryOf(oid) == 'A';
    }

    private static boolean isPseudo(int oid) {
        return BuiltinCallTypes.categoryOf(oid) == 'P';
    }

    private static int elementOf(int arrayOid) {
        DataType array = DataType.fromOid(arrayOid);
        DataType element = array == null ? null : DataType.elementOf(array);
        return element == null ? 0 : element.getOid();
    }
}
