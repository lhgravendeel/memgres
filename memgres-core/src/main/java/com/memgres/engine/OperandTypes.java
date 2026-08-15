package com.memgres.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The type each operand of an operator is read as, and the one type a construct settles its arms on.
 *
 * <p>A stored query holds what parse analysis made of the text, and parse analysis writes down
 * which operator a spelling resolved to. Where an operand's own type is not the one that operator
 * declares, PostgreSQL puts the conversion in front of the operand and keeps it, which is why a
 * definition prints it: {@code character varying} has no operators of its own, so {@code vc = 'b'}
 * is text's equality and comes back as {@code vc::text = 'b'::text}, and a {@code cidr} beside an
 * {@code inet} comes back as {@code cd::inet}.
 *
 * <p>Which entry of {@code pg_operator} a spelling resolves to is decided the way PostgreSQL
 * decides it: gather the entries both operands reach, keep the ones matching most operands
 * exactly, then the ones standing on their category's preferred type, and settle a position
 * written with no type of its own by its category last. A spelling this is left undecided about
 * is one it says nothing about, and the operands are then printed as they were written.
 */
final class OperandTypes {

    private OperandTypes() {
    }

    /** PostgreSQL's unknown: what a written string constant has before anything types it. */
    static final int UNKNOWN = 705;

    /** The rows of pg_operator by spelling, so a spelling the table has none of says nothing. */
    private static final Map<String, List<Object[]>> BY_NAME = index();

    private static Map<String, List<Object[]>> index() {
        Map<String, List<Object[]>> byName = new HashMap<String, List<Object[]>>();
        for (Object[] row : PgOperatorTable.OPERATORS) {
            String name = (String) row[0];
            List<Object[]> rows = byName.get(name);
            if (rows == null) {
                rows = new ArrayList<Object[]>();
                byName.put(name, rows);
            }
            rows.add(row);
        }
        return byName;
    }

    /**
     * The two types the operator this spelling resolves to reads its operands as, or null where
     * the spelling, the operand types or the choice between the entries settles nothing.
     *
     * @param left  the left operand's type OID, {@link #UNKNOWN} when untyped, 0 when unjudgeable
     * @param right the right operand's type OID on the same terms
     */
    static int[] forOperator(String name, int left, int right) {
        if (name == null || left <= 0 || right <= 0) return null;
        List<Object[]> rows = BY_NAME.get(name);
        if (rows == null) return null;
        // PostgreSQL looks for an entry of exactly these two types before it weighs anything,
        // reading an operand written with no type of its own as the other operand's type. That is
        // what keeps a timestamp compared with a written date a timestamp comparison, rather than
        // letting the preferred type of the category pull it to timestamptz.
        int exactLeft = left == UNKNOWN ? right : left;
        int exactRight = right == UNKNOWN ? exactLeft : right;
        if (exactLeft != UNKNOWN && exactRight != UNKNOWN) {
            for (int i = 0; i < rows.size(); i++) {
                Object[] row = rows.get(i);
                if (!"b".equals(row[1])) continue;
                if (((Integer) row[2]).intValue() != exactLeft) continue;
                if (((Integer) row[3]).intValue() != exactRight) continue;
                return new int[]{exactLeft, exactRight};
            }
        }
        List<int[]> candidates = new ArrayList<int[]>();
        for (int i = 0; i < rows.size(); i++) {
            Object[] row = rows.get(i);
            if (!"b".equals(row[1])) continue;
            int declaredLeft = ((Integer) row[2]).intValue();
            int declaredRight = ((Integer) row[3]).intValue();
            if (!fits(left, declaredLeft) || !fits(right, declaredRight)) continue;
            candidates.add(new int[]{declaredLeft, declaredRight});
        }
        return choose(candidates, new int[]{left, right});
    }

    /**
     * The type the operator a prefix spelling resolves to reads its operand as, or 0 where nothing
     * settles the choice.
     */
    static int forPrefixOperator(String name, int operand) {
        if (name == null || operand <= 0) return 0;
        List<Object[]> rows = BY_NAME.get(name);
        if (rows == null) return 0;
        if (operand != UNKNOWN) {
            for (int i = 0; i < rows.size(); i++) {
                Object[] row = rows.get(i);
                if ("l".equals(row[1]) && ((Integer) row[3]).intValue() == operand) return operand;
            }
        }
        List<int[]> candidates = new ArrayList<int[]>();
        for (int i = 0; i < rows.size(); i++) {
            Object[] row = rows.get(i);
            if (!"l".equals(row[1])) continue;
            int declared = ((Integer) row[3]).intValue();
            if (!fits(operand, declared)) continue;
            candidates.add(new int[]{declared});
        }
        int[] chosen = choose(candidates, new int[]{operand});
        return chosen == null ? 0 : chosen[0];
    }

    /**
     * The one type a construct reads every arm as -- what {@code COALESCE}, a {@code CASE} and an
     * array constructor settle on. PostgreSQL keeps the first arm's type and moves to a later one
     * only where the one it holds reaches that type without being asked and the other way round
     * does not; a preferred type is never given up. Everything written with no type of its own is
     * read as text, which is what an array of bare strings settles on.
     *
     * @return 0 where an arm's type is not one this can name, or where two arms are of categories
     *         that have no common type at all
     */
    static int commonType(int[] written) {
        int settled = 0;
        boolean preferred = false;
        for (int i = 0; i < written.length; i++) {
            int next = written[i];
            if (next == 0) return 0;
            if (next == UNKNOWN || next == settled) continue;
            if (settled == 0) {
                settled = next;
                preferred = isPreferred(next);
                continue;
            }
            char held = BuiltinCallTypes.categoryOf(settled);
            char offered = BuiltinCallTypes.categoryOf(next);
            if (held == 0 || offered == 0 || held != offered) return 0;
            if (!preferred && !BuiltinCallTypes.reaches(next, settled)
                    && BuiltinCallTypes.reaches(settled, next)) {
                settled = next;
                preferred = isPreferred(next);
            }
        }
        return settled == 0 ? DataType.TEXT.getOid() : settled;
    }

    /** Whether a type is the one its category resolves an otherwise undecided position to. */
    private static boolean isPreferred(int oid) {
        char category = BuiltinCallTypes.categoryOf(oid);
        return category != 0 && BuiltinCallTypes.preferredOf(category) == oid;
    }

    /** Whether an operand of this type can stand where an entry asks for that one. */
    private static boolean fits(int operand, int declared) {
        if (operand == UNKNOWN) return true;
        if (operand == declared) return true;
        if (BuiltinCallTypes.isPolymorphic(declared)) {
            DataType type = DataType.fromOid(operand);
            boolean array = type != null && DataType.isArrayType(type);
            if (declared == ANYARRAY || declared == ANYCOMPATIBLEARRAY) return array;
            if (declared == ANYNONARRAY) return !array;
            return true;
        }
        return BuiltinCallTypes.reaches(operand, declared);
    }

    private static final int ANYARRAY = 2277;
    private static final int ANYNONARRAY = 2776;
    private static final int ANYCOMPATIBLEARRAY = 5078;

    /**
     * The entry PostgreSQL keeps, or null where more than one survives every rule it applies.
     * Being left with a choice is not a failure to report here: it means the definition is written
     * without the conversion, which is what leaving the operands alone produces.
     */
    private static int[] choose(List<int[]> candidates, int[] written) {
        List<int[]> kept = candidates;
        if (kept.size() > 1) kept = keepBest(kept, written, true);
        if (kept.size() > 1) kept = keepBest(kept, written, false);
        for (int i = 0; kept.size() > 1 && i < written.length; i++) {
            if (written[i] != UNKNOWN) continue;
            kept = withoutPolymorphic(kept, i);
            if (kept.size() > 1) kept = narrowUnsettled(kept, i);
        }
        return kept.size() == 1 ? kept.get(0) : null;
    }

    /**
     * Keeps the entries matching the most written operands exactly, or -- where {@code exact} is
     * false -- standing on the preferred type of the operand's own category.
     */
    private static List<int[]> keepBest(List<int[]> candidates, int[] written, boolean exact) {
        int bestScore = -1;
        List<int[]> best = new ArrayList<int[]>();
        for (int c = 0; c < candidates.size(); c++) {
            int[] declared = candidates.get(c);
            int score = 0;
            for (int i = 0; i < written.length; i++) {
                if (written[i] == UNKNOWN) continue;
                if (exact) {
                    if (written[i] == declared[i]) score++;
                } else {
                    char category = BuiltinCallTypes.categoryOf(written[i]);
                    if (category != 0 && BuiltinCallTypes.preferredOf(category) == declared[i]) {
                        score++;
                    }
                }
            }
            if (score > bestScore) {
                bestScore = score;
                best.clear();
                best.add(declared);
            } else if (score == bestScore) {
                best.add(declared);
            }
        }
        return best;
    }

    /**
     * An entry written over "whatever was passed" cannot take an operand that has not said what it
     * is, so PostgreSQL drops those before it looks at categories.
     */
    private static List<int[]> withoutPolymorphic(List<int[]> candidates, int position) {
        List<int[]> kept = new ArrayList<int[]>();
        for (int c = 0; c < candidates.size(); c++) {
            if (!BuiltinCallTypes.isPolymorphic(candidates.get(c)[position])) {
                kept.add(candidates.get(c));
            }
        }
        return kept.isEmpty() ? candidates : kept;
    }

    /**
     * Settles one operand written with no type of its own the way PostgreSQL does: the string
     * category if any entry takes one there, otherwise the one category the entries agree on, and
     * within it the category's preferred type where an entry offers it.
     */
    private static List<int[]> narrowUnsettled(List<int[]> candidates, int position) {
        char selected = 0;
        boolean mixed = false;
        for (int c = 0; c < candidates.size(); c++) {
            char category = BuiltinCallTypes.categoryOf(candidates.get(c)[position]);
            if (category == 0 || BuiltinCallTypes.isPolymorphic(candidates.get(c)[position])) {
                return candidates;
            }
            if (category == STRING) {
                selected = category;
                mixed = false;
                break;
            }
            if (selected == 0) selected = category;
            else if (selected != category) mixed = true;
        }
        if (selected == 0 || mixed) return candidates;

        List<int[]> kept = new ArrayList<int[]>();
        for (int c = 0; c < candidates.size(); c++) {
            if (BuiltinCallTypes.categoryOf(candidates.get(c)[position]) == selected) {
                kept.add(candidates.get(c));
            }
        }
        if (kept.isEmpty()) return candidates;
        int preferred = BuiltinCallTypes.preferredOf(selected);
        if (preferred == 0) return kept;
        List<int[]> onPreferred = new ArrayList<int[]>();
        for (int c = 0; c < kept.size(); c++) {
            if (kept.get(c)[position] == preferred) onPreferred.add(kept.get(c));
        }
        return onPreferred.isEmpty() ? kept : onPreferred;
    }

    /** pg_type's category letter for the string types, which wins every conflict of categories. */
    private static final char STRING = 'S';
}
