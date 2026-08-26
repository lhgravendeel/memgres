package com.memgres.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Whether an operator exists for the types it was written over.
 *
 * <p>Operators used to be chosen by looking at the runtime classes of the two values, so an
 * operator PostgreSQL has no entry for still ran: {@code 1 || 2} concatenated, {@code money + 1}
 * added, {@code date LIKE '2020%'} matched, {@code @ date} answered the date. An application whose
 * query has a type error learns nothing, because there is nothing for the wrong pair to fail
 * against.
 *
 * <p>PostgreSQL resolves an operator from {@code pg_operator}: gather every operator of that
 * spelling, keep the ones the operands can reach by implicit conversion, and then decide. None
 * reachable is {@code 42883 operator does not exist}; more than one with nothing to choose between
 * them is {@code 42725 operator is not unique}. This reads the same table the catalogs publish.
 *
 * <p>Like the cast rule, this is one-sided: it refuses only a pair it can positively say has no
 * entry — both operand types resolved to types the table knows, the spelling itself present in the
 * table — and says nothing about everything else, so an operator memgres implements over a type
 * PostgreSQL does not model is left alone.
 */
final class OperatorResolution {

    private OperatorResolution() {
    }

    /** PostgreSQL's {@code unknown}: what an unadorned literal has before anything types it. */
    static final int UNKNOWN = 705;

    private static final int ANYELEMENT = 2283;
    private static final int ANYARRAY = 2277;
    private static final int ANYNONARRAY = 2776;
    private static final int ANYCOMPATIBLE = 5077;
    private static final int ANYCOMPATIBLEARRAY = 5078;

    /** Rows by operator spelling, so a spelling the table does not have can be left alone. */
    private static final Map<String, List<Object[]>> BY_NAME = new HashMap<String, List<Object[]>>();

    /** Every type that appears as an operand anywhere, for judging whether a type is modelled. */
    private static final Set<Integer> OPERAND_TYPES = new HashSet<Integer>();

    static {
        for (Object[] row : PgOperatorTable.OPERATORS) {
            String name = (String) row[0];
            List<Object[]> rows = BY_NAME.get(name);
            if (rows == null) {
                rows = new ArrayList<Object[]>();
                BY_NAME.put(name, rows);
            }
            rows.add(row);
            OPERAND_TYPES.add((Integer) row[2]);
            OPERAND_TYPES.add((Integer) row[3]);
        }
        // PostgreSQL declares no operator whose own operand is xml -- pg_operator names it
        // nowhere -- so the table's silence about it is a judgement and not an absence. Left
        // unjudgeable, xml was compared as the text it prints as and so had an equality and an
        // ordering it does not have; a GROUP BY or a DISTINCT over an xml column ran where a
        // real server refuses the query. It still reaches the polymorphic rows, which is how
        // 'a' || '<a/>'::xml resolves.
        OPERAND_TYPES.add(Integer.valueOf(DataType.XML.getOid()));
    }

    private static boolean isPolymorphic(int oid) {
        return oid == ANYELEMENT || oid == ANYARRAY || oid == ANYNONARRAY
                || oid == ANYCOMPATIBLE || oid == ANYCOMPATIBLEARRAY;
    }

    /** Whether an operand of this type can stand where the table asks for that one. */
    private static boolean fits(int operand, int declared) {
        if (operand == UNKNOWN) return true;  // an untyped literal reads as whatever is wanted
        if (operand == declared) return true;
        if (isPolymorphic(declared)) {
            boolean array = DataType.fromOid(operand) != null
                    && DataType.isArrayType(DataType.fromOid(operand));
            if (declared == ANYARRAY || declared == ANYCOMPATIBLEARRAY) return array;
            if (declared == ANYNONARRAY) return !array;
            return true;
        }
        return BuiltinCallTypes.reaches(operand, declared);
    }

    /**
     * The complaint PostgreSQL raises for this operator over these operand types, or null when
     * this rule cannot say the operator is missing.
     *
     * @param name  the operator as written, e.g. {@code "+"} or {@code "~~"}
     * @param left  the left operand's type OID, {@link #UNKNOWN} when untyped, 0 when unjudgeable
     * @param right the right operand's type OID, or 0 for a prefix operator's absent left operand
     */
    static MemgresException refusalFor(String name, int left, int right, String leftName,
                                       String rightName) {
        List<Object[]> rows = BY_NAME.get(name);
        if (rows == null || rows.isEmpty()) return null;   // a spelling this table does not model
        boolean prefix = left == 0;
        if (!judgeable(left) && !prefix) return null;
        if (!judgeable(right)) return null;

        List<Object[]> candidates = new ArrayList<Object[]>();
        for (Object[] row : rows) {
            boolean rowPrefix = "l".equals(row[1]);
            if (rowPrefix != prefix) continue;
            int rowLeft = (Integer) row[2];
            int rowRight = (Integer) row[3];
            if (!prefix && !fits(left, rowLeft)) continue;
            if (!fits(right, rowRight)) continue;
            // Two polymorphic operands have to unify: int4[] = int8[] has no operator even though
            // anyarray = anyarray accepts an array on each side.
            if (!prefix && isPolymorphic(rowLeft) && isPolymorphic(rowRight)
                    && left != UNKNOWN && right != UNKNOWN
                    && !unifies(left, rowLeft, right, rowRight)) {
                continue;
            }
            candidates.add(row);
        }

        if (candidates.isEmpty()) {
            return new MemgresException("operator does not exist: "
                    + (prefix ? "" : displayName(left, leftName) + " ") + name
                    + " " + displayName(right, rightName), "42883");
        }
        // Nothing typed either side. PostgreSQL first tries reading the unknowns as the preferred
        // type of the category every candidate accepts there; for the string category that is
        // text, which is what makes 'abc' LIKE 'a%' resolve rather than being ambiguous. Only when
        // no candidate takes text on both sides is there genuinely nothing to choose between them.
        boolean bothUnknown = (prefix || left == UNKNOWN) && right == UNKNOWN;
        if (bothUnknown && !hasTextCandidate(candidates, prefix)
                && distinctShapes(candidates, prefix) > 1) {
            return new MemgresException("operator is not unique: "
                    + (prefix ? "" : "unknown ") + name + " unknown", "42725");
        }
        return null;
    }

    /**
     * Whether two polymorphic positions can be filled by these operand types at once. Each side
     * contributes an element type — the array's element where the position asks for an array, the
     * operand itself otherwise — and the two have to meet.
     *
     * <p>Which meeting is allowed depends on the family. The anyelement family holds every position
     * to the same type, so int4[] = int8[] has no operator. The anycompatible family settles on a
     * common type instead, which is what lets a text[] take a bpchar appended to it.
     */
    private static boolean unifies(int left, int rowLeft, int right, int rowRight) {
        int leftElement = elementFor(left, rowLeft);
        int rightElement = elementFor(right, rowRight);
        if (leftElement == 0 || rightElement == 0) return true;   // nothing this can check
        if (leftElement == rightElement) return true;
        boolean compatible = rowLeft == ANYCOMPATIBLE || rowLeft == ANYCOMPATIBLEARRAY
                || rowRight == ANYCOMPATIBLE || rowRight == ANYCOMPATIBLEARRAY;
        return compatible && (BuiltinCallTypes.reaches(leftElement, rightElement)
                || BuiltinCallTypes.reaches(rightElement, leftElement));
    }

    /** What an operand contributes to a polymorphic position: its element type, or itself. */
    private static int elementFor(int oid, int declared) {
        if (declared != ANYARRAY && declared != ANYCOMPATIBLEARRAY) return oid;
        DataType array = DataType.fromOid(oid);
        DataType element = array == null ? null : DataType.elementOf(array);
        return element == null ? 0 : element.getOid();
    }

    /** Whether some candidate takes text where the query wrote nothing typed. */
    private static boolean hasTextCandidate(List<Object[]> candidates, boolean prefix) {
        for (Object[] row : candidates) {
            boolean rightIsText = ((Integer) row[3]).intValue() == DataType.TEXT.getOid();
            boolean leftIsText = prefix || ((Integer) row[2]).intValue() == DataType.TEXT.getOid();
            if (leftIsText && rightIsText) return true;
        }
        return false;
    }

    private static int distinctShapes(List<Object[]> candidates, boolean prefix) {
        Set<String> shapes = new HashSet<String>();
        for (Object[] row : candidates) {
            shapes.add((prefix ? "" : row[2] + ":") + row[3]);
        }
        return shapes.size();
    }

    /**
     * The complaint PostgreSQL raises for sorting on a value of this type, or null where the
     * type can be sorted at all.
     *
     * <p>A sort is done by an ordering operator, and {@code <} is the one every sortable type
     * has. xid and cid have equality and nothing else -- PostgreSQL registers no ordering over
     * either -- so {@code ORDER BY xmin} is not a sort but a query that cannot be planned, and
     * memgres sorted the rows by whatever the values happened to compare as.
     *
     * <p>Read off the same table the operators themselves are read from, so a type it does not
     * model is left alone by {@link #refusalFor}'s own judgement rather than by a second list
     * that would have to be kept in step with it.
     */
    static MemgresException noOrderingFor(DataType type) {
        if (type == null) return null;
        int oid = type.getOid();
        if (refusalFor("<", oid, oid, null, null) == null) return null;
        return new MemgresException(
                "could not identify an ordering operator for type " + displayName(oid, null)
                        + "\n  Hint: Use an explicit ordering operator or modify the query.",
                "42883");
    }

    /**
     * The complaint PostgreSQL raises for putting values of this type in a set -- for DISTINCT,
     * GROUP BY, PARTITION BY and the set operations -- or null where the type can be compared at
     * all.
     *
     * <p>Each of those gathers equal values together, and so needs an equality operator. json has
     * none: two documents are two texts, and PostgreSQL will not say which of two spellings of one
     * document is which, so it refuses the query rather than answering by the text. point is the
     * other way round from xid -- it has ordering operators and no equality.
     *
     * <p>Read off the operator table for the same reason {@link #noOrderingFor} is: a type it does
     * not model is left alone rather than listed a second time somewhere else.
     */
    static MemgresException noEqualityFor(DataType type) {
        if (type == null) return null;
        int oid = type.getOid();
        // xml is the one type PostgreSQL gives no operator of any kind, so it appears nowhere in
        // the table and the table's silence about it says nothing either way. Every other type
        // with no equality is read off the table.
        if (refusalFor("=", oid, oid, null, null) == null) return null;
        return new MemgresException("could not identify an equality operator for type "
                + type.toRegtypeDisplay(), "42883");
    }

    /**
     * Whether a missing entry for this type proves anything. A type the table never mentions as an
     * operand is one it does not model, and its absence says nothing.
     */
    private static boolean judgeable(int oid) {
        if (oid == UNKNOWN) return true;
        if (oid == 0) return false;
        if (OPERAND_TYPES.contains(Integer.valueOf(oid))) return true;
        // An array type is modelled through the polymorphic rows rather than by its own oid.
        DataType t = DataType.fromOid(oid);
        return t != null && DataType.isArrayType(t);
    }

    /** The name PostgreSQL puts in the message for this operand. */
    private static String displayName(int oid, String written) {
        if (oid == UNKNOWN) return "unknown";
        DataType t = DataType.fromOid(oid);
        if (t == null) return written == null ? "unknown" : written;
        switch (t) {
            case INTEGER: return "integer";
            case SMALLINT: return "smallint";
            case BIGINT: return "bigint";
            case REAL: return "real";
            case DOUBLE_PRECISION: return "double precision";
            case BOOLEAN: return "boolean";
            case TIMESTAMPTZ: return "timestamp with time zone";
            case TIMESTAMP: return "timestamp without time zone";
            case TIMETZ: return "time with time zone";
            case TIME: return "time without time zone";
            case VARCHAR: return "character varying";
            case CHAR: return "character";
            default:
                if (DataType.isArrayType(t)) {
                    DataType element = DataType.elementOf(t);
                    return element == null ? t.getPgName() : displayName(element.getOid(), null) + "[]";
                }
                return t.getPgName();
        }
    }
}
