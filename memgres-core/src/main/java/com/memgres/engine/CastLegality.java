package com.memgres.engine;

import java.util.HashSet;
import java.util.Set;

/**
 * Whether a cast exists at all.
 *
 * <p>A cast used to be "render the source to text and feed the text to the target's input
 * function", so whether {@code x::T} was allowed depended on whether the text happened to parse.
 * That let through 419 of the 650 pairs PostgreSQL refuses — {@code true::int8} answered 1,
 * {@code '(1,1)'::point::money} answered a price — and where the text did not parse it reported
 * 22P02 rather than the 42846 PostgreSQL raises for a conversion that does not exist.
 *
 * <p>PostgreSQL decides this from {@code pg_cast} and two general rules: a type always casts to
 * itself, and an <em>explicit</em> cast may go through the type's own text form when either side
 * is a string type. Everything else needs a registered cast.
 *
 * <p>This is deliberately one-sided. It refuses only a pair it can positively say PostgreSQL has
 * no path for — both sides resolved to base types this engine models, neither a string type,
 * neither polymorphic — and leaves every pair it is unsure of to the conversion that follows.
 */
final class CastLegality {

    private CastLegality() {
    }

    /** The registered pairs, by {@code source * 100000 + target}. */
    private static final Set<Long> REGISTERED = new HashSet<Long>();

    static {
        for (Object[] row : PgCastTable.CASTS) {
            REGISTERED.add(key((Integer) row[0], (Integer) row[1]));
        }
    }

    private static long key(int source, int target) {
        return ((long) source << 32) | (target & 0xFFFFFFFFL);
    }

    /**
     * PostgreSQL's string category. An explicit cast to or from one of these is allowed through
     * the type's own I/O functions whether or not pg_cast has a row for it.
     */
    private static boolean isStringType(DataType t) {
        switch (t) {
            case TEXT: case VARCHAR: case CHAR: case NAME: case INTERNAL_CHAR:
                return true;
            default:
                return false;
        }
    }

    /**
     * Types this rule will not judge: their values are carried as text, or their identity depends
     * on a modifier or a user declaration, so a missing pg_cast row proves nothing about them.
     */
    private static boolean isUnjudged(DataType t) {
        if (t == null) return true;
        if (DataType.isArrayType(t)) return true;
        switch (t) {
            case ENUM: case RECORD: case VOID: case ANYARRAY:
                return true;
            // hstore is an extension type: its casts are registered by the extension, not by the
            // core catalogue this table copies, so a missing row here says nothing about whether
            // PostgreSQL has the cast. It has hstore -> json and hstore -> jsonb, for instance.
            case HSTORE: case HSTORE_ARRAY:
                return true;
            default:
                return false;
        }
    }

    /**
     * The complaint PostgreSQL raises for a cast that does not exist, or null when this rule
     * cannot say the cast is missing.
     */
    static MemgresException refusalFor(DataType source, DataType target) {
        if (isUnjudged(source) || isUnjudged(target)) return null;
        if (source == target) return null;
        if (isStringType(source) || isStringType(target)) return null;
        if (REGISTERED.contains(key(source.getOid(), target.getOid()))) return null;
        // A serial is an integer wearing a default; the pair is the integer's.
        if (normalise(source) != source || normalise(target) != target) {
            return refusalFor(normalise(source), normalise(target));
        }
        return new MemgresException("cannot cast type " + displayName(source) + " to "
                + displayName(target), "42846");
    }

    /** serial and its siblings are not types of their own as far as a cast is concerned. */
    private static DataType normalise(DataType t) {
        switch (t) {
            case SERIAL: return DataType.INTEGER;
            case BIGSERIAL: return DataType.BIGINT;
            case SMALLSERIAL: return DataType.SMALLINT;
            default: return t;
        }
    }

    private static String displayName(DataType t) {
        switch (t) {
            case INTEGER: return "integer";
            case SMALLINT: return "smallint";
            case BIGINT: return "bigint";
            case REAL: return "real";
            case DOUBLE_PRECISION: return "double precision";
            case BOOLEAN: return "boolean";
            case TIMESTAMPTZ: return "timestamp with time zone";
            case TIMETZ: return "time with time zone";
            case TIMESTAMP: return "timestamp without time zone";
            case TIME: return "time without time zone";
            default: return t.getPgName();
        }
    }
}
