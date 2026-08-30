package com.memgres.engine;

/**
 * Definition-time checking of sequence options, for both CREATE and ALTER SEQUENCE.
 *
 * <p>The checks are ordered the way PostgreSQL orders them, because the option a statement is
 * rejected for is observable: {@code CACHE 0 MINVALUE 10 MAXVALUE 5} is reported as the MINVALUE
 * problem, not the CACHE one. Defaults are resolved before the crosschecks for the same reason —
 * an unstated MAXVALUE is {@code -1} for a descending sequence, which is what makes
 * {@code INCREMENT -1 START 5} a START-above-MAXVALUE error rather than an accepted definition.
 */
final class DdlSequenceValidator {

    private DdlSequenceValidator() {
    }

    /** Resolved sequence parameters, ready to be applied to a {@link Sequence}. */
    static final class Params {
        String dataType;
        long incrementBy;
        long minValue;
        long maxValue;
        long startWith;
        long lastValue;
        boolean isCalled;
        int cache;
    }

    /** Normalise an {@code AS} type name, rejecting anything a sequence cannot be. */
    static String normalizeType(String asType) {
        if (asType == null) return null;
        String t = asType.toLowerCase(java.util.Locale.ROOT).trim();
        if (t.equals("smallint") || t.equals("int2")) return "smallint";
        if (t.equals("int") || t.equals("integer") || t.equals("int4")) return "integer";
        if (t.equals("bigint") || t.equals("int8")) return "bigint";
        throw PgErrors.invalidParameter("sequence type must be smallint, integer, or bigint");
    }

    static long typeMin(String type) {
        if ("smallint".equals(type)) return Short.MIN_VALUE;
        if ("integer".equals(type)) return Integer.MIN_VALUE;
        return Long.MIN_VALUE;
    }

    static long typeMax(String type) {
        if ("smallint".equals(type)) return Short.MAX_VALUE;
        if ("integer".equals(type)) return Integer.MAX_VALUE;
        return Long.MAX_VALUE;
    }

    /**
     * Resolve and check the options of a CREATE SEQUENCE.
     *
     * @param cache {@code null} when the statement said nothing about CACHE
     */
    static Params forCreate(String asType, Long increment, Long minValue, Long maxValue,
                            Long startWith, Integer cache) {
        Params p = new Params();
        p.dataType = asType != null ? normalizeType(asType) : "bigint";
        long tMin = typeMin(p.dataType), tMax = typeMax(p.dataType);

        p.incrementBy = increment != null ? increment : 1L;
        checkIncrement(p.incrementBy);

        p.maxValue = maxValue != null ? maxValue : (p.incrementBy > 0 ? tMax : -1L);
        checkMax(p.maxValue, tMin, tMax, p.dataType);
        p.minValue = minValue != null ? minValue : (p.incrementBy > 0 ? 1L : tMin);
        checkMin(p.minValue, tMin, tMax, p.dataType);
        checkMinBelowMax(p.minValue, p.maxValue);

        p.startWith = startWith != null ? startWith : (p.incrementBy > 0 ? p.minValue : p.maxValue);
        checkStart(p.startWith, p.minValue, p.maxValue);

        p.lastValue = p.startWith;
        p.isCalled = false;
        checkRestart(p.lastValue, p.minValue, p.maxValue);

        p.cache = cache != null ? checkCache(cache) : 1;
        return p;
    }

    /**
     * Resolve and check the options of an ALTER SEQUENCE against what the sequence already is.
     * Unstated options keep their present value, except that changing the data type also moves a
     * MIN/MAX that was still the old type's limit — PostgreSQL leaves explicitly chosen bounds alone.
     */
    static Params forAlter(Sequence seq, String asType, Long increment, Long minValue, Long maxValue,
                           Long startWith, boolean restart, Long restartWith, Integer cache) {
        Params p = new Params();
        boolean resetMax = false, resetMin = false;
        if (asType != null) {
            String newType = normalizeType(asType);
            String oldType = seq.getDataType();
            resetMax = seq.getMaxValue() == typeMax(oldType);
            resetMin = seq.getMinValue() == typeMin(oldType);
            p.dataType = newType;
        } else {
            p.dataType = seq.getDataType();
        }
        long tMin = typeMin(p.dataType), tMax = typeMax(p.dataType);

        p.incrementBy = increment != null ? increment : seq.getIncrementBy();
        checkIncrement(p.incrementBy);

        if (maxValue != null) p.maxValue = maxValue;
        else if (resetMax) p.maxValue = p.incrementBy > 0 ? tMax : -1L;
        else p.maxValue = seq.getMaxValue();
        checkMax(p.maxValue, tMin, tMax, p.dataType);

        if (minValue != null) p.minValue = minValue;
        else if (resetMin) p.minValue = p.incrementBy > 0 ? 1L : tMin;
        else p.minValue = seq.getMinValue();
        checkMin(p.minValue, tMin, tMax, p.dataType);
        checkMinBelowMax(p.minValue, p.maxValue);

        p.startWith = startWith != null ? startWith : seq.getStartWith();
        checkStart(p.startWith, p.minValue, p.maxValue);

        if (restart) {
            p.lastValue = restartWith != null ? restartWith : p.startWith;
            p.isCalled = false;
        } else {
            p.lastValue = seq.currValRaw();
            p.isCalled = seq.isCalled();
        }
        checkRestart(p.lastValue, p.minValue, p.maxValue);

        p.cache = cache != null ? checkCache(cache) : seq.getCache();
        return p;
    }

    /** Write resolved parameters onto the sequence. Only called once every check has passed. */
    static void apply(Sequence seq, Params p) {
        seq.setDataType(p.dataType);
        seq.setIncrementBy(p.incrementBy);
        seq.setMinValue(p.minValue);
        seq.setMaxValue(p.maxValue);
        seq.setStartWith(p.startWith);
        seq.setCache(p.cache);
        seq.setCurrentValue(p.lastValue, p.isCalled);
    }

    private static void checkIncrement(long increment) {
        if (increment == 0) {
            throw PgErrors.invalidParameter("INCREMENT must not be zero");
        }
    }

    private static void checkMax(long max, long tMin, long tMax, String type) {
        if (max < tMin || max > tMax) {
            throw PgErrors.invalidParameter(
                    "MAXVALUE (" + max + ") is out of range for sequence data type " + type);
        }
    }

    private static void checkMin(long min, long tMin, long tMax, String type) {
        if (min < tMin || min > tMax) {
            throw PgErrors.invalidParameter(
                    "MINVALUE (" + min + ") is out of range for sequence data type " + type);
        }
    }

    private static void checkMinBelowMax(long min, long max) {
        if (min >= max) {
            throw PgErrors.invalidParameter(
                    "MINVALUE (" + min + ") must be less than MAXVALUE (" + max + ")");
        }
    }

    private static void checkStart(long start, long min, long max) {
        if (start < min) {
            throw PgErrors.invalidParameter(
                    "START value (" + start + ") cannot be less than MINVALUE (" + min + ")");
        }
        if (start > max) {
            throw PgErrors.invalidParameter(
                    "START value (" + start + ") cannot be greater than MAXVALUE (" + max + ")");
        }
    }

    private static void checkRestart(long last, long min, long max) {
        if (last < min) {
            throw PgErrors.invalidParameter(
                    "RESTART value (" + last + ") cannot be less than MINVALUE (" + min + ")");
        }
        if (last > max) {
            throw PgErrors.invalidParameter(
                    "RESTART value (" + last + ") cannot be greater than MAXVALUE (" + max + ")");
        }
    }

    private static int checkCache(int cache) {
        if (cache <= 0) {
            throw PgErrors.invalidParameter("CACHE (" + cache + ") must be greater than zero");
        }
        return cache;
    }
}
