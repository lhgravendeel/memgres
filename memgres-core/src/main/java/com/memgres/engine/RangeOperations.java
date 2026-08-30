package com.memgres.engine;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/**
 * PG range type support: int4range, int8range, numrange, daterange, tsrange, tstzrange.
 *
 * <p>A range is stored as its canonical text form, so the text has to carry everything the value
 * means: the element type decides how a bound is read, how it compares and how it is written back
 * out. A bound is therefore parsed into the element type's own Java value (a {@link LocalDate} for
 * daterange, a {@link LocalDateTime} for tsrange, and so on) alongside a numeric sort key, and the
 * text form is rendered from that value rather than echoed from the input.
 */
public class RangeOperations {

    /** Element kinds a range can be built over. */
    static final String INT = "int";
    /** int8range's element: discrete like int4range's, but with a wider limit to overflow. */
    static final String INT8 = "int8";
    static final String NUM = "num";
    static final String DATE = "date";
    static final String TS = "ts";
    static final String TSTZ = "tstz";

    /**
     * The name PostgreSQL gives the multirange type it creates alongside a range type: a name that
     * already ends in "range" has that word replaced, and any other name is given the suffix. Every
     * range type has one, so the name is derived from the range's rather than stored beside it.
     */
    public static String multirangeTypeName(String rangeTypeName) {
        return rangeTypeName.endsWith("range")
                ? rangeTypeName.substring(0, rangeTypeName.length() - "range".length()) + "multirange"
                : rangeTypeName + "_multirange";
    }

    /** The element kind of a named range type, or null when the type is not one of PG's. */
    static String elemOfRangeType(String rangeType) {
        if (rangeType == null) return null;
        String t = rangeType.toLowerCase(java.util.Locale.ROOT).trim();
        if (t.endsWith("multirange")) t = t.substring(0, t.length() - "multirange".length()) + "range";
        if (t.equals("int4range")) return INT;
        if (t.equals("int8range")) return INT8;
        if (t.equals("numrange")) return NUM;
        if (t.equals("daterange")) return DATE;
        if (t.equals("tsrange")) return TS;
        if (t.equals("tstzrange")) return TSTZ;
        return null;
    }

    /** A parsed PG range value. */
    public static final class PgRange {
        public final Number lower;
        public final Number upper;
        public final boolean lowerInclusive;
        public final boolean upperInclusive;
        public final boolean empty;
        // Text form of each bound, already normalised to the element type's output syntax
        public final String lowerStr;
        public final String upperStr;
        /** Element kind (see the constants on the enclosing class); null when nothing said. */
        public final String elemType;
        // The bound as a value of the element type: Long, BigDecimal, LocalDate, LocalDateTime,
        // OffsetDateTime, or the strings "infinity"/"-infinity" for the temporal infinities.
        private final Object lowerVal;
        private final Object upperVal;

        public PgRange(
                Number lower,
                Number upper,
                boolean lowerInclusive,
                boolean upperInclusive,
                boolean empty
        ) {
            this(lower, upper, lowerInclusive, upperInclusive, empty, null, null);
        }

        public PgRange(
                Number lower,
                Number upper,
                boolean lowerInclusive,
                boolean upperInclusive,
                boolean empty,
                String lowerStr,
                String upperStr
        ) {
            this(lower, upper, lowerInclusive, upperInclusive, empty, lowerStr, upperStr,
                    null, null, null);
        }

        PgRange(
                Number lower,
                Number upper,
                boolean lowerInclusive,
                boolean upperInclusive,
                boolean empty,
                String lowerStr,
                String upperStr,
                String elemType,
                Object lowerVal,
                Object upperVal
        ) {
            this.lower = lower;
            this.upper = upper;
            this.lowerInclusive = lower == null ? false : lowerInclusive;
            this.upperInclusive = upper == null ? false : upperInclusive;
            this.empty = empty;
            this.lowerStr = lowerStr;
            this.upperStr = upperStr;
            this.elemType = elemType;
            this.lowerVal = lowerVal;
            this.upperVal = upperVal;
        }

        /** Canonical string form: [lower,upper) or empty */
        @Override
        public String toString() {
            if (isEmpty()) return "empty";
            String lo = lowerStr != null ? lowerStr : (lower != null ? lower.toString() : "");
            String hi = upperStr != null ? upperStr : (upper != null ? upper.toString() : "");
            return (lowerInclusive ? "[" : "(") + quoteBound(lo) + ","
                    + quoteBound(hi) + (upperInclusive ? "]" : ")");
        }

        /** Check if this range contains a value. */
        public boolean contains(Number value) {
            if (isEmpty() || value == null) return false;
            if (lower != null) {
                int c = cmpVals(value, lower);
                if (lowerInclusive ? c < 0 : c <= 0) return false;
            }
            if (upper != null) {
                int c = cmpVals(value, upper);
                if (upperInclusive ? c > 0 : c >= 0) return false;
            }
            return true;
        }

        /**
         * Check if this range contains a value of any shape the element type can read. The probe
         * has to be brought onto the same scale the bounds were stored on, or a tsrange compares
         * seconds against a date's day number and answers plausibly but wrongly.
         */
        public Boolean containsValue(Object value) {
            if (value == null) return Boolean.FALSE;
            Number key;
            try {
                key = sortKeyFor(value, elemType);
            } catch (RuntimeException e) {
                return null;
            }
            if (key == null) return null;
            return Boolean.valueOf(contains(key));
        }

        /** Check if this range contains another range. Every range contains the empty range. */
        public boolean containsRange(PgRange other) {
            if (other.isEmpty()) return true;
            if (isEmpty()) return false;
            return cmpLowerBounds(lower, lowerInclusive, other.lower, other.lowerInclusive) <= 0
                && cmpUpperBounds(upper, upperInclusive, other.upper, other.upperInclusive) >= 0;
        }

        /** Check if this range overlaps with another. Empty ranges never overlap. */
        public boolean overlaps(PgRange other) {
            if (isEmpty() || other.isEmpty()) return false;
            return lowerBeforeUpper(lower, lowerInclusive, other.upper, other.upperInclusive)
                && lowerBeforeUpper(other.lower, other.lowerInclusive, upper, upperInclusive);
        }

        public boolean isEmpty() {
            if (empty) return true;
            if (lower == null || upper == null) return false;
            int c = cmpVals(lower, upper);
            return c > 0 || (c == 0 && !(lowerInclusive && upperInclusive));
        }

        /** Get the lower bound (null if unbounded). */
        public Number getLower() {
            return lower;
        }

        /** Get the upper bound (null if unbounded). */
        public Number getUpper() {
            if (upper == null) return null;
            return upper;
        }

        /**
         * The lower bound as a value of the element type — what {@code lower(anyrange)} returns.
         * Falls back to the sort key when nothing typed the range.
         */
        public Object lowerValue() {
            return lowerVal != null ? lowerVal : lower;
        }

        /** The upper bound as a value of the element type — what {@code upper(anyrange)} returns. */
        public Object upperValue() {
            return upperVal != null ? upperVal : upper;
        }

        public Number lower() { return lower; }
        public Number upper() { return upper; }
        public boolean lowerInclusive() { return lowerInclusive; }
        public boolean upperInclusive() { return upperInclusive; }
        public boolean empty() { return empty; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgRange that = (PgRange) o;
            return java.util.Objects.equals(lower, that.lower)
                && java.util.Objects.equals(upper, that.upper)
                && lowerInclusive == that.lowerInclusive
                && upperInclusive == that.upperInclusive
                && empty == that.empty;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(lower, upper, lowerInclusive, upperInclusive, empty);
        }
    }

    // ---- bound values ----

    /** An infinite or not-a-number bound; the ordinary bounds are plain numbers. */
    private static boolean isSpecial(Number n) {
        if (!(n instanceof Double)) return false;
        Double d = (Double) n;
        return d.isInfinite() || d.isNaN();
    }

    /** Convert a bound to BigDecimal for exact comparison (no rounding of numrange bounds). */
    private static BigDecimal big(Number n) {
        if (n instanceof BigDecimal) return (BigDecimal) n;
        if (n instanceof Double || n instanceof Float) return new BigDecimal(n.toString());
        return BigDecimal.valueOf(n.longValue());
    }

    /**
     * Order two bound sort keys. PG orders the specials the way each element type does:
     * -infinity below every value, +infinity above every value, and numeric NaN above them all.
     */
    static int cmpVals(Number a, Number b) {
        boolean sa = isSpecial(a);
        boolean sb = isSpecial(b);
        if (!sa && !sb) return big(a).compareTo(big(b));
        double da = sa ? a.doubleValue() : 0;
        double db = sb ? b.doubleValue() : 0;
        int ra = sa ? rankSpecial(da) : 0;
        int rb = sb ? rankSpecial(db) : 0;
        return Integer.compare(ra, rb);
    }

    private static int rankSpecial(double d) {
        if (Double.isNaN(d)) return 2;
        return d > 0 ? 1 : -1;
    }

    /**
     * Compare two lower bounds: negative if a starts before b.
     * A null bound is -infinity; at equal values an inclusive bound starts earlier.
     */
    private static int cmpLowerBounds(Number aVal, boolean aInc, Number bVal, boolean bInc) {
        if (aVal == null) return bVal == null ? 0 : -1;
        if (bVal == null) return 1;
        int c = cmpVals(aVal, bVal);
        if (c != 0) return c;
        if (aInc == bInc) return 0;
        return aInc ? -1 : 1;
    }

    /**
     * Compare two upper bounds: positive if a ends after b.
     * A null bound is +infinity; at equal values an inclusive bound ends later.
     */
    private static int cmpUpperBounds(Number aVal, boolean aInc, Number bVal, boolean bInc) {
        if (aVal == null) return bVal == null ? 0 : 1;
        if (bVal == null) return -1;
        int c = cmpVals(aVal, bVal);
        if (c != 0) return c;
        if (aInc == bInc) return 0;
        return aInc ? 1 : -1;
    }

    /** True if a lower bound admits at least one point before the given upper bound. */
    private static boolean lowerBeforeUpper(Number lo, boolean loInc, Number hi, boolean hiInc) {
        if (lo == null || hi == null) return true;
        int c = cmpVals(lo, hi);
        if (c < 0) return true;
        if (c > 0) return false;
        return loInc && hiInc;
    }

    /**
     * The sort key a probe value takes under {@code elem}. A range's bounds and the values it is
     * compared against have to be measured the same way — days for daterange, seconds for the
     * timestamp ranges — so this mirrors {@link #parseBound}.
     */
    private static Number sortKeyFor(Object value, String elem) {
        if (value instanceof java.util.List || value instanceof Boolean) return null;
        if (DATE.equals(elem)) {
            if (value instanceof LocalDate) return ((LocalDate) value).toEpochDay();
            if (value instanceof LocalDateTime) return ((LocalDateTime) value).toLocalDate().toEpochDay();
            if (value instanceof OffsetDateTime) {
                return ((OffsetDateTime) value).atZoneSameInstant(TypeCoercion.sessionZone())
                        .toLocalDate().toEpochDay();
            }
            if (value instanceof Number) return (Number) value;
            return dateKey(TypeCoercion.toLocalDateOrBc(value.toString()));
        }
        if (TS.equals(elem)) {
            if (value instanceof Number) return (Number) value;
            return tsKey(TypeCoercion.toLocalDateTimeOrInfinity(value));
        }
        if (TSTZ.equals(elem)) {
            if (value instanceof Number) return (Number) value;
            return tstzKey(value);
        }
        if (value instanceof Number) return (Number) value;
        // Untyped range: keep the historical shapes so nothing that worked stops working.
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toEpochSecond(java.time.ZoneOffset.UTC);
        }
        if (value instanceof LocalDate) return ((LocalDate) value).toEpochDay();
        String s = value.toString().trim();
        Double special = specialNumber(s);
        if (special != null) return special;
        try {
            return Long.valueOf(Long.parseLong(s));
        } catch (NumberFormatException e) {
            try {
                return new BigDecimal(s);
            } catch (NumberFormatException e2) {
                if (s.matches("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}.*")) {
                    return TypeCoercion.toLocalDateTime(s).toEpochSecond(java.time.ZoneOffset.UTC);
                }
                if (s.matches("\\d{4}-\\d{2}-\\d{2}")) return LocalDate.parse(s).toEpochDay();
                return null;
            }
        }
    }

    private static Number dateKey(Object v) {
        if (v instanceof LocalDate) return ((LocalDate) v).toEpochDay();
        return specialOfWord(v.toString());
    }

    private static Number tsKey(Object v) {
        if (v instanceof LocalDateTime) {
            LocalDateTime t = (LocalDateTime) v;
            return BigDecimal.valueOf(t.toEpochSecond(java.time.ZoneOffset.UTC))
                    .add(BigDecimal.valueOf(t.getNano(), 9));
        }
        return specialOfWord(v.toString());
    }

    private static Number tstzKey(Object v) {
        String word = v instanceof String ? ((String) v).trim() : null;
        if (word != null) {
            Double special = specialNumber(word);
            if (special != null) return special;
        }
        OffsetDateTime odt = TypeCoercion.toOffsetDateTime(v, TypeCoercion.sessionZone());
        return BigDecimal.valueOf(odt.toEpochSecond()).add(BigDecimal.valueOf(odt.getNano(), 9));
    }

    private static Double specialOfWord(String s) {
        Double d = specialNumber(s);
        return d == null ? Double.valueOf(0) : d;
    }

    /** The infinity/NaN word a bound may be written as, or null when it is an ordinary value. */
    private static Double specialNumber(String raw) {
        String s = raw.trim().toLowerCase(java.util.Locale.ROOT);
        if (s.equals("infinity") || s.equals("+infinity") || s.equals("inf") || s.equals("+inf")) {
            return Double.valueOf(Double.POSITIVE_INFINITY);
        }
        if (s.equals("-infinity") || s.equals("-inf")) {
            return Double.valueOf(Double.NEGATIVE_INFINITY);
        }
        if (s.equals("nan")) return Double.valueOf(Double.NaN);
        return null;
    }

    /** A parsed bound: its typed value, its sort key and the text it is written back as. */
    private static final class Bound {
        final Object value;
        final Number key;
        final String text;

        Bound(Object value, Number key, String text) {
            this.value = value;
            this.key = key;
            this.text = text;
        }
    }

    private static Bound parseBound(String raw, String elem) {
        String s = stripQuotes(raw);
        Double special = specialNumber(s);
        if (INT.equals(elem) || INT8.equals(elem)) {
            if (special != null) {
                throw new MemgresException(
                        "invalid input syntax for type integer: \"" + s + "\"", "22P02");
            }
            try {
                long v = Long.parseLong(s.trim());
                return new Bound(Long.valueOf(v), Long.valueOf(v), Long.toString(v));
            } catch (NumberFormatException e) {
                throw new MemgresException(
                        "invalid input syntax for type integer: \"" + s + "\"", "22P02");
            }
        }
        if (NUM.equals(elem)) {
            if (special != null) return new Bound(special, special, numSpecialText(special));
            try {
                BigDecimal bd = new BigDecimal(s.trim());
                return new Bound(bd, bd, bd.toPlainString());
            } catch (NumberFormatException e) {
                throw new MemgresException(
                        "invalid input syntax for type numeric: \"" + s + "\"", "22P02");
            }
        }
        if (DATE.equals(elem)) {
            if (special != null) {
                if (Double.isNaN(special)) {
                    throw new MemgresException(
                            "invalid input syntax for type date: \"" + s + "\"", "22007");
                }
                String word = special > 0 ? "infinity" : "-infinity";
                return new Bound(word, special, word);
            }
            LocalDate d = TypeCoercion.toLocalDate(s);
            return new Bound(d, Long.valueOf(d.toEpochDay()), formatDate(d));
        }
        if (TS.equals(elem)) {
            if (special != null) {
                if (Double.isNaN(special)) {
                    throw new MemgresException(
                            "invalid input syntax for type timestamp: \"" + s + "\"", "22007");
                }
                String word = special > 0 ? "infinity" : "-infinity";
                return new Bound(word, special, word);
            }
            LocalDateTime t = TypeCoercion.toLocalDateTime(s);
            return new Bound(t, tsKey(t), formatTimestamp(t));
        }
        if (TSTZ.equals(elem)) {
            if (special != null) {
                if (Double.isNaN(special)) {
                    throw new MemgresException(
                            "invalid input syntax for type timestamp with time zone: \"" + s + "\"",
                            "22007");
                }
                String word = special > 0 ? "infinity" : "-infinity";
                return new Bound(word, special, word);
            }
            OffsetDateTime odt = TypeCoercion.toOffsetDateTime(s, TypeCoercion.sessionZone());
            return new Bound(odt, tstzKey(odt), formatTimestamptz(odt));
        }
        // Nothing said what the element type is: read the bound as whatever it looks like.
        if (special != null) return new Bound(special, special, s.trim());
        try {
            long v = Long.parseLong(s.trim());
            return new Bound(Long.valueOf(v), Long.valueOf(v), Long.toString(v));
        } catch (NumberFormatException e) {
            try {
                BigDecimal bd = new BigDecimal(s.trim());
                return new Bound(bd, bd, s.trim());
            } catch (NumberFormatException e2) {
                throw new MemgresException(
                        "invalid input syntax for type integer: \"" + s + "\"", "22P02");
            }
        }
    }

    private static String numSpecialText(Double d) {
        if (d.isNaN()) return "NaN";
        return d > 0 ? "Infinity" : "-Infinity";
    }

    static String formatDate(LocalDate d) {
        return d.toString();
    }

    /** PG's timestamp output: seconds always, fractional seconds only when there are any. */
    static String formatTimestamp(LocalDateTime t) {
        StringBuilder sb = new StringBuilder(32);
        sb.append(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                t.getYear(), t.getMonthValue(), t.getDayOfMonth(),
                t.getHour(), t.getMinute(), t.getSecond()));
        appendFraction(sb, t.getNano());
        return sb.toString();
    }

    static String formatTimestamptz(OffsetDateTime odt) {
        StringBuilder sb = new StringBuilder(formatTimestamp(odt.toLocalDateTime()));
        int total = odt.getOffset().getTotalSeconds();
        sb.append(total < 0 ? '-' : '+');
        int abs = Math.abs(total);
        sb.append(String.format("%02d", abs / 3600));
        int mins = (abs % 3600) / 60;
        int secs = abs % 60;
        if (mins != 0 || secs != 0) sb.append(String.format(":%02d", mins));
        if (secs != 0) sb.append(String.format(":%02d", secs));
        return sb.toString();
    }

    private static void appendFraction(StringBuilder sb, int nano) {
        if (nano == 0) return;
        String micros = String.format("%06d", nano / 1000);
        int end = micros.length();
        while (end > 1 && micros.charAt(end - 1) == '0') end--;
        sb.append('.').append(micros, 0, end);
    }

    /** PG quotes a bound whose text would otherwise not read back as one token. */
    static String quotedBound(String text) {
        return quoteBound(text);
    }

    private static String quoteBound(String text) {
        if (text == null || text.isEmpty()) return "";
        boolean needs = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c) || c == '"' || c == '\\' || c == ','
                    || c == '(' || c == ')' || c == '[' || c == ']') {
                needs = true;
                break;
            }
        }
        if (!needs) return text;
        StringBuilder sb = new StringBuilder(text.length() + 2).append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\');
            sb.append(c);
        }
        return sb.append('"').toString();
    }

    // ---- set operations ----

    /** The range's lower bound as a bound value, or null when it is unbounded. */
    private static Bound lowerBoundOf(PgRange r) {
        return r.lower == null ? null : new Bound(r.lowerValue(), r.lower, r.lowerStr);
    }

    /** The range's upper bound as a bound value, or null when it is unbounded. */
    private static Bound upperBoundOf(PgRange r) {
        return r.upper == null ? null : new Bound(r.upperValue(), r.upper, r.upperStr);
    }

    /** A range assembled from two bounds already read; empty when the two admit no points. */
    private static PgRange make(Bound lo, boolean li, Bound hi, boolean ui, String elem) {
        if (lo == null) li = false;
        if (hi == null) ui = false;
        PgRange r = new PgRange(lo == null ? null : lo.key, hi == null ? null : hi.key, li, ui,
                false, lo == null ? null : lo.text, hi == null ? null : hi.text,
                elem, lo == null ? null : lo.value, hi == null ? null : hi.value);
        return r.isEmpty() ? emptyLike(elem) : r;
    }

    private static String elemOf(PgRange a, PgRange b) {
        return a.elemType != null ? a.elemType : b.elemType;
    }

    private static PgRange emptyLike(String elem) {
        return new PgRange(null, null, false, false, true, null, null, elem, null, null);
    }

    /** Union of two ranges (PG + operator). Ranges must overlap or be adjacent. */
    public static PgRange union(PgRange a, PgRange b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        if (!a.overlaps(b) && !areAdjacent(a, b) && !areAdjacent(b, a)) {
            throw new MemgresException("result of range union would not be contiguous", "22000");
        }
        return merge(a, b);
    }

    /** Intersection of two ranges (PG * operator). Returns empty if no overlap. */
    public static PgRange intersection(PgRange a, PgRange b) {
        String elem = elemOf(a, b);
        if (a.isEmpty() || b.isEmpty()) return emptyLike(elem);
        PgRange loSrc = cmpLowerBounds(a.lower, a.lowerInclusive, b.lower, b.lowerInclusive) >= 0 ? a : b;
        PgRange hiSrc = cmpUpperBounds(a.upper, a.upperInclusive, b.upper, b.upperInclusive) <= 0 ? a : b;
        return make(lowerBoundOf(loSrc), loSrc.lowerInclusive,
                upperBoundOf(hiSrc), hiSrc.upperInclusive, elem);
    }

    /** Merge two ranges: smallest range containing both (does not require adjacency). */
    public static PgRange merge(PgRange a, PgRange b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        PgRange loSrc = cmpLowerBounds(a.lower, a.lowerInclusive, b.lower, b.lowerInclusive) <= 0 ? a : b;
        PgRange hiSrc = cmpUpperBounds(a.upper, a.upperInclusive, b.upper, b.upperInclusive) >= 0 ? a : b;
        return make(lowerBoundOf(loSrc), loSrc.lowerInclusive,
                upperBoundOf(hiSrc), hiSrc.upperInclusive, elemOf(a, b));
    }

    /**
     * Set difference of two ranges (PG - operator).
     * Result must be a contiguous range; throws 22000 if not.
     */
    public static PgRange subtract(PgRange a, PgRange b) {
        if (a.isEmpty() || b.isEmpty() || !a.overlaps(b)) return a;
        String elem = elemOf(a, b);
        if (b.containsRange(a)) return emptyLike(elem);
        boolean coversLower =
                cmpLowerBounds(b.lower, b.lowerInclusive, a.lower, a.lowerInclusive) <= 0;
        boolean coversUpper =
                cmpUpperBounds(b.upper, b.upperInclusive, a.upper, a.upperInclusive) >= 0;
        // What b covers is taken out; what is left keeps the bound on the far side of b.
        if (coversLower) {
            return make(upperBoundOf(b), !b.upperInclusive, upperBoundOf(a), a.upperInclusive, elem);
        }
        if (coversUpper) {
            return make(lowerBoundOf(a), a.lowerInclusive, lowerBoundOf(b), !b.lowerInclusive, elem);
        }
        throw new MemgresException("result of range difference would not be contiguous", "22000");
    }

    /** Construct an int4range from lower and upper bounds. Default: [lower, upper) */
    public static PgRange int4range(int lower, int upper) {
        return int4range(lower, upper, "[)");
    }

    private static void checkBoundFlags(String bounds) {
        if (bounds == null || bounds.length() != 2
                || (bounds.charAt(0) != '[' && bounds.charAt(0) != '(')
                || (bounds.charAt(1) != ']' && bounds.charAt(1) != ')')) {
            // The four spellings belong in the hint rather than the message, which is where
            // PostgreSQL puts the advice it offers about a value it has already refused.
            MemgresException e = new MemgresException("invalid range bound flags", "42601");
            e.setHint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\".");
            throw e;
        }
    }

    /** Construct an int4range with nullable bounds. NULL means unbounded. */
    public static PgRange int4rangeNullable(Integer lower, Integer upper, String bounds) {
        if (bounds == null) bounds = "[)";
        checkBoundFlags(bounds);
        return build(
                lower == null ? null : longBound(lower.longValue()),
                upper == null ? null : longBound(upper.longValue()),
                bounds.charAt(0) == '[', bounds.charAt(1) == ']', INT);
    }

    /** Construct an int4range with bounds specification: "[]", "[)", "(]", "()" */
    public static PgRange int4range(int lower, int upper, String bounds) {
        checkBoundFlags(bounds);
        return build(longBound(lower), longBound(upper),
                bounds.charAt(0) == '[', bounds.charAt(1) == ']', INT);
    }

    private static Bound longBound(long v) {
        return new Bound(Long.valueOf(v), Long.valueOf(v), Long.toString(v));
    }

    /**
     * Build a range from its parsed bounds, canonicalising the discrete element types the way PG
     * does: an int or date range is always rewritten to {@code [lo,hi)}, and a range with no
     * points at all becomes {@code empty}.
     */
    private static PgRange build(Bound lo, Bound hi, boolean li, boolean ui, String elem) {
        // The bounds are checked as they were written, before anything is canonicalised. Doing it
        // the other way round refused (5,5) — which is the empty range — and accepted [5,4],
        // because canonicalising had already moved the bounds past each other.
        if (lo != null && hi != null) {
            int written = cmpVals(lo.key, hi.key);
            if (written > 0) {
                throw new MemgresException(
                        "range lower bound must be less than or equal to range upper bound", "22000");
            }
            if (written == 0 && !(li && ui)) return emptyLike(elem);
        }
        boolean discrete = INT.equals(elem) || INT8.equals(elem) || DATE.equals(elem);
        if (discrete) {
            if (lo != null && !li && !isSpecial(lo.key)) {
                lo = successor(lo, elem, false);
                li = true;
            }
            if (hi != null && ui && !isSpecial(hi.key)) {
                hi = successor(hi, elem, true);
                ui = false;
            }
        }
        if (lo == null) li = false;
        if (hi == null) ui = false;
        if (lo != null && hi != null) {
            int c = cmpVals(lo.key, hi.key);
            if (c > 0) {
                throw new MemgresException(
                        "range lower bound must be less than or equal to range upper bound", "22000");
            }
            if (c == 0 && !(li && ui)) return emptyLike(elem);
        }
        return new PgRange(lo == null ? null : lo.key, hi == null ? null : hi.key, li, ui, false,
                lo == null ? null : lo.text, hi == null ? null : hi.text,
                elem, lo == null ? null : lo.value, hi == null ? null : hi.value);
    }

    /**
     * The next value of a discrete element type. There is not always one: a range whose bound is
     * the largest value its type holds cannot be canonicalised, and PostgreSQL says the element
     * type overflowed rather than wrapping the bound round to a negative number.
     */
    private static Bound successor(Bound b, String elem, boolean upper) {
        if (DATE.equals(elem)) {
            LocalDate next = ((LocalDate) b.value).plusDays(1);
            return new Bound(next, Long.valueOf(next.toEpochDay()), formatDate(next));
        }
        long value = ((Number) b.value).longValue();
        long limit = INT8.equals(elem) ? Long.MAX_VALUE : Integer.MAX_VALUE;
        if (value >= limit) {
            throw new MemgresException(
                    (INT8.equals(elem) ? "bigint" : "integer") + " out of range", "22003");
        }
        return longBound(value + 1);
    }

    /** Parse a range from string format: [1,10), (5,15], empty, etc. */
    public static PgRange parse(String s) {
        return parse(s, null);
    }

    /**
     * Read the text form of a range, reading each bound as a value of {@code rangeType}'s element
     * type. Without the type name the element is inferred from the bounds themselves, which is
     * what re-reading a range's own canonical output relies on.
     */
    public static PgRange parse(String s, String rangeType) {
        if (s == null) return null;
        s = s.trim();
        String elem = elemOfRangeType(rangeType);
        if (s.equalsIgnoreCase("empty")) return emptyLike(elem);
        if (s.isEmpty() || (s.charAt(0) != '[' && s.charAt(0) != '(')) {
            throw malformedRange(s, "Missing left parenthesis or bracket.");
        }
        char first = s.charAt(0);
        // A range ends at the first bracket that closes it, not at the last bracket in the text.
        // Reading to the end instead made "[1,2)]" a range whose upper bound was "2)", and the
        // complaint that came back was about the integer rather than about the literal.
        int close = closingBracket(s, 0);
        if (close < 0) throw malformedRange(s, "Unexpected end of input.");
        if (!s.substring(close + 1).trim().isEmpty()) {
            throw malformedRange(s, "Junk after right parenthesis or bracket.");
        }

        char last = s.charAt(close);
        boolean li = first == '[';
        boolean ui = last == ']';
        String inner = s.substring(1, close);
        String[] parts = splitBounds(inner);
        if (parts == null) {
            // A range has exactly two bounds, so the comma between them is either missing or
            // there is one more than there is room for.
            throw malformedRange(s, boundSeparator(inner) < -1
                    ? "Too many commas." : "Missing comma after lower bound.");
        }

        String loRaw = parts[0].trim();
        String hiRaw = parts[1].trim();
        if (elem == null) elem = inferElem(loRaw, hiRaw);

        Bound lo = loRaw.isEmpty() ? null : parseBound(loRaw, elem);
        Bound hi = hiRaw.isEmpty() ? null : parseBound(hiRaw, elem);
        return build(lo, hi, li, ui, elem);
    }

    /**
     * Split a range's inner text at the separating comma, leaving quoted bounds alone. A range has
     * exactly two bounds, so a second bare comma means the text is not a range literal at all —
     * which is also what keeps the geometric types, written with several commas, out of here.
     */
    private static String[] splitBounds(String inner) {
        int at = boundSeparator(inner);
        if (at < 0) return null;
        return new String[]{inner.substring(0, at), inner.substring(at + 1)};
    }

    /**
     * Where a range's inner text separates its bounds, ignoring commas inside a quoted bound: the
     * comma's position, -1 when there is none, and -2 when there is more than one. Which of the
     * two failures it was is what PostgreSQL puts in the error's detail.
     */
    private static int boundSeparator(String inner) {
        boolean inQuotes = false;
        int at = -1;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '\\') { i++; continue; }
            if (c == '"') { inQuotes = !inQuotes; continue; }
            if (c == ',' && !inQuotes) {
                if (at >= 0) return -2;
                at = i;
            }
        }
        return at;
    }

    /**
     * The two bound texts of a range literal, unquoted, or null when it has no such pair.
     *
     * <p>The range ends where it closes, as it does when it is read: text with something after the
     * closing bracket has no pair of bounds to offer, and saying so leaves the complaint to the
     * reader, which knows the literal is malformed rather than the bound out of range.
     */
    static String[] boundTexts(String literal) {
        String s = literal.trim();
        if (s.length() < 3) return null;
        char first = s.charAt(0);
        if (first != '[' && first != '(') return null;
        int close = closingBracket(s, 0);
        if (close < 0 || !s.substring(close + 1).trim().isEmpty()) return null;
        String[] parts = splitBounds(s.substring(1, close));
        if (parts == null) return null;
        return new String[]{stripQuotes(parts[0]), stripQuotes(parts[1])};
    }

    /** The element type the written bounds point at; the more specific of the two wins. */
    private static String inferElem(String loRaw, String hiRaw) {
        String a = inferElem(loRaw);
        String b = inferElem(hiRaw);
        if (rankElem(a) >= rankElem(b)) return a == null ? defaultElem(loRaw, hiRaw) : a;
        return b;
    }

    private static String defaultElem(String loRaw, String hiRaw) {
        // Both bounds are unbounded or spelled "infinity", which every temporal type writes the
        // same way; reading them as timestamps keeps the text form and the bound values intact.
        boolean anyWord = specialNumber(stripQuotes(loRaw)) != null
                || specialNumber(stripQuotes(hiRaw)) != null;
        return anyWord ? TS : null;
    }

    private static int rankElem(String elem) {
        if (elem == null) return -1;
        if (INT.equals(elem)) return 0;
        if (INT8.equals(elem)) return 1;
        if (NUM.equals(elem)) return 2;
        if (DATE.equals(elem)) return 3;
        if (TS.equals(elem)) return 4;
        return 5;
    }

    private static String inferElem(String raw) {
        String t = stripQuotes(raw.trim());
        if (t.isEmpty()) return null;
        // PG's numeric writes its specials capitalised and the temporal types write theirs in
        // lower case, so the spelling is what says which type a bare "infinity" came from.
        if (t.equals("Infinity") || t.equals("-Infinity") || t.equals("NaN")) return NUM;
        if (specialNumber(t) != null) return null;
        if (t.matches("[-+]?\\d{4,}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}.*[+-]\\d{2}(:\\d{2}){0,2}")) return TSTZ;
        if (t.matches("[-+]?\\d{4,}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}.*")) return TS;
        if (t.matches("[-+]?\\d{4,}-\\d{2}-\\d{2}( BC)?")) return DATE;
        if (t.indexOf('.') >= 0 || t.indexOf('e') >= 0 || t.indexOf('E') >= 0) return NUM;
        return INT;
    }

    private static String stripQuotes(String s) {
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return s;
    }

    /**
     * Check if two ranges are adjacent (touching but not overlapping).
     * For int4range: [1,5) -|- [5,10) is true since 5 = upper(a) = lower(b)
     */
    public static boolean areAdjacent(PgRange a, PgRange b) {
        // Empty ranges are never adjacent to anything
        if (a.isEmpty() || b.isEmpty()) return false;
        Number aUpper = a.upper();
        Number bLower = b.lower();
        Number aLower = a.lower();
        Number bUpper = b.upper();
        if (aUpper != null && bLower != null && cmpVals(aUpper, bLower) == 0) {
            if (!a.upperInclusive() && b.lowerInclusive()) return true;
            if (a.upperInclusive() && !b.lowerInclusive()) return true;
        }
        if (aLower != null && bUpper != null && cmpVals(bUpper, aLower) == 0) {
            if (!b.upperInclusive() && a.lowerInclusive()) return true;
            if (b.upperInclusive() && !a.lowerInclusive()) return true;
        }
        return false;
    }

    /**
     * {@code a << b}: every point of {@code a} is strictly below every point of {@code b}.
     * An empty range is strictly left of nothing, and nothing is strictly left of it.
     */
    public static boolean strictlyLeftOf(PgRange a, PgRange b) {
        if (a.isEmpty() || b.isEmpty()) return false;
        Number aUpper = a.upper();
        Number bLower = b.lower();
        // An unbounded side reaches infinity, so it can never be strictly to one side.
        if (aUpper == null || bLower == null) return false;
        int cmp = cmpVals(aUpper, bLower);
        if (cmp < 0) return true;
        // Equal endpoints only stay disjoint when at least one of them is exclusive.
        return cmp == 0 && !(a.upperInclusive() && b.lowerInclusive());
    }

    /** {@code a >> b}: every point of {@code a} is strictly above every point of {@code b}. */
    public static boolean strictlyRightOf(PgRange a, PgRange b) {
        return strictlyLeftOf(b, a);
    }

    /** {@code a << b} where either side may be a multirange; empty multiranges are never disjoint. */
    public static boolean multirangeStrictlyLeftOf(java.util.List<PgRange> a, java.util.List<PgRange> b) {
        java.util.List<PgRange> aParts = nonEmptyParts(a);
        java.util.List<PgRange> bParts = nonEmptyParts(b);
        if (aParts.isEmpty() || bParts.isEmpty()) return false;
        // A multirange is stored in ascending order, so only the extreme parts matter.
        return strictlyLeftOf(aParts.get(aParts.size() - 1), bParts.get(0));
    }

    /**
     * {@code a &< b}: whether {@code a} does not reach past the end of {@code b}. An empty range
     * takes part in no such comparison, and PostgreSQL answers false rather than true for it.
     */
    public static boolean multirangeDoesNotExtendRight(java.util.List<PgRange> a,
            java.util.List<PgRange> b) {
        java.util.List<PgRange> aParts = nonEmptyParts(a);
        java.util.List<PgRange> bParts = nonEmptyParts(b);
        if (aParts.isEmpty() || bParts.isEmpty()) return false;
        PgRange last = aParts.get(aParts.size() - 1);
        PgRange other = bParts.get(bParts.size() - 1);
        return cmpUpperBounds(last.upper, last.upperInclusive,
                other.upper, other.upperInclusive) <= 0;
    }

    private static java.util.List<PgRange> nonEmptyParts(java.util.List<PgRange> parts) {
        java.util.List<PgRange> out = new java.util.ArrayList<>();
        if (parts != null) {
            for (PgRange r : parts) {
                if (!r.isEmpty()) out.add(r);
            }
        }
        return out;
    }

    /** Check if a value is a PG range string (not a geometric type). */
    public static boolean isRangeString(String s) {
        if (s == null || s.length() < 3) return false;
        s = s.trim();
        if (s.equalsIgnoreCase("empty")) return true;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first != '[' && first != '(') || (last != ']' && last != ')')) return false;
        String inner = s.substring(1, s.length() - 1);
        String[] parts = splitBounds(inner);
        if (parts == null) return false; // Geometric types have a different shape
        // Each part should be empty or a number, a date/timestamp, or an infinity
        for (String part : parts) {
            String t = stripQuotes(part);
            if (t.isEmpty()) continue;
            if (specialNumber(t) != null) continue;
            if (t.matches("[-+]?\\d{4,}-\\d{2}-\\d{2}.*")) continue;
            try {
                Integer.parseInt(t);
            } catch (NumberFormatException e) {
                try {
                    Double.parseDouble(t);
                } catch (NumberFormatException e2) {
                    return false;
                }
            }
        }
        return true;
    }

    // ---- Multirange support ----

    /**
     * Check if a string is a PG multirange literal, e.g. '{[1,4),[10,12)}'.
     * Multiranges are enclosed in curly braces and contain zero or more range literals.
     */
    public static boolean isMultirangeString(String s) {
        if (s == null || s.length() < 2) return false;
        s = s.trim();
        if (s.charAt(0) != '{' || s.charAt(s.length() - 1) != '}') return false;
        String inner = s.substring(1, s.length() - 1).trim();
        if (inner.isEmpty()) return false; // empty '{}' is ambiguous, treat as JSON/array, not multirange
        // Check that inner content starts with a range literal bracket
        if (inner.charAt(0) != '[' && inner.charAt(0) != '(') return false;
        int closeIdx = closingBracket(inner, 0);
        if (closeIdx < 0) return false;
        String firstElem = inner.substring(0, closeIdx + 1);
        return isRangeString(firstElem);
    }

    /** Check if a multirange is adjacent to a range (last sub-range -|- range or range -|- first sub-range). */
    public static boolean multirangeAdjacentRange(String multirangeStr, PgRange range) {
        java.util.List<PgRange> ranges = parseMultirange(multirangeStr);
        if (ranges.isEmpty() || range.isEmpty()) return false;
        // Check last sub-range adjacent to the given range, or first sub-range
        PgRange last = ranges.get(ranges.size() - 1);
        PgRange first = ranges.get(0);
        return areAdjacent(last, range) || areAdjacent(first, range);
    }

    /** Check if two multiranges are adjacent (last of one -|- first of other). */
    public static boolean multirangeAdjacentMultirange(String mr1, String mr2) {
        java.util.List<PgRange> ranges1 = parseMultirange(mr1);
        java.util.List<PgRange> ranges2 = parseMultirange(mr2);
        if (ranges1.isEmpty() || ranges2.isEmpty()) return false;
        PgRange last1 = ranges1.get(ranges1.size() - 1);
        PgRange first2 = ranges2.get(0);
        PgRange first1 = ranges1.get(0);
        PgRange last2 = ranges2.get(ranges2.size() - 1);
        return areAdjacent(last1, first2) || areAdjacent(last2, first1);
    }

    /** Like isMultirangeString but also accepts '{}' as an empty multirange. */
    public static boolean isMultirangeOrEmpty(String s) {
        if (s == null) return false;
        s = s.trim();
        if (s.equals("{}")) return true;
        return isMultirangeString(s);
    }

    /**
     * Read the text form of a multirange the way PostgreSQL reads it: a brace-wrapped list of range
     * literals, whitespace allowed around each. {@code empty} is a range like any other and is
     * spelled out, so it belongs in the list rather than breaking it; it simply contributes nothing
     * to the result. Anything else between the braces is a malformed literal.
     */
    public static java.util.List<PgRange> parseMultirangeLiteral(String text) {
        return parseMultirangeLiteral(text, null);
    }

    /** As {@link #parseMultirangeLiteral(String)}, reading each part as {@code rangeType}. */
    public static java.util.List<PgRange> parseMultirangeLiteral(String text, String rangeType) {
        java.util.List<PgRange> out = new java.util.ArrayList<PgRange>();
        int p = skipSpace(text, 0);
        if (p >= text.length() || text.charAt(p) != '{') {
            throw malformedMultirange(text, "Missing left brace.");
        }
        p++;
        p = skipSpace(text, p);
        if (p < text.length() && text.charAt(p) == '}') {
            p++;
        } else {
            for (;;) {
                p = skipSpace(text, p);
                if (p >= text.length()) {
                    throw malformedMultirange(text, "Unexpected end of input.");
                }
                char c = text.charAt(p);
                if (c == '[' || c == '(') {
                    int end = closingBracket(text, p);
                    if (end < 0) throw malformedMultirange(text, "Unexpected end of input.");
                    out.add(parse(text.substring(p, end + 1), rangeType));
                    p = end + 1;
                } else if (text.regionMatches(true, p, "empty", 0, 5)) {
                    out.add(emptyLike(elemOfRangeType(rangeType)));
                    p += 5;
                } else {
                    throw malformedMultirange(text, "Expected range start.");
                }
                p = skipSpace(text, p);
                if (p < text.length() && text.charAt(p) == ',') {
                    p++;
                    continue;
                }
                if (p < text.length() && text.charAt(p) == '}') {
                    p++;
                    break;
                }
                throw malformedMultirange(text, p >= text.length()
                        ? "Unexpected end of input." : "Expected comma or end of multirange.");
            }
        }
        if (skipSpace(text, p) != text.length()) {
            throw malformedMultirange(text, "Junk after closing right brace.");
        }
        return out;
    }

    private static int skipSpace(String s, int from) {
        int p = from;
        while (p < s.length() && Character.isWhitespace(s.charAt(p))) p++;
        return p;
    }

    /** The bracket that closes the range starting at {@code from}, ignoring quoted bounds. */
    private static int closingBracket(String s, int from) {
        boolean inQuotes = false;
        for (int i = from + 1; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') { i++; continue; }
            if (c == '"') { inQuotes = !inQuotes; continue; }
            if (!inQuotes && (c == ')' || c == ']')) return i;
        }
        return -1;
    }

    private static MemgresException malformedRange(String text, String detail) {
        MemgresException e =
                new MemgresException("malformed range literal: \"" + text + "\"", "22P02");
        e.setDetail(detail);
        return e;
    }

    private static MemgresException malformedMultirange(String text, String detail) {
        MemgresException e =
                new MemgresException("malformed multirange literal: \"" + text + "\"", "22P02");
        e.setDetail(detail);
        return e;
    }

    /**
     * Parse a multirange string like '{[1,4),[10,12)}' into a list of PgRange.
     */
    public static java.util.List<PgRange> parseMultirange(String s) {
        return parseMultirange(s, null);
    }

    /**
     * The same, reading each member as a range of {@code rangeType}. Without the type name the
     * members are read from their own spelling, so a nummultirange whose bounds are whole numbers
     * came back canonicalised as if it were an integer multirange.
     */
    public static java.util.List<PgRange> parseMultirange(String s, String rangeType) {
        if (s == null) return java.util.Collections.emptyList();
        s = s.trim();
        if (s.isEmpty()) return java.util.Collections.emptyList();
        if (s.charAt(0) == '{' && s.charAt(s.length() - 1) == '}') {
            s = s.substring(1, s.length() - 1).trim();
        }
        if (s.isEmpty()) return java.util.Collections.emptyList();

        java.util.List<PgRange> ranges = new java.util.ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '[' || c == '(') {
                int end = closingBracket(s, i);
                if (end < 0) break;
                ranges.add(parse(s.substring(i, end + 1), rangeType));
                i = end + 1;
                // Skip comma separator
                while (i < s.length() && (s.charAt(i) == ',' || s.charAt(i) == ' ')) i++;
            } else {
                i++;
            }
        }
        return ranges;
    }

    /** Check if a multirange contains a value of any of the element type's shapes. */
    public static boolean multirangeContainsValue(String multirangeStr, Object value) {
        java.util.List<PgRange> ranges = parseMultirange(multirangeStr);
        for (PgRange r : ranges) {
            if (Boolean.TRUE.equals(r.containsValue(value))) return true;
        }
        return false;
    }

    /**
     * Format a list of PgRange as a multirange string like '{[1,4),[10,12)}'.
     */
    public static String formatMultirange(java.util.List<PgRange> ranges) {
        if (ranges == null || ranges.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < ranges.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(ranges.get(i).toString());
        }
        sb.append("}");
        return sb.toString();
    }

    /** Check if a multirange overlaps with a range. */
    public static boolean multirangeOverlapsRange(String multirangeStr, PgRange range) {
        java.util.List<PgRange> ranges = parseMultirange(multirangeStr);
        for (PgRange r : ranges) {
            if (r.overlaps(range)) return true;
        }
        return false;
    }

    /** Check if two multiranges overlap. */
    public static boolean multirangeOverlapsMultirange(String mr1, String mr2) {
        java.util.List<PgRange> ranges1 = parseMultirange(mr1);
        java.util.List<PgRange> ranges2 = parseMultirange(mr2);
        for (PgRange r1 : ranges1) {
            for (PgRange r2 : ranges2) {
                if (r1.overlaps(r2)) return true;
            }
        }
        return false;
    }

    /** Check if a multirange contains a range. */
    public static boolean multirangeContainsRange(String multirangeStr, PgRange range) {
        if (range.isEmpty()) return true;
        java.util.List<PgRange> ranges = parseMultirange(multirangeStr);
        for (PgRange r : ranges) {
            if (r.containsRange(range)) return true;
        }
        return false;
    }

    /** Check if a multirange contains another multirange. */
    public static boolean multirangeContainsMultirange(String mr1, String mr2) {
        java.util.List<PgRange> ranges2 = parseMultirange(mr2);
        for (PgRange r2 : ranges2) {
            if (!multirangeContainsRange(mr1, r2)) return false;
        }
        return true;
    }

    /** Union two multiranges, merging overlapping/adjacent ranges. */
    public static String multirangeUnion(String mr1, String mr2) {
        return multirangeUnion(mr1, mr2, null);
    }

    /** The same, reading both operands as multiranges of the named type. */
    public static String multirangeUnion(String mr1, String mr2, String rangeType) {
        java.util.List<PgRange> all = new java.util.ArrayList<>();
        all.addAll(parseMultirange(mr1, rangeType));
        all.addAll(parseMultirange(mr2, rangeType));
        return formatMultirange(mergeAndSort(all));
    }

    /** Intersect two multiranges. */
    public static String multirangeIntersect(String mr1, String mr2) {
        java.util.List<PgRange> ranges1 = parseMultirange(mr1);
        java.util.List<PgRange> ranges2 = parseMultirange(mr2);
        java.util.List<PgRange> result = new java.util.ArrayList<>();
        for (PgRange r1 : ranges1) {
            for (PgRange r2 : ranges2) {
                PgRange inter = intersection(r1, r2);
                if (!inter.isEmpty()) result.add(inter);
            }
        }
        return formatMultirange(mergeAndSort(result));
    }

    /** Subtract a multirange from another multirange. */
    public static String multirangeSubtract(String mr1, String mr2) {
        return multirangeSubtract(mr1, mr2, null);
    }

    /** The same, reading both operands as multiranges of the named type. */
    public static String multirangeSubtract(String mr1, String mr2, String rangeType) {
        java.util.List<PgRange> result = new java.util.ArrayList<>(parseMultirange(mr1, rangeType));
        for (PgRange sub : parseMultirange(mr2, rangeType)) {
            java.util.List<PgRange> next = new java.util.ArrayList<>();
            for (PgRange r : result) {
                subtractSingle(r, sub, next);
            }
            result = next;
        }
        return formatMultirange(mergeAndSort(result));
    }

    /** Subtract single range from a range, adding results to output list. */
    private static void subtractSingle(PgRange a, PgRange b, java.util.List<PgRange> out) {
        if (a.isEmpty()) return;
        if (b.isEmpty() || !a.overlaps(b)) { out.add(a); return; }
        if (b.containsRange(a)) return;
        String elem = elemOf(a, b);
        boolean coversLower =
                cmpLowerBounds(b.lower, b.lowerInclusive, a.lower, a.lowerInclusive) <= 0;
        boolean coversUpper =
                cmpUpperBounds(b.upper, b.upperInclusive, a.upper, a.upperInclusive) >= 0;
        if (coversLower) {
            out.add(make(upperBoundOf(b), !b.upperInclusive, upperBoundOf(a), a.upperInclusive, elem));
            return;
        }
        if (coversUpper) {
            out.add(make(lowerBoundOf(a), a.lowerInclusive, lowerBoundOf(b), !b.lowerInclusive, elem));
            return;
        }
        // b is in the middle — two pieces
        out.add(make(lowerBoundOf(a), a.lowerInclusive, lowerBoundOf(b), !b.lowerInclusive, elem));
        out.add(make(upperBoundOf(b), !b.upperInclusive, upperBoundOf(a), a.upperInclusive, elem));
    }

    /** Sort and merge overlapping/adjacent ranges, the form a multirange is stored in. */
    static java.util.List<PgRange> mergeAndSort(java.util.List<PgRange> ranges) {
        if (ranges.isEmpty()) return ranges;
        // Remove empty ranges
        java.util.List<PgRange> nonEmpty = new java.util.ArrayList<>();
        for (PgRange r : ranges) {
            if (!r.isEmpty()) nonEmpty.add(r);
        }
        if (nonEmpty.isEmpty()) return nonEmpty;
        nonEmpty.sort(new java.util.Comparator<PgRange>() {
            @Override
            public int compare(PgRange a, PgRange b) {
                return cmpLowerBounds(a.lower, a.lowerInclusive, b.lower, b.lowerInclusive);
            }
        });
        java.util.List<PgRange> merged = new java.util.ArrayList<>();
        merged.add(nonEmpty.get(0));
        for (int i = 1; i < nonEmpty.size(); i++) {
            PgRange last = merged.get(merged.size() - 1);
            PgRange curr = nonEmpty.get(i);
            // Two parts that share a point or sit end to end are one part.
            if (last.overlaps(curr) || areAdjacent(last, curr) || areAdjacent(curr, last)) {
                merged.set(merged.size() - 1, merge(last, curr));
            } else {
                merged.add(curr);
            }
        }
        return merged;
    }
}
