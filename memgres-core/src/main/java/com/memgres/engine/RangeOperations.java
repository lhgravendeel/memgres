package com.memgres.engine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PG range type support: int4range, int8range, numrange, daterange, tsrange, tstzrange.
 * Ranges are stored as canonical string form: "[lower,upper)" with inclusive/exclusive bounds.
 */
public class RangeOperations {

    /** A parsed PG range value. */
        public static final class PgRange {
        public final Number lower;
        public final Number upper;
        public final boolean lowerInclusive;
        public final boolean upperInclusive;
        public final boolean empty;
        // Original string representations for proper formatting (dates, timestamps, decimals)
        public final String lowerStr;
        public final String upperStr;

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
            this.lower = lower;
            this.upper = upper;
            this.lowerInclusive = lowerInclusive;
            this.upperInclusive = upperInclusive;
            this.empty = empty;
            this.lowerStr = lowerStr;
            this.upperStr = upperStr;
        }

        /** Canonical string form: [lower,upper) or empty */
        @Override
        public String toString() {
            if (empty) return "empty";
            String lo = lowerStr != null ? lowerStr : (lower != null ? lower.toString() : "");
            String hi = upperStr != null ? upperStr : (upper != null ? upper.toString() : "");
            // Quote timestamp bounds (contain space between date and time)
            if (lo.contains(" ") && lo.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}.*")) lo = "\"" + lo + "\"";
            if (hi.contains(" ") && hi.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}.*")) hi = "\"" + hi + "\"";
            return (lowerInclusive ? "[" : "(") + lo + "," + hi + (upperInclusive ? "]" : ")");
        }

        /** Check if this range contains a value. */
        public boolean contains(Number value) {
            if (empty || value == null) return false;
            long v = value.longValue();
            if (lower != null) {
                long lo = lower.longValue();
                if (lowerInclusive ? v < lo : v <= lo) return false;
            }
            if (upper != null) {
                long hi = upper.longValue();
                if (upperInclusive ? v > hi : v >= hi) return false;
            }
            return true;
        }

        /** Check if this range contains another range. */
        public boolean containsRange(PgRange other) {
            if (empty || other.empty) return empty == other.empty;
            long thisLo = effectiveLower();
            long thisHi = effectiveUpper();
            long otherLo = other.effectiveLower();
            long otherHi = other.effectiveUpper();
            return thisLo <= otherLo && thisHi >= otherHi;
        }

        /** Check if this range overlaps with another. */
        public boolean overlaps(PgRange other) {
            if (empty || other.empty) return false;
            long thisLo = effectiveLower();
            long thisHi = effectiveUpper();
            long otherLo = other.effectiveLower();
            long otherHi = other.effectiveUpper();
            return thisLo < otherHi && otherLo < thisHi;
        }

        /** Effective lower bound (inclusive, for integer ranges). */
        long effectiveLower() {
            if (lower == null) return Long.MIN_VALUE;
            return lowerInclusive ? lower.longValue() : lower.longValue() + 1;
        }

        /** Effective upper bound (exclusive, for integer ranges). */
        long effectiveUpper() {
            if (upper == null) return Long.MAX_VALUE;
            return upperInclusive ? upper.longValue() + 1 : upper.longValue();
        }

        public boolean isEmpty() {
            if (empty) return true;
            return effectiveLower() >= effectiveUpper();
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

    /** Union of two ranges (PG + operator). Ranges must overlap or be adjacent. */
    public static PgRange union(PgRange a, PgRange b) {
        if (a.empty) return b;
        if (b.empty) return a;
        long aLo = a.effectiveLower();
        long aHi = a.effectiveUpper();
        long bLo = b.effectiveLower();
        long bHi = b.effectiveUpper();
        if (aHi < bLo || bHi < aLo) {
            throw new MemgresException("result of range union would not be contiguous", "22000");
        }
        long lo = Math.min(aLo, bLo);
        long hi = Math.max(aHi, bHi);
        if (lo == hi) return new PgRange(null, null, true, false, true);
        // Preserve string representations from the range that contributes the bound
        String loStr = (aLo <= bLo) ? a.lowerStr : b.lowerStr;
        String hiStr = (aHi >= bHi) ? a.upperStr : b.upperStr;
        return new PgRange(lo, hi, true, false, false, loStr, hiStr);
    }

    /** Intersection of two ranges (PG * operator). Returns empty if no overlap. */
    public static PgRange intersection(PgRange a, PgRange b) {
        if (a.empty || b.empty) return new PgRange(null, null, true, false, true);
        long aLo = a.effectiveLower();
        long aHi = a.effectiveUpper();
        long bLo = b.effectiveLower();
        long bHi = b.effectiveUpper();
        long lo = Math.max(aLo, bLo);
        long hi = Math.min(aHi, bHi);
        if (lo >= hi) return new PgRange(null, null, true, false, true);
        return new PgRange(lo, hi, true, false, false);
    }

    /** Merge two ranges: smallest range containing both (does not require adjacency). */
    public static PgRange merge(PgRange a, PgRange b) {
        if (a.empty) return b;
        if (b.empty) return a;
        long aLo = a.effectiveLower();
        long aHi = a.effectiveUpper();
        long bLo = b.effectiveLower();
        long bHi = b.effectiveUpper();
        long lo = Math.min(aLo, bLo);
        long hi = Math.max(aHi, bHi);
        if (lo == hi) return new PgRange(null, null, true, false, true);
        // Preserve string representations from the range that contributes the bound
        String loStr = (aLo <= bLo) ? a.lowerStr : b.lowerStr;
        String hiStr = (aHi >= bHi) ? a.upperStr : b.upperStr;
        return new PgRange(lo, hi, true, false, false, loStr, hiStr);
    }

    /**
     * Set difference of two ranges (PG - operator).
     * Result must be a contiguous range; throws 22000 if not.
     */
    public static PgRange subtract(PgRange a, PgRange b) {
        if (a.empty) return a;
        if (b.empty) return a;
        long aLo = a.effectiveLower();
        long aHi = a.effectiveUpper();
        long bLo = b.effectiveLower();
        long bHi = b.effectiveUpper();

        // No overlap: a - b = a
        if (aHi <= bLo || bHi <= aLo) return a;

        // b fully contains a: a - b = empty
        if (bLo <= aLo && bHi >= aHi) return new PgRange(null, null, true, false, true);

        // b overlaps left side: result is [b.hi, a.hi)
        if (bLo <= aLo && bHi < aHi) {
            return new PgRange(bHi, aHi, true, false, false);
        }

        // b overlaps right side: result is [a.lo, b.lo)
        if (bLo > aLo && bHi >= aHi) {
            return new PgRange(aLo, bLo, true, false, false);
        }

        // b is in the middle of a: result would be two disjoint ranges, which is an error
        throw new MemgresException("result of range difference would not be contiguous", "22000");
    }

    /** Construct an int4range from lower and upper bounds. Default: [lower, upper) */
    public static PgRange int4range(int lower, int upper) {
        return int4range(lower, upper, "[)");
    }

    /** Construct an int4range with nullable bounds. NULL means unbounded. */
    public static PgRange int4rangeNullable(Integer lower, Integer upper, String bounds) {
        if (bounds == null) bounds = "[)";
        if (bounds.length() != 2
                || (bounds.charAt(0) != '[' && bounds.charAt(0) != '(')
                || (bounds.charAt(1) != ']' && bounds.charAt(1) != ')')) {
            throw new MemgresException(
                    "range bound flags must be one of \"[]\", \"[)\", \"(]\", or \"()\"", "42601");
        }
        boolean li = bounds.charAt(0) == '[';
        boolean ui = bounds.charAt(1) == ']';
        if (lower == null && upper == null) {
            return new PgRange(null, null, false, false, false);
        }
        if (lower == null) {
            int canonHi = ui ? upper + 1 : upper;
            return new PgRange(null, canonHi, false, false, false);
        }
        if (upper == null) {
            int canonLo = li ? lower : lower + 1;
            return new PgRange(canonLo, null, true, false, false);
        }
        return int4range(lower, upper, bounds);
    }

    /** Construct an int4range with bounds specification: "[]", "[)", "(]", "()" */
    public static PgRange int4range(int lower, int upper, String bounds) {
        if (bounds == null || bounds.length() != 2
                || (bounds.charAt(0) != '[' && bounds.charAt(0) != '(')
                || (bounds.charAt(1) != ']' && bounds.charAt(1) != ')')) {
            throw new MemgresException(
                    "range bound flags must be one of \"[]\", \"[)\", \"(]\", or \"()\"", "42601");
        }
        boolean li = bounds.charAt(0) == '[';
        boolean ui = bounds.charAt(1) == ']';
        // Canonicalize integer ranges to [lo, hi) form
        int canonLo = li ? lower : lower + 1;
        int canonHi = ui ? upper + 1 : upper;
        if (canonLo > canonHi) {
            throw new MemgresException("range lower bound must be less than or equal to range upper bound", "22000");
        }
        if (canonLo == canonHi) {
            return new PgRange(null, null, true, false, true);
        }
        return new PgRange(canonLo, canonHi, true, false, false);
    }

    /** Parse a range from string format: [1,10), (5,15], empty, etc. */
    /** Parse a range bound string that may be integer or decimal (rounds decimal to integer). */
    private static Long parseRangeBoundLong(String s) {
        s = s.trim();
        // Strip surrounding quotes (timestamp ranges use quoted bounds)
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        // Timestamp: YYYY-MM-DD HH:MM[:SS], parsed as epoch seconds for tsrange support
        if (s.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}.*")) {
            try {
                java.time.LocalDateTime ldt = TypeCoercion.toLocalDateTime(s);
                return ldt.toEpochSecond(java.time.ZoneOffset.UTC);
            } catch (Exception ignored) {}
        }
        // Date-only: YYYY-MM-DD, use epoch days for daterange (discrete type)
        if (s.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return java.time.LocalDate.parse(s).toEpochDay();
        }
        try {
            return (long) Integer.parseInt(s);
        } catch (NumberFormatException e) {
            try {
                // Handle decimal values by rounding to nearest integer
                double d = Double.parseDouble(s);
                return Math.round(d);
            } catch (NumberFormatException e2) {
                throw new MemgresException("invalid input syntax for type integer: \"" + s + "\"", "22P02");
            }
        }
    }

    public static PgRange parse(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.equalsIgnoreCase("empty")) return new PgRange(null, null, true, false, true);
        if (s.length() < 3) throw new MemgresException("malformed range literal: \"" + s + "\"", "22P02");

        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first != '[' && first != '(') || (last != ']' && last != ')')) {
            throw new MemgresException("malformed range literal: \"" + s + "\"", "22P02");
        }

        boolean li = first == '[';
        boolean ui = last == ']';
        String inner = s.substring(1, s.length() - 1);
        String[] parts = inner.split(",", 2);
        if (parts.length != 2) throw new MemgresException("malformed range literal: \"" + s + "\"", "22P02");

        String loRaw = parts[0].trim();
        String hiRaw = parts[1].trim();
        // Strip surrounding quotes from raw strings for display
        String loDisplay = stripQuotes(loRaw);
        String hiDisplay = stripQuotes(hiRaw);

        Long lo = loRaw.isEmpty() ? null : parseRangeBoundLong(loRaw);
        Long hi = hiRaw.isEmpty() ? null : parseRangeBoundLong(hiRaw);

        // Determine if this is a non-integer type (dates, timestamps, decimals)
        boolean isNonInteger = isNonIntegerBound(loRaw.isEmpty() ? hiRaw : loRaw);

        if (lo != null && hi != null && !isNonInteger) {
            // Canonicalize integer ranges to [lo, hi)
            long canonLo = li ? lo : lo + 1;
            long canonHi = ui ? hi + 1 : hi;
            if (canonLo > canonHi) {
                throw new MemgresException("range lower bound must be less than or equal to range upper bound", "22000");
            }
            if (canonLo == canonHi) {
                return new PgRange(null, null, true, false, true);
            }
            return new PgRange(canonLo, canonHi, true, false, false);
        }
        // For non-integer types, preserve original string representations
        return new PgRange(lo, hi, li, ui, false,
                lo != null ? loDisplay : null,
                hi != null ? hiDisplay : null);
    }

    private static String stripQuotes(String s) {
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static boolean isNonIntegerBound(String raw) {
        raw = stripQuotes(raw.trim());
        if (raw.isEmpty()) return false;
        // Date: YYYY-MM-DD
        if (raw.matches("\\d{4}-\\d{2}-\\d{2}")) return true;
        // Timestamp: YYYY-MM-DD HH:MM...
        if (raw.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}.*")) return true;
        // Decimal: contains a dot
        if (raw.contains(".")) return true;
        return false;
    }

    /** Check if a value is a PG range string (not a geometric type). */
    /**
     * Check if two ranges are adjacent (touching but not overlapping).
     * For int4range: [1,5) -|- [5,10) is true since 5 = upper(a) = lower(b)
     */
    public static boolean areAdjacent(PgRange a, PgRange b) {
        if (a.empty() || b.empty()) return false;
        // Check if upper of a equals lower of b (with bound consideration)
        // For integer ranges: [1,5) upper=5 exclusive, [5,10) lower=5 inclusive → adjacent
        // Upper bound of a: value at upper
        Number aUpper = a.upper();
        Number bLower = b.lower();
        Number aLower = a.lower();
        Number bUpper = b.upper();
        if (aUpper != null && bLower != null) {
            // a's upper = b's lower; adjacent if one is exclusive and the other inclusive
            // In PG: ranges are adjacent if they share a "boundary"
            // For int4range [1,5) -|- [5,10): aUpper=5(excl), bLower=5(incl) → 5==5 → adjacent
            double au = aUpper.doubleValue() + (a.upperInclusive() ? 0 : 0);
            double bl = bLower.doubleValue() + (b.lowerInclusive() ? 0 : 0);
            // Normalized: [lo,hi) style. a's upper exclusive point = aUpper value, b's lower inclusive start = bLower
            if (aUpper.doubleValue() == bLower.doubleValue()) {
                // One is exclusive, other is inclusive → adjacent
                if (!a.upperInclusive() && b.lowerInclusive()) return true;
                if (a.upperInclusive() && !b.lowerInclusive()) return true;
            }
        }
        if (aLower != null && bUpper != null) {
            // b's upper = a's lower
            if (bUpper.doubleValue() == aLower.doubleValue()) {
                if (!b.upperInclusive() && a.lowerInclusive()) return true;
                if (b.upperInclusive() && !a.lowerInclusive()) return true;
            }
        }
        return false;
    }

    public static boolean isRangeString(String s) {
        if (s == null || s.length() < 3) return false;
        s = s.trim();
        if (s.equalsIgnoreCase("empty")) return true;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first != '[' && first != '(') || (last != ']' && last != ')')) return false;
        // Must have exactly one comma separating two numeric-ish values (not geometric points)
        String inner = s.substring(1, s.length() - 1);
        int commaCount = 0;
        for (char c : inner.toCharArray()) if (c == ',') commaCount++;
        if (commaCount != 1) return false; // Geometric types have multiple commas
        String[] parts = inner.split(",", 2);
        // Each part should be empty or a number or a date
        for (String part : parts) {
            String t = part.trim();
            // Strip surrounding quotes (timestamp ranges)
            if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
                t = t.substring(1, t.length() - 1);
            }
            if (!t.isEmpty()) {
                // Date: YYYY-MM-DD (possibly with time suffix)
                if (t.matches("\\d{4}-\\d{2}-\\d{2}.*")) continue;
                try { Integer.parseInt(t); } catch (NumberFormatException e) {
                    try { Double.parseDouble(t); } catch (NumberFormatException e2) { return false; }
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
        // Exclude JSON objects (contain quoted keys with colons but no range brackets)
        // Multiranges can contain quoted bounds (e.g., timestamps like "2024-01-01 00:00:00"),
        // so only exclude when the content doesn't start with a range bracket.
        // Since we already checked that inner starts with [ or (, this is a valid range/multirange.
        // Extract the first element and validate it's actually a range (not a record like "(1)")
        // Ranges can use mixed brackets (e.g. [1,5) or (1,5]), so find the first ']' or ')' after a comma
        int commaIdx = inner.indexOf(',');
        if (commaIdx < 0) return false; // ranges require a comma between bounds
        int closeSquare = inner.indexOf(']', commaIdx);
        int closeParen = inner.indexOf(')', commaIdx);
        int closeIdx;
        if (closeSquare < 0) closeIdx = closeParen;
        else if (closeParen < 0) closeIdx = closeSquare;
        else closeIdx = Math.min(closeSquare, closeParen);
        if (closeIdx < 0) return false;
        String firstElem = inner.substring(0, closeIdx + 1);
        return isRangeString(firstElem);
    }

    /** Check if a multirange is adjacent to a range (last sub-range -|- range or range -|- first sub-range). */
    public static boolean multirangeAdjacentRange(String multirangeStr, PgRange range) {
        java.util.List<PgRange> ranges = parseMultirange(multirangeStr);
        if (ranges.isEmpty() || range.empty) return false;
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
     * Parse a multirange string like '{[1,4),[10,12)}' into a list of PgRange.
     */
    public static java.util.List<PgRange> parseMultirange(String s) {
        if (s == null) return java.util.Collections.emptyList();
        s = s.trim();
        if (s.charAt(0) == '{' && s.charAt(s.length() - 1) == '}') {
            s = s.substring(1, s.length() - 1).trim();
        }
        if (s.isEmpty()) return java.util.Collections.emptyList();

        java.util.List<PgRange> ranges = new java.util.ArrayList<>();
        // Split by finding matching bracket pairs
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '[' || c == '(') {
                // Find the closing bracket
                int close = s.indexOf(')', i);
                int close2 = s.indexOf(']', i);
                int end;
                if (close < 0) end = close2;
                else if (close2 < 0) end = close;
                else end = Math.min(close, close2);
                if (end < 0) break;
                String rangeStr = s.substring(i, end + 1);
                ranges.add(parse(rangeStr));
                i = end + 1;
                // Skip comma separator
                while (i < s.length() && (s.charAt(i) == ',' || s.charAt(i) == ' ')) i++;
            } else {
                i++;
            }
        }
        return ranges;
    }

    /**
     * Check if a multirange contains a value.
     * Returns true if any sub-range contains the value.
     */
    public static boolean multirangeContains(String multirangeStr, Number value) {
        java.util.List<PgRange> ranges = parseMultirange(multirangeStr);
        for (PgRange r : ranges) {
            if (r.contains(value)) return true;
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
        if (range.empty) return true;
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
        java.util.List<PgRange> all = new java.util.ArrayList<>();
        all.addAll(parseMultirange(mr1));
        all.addAll(parseMultirange(mr2));
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
        java.util.List<PgRange> result = new java.util.ArrayList<>(parseMultirange(mr1));
        for (PgRange sub : parseMultirange(mr2)) {
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
        if (a.empty) return;
        if (b.empty) { out.add(a); return; }
        long aLo = a.effectiveLower();
        long aHi = a.effectiveUpper();
        long bLo = b.effectiveLower();
        long bHi = b.effectiveUpper();
        if (aHi <= bLo || bHi <= aLo) { out.add(a); return; }
        if (bLo <= aLo && bHi >= aHi) return; // fully subtracted
        if (bLo <= aLo && bHi < aHi) {
            out.add(new PgRange(bHi, aHi, true, false, false));
            return;
        }
        if (bLo > aLo && bHi >= aHi) {
            out.add(new PgRange(aLo, bLo, true, false, false));
            return;
        }
        // b is in the middle — two pieces
        out.add(new PgRange(aLo, bLo, true, false, false));
        out.add(new PgRange(bHi, aHi, true, false, false));
    }

    /**
     * Normalize date-only bounds (YYYY-MM-DD) to timestamp format (YYYY-MM-DD HH:MM:SS)
     * for tsmultirange/tstzmultirange types.
     */
    public static String normalizeDateBoundsToTimestamp(String s) {
        Pattern datePattern = Pattern.compile("\"(\\d{4}-\\d{2}-\\d{2})\"");

        Matcher m = datePattern.matcher(s);
        StringBuilder sb = new StringBuilder();

        int last = 0;
        while (m.find()) {
            sb.append(s, last, m.start());
            sb.append("\"").append(m.group(1)).append(" 00:00:00\"");
            last = m.end();
        }
        sb.append(s, last, s.length());

        return sb.toString();
    }

    /** Sort and merge overlapping/adjacent ranges. */
    private static java.util.List<PgRange> mergeAndSort(java.util.List<PgRange> ranges) {
        if (ranges.isEmpty()) return ranges;
        // Remove empty ranges
        java.util.List<PgRange> nonEmpty = new java.util.ArrayList<>();
        for (PgRange r : ranges) {
            if (!r.isEmpty()) nonEmpty.add(r);
        }
        if (nonEmpty.isEmpty()) return nonEmpty;
        nonEmpty.sort((a, b) -> Long.compare(a.effectiveLower(), b.effectiveLower()));
        java.util.List<PgRange> merged = new java.util.ArrayList<>();
        merged.add(nonEmpty.get(0));
        for (int i = 1; i < nonEmpty.size(); i++) {
            PgRange last = merged.get(merged.size() - 1);
            PgRange curr = nonEmpty.get(i);
            if (last.effectiveUpper() >= curr.effectiveLower()) {
                merged.set(merged.size() - 1, merge(last, curr));
            } else {
                merged.add(curr);
            }
        }
        return merged;
    }
}
