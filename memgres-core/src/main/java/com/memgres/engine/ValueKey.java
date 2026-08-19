package com.memgres.engine;

import java.math.BigDecimal;

/**
 * How a value is read where rows are matched to one another by value rather than by name: the
 * partitions a window is cut into, and the rows the two sides of a join agree on.
 *
 * <p>Both of those used to read a value by printing it and comparing the print. Printing is not
 * equality. A numeric 1.0 and a numeric 1.00 are the same number and print differently, so a join
 * on them found nothing and {@code PARTITION BY} put them in separate partitions. A bpchar
 * compares without the blanks its declaration padded it out to, so {@code 'a'::char(3)} joined to
 * {@code 'a'::char(6)} found nothing either. And the parts of a composite key were run together
 * with a separator character, so a value containing that character reached into its neighbour and
 * two different keys read as one.
 *
 * <p>Equality here is the engine's own, {@link TypeCoercion#areEqual}, which is the same rule the
 * {@code =} operator answers by. The hash label is a bucket and nothing more: two values that are
 * equal are guaranteed to land in the same bucket, two values in the same bucket are not
 * necessarily equal. Every caller that buckets confirms each candidate pair with the equality
 * afterwards, which is what makes that asymmetry the safe one — a bucket too coarse costs
 * comparisons, a bucket too fine loses rows.
 */
final class ValueKey {

    private ValueKey() {
    }

    /**
     * Whether two resolved values are the same value.
     *
     * @param blankPadded whether either side is declared {@code character(n)}, whose trailing
     *                    blanks are padding rather than content and are not compared
     */
    static boolean sameValue(Object a, Object b, boolean blankPadded) {
        if (blankPadded && a instanceof String && b instanceof String) {
            a = BlankPadding.trimmed(a);
            b = BlankPadding.trimmed(b);
        }
        return TypeCoercion.areEqual(a, b);
    }

    /**
     * Appends one value's bucket label to a composite key, written with its length in front so
     * that no value can run into the one after it whatever characters it contains.
     */
    static void appendTo(StringBuilder sb, Object val) {
        String part = hashLabel(val);
        sb.append(part.length()).append(':').append(part);
    }

    /**
     * The bucket a value belongs in. Values that compare equal share one; values that share one
     * still have to be compared.
     */
    static String hashLabel(Object val) {
        if (val == null) return "-";
        // A regproc prints its function's name and compares as its OID, so hashing the print put
        // a catalog's regproc column and pg_proc.oid in buckets that never met.
        if (val instanceof RegprocValue) return numberLabel(BigDecimal.valueOf(((RegprocValue) val).oid()));
        if (val instanceof Number) {
            Number n = (Number) val;
            double d = n.doubleValue();
            if (Double.isNaN(d)) return "#nan";
            if (Double.isInfinite(d)) return d > 0 ? "#inf" : "#-inf";
            try {
                return numberLabel(TypeCoercion.toBigDecimal(n));
            } catch (RuntimeException e) {
                return "#";
            }
        }
        if (val instanceof Boolean) return ((Boolean) val) ? "bt" : "bf";
        if (val instanceof String) {
            String s = (String) val;
            BigDecimal num = numericValueOf(s);
            // A number written as text reads as the same number, and the blanks a bpchar was
            // padded out to are not part of it. Dropping both here can only put values in a
            // bucket together that are then told apart by the comparison; keeping them would put
            // values that are equal in buckets that never meet.
            if (num != null) return numberLabel(num);
            return "s" + BlankPadding.trimmed(s);
        }
        if (val instanceof java.util.UUID) return "s" + val;
        // Equal byte arrays are different objects, so what one prints says nothing about the
        // other. Their content hash is what they have in common.
        if (val instanceof byte[]) return "x" + java.util.Arrays.hashCode((byte[]) val);
        // Everything else — the date and time types above all, whose equality is decided across
        // representations that no single spelling restates, along with arrays, records, enums and
        // ranges — shares one bucket and is told apart by the comparison rather than by the
        // index. That costs the comparisons a bucket was meant to save; it does not cost a row.
        return "?";
    }

    private static String numberLabel(BigDecimal d) {
        return "#" + d.stripTrailingZeros().toPlainString();
    }

    /** The number a string spells, or null when it does not spell one. */
    private static BigDecimal numericValueOf(String s) {
        int n = s.length();
        if (n == 0) return null;
        boolean digit = false;
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                digit = true;
            } else if (c != '+' && c != '-' && c != '.' && c != 'e' && c != 'E') {
                return null;
            }
        }
        if (!digit) return null;
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
