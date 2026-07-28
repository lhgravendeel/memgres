package com.memgres.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL's numeric formatting templates, as used by {@code to_char(numeric, text)}.
 *
 * <p>A template is read once into a description of the field — how many digit positions sit on
 * each side of the point, where zero filling starts, which sign form was asked for — and then the
 * same node list is walked to lay the value out. That two-pass shape is what makes the awkward
 * parts fall out: the sign lands immediately left of the first digit rather than at the far left,
 * a group separator ahead of every digit prints as a blank, and a value too wide for its field
 * fills the digit positions with {@code #} instead of silently widening them.
 *
 * <p>The locale-sensitive patterns {@code L}, {@code D} and {@code G} follow the locale memgres
 * reports for {@code lc_numeric} and {@code lc_monetary}, so their output moves with a server
 * built for a different locale exactly as PostgreSQL's does.
 */
final class NumericTemplate {

    private NumericTemplate() {
    }

    /** The conventions of {@code en_US.UTF-8}, which is what memgres reports for lc_numeric. */
    private static final String DECIMAL_SEP = ".";
    private static final String GROUP_SEP = ",";
    private static final String CURRENCY = "$";

    private static final int T_CHAR = 0, T_9 = 1, T_0 = 2, T_DEC = 3, T_COMMA = 4, T_D = 5,
            T_G = 6, T_L = 7, T_C = 8, T_B = 9, T_S = 10, T_MI = 11, T_PL = 12, T_SG = 13,
            T_PR = 14, T_RN = 15, T_TH = 16, T_V = 17, T_E = 18, T_SP = 19;

    private static final int LSIGN_NONE = 0, LSIGN_PRE = 1, LSIGN_POST = 2;

    /** Template keywords in PostgreSQL's order, so that {@code SG} is preferred over {@code S}. */
    private static final String[] KEYWORDS = {
            ",", ".", "0", "9", "B", "C", "D", "EEEE", "FM", "G", "L", "MI", "PL", "PR", "RN",
            "SG", "SP", "S", "TH", "V",
            "b", "c", "d", "eeee", "fm", "g", "l", "mi", "pl", "pr", "rn", "sg", "sp", "s",
            "th", "v",
    };

    private static final class Node {
        final int type;
        final char ch;
        final boolean upper;

        Node(int type, char ch, boolean upper) {
            this.type = type;
            this.ch = ch;
            this.upper = upper;
        }
    }

    /** What the template says the field looks like, independent of any particular value. */
    private static final class Desc {
        int pre, post, multi, zeroStart, zeroEnd;
        boolean fm, decimal, zero, multiFlag, roman, romanUpper, eeee;
        boolean minus, plus, bracket;
        int lsign = LSIGN_NONE;
        int preLsignNum;
    }

    // --------------------------------------------------------------- to_char

    static String format(Number value, String fmt) {
        if (fmt.isEmpty()) return "";
        Desc d = new Desc();
        List<Node> nodes = parse(fmt, d);
        if (d.eeee) checkEeee(d);
        // "S" written after every digit position is a trailing sign, not a leading one.
        if (d.lsign == LSIGN_PRE && d.preLsignNum == d.pre) d.lsign = LSIGN_POST;

        boolean halfEven = value instanceof Double || value instanceof Float;
        // Only a float carries NaN or an infinity; a BigDecimal that merely overflows double
        // is an ordinary number the template lays out digit by digit.
        double raw = halfEven ? value.doubleValue() : 0.0;
        boolean isNan = Double.isNaN(raw);
        boolean isInfinite = Double.isInfinite(raw);
        BigDecimal v = toDecimal(value);

        if (d.roman) return roman(v, d.fm, d.romanUpper);
        if (d.eeee) return exponential(v, d);

        // NaN has no digits to lay out, so PG right-justifies the word in the field the
        // template describes; an infinity is larger than any field, which is the overflow the
        // template already reports as a row of #.
        if (isNan) {
            if (d.fm) return "NaN";
            StringBuilder pad = new StringBuilder();
            for (int i = 3; i < d.pre + 1; i++) pad.append(' ');
            return pad + "NaN";
        }

        int pre = d.pre;
        int post = d.post;
        if (d.multiFlag) {
            v = v.scaleByPowerOfTen(d.multi);
            pre += d.multi;
            post = 0;
        }
        v = v.setScale(post, halfEven ? RoundingMode.HALF_EVEN : RoundingMode.HALF_UP);
        boolean neg = isInfinite ? raw < 0 : v.signum() < 0;
        String plain = v.abs().toPlainString();
        int dot = plain.indexOf('.');
        String intStr = dot < 0 ? plain : plain.substring(0, dot);
        StringBuilder frac = new StringBuilder(dot < 0 ? "" : plain.substring(dot + 1));
        while (frac.length() < post) frac.append('0');
        String fracStr = frac.toString();

        boolean overflow = isInfinite || intStr.length() > pre;
        int outPreSpaces = overflow ? 0 : pre - intStr.length();

        int lastNonZeroFrac = -1;
        for (int j = 0; j < post; j++) if (fracStr.charAt(j) != '0') lastNonZeroFrac = j;
        int lastFrac = lastNonZeroFrac;
        if (d.zeroEnd > pre) lastFrac = Math.max(lastFrac, d.zeroEnd - pre - 1);

        // The lone leading zero of a fraction-only value prints as a blank, which pushes the
        // sign past it: to_char(-0.1, '9.9') is ' -.1', not '-0.1'. Fill mode drops the blank
        // altogether — unless the fraction is all zeros, where the zero is all that is left.
        boolean predec = !d.zero && post > 0 && intStr.equals("0") && !overflow;
        boolean predecPrintsZero = predec && d.fm && lastNonZeroFrac < 0;
        int signSlot = outPreSpaces;
        if (d.zero && d.zeroStart < signSlot) signSlot = d.zeroStart;
        if (predec && !predecPrintsZero) signSlot = pre;

        String signPrefix;
        if (d.bracket) signPrefix = neg ? "<" : (d.fm ? "" : " ");
        else if (d.lsign == LSIGN_PRE) signPrefix = neg ? "-" : "+";
        else if (d.lsign == LSIGN_POST || d.minus) signPrefix = "";
        else signPrefix = neg ? "-" : (d.fm ? "" : " ");

        StringBuilder sb = new StringBuilder();
        boolean numIn = false;
        boolean signDone = false;
        int slot = 0;
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            switch (n.type) {
                case T_CHAR:
                    sb.append(n.ch);
                    break;
                case T_DEC:
                case T_D:
                    if (!signDone && slot >= signSlot) {
                        sb.append(signPrefix);
                        signDone = true;
                    }
                    if (post > 0) sb.append(n.type == T_D ? DECIMAL_SEP : ".");
                    break;
                case T_9:
                case T_0: {
                    if (!signDone && slot >= signSlot) {
                        sb.append(signPrefix);
                        signDone = true;
                    }
                    if (overflow) {
                        sb.append('#');
                        numIn = true;
                    } else if (slot < outPreSpaces) {
                        if (d.zero && slot >= d.zeroStart) {
                            sb.append('0');
                            numIn = true;
                        } else if (!d.fm) {
                            sb.append(' ');
                        }
                    } else if (slot < pre) {
                        if (predec) {
                            if (predecPrintsZero) {
                                sb.append('0');
                                numIn = true;
                            } else if (!d.fm) {
                                sb.append(' ');
                            }
                        } else {
                            sb.append(intStr.charAt(slot - outPreSpaces));
                            numIn = true;
                        }
                    } else {
                        int j = slot - pre;
                        if (d.fm && j > lastFrac && n.type != T_0) {
                            // fill mode drops the trailing zeros nothing asked for
                        } else {
                            sb.append(fracStr.charAt(j));
                            numIn = true;
                        }
                    }
                    slot++;
                    break;
                }
                case T_COMMA:
                    if (numIn) sb.append(',');
                    else if (!d.fm) sb.append(' ');
                    break;
                case T_G:
                    if (numIn) sb.append(GROUP_SEP);
                    else if (!d.fm) for (int k = 0; k < GROUP_SEP.length(); k++) sb.append(' ');
                    break;
                case T_L:
                    sb.append(CURRENCY);
                    break;
                case T_S:
                    if (d.lsign == LSIGN_POST) sb.append(neg ? '-' : '+');
                    break;
                case T_MI:
                    if (neg) sb.append('-');
                    else if (!d.fm) sb.append(' ');
                    break;
                case T_PL:
                    if (!neg) sb.append('+');
                    else if (!d.fm) sb.append(' ');
                    break;
                case T_SG:
                    sb.append(neg ? '-' : '+');
                    break;
                case T_PR:
                    if (neg) sb.append('>');
                    else if (!d.fm) sb.append(' ');
                    break;
                case T_TH:
                    if (!overflow && !neg && !d.decimal) {
                        sb.append(DateTimeTemplate.ordinal(intStr, n.upper));
                    }
                    break;
                default:
                    break; // B, C, V and SP contribute nothing of their own
            }
        }
        return sb.toString();
    }

    private static BigDecimal toDecimal(Number value) {
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof java.math.BigInteger) return new BigDecimal((java.math.BigInteger) value);
        if (value instanceof Double || value instanceof Float) {
            double dv = value.doubleValue();
            if (Double.isNaN(dv) || Double.isInfinite(dv)) return BigDecimal.ZERO;
            return new BigDecimal(Double.toString(dv));
        }
        return BigDecimal.valueOf(value.longValue());
    }

    private static String roman(BigDecimal v, boolean fm, boolean upper) {
        int n = v.setScale(0, RoundingMode.HALF_UP).intValue();
        String s;
        if (n < 1 || n > 3999) {
            s = "###############";
        } else {
            int[] vals = {1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1};
            String[] sym = {"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"};
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < vals.length; i++) {
                while (n >= vals[i]) {
                    sb.append(sym[i]);
                    n -= vals[i];
                }
            }
            s = sb.toString();
            if (!upper) s = s.toLowerCase(java.util.Locale.ROOT);
        }
        if (fm) return s;
        StringBuilder pad = new StringBuilder();
        for (int i = s.length(); i < 15; i++) pad.append(' ');
        return pad + s;
    }

    private static String exponential(BigDecimal v, Desc d) {
        String s = String.format(java.util.Locale.US, "%." + d.post + "e", v.doubleValue());
        return v.signum() < 0 ? s : " " + s;
    }

    private static void checkEeee(Desc d) {
        if (d.fm || d.multiFlag || d.roman || d.minus || d.plus || d.bracket
                || d.lsign != LSIGN_NONE || d.zero) {
            throw new MemgresException("\"EEEE\" is incompatible with other formats"
                    + "\n  Detail: \"EEEE\" may only be used together with digit and decimal"
                    + " point patterns.", "42601");
        }
    }

    // ------------------------------------------------------------- tokenizing

    private static List<Node> parse(String fmt, Desc d) {
        List<Node> out = new ArrayList<Node>();
        int i = 0;
        while (i < fmt.length()) {
            char c = fmt.charAt(i);
            if (c == '"') {
                i++;
                while (i < fmt.length() && fmt.charAt(i) != '"') {
                    if (fmt.charAt(i) == '\\' && i + 1 < fmt.length()) i++;
                    out.add(new Node(T_CHAR, fmt.charAt(i), false));
                    i++;
                }
                if (i < fmt.length()) i++;
                continue;
            }
            String kw = null;
            for (int k = 0; k < KEYWORDS.length; k++) {
                if (fmt.startsWith(KEYWORDS[k], i)) {
                    kw = KEYWORDS[k];
                    break;
                }
            }
            if (kw == null) {
                if (c == '\\' && i + 1 < fmt.length() && fmt.charAt(i + 1) == '"') {
                    i++;
                    c = '"';
                }
                out.add(new Node(T_CHAR, c, false));
                i++;
                continue;
            }
            i += kw.length();
            boolean upper = kw.equals(kw.toUpperCase(java.util.Locale.ROOT));
            String u = kw.toUpperCase(java.util.Locale.ROOT);
            if (u.equals("FM")) {
                d.fm = true;
                continue; // a flag, not a position
            }
            int type = typeOf(u, d);
            if (type == T_RN) d.romanUpper = upper;
            out.add(new Node(type, '\0', upper));
        }
        return out;
    }

    private static int typeOf(String u, Desc d) {
        // Nothing may follow the exponent, so this outranks any other complaint about the template.
        if (d.eeee) throw fmtError("\"EEEE\" must be the last pattern used");
        if (u.equals("9")) {
            if (d.bracket) throw fmtError("\"9\" must be ahead of \"PR\"");
            if (d.multiFlag) {
                d.multi++;
            } else if (d.decimal) {
                d.post++;
            } else {
                d.pre++;
            }
            return T_9;
        }
        if (u.equals("0")) {
            if (d.bracket) throw fmtError("\"0\" must be ahead of \"PR\"");
            if (!d.zero && !d.decimal) {
                d.zero = true;
                d.zeroStart = d.pre;
            }
            if (d.decimal) d.post++;
            else d.pre++;
            d.zeroEnd = d.pre + d.post;
            return T_0;
        }
        if (u.equals(".")) {
            markDecimal(d);
            return T_DEC;
        }
        if (u.equals("D")) {
            markDecimal(d);
            return T_D;
        }
        if (u.equals(",")) return T_COMMA;
        if (u.equals("G")) return T_G;
        if (u.equals("L")) return T_L;
        if (u.equals("C")) return T_C;
        if (u.equals("B")) return T_B;
        if (u.equals("V")) {
            if (d.decimal) throw fmtError("cannot use \"V\" and decimal point together");
            d.multiFlag = true;
            return T_V;
        }
        if (u.equals("S")) {
            if (d.lsign != LSIGN_NONE) throw fmtError("cannot use \"S\" twice");
            if (d.plus || d.minus || d.bracket) {
                throw fmtError("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
            }
            if (!d.decimal) {
                d.lsign = LSIGN_PRE;
                d.preLsignNum = d.pre;
            } else if (d.lsign == LSIGN_NONE) {
                d.lsign = LSIGN_POST;
            }
            return T_S;
        }
        if (u.equals("MI")) {
            if (d.lsign != LSIGN_NONE) throw fmtError("cannot use \"S\" and \"MI\" together");
            d.minus = true;
            return T_MI;
        }
        if (u.equals("PL")) {
            if (d.lsign != LSIGN_NONE) throw fmtError("cannot use \"S\" and \"PL\" together");
            d.plus = true;
            return T_PL;
        }
        if (u.equals("SG")) {
            if (d.lsign != LSIGN_NONE) throw fmtError("cannot use \"S\" and \"SG\" together");
            d.minus = true;
            d.plus = true;
            return T_SG;
        }
        if (u.equals("PR")) {
            if (d.lsign != LSIGN_NONE || d.plus || d.minus) {
                throw fmtError("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
            }
            d.bracket = true;
            return T_PR;
        }
        if (u.equals("RN")) {
            d.roman = true;
            return T_RN;
        }
        if (u.equals("EEEE")) {
            d.eeee = true;
            return T_E;
        }
        if (u.equals("TH")) return T_TH;
        if (u.equals("SP")) return T_SP;
        return T_CHAR;
    }

    private static void markDecimal(Desc d) {
        if (d.decimal) throw fmtError("multiple decimal points");
        if (d.multiFlag) throw fmtError("cannot use \"V\" and decimal point together");
        d.decimal = true;
    }

    private static MemgresException fmtError(String message) {
        return new MemgresException(message, "42601");
    }
}
