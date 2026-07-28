package com.memgres.engine;

/**
 * PostgreSQL's degree-based trigonometry.
 *
 * <p>Converting degrees to radians and calling the radian function gets the exact cases wrong:
 * {@code sind(30)} comes out 0.49999999999999994 and {@code tand(90)} a finite 1.6e16 rather than
 * infinity, because 30 degrees has no exact radian measure. PostgreSQL instead stitches each
 * quadrant together from the one value it can pin down — {@code sin(30)} for the sines,
 * {@code 1-cos(60)} for the cosines — so the quarter-turn multiples come out exactly, and reduces
 * every argument into the first quadrant first so {@code tand(90)} divides one by an exact zero.
 */
final class DegreeTrig {

    private static final double RADIANS_PER_DEGREE = Math.PI / 180.0;

    private static final double SIN_30 = Math.sin(30.0 * RADIANS_PER_DEGREE);
    private static final double ONE_MINUS_COS_60 = 1.0 - Math.cos(60.0 * RADIANS_PER_DEGREE);
    private static final double ASIN_0_5 = Math.asin(0.5);
    private static final double ACOS_0_5 = Math.acos(0.5);
    private static final double ATAN_1_0 = Math.atan(1.0);
    private static final double TAN_45 = sindQ1(45.0) / cosdQ1(45.0);
    private static final double COT_45 = cosdQ1(45.0) / sindQ1(45.0);

    private DegreeTrig() {}

    /** sin(x) for x in [0, 30] degrees, normalised so sind(30) is exactly one half. */
    private static double sind0To30(double x) {
        return (Math.sin(x * RADIANS_PER_DEGREE) / SIN_30) / 2.0;
    }

    /** cos(x) for x in [0, 60] degrees, normalised so cosd(60) is exactly one half. */
    private static double cosd0To60(double x) {
        return 1.0 - ((1.0 - Math.cos(x * RADIANS_PER_DEGREE)) / ONE_MINUS_COS_60) / 2.0;
    }

    private static double sindQ1(double x) {
        return x <= 30.0 ? sind0To30(x) : cosd0To60(90.0 - x);
    }

    private static double cosdQ1(double x) {
        return x <= 60.0 ? cosd0To60(x) : sind0To30(90.0 - x);
    }

    static double sind(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        if (Double.isInfinite(arg)) throw NumericLimits.inputOutOfRange();
        double sign = 1.0;
        double x = arg % 360.0;
        if (x < 0) { x = -x; sign = -sign; }
        if (x > 180.0) { x -= 180.0; sign = -sign; }
        if (x > 90.0) x = 180.0 - x;
        return sign * sindQ1(x);
    }

    static double cosd(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        if (Double.isInfinite(arg)) throw NumericLimits.inputOutOfRange();
        double sign = 1.0;
        double x = arg % 360.0;
        if (x < 0) x = -x;
        if (x > 180.0) x = 360.0 - x;
        if (x > 90.0) { x = 180.0 - x; sign = -sign; }
        return sign * cosdQ1(x);
    }

    static double tand(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        if (Double.isInfinite(arg)) throw NumericLimits.inputOutOfRange();
        double sign = 1.0;
        double x = arg % 360.0;
        if (x < 0) { x = -x; sign = -sign; }
        if (x > 180.0) { x -= 180.0; sign = -sign; }
        if (x > 90.0) { x = 180.0 - x; sign = -sign; }
        double result = sign * ((sindQ1(x) / cosdQ1(x)) / TAN_45);
        // The reduction can leave a negative zero, which PG normalises away.
        return result == 0.0 ? 0.0 : result;
    }

    static double cotd(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        if (Double.isInfinite(arg)) throw NumericLimits.inputOutOfRange();
        double sign = 1.0;
        double x = arg % 360.0;
        if (x < 0) { x = -x; sign = -sign; }
        if (x > 180.0) { x -= 180.0; sign = -sign; }
        if (x > 90.0) { x = 180.0 - x; sign = -sign; }
        double result = sign * ((cosdQ1(x) / sindQ1(x)) / COT_45);
        return result == 0.0 ? 0.0 : result;
    }

    /**
     * asin(x) in degrees for x in [0, 1], stitched from asin below a half and acos above it —
     * both give exactly 30 at a half, and each is exact at the end of its own range.
     */
    private static double asindQ1(double x) {
        if (x <= 0.5) return (Math.asin(x) / ASIN_0_5) * 30.0;
        return 90.0 - (Math.acos(x) / ACOS_0_5) * 60.0;
    }

    /** acos(x) in degrees for x in [0, 1], stitched the same way. */
    private static double acosdQ1(double x) {
        if (x <= 0.5) return 90.0 - (Math.asin(x) / ASIN_0_5) * 30.0;
        return (Math.acos(x) / ACOS_0_5) * 60.0;
    }

    static double asind(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        if (arg < -1.0 || arg > 1.0) throw NumericLimits.inputOutOfRange();
        return arg >= 0 ? asindQ1(arg) : -asindQ1(-arg);
    }

    static double acosd(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        if (arg < -1.0 || arg > 1.0) throw NumericLimits.inputOutOfRange();
        return arg >= 0 ? acosdQ1(arg) : 90.0 + asindQ1(-arg);
    }

    static double atand(double arg) {
        if (Double.isNaN(arg)) return Double.NaN;
        return (Math.atan(arg) / ATAN_1_0) * 45.0;
    }

    static double atan2d(double y, double x) {
        if (Double.isNaN(y) || Double.isNaN(x)) return Double.NaN;
        return (Math.atan2(y, x) / ATAN_1_0) * 45.0;
    }
}
