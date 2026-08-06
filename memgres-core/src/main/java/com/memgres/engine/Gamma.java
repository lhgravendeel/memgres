package com.memgres.engine;

import java.math.BigDecimal;

/**
 * The gamma function and its logarithm, as PostgreSQL 18 answers them.
 *
 * <p>Gamma extends the factorial to the whole real line: {@code gamma(n)} is {@code (n-1)!} at a
 * positive integer, and is defined everywhere except at zero and the negative integers, where it
 * has poles. Java has neither function, so both are computed here — with the exact factorial where
 * the argument is a whole number, and with the Lanczos approximation everywhere else.
 *
 * <p>The exact path is what makes {@code gamma(5)} answer 24 rather than 23.999999999999996. An
 * approximation good to a few parts in 10^16 is good enough for the value and not for the way it
 * prints: a double is written out in as few digits as identify it, so being one bit out is
 * visible. Where the answer is a whole number that a double holds exactly, it is computed as one.
 *
 * <p>PostgreSQL reports the poles, and any argument whose answer is past what a double holds, as
 * an overflow rather than as infinity or a domain error — including the arguments so near zero
 * that gamma of them overflows.
 */
final class Gamma {

    private Gamma() {
    }

    /**
     * The Lanczos coefficients at g = 607/128, which carry about fifteen significant digits — the
     * set used by the implementations PostgreSQL's own C library gamma is built on.
     */
    private static final double G = 607.0 / 128.0;

    private static final double[] LANCZOS = {
        0.99999999999999709182,
        57.156235665862923517,
        -59.597960355475491248,
        14.136097974741747174,
        -0.49191381609762019978,
        0.33994649984811888699e-4,
        0.46523628927048575665e-4,
        -0.98374475304879564677e-4,
        0.15808870322491248884e-3,
        -0.21026444172410488319e-3,
        0.21743961811521264320e-3,
        -0.16431810653676389022e-3,
        0.84418223983852743293e-4,
        -0.26190838401581408670e-4,
        0.36899182659531622704e-5,
    };

    private static final double LOG_SQRT_TWO_PI = 0.91893853320467274178;

    /** Beyond this, {@code (x-1)!} is past what a double holds and gamma overflows. */
    private static final int LARGEST_EXACT_ARGUMENT = 171;

    /** The value PostgreSQL reports when a result is past what a double holds. */
    private static MemgresException overflow() {
        return new MemgresException("value out of range: overflow", "22003");
    }

    /**
     * {@code gamma(x)}. Infinity at the input is infinity at the output, NaN is NaN, and a pole or
     * a result too large to hold is an overflow.
     */
    static double gamma(double x) {
        if (Double.isNaN(x)) return Double.NaN;
        if (Double.isInfinite(x)) {
            if (x > 0) return Double.POSITIVE_INFINITY;
            throw overflow();          // gamma has no limit as x runs to minus infinity
        }
        if (x == Math.floor(x)) {
            if (x <= 0) throw overflow();                       // the poles
            if (x <= LARGEST_EXACT_ARGUMENT) return factorial((int) x - 1);
            throw overflow();
        }
        double result = gammaAt(x);
        if (Double.isInfinite(result) || Double.isNaN(result)) throw overflow();
        return result;
    }

    /**
     * {@code lgamma(x)}, the natural logarithm of the absolute value of gamma. It grows slowly
     * enough that only the poles overflow.
     */
    static double lgamma(double x) {
        if (Double.isNaN(x)) return Double.NaN;
        if (Double.isInfinite(x)) return Double.POSITIVE_INFINITY;
        if (x == Math.floor(x) && x <= 0) throw overflow();     // the same poles
        if (x == 1.0 || x == 2.0) return 0.0;                   // gamma is 1 at both, so its log is 0
        double result = lgammaAt(x);
        if (Double.isInfinite(result) || Double.isNaN(result)) throw overflow();
        return result;
    }

    /**
     * {@code n!} as the double nearest it. Computed whole and rounded once, so that the answer is
     * the same one a correctly rounded gamma gives rather than the accumulation of 170 roundings.
     */
    private static double factorial(int n) {
        BigDecimal product = BigDecimal.ONE;
        for (int i = 2; i <= n; i++) {
            product = product.multiply(BigDecimal.valueOf(i));
        }
        return product.doubleValue();
    }

    private static double gammaAt(double x) {
        // Below the half line the reflection formula gives the answer from the value above it,
        // which is where the approximation is accurate.
        if (x < 0.5) {
            return Math.PI / (Math.sin(Math.PI * x) * gammaAt(1 - x));
        }
        double z = x - 1;
        double t = z + G + 0.5;
        return Math.sqrt(2 * Math.PI) * Math.pow(t, z + 0.5) * Math.exp(-t) * series(z);
    }

    /** How near 1 or 2 the argument has to be for the series below to be the accurate route. */
    private static final double NEAR = 0.1;

    /** Euler's constant, and the zeta values the series about 1 is written with. */
    private static final double EULER = 0.5772156649015329;

    private static final double[] ZETA = {
        1.6449340668482264, 1.2020569031595943, 1.0823232337111382, 1.0369277551433699,
        1.0173430619844491, 1.0083492773819228, 1.0040773561979443, 1.0020083928260822,
        1.0009945751278181, 1.0004941886041195, 1.0002460865533080, 1.0001227133475785,
        1.0000612481350587, 1.0000305882363070, 1.0000152822594086, 1.0000076371976379,
        1.0000038172932650, 1.0000019082127166, 1.0000009539620339, 1.0000004769329868,
    };

    /**
     * {@code ln gamma(1+z)} for a small z, summed directly rather than taken as the logarithm of
     * a value near 1.
     *
     * <p>Gamma is 1 at both 1 and 2, so its logarithm is 0 there and everything near it is a small
     * difference of larger numbers. Computing it the general way lost most of the answer: lgamma
     * of 1.0000001 came out with nine correct digits where a double holds sixteen.
     */
    private static double lgammaNearOne(double z) {
        double sum = -EULER * z;
        double power = z;
        for (int k = 2; k <= ZETA.length + 1; k++) {
            power *= z;
            double term = ZETA[k - 2] / k * power;
            sum += (k % 2 == 0) ? term : -term;
        }
        return sum;
    }

    private static double lgammaAt(double x) {
        if (Math.abs(x - 1) <= NEAR) return lgammaNearOne(x - 1);
        if (Math.abs(x - 2) <= NEAR) return lgammaNearOne(x - 2) + Math.log1p(x - 2);
        if (x < 0.5) {
            // The same reflection, taken in logarithms so that a large argument does not have to
            // pass through a value gamma itself could not hold.
            return Math.log(Math.PI / Math.abs(Math.sin(Math.PI * x))) - lgammaAt(1 - x);
        }
        double z = x - 1;
        double t = z + G + 0.5;
        return LOG_SQRT_TWO_PI + (z + 0.5) * Math.log(t) - t + Math.log(series(z));
    }

    private static double series(double z) {
        double sum = LANCZOS[0];
        for (int i = 1; i < LANCZOS.length; i++) {
            sum += LANCZOS[i] / (z + i);
        }
        return sum;
    }
}
