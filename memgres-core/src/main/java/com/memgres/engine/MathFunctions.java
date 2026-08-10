package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import java.math.BigDecimal;

/**
 * Math function evaluation, extracted from FunctionEvaluator to reduce class size.
 *
 * <p>PostgreSQL offers most of these over both {@code numeric} and {@code double precision}, and
 * which one a call resolves to decides both the answer and its type: {@code round} on a
 * {@code double} rounds half to even, on a {@code numeric} half away from zero. Anything that is
 * not already a {@code numeric} — an integer, an untyped literal, a {@code real} — resolves to the
 * {@code double precision} form, because {@code float8} is the preferred type of the numeric
 * category. {@link #isNumeric} is that decision, made from the value the argument produced.
 */
class MathFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    MathFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    /** True when the argument is a {@code numeric}; everything else resolves to float8. */
    private static boolean isNumeric(Object v) {
        return v instanceof BigDecimal;
    }

    /** A NaN or infinity carried in a numeric answers with itself from most of these. */
    private static Double special(Object v) {
        return NumericLimits.isSpecial(v) ? Double.valueOf(((Number) v).doubleValue()) : null;
    }

    // DRY helper: evaluate a single-arg float8 math function with null guard
    private Object mathUnary(FunctionCallExpr fn, RowContext ctx, java.util.function.DoubleUnaryOperator op) {
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        return arg == null ? null : Double.valueOf(op.applyAsDouble(executor.toDouble(arg)));
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "abs": {
                if (fn.args().size() < 1) {
                    throw new MemgresException(
                        "function abs() does not exist", "42883");
                }
                if (fn.args().size() > 1) {
                    throw new MemgresException("function abs() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                Object absolute = NumericLimits.absExact(arg);
                return absolute != null ? absolute : Double.valueOf(Math.abs(executor.toDouble(arg)));
            }
            case "ceil":
            case "ceiling":
                return roundingUnary(fn, ctx, java.math.RoundingMode.CEILING);
            case "floor":
                return roundingUnary(fn, ctx, java.math.RoundingMode.FLOOR);
            case "round": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                Double sp = special(arg);
                if (sp != null) return sp;
                if (fn.args().size() > 1) {
                    Object scaleArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (scaleArg == null) return null;
                    int scale = executor.toInt(scaleArg);
                    return toBigDecimal(arg).setScale(scale, java.math.RoundingMode.HALF_UP);
                }
                // numeric rounds half away from zero; float8 rounds half to even, which is what
                // rint does — round(2.5::float8) is 2 and round(3.5::float8) is 4.
                if (isNumeric(arg)) {
                    return ((BigDecimal) arg).setScale(0, java.math.RoundingMode.HALF_UP);
                }
                return Double.valueOf(Math.rint(executor.toDouble(arg)));
            }
            case "random": {
                if (fn.args().size() == 2) {
                    // random(min, max) → random integer in [min, max] inclusive
                    long min = executor.toLong(executor.evalExpr(fn.args().get(0), ctx));
                    long max = executor.toLong(executor.evalExpr(fn.args().get(1), ctx));
                    if (min > max) throw new com.memgres.engine.MemgresException(
                            "lower bound must be less than or equal to upper bound", "22023");
                    return min + (long) (Math.random() * (max - min + 1));
                }
                return Math.random();
            }
            case "random_normal": {
                // random_normal(mean, stddev) → returns random normal distributed value
                double mean = 0.0;
                double stddev = 1.0;
                if (fn.args().size() >= 1) {
                    mean = executor.toDouble(executor.evalExpr(fn.args().get(0), ctx));
                }
                if (fn.args().size() >= 2) {
                    stddev = executor.toDouble(executor.evalExpr(fn.args().get(1), ctx));
                }
                return mean + stddev * new java.util.Random().nextGaussian();
            }
            case "setseed": {
                // The seed is not kept, but it is a double precision, and something that cannot
                // be read as one is not a seed that was ignored — it is not a seed.
                Object seed = executor.evalExpr(fn.args().get(0), ctx);
                if (seed != null) {
                    double value = executor.exprEvaluator.toDouble(seed);
                    // A seed is a fraction of the whole range; one outside it is not a seed.
                    if (value < -1 || value > 1) {
                        throw new com.memgres.engine.MemgresException("setseed parameter "
                                + String.valueOf(seed) + " is out of allowed range [-1,1]", "22023");
                    }
                }
                return "";
            }
            case "trunc": {
                rejectAllUnknownArguments("trunc", fn);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                // macaddr/macaddr8 trunc
                if (arg instanceof MacaddrValue) return ((MacaddrValue) arg).trunc();
                if (arg instanceof Macaddr8Value) return ((Macaddr8Value) arg).trunc();
                Double sp = special(arg);
                if (sp != null) return sp;
                if (fn.args().size() > 1) {
                    Object scaleArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (scaleArg == null) return null;
                    int scale = executor.toInt(scaleArg);
                    return toBigDecimal(arg).setScale(scale, java.math.RoundingMode.DOWN);
                }
                if (isNumeric(arg)) {
                    return ((BigDecimal) arg).setScale(0, java.math.RoundingMode.DOWN);
                }
                double truncVal = executor.toDouble(arg);
                return Double.valueOf(truncVal < 0 ? Math.ceil(truncVal) : Math.floor(truncVal));
            }
            case "mod": {
                rejectAllUnknownArguments("mod", fn);
                Object a = readAs(executor.evalExpr(fn.args().get(0), ctx),
                        executor.evalExpr(fn.args().get(1), ctx));
                Object b = readAs(executor.evalExpr(fn.args().get(1), ctx),
                        executor.evalExpr(fn.args().get(0), ctx));
                if (a == null || b == null) return null;
                if (a instanceof Integer && b instanceof Integer) return ((Integer) a) % ((Integer) b);
                // A numeric NaN or infinity is carried as the matching double, which has no
                // BigDecimal form; IEEE remainder gives the NaN PG's numeric_mod also returns.
                if (NumericLimits.isSpecial(a) || NumericLimits.isSpecial(b)) {
                    return executor.toDouble(a) % executor.toDouble(b);
                }
                if (a instanceof BigDecimal || b instanceof BigDecimal) {
                    BigDecimal bdA = a instanceof BigDecimal ? (BigDecimal) a : new BigDecimal(a.toString());
                    BigDecimal bdB = b instanceof BigDecimal ? (BigDecimal) b : new BigDecimal(b.toString());
                    return bdA.remainder(bdB);
                }
                return executor.toLong(a) % executor.toLong(b);
            }
            case "power":
            case "pow": {
                Object base = executor.evalExpr(fn.args().get(0), ctx);
                Object exp = executor.evalExpr(fn.args().get(1), ctx);
                if (base == null || exp == null) return null;
                boolean specialOperand = NumericLimits.isSpecial(base) || NumericLimits.isSpecial(exp);
                if (specialOperand) {
                    // PG answers a plain 1 for any power of zero and any power of one, NaN
                    // operands included, before it looks at the special at all.
                    if (isZero(exp) || isOne(base)) return BigDecimal.ONE;
                    return Math.pow(executor.toDouble(base), executor.toDouble(exp));
                }
                if (isNumeric(base) || isNumeric(exp)) {
                    return NumericMath.power(toBigDecimal(base), toBigDecimal(exp));
                }
                double b = executor.toDouble(base);
                double e = executor.toDouble(exp);
                NumericLimits.checkPowerDomain(b, e);
                return NumericLimits.checkFloat8(Math.pow(b, e), b, e);
            }
            case "sqrt": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof String && !FunctionEvaluator.isNumericString(((String) arg))) {
                    throw new MemgresException("function sqrt(text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Double sp = special(arg);
                if (sp != null) {
                    if (sp.doubleValue() < 0) {
                        throw new MemgresException("cannot take square root of a negative number", "2201F");
                    }
                    return sp;
                }
                if (isNumeric(arg)) return NumericMath.sqrt((BigDecimal) arg);
                double dv = executor.toDouble(arg);
                if (dv < 0) throw new MemgresException("cannot take square root of a negative number", "2201F");
                return Double.valueOf(Math.sqrt(dv));
            }
            case "cbrt":
                return mathUnary(fn, ctx, Math::cbrt);
            case "gamma":
            case "lgamma": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double x = executor.toDouble(arg);
                return name.equals("gamma") ? Gamma.gamma(x) : Gamma.lgamma(x);
            }
            case "log10":
            case "log": {
                // log(base, x) or log(x) [base 10]
                if (fn.args().size() == 1) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg == null) return null;
                    Double sp = special(arg);
                    if (sp != null) return logSpecial(sp);
                    if (isNumeric(arg)) return NumericMath.log(BigDecimal.TEN, (BigDecimal) arg);
                    double dv = executor.toDouble(arg);
                    NumericLimits.checkLogDomain(dv);
                    return Double.valueOf(Math.log10(dv));
                }
                Object base = executor.evalExpr(fn.args().get(0), ctx);
                Object val = executor.evalExpr(fn.args().get(1), ctx);
                if (base == null || val == null) return null;
                // PG has only log(numeric, numeric): the two-argument form answers in numeric
                // whatever the arguments were written as.
                if (special(base) == null && special(val) == null) {
                    return NumericMath.log(toBigDecimal(base), toBigDecimal(val));
                }
                double bd = executor.toDouble(base);
                double vd = executor.toDouble(val);
                NumericLimits.checkLogDomain(bd);
                NumericLimits.checkLogDomain(vd);
                double lnBase = Math.log(bd);
                // PG's log(base, x) is ln(x)/ln(base), so a base of one divides by zero
                if (lnBase == 0) throw new MemgresException("division by zero", "22012");
                return Double.valueOf(Math.log(vd) / lnBase);
            }
            case "ln": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                Double sp = special(arg);
                if (sp != null) return logSpecial(sp);
                if (isNumeric(arg)) return NumericMath.ln((BigDecimal) arg);
                double dv = executor.toDouble(arg);
                NumericLimits.checkLogDomain(dv);
                return Double.valueOf(Math.log(dv));
            }
            case "exp": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                Double sp = special(arg);
                if (sp != null) return Double.valueOf(Math.exp(sp.doubleValue()));
                if (isNumeric(arg)) return NumericMath.exp((BigDecimal) arg);
                double dv = executor.toDouble(arg);
                return Double.valueOf(NumericLimits.checkExp(Math.exp(dv), dv));
            }
            case "sign": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                Double sp = special(arg);
                if (sp != null) return sp.isNaN() ? sp : Double.valueOf(sp.doubleValue() > 0 ? 1 : -1);
                if (isNumeric(arg)) return BigDecimal.valueOf(((BigDecimal) arg).signum());
                double d = executor.toDouble(arg);
                return Double.valueOf(d > 0 ? 1 : (d < 0 ? -1 : 0));
            }
            case "pi":
                return Math.PI;
            case "degrees":
                return mathUnary(fn, ctx, Math::toDegrees);
            case "radians":
                return mathUnary(fn, ctx, Math::toRadians);
            case "sin":
                return mathUnary(fn, ctx, Math::sin);
            case "cos":
                return mathUnary(fn, ctx, Math::cos);
            case "tan":
                return mathUnary(fn, ctx, Math::tan);
            case "cot":
                return mathUnary(fn, ctx, x -> 1.0 / Math.tan(x));
            case "asin":
                return mathUnary(fn, ctx, x -> { NumericLimits.checkUnitInterval(x); return Math.asin(x); });
            case "acos":
                return mathUnary(fn, ctx, x -> { NumericLimits.checkUnitInterval(x); return Math.acos(x); });
            case "atan":
                return mathUnary(fn, ctx, Math::atan);
            case "atan2": {
                Object y = executor.evalExpr(fn.args().get(0), ctx);
                Object x = executor.evalExpr(fn.args().get(1), ctx);
                if (y == null || x == null) return null;
                return Double.valueOf(Math.atan2(executor.toDouble(y), executor.toDouble(x)));
            }
            case "sind":
                return mathUnary(fn, ctx, DegreeTrig::sind);
            case "cosd":
                return mathUnary(fn, ctx, DegreeTrig::cosd);
            case "tand":
                return mathUnary(fn, ctx, DegreeTrig::tand);
            case "cotd":
                return mathUnary(fn, ctx, DegreeTrig::cotd);
            case "asind":
                return mathUnary(fn, ctx, DegreeTrig::asind);
            case "acosd":
                return mathUnary(fn, ctx, DegreeTrig::acosd);
            case "atand":
                return mathUnary(fn, ctx, DegreeTrig::atand);
            case "atan2d": {
                Object y = executor.evalExpr(fn.args().get(0), ctx);
                Object x = executor.evalExpr(fn.args().get(1), ctx);
                if (y == null || x == null) return null;
                return Double.valueOf(DegreeTrig.atan2d(executor.toDouble(y), executor.toDouble(x)));
            }
            case "sinh":
                return mathUnary(fn, ctx, Math::sinh);
            case "cosh":
                return mathUnary(fn, ctx, Math::cosh);
            case "tanh":
                return mathUnary(fn, ctx, Math::tanh);
            case "asinh":
                return mathUnary(fn, ctx, x -> Math.log(x + Math.sqrt(x * x + 1)));
            case "acosh":
                return mathUnary(fn, ctx, x -> {
                    // acosh is defined on [1, inf); PG raises below it rather than answering NaN.
                    if (!Double.isNaN(x) && x < 1.0) throw NumericLimits.inputOutOfRange();
                    return Math.log(x + Math.sqrt(x * x - 1));
                });
            case "atanh":
                return mathUnary(fn, ctx, x -> {
                    // atanh is defined on [-1, 1] and infinite at the ends, undefined past them.
                    if (!Double.isNaN(x) && Math.abs(x) > 1.0) throw NumericLimits.inputOutOfRange();
                    return 0.5 * Math.log((1 + x) / (1 - x));
                });
            case "factorial": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                long n = executor.toLong(arg);
                if (n < 0) throw new MemgresException("factorial of a negative number is undefined", "22003");
                // 32177! is the widest factorial that still fits numeric's 131072 integral digits;
                // past it PG refuses rather than build a number it could not store.
                if (n > 32177) throw NumericLimits.valueOverflowsNumeric();
                // Use BigDecimal for exact large results (PG returns numeric for factorial)
                java.math.BigDecimal result = java.math.BigDecimal.ONE;
                for (long i = 2; i <= n; i++) result = result.multiply(java.math.BigDecimal.valueOf(i));
                return result;
            }
            case "div": {
                // PG's div(numeric, numeric) is the integral part of the quotient, as a numeric.
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                Double sa = special(a);
                Double sb = special(b);
                if (sa != null || sb != null) {
                    if ((sa != null && sa.isNaN()) || (sb != null && sb.isNaN())) return Double.valueOf(Double.NaN);
                    if (sb != null) {
                        // A finite dividend over an infinite divisor truncates to zero; two
                        // infinities have no integral quotient at all.
                        if (sa != null) return Double.valueOf(Double.NaN);
                        return BigDecimal.ZERO;
                    }
                    return sa;
                }
                BigDecimal bdB = toBigDecimal(b);
                if (bdB.signum() == 0) throw new MemgresException("division by zero", "22012");
                return toBigDecimal(a).divideToIntegralValue(bdB).setScale(0, java.math.RoundingMode.DOWN);
            }
            case "gcd": {
                rejectAllUnknownArguments("gcd", fn);
                Object av = readAs(executor.evalExpr(fn.args().get(0), ctx),
                        executor.evalExpr(fn.args().get(1), ctx));
                Object bv = readAs(executor.evalExpr(fn.args().get(1), ctx),
                        executor.evalExpr(fn.args().get(0), ctx));
                if (av == null || bv == null) return null;
                // numeric has no int8 ceiling, so |-9223372036854775808| is a perfectly good gcd
                if (isNumeric(av) || isNumeric(bv)) {
                    BigDecimal x = toBigDecimal(av);
                    BigDecimal y = toBigDecimal(bv);
                    while (y.signum() != 0) {
                        BigDecimal t = y;
                        y = x.remainder(y);
                        x = t;
                    }
                    return NumericLimits.check(x.abs());
                }
                long a = executor.toLong(av);
                long b = executor.toLong(bv);
                while (b != 0) { long t = b; b = a % b; a = t; }
                // gcd is |a|, which the two's-complement minimum cannot represent
                return NumericLimits.narrowToIntegerType(NumericLimits.absExactLong(a, av, bv), av, bv);
            }
            case "lcm": {
                rejectAllUnknownArguments("lcm", fn);
                Object av = readAs(executor.evalExpr(fn.args().get(0), ctx),
                        executor.evalExpr(fn.args().get(1), ctx));
                Object bv = readAs(executor.evalExpr(fn.args().get(1), ctx),
                        executor.evalExpr(fn.args().get(0), ctx));
                if (av == null || bv == null) return null;
                if (isNumeric(av) || isNumeric(bv)) {
                    BigDecimal x = toBigDecimal(av);
                    BigDecimal y = toBigDecimal(bv);
                    if (x.signum() == 0 || y.signum() == 0) return BigDecimal.ZERO;
                    BigDecimal g = x;
                    BigDecimal t2 = y;
                    while (t2.signum() != 0) {
                        BigDecimal t = t2;
                        t2 = g.remainder(t2);
                        g = t;
                    }
                    return NumericLimits.check(x.divide(g).multiply(y).abs());
                }
                long a = executor.toLong(av);
                long b = executor.toLong(bv);
                if (a == 0 || b == 0) return NumericLimits.narrowToIntegerType(0L, av, bv);
                long gcd = a;
                long temp = b;
                while (temp != 0) { long t = temp; temp = gcd % temp; gcd = t; }
                long result;
                try {
                    result = Math.multiplyExact(
                            NumericLimits.absExactLong(a / gcd, av, bv),
                            NumericLimits.absExactLong(b, av, bv));
                } catch (ArithmeticException e) {
                    throw NumericLimits.integerOutOfRange(NumericLimits.widestIntegerType(av, bv));
                }
                return NumericLimits.narrowToIntegerType(result, av, bv);
            }
            case "scale": {
                // PG: scale(numeric) -> integer: number of decimal digits in the fractional part.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                // NaN and the infinities have no digits, so they have no scale to report either.
                if (NumericLimits.isSpecial(arg)) return null;
                BigDecimal bd = toBigDecimal(arg);
                return Math.max(0, bd.scale());
            }
            case "min_scale": {
                // PG: min_scale(numeric) -> smallest scale that preserves the value.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (NumericLimits.isSpecial(arg)) return null;
                BigDecimal bd = toBigDecimal(arg);
                BigDecimal stripped = bd.stripTrailingZeros();
                return Math.max(0, stripped.scale());
            }
            case "trim_scale": {
                // PG: trim_scale(numeric) -> numeric with trailing zeros stripped from fractional part.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (NumericLimits.isSpecial(arg)) return arg;
                BigDecimal bd = toBigDecimal(arg);
                BigDecimal stripped = bd.stripTrailingZeros();
                // Match PG: zero scale means no fractional part, not scientific notation like 1E+2
                if (stripped.scale() < 0) stripped = stripped.setScale(0);
                return stripped;
            }
            case "width_bucket": {
                if (fn.args().size() == 2) {
                    // Array variant: width_bucket(operand, thresholds_array)
                    Object operand = executor.evalExpr(fn.args().get(0), ctx);
                    Object arrayArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (operand == null || arrayArg == null) return null;
                    double val = executor.toDouble(operand);
                    java.util.List<?> thresholds;
                    if (arrayArg instanceof java.util.List<?>) {
                        thresholds = (java.util.List<?>) arrayArg;
                    } else if (arrayArg instanceof Object[]) {
                        thresholds = java.util.Arrays.asList((Object[]) arrayArg);
                    } else {
                        throw new com.memgres.engine.MemgresException(
                            "width_bucket second argument must be an array", "42804");
                    }
                    // PostgreSQL finds the bucket by bisection, not by walking the thresholds in
                    // order. The two agree on a sorted array and disagree on every other one, and
                    // the answer for an unsorted array is the one bisection gives.
                    int left = 0;
                    int right = thresholds.size();
                    while (left < right) {
                        int mid = (left + right) / 2;
                        if (val < executor.toDouble(thresholds.get(mid))) right = mid;
                        else left = mid + 1;
                    }
                    return left;
                }
                Object operand = executor.evalExpr(fn.args().get(0), ctx);
                Object lower = executor.evalExpr(fn.args().get(1), ctx);
                Object upper = executor.evalExpr(fn.args().get(2), ctx);
                Object countArg = executor.evalExpr(fn.args().get(3), ctx);
                // A histogram over a range of no width has no buckets to divide it into.
                if (lower != null && upper != null
                        && executor.toDouble(lower) == executor.toDouble(upper)) {
                    throw new com.memgres.engine.MemgresException(
                            "lower bound cannot equal upper bound", "2201G");
                }
                if (operand == null || lower == null || upper == null || countArg == null) return null;
                double val = executor.toDouble(operand);
                double lo = executor.toDouble(lower);
                double hi = executor.toDouble(upper);
                // A NaN belongs in no bucket, and PG says so rather than picking one.
                if (Double.isNaN(val) || Double.isNaN(lo) || Double.isNaN(hi)) {
                    throw new MemgresException(
                            "operand, lower bound, and upper bound cannot be NaN", "2201G");
                }
                int count = executor.toInt(countArg);
                // There is no bucket to fall into when none were asked for, and dividing by the
                // count would be a division by zero besides.
                if (count <= 0) {
                    throw new MemgresException("count must be greater than zero", "2201G");
                }
                if (val < lo) return 0;
                if (val >= hi) return count + 1;
                return (int) ((val - lo) / (hi - lo) * count) + 1;
            }
            default:
                return NOT_HANDLED;
        }
    }

    /**
     * Refuse a call whose arguments are all untyped literals, for the routines whose overloads
     * give such a call no single reading. {@code round('2.5')} resolves to float8, because float8
     * is the numeric category's preferred type and there is a {@code round(float8)}; but
     * {@code trunc} has a macaddr overload outside that category and {@code mod}, {@code gcd} and
     * {@code lcm} have no float8 form at all, so PostgreSQL reports the ambiguity instead of
     * picking one. One typed argument is enough to settle it, so only an all-untyped call fails.
     */
    private static void rejectAllUnknownArguments(String name, FunctionCallExpr fn) {
        if (fn.args().isEmpty()) return;
        StringBuilder unknowns = new StringBuilder();
        for (Expression arg : fn.args()) {
            if (!(arg instanceof Literal)
                    || ((Literal) arg).literalType() != Literal.LiteralType.STRING) return;
            if (unknowns.length() > 0) unknowns.append(", ");
            unknowns.append("unknown");
        }
        throw new MemgresException("function " + name + "(" + unknowns + ") is not unique"
                + "\n  Hint: Could not choose a best candidate function."
                + " You might need to add explicit type casts.", "42725");
    }

    /** ceil and floor, which keep numeric a numeric and answer in float8 for everything else. */
    private Object roundingUnary(FunctionCallExpr fn, RowContext ctx, java.math.RoundingMode mode) {
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        if (arg == null) return null;
        Double sp = special(arg);
        if (sp != null) return sp;
        if (isNumeric(arg)) return ((BigDecimal) arg).setScale(0, mode);
        double d = executor.toDouble(arg);
        return Double.valueOf(mode == java.math.RoundingMode.CEILING ? Math.ceil(d) : Math.floor(d));
    }

    /** ln and log of a numeric special: PG passes NaN and positive infinity straight through. */
    private static Object logSpecial(Double sp) {
        if (sp.isNaN()) return sp;
        if (sp.doubleValue() > 0) return sp;
        throw new MemgresException("cannot take logarithm of a negative number", "2201E");
    }

    /**
     * Read an untyped literal in the type the other argument brought. {@code mod('5', 2)} is
     * {@code mod(integer, integer)} in PostgreSQL, and answers with an integer; without this the
     * unknown side stays text and the whole call widens to bigint.
     */
    private static Object readAs(Object value, Object model) {
        if (!(value instanceof String) || model == null || model instanceof String) return value;
        if (model instanceof Short) return TypeCoercion.coerce(value, DataType.SMALLINT);
        if (model instanceof Integer) return TypeCoercion.coerce(value, DataType.INTEGER);
        if (model instanceof Long) return TypeCoercion.coerce(value, DataType.BIGINT);
        if (model instanceof BigDecimal) return TypeCoercion.toBigDecimal(value);
        if (model instanceof Double || model instanceof Float) {
            return TypeCoercion.coerce(value, DataType.DOUBLE_PRECISION);
        }
        return value;
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal) return (BigDecimal) v;
        return TypeCoercion.toBigDecimal(v);
    }

    private static boolean isZero(Object v) {
        if (v instanceof BigDecimal) return ((BigDecimal) v).signum() == 0;
        return v instanceof Number && !NumericLimits.isSpecial(v) && ((Number) v).doubleValue() == 0.0;
    }

    private static boolean isOne(Object v) {
        if (v instanceof BigDecimal) return ((BigDecimal) v).compareTo(BigDecimal.ONE) == 0;
        return v instanceof Number && !NumericLimits.isSpecial(v) && ((Number) v).doubleValue() == 1.0;
    }
}
