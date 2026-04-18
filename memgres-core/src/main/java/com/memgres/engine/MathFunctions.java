package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import java.math.BigDecimal;

/**
 * Math function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class MathFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    MathFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    // DRY helper: evaluate a single-arg math function with null guard
    private Object mathUnary(FunctionCallExpr fn, RowContext ctx, java.util.function.DoubleUnaryOperator op) {
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        return arg == null ? null : numericResult(op.applyAsDouble(executor.toDouble(arg)));
    }

    // DRY helper: same but returns raw double (no numericResult normalisation)
    private Object mathUnaryRaw(FunctionCallExpr fn, RowContext ctx, java.util.function.DoubleUnaryOperator op) {
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        return arg == null ? null : op.applyAsDouble(executor.toDouble(arg));
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
                if (arg instanceof Integer) return Math.abs(((Integer) arg));
                if (arg instanceof Long) return Math.abs(((Long) arg));
                if (arg instanceof BigDecimal) return ((BigDecimal) arg).abs();
                return Math.abs(executor.toDouble(arg));
            }
            case "ceil":
            case "ceiling":
                return mathUnary(fn, ctx, Math::ceil);
            case "floor":
                return mathUnary(fn, ctx, Math::floor);
            case "round": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double val = executor.toDouble(arg);
                if (fn.args().size() > 1) {
                    int scale = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                    java.math.BigDecimal bd = java.math.BigDecimal.valueOf(val)
                            .setScale(scale, java.math.RoundingMode.HALF_UP);
                    return bd;
                }
                return (long) Math.round(val);
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
                executor.evalExpr(fn.args().get(0), ctx);
                return "";
            }
            case "trunc": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                // macaddr trunc: zero out the last 3 bytes
                if (arg instanceof String) {
                    String s = ((String) arg).trim().toLowerCase();
                    if (s.matches("[0-9a-f]{2}(:[0-9a-f]{2}){5}")) {
                        String[] parts = s.split(":");
                        return parts[0] + ":" + parts[1] + ":" + parts[2] + ":00:00:00";
                    }
                }
                double val = executor.toDouble(arg);
                if (fn.args().size() > 1) {
                    int scale = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                    double factor = Math.pow(10, scale);
                    return (long) (val * factor) / factor;
                }
                return (long) val;
            }
            case "mod": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                if (a instanceof Integer && b instanceof Integer) return ((Integer) a) % ((Integer) b);
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
                return numericResult(Math.pow(executor.toDouble(base), executor.toDouble(exp)));
            }
            case "sqrt": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof String && !FunctionEvaluator.isNumericString(((String) arg))) {
                    String s = (String) arg;
                    throw new MemgresException("function sqrt(text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                double dv = executor.toDouble(arg);
                if (dv < 0) throw new MemgresException("cannot take square root of a negative number", "2201F");
                return numericResult(Math.sqrt(dv));
            }
            case "cbrt":
                return mathUnary(fn, ctx, Math::cbrt);
            case "log": {
                // log(base, x) or log(x) [base 10]
                if (fn.args().size() == 1) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg == null) return null;
                    double dv = executor.toDouble(arg);
                    if (dv == 0) throw new MemgresException("cannot take logarithm of zero", "2201E");
                    if (dv < 0) throw new MemgresException("cannot take logarithm of a negative number", "2201E");
                    return numericResult(Math.log10(dv));
                }
                Object base = executor.evalExpr(fn.args().get(0), ctx);
                Object val = executor.evalExpr(fn.args().get(1), ctx);
                if (base == null || val == null) return null;
                return numericResult(Math.log(executor.toDouble(val)) / Math.log(executor.toDouble(base)));
            }
            case "ln": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double dv = executor.toDouble(arg);
                if (dv == 0) throw new MemgresException("cannot take logarithm of zero", "2201E");
                if (dv < 0) throw new MemgresException("cannot take logarithm of a negative number", "2201F");
                return numericResult(Math.log(dv));
            }
            case "exp":
                return mathUnary(fn, ctx, Math::exp);
            case "sign": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double d = executor.toDouble(arg);
                return d > 0 ? 1 : (d < 0 ? -1 : 0);
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
            case "asin":
                return mathUnary(fn, ctx, Math::asin);
            case "acos":
                return mathUnary(fn, ctx, Math::acos);
            case "atan":
                return mathUnary(fn, ctx, Math::atan);
            case "atan2": {
                Object y = executor.evalExpr(fn.args().get(0), ctx);
                Object x = executor.evalExpr(fn.args().get(1), ctx);
                if (y == null || x == null) return null;
                return numericResult(Math.atan2(executor.toDouble(y), executor.toDouble(x)));
            }
            case "sind":
                return mathUnary(fn, ctx, x -> Math.sin(Math.toRadians(x)));
            case "cosd":
                return mathUnary(fn, ctx, x -> Math.cos(Math.toRadians(x)));
            case "tand":
                return mathUnaryRaw(fn, ctx, x -> Math.tan(Math.toRadians(x)));
            case "asind":
                return mathUnaryRaw(fn, ctx, x -> Math.toDegrees(Math.asin(x)));
            case "acosd":
                return mathUnaryRaw(fn, ctx, x -> Math.toDegrees(Math.acos(x)));
            case "atand":
                return mathUnaryRaw(fn, ctx, x -> Math.toDegrees(Math.atan(x)));
            case "atan2d": {
                Object y = executor.evalExpr(fn.args().get(0), ctx);
                Object x = executor.evalExpr(fn.args().get(1), ctx);
                if (y == null || x == null) return null;
                return Math.toDegrees(Math.atan2(executor.toDouble(y), executor.toDouble(x)));
            }
            case "sinh":
                return mathUnaryRaw(fn, ctx, Math::sinh);
            case "cosh":
                return mathUnaryRaw(fn, ctx, Math::cosh);
            case "tanh":
                return mathUnaryRaw(fn, ctx, Math::tanh);
            case "asinh": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double x = executor.toDouble(arg);
                return Math.log(x + Math.sqrt(x * x + 1));
            }
            case "acosh": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double x = executor.toDouble(arg);
                return Math.log(x + Math.sqrt(x * x - 1));
            }
            case "atanh": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                double x = executor.toDouble(arg);
                return 0.5 * Math.log((1 + x) / (1 - x));
            }
            case "factorial": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                long n = executor.toLong(arg);
                if (n < 0) throw new MemgresException("factorial of a negative number is undefined", "2201F");
                // Use BigDecimal for exact large results (PG returns numeric for factorial)
                java.math.BigDecimal result = java.math.BigDecimal.ONE;
                for (long i = 2; i <= n; i++) result = result.multiply(java.math.BigDecimal.valueOf(i));
                return result;
            }
            case "div": {
                // Integer division
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                return executor.toLong(a) / executor.toLong(b);
            }
            case "gcd": {
                long a = executor.toLong(executor.evalExpr(fn.args().get(0), ctx));
                long b = executor.toLong(executor.evalExpr(fn.args().get(1), ctx));
                while (b != 0) { long t = b; b = a % b; a = t; }
                return Math.abs(a);
            }
            case "lcm": {
                long a = executor.toLong(executor.evalExpr(fn.args().get(0), ctx));
                long b = executor.toLong(executor.evalExpr(fn.args().get(1), ctx));
                if (a == 0 || b == 0) return 0L;
                long gcd = a;
                long temp = b;
                while (temp != 0) { long t = temp; temp = gcd % temp; gcd = t; }
                long div = a / gcd;
                // Check for overflow: use Math.multiplyHigh or manual check
                long result;
                try {
                    result = Math.multiplyExact(Math.abs(div), Math.abs(b));
                } catch (ArithmeticException e) {
                    throw new MemgresException("bigint out of range", "22003");
                }
                return result;
            }
            case "scale": {
                // PG: scale(numeric) -> integer: number of decimal digits in the fractional part.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                BigDecimal bd = arg instanceof BigDecimal ? (BigDecimal) arg : new BigDecimal(arg.toString());
                return Math.max(0, bd.scale());
            }
            case "min_scale": {
                // PG: min_scale(numeric) -> smallest scale that preserves the value.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                BigDecimal bd = arg instanceof BigDecimal ? (BigDecimal) arg : new BigDecimal(arg.toString());
                BigDecimal stripped = bd.stripTrailingZeros();
                return Math.max(0, stripped.scale());
            }
            case "trim_scale": {
                // PG: trim_scale(numeric) -> numeric with trailing zeros stripped from fractional part.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                BigDecimal bd = arg instanceof BigDecimal ? (BigDecimal) arg : new BigDecimal(arg.toString());
                BigDecimal stripped = bd.stripTrailingZeros();
                // Match PG: zero scale means no fractional part, not scientific notation like 1E+2
                if (stripped.scale() < 0) stripped = stripped.setScale(0);
                return stripped;
            }
            case "width_bucket": {
                if (fn.args().size() == 2) {
                    // Array variant: width_bucket(operand, thresholds_array)
                    double val = executor.toDouble(executor.evalExpr(fn.args().get(0), ctx));
                    Object arrayArg = executor.evalExpr(fn.args().get(1), ctx);
                    java.util.List<?> thresholds;
                    if (arrayArg instanceof java.util.List<?>) {
                        thresholds = (java.util.List<?>) arrayArg;
                    } else if (arrayArg instanceof Object[]) {
                        thresholds = java.util.Arrays.asList((Object[]) arrayArg);
                    } else {
                        throw new com.memgres.engine.MemgresException(
                            "width_bucket second argument must be an array", "42804");
                    }
                    // Return 0 if below all thresholds, N+1 if above all, else 1-based bucket index
                    for (int i = 0; i < thresholds.size(); i++) {
                        double threshold = executor.toDouble(thresholds.get(i));
                        if (val < threshold) return i;
                    }
                    return thresholds.size();
                }
                double val = executor.toDouble(executor.evalExpr(fn.args().get(0), ctx));
                double lo = executor.toDouble(executor.evalExpr(fn.args().get(1), ctx));
                double hi = executor.toDouble(executor.evalExpr(fn.args().get(2), ctx));
                int count = executor.toInt(executor.evalExpr(fn.args().get(3), ctx));
                if (val < lo) return 0;
                if (val >= hi) return count + 1;
                return (int) ((val - lo) / (hi - lo) * count) + 1;
            }
            default:
                return NOT_HANDLED;
        }
    }

    static Object numericResult(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) return d;
        if (d == Math.floor(d) && Math.abs(d) < Long.MAX_VALUE) return (long) d;
        return d;
    }
}
