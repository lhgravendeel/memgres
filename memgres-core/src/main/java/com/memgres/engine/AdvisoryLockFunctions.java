package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

/**
 * Advisory lock function evaluation, extracted from FunctionEvaluator to reduce class size.
 *
 * PostgreSQL semantics implemented here:
 * <ul>
 *   <li>The non-try forms ({@code pg_advisory_lock} etc.) block until the lock is available.</li>
 *   <li>Acquisitions are reference-counted per (session, key, mode, ownership); each successful
 *       lock needs a matching unlock.</li>
 *   <li>Shared and exclusive are distinct modes: multiple shared holders may coexist, an
 *       exclusive holder excludes everyone else, and an unlock must match the mode held
 *       ({@code pg_advisory_unlock_shared} never releases an exclusive hold).</li>
 *   <li>The one-argument (bigint) and two-argument (int, int) forms live in distinct keyspaces.</li>
 *   <li>{@code pg_advisory_xact_lock} variants take transaction-level ownership: released
 *       automatically at COMMIT/ROLLBACK and not releasable via {@code pg_advisory_unlock}.</li>
 *   <li>Unlocking a lock that is not held returns false (PG also emits a WARNING, which is
 *       not part of the result set; memgres just returns false).</li>
 * </ul>
 */
class AdvisoryLockFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    /**
     * What a function declared {@code RETURNS void} hands back. PG renders it as an empty string,
     * so returning SQL NULL instead changes what the client reads and how it tests the result.
     */
    private static final Object VOID_RESULT = "";

    private final AstExecutor executor;

    AdvisoryLockFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    private Database.AdvisoryLockId advisoryKey(FunctionCallExpr fn, RowContext ctx) {
        // The two forms take different types, so a key neither can read is reported against the
        // one that was called: two keys are integers apiece, a lone key is a bigint.
        if (fn.args().size() >= 2) {
            long key1 = narrowed(fn, ctx, 0, "integer");
            long key2 = narrowed(fn, ctx, 1, "integer");
            // Two-int form: classid = first arg, objid = second arg, in its own keyspace.
            return new Database.AdvisoryLockId(((key1 & 0xFFFFFFFFL) << 32) | (key2 & 0xFFFFFFFFL), true);
        }
        return new Database.AdvisoryLockId(narrowed(fn, ctx, 0, "bigint"), false);
    }

    /**
     * One key argument, refused when it does not fit the parameter the form declares.
     *
     * <p>The two-key form takes two integers and the one-key form a bigint; a wider value simply
     * has no such function. Coercing it into range instead took a lock on a different key from
     * the one the caller named, so two callers naming different keys could block each other.
     */
    private long narrowed(FunctionCallExpr fn, RowContext ctx, int index, String declared) {
        Object value = executor.evalExpr(fn.args().get(index), ctx);
        java.math.BigInteger written;
        if (value instanceof java.math.BigDecimal) {
            java.math.BigDecimal decimal = (java.math.BigDecimal) value;
            if (decimal.stripTrailingZeros().scale() > 0) refuse(fn, "numeric", declared);
            written = decimal.toBigInteger();
        } else if (value instanceof Number) {
            written = java.math.BigInteger.valueOf(((Number) value).longValue());
        } else if ("integer".equals(declared)) {
            written = java.math.BigInteger.valueOf(executor.toInt(value));
        } else {
            written = java.math.BigInteger.valueOf(executor.toLong(value));
        }
        if ("integer".equals(declared)) {
            if (written.bitLength() > 31) {
                refuse(fn, written.bitLength() > 63 ? "numeric" : "bigint", declared);
            }
            return written.longValue();
        }
        if (written.bitLength() > 63) refuse(fn, "numeric", declared);
        return written.longValue();
    }

    /** Name the function the way the arguments written would have to be declared for it. */
    private void refuse(FunctionCallExpr fn, String actual, String declared) {
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < fn.args().size(); i++) {
            if (i > 0) types.append(", ");
            types.append(i == 0 ? actual : declared);
        }
        throw new MemgresException("function " + FunctionEvaluator.stripSchemaPrefix(fn.name())
                + "(" + types + ") does not exist", "42883");
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "pg_advisory_lock": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, false, false);
                return VOID_RESULT;
            }
            case "pg_advisory_lock_shared": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, true, false);
                return VOID_RESULT;
            }
            case "pg_advisory_xact_lock": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, false, true);
                return VOID_RESULT;
            }
            case "pg_advisory_xact_lock_shared": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, true, true);
                return VOID_RESULT;
            }
            case "pg_try_advisory_lock": {
                return executor.database.tryAdvisoryLock(advisoryKey(fn, ctx), executor.session, false, false);
            }
            case "pg_try_advisory_lock_shared": {
                return executor.database.tryAdvisoryLock(advisoryKey(fn, ctx), executor.session, true, false);
            }
            case "pg_try_advisory_xact_lock": {
                return executor.database.tryAdvisoryLock(advisoryKey(fn, ctx), executor.session, false, true);
            }
            case "pg_try_advisory_xact_lock_shared": {
                return executor.database.tryAdvisoryLock(advisoryKey(fn, ctx), executor.session, true, true);
            }
            case "pg_advisory_unlock": {
                return executor.database.advisoryUnlock(advisoryKey(fn, ctx), executor.session, false);
            }
            case "pg_advisory_unlock_shared": {
                return executor.database.advisoryUnlock(advisoryKey(fn, ctx), executor.session, true);
            }
            case "pg_advisory_unlock_all": {
                executor.database.advisoryUnlockAll(executor.session);
                return VOID_RESULT;
            }
            default:
                return NOT_HANDLED;
        }
    }
}
