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

    private final AstExecutor executor;

    AdvisoryLockFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    private Database.AdvisoryLockId advisoryKey(FunctionCallExpr fn, RowContext ctx) {
        long key1 = executor.toLong(executor.evalExpr(fn.args().get(0), ctx));
        if (fn.args().size() >= 2) {
            long key2 = executor.toLong(executor.evalExpr(fn.args().get(1), ctx));
            // Two-int form: classid = first arg, objid = second arg, in its own keyspace.
            return new Database.AdvisoryLockId(((key1 & 0xFFFFFFFFL) << 32) | (key2 & 0xFFFFFFFFL), true);
        }
        return new Database.AdvisoryLockId(key1, false);
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "pg_advisory_lock": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, false, false);
                return null;
            }
            case "pg_advisory_lock_shared": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, true, false);
                return null;
            }
            case "pg_advisory_xact_lock": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, false, true);
                return null;
            }
            case "pg_advisory_xact_lock_shared": {
                executor.database.advisoryLock(advisoryKey(fn, ctx), executor.session, true, true);
                return null;
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
                return null;
            }
            default:
                return NOT_HANDLED;
        }
    }
}
