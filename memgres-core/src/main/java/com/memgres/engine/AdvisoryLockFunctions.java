package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

/**
 * Advisory lock function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class AdvisoryLockFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    AdvisoryLockFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    private long advisoryKey(FunctionCallExpr fn, RowContext ctx) {
        long key1 = executor.toLong(executor.evalExpr(fn.args().get(0), ctx));
        if (fn.args().size() >= 2) {
            long key2 = executor.toLong(executor.evalExpr(fn.args().get(1), ctx));
            return (key1 << 32) | (key2 & 0xFFFFFFFFL);
        }
        return key1;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "pg_advisory_lock": {
                long key = advisoryKey(fn, ctx);
                executor.database.tryAdvisoryLock(key, executor.session);
                return null;
            }
            case "pg_advisory_unlock": {
                long key = advisoryKey(fn, ctx);
                return executor.database.advisoryUnlock(key, executor.session);
            }
            case "pg_try_advisory_lock": {
                long key = advisoryKey(fn, ctx);
                return executor.database.tryAdvisoryLock(key, executor.session);
            }
            case "pg_advisory_xact_lock": {
                long key = advisoryKey(fn, ctx);
                executor.database.tryAdvisoryLock(key, executor.session);
                if (executor.session != null) executor.session.addXactAdvisoryLock(key);
                return null;
            }
            case "pg_try_advisory_xact_lock": {
                long key = advisoryKey(fn, ctx);
                boolean acquired = executor.database.tryAdvisoryLock(key, executor.session);
                if (acquired && executor.session != null) executor.session.addXactAdvisoryLock(key);
                return acquired;
            }
            case "pg_advisory_unlock_all": {
                executor.database.advisoryUnlockAll(executor.session);
                return null;
            }
            case "pg_advisory_lock_shared": {
                long key = advisoryKey(fn, ctx);
                executor.database.tryAdvisoryLock(key, executor.session);
                return null;
            }
            case "pg_advisory_xact_lock_shared": {
                long key = advisoryKey(fn, ctx);
                executor.database.tryAdvisoryLock(key, executor.session);
                return null;
            }
            case "pg_try_advisory_lock_shared": {
                long key = advisoryKey(fn, ctx);
                return executor.database.tryAdvisoryLock(key, executor.session);
            }
            case "pg_advisory_unlock_shared": {
                long key = advisoryKey(fn, ctx);
                return executor.database.advisoryUnlock(key, executor.session);
            }
            default:
                return NOT_HANDLED;
        }
    }
}
