package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

/**
 * Network function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class NetworkFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    NetworkFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "host": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : NetworkOperations.host(arg.toString());
            }
            case "masklen": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : NetworkOperations.masklen(arg.toString());
            }
            case "set_masklen": {
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                return arg1 == null ? null : NetworkOperations.setMasklen(arg1.toString(), executor.toInt(arg2));
            }
            case "netmask": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : NetworkOperations.netmask(arg.toString());
            }
            case "broadcast": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : NetworkOperations.broadcast(arg.toString());
            }
            case "network": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : NetworkOperations.network(arg.toString());
            }
            case "inet_same_family": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                return (a == null || b == null) ? null : NetworkOperations.inetSameFamily(a.toString(), b.toString());
            }
            case "inet_merge": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                return (a == null || b == null) ? null : NetworkOperations.inetMerge(a.toString(), b.toString());
            }
            case "abbrev": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : NetworkOperations.abbrev(arg.toString());
            }
            case "family": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : (arg.toString().contains(":") ? 6 : 4);
            }
            default:
                return NOT_HANDLED;
        }
    }
}
