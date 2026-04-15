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
            case "macaddr8": {
                // Convert macaddr (6-byte) to macaddr8 (8-byte) by inserting ff:fe in the middle
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String mac = arg.toString().toLowerCase().trim();
                String[] parts = mac.split(":");
                if (parts.length == 6) {
                    // EUI-48 to EUI-64: insert ff:fe between bytes 3 and 4
                    return parts[0] + ":" + parts[1] + ":" + parts[2] + ":ff:fe:" + parts[3] + ":" + parts[4] + ":" + parts[5];
                }
                return mac; // already macaddr8 format or pass through
            }
            default:
                return NOT_HANDLED;
        }
    }
}
