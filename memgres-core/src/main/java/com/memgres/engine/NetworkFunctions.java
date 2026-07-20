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
                if (arg == null) return null;
                InetValue inet = toInet(arg);
                return inet.host();
            }
            case "masklen": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                InetValue inet = toInet(arg);
                return inet.getPrefixLength();
            }
            case "set_masklen": {
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null) return null;
                InetValue inet = toInet(arg1);
                int len = executor.toInt(arg2);
                if (arg1 instanceof CidrValue) {
                    return ((CidrValue) arg1).setCidrMasklen(len);
                }
                return inet.setMasklen(len);
            }
            case "netmask": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                InetValue inet = toInet(arg);
                return inet.netmask();
            }
            case "hostmask": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                InetValue inet = toInet(arg);
                return inet.hostmask();
            }
            case "broadcast": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                InetValue inet = toInet(arg);
                return inet.broadcast();
            }
            case "network": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                InetValue inet = toInet(arg);
                InetValue net = inet.network();
                return CidrValue.fromInet(net);
            }
            case "inet_same_family": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                return toInet(a).sameFamily(toInet(b));
            }
            case "inet_merge": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                InetValue merged = toInet(a).merge(toInet(b));
                return CidrValue.fromInet(merged);
            }
            case "abbrev": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof CidrValue) return ((CidrValue) arg).abbrev();
                if (arg instanceof InetValue) return ((InetValue) arg).abbrev();
                // fallback for string
                return InetValue.parse(arg.toString()).abbrev();
            }
            case "family": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return toInet(arg).family();
            }
            case "text": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof InetValue) return ((InetValue) arg).text();
                return NOT_HANDLED;
            }
            case "macaddr8": {
                // Convert macaddr (6-byte) to macaddr8 (8-byte) by inserting ff:fe in the middle
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof MacaddrValue) return ((MacaddrValue) arg).toMacaddr8();
                return MacaddrValue.parse(arg.toString()).toMacaddr8();
            }
            case "macaddr8_set7bit": {
                // Set the 7th bit (universal/local bit) of a macaddr8 value
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof Macaddr8Value) return ((Macaddr8Value) arg).set7bit();
                return Macaddr8Value.parse(arg.toString()).set7bit();
            }
            case "trunc": {
                // trunc(macaddr) or trunc(macaddr8)
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof MacaddrValue) return ((MacaddrValue) arg).trunc();
                if (arg instanceof Macaddr8Value) return ((Macaddr8Value) arg).trunc();
                return NOT_HANDLED;
            }
            default:
                return NOT_HANDLED;
        }
    }

    private static InetValue toInet(Object val) {
        if (val instanceof InetValue) return (InetValue) val;
        return InetValue.parse(val.toString());
    }
}
