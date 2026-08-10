package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * A subscripted expression: {@code a[i]}, {@code a[i][j]}, {@code a[i:j]}, {@code j['k']}.
 *
 * <p>Every {@code [...]} used to be lowered to the jsonb {@code ->} operator, so an array
 * subscript inherited jsonb's rules — a fractional index was refused as an operator that does not
 * exist, a subscript of something not subscriptable named jsonb, and a chain of subscripts was a
 * chain of separate lookups rather than one reference into one array. PostgreSQL reads all the
 * brackets that follow an expression as a single reference, which is what this node holds.
 */
public final class SubscriptExpr implements Expression {

    /** One pair of brackets: an index, or a range when the brackets held a colon. */
    public static final class Subscript {
        private final Expression lower;
        private final Expression upper;
        private final boolean slice;

        public Subscript(Expression lower, Expression upper, boolean slice) {
            this.lower = lower;
            this.upper = upper;
            this.slice = slice;
        }

        /** The index, or the lower bound of a range; null when a range states none. */
        public Expression lower() {
            return lower;
        }

        /** The upper bound of a range; null when it states none or this is not a range. */
        public Expression upper() {
            return upper;
        }

        /** True when the brackets held a colon, which makes this a range rather than an index. */
        public boolean slice() {
            return slice;
        }
    }

    private final Expression base;
    private final List<Subscript> subscripts;

    public SubscriptExpr(Expression base, List<Subscript> subscripts) {
        this.base = base;
        this.subscripts = subscripts;
    }

    public Expression base() {
        return base;
    }

    public List<Subscript> subscripts() {
        return subscripts;
    }

    /** True when any pair of brackets held a colon, which makes the whole reference a slice. */
    public boolean isSlice() {
        for (Subscript s : subscripts) {
            if (s.slice()) return true;
        }
        return false;
    }
}
