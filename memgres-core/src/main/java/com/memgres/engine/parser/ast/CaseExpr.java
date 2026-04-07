package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CASE WHEN cond THEN result [WHEN ...] [ELSE result] END
 */
public final class CaseExpr implements Expression {
    public final Expression operand;
    public final List<WhenClause> whenClauses;
    public final Expression elseExpr;

    public CaseExpr(Expression operand, List<WhenClause> whenClauses, Expression elseExpr) {
        this.operand = operand;
        this.whenClauses = whenClauses;
        this.elseExpr = elseExpr;
    }

    public static final class WhenClause {
        public final Expression condition;
        public final Expression result;

        public WhenClause(Expression condition, Expression result) {
            this.condition = condition;
            this.result = result;
        }

        public Expression condition() { return condition; }
        public Expression result() { return result; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WhenClause that = (WhenClause) o;
            return java.util.Objects.equals(condition, that.condition)
                && java.util.Objects.equals(result, that.result);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(condition, result);
        }

        @Override
        public String toString() {
            return "WhenClause[condition=" + condition + ", " + "result=" + result + "]";
        }
    }

    public Expression operand() { return operand; }
    public List<WhenClause> whenClauses() { return whenClauses; }
    public Expression elseExpr() { return elseExpr; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CaseExpr that = (CaseExpr) o;
        return java.util.Objects.equals(operand, that.operand)
            && java.util.Objects.equals(whenClauses, that.whenClauses)
            && java.util.Objects.equals(elseExpr, that.elseExpr);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(operand, whenClauses, elseExpr);
    }

    @Override
    public String toString() {
        return "CaseExpr[operand=" + operand + ", " + "whenClauses=" + whenClauses + ", " + "elseExpr=" + elseExpr + "]";
    }
}
