package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts AST Statement nodes back to SQL text.
 * Used by pg_get_viewdef(), information_schema.views, and similar introspection functions.
 */
public class SqlUnparser {

    /**
     * Convert a Statement AST to SQL text.
     */
    public static String toSql(Statement stmt) {
        if (stmt == null) return null;
        if (stmt instanceof SelectStmt) return selectToSql(((SelectStmt) stmt));
        if (stmt instanceof SetOpStmt) return setOpToSql(((SetOpStmt) stmt));
        return stmt.toString(); // fallback
    }

    private static String selectToSql(SelectStmt sel) {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (sel.distinct()) sb.append("DISTINCT ");

        // Targets
        if (sel.targets() == null || sel.targets().isEmpty()) {
            // empty target list
        } else {
            sb.append(sel.targets().stream()
                    .map(SqlUnparser::targetToSql)
                    .collect(Collectors.joining(", ")));
        }

        // FROM
        if (sel.from() != null && !sel.from().isEmpty()) {
            sb.append(" FROM ");
            sb.append(sel.from().stream()
                    .map(SqlUnparser::fromItemToSql)
                    .collect(Collectors.joining(", ")));
        }

        // WHERE
        if (sel.where() != null) {
            sb.append(" WHERE (").append(exprToSql(sel.where())).append(")");
        }

        // GROUP BY
        if (sel.groupBy() != null && !sel.groupBy().isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(sel.groupBy().stream()
                    .map(SqlUnparser::exprToSql)
                    .collect(Collectors.joining(", ")));
        }

        // HAVING
        if (sel.having() != null) {
            sb.append(" HAVING ").append(exprToSql(sel.having()));
        }

        // ORDER BY
        if (sel.orderBy() != null && !sel.orderBy().isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(sel.orderBy().stream()
                    .map(ob -> exprToSql(ob.expr()) + (ob.descending() ? " DESC" : "")
                            + (ob.nullsFirst() != null ? (ob.nullsFirst() ? " NULLS FIRST" : " NULLS LAST") : ""))
                    .collect(Collectors.joining(", ")));
        }

        // LIMIT
        if (sel.limit() != null) {
            sb.append(" LIMIT ").append(exprToSql(sel.limit()));
        }

        // OFFSET
        if (sel.offset() != null) {
            sb.append(" OFFSET ").append(exprToSql(sel.offset()));
        }

        return sb.toString();
    }

    private static String setOpToSql(SetOpStmt setOp) {
        String left = toSql(setOp.left());
        String right = toSql(setOp.right());
        String op = setOp.op().name();
        return left + " " + op + (setOp.all() ? " ALL " : " ") + right;
    }

    private static String targetToSql(SelectStmt.SelectTarget target) {
        String expr = exprToSql(target.expr());
        if (target.alias() != null) {
            return expr + " AS " + target.alias();
        }
        return expr;
    }

    private static String fromItemToSql(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) item;
            return tr.table() + (tr.alias() != null ? " " + tr.alias() : "");
        }
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            return fromItemToSql(join.left()) + " " + join.joinType().name().replace("_", " ") +
                    " JOIN " + fromItemToSql(join.right()) +
                    (join.on() != null ? " ON " + exprToSql(join.on()) : "") +
                    (join.using() != null ? " USING (" + String.join(", ", join.using()) + ")" : "");
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sq = (SelectStmt.SubqueryFrom) item;
            return "(" + toSql(sq.subquery()) + ")" + (sq.alias() != null ? " " + sq.alias() : "");
        }
        return item.toString();
    }

    /**
     * Convert an Expression AST to SQL text.
     */
    public static String exprToSql(Expression expr) {
        if (expr == null) return "NULL";
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case STRING:
                    return "'" + lit.value().replace("'", "''") + "'";
                case BIT_STRING:
                    return "B'" + lit.value() + "'";
                case NULL:
                    return "NULL";
                case BOOLEAN:
                    return lit.value();
                case DEFAULT:
                    return "DEFAULT";
                default:
                    return lit.value();
            }
        } else if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            return (ref.table() != null ? ref.table() + "." : "") + ref.column();
        } else if (expr instanceof WildcardExpr) {
            WildcardExpr w = (WildcardExpr) expr;
            return w.table() != null ? w.table() + ".*" : "*";
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            return "(" + exprToSql(bin.left()) + " " + binOpToSql(bin.op()) + " " + exprToSql(bin.right()) + ")";
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) {
                return "(" + exprToSql(cop.left()) + " " + cop.opSymbol() + " " + exprToSql(cop.right()) + ")";
            } else {
                return "(" + cop.opSymbol() + " " + exprToSql(cop.right()) + ")";
            }
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            switch (un.op()) {
                case NEGATE:
                    return "(-" + exprToSql(un.operand()) + ")";
                case NOT:
                    return "(NOT " + exprToSql(un.operand()) + ")";
                case BIT_NOT:
                    return "(~" + exprToSql(un.operand()) + ")";
                default:
                    return exprToSql(un.operand());
            }
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            return fn.name() + "(" +
                    fn.args().stream().map(SqlUnparser::exprToSql).collect(Collectors.joining(", ")) + ")";
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return exprToSql(cast.expr()) + "::" + cast.typeName();
        } else if (expr instanceof IsNullExpr) {
            IsNullExpr isn = (IsNullExpr) expr;
            return exprToSql(isn.expr()) + (isn.negated() ? " IS NOT NULL" : " IS NULL");
        } else if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            return caseToSql(c);
        } else if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            return "(" + toSql(sq.subquery()) + ")";
        } else if (expr instanceof ExistsExpr) {
            ExistsExpr ex = (ExistsExpr) expr;
            return "EXISTS (" + toSql(ex.subquery()) + ")";
        } else if (expr instanceof BetweenExpr) {
            BetweenExpr bet = (BetweenExpr) expr;
            return exprToSql(bet.expr()) + (bet.negated() ? " NOT" : "") +
                    " BETWEEN " + exprToSql(bet.low()) + " AND " + exprToSql(bet.high());
        } else if (expr instanceof LikeExpr) {
            LikeExpr like = (LikeExpr) expr;
            return exprToSql(like.left()) + (like.negated() ? " NOT" : "") +
                    (like.caseInsensitive() ? " ILIKE " : " LIKE ") + exprToSql(like.pattern());
        } else if (expr instanceof ArrayExpr) {
            ArrayExpr arr = (ArrayExpr) expr;
            return arr.isRow() ? "ROW(" +
                    arr.elements().stream().map(SqlUnparser::exprToSql).collect(Collectors.joining(", ")) + ")"
                    : "ARRAY[" + arr.elements().stream().map(SqlUnparser::exprToSql).collect(Collectors.joining(", ")) + "]";
        } else {
            return expr.toString();
        }
    }

    private static String binOpToSql(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD:
                return "+";
            case SUBTRACT:
                return "-";
            case MULTIPLY:
                return "*";
            case DIVIDE:
                return "/";
            case MODULO:
                return "%";
            case EQUAL:
                return "=";
            case NOT_EQUAL:
                return "<>";
            case LESS_THAN:
                return "<";
            case GREATER_THAN:
                return ">";
            case LESS_EQUAL:
                return "<=";
            case GREATER_EQUAL:
                return ">=";
            case AND:
                return "AND";
            case OR:
                return "OR";
            case CONCAT:
                return "||";
            case LIKE:
                return "~~";
            case ILIKE:
                return "~~*";
            case JSON_ARROW:
                return "->";
            case JSON_ARROW_TEXT:
                return "->>";
            case POWER:
                return "^";
            case CONTAINS:
                return "@>";
            case CONTAINED_BY:
                return "<@";
            case OVERLAP:
                return "&&";
            default:
                return op.name();
        }
    }

    private static String caseToSql(CaseExpr c) {
        StringBuilder sb = new StringBuilder("CASE");
        if (c.operand() != null) sb.append(" ").append(exprToSql(c.operand()));
        for (CaseExpr.WhenClause w : c.whenClauses()) {
            sb.append(" WHEN ").append(exprToSql(w.condition())).append(" THEN ").append(exprToSql(w.result()));
        }
        if (c.elseExpr() != null) sb.append(" ELSE ").append(exprToSql(c.elseExpr()));
        sb.append(" END");
        return sb.toString();
    }
}
