package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Turns an analysed statement into the plan tree EXPLAIN prints.
 *
 * <p>The shape follows what the engine actually does with the statement — a scan under a sort under
 * an aggregate, a CTE beside the query that reads it — using the node names PostgreSQL uses for the
 * same shapes. Where PostgreSQL's planner has a choice memgres does not make (a hash join against a
 * nested loop, an index scan against a sequential one) the node named is the one memgres runs.
 */
final class ExplainPlanBuilder {

    private final AstExecutor executor;
    private final boolean verbose;
    /** The CTE names in scope, so a FROM item that names one is read as a CTE and not a table. */
    private final Set<String> cteNames = new HashSet<String>();

    ExplainPlanBuilder(AstExecutor executor, boolean verbose) {
        this.executor = executor;
        this.verbose = verbose;
    }

    ExplainPlan build(Statement stmt) {
        if (stmt instanceof SelectStmt) return buildSelect((SelectStmt) stmt);
        if (stmt instanceof SetOpStmt) return buildSetOp((SetOpStmt) stmt);
        if (stmt instanceof InsertStmt) return buildInsert((InsertStmt) stmt);
        if (stmt instanceof UpdateStmt) return buildUpdate((UpdateStmt) stmt);
        if (stmt instanceof DeleteStmt) return buildDelete((DeleteStmt) stmt);
        if (stmt instanceof MergeStmt) return buildMerge((MergeStmt) stmt);
        if (stmt instanceof DeclareCursorStmt) return build(((DeclareCursorStmt) stmt).query());
        if (stmt instanceof CreateTableAsStmt) return build(((CreateTableAsStmt) stmt).query());
        return new ExplainPlan("Result");
    }

    // ---- SELECT ----

    private ExplainPlan buildSelect(SelectStmt sel) {
        if (sel.withClauses() != null) {
            for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                cteNames.add(cte.name().toLowerCase());
            }
        }
        ExplainPlan node = scanLayer(sel);
        node = groupingLayer(sel, node);
        node = windowLayer(sel, node);
        node = distinctLayer(sel, node);
        node = sortLayer(sel.orderBy(), node);
        node = limitLayer(sel.limit(), sel.offset(), node);
        node = cteLayer(sel.withClauses(), node);
        return node;
    }

    /** The node that produces the rows: a scan, a join, or a Result when there is no FROM. */
    private ExplainPlan scanLayer(SelectStmt sel) {
        if (sel.from() == null || sel.from().isEmpty()) {
            ExplainPlan result = new ExplainPlan("Result");
            if (isConstantFalse(sel.where())) result.detail("One-Time Filter", "false");
            addOutput(result, sel);
            return result;
        }
        ExplainPlan node = fromItem(sel.from().get(0));
        for (int i = 1; i < sel.from().size(); i++) {
            // A comma-separated FROM list is a join with no condition.
            ExplainPlan loop = new ExplainPlan("Nested Loop");
            loop.child(node, "Outer");
            ExplainPlan right = new ExplainPlan("Materialize");
            right.child(fromItem(sel.from().get(i)));
            loop.child(right, "Inner");
            node = loop;
        }
        addOutput(node, sel);
        if (sel.where() != null) {
            if (isConstantFalse(sel.where())) {
                ExplainPlan result = new ExplainPlan("Result");
                result.detail("One-Time Filter", "false");
                return result;
            }
            node.detail("Filter", parenthesised(sel.where()));
        }
        return node;
    }

    private ExplainPlan fromItem(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            if (ref.schema() == null && cteNames.contains(ref.table().toLowerCase())) {
                return new ExplainPlan("CTE Scan").on(ref.table(), ref.alias());
            }
            return new ExplainPlan("Seq Scan").on(ref.table(), ref.alias());
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom fn = (SelectStmt.FunctionFrom) item;
            return new ExplainPlan("Function Scan").on(fn.functionName, fn.alias);
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            if (isValuesList(sub.subquery)) {
                return new ExplainPlan("Values Scan").on("\"*VALUES*\"", null);
            }
            // A sub-select that needs no materialising is flattened into its parent, exactly as
            // PostgreSQL pulls a simple subquery up rather than printing a Subquery Scan for it.
            return build(sub.subquery);
        }
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            ExplainPlan node = new ExplainPlan(joinNodeName(join.joinType));
            if (join.on != null) node.detail("Hash Cond", parenthesised(join.on));
            node.child(fromItem(join.left), "Outer");
            ExplainPlan hash = new ExplainPlan("Hash");
            hash.child(fromItem(join.right));
            node.child(hash, "Inner");
            return node;
        }
        return new ExplainPlan("Result");
    }

    private static String joinNodeName(SelectStmt.JoinType type) {
        if (type == null) return "Hash Join";
        switch (type) {
            case LEFT: return "Hash Left Join";
            case RIGHT: return "Hash Right Join";
            case FULL: return "Hash Full Join";
            case CROSS: return "Nested Loop";
            default: return "Hash Join";
        }
    }

    private ExplainPlan groupingLayer(SelectStmt sel, ExplainPlan child) {
        boolean grouped = sel.groupBy() != null && !sel.groupBy().isEmpty();
        boolean aggregated = grouped || sel.having() != null || hasAggregate(sel);
        if (!aggregated) return child;
        ExplainPlan node = new ExplainPlan(grouped ? "HashAggregate" : "Aggregate");
        if (grouped) node.detail("Group Key", commaList(sel.groupBy()));
        if (sel.having() != null) node.detail("Filter", parenthesised(sel.having()));
        node.child(child);
        return node;
    }

    private ExplainPlan windowLayer(SelectStmt sel, ExplainPlan child) {
        if (!hasWindowFunction(sel)) return child;
        ExplainPlan node = new ExplainPlan("WindowAgg");
        node.child(child);
        return node;
    }

    private ExplainPlan distinctLayer(SelectStmt sel, ExplainPlan child) {
        if (!sel.distinct) return child;
        ExplainPlan node = new ExplainPlan("HashAggregate");
        node.detail("Group Key", commaList(targetExpressions(sel)));
        node.child(child);
        return node;
    }

    private ExplainPlan sortLayer(List<SelectStmt.OrderByItem> orderBy, ExplainPlan child) {
        if (orderBy == null || orderBy.isEmpty()) return child;
        // A sort over a single constant row is dropped, the way the planner drops it.
        if ("Result".equals(child.nodeType()) && child.children().isEmpty()) return child;
        ExplainPlan node = new ExplainPlan("Sort");
        StringBuilder keys = new StringBuilder();
        for (int i = 0; i < orderBy.size(); i++) {
            if (i > 0) keys.append(", ");
            keys.append(SqlUnparser.exprToSql(orderBy.get(i).expr()));
            if (orderBy.get(i).descending()) keys.append(" DESC");
        }
        node.detail("Sort Key", keys.toString());
        node.child(child);
        return node;
    }

    private ExplainPlan limitLayer(Expression limit, Expression offset, ExplainPlan child) {
        if (limit == null && offset == null) return child;
        ExplainPlan node = new ExplainPlan("Limit");
        node.child(child);
        return node;
    }

    private ExplainPlan cteLayer(List<SelectStmt.CommonTableExpr> ctes, ExplainPlan child) {
        if (ctes == null || ctes.isEmpty()) return child;
        for (SelectStmt.CommonTableExpr cte : ctes) {
            child.subPlan("CTE " + cte.name(), build(cte.query()));
        }
        return child;
    }

    // ---- set operations ----

    private ExplainPlan buildSetOp(SetOpStmt set) {
        // A multi-row VALUES list is written as a chain of unions, but it is one scan over a
        // constant table, and that is the node PostgreSQL names for it.
        if (isValuesList(set)) {
            ExplainPlan values = new ExplainPlan("Values Scan").on("\"*VALUES*\"", null);
            values = sortLayer(set.orderBy(), values);
            return limitLayer(set.limit(), set.offset(), values);
        }
        ExplainPlan left = build(set.left());
        ExplainPlan right = build(set.right());
        ExplainPlan node;
        if (set.op() == SetOpStmt.SetOpType.UNION) {
            ExplainPlan append = new ExplainPlan("Append");
            append.child(left, "Member");
            append.child(right, "Member");
            if (set.all()) {
                node = append;
            } else {
                // UNION without ALL sorts and then drops the neighbours that repeat.
                ExplainPlan sort = new ExplainPlan("Sort");
                sort.detail("Sort Key", setOpSortKey(set));
                sort.child(append);
                node = new ExplainPlan("Unique");
                node.child(sort);
            }
        } else {
            String name = set.op() == SetOpStmt.SetOpType.EXCEPT ? "SetOp Except" : "SetOp Intersect";
            node = new ExplainPlan(set.all() ? name + " All" : name);
            node.child(left);
            node.child(right);
        }
        node = sortLayer(set.orderBy(), node);
        node = limitLayer(set.limit(), set.offset(), node);
        return node;
    }

    /** The key a bare UNION sorts on: the branch's output columns, parenthesised as PG prints them. */
    private String setOpSortKey(SetOpStmt set) {
        Statement leftmost = set.left();
        while (leftmost instanceof SetOpStmt) leftmost = ((SetOpStmt) leftmost).left();
        if (!(leftmost instanceof SelectStmt)) return "(1)";
        SelectStmt sel = (SelectStmt) leftmost;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sel.targets.size(); i++) {
            if (i > 0) sb.append(", ");
            Expression expr = sel.targets.get(i).expr();
            String text = SqlUnparser.exprToSql(expr);
            sb.append(expr instanceof ColumnRef ? text : "(" + text + ")");
        }
        return sb.length() == 0 ? "(1)" : sb.toString();
    }

    // ---- statements that change rows ----

    private ExplainPlan buildInsert(InsertStmt ins) {
        ExplainPlan node = new ExplainPlan("Insert").on(ins.table(), null);
        if (ins.selectStmt != null) {
            node.child(build(ins.selectStmt));
        } else if (ins.values != null && ins.values.size() > 1) {
            node.child(new ExplainPlan("Values Scan").on("\"*VALUES*\"", null));
        } else {
            node.child(new ExplainPlan("Result"));
        }
        return node;
    }

    private ExplainPlan buildUpdate(UpdateStmt upd) {
        ExplainPlan node = new ExplainPlan("Update").on(upd.table(), null);
        ExplainPlan scan = new ExplainPlan("Seq Scan").on(upd.table(), null);
        if (upd.where() != null) scan.detail("Filter", parenthesised(upd.where()));
        node.child(scan);
        return node;
    }

    private ExplainPlan buildDelete(DeleteStmt del) {
        ExplainPlan node = new ExplainPlan("Delete").on(del.table(), null);
        ExplainPlan scan = new ExplainPlan("Seq Scan").on(del.table(), null);
        if (del.where() != null) scan.detail("Filter", parenthesised(del.where()));
        node.child(scan);
        return node;
    }

    private ExplainPlan buildMerge(MergeStmt merge) {
        ExplainPlan node = new ExplainPlan("Merge").on(merge.targetTable(), merge.targetAlias());
        node.child(new ExplainPlan("Seq Scan").on(merge.targetTable(), merge.targetAlias()));
        return node;
    }

    // ---- helpers ----

    private void addOutput(ExplainPlan node, SelectStmt sel) {
        if (!verbose) return;
        List<Expression> exprs = targetExpressions(sel);
        if (exprs.isEmpty()) return;
        node.detail("Output", outputList(exprs));
    }

    private static List<Expression> targetExpressions(SelectStmt sel) {
        List<Expression> out = new ArrayList<Expression>();
        if (sel.targets == null) return out;
        for (SelectStmt.SelectTarget t : sel.targets) {
            if (t.expr() != null) out.add(t.expr());
        }
        return out;
    }

    private static String commaList(List<Expression> exprs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < exprs.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(SqlUnparser.exprToSql(exprs.get(i)));
        }
        return sb.toString();
    }

    /**
     * The output list as EXPLAIN VERBOSE prints it. Arithmetic between constants is worked out
     * before the plan is built, so {@code SELECT 1+1} reports {@code Output: 2} and not the sum
     * that was written.
     */
    private static String outputList(List<Expression> exprs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < exprs.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(SqlUnparser.exprToSql(foldConstants(exprs.get(i))));
        }
        return sb.toString();
    }

    private static Expression foldConstants(Expression expr) {
        if (!(expr instanceof BinaryExpr)) return expr;
        BinaryExpr bin = (BinaryExpr) expr;
        Expression left = foldConstants(bin.left());
        Expression right = foldConstants(bin.right());
        if (!(left instanceof Literal) || !(right instanceof Literal)) return expr;
        Literal a = (Literal) left;
        Literal b = (Literal) right;
        if (a.literalType() != Literal.LiteralType.INTEGER
                || b.literalType() != Literal.LiteralType.INTEGER) {
            return expr;
        }
        long x;
        long y;
        try {
            x = Long.parseLong(a.value());
            y = Long.parseLong(b.value());
        } catch (NumberFormatException e) {
            return expr;
        }
        Long folded = null;
        switch (bin.op()) {
            case ADD: folded = Long.valueOf(x + y); break;
            case SUBTRACT: folded = Long.valueOf(x - y); break;
            case MULTIPLY: folded = Long.valueOf(x * y); break;
            default: break;
        }
        return folded == null ? expr : Literal.ofInt(folded.toString());
    }

    /** PostgreSQL prints a qual wrapped in parentheses. */
    private static String parenthesised(Expression expr) {
        String text = SqlUnparser.exprToSql(expr);
        if (text.startsWith("(") && text.endsWith(")")) return text;
        return "(" + text + ")";
    }

    private static boolean isConstantFalse(Expression expr) {
        if (!(expr instanceof Literal)) return false;
        Literal lit = (Literal) expr;
        return lit.literalType() == Literal.LiteralType.BOOLEAN && "false".equalsIgnoreCase(lit.value());
    }

    private static boolean isValuesList(Statement stmt) {
        if (stmt instanceof SelectStmt) return ((SelectStmt) stmt).fromValues();
        if (stmt instanceof SetOpStmt) {
            SetOpStmt set = (SetOpStmt) stmt;
            return set.op() == SetOpStmt.SetOpType.UNION && set.all()
                    && isValuesList(set.left()) && isValuesList(set.right());
        }
        return false;
    }

    private static boolean hasAggregate(SelectStmt sel) {
        for (Expression e : targetExpressions(sel)) {
            if (containsAggregate(e)) return true;
        }
        return false;
    }

    private static final Set<String> AGGREGATES = new HashSet<String>();
    static {
        String[] names = {"count", "sum", "avg", "min", "max", "array_agg", "string_agg",
                "bool_and", "bool_or", "every", "json_agg", "jsonb_agg", "xmlagg",
                "stddev", "variance", "stddev_pop", "stddev_samp", "var_pop", "var_samp"};
        for (String n : names) AGGREGATES.add(n);
    }

    private static boolean containsAggregate(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            if (AGGREGATES.contains(fn.name().toLowerCase())) return true;
            if (fn.args() != null) {
                for (Expression a : fn.args()) if (containsAggregate(a)) return true;
            }
            return false;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) expr;
            return containsAggregate(b.left()) || containsAggregate(b.right());
        }
        if (expr instanceof UnaryExpr) return containsAggregate(((UnaryExpr) expr).operand());
        return false;
    }

    private static boolean hasWindowFunction(SelectStmt sel) {
        for (Expression e : targetExpressions(sel)) {
            if (e instanceof WindowFuncExpr) return true;
        }
        return false;
    }
}
