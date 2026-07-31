package com.memgres.engine;

import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.NamedArgExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Who may carry a FILTER clause — and, in the same words, a DISTINCT or an ORDER BY among its
 * arguments — and which of a statement's faults is reported.
 *
 * <p>FILTER says which rows a call accumulates, DISTINCT says which of them it counts once, and an
 * ORDER BY among the arguments says which order it accumulates them in. All three mean something
 * only to a call that accumulates rows. Written on an ordinary function none of them is a clause
 * that can be ignored but a call that cannot be built, and PostgreSQL refuses it naming the
 * function:
 * {@code FILTER specified, but abs is not an aggregate function} (42809). The refusal is a
 * property of the call rather than of the clause it stands in, so it holds in a select list, in
 * WHERE, in HAVING, in ORDER BY, inside a CTE or a derived table, in an aggregate's own argument
 * list, and in the SET list of an UPDATE alike.
 *
 * <p>The name in the message is the name as parsed: unquoted parts are lower case because the
 * lexer folds them, a quoted part keeps its case, and a schema qualifier is kept and joined with
 * a dot — {@code pg_catalog.abs}, not {@code abs}.
 *
 * <p>Three kinds of call are deliberately left alone, because PostgreSQL has a different answer
 * for each:
 *
 * <ul>
 *   <li>An <b>aggregate</b> — built-in or one the user declared with CREATE AGGREGATE — is what
 *       FILTER is for.</li>
 *   <li>A <b>window function</b> ({@code row_number}, {@code lag}, {@code rank} …) carrying
 *       FILTER is 0A000 "FILTER is not implemented for non-aggregate window functions" when it
 *       has an OVER clause and 42809 "window function … requires an OVER clause" when it has
 *       none. Both already exist; naming one here would replace the better message.</li>
 *   <li>A name that is <b>no function memgres knows</b> is a missing function first: PostgreSQL
 *       resolves the call before it judges the FILTER, so {@code nosuch(v) FILTER (…)} is 42883,
 *       and an unrecognised name is therefore left to fail that way. This also keeps the rule off
 *       anything a later PostgreSQL might add as an aggregate.</li>
 * </ul>
 *
 * <p>The rest of this class is about order. A statement with several faults reports one of them,
 * and which one is decided by the order PostgreSQL analyses it in rather than by the order memgres
 * happens to check things. {@link Ordered} walks a query level's clauses in that order once a
 * refusal at that level has been found, so what changes is which fault a doomed statement reports.
 */
final class FilterCheck {

    private FilterCheck() {
    }

    /**
     * Refuses the statement for the fault PostgreSQL would name first.
     *
     * <p>A call written with a number of arguments no signature of its name has resolves to no
     * function, so that is settled before anything else. Then, for a query level whose relations
     * are described, the clauses are read in the order PostgreSQL reads them and the first fault
     * of the earliest one is reported. What is left is the older, coarser walk: the reflective one,
     * which reaches every expression position in the statement — including inside sub-queries and
     * CTEs, which is where PostgreSQL raises this too — and refuses the call it finds there.
     *
     * <p>{@code scope} is null wherever the query level's relations are not resolved yet, in which
     * case a call is refused on its name alone as before.
     */
    static void reject(SelectExecutor select, Object root, QueryLevelScope scope) {
        rejectAggregateArity(select, root, scope);
        // The ordered walk only ever chooses which fault a statement already doomed at this query
        // level reports, so it runs only once one has been found. The checks it consults answer
        // from the bindings alone and are one-sided by design: run over a statement that was going
        // to succeed they would refuse it, which is the worse mistake by far.
        if (scope != null && root instanceof SelectStmt) {
            Object doomed = AstWalk.findFirst(root, node -> carriesRefusedFilter(select, node));
            if (doomed != null && QueryLevelScope.isOwnLevel(root, doomed)) {
                new Ordered(select, scope).statement((SelectStmt) root);
            }
        }
        rejectByName(select, root, scope);
    }

    /**
     * How many arguments each aggregate takes. An aggregate is resolved by name and argument list
     * like any other function, so a call with the wrong number of them is a function that does not
     * exist — {@code sum(integer, integer)} — rather than sum applied to the first of them.
     * Aggregates carry no row in {@link BuiltinFunctionSignatures}, so their arities are here.
     */
    private static final java.util.Map<String, Integer> AGGREGATE_ARITY = aggregateArity();

    private static java.util.Map<String, Integer> aggregateArity() {
        java.util.Map<String, Integer> m = new java.util.HashMap<String, Integer>();
        String[] one = {"count", "sum", "avg", "min", "max", "any_value", "array_agg",
                "bit_and", "bit_or", "bit_xor", "bool_and", "bool_or", "every",
                "json_agg", "jsonb_agg", "xmlagg", "stddev", "stddev_pop", "stddev_samp",
                "variance", "var_pop", "var_samp", "range_agg", "range_intersect_agg"};
        for (String n : one) m.put(n, Integer.valueOf(1));
        String[] two = {"string_agg", "json_object_agg", "jsonb_object_agg", "corr",
                "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count",
                "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy"};
        for (String n : two) m.put(n, Integer.valueOf(2));
        return java.util.Collections.unmodifiableMap(m);
    }

    /** Refuses the first aggregate call written with a number of arguments it does not take. */
    private static void rejectAggregateArity(SelectExecutor select, Object root,
                                             QueryLevelScope scope) {
        Object found = AstWalk.findFirst(root, node -> wrongAggregateArity(select, node));
        if (found == null) return;
        FunctionCallExpr call = (FunctionCallExpr) found;
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < call.args().size(); i++) {
            if (i > 0) types.append(", ");
            DataType type = scope == null ? null : scope.certainTypeOf(call.args().get(i));
            types.append(type == null ? "unknown" : CatalogHelper.pgTypeName(type));
        }
        throw new MemgresException(
                "function " + call.name() + "(" + types + ") does not exist\n"
                        + "  Hint: No function matches the given name and argument types.",
                "42883");
    }

    private static boolean wrongAggregateArity(SelectExecutor select, Object node) {
        if (!(node instanceof FunctionCallExpr)) return false;
        FunctionCallExpr call = (FunctionCallExpr) node;
        // count(*) counts rows rather than values and is written with no argument list at all.
        if (call.star()) return false;
        String bare = QueryLevelScope.bareName(call.name());
        // A user's aggregate of that name decides its own arity.
        if (select.hasUserFunction(bare)) return false;
        Integer takes = AGGREGATE_ARITY.get(bare);
        if (takes == null) return false;
        for (Expression arg : call.args()) {
            // A named or variadic argument binds by name, so counting written ones settles nothing.
            if (arg instanceof NamedArgExpr) return false;
        }
        return call.args().size() != takes.intValue();
    }

    /**
     * The order PostgreSQL analyses one query level in, which is the order its faults are reported.
     *
     * <p>WITH items first, then the FROM clause with its join conditions, then the select list,
     * WHERE, HAVING, ORDER BY and GROUP BY — measured against the reference server, which is why
     * HAVING stands before ORDER BY and GROUP BY comes last of all. Within a clause the expressions
     * are read left to right, and within one call its arguments are read before its FILTER
     * predicate, which is read before the function itself is resolved.
     *
     * <p>Every refusal raised here is one of {@link QueryLevelScope}'s, which only speak when the
     * bindings settle the answer. This changes which fault a doomed statement reports; it does not
     * decide that a statement is doomed.
     */
    private static final class Ordered {

        private final SelectExecutor select;
        private final QueryLevelScope scope;

        Ordered(SelectExecutor select, QueryLevelScope scope) {
            this.select = select;
            this.scope = scope;
        }

        void statement(SelectStmt stmt) {
            if (stmt.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : stmt.withClauses()) {
                    rejectByName(select, cte.query(), null);
                }
            }
            if (stmt.from() != null) {
                for (SelectStmt.FromItem item : stmt.from()) fromItem(item);
            }
            if (stmt.targets() != null) {
                for (SelectStmt.SelectTarget target : stmt.targets()) node(target.expr());
            }
            node(stmt.where());
            node(stmt.having());
            if (stmt.orderBy() != null) {
                for (SelectStmt.OrderByItem item : stmt.orderBy()) node(item.expr());
            }
            if (stmt.groupBy() != null) {
                for (Expression group : stmt.groupBy()) node(group);
            }
        }

        /** A join condition belongs to the FROM clause and is read while it is being built. */
        private void fromItem(SelectStmt.FromItem item) {
            if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
                fromItem(join.left());
                fromItem(join.right());
                node(join.on());
                return;
            }
            if (item instanceof SelectStmt.SubqueryFrom) {
                rejectByName(select, ((SelectStmt.SubqueryFrom) item).subquery(), null);
            }
        }

        private void node(Object n) {
            if (n == null) return;
            // A nested query level has a FROM clause of its own, which these bindings say nothing
            // about, so it is judged on its name alone here and properly when it is run.
            if (n instanceof Statement) {
                rejectByName(select, n, null);
                return;
            }
            if (n instanceof ColumnRef) {
                scope.rejectUnresolvedColumns((Expression) n);
                return;
            }
            if (n instanceof FunctionCallExpr || n instanceof WindowFuncExpr) {
                call(n);
                return;
            }
            // An operator's operands are written left to right and read in that order; the
            // reflective walk gives no order of its own.
            if (n instanceof BinaryExpr) {
                node(((BinaryExpr) n).left());
                node(((BinaryExpr) n).right());
                return;
            }
            List<Object> children = new ArrayList<Object>();
            AstWalk.forEachChild(n, children::add);
            for (Object child : children) node(child);
        }

        /**
         * A call is transformed argument by argument, then its FILTER predicate — which is coerced
         * to boolean — and only then is the function itself resolved. The complaint that the call
         * is not an aggregate is therefore the last thing said about it, not the first.
         */
        private void call(Object c) {
            List<Expression> args = c instanceof FunctionCallExpr
                    ? ((FunctionCallExpr) c).args() : ((WindowFuncExpr) c).args();
            if (args != null) {
                for (Expression arg : args) node(arg);
            }
            Expression filter = filterOf(c);
            if (filter != null) {
                node(filter);
                // An aggregate written in a FILTER predicate is its own complaint, and that one
                // comes first; coercing the predicate would replace it.
                if (!select.containsAggregate(filter)) scope.rejectNonBooleanFilter(filter);
            }
            if (c instanceof WindowFuncExpr) {
                WindowFuncExpr window = (WindowFuncExpr) c;
                if (window.partitionBy() != null) {
                    for (Expression p : window.partitionBy()) node(p);
                }
                if (window.orderBy() != null) {
                    for (SelectStmt.OrderByItem o : window.orderBy()) node(o.expr());
                }
            }
            String name = nameOf(c);
            if (name == null) return;
            scope.rejectUnresolvableCall(c, QueryLevelScope.bareName(name));
            if (carriesRefusedFilter(select, c)) throw refusal(c);
        }
    }

    /** The 42809 a call carrying a clause it may not have is refused with. */
    private static MemgresException refusal(Object found) {
        String name = nameOf(found);
        MemgresException e = new MemgresException(
                clauseOf(found) + " specified, but " + name + " is not an aggregate function",
                "42809");
        // PostgreSQL points at the first character of the function name, which for a qualified
        // call is the first character of the qualifier.
        int dot = name.indexOf('.');
        e.setPositionToken(dot > 0 ? name.substring(0, dot) : name);
        return e;
    }

    private static void rejectByName(SelectExecutor select, Object root, QueryLevelScope scope) {
        Object found = AstWalk.findFirst(root, node -> carriesRefusedFilter(select, node));
        if (found == null) return;
        if (scope != null && QueryLevelScope.isOwnLevel(root, found)) {
            reportEarlierFault(select, scope, found);
        }
        throw refusal(found);
    }

    /**
     * Whatever PostgreSQL would have complained about before it got this far: an argument naming a
     * column that is not there, then the same in the FILTER predicate, then a predicate that is not
     * a condition, then a call whose argument types resolve to no function of that name.
     */
    private static void reportEarlierFault(SelectExecutor select, QueryLevelScope scope,
                                           Object found) {
        List<Expression> args = found instanceof FunctionCallExpr
                ? ((FunctionCallExpr) found).args() : ((WindowFuncExpr) found).args();
        for (Expression arg : args) scope.rejectUnresolvedColumns(arg);
        Expression filter = filterOf(found);
        scope.rejectUnresolvedColumns(filter);
        if (filter != null) scope.rejectNonBooleanFilter(filter);
        scope.rejectUnresolvableCall(found, QueryLevelScope.bareName(nameOf(found)));
    }

    private static boolean carriesRefusedFilter(SelectExecutor select, Object node) {
        String rawName = nameOf(node);
        if (rawName == null) return false;
        Expression filter = filterOf(node);
        // DISTINCT inside a call means the same thing FILTER does — accumulate a chosen subset —
        // and an ORDER BY written among a call's arguments says which order it accumulates them
        // in. Both belong to a call that accumulates rows, so PostgreSQL refuses either on a plain
        // function in the same words it refuses FILTER.
        if (filter == null && !isDistinct(node) && !hasAggregateOrderBy(node)) return false;
        // What is inside the FILTER is judged before the call carrying it: an aggregate written
        // in a FILTER predicate is 42803 "aggregate functions are not allowed in FILTER", and
        // PostgreSQL says that rather than this whichever call the FILTER hangs off.
        if (filter != null && select.containsAggregate(filter)) return false;
        String bare = FunctionEvaluator.stripSchemaPrefix(rawName.toLowerCase(Locale.ROOT));
        if (select.isAggregateFunction(rawName)) return false;
        if (PlacementCheck.isWindowFunctionName(bare) && !select.hasUserFunction(bare)) return false;
        // A qualifier has to name a schema the function is actually in, or the call resolves to
        // nothing and PostgreSQL says so before it looks at the FILTER: information_schema.abs is
        // 42883, not a complaint about abs not being an aggregate. Built-ins answer to pg_catalog
        // and nothing else; anything a user declared is looked for by name.
        int dot = rawName.lastIndexOf('.');
        // The lexer folds an unquoted name and leaves a quoted one as written, so a name that
        // still carries a capital was written in quotes. "ABS" is not abs and resolves to no
        // built-in at all, and PostgreSQL reports the missing function rather than the FILTER.
        String written = rawName.substring(dot + 1);
        if (dot > 0) {
            String qualifier = rawName.substring(0, dot).toLowerCase(Locale.ROOT);
            return "pg_catalog".equals(qualifier)
                    ? BuiltinFunctionNames.contains(written)
                    : select.hasUserFunction(bare);
        }
        return BuiltinFunctionNames.contains(written) || select.hasUserFunction(bare);
    }

    /**
     * The clause PostgreSQL names when a call carries more than one of them. It reads them in a
     * fixed order — DISTINCT, then WITHIN GROUP, then ORDER BY, then FILTER, then OVER — and stops
     * at the first, so {@code abs(v ORDER BY v) FILTER (WHERE true)} is complained about for its
     * ORDER BY and not for its FILTER.
     */
    private static String clauseOf(Object node) {
        if (isDistinct(node)) return "DISTINCT";
        if (hasAggregateOrderBy(node)) return "ORDER BY";
        return "FILTER";
    }

    private static boolean isDistinct(Object node) {
        return node instanceof FunctionCallExpr && ((FunctionCallExpr) node).distinct;
    }

    /**
     * An ORDER BY written among a call's arguments, which only an aggregate has a use for. A
     * window call cannot carry one at all — the parser refuses the combination — so this is a
     * plain call's property alone.
     */
    private static boolean hasAggregateOrderBy(Object node) {
        if (!(node instanceof FunctionCallExpr)) return false;
        List<SelectStmt.OrderByItem> orderBy = ((FunctionCallExpr) node).orderBy();
        return orderBy != null && !orderBy.isEmpty();
    }

    private static String nameOf(Object node) {
        if (node instanceof FunctionCallExpr) return ((FunctionCallExpr) node).name();
        if (node instanceof WindowFuncExpr) return ((WindowFuncExpr) node).name();
        return null;
    }

    private static Expression filterOf(Object node) {
        if (node instanceof FunctionCallExpr) return ((FunctionCallExpr) node).filter();
        if (node instanceof WindowFuncExpr) return ((WindowFuncExpr) node).filter();
        return null;
    }
}
