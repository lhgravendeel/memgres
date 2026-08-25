package com.memgres.engine;

import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.CompositeStarExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.NamedArgExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WildcardExpr;
import com.memgres.engine.parser.ast.WindowFuncExpr;
import com.memgres.engine.util.Cols;

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
 * happens to check things. {@link Resolve} resolves every call the statement makes before any
 * clause is judged, which is where PostgreSQL resolves it. {@link Ordered} then walks a query
 * level's clauses in that order — WITH items, the FROM clause and its join conditions, the select
 * list, WHERE, HAVING, ORDER BY, and GROUP BY last of all — and refuses the first fault it finds.
 *
 * <p>It walks every statement, not only one already known to be doomed, which is what lets a fault
 * be found in a clause of a query that would have read no rows. What makes that safe is that every
 * refusal it raises is one of {@link QueryLevelScope}'s, and those speak only where the relations
 * this query level supplies are all described and settle the answer. A relation the description
 * could not follow, a call in FROM whose columns only running it settles, an enclosing query that
 * may supply a name this one does not — each of those is a scope this says nothing about.
 */
final class FilterCheck {

    private FilterCheck() {
    }

    /**
     * Refuses the statement for the fault PostgreSQL would name first.
     *
     * <p>A call resolves to a function or it resolves to nothing, and that is settled before any
     * clause is judged: first for an aggregate written with a number of arguments it does not
     * take, then by {@link Resolve} for every other call in the statement. Then, for a query level
     * whose relations are described, the clauses are read in the order PostgreSQL reads them and
     * the first fault of the earliest one is reported. What is left is the older, coarser walk:
     * the reflective one, which reaches every expression position in the statement — including
     * inside sub-queries and CTEs, which is where PostgreSQL raises this too — and refuses the
     * call it finds there.
     *
     * <p>{@code scope} is null wherever the query level's relations are not resolved yet, in which
     * case a call is refused on its name alone as before.
     */
    static void reject(SelectExecutor select, Object root, QueryLevelScope scope) {
        rejectAggregateArity(select, root, scope);
        Resolve resolve = new Resolve(select);
        resolve.node(root, scope != null ? scope : resolve.nested);
        if (scope != null && root instanceof SelectStmt) {
            new Ordered(select, scope).statement((SelectStmt) root);
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
                "json_agg", "jsonb_agg", "json_agg_strict", "jsonb_agg_strict",
                "xmlagg", "stddev", "stddev_pop", "stddev_samp",
                "variance", "var_pop", "var_samp", "range_agg", "range_intersect_agg"};
        for (String n : one) m.put(n, Integer.valueOf(1));
        String[] two = {"string_agg", "json_object_agg", "jsonb_object_agg", "corr",
                "json_object_agg_strict", "jsonb_object_agg_strict",
                "json_object_agg_unique", "jsonb_object_agg_unique",
                "json_object_agg_unique_strict", "jsonb_object_agg_unique_strict",
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
        // A parameterless aggregate is not written with an empty argument list, because the list
        // is not what is empty: the star in count(*) says which rows to count, not what. PG names
        // the spelling to use rather than reporting a count of some other arity.
        String bare = QueryLevelScope.bareName(call.name());
        if (call.args().isEmpty() && SelectAggregateEvaluator.isParameterlessAggregate(bare)) {
            throw new MemgresException(
                    bare + "(*) must be used to call a parameterless aggregate function", "42809");
        }
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < call.args().size(); i++) {
            if (i > 0) types.append(", ");
            DataType type = scope == null ? null : scope.certainTypeOf(call.args().get(i));
            types.append(type == null ? "unknown" : CatalogHelper.pgTypeName(type));
        }
        throw new MemgresException(
                "function " + call.name() + "(" + types + ") does not exist\n"
                        + "  Hint: No function matches the given name and argument types. You might need to add explicit type casts.",
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
     * Resolving every call in a statement, in the order PostgreSQL resolves them.
     *
     * <p>PostgreSQL resolves a call — name, then argument count, then argument types — before it
     * judges the clause the call stands in, so an unknown name outranks a complaint about FILTER,
     * about OVER, and about a column further along the select list. memgres resolved names while
     * it ran, so those complaints came first; this walk moves resolution to where PostgreSQL does
     * it. Unlike {@link Ordered} it runs whether or not the statement is already doomed, which it
     * can only do because it asks nothing but {@link QueryLevelScope#rejectUnresolvableCall} —
     * which reads a complete register of the names the engine can dispatch and a complete table of
     * the signatures PostgreSQL declares, and says nothing about anything either leaves open.
     *
     * <p>The clause order is the same one {@link Ordered} walks. A nested query level is walked
     * too, because a call in one resolves against the same catalog; only its bindings are out of
     * reach, and a scope with none answers for the literals and casts and stays silent about the
     * rest.
     */
    private static final class Resolve {

        private final QueryLevelScope nested;

        Resolve(SelectExecutor select) {
            this.nested = new QueryLevelScope(select, Cols.listOf(), null, null);
        }

        /**
         * @param here the bindings to read a column's type from, which are this query level's; a
         *             nested statement has a FROM clause of its own, so descending into one
         *             carries the bindingless scope instead
         */
        void node(Object n, QueryLevelScope here) {
            if (n == null) return;
            if (n instanceof SelectStmt) {
                statement((SelectStmt) n, here);
                return;
            }
            if (n instanceof FunctionCallExpr || n instanceof WindowFuncExpr) {
                call(n, here);
                return;
            }
            if (n instanceof BinaryExpr) {
                node(((BinaryExpr) n).left(), here);
                node(((BinaryExpr) n).right(), here);
                return;
            }
            QueryLevelScope inner = n instanceof Statement ? nested : here;
            List<Object> children = new ArrayList<Object>();
            AstWalk.forEachChild(n, children::add);
            for (Object child : children) node(child, inner);
        }

        private void statement(SelectStmt stmt, QueryLevelScope here) {
            if (stmt.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : stmt.withClauses()) node(cte.query(), nested);
            }
            if (stmt.from() != null) {
                for (SelectStmt.FromItem item : stmt.from()) node(item, here);
            }
            if (stmt.targets() != null) {
                for (SelectStmt.SelectTarget target : stmt.targets()) node(target.expr(), here);
            }
            node(stmt.where(), here);
            node(stmt.having(), here);
            if (stmt.orderBy() != null) {
                for (SelectStmt.OrderByItem item : stmt.orderBy()) node(item.expr(), here);
            }
            if (stmt.groupBy() != null) {
                for (Expression group : stmt.groupBy()) node(group, here);
            }
        }

        /** Arguments are transformed before the function is resolved, so they are read first. */
        private void call(Object c, QueryLevelScope here) {
            List<Expression> args = c instanceof FunctionCallExpr
                    ? ((FunctionCallExpr) c).args() : ((WindowFuncExpr) c).args();
            if (args != null) {
                for (Expression arg : args) node(arg, here);
            }
            node(filterOf(c), here);
            if (c instanceof WindowFuncExpr) {
                WindowFuncExpr window = (WindowFuncExpr) c;
                if (window.partitionBy() != null) {
                    for (Expression p : window.partitionBy()) node(p, here);
                }
                if (window.orderBy() != null) {
                    for (SelectStmt.OrderByItem o : window.orderBy()) node(o.expr(), here);
                }
            }
            String name = nameOf(c);
            if (name == null) return;
            // count(*) has no argument list to resolve against.
            if (c instanceof FunctionCallExpr && ((FunctionCallExpr) c).star()) return;
            here.rejectUnresolvableCall(c, QueryLevelScope.bareName(name), true);
        }
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
     * bindings settle the answer. What that costs is a fault reported one clause too late wherever
     * the bindings settle nothing; what it buys is that no statement is refused on a guess.
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
            orderBy(stmt);
            if (stmt.groupBy() != null) {
                for (Expression group : stmt.groupBy()) node(group);
            }
        }

        /**
         * ORDER BY, read the way PostgreSQL reads it. A constant there is an output-column
         * position and nothing else, so a position outside the select list is out of range before
         * a name in any later clause is looked up, and a constant that is not an integer at all
         * names no column. The target a position names was read with the select list already.
         *
         * <p>A star stands for however many columns the relations hold, which this does not
         * count, so a select list carrying one leaves the position to the later check that does.
         */
        private void orderBy(SelectStmt stmt) {
            if (stmt.orderBy() == null) return;
            List<SelectStmt.SelectTarget> targets = stmt.targets();
            boolean counted = targets != null && !carriesStar(targets);
            for (SelectStmt.OrderByItem item : stmt.orderBy()) {
                Expression expr = item.expr();
                Integer position = GroupByValidator.integerConstant(expr);
                if (position != null) {
                    if (counted
                            && (position.intValue() < 1 || position.intValue() > targets.size())) {
                        throw new MemgresException(
                                "ORDER BY position " + position + " is not in select list", "42P10");
                    }
                    continue;
                }
                if (expr instanceof Literal) {
                    throw new MemgresException("non-integer constant in ORDER BY", "42601");
                }
                node(expr);
            }
        }

        private static boolean carriesStar(List<SelectStmt.SelectTarget> targets) {
            for (SelectStmt.SelectTarget target : targets) {
                if (target.expr() instanceof WildcardExpr
                        || target.expr() instanceof CompositeStarExpr) {
                    return true;
                }
            }
            return false;
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
            // The OVER specification is deliberately not read here. PostgreSQL collects the window
            // definitions and transforms them once every clause has been read, so nothing written
            // in one is reached while the clause holding the call is being judged: a window call
            // standing in WHERE is refused for standing there, whatever its OVER names.
            String name = nameOf(c);
            if (name == null) return;
            scope.rejectUnresolvableCall(c, QueryLevelScope.bareName(name));
            if (carriesRefusedFilter(select, c)) throw refusal(c);
        }
    }

    /** The 42809 a call carrying a clause it may not have is refused with. */
    static MemgresException refusal(Object found) {
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

    static boolean carriesRefusedFilter(SelectExecutor select, Object node) {
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
