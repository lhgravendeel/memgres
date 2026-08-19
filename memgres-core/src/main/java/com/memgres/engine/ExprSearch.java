package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.OrderedSetAggExpr;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Which aggregate and window calls an expression holds.
 *
 * <p>Whether a query is grouped, and which calls the window pass has to answer for, is decided by
 * looking through an expression for the calls written inside it. Asked by enumerating node types
 * by hand, that question is answered only for the shapes somebody remembered to list: a call
 * written under BETWEEN, inside an ARRAY constructor, in an IN list, under ROW, COLLATE, AT TIME
 * ZONE or IS TRUE was not found, so the query ran ungrouped over rows PostgreSQL folds into one,
 * or took the plain path where a window call has no value and every row answered NULL. The search
 * is over the tree itself, so a container nobody listed is still looked into.
 *
 * <p>It stops at anything standing for a query of its own. A call written inside a sub-select
 * belongs to that sub-select, and reporting it here would group the query around it.
 */
final class ExprSearch {

    private ExprSearch() {}

    /** A node standing for a query of its own: what is written inside it belongs to that query. */
    private static final Predicate<Object> OWN_QUERY = node -> node instanceof Statement;

    /** The same, and each window call, which the searches below do not look inside. */
    private static final Predicate<Object> OWN_QUERY_OR_WINDOW =
            node -> node instanceof Statement || node instanceof WindowFuncExpr;

    /**
     * Whether an aggregate call is written anywhere in the expression.
     *
     * <p>A window function is not itself an aggregate, but what it reads may be one: it runs over
     * the grouped result, so {@code sum(sum(v)) OVER ()} and {@code rank() OVER (ORDER BY sum(v))}
     * are queries with one row per group exactly as {@code sum(v)} on its own would be.
     *
     * <p>A call {@code answered} recognises is not one: an aggregate belonging to an enclosing
     * query level has its value before this query runs, so this query does not group around it.
     */
    static boolean holdsAggregate(Expression expr, Predicate<String> isAggregateName,
                                  Predicate<Expression> answered) {
        final boolean[] found = {false};
        AstWalk.forEachOutside(expr, OWN_QUERY, node -> {
            if (found[0]) return;
            if (!isAggregateCall(node, isAggregateName)) return;
            if (!answered.test((Expression) node)) found[0] = true;
        });
        return found[0];
    }

    /**
     * Every aggregate call written in the expression.
     *
     * <p>The walk stops at each one: what an aggregate reads is read as part of computing it.
     */
    static List<Expression> aggregateCallsIn(Expression expr, Predicate<String> isAggregateName) {
        final List<Expression> out = new ArrayList<Expression>();
        AstWalk.forEachOutside(expr,
                node -> node instanceof Statement || isAggregateCall(node, isAggregateName),
                node -> {
                    if (isAggregateCall(node, isAggregateName)) out.add((Expression) node);
                });
        return out;
    }

    private static boolean isAggregateCall(Object node, Predicate<String> isAggregateName) {
        return node instanceof OrderedSetAggExpr
                || (node instanceof FunctionCallExpr
                        && isAggregateName.test(((FunctionCallExpr) node).name()));
    }

    /** Whether a window call is written anywhere in the expression. */
    static boolean holdsWindowFunction(Expression expr) {
        final boolean[] found = {false};
        AstWalk.forEachOutside(expr, OWN_QUERY_OR_WINDOW, node -> {
            if (node instanceof WindowFuncExpr) found[0] = true;
        });
        return found[0];
    }

    /**
     * Every window call written in the expression.
     *
     * <p>The walk stops at each one: a window function cannot be written inside another, and what
     * the call reads is computed as part of computing the call.
     */
    static List<WindowFuncExpr> windowFunctionsIn(Expression expr) {
        final List<WindowFuncExpr> out = new ArrayList<WindowFuncExpr>();
        AstWalk.forEachOutside(expr, OWN_QUERY_OR_WINDOW, node -> {
            if (node instanceof WindowFuncExpr) out.add((WindowFuncExpr) node);
        });
        return out;
    }

    /**
     * The same, and the calls written inside one, which nesting makes meaningless but which are
     * still there to be refused.
     */
    static List<WindowFuncExpr> allWindowFunctionsIn(Expression expr) {
        final List<WindowFuncExpr> out = new ArrayList<WindowFuncExpr>();
        AstWalk.forEachOutside(expr, OWN_QUERY, node -> {
            if (node instanceof WindowFuncExpr) out.add((WindowFuncExpr) node);
        });
        return out;
    }
}
