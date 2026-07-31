package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.List;
import java.util.Locale;

/**
 * Who may carry a FILTER clause.
 *
 * <p>FILTER says which rows a call accumulates, so it means something only to a call that
 * accumulates rows. Written on an ordinary function it is not a clause that can be ignored but a
 * call that cannot be built, and PostgreSQL refuses it naming the function:
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
 */
final class FilterCheck {

    private FilterCheck() {
    }

    /**
     * Refuses the first call in {@code root} that carries a FILTER it may not have.
     *
     * <p>The walk is the reflective one, so it reaches every expression position in the statement
     * — including the ones inside sub-queries and CTEs, which is where PostgreSQL raises this too.
     *
     * <p>PostgreSQL reaches this complaint last, so before it is raised the call is put through
     * the analysis that comes first: its arguments are resolved, then its FILTER predicate, which
     * is coerced to boolean, and only then is the function itself looked up. Each of those can
     * report a fault of its own, and PostgreSQL reports that one instead. {@code scope} is what
     * makes that possible and is null wherever the query level's relations are not resolved yet,
     * in which case the call is refused on its name alone as before.
     */
    static void reject(SelectExecutor select, Object root, QueryLevelScope scope) {
        Object found = AstWalk.findFirst(root, node -> carriesRefusedFilter(select, node));
        if (found == null) return;
        if (scope != null && QueryLevelScope.isOwnLevel(root, found)) {
            reportEarlierFault(select, scope, found);
        }
        String name = nameOf(found);
        MemgresException e = new MemgresException(
                clauseOf(found) + " specified, but " + name + " is not an aggregate function",
                "42809");
        // PostgreSQL points at the first character of the function name, which for a qualified
        // call is the first character of the qualifier.
        int dot = name.indexOf('.');
        e.setPositionToken(dot > 0 ? name.substring(0, dot) : name);
        throw e;
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
        // so PostgreSQL refuses it on a plain function in the same words.
        if (filter == null && !isDistinct(node)) return false;
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

    /** The clause PostgreSQL names in the message: FILTER when there is one, else DISTINCT. */
    private static String clauseOf(Object node) {
        return filterOf(node) != null ? "FILTER" : "DISTINCT";
    }

    private static boolean isDistinct(Object node) {
        return node instanceof FunctionCallExpr && ((FunctionCallExpr) node).distinct;
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
