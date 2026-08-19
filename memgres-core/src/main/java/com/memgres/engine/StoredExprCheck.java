package com.memgres.engine;

import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Definition-time checking of a boolean expression stored inside another object's definition —
 * a trigger's {@code WHEN} clause and a policy's {@code USING} / {@code WITH CHECK} clauses.
 *
 * <p>These three clauses are never evaluated when they are written, so a name that does not
 * resolve or a value that is not a boolean stays invisible until the trigger fires or the policy
 * is applied to a row — at which point the failure surfaces during someone else's DML. PostgreSQL
 * resolves them against the target relation at definition time instead, and the checks it makes
 * are the same for all three, so they live here rather than being restated at each call site.
 *
 * <p>The type check is deliberately one-sided: an expression whose type cannot be determined
 * without evaluation is left alone, and only one that provably yields something other than
 * boolean is rejected.
 */
final class StoredExprCheck {

    private final Table table;
    private final String argumentLabel;
    private final Set<String> rowAliases;
    private final String subqueryError;
    private final String aggregateError;
    private final boolean bareColumnIsAmbiguous;

    private StoredExprCheck(Table table, String argumentLabel, Set<String> rowAliases,
                            String subqueryError, String aggregateError, boolean bareColumnIsAmbiguous) {
        this.table = table;
        this.argumentLabel = argumentLabel;
        this.rowAliases = rowAliases;
        this.subqueryError = subqueryError;
        this.aggregateError = aggregateError;
        this.bareColumnIsAmbiguous = bareColumnIsAmbiguous;
    }

    /**
     * A row trigger's {@code WHEN} condition. Both {@code OLD} and {@code NEW} are in scope, so an
     * unqualified column name is ambiguous rather than a shorthand for either of them; a subquery
     * has no plan to run in at row level and PostgreSQL rejects it outright.
     */
    static StoredExprCheck forTriggerWhen(Table table) {
        Set<String> aliases = new LinkedHashSet<>();
        aliases.add("new");
        aliases.add("old");
        return new StoredExprCheck(table, "WHEN", aliases,
                "cannot use subquery in trigger WHEN condition",
                "aggregate functions are not allowed in trigger WHEN conditions", true);
    }

    /** A policy's {@code USING} or {@code WITH CHECK} expression, resolved against the table. */
    static StoredExprCheck forPolicy(Table table) {
        Set<String> aliases = new LinkedHashSet<>();
        aliases.add(table.getName().toLowerCase(Locale.ROOT));
        return new StoredExprCheck(table, "POLICY", aliases, null,
                "aggregate functions are not allowed in policy expressions", false);
    }

    /** @param select the engine, which is what says whether a call is an aggregate */
    void check(Expression expr, SelectExecutor select) {
        if (expr == null) return;
        if (subqueryError != null && AstWalk.anyMatch(expr, StoredExprCheck::isSubquery)) {
            throw PgErrors.notImplemented(subqueryError);
        }
        // A call carrying FILTER, DISTINCT or an aggregate ORDER BY without being an aggregate is
        // refused by the same walk that finds a misplaced aggregate, because PostgreSQL reports
        // whichever of the two it reads first rather than one kind before the other.
        select.placementCheck.rejectStoredDefinition(expr, aggregateClause(), null);
        if (aggregateError != null && aggregatesOutsideSubquery(expr)) {
            throw new MemgresException(aggregateError, "42803");
        }
        checkColumnRefs(expr);
        BooleanContext.check(expr, argumentLabel, BooleanContext.Types.of(table, rowAliases));
    }

    /** The clause name in {@link #aggregateError}, which is what a misplaced call is refused for. */
    private String aggregateClause() {
        String prefix = "aggregate functions are not allowed in ";
        return aggregateError != null && aggregateError.startsWith(prefix)
                ? aggregateError.substring(prefix.length()) : argumentLabel;
    }

    /**
     * A subquery brings its own relations into scope, so neither its column names nor an
     * aggregate inside it belong to the clause being checked. Both walks stop at its boundary.
     */
    private void checkColumnRefs(Object node) {
        if (node == null || isSubquery(node)) return;
        if (node instanceof ColumnRef) checkColumnRef((ColumnRef) node);
        AstWalk.forEachChild(node, this::checkColumnRefs);
    }

    private static boolean aggregatesOutsideSubquery(Object node) {
        if (node == null || isSubquery(node)) return false;
        if (isAggregateCall(node)) return true;
        boolean[] found = new boolean[1];
        AstWalk.forEachChild(node, child -> {
            if (!found[0] && aggregatesOutsideSubquery(child)) found[0] = true;
        });
        return found[0];
    }

    private void checkColumnRef(ColumnRef ref) {
        String qualifier = ref.table();
        if (qualifier != null) {
            if (!rowAliases.contains(qualifier.toLowerCase(Locale.ROOT))) {
                throw new MemgresException(
                        "missing FROM-clause entry for table \"" + qualifier + "\"", "42P01");
            }
            // Every relation carries the system columns whether or not anybody declared them, so
            // NEW.ctid and OLD.tableoid resolve in a trigger's WHEN condition exactly as they do
            // in a query. Testing only the declared columns reported one of them as a column that
            // is not there.
            if (DdlDefinitionChecks.isSystemColumnName(ref.column())) return;
            if (table.getColumnIndex(ref.column()) < 0) {
                throw new MemgresException("column " + qualifier.toLowerCase(Locale.ROOT) + "."
                        + ref.column() + " does not exist", "42703");
            }
            return;
        }
        // As above, for a name written without a relation: a policy decides about the relation's
        // own rows, and reads a system column off one exactly as it reads a declared column.
        if (table.getColumnIndex(ref.column()) < 0
                && !DdlDefinitionChecks.isSystemColumnName(ref.column())) {
            throw new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
        }
        if (bareColumnIsAmbiguous) {
            throw new MemgresException(
                    "column reference \"" + ref.column() + "\" is ambiguous", "42702");
        }
    }

    private static boolean isSubquery(Object node) {
        return node instanceof SubqueryExpr || node instanceof ExistsExpr || node instanceof SelectStmt;
    }

    /** True when an aggregate or window call appears anywhere inside the expression. */
    static boolean hasAggregate(Expression expr) {
        return AstWalk.anyMatch(expr, n -> n instanceof WindowFuncExpr || isAggregateCall(n));
    }

    private static boolean isAggregateCall(Object node) {
        if (!(node instanceof FunctionCallExpr)) return false;
        String name = ((FunctionCallExpr) node).name().toLowerCase(Locale.ROOT);
        int dot = name.lastIndexOf('.');
        if (dot >= 0) name = name.substring(dot + 1);
        return SelectExecutor.AGGREGATE_FUNCTIONS.contains(name);
    }

}
