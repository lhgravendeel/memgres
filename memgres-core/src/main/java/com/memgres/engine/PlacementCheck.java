package com.memgres.engine;

import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.OrderedSetAggExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The clauses in which an aggregate or a window call may not stand, and the walk that finds one.
 *
 * <p>An aggregate has a value only once a group of rows has been collected, and a window call only
 * once the result rows exist to be numbered against one another. A clause that is read before
 * either of those things has happened — WHERE, a JOIN condition, LIMIT, OFFSET, a VALUES row, the
 * SET list of an UPDATE, a CHECK constraint, an index expression, a DEFAULT — therefore cannot
 * hold one at all, and PostgreSQL says so naming the clause rather than evaluating something
 * arbitrary.
 *
 * <p>Two properties make this correct where a per-node-type test is not:
 *
 * <ul>
 *   <li><b>The walk is complete.</b> It descends every field of every AST node reflectively, so
 *       {@code WHERE a IN (1, count(*))}, {@code WHERE a BETWEEN 0 AND count(*)} and
 *       {@code WHERE a = ANY (ARRAY[count(*)])} are caught by the same code that catches
 *       {@code WHERE count(*) > 1}. Hand-written descent has to name each container, and the
 *       containers it forgets silently become places an aggregate may be written.</li>
 *   <li><b>It stops at a nested query.</b> A subquery has its own FROM and its own grouping, so
 *       {@code WHERE a &gt; (SELECT count(*) FROM u)} is ordinary SQL: the aggregate belongs to the
 *       subquery, not to the WHERE that contains it.</li>
 * </ul>
 *
 * <p>The one exception to that boundary is PostgreSQL's rule for which query level an aggregate
 * belongs to: the innermost level any of its argument variables comes from. An aggregate whose
 * arguments name <em>only</em> columns of an enclosing relation therefore belongs to that
 * enclosing query — {@code UPDATE t SET c = (SELECT max(c) FROM other)}, where {@code c} is a
 * column of {@code t} and not of {@code other}, is an aggregate in the UPDATE rather than in the
 * sub-select, and PostgreSQL rejects it as one. See {@link #rejectOuterLevelAggregate}.
 */
final class PlacementCheck {

    private final SelectExecutor select;

    PlacementCheck(SelectExecutor select) {
        this.select = select;
    }

    /**
     * Rejects a window call, then an aggregate, written anywhere in {@code node}'s own scope.
     *
     * <p>The window call is reported first because an aggregate inside an OVER clause belongs to
     * the window specification: the call that may not stand here at all is the window one.
     *
     * @param clause the clause name PostgreSQL puts in the message ("WHERE", "VALUES", ...)
     */
    void reject(Object node, String clause) {
        if (node == null) return;
        if (findInScope(node, true) != null) {
            throw new MemgresException("window functions are not allowed in " + clause, "42P20");
        }
        if (findInScope(node, false) != null) {
            throw new MemgresException("aggregate functions are not allowed in " + clause, "42803");
        }
    }

    /**
     * The first window call (or aggregate) in the tree, not descending into a nested query.
     * Written as an explicit search rather than {@link AstWalk#anyMatch} because that walk has no
     * way to refuse a subtree, and refusing the subtree is the whole scope rule.
     */
    private Object findInScope(Object node, boolean windows) {
        if (node == null || node instanceof Statement) return null;
        if (windows ? node instanceof WindowFuncExpr : isAggregateCall(node)) return node;
        Object[] found = new Object[1];
        AstWalk.forEachChild(node, child -> {
            if (found[0] == null) {
                Object hit = findInScope(child, windows);
                if (hit != null) found[0] = hit;
            }
        });
        return found[0];
    }

    private boolean isAggregateCall(Object node) {
        if (node instanceof OrderedSetAggExpr) return true;
        return node instanceof FunctionCallExpr
                && select.isAggregateFunction(((FunctionCallExpr) node).name());
    }

    // ---- Aggregates that belong to an enclosing query level ----

    /**
     * Rejects an aggregate written inside a sub-select of this clause whose arguments name only
     * columns of {@code outer} — PostgreSQL counts such an aggregate as belonging to the enclosing
     * query, so it stands in this clause after all.
     *
     * <p>Deliberately narrow, because the cost of being wrong is refusing a valid statement: the
     * sub-select's own scope has to be readable (every FROM item a plain named relation this
     * engine can look up), the aggregate has to name at least one column, none of its columns may
     * be one the sub-select itself supplies, and every one of them has to be a column of
     * {@code outer}. Anything less certain is left alone.
     *
     * @param outerAlias the name the enclosing relation is known by, for qualified references
     */
    void rejectOuterLevelAggregate(Object node, String clause, Table outer, String outerAlias) {
        if (node == null || outer == null) return;
        List<SelectStmt> nested = new ArrayList<SelectStmt>();
        collectNestedSelects(node, nested);
        for (SelectStmt sub : nested) {
            Set<String> ownScope = visibleColumns(sub);
            if (ownScope == null) continue;
            List<Object> aggregates = new ArrayList<Object>();
            collectAggregatesInScope(sub.targets(), aggregates);
            collectAggregatesInScope(sub.where(), aggregates);
            collectAggregatesInScope(sub.having(), aggregates);
            for (Object aggregate : aggregates) {
                if (belongsToOuterLevel(aggregate, ownScope, outer, outerAlias)) {
                    throw new MemgresException(
                            "aggregate functions are not allowed in " + clause, "42803");
                }
            }
        }
    }

    /** Every sub-select written directly in this clause (one level down; each judges its own). */
    private void collectNestedSelects(Object node, List<SelectStmt> out) {
        if (node == null) return;
        if (node instanceof SelectStmt) {
            out.add((SelectStmt) node);
            return;
        }
        if (node instanceof Statement) return;
        AstWalk.forEachChild(node, child -> collectNestedSelects(child, out));
    }

    private void collectAggregatesInScope(Object node, List<Object> out) {
        if (node == null || node instanceof Statement) return;
        if (isAggregateCall(node)) {
            out.add(node);
            return;
        }
        AstWalk.forEachChild(node, child -> collectAggregatesInScope(child, out));
    }

    /**
     * The lowercased column names a sub-select's own FROM supplies, or null when any FROM item is
     * something this check cannot read — a sub-select, a function, a CTE name, a missing relation.
     * A null answer means "unknown", and an unknown scope disables the rule for that sub-select.
     */
    private Set<String> visibleColumns(SelectStmt sub) {
        Set<String> names = new LinkedHashSet<String>();
        if (sub.from() == null || sub.from().isEmpty()) return null;
        for (SelectStmt.FromItem item : sub.from()) {
            if (!addVisibleColumns(item, names)) return null;
        }
        return names;
    }

    private boolean addVisibleColumns(SelectStmt.FromItem item, Set<String> names) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            return addVisibleColumns(join.left(), names) && addVisibleColumns(join.right(), names);
        }
        if (!(item instanceof SelectStmt.TableRef)) return false;
        SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
        Table table = select.lookupRelationOrNull(ref.schema(), ref.table());
        if (table == null) return false;
        // The alias, if any, is a name this sub-select supplies; the relation's own name is too
        // when it is not aliased away.
        names.add(ref.alias() != null ? ref.alias().toLowerCase(Locale.ROOT)
                : table.getName().toLowerCase(Locale.ROOT));
        for (Column column : table.getColumns()) {
            names.add(column.getName().toLowerCase(Locale.ROOT));
        }
        return true;
    }

    private boolean belongsToOuterLevel(Object aggregate, Set<String> ownScope,
                                        Table outer, String outerAlias) {
        List<ColumnRef> refs = new ArrayList<ColumnRef>();
        collectColumnRefsInScope(aggregate, refs);
        if (refs.isEmpty()) return false;
        String alias = outerAlias == null ? null : outerAlias.toLowerCase(Locale.ROOT);
        for (ColumnRef ref : refs) {
            String qualifier = ref.table() == null ? null : ref.table().toLowerCase(Locale.ROOT);
            if (qualifier != null && ownScope.contains(qualifier)) return false;
            if (qualifier == null && ownScope.contains(ref.column().toLowerCase(Locale.ROOT))) {
                return false;
            }
            if (qualifier != null && !qualifier.equals(alias)) return false;
            if (outer.getColumnIndex(ref.column()) < 0) return false;
        }
        return true;
    }

    private void collectColumnRefsInScope(Object node, List<ColumnRef> out) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof ColumnRef) {
            out.add((ColumnRef) node);
            return;
        }
        AstWalk.forEachChild(node, child -> collectColumnRefsInScope(child, out));
    }

    // ---- Window calls written without an OVER clause ----

    /**
     * The calls that exist only as window functions. Written without OVER they are not unknown
     * functions — PostgreSQL knows exactly what they are and says the OVER clause is what is
     * missing, which is the difference between "you spelled it wrong" and "you left off a word".
     */
    private static final Set<String> WINDOW_ONLY_FUNCTIONS = new LinkedHashSet<String>();
    /** Calls that are a window function with no arguments and an ordered-set aggregate with them. */
    private static final Set<String> HYPOTHETICAL_SET_FUNCTIONS = new LinkedHashSet<String>();

    static {
        WINDOW_ONLY_FUNCTIONS.add("row_number");
        WINDOW_ONLY_FUNCTIONS.add("ntile");
        WINDOW_ONLY_FUNCTIONS.add("lag");
        WINDOW_ONLY_FUNCTIONS.add("lead");
        WINDOW_ONLY_FUNCTIONS.add("first_value");
        WINDOW_ONLY_FUNCTIONS.add("last_value");
        WINDOW_ONLY_FUNCTIONS.add("nth_value");
        HYPOTHETICAL_SET_FUNCTIONS.add("rank");
        HYPOTHETICAL_SET_FUNCTIONS.add("dense_rank");
        HYPOTHETICAL_SET_FUNCTIONS.add("percent_rank");
        HYPOTHETICAL_SET_FUNCTIONS.add("cume_dist");
    }

    /**
     * Rejects a window function written as a plain call. The four hypothetical-set names are also
     * ordered-set aggregates, so with arguments what they are missing is WITHIN GROUP and without
     * them it is OVER; PostgreSQL distinguishes the two and so does this.
     */
    void rejectWindowCallWithoutOver(Object node) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) node;
            String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase(Locale.ROOT));
            // A function the user declared under one of these names is that function, not the
            // built-in window one.
            if (!select.hasUserFunction(name)) {
                if (HYPOTHETICAL_SET_FUNCTIONS.contains(name)) {
                    throw new MemgresException(fn.args().isEmpty()
                            ? "window function " + name + " requires an OVER clause"
                            : "WITHIN GROUP is required for ordered-set aggregate " + name,
                            "42809");
                }
                if (WINDOW_ONLY_FUNCTIONS.contains(name)) {
                    throw new MemgresException(
                            "window function " + name + " requires an OVER clause", "42809");
                }
            }
        }
        AstWalk.forEachChild(node, this::rejectWindowCallWithoutOver);
    }

    /** Every clause of a query that is read in the query's own scope, for the OVER-clause check. */
    void rejectWindowCallWithoutOver(SelectStmt stmt) {
        for (SelectStmt.SelectTarget target : stmt.targets()) rejectWindowCallWithoutOver(target.expr());
        rejectWindowCallWithoutOver(stmt.where());
        if (stmt.groupBy() != null) {
            for (Expression g : stmt.groupBy()) rejectWindowCallWithoutOver(g);
        }
        rejectWindowCallWithoutOver(stmt.having());
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem item : stmt.orderBy()) rejectWindowCallWithoutOver(item.expr());
        }
        rejectWindowCallWithoutOver(stmt.limit());
        rejectWindowCallWithoutOver(stmt.offset());
    }
}
