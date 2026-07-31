package com.memgres.engine;

import com.memgres.engine.parser.ast.AnyAllExpr;
import com.memgres.engine.parser.ast.ArraySubqueryExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.OrderedSetAggExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.SubqueryExpr;
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

    /** What a node is, when it is one of the calls a clause may forbid. */
    private enum Kind { WINDOW, NO_OVER, WITHIN_GROUP, GROUPING, AGGREGATE }

    /**
     * Rejects the first misplaced call written anywhere in {@code node}'s own scope.
     *
     * <p>One walk finds all four kinds, and the first one reached decides the message, because
     * PostgreSQL analyses an expression as it reads it: {@code WHERE count(*) > 1 AND rank() = 1}
     * complains about the aggregate and the same two the other way round about the missing OVER.
     * A window call encloses whatever its OVER clause reads, so an aggregate under one is reached
     * second and the window call is still what gets named.
     *
     * <p>GROUPING is found by the same walk as the aggregates because it is evaluated at the same
     * point, but it reads the grouping rather than the rows, and PostgreSQL keeps a parallel set of
     * messages naming it as its own kind of operation. So does a window function written without
     * OVER: PostgreSQL knows what it is and says the OVER clause is what is missing.
     *
     * @param clause the clause name PostgreSQL puts in the message ("WHERE", "VALUES", ...)
     */
    void reject(Object node, String clause) {
        reject(node, clause, null);
    }

    /**
     * The same, for a clause whose query level has already resolved its relations.
     *
     * <p>An aggregate's arguments are transformed before the aggregate is placed, so a column that
     * is not there is what PostgreSQL reports for {@code WHERE count(nosuchcol) > 0} — the
     * misplacement is the second fault, not the first. {@code scope} is what says which columns
     * exist; a null one leaves the misplacement to be reported on its own as before.
     */
    void reject(Object node, String clause, QueryLevelScope scope) {
        Object found = findInScope(node, false);
        if (found == null) return;
        // What PostgreSQL has already transformed when it refuses the call is the call's own
        // arguments, not its OVER specification: the window definitions of a query are transformed
        // as a group, after every clause has been read, so a column that is not there in one is
        // never reached for a window call the clause could not hold anyway.
        if (scope != null) scope.rejectUnresolvedColumns(argumentsOf(found));
        throw misplaced(found, clause);
    }

    /** The parts of a call PostgreSQL resolves before it decides the call may not stand here. */
    private static Object argumentsOf(Object found) {
        if (!(found instanceof WindowFuncExpr)) return found;
        WindowFuncExpr wf = (WindowFuncExpr) found;
        List<Object> parts = new ArrayList<Object>();
        if (wf.args() != null) parts.addAll(wf.args());
        if (wf.filter() != null) parts.add(wf.filter());
        return parts;
    }

    /**
     * Rejects a window call only. A window specification is read once per result row, so an
     * aggregate in one is ordinary — {@code WINDOW w AS (PARTITION BY count(*))} over a grouped
     * query partitions by the group's count — while a window call in one would have to be numbered
     * against the rows it is helping to define.
     */
    void rejectWindowCall(Object node, String clause) {
        Object found = findInScope(node, true);
        if (found != null) throw misplaced(found, clause);
    }

    /**
     * The first misplaced call PostgreSQL would reach, not descending into a nested query. Written
     * as an explicit search rather than {@link AstWalk#anyMatch} because that walk has no way to
     * refuse a subtree, and refusing the subtree is the whole scope rule — and because the order
     * matters as well as the answer, which {@link #forEachAnalysedChild} is what settles.
     */
    private Object findInScope(Object node, boolean windowsOnly) {
        if (node == null || node instanceof Statement) return null;
        Object[] found = new Object[1];
        forEachAnalysedChild(node, child -> {
            if (found[0] == null) {
                Object hit = findInScope(child, windowsOnly);
                if (hit != null) found[0] = hit;
            }
        });
        if (found[0] != null) return found[0];
        return kindOf(node, windowsOnly) != null ? node : null;
    }

    /**
     * The children of a node in the order PostgreSQL analyses them, which is what decides the
     * message when a clause holds more than one thing it may not hold.
     *
     * <p>An expression is transformed from the leaves up, so a call's arguments are analysed before
     * the call itself: {@code HAVING sum(row_number() OVER ()) > 1} is the window call being
     * refused, not the aggregate around it, even though the aggregate is written first.
     *
     * <p>A window call's OVER specification is the exception. The window definitions of a query are
     * transformed together once every clause has been read, so nothing in one is reached while the
     * clause holding the call is being judged — {@code WHERE row_number() OVER (ORDER BY nosuch)}
     * is the window call standing in WHERE, not the column that is not there.
     */
    private static void forEachAnalysedChild(Object node, java.util.function.Consumer<Object> action) {
        if (!(node instanceof WindowFuncExpr)) {
            AstWalk.forEachChild(node, action);
            return;
        }
        WindowFuncExpr wf = (WindowFuncExpr) node;
        if (wf.args() != null) for (Expression arg : wf.args()) if (arg != null) action.accept(arg);
        if (wf.filter() != null) action.accept(wf.filter());
    }

    private Kind kindOf(Object node, boolean windowsOnly) {
        if (node instanceof WindowFuncExpr) return Kind.WINDOW;
        if (node instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) node;
            String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase(Locale.ROOT));
            // A function the user declared under one of these names is that function, not the
            // built-in window one.
            if (!select.hasUserFunction(name)) {
                if (HYPOTHETICAL_SET_FUNCTIONS.contains(name)) {
                    return fn.args().isEmpty() ? Kind.NO_OVER : Kind.WITHIN_GROUP;
                }
                if (WINDOW_ONLY_FUNCTIONS.contains(name)) return Kind.NO_OVER;
            }
            if (windowsOnly) return null;
            if (GroupByValidator.isGroupingCall(fn)) return Kind.GROUPING;
            if (select.isAggregateFunction(fn.name())) return Kind.AGGREGATE;
            return null;
        }
        if (!windowsOnly && node instanceof OrderedSetAggExpr) return Kind.AGGREGATE;
        return null;
    }

    private MemgresException misplaced(Object node, String clause) {
        Kind kind = kindOf(node, false);
        if (kind == Kind.WINDOW) {
            return new MemgresException("window functions are not allowed in " + clause, "42P20");
        }
        if (kind == Kind.GROUPING) {
            return new MemgresException(
                    "grouping operations are not allowed in " + clause, "42803");
        }
        if (kind == Kind.AGGREGATE) {
            return new MemgresException(
                    "aggregate functions are not allowed in " + clause, "42803");
        }
        return missingClause(node, kind);
    }

    /**
     * What a window function written as a plain call is missing. The four hypothetical-set names
     * are ordered-set aggregates as well as window functions, so with arguments what they lack is
     * WITHIN GROUP and without them it is OVER; PostgreSQL distinguishes the two and so does this.
     * Neither message names the clause, so this needs no clause to name.
     */
    private static MemgresException missingClause(Object node, Kind kind) {
        String name = FunctionEvaluator.stripSchemaPrefix(
                ((FunctionCallExpr) node).name().toLowerCase(Locale.ROOT));
        return new MemgresException(kind == Kind.WITHIN_GROUP
                ? "WITHIN GROUP is required for ordered-set aggregate " + name
                : "window function " + name + " requires an OVER clause", "42809");
    }

    // ---- Sub-queries where a definition may not hold one ----

    /**
     * Rejects a sub-query written in a definition that is stored and replayed per row. A CHECK
     * constraint and an index expression are evaluated against one row with no query around them,
     * so PostgreSQL refuses to store one that would have to read another relation — the walk in
     * {@link #reject} deliberately stops at a nested query, which is why this is asked separately.
     *
     * @param what the phrase PostgreSQL completes "cannot use subquery in ..." with
     */
    void rejectSubquery(Object node, String what) {
        if (node == null) return;
        if (AstWalk.anyMatch(node, n -> n instanceof SubqueryExpr || n instanceof ExistsExpr
                || n instanceof AnyAllExpr || n instanceof ArraySubqueryExpr)) {
            throw PgErrors.notImplemented("cannot use subquery in " + what);
        }
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

    private boolean isAggregateCall(Object node) {
        if (node instanceof OrderedSetAggExpr) return true;
        return node instanceof FunctionCallExpr
                && select.isAggregateFunction(((FunctionCallExpr) node).name());
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
     * True when a call is a window function, and so may carry an OVER clause on its own account.
     * An aggregate may carry one too, but for a different reason, so the callers ask separately.
     */
    static boolean isWindowFunctionName(String name) {
        if (name == null) return false;
        String stripped = FunctionEvaluator.stripSchemaPrefix(name.toLowerCase());
        return WINDOW_ONLY_FUNCTIONS.contains(stripped)
                || HYPOTHETICAL_SET_FUNCTIONS.contains(stripped);
    }

    /**
     * Rejects a window function written as a plain call, anywhere in the tree — including inside
     * another window call's own specification, which is why this is a walk of its own rather than
     * the one {@link #reject} uses, which stops at the enclosing window call.
     */
    void rejectWindowCallWithoutOver(Object node) {
        if (node == null || node instanceof Statement) return;
        Kind kind = kindOf(node, true);
        if (kind == Kind.NO_OVER || kind == Kind.WITHIN_GROUP) throw missingClause(node, kind);
        AstWalk.forEachChild(node, this::rejectWindowCallWithoutOver);
    }

    /**
     * The select list, which PostgreSQL analyses before WHERE.
     *
     * <p>The OVER-clause check is split at WHERE rather than run over the whole query at once
     * because PostgreSQL raises it while it transforms each clause, in the order it transforms
     * them — FROM, then the select list, then WHERE, then HAVING, the window definitions, ORDER
     * BY, GROUP BY and last LIMIT and OFFSET. A single pass ahead of the rest reports whichever
     * bare window call it happens to reach first, so {@code SELECT * FROM t WHERE count(*) > 1 AND
     * row_number() = 1} named the window call where PostgreSQL names the aggregate: within WHERE
     * both kinds are found by one left-to-right walk, and count(*) is written first.
     */
    void rejectWindowCallWithoutOverInTargets(SelectStmt stmt) {
        if (stmt.targets() == null) return;
        for (SelectStmt.SelectTarget target : stmt.targets()) rejectWindowCallWithoutOver(target.expr());
    }

    /** The clauses read after WHERE, in the order PostgreSQL reads them. */
    void rejectWindowCallWithoutOverAfterWhere(SelectStmt stmt) {
        rejectWindowCallWithoutOver(stmt.having());
        // A named window is a specification like any other, and is read even when nothing
        // references it.
        if (stmt.windowDefs() != null) {
            for (SelectStmt.WindowDef def : stmt.windowDefs()) {
                if (def.partitionBy() != null) {
                    for (Expression p : def.partitionBy()) rejectWindowCallWithoutOver(p);
                }
                if (def.orderBy() != null) {
                    for (SelectStmt.OrderByItem o : def.orderBy()) rejectWindowCallWithoutOver(o.expr());
                }
            }
        }
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem item : stmt.orderBy()) rejectWindowCallWithoutOver(item.expr());
        }
        if (stmt.groupBy() != null) {
            for (Expression g : stmt.groupBy()) rejectWindowCallWithoutOver(g);
        }
        rejectWindowCallWithoutOver(stmt.limit());
        rejectWindowCallWithoutOver(stmt.offset());
    }
}
