package com.memgres.engine;

import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WildcardExpr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A correlated {@code EXISTS} answered from a set of key values instead of a scan per outer row.
 *
 * <p>Every catalog integrity check has the same shape:
 * {@code NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = o.oprcode)}. Run as written, the
 * relation behind the subquery is scanned once for every row of the outer one — a few thousand
 * rows on either side is a few million comparisons, and the check that PostgreSQL answers in
 * milliseconds took seconds. The subquery reads one relation and tests one column against a
 * value that does not depend on it, so the column's values are collected once for the statement
 * and the outer value is looked up in them.
 *
 * <p>Only that exact shape is taken, and only when the key is a number: anything else — a join,
 * a grouped subquery, a key of a type whose equality is not exact — answers null here and is run
 * as written, because a lookup that disagrees with the engine's own comparison would be worse
 * than a slow one.
 */
final class ExistsKeyIndex {

    /** Stands for a subquery of a shape this cannot answer; recorded so it is examined once. */
    static final ExistsKeyIndex NOT_INDEXABLE = new ExistsKeyIndex(null, null);

    /** {@code SELECT <key column> FROM <the subquery's relation>}, with the equality dropped. */
    private final SelectStmt keyQuery;

    /** The side of the equality that does not read the subquery's own relation. */
    private final Expression outerSide;

    /** The key column's values, built on the first probe and kept for the statement. */
    private Set<Object> keys;

    /** True once the key column turned out to hold something this cannot compare exactly. */
    private boolean unusable;

    private ExistsKeyIndex(SelectStmt keyQuery, Expression outerSide) {
        this.keyQuery = keyQuery;
        this.outerSide = outerSide;
    }

    /** The expression whose value is looked up; evaluated in the outer row's context. */
    Expression outerSide() {
        return outerSide;
    }

    /** The plan for this subquery, or {@link #NOT_INDEXABLE} when it has another shape. */
    static ExistsKeyIndex plan(ExistsExpr ex) {
        Statement stmt = ex.subquery();
        if (!(stmt instanceof SelectStmt)) return NOT_INDEXABLE;
        SelectStmt s = (SelectStmt) stmt;
        if (s.distinct() || notEmpty(s.distinctOn()) || notEmpty(s.groupBy()) || s.having() != null
                || notEmpty(s.orderBy()) || s.limit() != null || s.offset() != null
                || notEmpty(s.withClauses()) || notEmpty(s.windowDefs())
                || s.groupingSets() != null && !s.groupingSets().isEmpty()
                || s.lockClause() != null) {
            return NOT_INDEXABLE;
        }
        if (s.from() == null || s.from().size() != 1) return NOT_INDEXABLE;
        if (!(s.from().get(0) instanceof SelectStmt.TableRef)) return NOT_INDEXABLE;
        SelectStmt.TableRef rel = (SelectStmt.TableRef) s.from().get(0);
        if (notEmpty(rel.columnAliases())) return NOT_INDEXABLE;
        // An aggregate in the select list makes the subquery answer one row however many the
        // relation holds, so EXISTS over it is true regardless of the WHERE. Only targets that
        // cannot be a function call at all are accepted.
        if (s.targets() == null || s.targets().isEmpty()) return NOT_INDEXABLE;
        for (SelectStmt.SelectTarget t : s.targets()) {
            Expression e = t.expr();
            if (!(e instanceof Literal) && !(e instanceof ColumnRef) && !(e instanceof WildcardExpr)) {
                return NOT_INDEXABLE;
            }
        }
        if (!(s.where() instanceof BinaryExpr)) return NOT_INDEXABLE;
        BinaryExpr eq = (BinaryExpr) s.where();
        if (eq.op() != BinaryExpr.BinOp.EQUAL) return NOT_INDEXABLE;
        String alias = rel.alias() != null ? rel.alias() : rel.table();
        if (alias == null) return NOT_INDEXABLE;
        ColumnRef key = keyColumn(eq.left(), alias);
        Expression outer = eq.right();
        if (key == null) {
            key = keyColumn(eq.right(), alias);
            outer = eq.left();
        }
        if (key == null || readsRelation(outer, alias)) return NOT_INDEXABLE;

        List<SelectStmt.SelectTarget> targets = new ArrayList<SelectStmt.SelectTarget>();
        targets.add(new SelectStmt.SelectTarget(key, null));
        List<SelectStmt.FromItem> from = new ArrayList<SelectStmt.FromItem>();
        from.add(rel);
        SelectStmt keyQuery = new SelectStmt(false, null, targets, from, null, null, null,
                null, null, null, null, null, null);
        return new ExistsKeyIndex(keyQuery, outer);
    }

    /**
     * Whether the key column holds {@code outerValue}. Null when the values cannot be compared
     * exactly this way, which leaves the subquery to be run as written.
     */
    Boolean contains(AstExecutor executor, Object outerValue) {
        Object probe = keyOf(outerValue);
        if (probe == null) return null;
        if (unusable) return null;
        if (keys == null) {
            Set<Object> built = new HashSet<Object>();
            QueryResult result;
            try {
                result = executor.executeStatement(keyQuery);
            } catch (RuntimeException notReadableThatWay) {
                // Reading the key column of every row is not what the subquery asked for: it asked
                // for the rows its own qualification holds of, and PostgreSQL stops reading at the
                // first of them. A column that raises for a row behind that one is never read, so
                // the subquery is run as it was written and answers for itself.
                unusable = true;
                return null;
            }
            for (Object[] row : result.getRows()) {
                Object v = row.length > 0 ? row[0] : null;
                if (v == null) continue;   // NULL is equal to nothing, so it is no key
                Object k = keyOf(v);
                if (k == null) {
                    unusable = true;
                    return null;
                }
                built.add(k);
            }
            keys = built;
        }
        return Boolean.valueOf(keys.contains(probe));
    }

    /**
     * A value's identity in the key set. Only whole numbers are taken: their equality is exact,
     * where text and the types built on it carry collation and padding rules that belong to the
     * engine's own comparison rather than to a hash set.
     */
    private static Object keyOf(Object v) {
        if (v instanceof RegprocValue) return Long.valueOf(((RegprocValue) v).oid());
        if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            return Long.valueOf(((Number) v).longValue());
        }
        return null;
    }

    /** The reference to a column of the subquery's own relation, or null when it is not one. */
    private static ColumnRef keyColumn(Expression e, String alias) {
        if (!(e instanceof ColumnRef)) return null;
        ColumnRef c = (ColumnRef) e;
        return alias.equalsIgnoreCase(c.table()) ? c : null;
    }

    /**
     * True when the expression reads the subquery's own relation — including through an
     * unqualified name, which might resolve to it.
     */
    private static boolean readsRelation(Expression e, String alias) {
        return AstWalk.anyMatch(e, node -> {
            if (!(node instanceof ColumnRef)) return false;
            ColumnRef c = (ColumnRef) node;
            return c.table() == null || alias.equalsIgnoreCase(c.table());
        });
    }

    private static boolean notEmpty(List<?> list) {
        return list != null && !list.isEmpty();
    }
}
