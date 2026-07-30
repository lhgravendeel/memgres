package com.memgres.engine;

import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.WildcardExpr;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Whether a subquery reads anything of the row it is written beside.
 *
 * <p>A subquery that does not is one PostgreSQL runs once for the whole statement — an InitPlan —
 * and this engine ran once per row, which turned a catalogue lookup written as
 * {@code att.attrelid = (SELECT oid FROM pg_class WHERE relname = 't')} into a scan of pg_class
 * for every attribute in the database. Running it once is both faster and closer to PostgreSQL:
 * an uncorrelated sublink there answers with a single value however many rows the query has, even
 * when what it computes is {@code random()}.
 *
 * <p>A correlated subquery must still be run per row, and being wrong about which is which gives
 * wrong answers rather than slow ones. So this reads the question off the query rather than off
 * an attempt to run it — a reference to an outer column inside a branch that is not taken makes a
 * subquery correlated whether or not running it once ever reaches that branch — and it answers no
 * to everything it cannot settle:
 *
 * <ul>
 *   <li>only a plain SELECT, with no WITH list of its own;</li>
 *   <li>whose FROM names only stored tables and system catalogues — a WITH item, a view, a
 *       subquery, a set-returning function or a join tree is left alone, because what those
 *       expose is not something to read off the catalogue;</li>
 *   <li>which holds no query of its own anywhere inside it, so every name in it belongs to the
 *       one scope this examines;</li>
 *   <li>and every column reference and qualified star in which names a relation of that FROM and
 *       a column that relation has.</li>
 * </ul>
 */
final class UncorrelatedSubquery {

    private UncorrelatedSubquery() {}

    /**
     * True when nothing in {@code stmt} reads a row outside it, so one answer serves every row.
     *
     * @param executor the executor whose catalogue and search path decide what the FROM names
     */
    static boolean readsNothingOutside(Statement stmt, AstExecutor executor) {
        if (!(stmt instanceof SelectStmt)) return false;
        SelectStmt select = (SelectStmt) stmt;
        if (notEmpty(select.withClauses())) return false;
        if (holdsAnotherQuery(select)) return false;

        Map<String, Table> scope = new HashMap<String, Table>();
        List<SelectStmt.FromItem> from = select.from();
        if (from != null) {
            for (int i = 0; i < from.size(); i++) {
                SelectStmt.FromItem item = from.get(i);
                if (!(item instanceof SelectStmt.TableRef)) return false;
                SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
                if (notEmpty(ref.columnAliases())) return false;
                Table table = storedRelation(ref, executor);
                if (table == null) return false;
                String alias = ref.alias() != null ? ref.alias() : ref.table();
                if (alias == null) return false;
                // Two entries under one name is a query that will not run; leave it to the
                // executor to say so rather than pick one of them here.
                if (scope.put(alias.toLowerCase(Locale.ROOT), table) != null) return false;
            }
        }
        return AstWalk.findFirst(select, node -> reachesOutside(node, scope)) == null;
    }

    /** True when this node names something the FROM clause examined does not hold. */
    private static boolean reachesOutside(Object node, Map<String, Table> scope) {
        if (node instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) node;
            // A schema in front of the name picks between FROM entries by the schema they came
            // from, which is not something an alias map can answer.
            if (ref.catalog() != null || ref.schema() != null) return true;
            if (ref.column() == null) return true;
            if (ref.table() != null) {
                Table table = scope.get(ref.table().toLowerCase(Locale.ROOT));
                return table == null || table.getColumnIndex(ref.column()) < 0;
            }
            for (Table table : scope.values()) {
                if (table.getColumnIndex(ref.column()) >= 0) return false;
            }
            // Including the system columns and the pseudo-columns: none of them is reachable
            // without a relation to read it from, so a name no relation here has is an outer one
            // or is not a column at all, and either way this subquery is not one to reuse.
            return true;
        }
        if (node instanceof WildcardExpr) {
            WildcardExpr star = (WildcardExpr) node;
            if (star.table() == null) return false;   // the bare * of count(*), or of this scope
            if (star.catalog() != null || star.schema() != null) return true;
            return !scope.containsKey(star.table().toLowerCase(Locale.ROOT));
        }
        return false;
    }

    /**
     * True when the query holds a query of its own — a subquery, an EXISTS, an IN over a select.
     * Those bring a scope this does not model, and a name inside one may belong to it, to this
     * one, or to a row outside both.
     */
    private static boolean holdsAnotherQuery(SelectStmt select) {
        return AstWalk.findFirst(select, node -> node != select
                && (node instanceof Statement || node instanceof SubqueryExpr
                    || node instanceof ExistsExpr)) != null;
    }

    /**
     * The stored table or system catalogue a FROM entry names, or null when it names anything
     * else — a WITH item, a view, a sequence read as a relation, or nothing at all.
     */
    private static Table storedRelation(SelectStmt.TableRef ref, AstExecutor executor) {
        if (ref.table() == null) return null;
        if (ref.schema() == null && executor.selectExecutor.lookupCte(ref.table()) != null) {
            return null;
        }
        if (ref.schema() != null
                ? executor.database.getView(ref.schema(), ref.table()) != null
                : executor.database.getView(ref.table()) != null) {
            return null;
        }
        if (ref.schema() == null) {
            Table table = executor.baseTableNamed(ref.table());
            if (table != null) return table;
        } else {
            Schema schema = executor.database.getSchema(ref.schema());
            if (schema != null) {
                Table table = schema.getTable(ref.table());
                if (table != null) return table;
            }
        }
        if (SystemCatalog.isSystemCatalog(ref.schema(), ref.table())) {
            return executor.systemCatalog.resolve(ref.schema(), ref.table(), executor.session);
        }
        return null;
    }

    private static boolean notEmpty(List<?> list) {
        return list != null && !list.isEmpty();
    }
}
