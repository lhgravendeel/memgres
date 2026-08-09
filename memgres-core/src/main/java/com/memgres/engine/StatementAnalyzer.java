package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads a statement and reports what is wrong with it, without running it.
 *
 * <p>memgres used to learn a statement's shape by executing it: EXPLAIN ran the query it was asked
 * to describe, and the protocol's Describe appended {@code LIMIT 0} to the text and executed that.
 * A statement is not free to run — a sequence is consumed, a trigger fires, {@code TRUNCATE} empties
 * a table — and none of that is anything the client asked for. Reading the statement is enough to
 * find the errors a client needs first: a relation that is not there, and a column that is not in
 * the relations named.
 *
 * <p>Analysis is deliberately conservative. Where it cannot decide — a function's result columns,
 * a name that a search path might reach in a schema it cannot see — it says nothing rather than
 * inventing an error, so it never refuses a statement PostgreSQL accepts.
 */
final class StatementAnalyzer {

    private final AstExecutor executor;

    StatementAnalyzer(AstExecutor executor) {
        this.executor = executor;
    }

    /** Check the statement, raising the first error PostgreSQL would raise for it. */
    void analyze(Statement stmt) {
        analyze(stmt, new HashSet<String>());
    }

    private void analyze(Statement stmt, Set<String> visibleCtes) {
        if (stmt instanceof SelectStmt) analyzeSelect((SelectStmt) stmt, visibleCtes);
        else if (stmt instanceof SetOpStmt) {
            SetOpStmt set = (SetOpStmt) stmt;
            analyze(set.left(), visibleCtes);
            analyze(set.right(), visibleCtes);
        } else if (stmt instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) stmt;
            Set<String> ctes = withCtes(ins.withClauses, visibleCtes);
            requireRelation(ins.schema, ins.table, ctes);
            if (ins.selectStmt != null) analyze(ins.selectStmt, ctes);
        } else if (stmt instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) stmt;
            requireRelation(upd.schema(), upd.table(), visibleCtes);
        } else if (stmt instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) stmt;
            requireRelation(del.schema(), del.table(), visibleCtes);
        } else if (stmt instanceof MergeStmt) {
            MergeStmt merge = (MergeStmt) stmt;
            requireRelation(null, merge.targetTable(), visibleCtes);
        } else if (stmt instanceof DeclareCursorStmt) {
            analyze(((DeclareCursorStmt) stmt).query(), visibleCtes);
        } else if (stmt instanceof CreateTableAsStmt) {
            analyze(((CreateTableAsStmt) stmt).query(), visibleCtes);
        }
    }

    private void analyzeSelect(SelectStmt sel, Set<String> outerCtes) {
        Set<String> ctes = withCtes(sel.withClauses(), outerCtes);
        if (sel.withClauses() != null) {
            for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                // A recursive CTE may name itself, so its own name is visible inside it.
                Set<String> inner = new HashSet<String>(ctes);
                inner.add(cte.name().toLowerCase());
                analyze(cte.query(), inner);
            }
        }
        List<Table> tables = new ArrayList<Table>();
        boolean complete = true;
        if (sel.from() != null) {
            for (SelectStmt.FromItem item : sel.from()) {
                complete &= collectFrom(item, ctes, tables);
            }
        }
        // Only when every FROM item resolved to a table this analyzer understands can a missing
        // column be told apart from one a sub-select or a function supplies.
        if (complete && !tables.isEmpty()) {
            checkColumns(sel, tables);
        }
    }

    private Set<String> withCtes(List<SelectStmt.CommonTableExpr> ctes, Set<String> outer) {
        if (ctes == null || ctes.isEmpty()) return outer;
        Set<String> names = new HashSet<String>(outer);
        for (SelectStmt.CommonTableExpr cte : ctes) names.add(cte.name().toLowerCase());
        return names;
    }

    /** Resolve one FROM item; returns false when its columns cannot be enumerated. */
    private boolean collectFrom(SelectStmt.FromItem item, Set<String> ctes, List<Table> tables) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            if (ref.schema() == null && ctes.contains(ref.table().toLowerCase())) return false;
            Table table = requireRelation(ref.schema(), ref.table(), ctes);
            if (table == null || ref.columnAliases() != null) return false;
            tables.add(table);
            return true;
        }
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            boolean left = collectFrom(join.left, ctes, tables);
            boolean right = collectFrom(join.right, ctes, tables);
            return left && right;
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            analyze(((SelectStmt.SubqueryFrom) item).subquery, ctes);
            return false;
        }
        return false;
    }

    /**
     * Resolve a relation, or raise 42P01 naming it the way it was written — schema and all, since
     * that is the name the reader used.
     */
    private Table requireRelation(String schema, String name, Set<String> ctes) {
        if (name == null) return null;
        if (schema == null && ctes.contains(name.toLowerCase())) return null;
        String written = schema == null ? name : schema + "." + name;
        try {
            return executor.resolveTable(schema != null ? schema : executor.defaultSchema(), name);
        } catch (MemgresException e) {
            // The catalogues are relations too, and a query may read them by name.
            Table catalog = executor.systemCatalog.resolve(schema, name, executor.session);
            if (catalog != null) return catalog;
            throw new MemgresException("relation \"" + written + "\" does not exist", "42P01");
        }
    }

    /** The columns a relation has without declaring them. */
    private static final Set<String> SYSTEM_COLUMNS = new HashSet<String>(java.util.Arrays.asList(
            "tableoid", "ctid", "xmin", "xmax", "cmin", "cmax", "oid"));

    /** Every bare column named in the query has to be a column of one of the relations read. */
    private void checkColumns(SelectStmt sel, List<Table> tables) {
        Set<String> known = new HashSet<String>();
        for (Table t : tables) {
            for (Column c : t.getColumns()) known.add(c.getName().toLowerCase());
        }
        List<Expression> toCheck = new ArrayList<Expression>();
        if (sel.targets != null) {
            for (SelectStmt.SelectTarget t : sel.targets) {
                if (t.expr() != null) toCheck.add(t.expr());
            }
        }
        if (sel.where() != null) toCheck.add(sel.where());
        if (sel.having() != null) toCheck.add(sel.having());
        if (sel.groupBy() != null) toCheck.addAll(sel.groupBy());
        for (Expression e : toCheck) checkColumnsIn(e, known);
    }

    private void checkColumnsIn(Expression expr, Set<String> known) {
        if (expr == null) return;
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            // A qualified name may reach a relation this analyzer did not enumerate; only a bare
            // name is certainly one of the columns in hand.
            if (ref.table() != null) return;
            String name = ref.column();
            if (name == null || name.equals("*")) return;
            // Every relation carries the system columns whether or not they were declared.
            if (SYSTEM_COLUMNS.contains(name.toLowerCase())) return;
            if (!known.contains(name.toLowerCase())) {
                throw new MemgresException("column \"" + name + "\" does not exist", "42703");
            }
            return;
        }
        if (expr instanceof BinaryExpr) {
            checkColumnsIn(((BinaryExpr) expr).left(), known);
            checkColumnsIn(((BinaryExpr) expr).right(), known);
            return;
        }
        if (expr instanceof UnaryExpr) {
            checkColumnsIn(((UnaryExpr) expr).operand(), known);
            return;
        }
        if (expr instanceof FunctionCallExpr) {
            List<Expression> args = ((FunctionCallExpr) expr).args();
            if (args != null) for (Expression a : args) checkColumnsIn(a, known);
        }
    }
}
