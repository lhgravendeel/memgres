package com.memgres.pgwire;

import com.memgres.engine.Column;
import com.memgres.engine.DataType;
import com.memgres.engine.Database;
import com.memgres.engine.Schema;
import com.memgres.engine.Session;
import com.memgres.engine.Table;
import com.memgres.engine.parser.Parser;
import com.memgres.engine.parser.ast.BetweenExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.DeleteStmt;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.InsertStmt;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.ParamRef;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.parser.ast.UpdateStmt;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * What a client is told the parameters of a prepared statement are.
 *
 * <p>PostgreSQL answers Describe with the types it resolved the parameters to while analysing the
 * statement, so a driver's {@code ParameterMetaData} names the column's type rather than a
 * placeholder. A parameter with nothing to go on is text either way; the cases worth resolving are
 * the ones where the statement itself says what the value has to be — a comparison against a
 * column, a value written into a column, or an operand beside a literal.
 *
 * <p>Only shapes that can be read off the parsed statement are resolved. Where the answer is not
 * plain the parameter keeps its unresolved type, because naming a type the statement does not
 * imply would tell the driver to send something the server then has to refuse.
 */
final class PgWireParamTypes {

    private PgWireParamTypes() {}

    /**
     * The OIDs to describe {@code numParams} parameters of {@code sql} with, 0 where the statement
     * does not say. Never throws: an unparseable statement simply resolves nothing.
     */
    static int[] infer(String sql, int numParams, Database database, Session session) {
        int[] oids = new int[numParams];
        if (numParams == 0 || sql == null || database == null) return oids;
        try {
            Statement stmt = Parser.parse(sql);
            new Resolver(database, session, oids, Collections.<String, Table>emptyMap()).statement(stmt);
        } catch (RuntimeException e) {
            // Describe is metadata, not execution: a statement this class cannot read still has to
            // be describable, with the types it would have had before.
        }
        return oids;
    }

    /** Walks one query level, recording a type for every parameter whose context fixes one. */
    private static final class Resolver {
        private final Database database;
        private final Session session;
        private final int[] oids;
        /** Relation names and aliases in scope, mapped to the relation they stand for. */
        private final Map<String, Table> scope;

        Resolver(Database database, Session session, int[] oids, Map<String, Table> outerScope) {
            this.database = database;
            this.session = session;
            this.oids = oids;
            this.scope = new HashMap<>(outerScope);
        }

        void statement(Statement stmt) {
            if (stmt instanceof SelectStmt) {
                SelectStmt s = (SelectStmt) stmt;
                subqueries(s.withClauses());
                addFromItems(s.from());
                for (SelectStmt.SelectTarget t : nullSafe(s.targets())) condition(t.expr());
                condition(s.where());
                condition(s.having());
                for (Expression g : nullSafe(s.groupBy())) condition(g);
                // A row count is a bigint, whatever the query it limits looks like.
                assignOid(s.limit(), DataType.BIGINT.getOid());
                assignOid(s.offset(), DataType.BIGINT.getOid());
                condition(s.limit());
                condition(s.offset());
            } else if (stmt instanceof SetOpStmt) {
                SetOpStmt so = (SetOpStmt) stmt;
                nested(so.left());
                nested(so.right());
            } else if (stmt instanceof UpdateStmt) {
                UpdateStmt u = (UpdateStmt) stmt;
                subqueries(u.withClauses());
                Table target = findTable(u.schema(), u.table());
                addRelation(u.alias() != null ? u.alias() : u.table(), target);
                addFromItems(u.from());
                for (InsertStmt.SetClause set : nullSafe(u.setClauses())) {
                    assignOid(set.value(), columnOid(target, set.column()));
                    condition(set.value());
                }
                condition(u.where());
            } else if (stmt instanceof DeleteStmt) {
                DeleteStmt d = (DeleteStmt) stmt;
                subqueries(d.withClauses());
                addRelation(d.alias() != null ? d.alias() : d.table(), findTable(d.schema(), d.table()));
                addFromItems(d.using());
                condition(d.where());
            } else if (stmt instanceof InsertStmt) {
                InsertStmt ins = (InsertStmt) stmt;
                subqueries(ins.withClauses());
                Table target = findTable(ins.schema(), ins.table());
                addRelation(ins.table(), target);
                List<String> cols = ins.columns();
                for (List<Expression> row : nullSafe(ins.values())) {
                    for (int i = 0; i < row.size(); i++) {
                        int want = 0;
                        if (cols != null && i < cols.size()) {
                            want = columnOid(target, cols.get(i));
                        } else if (cols == null && target != null && i < target.getColumns().size()) {
                            want = oidOf(target.getColumns().get(i));
                        }
                        assignOid(row.get(i), want);
                        condition(row.get(i));
                    }
                }
                if (ins.selectStmt() != null) nested(ins.selectStmt());
            }
        }

        /** Walk a nested query with this level's relations still visible, as a correlated one is. */
        private void nested(Statement stmt) {
            if (stmt == null) return;
            new Resolver(database, session, oids, scope).statement(stmt);
        }

        private void subqueries(List<SelectStmt.CommonTableExpr> ctes) {
            for (SelectStmt.CommonTableExpr cte : nullSafe(ctes)) nested(cte.query());
        }

        // ---- FROM scope ----

        private void addFromItems(List<SelectStmt.FromItem> items) {
            for (SelectStmt.FromItem item : nullSafe(items)) addFromItem(item);
        }

        private void addFromItem(SelectStmt.FromItem item) {
            if (item instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
                addRelation(ref.alias() != null ? ref.alias() : ref.table(), findTable(ref.schema(), ref.table()));
            } else if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
                addFromItem(join.left());
                addFromItem(join.right());
                condition(join.on());
            } else if (item instanceof SelectStmt.SubqueryFrom) {
                nested(((SelectStmt.SubqueryFrom) item).subquery());
            }
        }

        private void addRelation(String name, Table table) {
            if (name == null || table == null) return;
            scope.put(name.toLowerCase(), table);
        }

        // ---- Expression walking ----

        /** Read an expression, resolving parameters against whatever they sit beside. */
        private void condition(Expression expr) {
            if (expr == null) return;
            if (expr instanceof BinaryExpr) {
                BinaryExpr b = (BinaryExpr) expr;
                assignOid(b.left(), typeOid(b.right()));
                assignOid(b.right(), typeOid(b.left()));
                condition(b.left());
                condition(b.right());
            } else if (expr instanceof UnaryExpr) {
                condition(((UnaryExpr) expr).operand());
            } else if (expr instanceof InExpr) {
                InExpr in = (InExpr) expr;
                int want = typeOid(in.expr());
                for (Expression v : nullSafe(in.values())) { assignOid(v, want); condition(v); }
                condition(in.expr());
            } else if (expr instanceof BetweenExpr) {
                BetweenExpr bt = (BetweenExpr) expr;
                int want = typeOid(bt.expr());
                assignOid(bt.low(), want);
                assignOid(bt.high(), want);
                condition(bt.expr());
                condition(bt.low());
                condition(bt.high());
            } else if (expr instanceof CastExpr) {
                condition(((CastExpr) expr).expr());
            } else if (expr instanceof SubqueryExpr) {
                nested(((SubqueryExpr) expr).subquery());
            } else if (expr instanceof ExistsExpr) {
                nested(((ExistsExpr) expr).subquery());
            }
        }

        /** Record {@code oid} for {@code expr} when it is a parameter with nothing yet. */
        private void assignOid(Expression expr, int oid) {
            if (oid == 0 || !(expr instanceof ParamRef)) return;
            int idx = ((ParamRef) expr).index() - 1;
            if (idx < 0 || idx >= oids.length) return;
            if (oids[idx] == 0) oids[idx] = oid;
        }

        /**
         * The type an expression has without knowing anything about the parameters in it. A
         * parameter itself has none, and neither has anything this class does not read.
         */
        private int typeOid(Expression expr) {
            if (expr instanceof ColumnRef) {
                ColumnRef ref = (ColumnRef) expr;
                if (ref.table() != null) {
                    return columnOid(scope.get(ref.table().toLowerCase()), ref.column());
                }
                int found = 0;
                for (Table t : scope.values()) {
                    int c = columnOid(t, ref.column());
                    if (c == 0) continue;
                    if (found != 0 && found != c) return 0;   // ambiguous, say nothing
                    found = c;
                }
                return found;
            }
            if (expr instanceof CastExpr) {
                String name = ((CastExpr) expr).typeName();
                if (name == null) return 0;
                DataType dt = DataType.fromPgName(baseTypeName(name));
                return dt == null ? 0 : unknownParamOid(dt);
            }
            if (expr instanceof Literal) {
                Literal lit = (Literal) expr;
                switch (lit.literalType()) {
                    case INTEGER: return integerLiteralType(lit.value()).getOid();
                    case FLOAT: return DataType.NUMERIC.getOid();
                    case BOOLEAN: return DataType.BOOLEAN.getOid();
                    default: return 0;   // a string literal is of unknown type until it is used
                }
            }
            return 0;
        }

        /** An integer literal is int4 while it fits, and int8 beyond that — as PostgreSQL reads it. */
        private DataType integerLiteralType(String value) {
            try {
                long v = Long.parseLong(value);
                return (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? DataType.INTEGER : DataType.BIGINT;
            } catch (RuntimeException e) {
                return DataType.NUMERIC;
            }
        }

        private int columnOid(Table table, String column) {
            if (table == null || column == null) return 0;
            for (Column c : table.getColumns()) {
                if (c.getName().equalsIgnoreCase(column)) return oidOf(c);
            }
            return 0;
        }

        private int oidOf(Column c) {
            if (c.getDomainTypeName() != null || c.getCompositeTypeName() != null) return 0;
            if (c.getArrayElementType() == null && c.getEnumTypeName() == null) {
                return unknownParamOid(c.getType());
            }
            return PgWireValueFormatter.columnTypeOid(c.getType(), c, session);
        }

        /**
         * A varchar column carries no comparison operator of its own — text's are used for it — so
         * a parameter set beside one resolves to text, not to varchar.
         */
        private int unknownParamOid(DataType type) {
            if (type == null) return 0;
            return type == DataType.VARCHAR ? DataType.TEXT.getOid() : type.getOid();
        }

        private Table findTable(String schemaName, String tableName) {
            if (tableName == null) return null;
            String lower = tableName.toLowerCase();
            if (schemaName != null) {
                Schema s = database.getSchemas().get(schemaName.toLowerCase());
                return s == null ? null : s.getTable(lower);
            }
            for (Schema s : database.getSchemas().values()) {
                Table t = s.getTable(lower);
                if (t != null) return t;
            }
            return null;
        }
    }

    /** A type name as written, without its precision or its array marker. */
    private static String baseTypeName(String name) {
        String base = name.trim();
        int paren = base.indexOf('(');
        if (paren > 0) base = base.substring(0, paren).trim();
        while (base.endsWith("[]")) base = base.substring(0, base.length() - 2).trim();
        int dot = base.lastIndexOf('.');
        if (dot >= 0) base = base.substring(dot + 1);
        return base;
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? Collections.<T>emptyList() : list;
    }
}
