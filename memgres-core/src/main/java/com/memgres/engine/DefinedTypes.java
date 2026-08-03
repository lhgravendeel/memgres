package com.memgres.engine;

import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.CompositeStarExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WildcardExpr;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The types a relation's definition settles for its columns, read from the definition alone.
 *
 * <p>A derived table, a WITH item, a view and a function in FROM are all built as a {@link Table}
 * whose columns carry the type the builder could read off the values the relation produced. That
 * is a guess, and it is often wrong: a boolean came back as an integer and {@code starts_with}
 * came back as text, so nothing downstream could trust it and the checks that read a column's type
 * had to stay silent over every one of these relations. This works the type out the other way —
 * from the text that defines the relation, never from a value — so a type it does answer is one
 * PostgreSQL would have settled the same way at parse time.
 *
 * <p>Every answer is one-sided in the same way the rest of the analysis is. A definition this
 * cannot read through — a star over a NATURAL join, a record-returning call with no column
 * definition list, a relation reached in a way this does not follow — settles nothing, and
 * nothing is what it says. Being silent leaves the column as untyped as it was before; being
 * wrong would refuse SQL PostgreSQL runs.
 *
 * <p>Types are named the way {@link DataType#toRegtypeDisplay} names them, which is the vocabulary
 * {@link BooleanContext} already compares against and puts in its messages.
 */
final class DefinedTypes {

    /**
     * How many definitions deep this reads before giving up. A derived table inside a derived
     * table is read through; a query nested past this is left unsettled rather than walked, which
     * bounds the work a deeply nested statement costs.
     */
    private static final int MAX_DEPTH = 8;

    private final AstExecutor executor;
    private int depth;
    /** The WITH items in scope, innermost last; a view body's WITH is not the caller's. */
    private final Deque<Map<String, SelectStmt.CommonTableExpr>> withScopes =
            new ArrayDeque<Map<String, SelectStmt.CommonTableExpr>>();
    /** The relation definitions being read, so a recursive one is not followed into itself. */
    private final Deque<Object> reading = new ArrayDeque<Object>();

    DefinedTypes(AstExecutor executor) {
        this.executor = executor;
    }

    // ---- What a relation built for a FROM item is entitled to be trusted about ----

    /**
     * The types the definition behind a query settles for its output columns, one per column of
     * the relation that was built from it, with null where it settles nothing.
     *
     * @param columnCount how many columns the built relation has; a definition that describes a
     *                    different number of them describes something else and settles nothing
     */
    String[] ofQuery(Statement query, int columnCount) {
        String[] answer = new String[columnCount];
        List<String> types = outputTypes(query);
        if (types != null && types.size() == columnCount) {
            for (int i = 0; i < columnCount; i++) answer[i] = types.get(i);
        }
        return answer;
    }

    /** The same for a WITH item: a recursive one's columns are its non-recursive term's. */
    String[] ofCte(SelectStmt.CommonTableExpr cte, int columnCount) {
        if (cte == null || reading.contains(cte)) return new String[columnCount];
        reading.push(cte);
        try {
            return ofQuery(cte.recursive() && cte.query() instanceof SetOpStmt
                    ? ((SetOpStmt) cte.query()).left() : cte.query(), columnCount);
        } finally {
            reading.pop();
        }
    }

    /** The same for a view, whose columns are the ones its query writes. */
    String[] ofView(Database.ViewDef view, int columnCount) {
        if (view == null || view.materialized() || reading.contains(view)) {
            return new String[columnCount];
        }
        reading.push(view);
        try {
            return ofQuery(view.query(), columnCount);
        } finally {
            reading.pop();
        }
    }

    /**
     * The types a function in FROM settles for the columns of the relation it produces.
     *
     * <p>A column definition list says what the call's record holds, and is the only thing that
     * does. Otherwise the call has to produce one column, whose type is the one the function
     * returns; WITH ORDINALITY adds a bigint after it.
     */
    String[] ofFunction(SelectStmt.FunctionFrom funcFrom, int columnCount) {
        String[] answer = new String[columnCount];
        if (funcFrom.functionName().startsWith("__")) return answer;
        List<String> declared = columnDefinitionTypes(funcFrom.columnAliases());
        if (declared != null) {
            for (int i = 0; i < declared.size() && i < columnCount; i++) answer[i] = declared.get(i);
            if (funcFrom.withOrdinality() && declared.size() < columnCount) {
                answer[declared.size()] = "bigint";
            }
            return answer;
        }
        int ordinality = funcFrom.withOrdinality() ? 1 : 0;
        if (columnCount - ordinality == 1) {
            answer[0] = singleColumnType(funcFrom);
        }
        if (ordinality == 1 && columnCount >= 1) answer[columnCount - 1] = "bigint";
        return answer;
    }

    /** The types a column definition list writes down, or null when the list is not one. */
    private static List<String> columnDefinitionTypes(List<String> aliases) {
        if (!FromFunctionResolver.hasColumnDefinitionList(aliases)) return null;
        List<String> types = new ArrayList<String>();
        for (String alias : aliases) {
            int space = alias == null ? -1 : alias.indexOf(' ');
            if (space <= 0) return null;
            types.add(named(alias.substring(space + 1).trim()));
        }
        return types;
    }

    /**
     * The one column a call in FROM produces. {@code generate_series} and {@code unnest} are
     * worked out from their arguments; every other name is answered only where every signature
     * recorded for it at that length returns one and the same concrete type.
     */
    private String singleColumnType(SelectStmt.FunctionFrom funcFrom) {
        DataType known;
        try {
            known = executor.fromResolver.functionResolver.singleColumnType(funcFrom);
        } catch (RuntimeException e) {
            return null;
        }
        if (known != null) return known.toRegtypeDisplay();
        String bare = QueryLevelScope.bareName(funcFrom.functionName());
        if (executor.database.getFunction(bare) != null) return null;
        int arity = funcFrom.args() == null ? 0 : funcFrom.args().size();
        String found = null;
        boolean recorded = false;
        for (String[] sig : BuiltinFunctionSignatures.SIGNATURES) {
            if (!sig[0].equalsIgnoreCase(bare)) continue;
            recorded = true;
            int params = sig[2].isEmpty() ? 0 : sig[2].split(" ").length;
            if (params != arity) continue;
            DataType returns = DataType.fromOid(parseOid(sig[1]));
            // A polymorphic or record result is decided by something this is not reading.
            if (returns == null) return null;
            String display = returns.toRegtypeDisplay();
            if (found != null && !found.equals(display)) return null;
            found = display;
        }
        return recorded ? found : null;
    }

    private static int parseOid(String oid) {
        try {
            return Integer.parseInt(oid);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // ---- Reading a query's output ----

    /** The type of each output column a query writes, or null when its shape is unsettled. */
    private List<String> outputTypes(Statement query) {
        Shape shape = shapeOf(query);
        return shape == null ? null : shape.types;
    }

    /** The names and types of a query's output columns, aligned. */
    private static final class Shape {
        final List<String> names = new ArrayList<String>();
        final List<String> types = new ArrayList<String>();

        void add(String name, String type) {
            names.add(name);
            types.add(type);
        }
    }

    private Shape shapeOf(Statement query) {
        if (query == null || depth >= MAX_DEPTH) return null;
        depth++;
        try {
            return shapeOfInner(query);
        } catch (RuntimeException e) {
            // A definition this cannot read is a definition that settles nothing.
            return null;
        } finally {
            depth--;
        }
    }

    private Shape shapeOfInner(Statement query) {
        if (query instanceof SetOpStmt) {
            SetOpStmt op = (SetOpStmt) query;
            Shape left = shapeOf(op.left());
            if (left == null) return null;
            Shape right = shapeOf(op.right());
            if (right == null || right.types.size() != left.types.size()) return null;
            Shape merged = new Shape();
            for (int i = 0; i < left.types.size(); i++) {
                // The arms are brought to a common type, so only where they already agree is the
                // result the type this can name.
                String type = left.types.get(i) != null && left.types.get(i).equals(right.types.get(i))
                        ? left.types.get(i) : null;
                merged.add(left.names.get(i), type);
            }
            return merged;
        }
        if (!(query instanceof SelectStmt)) return null;
        SelectStmt sel = (SelectStmt) query;
        if (sel.targets() == null || sel.targets().isEmpty()) return null;
        pushWith(sel.withClauses());
        try {
            List<RowContext.TableBinding> scope = describe(sel.from());
            if (scope == null) return null;
            BooleanContext.Types types = BooleanContext.Types.of(executor, scope);
            Shape shape = new Shape();
            for (SelectStmt.SelectTarget target : sel.targets()) {
                Expression expr = target.expr();
                if (expr instanceof WildcardExpr) {
                    if (!expandStar((WildcardExpr) expr, scope, shape)) return null;
                    continue;
                }
                // (x).* stands for however many fields the composite holds, which this does not
                // read, so the query's very width is unsettled.
                if (expr instanceof CompositeStarExpr) return null;
                String name = target.alias() != null ? target.alias() : nameOf(expr);
                shape.add(name, BooleanContext.typeOf(expr, types));
            }
            return shape;
        } finally {
            popWith(sel.withClauses());
        }
    }

    /** The columns a star stands for, or false when this cannot say which they are. */
    private boolean expandStar(WildcardExpr star, List<RowContext.TableBinding> scope, Shape shape) {
        if (scope.isEmpty()) return false;
        boolean any = false;
        for (RowContext.TableBinding b : scope) {
            String exposed = b.alias() != null ? b.alias() : b.table().getName();
            if (star.table() != null
                    && (exposed == null || !star.table().equalsIgnoreCase(exposed))) {
                continue;
            }
            any = true;
            List<Column> columns = b.table().getColumns();
            for (int i = 0; i < columns.size(); i++) {
                shape.add(columns.get(i).getName(), typeIn(b.table(), i));
            }
        }
        return any;
    }

    /** The name a select-list expression answers to when nothing renamed it. */
    private String nameOf(Expression expr) {
        if (expr instanceof ColumnRef) return ((ColumnRef) expr).column();
        try {
            return executor.exprToAlias(expr);
        } catch (RuntimeException e) {
            return null;
        }
    }

    // ---- Describing what a FROM clause supplies, without reading a row of it ----

    /**
     * The relations a FROM clause supplies, described from the text. Null when one of them is
     * something this does not follow, which leaves the whole clause — and so the query's output —
     * unsettled.
     */
    private List<RowContext.TableBinding> describe(List<SelectStmt.FromItem> from) {
        List<RowContext.TableBinding> out = new ArrayList<RowContext.TableBinding>();
        if (from == null || from.isEmpty()) return out;
        for (SelectStmt.FromItem item : from) {
            if (!describeItem(item, out)) return null;
        }
        return out;
    }

    private boolean describeItem(SelectStmt.FromItem item, List<RowContext.TableBinding> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            // USING and NATURAL merge two columns into one, which changes both how many columns a
            // star stands for and which relation each of them came from.
            if (join.using() != null && !join.using().isEmpty()) return false;
            if (FromJoinExecutor.isNatural(join.joinType())) return false;
            return describeItem(join.left(), out) && describeItem(join.right(), out);
        }
        if (item instanceof SelectStmt.TableRef) {
            return describeTableRef((SelectStmt.TableRef) item, out);
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            if (sub.alias() == null) return false;
            Shape shape = shapeOf(sub.subquery());
            if (shape == null) return false;
            Table virtual = virtualRelation(sub.alias(), shape, sub.columnAliases());
            if (virtual == null) return false;
            out.add(binding(virtual, sub.alias()));
            return true;
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom func = (SelectStmt.FunctionFrom) item;
            if (func.functionName().startsWith("__")) return false;
            if (func.withOrdinality()) return false;
            List<String> declaredTypes = columnDefinitionTypes(func.columnAliases());
            String alias = func.alias() != null ? func.alias() : func.functionName();
            Shape shape = new Shape();
            if (declaredTypes != null) {
                for (int i = 0; i < declaredTypes.size(); i++) {
                    String written = func.columnAliases().get(i);
                    shape.add(FromFunctionResolver.stripColType(written), declaredTypes.get(i));
                }
            } else {
                String type = singleColumnType(func);
                if (type == null) return false;
                List<String> aliases = func.columnAliases();
                if (aliases != null && aliases.size() != 1) return false;
                shape.add(aliases != null && !aliases.isEmpty()
                        ? FromFunctionResolver.stripColType(aliases.get(0))
                        : QueryLevelScope.bareName(func.functionName()), type);
            }
            Table virtual = virtualRelation(alias, shape, null);
            if (virtual == null) return false;
            virtual.setFunctionResult(true);
            out.add(binding(virtual, alias));
            return true;
        }
        return false;
    }

    private boolean describeTableRef(SelectStmt.TableRef ref, List<RowContext.TableBinding> out) {
        if (ref.table() == null) return false;
        String alias = ref.alias() != null ? ref.alias() : ref.table();
        SelectStmt.CommonTableExpr cte = ref.schema() == null ? lookupCte(ref.table()) : null;
        if (cte != null) {
            if (reading.contains(cte)) return false;
            reading.push(cte);
            Shape shape;
            try {
                // A recursive item's columns are the non-recursive term's; the recursive term is
                // required to match them rather than allowed to widen them.
                shape = shapeOf(cte.recursive() && cte.query() instanceof SetOpStmt
                        ? ((SetOpStmt) cte.query()).left() : cte.query());
            } finally {
                reading.pop();
            }
            if (shape == null) return false;
            Table virtual = virtualRelation(alias, shape,
                    cte.columnNames() != null && !cte.columnNames().isEmpty()
                            ? cte.columnNames() : ref.columnAliases());
            if (virtual == null) return false;
            out.add(binding(virtual, alias));
            return true;
        }
        Database.ViewDef view = ref.schema() != null
                ? executor.database.getView(ref.schema(), ref.table())
                : executor.database.getView(ref.table());
        if (view != null) {
            if (reading.contains(view)) return false;
            if (view.materialized()) return false;
            reading.push(view);
            Shape shape;
            try {
                shape = shapeOf(view.query());
            } finally {
                reading.pop();
            }
            if (shape == null) return false;
            Table virtual = virtualRelation(alias, shape, ref.columnAliases());
            if (virtual == null) return false;
            out.add(binding(virtual, alias));
            return true;
        }
        String schema = ref.schema() != null ? ref.schema() : executor.defaultSchema();
        Table table;
        try {
            table = executor.resolveTable(schema, ref.table(), ref.schema() != null);
        } catch (RuntimeException e) {
            return false;
        }
        if (table == null) return false;
        // An alias list renames the columns; the types behind them are the relation's own.
        if (ref.columnAliases() != null && !ref.columnAliases().isEmpty()) {
            Shape shape = new Shape();
            List<Column> columns = table.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                shape.add(columns.get(i).getName(), typeIn(table, i));
            }
            Table virtual = virtualRelation(alias, shape, ref.columnAliases());
            if (virtual == null) return false;
            out.add(binding(virtual, alias));
            return true;
        }
        out.add(binding(table, alias));
        return true;
    }

    /**
     * A relation described rather than read: its columns' names and the types its definition
     * settles, which {@link BooleanContext} reads instead of the placeholder each column carries.
     */
    private Table virtualRelation(String alias, Shape shape, List<String> renames) {
        if (renames != null && renames.size() > shape.names.size()) return null;
        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < shape.names.size(); i++) {
            String name = renames != null && i < renames.size() ? renames.get(i) : shape.names.get(i);
            if (name == null) return null;
            DataType placeholder = shape.types.get(i) == null ? DataType.TEXT
                    : DataType.fromPgName(shape.types.get(i));
            columns.add(new Column(name, placeholder == null ? DataType.TEXT : placeholder,
                    true, false, null));
        }
        Table virtual = new Table(alias, columns);
        virtual.setDefinedColumnTypes(shape.types.toArray(new String[0]));
        return virtual;
    }

    private static RowContext.TableBinding binding(Table table, String alias) {
        return new RowContext.TableBinding(table, alias, new Object[table.getColumns().size()]);
    }

    /**
     * The type a relation's column certainly has, as PostgreSQL spells it. Mirrors what
     * {@link BooleanContext.Types} will accept: a type that answers to a name of its own — an
     * enum, a domain, a composite — is not one this names.
     */
    static String typeIn(Table table, int index) {
        if (table == null || index < 0 || index >= table.getColumns().size()) return null;
        if (table.hasDefinedColumnTypes()) return table.definedColumnType(index);
        Column column = table.getColumns().get(index);
        if (column.getEnumTypeName() != null || column.getDomainTypeName() != null
                || column.getCompositeTypeName() != null) {
            return null;
        }
        if (column.getType() == null) return null;
        if (column.getArrayElementType() != null) {
            return column.getArrayElementType().toRegtypeDisplay() + "[]";
        }
        return column.getType().toRegtypeDisplay();
    }

    /** The type a written name stands for, spelled the way the rest of this names types. */
    private static String named(String written) {
        if (written == null) return null;
        String name = written.trim().toLowerCase(Locale.ROOT);
        int paren = name.indexOf('(');
        if (paren > 0) name = name.substring(0, paren).trim();
        boolean array = name.endsWith("[]");
        if (array) name = name.substring(0, name.length() - 2).trim();
        DataType type = DataType.fromPgName(name);
        if (type == null) return null;
        return array ? type.toRegtypeDisplay() + "[]" : type.toRegtypeDisplay();
    }

    // ---- WITH items in scope ----

    private void pushWith(List<SelectStmt.CommonTableExpr> items) {
        if (items == null || items.isEmpty()) return;
        Map<String, SelectStmt.CommonTableExpr> scope =
                new HashMap<String, SelectStmt.CommonTableExpr>();
        for (SelectStmt.CommonTableExpr item : items) {
            if (item.name() != null) scope.put(item.name().toLowerCase(Locale.ROOT), item);
        }
        withScopes.push(scope);
    }

    private void popWith(List<SelectStmt.CommonTableExpr> items) {
        if (items == null || items.isEmpty()) return;
        withScopes.pop();
    }

    /** The WITH item a name reaches: one this walk pushed, or one the running statement holds. */
    private SelectStmt.CommonTableExpr lookupCte(String name) {
        String key = name.toLowerCase(Locale.ROOT);
        for (Map<String, SelectStmt.CommonTableExpr> scope : withScopes) {
            SelectStmt.CommonTableExpr found = scope.get(key);
            if (found != null) return found;
        }
        try {
            return executor.selectExecutor.lookupCte(name);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
