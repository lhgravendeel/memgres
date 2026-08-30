package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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
    /** Where the text this tree was parsed from begins in the statement the client sent. */
    private final int textOffset;

    StatementAnalyzer(AstExecutor executor) {
        this(executor, 0);
    }

    StatementAnalyzer(AstExecutor executor, int textOffset) {
        this.executor = executor;
        this.textOffset = textOffset;
    }

    /**
     * The reading Parse does, before the client has asked for anything to be run.
     *
     * <p>Not everything a statement is answered for is settled here. PostgreSQL looks a prepared
     * statement's name up when the EXECUTE naming it runs rather than when its text is read, so a
     * client that Parses an EXECUTE of a statement the session has not got hears about it at
     * Execute — while EXPLAIN, which reads what it was given as part of running, hears about it
     * where it stands.
     */
    void analyzeAtParse(Statement stmt) {
        atParse = true;
        analyze(stmt);
    }

    /** Whether this reading is the one Parse does. */
    private boolean atParse;

    /** Check the statement, raising the first error PostgreSQL would raise for it. */
    void analyze(Statement stmt) {
        analyze(stmt, noCtes());
        // A select with no FROM at the top of a statement has no relation to take a column from,
        // and nothing around it to borrow one from either, so every bare name in it has to be
        // something other than a column. Only the outermost query can be read this way: a query
        // with no FROM nested inside another one may be reading a column of the query it stands in.
        if (stmt instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) stmt;
            if (sel.from() == null || sel.from().isEmpty()) {
                checkColumns(sel, new ArrayList<RowContext.TableBinding>(),
                        withCtes(sel.withClauses(), noCtes()));
            }
        }
    }

    private void analyze(Statement stmt, Map<String, SelectStmt.CommonTableExpr> visibleCtes) {
        if (stmt instanceof SelectStmt) analyzeSelect((SelectStmt) stmt, visibleCtes);
        else if (stmt instanceof SetOpStmt) {
            SetOpStmt set = (SetOpStmt) stmt;
            // A WITH written in front of a set operation is parsed onto its left branch, and its
            // items are in scope on both sides: the right branch reads a CTE it did not declare.
            Map<String, SelectStmt.CommonTableExpr> shared = set.left() instanceof SelectStmt
                    ? withCtes(((SelectStmt) set.left()).withClauses(), visibleCtes)
                    : visibleCtes;
            analyze(set.left(), visibleCtes);
            analyze(set.right(), shared);
        } else if (stmt instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) stmt;
            Map<String, SelectStmt.CommonTableExpr> ctes = withCtes(ins.withClauses, visibleCtes);
            requireColumns(requireRelation(ins.schema, ins.table, ctes), ins.table, ins.columns);
            if (ins.selectStmt != null) analyze(ins.selectStmt, ctes);
        } else if (stmt instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) stmt;
            Table target = requireRelation(upd.schema(), upd.table(), visibleCtes);
            if (upd.setClauses != null) {
                List<String> assigned = new ArrayList<String>();
                for (InsertStmt.SetClause set : upd.setClauses) {
                    // An assignment that writes through a field is left alone: SET t.i = 1 and
                    // SET pos.x = 1 are spelled the same, and telling a qualified target from a
                    // composite one is the executor's to do — it is what carries the hint.
                    if (set.subField() == null) assigned.add(set.column());
                }
                // PostgreSQL settles what every assignment writes before it resolves any of
                // the columns written to, so a keyword misplaced in a value is what is reported
                // even when the statement also names a column the relation does not have.
                MisplacedDefault.reject(upd, textOffset);
                requireColumns(target, upd.table(), assigned);
            }
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
        } else if (stmt instanceof ExecuteStmt) {
            if (!atParse) analyzeExecute((ExecuteStmt) stmt);
        } else if (stmt instanceof ExplainStmt) {
            // EXPLAIN is answered for out of the statement it was given, and PostgreSQL reads that
            // statement while it reads the EXPLAIN: a column that is not there is reported before
            // there is any question of a plan.
            Statement explained = ((ExplainStmt) stmt).statement();
            if (explained != null) analyze(explained, visibleCtes);
        } else if (stmt instanceof SetStmt) {
            analyzeShow((SetStmt) stmt);
        }
    }

    /**
     * SHOW names a setting the server has, and PostgreSQL settles which one while it reads the
     * statement: a name no setting answers to is refused there rather than when the value would
     * have been fetched. Which names those are is the reader's own rule, asked here so that the
     * two cannot disagree — a statement is refused at Parse only where running it would refuse it.
     */
    private void analyzeShow(SetStmt stmt) {
        if (!"show".equals(stmt.name())) return;
        String param = stmt.value();
        if (param == null || param.isEmpty()) return;
        if ("ALL".equalsIgnoreCase(param)) return;
        GucSettings settings = executor.session == null
                ? null : executor.session.getGucSettings();
        if (settings == null || settings.isKnown(param)) return;
        throw new MemgresException(
                "unrecognized configuration parameter \"" + param + "\"", "42704");
    }

    /**
     * A prepared statement has to be one the session holds, and it is executed with the number of
     * arguments it was prepared for. Both are settled before the statement runs, so EXPLAIN of an
     * EXECUTE reports them rather than describing a plan for a statement that is not there.
     */
    private void analyzeExecute(ExecuteStmt exec) {
        if (executor.session == null) return;
        Session.PreparedStmt prepared = executor.session.getPreparedStatement(exec.name());
        if (prepared == null) {
            throw new MemgresException(
                    "prepared statement \"" + exec.name() + "\" does not exist", "26000");
        }
        int declared = prepared.paramTypes() == null ? 0 : prepared.paramTypes().size();
        int expected = declared > 0 ? Math.max(declared, prepared.inferredParamCount())
                : prepared.inferredParamCount();
        int given = exec.params() == null ? 0 : exec.params().size();
        if (given != expected) {
            // The two counts are the detail behind the complaint rather than part of it, which is
            // what keeps the message the same for every prepared statement.
            throw new MemgresException(
                    "wrong number of parameters for prepared statement \"" + exec.name() + "\""
                            + "\n  Detail: Expected " + expected + " parameters but got "
                            + given + ".", "42601");
        }
    }

    private void analyzeSelect(SelectStmt sel, Map<String, SelectStmt.CommonTableExpr> outerCtes) {
        Map<String, SelectStmt.CommonTableExpr> ctes = withCtes(sel.withClauses(), outerCtes);
        if (sel.withClauses() != null) {
            for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                // A recursive CTE may name itself, so its own name is visible inside it.
                Map<String, SelectStmt.CommonTableExpr> inner =
                        new HashMap<String, SelectStmt.CommonTableExpr>(ctes);
                inner.put(cte.name().toLowerCase(java.util.Locale.ROOT), cte);
                analyze(cte.query(), inner);
                checkSearchAndCycleNames(cte, inner);
            }
        }
        // A WITH item is read before anything that reads from it, so a keyword misplaced in
        // one is met before the query around it — but after the item itself has been read,
        // which is why this stands behind the loop above rather than in front of it.
        MisplacedDefault.reject(sel.withClauses(), textOffset);
        List<RowContext.TableBinding> relations = new ArrayList<RowContext.TableBinding>();
        boolean complete = true;
        if (sel.from() != null) {
            for (SelectStmt.FromItem item : sel.from()) {
                complete &= collectFrom(item, ctes, relations);
            }
        }
        // The range table is built before anything the query selects is read, so a keyword
        // misplaced in a FROM item — in a join condition, or in a sub-select the query reads
        // from — is met before the select list, and after the relations the clause names.
        MisplacedDefault.reject(sel.from(), textOffset);
        // Only when every FROM item resolved to a relation this analyzer understands can a missing
        // column be told apart from one a function supplies.
        if (complete && !relations.isEmpty()) {
            checkColumns(sel, relations, ctes);
        }
    }

    /** No WITH item is in scope. */
    private static Map<String, SelectStmt.CommonTableExpr> noCtes() {
        return new HashMap<String, SelectStmt.CommonTableExpr>();
    }

    /**
     * The WITH items in scope, by the names they are read under. The item itself travels with its
     * name because what a query reads from it is settled by the query it was written as, and that
     * is in hand wherever the name is.
     */
    private Map<String, SelectStmt.CommonTableExpr> withCtes(
            List<SelectStmt.CommonTableExpr> ctes,
            Map<String, SelectStmt.CommonTableExpr> outer) {
        if (ctes == null || ctes.isEmpty()) return outer;
        Map<String, SelectStmt.CommonTableExpr> named =
                new HashMap<String, SelectStmt.CommonTableExpr>(outer);
        for (SelectStmt.CommonTableExpr cte : ctes) named.put(cte.name().toLowerCase(java.util.Locale.ROOT), cte);
        return named;
    }

    /**
     * The relations each query the one being read stands inside reads, outermost first.
     *
     * <p>A subquery is not read on its own: a correlated one names the columns of the query around
     * it, and one written LATERAL in a FROM clause names the entries written before it. Both are
     * references the query in hand cannot answer for, so a name this analyzer cannot find among
     * the relations in front of it may still be one of theirs.
     */
    private final List<List<RowContext.TableBinding>> scopes =
            new ArrayList<List<RowContext.TableBinding>>();

    /**
     * The relations in a FROM clause that stand for a query's own output — a WITH item, a
     * sub-select, a view — rather than for something with rows of its own on disk.
     *
     * <p>PostgreSQL gives a relation the system columns only where there is a tuple to answer them
     * from, so {@code ctid} and {@code xmin} are columns of a table and of a materialized view and
     * of nothing else: over a view or a sub-select they are names nothing answers to.
     */
    private final Map<Table, Boolean> queryRelations = new IdentityHashMap<Table, Boolean>();

    /**
     * The schema each relation in scope lives in: its name for something the catalogue holds, the
     * empty string for a WITH item or a sub-select, which live in no schema at all, and nothing
     * recorded where this reader could not tell.
     */
    private final Map<Table, String> relationSchemas = new IdentityHashMap<Table, String>();

    /** The WITH items whose own columns are being worked out, so a self-reference stops. */
    private final Map<SelectStmt.CommonTableExpr, Boolean> cteInProgress =
            new IdentityHashMap<SelectStmt.CommonTableExpr, Boolean>();

    /** The relations that stand for the rows a call in FROM produced. */
    private final Map<Table, Boolean> functionRelations = new IdentityHashMap<Table, Boolean>();

    /**
     * The names of the relations a sub-select in the FROM clause reads, which the query around it
     * cannot reach: the sub-select answers under its own name and holds theirs inside.
     */
    private final Set<String> hidden = new HashSet<String>();

    /** Every relation a name in the query being read may come from. */
    private List<RowContext.TableBinding> inScope() {
        List<RowContext.TableBinding> all = new ArrayList<RowContext.TableBinding>();
        for (List<RowContext.TableBinding> level : scopes) all.addAll(level);
        return all;
    }

    /**
     * The same relations, the query in hand first. PostgreSQL offers one suggestion per relation
     * and works outwards from where the name was written, so a near miss in the query being read
     * is named before one in the query around it.
     */
    private List<RowContext.TableBinding> suggestionOrder() {
        List<RowContext.TableBinding> all = new ArrayList<RowContext.TableBinding>();
        for (int i = scopes.size() - 1; i >= 0; i--) all.addAll(scopes.get(i));
        return all;
    }

    /** Resolve one FROM item; returns false when its columns cannot be enumerated. */
    private boolean collectFrom(SelectStmt.FromItem item,
                                Map<String, SelectStmt.CommonTableExpr> ctes,
                                List<RowContext.TableBinding> relations) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            SelectStmt.CommonTableExpr cte = ref.schema() == null
                    ? ctes.get(ref.table().toLowerCase(java.util.Locale.ROOT)) : null;
            if (cte != null) {
                requireReturning(cte);
                List<String> columns =
                        renamedOrRefuse(cteColumns(cte, ctes), ref.columnAliases(), item);
                if (columns == null) return opaque();
                relations.add(queryRelation(ref.table(), ref.alias(), columns, ""));
                return true;
            }
            Database.ViewDef view = viewDefinition(ref.schema(), ref.table());
            if (view != null) {
                List<String> columns =
                        renamedOrRefuse(viewColumns(view), ref.columnAliases(), item);
                if (columns == null) return opaque();
                // A materialized view is written out like a table, so it answers for the system
                // columns as a table does and is not one of the relations that stand for a query.
                if (view.materialized()) {
                    relations.add(storedRelation(ref.table(), ref.alias(), columns,
                            view.schemaName()));
                } else {
                    relations.add(queryRelation(ref.table(), ref.alias(), columns,
                            view.schemaName()));
                }
                return true;
            }
            Table table = requireRelation(ref.schema(), ref.table(), ctes);
            if (table == null) return opaque();
            String schema = executor.database.schemaNameOf(table);
            if (ref.columnAliases() != null && !ref.columnAliases().isEmpty()) {
                // An alias list renames the relation's first columns and leaves the rest as they
                // were.
                List<String> columns = renamedOrRefuse(namesOf(table), ref.columnAliases(), item);
                if (columns == null) return opaque();
                relations.add(storedRelation(ref.table(), ref.alias(), columns, schema));
                return true;
            }
            // The alias is kept with the relation because a suggestion for a misspelled column
            // names it the way the query does.
            if (schema != null) relationSchemas.put(table, schema);
            relations.add(new RowContext.TableBinding(table, ref.alias(), null));
            return true;
        }
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            boolean left = collectFrom(join.left, ctes, relations);
            boolean right = collectFrom(join.right, ctes, relations);
            // A named USING clause is one more relation the query answers to, and its columns are
            // the ones the clause merged and nothing else -- so j.x is a column j has not got
            // rather than a relation the query has not got.
            if (join.usingAlias() != null && join.using() != null) {
                relations.add(queryRelation(join.usingAlias(), null, join.using(), ""));
            }
            return left && right;
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            // Written LATERAL, the subquery reads the entries in front of it, so it is read inside
            // their scope: a reference to one of them is not a relation the subquery has not got.
            scopes.add(new ArrayList<RowContext.TableBinding>(relations));
            try {
                analyze(sub.subquery, ctes);
            } finally {
                scopes.remove(scopes.size() - 1);
            }
            if (sub.alias() == null) return opaque();
            List<String> columns = renamedOrRefuse(outputOf(sub.subquery, ctes),
                    sub.columnAliases(), item);
            if (columns == null) return opaque();
            noteHidden(sub.subquery);
            relations.add(queryRelation(sub.alias(), null, columns, ""));
            return true;
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom call = (SelectStmt.FunctionFrom) item;
            List<String> columns =
                    renamedOrRefuse(functionColumns(call), call.columnAliases(), item);
            if (columns == null) return opaque();
            RowContext.TableBinding binding = queryRelation(itemName(call), null, columns, "");
            functionRelations.put(binding.table(), Boolean.TRUE);
            relations.add(binding);
            return true;
        }
        return opaque();
    }

    /**
     * Whether every relation the scope covers is one whose columns are in hand. A FROM item this
     * analyzer cannot enumerate still stands for relations a qualifier may have meant, so once one
     * has been met a qualifier no relation answers to is no longer a fault worth naming.
     */
    private boolean scopeComplete = true;

    /** Records a FROM item whose columns are not in hand, and says so to the caller. */
    private boolean opaque() {
        scopeComplete = false;
        return false;
    }

    /** A relation standing for the columns a query answers with, under the name it is read by. */
    private RowContext.TableBinding queryRelation(String name, String alias, List<String> columns,
                                                  String schema) {
        RowContext.TableBinding binding = storedRelation(name, alias, columns, schema);
        queryRelations.put(binding.table(), Boolean.TRUE);
        return binding;
    }

    /** The same, for a relation whose rows are its own and which therefore has system columns. */
    private RowContext.TableBinding storedRelation(String name, String alias, List<String> columns,
                                                   String schema) {
        List<Column> declared = new ArrayList<Column>();
        for (String column : columns) {
            declared.add(new Column(column, DataType.TEXT, true, false, null));
        }
        Table table = new Table(name, declared);
        relationSchemas.put(table, schema == null ? "" : schema);
        return new RowContext.TableBinding(table, alias, null);
    }

    /** The names a relation's columns carry, in order. */
    private static List<String> namesOf(Table table) {
        List<String> names = new ArrayList<String>();
        for (Column column : table.getColumns()) names.add(column.getName());
        return names;
    }

    /**
     * The same columns under the names a FROM clause's alias list gives the first of them.
     * PostgreSQL renames as far as the list reaches and leaves the rest of the relation's columns
     * answering to their own names, so a short list is not a narrower relation.
     */
    private static List<String> renamed(List<String> columns, List<String> aliases) {
        if (columns == null) return null;
        if (aliases == null || aliases.isEmpty()) return columns;
        if (aliases.size() > columns.size()) return null;
        List<String> named = new ArrayList<String>(columns);
        for (int i = 0; i < aliases.size(); i++) named.set(i, aliases.get(i));
        return named;
    }

    /**
     * The same renaming, for the clause being read rather than for a shape being worked out: a
     * list naming more columns than the relation has is refused where it stands.
     *
     * <p>PostgreSQL settles an alias list while it builds the range table, which is before it
     * resolves a single column, so a query whose FROM clause over-names a relation and whose
     * select list also names a column that is not there hears about the list. The relation is
     * named the way the query reads it, and the count it is held against includes the ordinality
     * column a call was given, since that is one of the columns the clause may rename.
     */
    private List<String> renamedOrRefuse(List<String> columns, List<String> aliases,
                                         SelectStmt.FromItem item) {
        if (columns != null && aliases != null && aliases.size() > columns.size()) {
            // PostgreSQL points at nothing for this one: the fault is the shape of the clause
            // rather than any one word in it.
            throw new MemgresException(FromResolver.aliasedItemNoun(item) + " \"" + itemName(item)
                    + "\" has " + columns.size()
                    + " columns available but " + aliases.size() + " columns specified", "42P10")
                    .suppressPosition();
        }
        return renamed(columns, aliases);
    }

    /**
     * The columns a FROM item stands for, or null where reading the statement does not settle
     * them. Nothing is raised on the way: a name this cannot resolve is a shape it has not got,
     * which is a different thing from a fault worth reporting.
     */
    private List<String> columnsOf(SelectStmt.FromItem item,
                                   Map<String, SelectStmt.CommonTableExpr> ctes) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            SelectStmt.CommonTableExpr cte = ref.schema() == null
                    ? ctes.get(ref.table().toLowerCase(java.util.Locale.ROOT)) : null;
            if (cte != null) return renamed(cteColumns(cte, ctes), ref.columnAliases());
            Database.ViewDef view = viewDefinition(ref.schema(), ref.table());
            if (view != null) return renamed(viewColumns(view), ref.columnAliases());
            Table table = resolveQuietly(ref.schema(), ref.table());
            return table == null ? null : renamed(namesOf(table), ref.columnAliases());
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            return renamed(outputOf(sub.subquery, ctes), sub.columnAliases());
        }
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            List<String> left = columnsOf(join.left(), ctes);
            List<String> right = columnsOf(join.right(), ctes);
            if (left == null || right == null) return null;
            return joined(join, left, right);
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom call = (SelectStmt.FunctionFrom) item;
            return renamed(functionColumns(call), call.columnAliases());
        }
        return null;
    }

    /**
     * The columns a call in FROM answers with, or null where reading the statement does not settle
     * them.
     *
     * <p>A call whose rows are one value is one column wide, and PostgreSQL names that column
     * after the alias the FROM clause gave the call — or after the call itself where the clause
     * gave it none. A function that declares a name for what it returns keeps that name however
     * the clause aliases it, which is why {@code json_array_elements(...) e} answers to
     * {@code e.value} and not to {@code e.e}, and why the pairs {@code json_each} hands back are
     * always {@code key} and {@code value}. {@code unnest} written over several arrays answers
     * with one column per array, and every one of them is named after the call however the clause
     * aliases it. Nothing else is read here: what a record-returning call holds is the function's
     * to say, and a call this cannot enumerate stands for columns it has not got.
     */
    private List<String> functionColumns(SelectStmt.FunctionFrom call) {
        String written = call.functionName();
        if (written == null) return null;
        // ROWS FROM, JSON_TABLE, XMLTABLE and TABLESAMPLE are carried as calls whose name is no
        // function's: each holds its own items, and their columns are the items'.
        if (written.startsWith("__")) return null;
        String bare = QueryLevelScope.bareName(written);
        boolean pairs = KEY_AND_VALUE.contains(bare);
        if (!pairs && !FromFunctionResolver.SINGLE_VALUE_SRFS.contains(bare)) return null;
        // A function the database holds under the name is the one the call reaches, and what it
        // returns is that function's declaration rather than any of this.
        if (executor.database.getFunction(bare) != null) return null;
        if (FromFunctionResolver.hasColumnDefinitionList(call.columnAliases())) return null;
        List<String> columns = new ArrayList<String>();
        if (pairs) {
            columns.add("key");
            columns.add("value");
        } else if (bare.equals("unnest")) {
            // An array of a composite type is unnested into one column per field, not into one
            // column, so unnest is enumerated only over an array whose elements are plainly not
            // composite: written out as literals, or cast to an array of a type this engine has a
            // value class for. Anything else — a column, a call, a parameter — is left alone.
            if (call.args() == null || call.args().isEmpty()) return null;
            for (Expression arg : call.args()) {
                if (!plainlyScalarArray(arg)) return null;
            }
            if (call.args().size() > 1) {
                // Written over several arrays the call answers with one column per array, and
                // every one of them carries the call's own name whatever the clause aliases it.
                for (int i = 0; i < call.args().size(); i++) columns.add(bare);
            } else {
                columns.add(call.alias() != null ? call.alias() : bare);
            }
        } else {
            columns.add(RESULT_NAMED_VALUE.contains(bare) ? "value"
                    : call.alias() != null ? call.alias() : bare);
        }
        // WITH ORDINALITY numbers the rows in a column of its own, written after the call's.
        if (call.withOrdinality()) columns.add("ordinality");
        return columns;
    }

    /**
     * Whether the text alone settles that unnesting this makes one column and not several.
     *
     * <p>PostgreSQL unnests an array of a composite type into one column per field, so the width
     * of the relation depends on what the argument turns out to hold — type resolution this reader
     * does not do. An array written out of literals holds none, and neither does one cast to an
     * array of a type this engine has a value class for; anything else is a call whose columns are
     * not in hand.
     */
    private boolean plainlyScalarArray(Expression arg) {
        if (arg instanceof ArrayExpr) {
            ArrayExpr array = (ArrayExpr) arg;
            if (array.isRow() || array.elements() == null || array.elements().isEmpty()) {
                return false;
            }
            for (Expression element : array.elements()) {
                if (!(element instanceof Literal)) return false;
            }
            return true;
        }
        if (arg instanceof CastExpr) {
            String written = ((CastExpr) arg).typeName();
            if (written == null || !written.endsWith("[]")) return false;
            String element = written.substring(0, written.length() - 2).trim();
            if (DataType.fromPgName(element) == null) return false;
            return !executor.database.getCompositeTypes().containsKey(element.toLowerCase(java.util.Locale.ROOT));
        }
        return false;
    }

    /**
     * The calls that declare a name for the value they return, which their column keeps whatever
     * the FROM clause calls the call itself.
     */
    private static final Set<String> RESULT_NAMED_VALUE = new HashSet<String>(
            java.util.Arrays.asList("json_array_elements", "jsonb_array_elements",
                    "json_array_elements_text", "jsonb_array_elements_text"));

    /** The calls that hand back a pair per row, under the names their own signature gives them. */
    private static final Set<String> KEY_AND_VALUE = new HashSet<String>(
            java.util.Arrays.asList("json_each", "json_each_text", "jsonb_each",
                    "jsonb_each_text", "each"));

    /**
     * What a join answers with. USING and a natural join make one column of the two they match,
     * and PostgreSQL writes those merged columns first, followed by whatever is left of the two
     * sides in the order they were written.
     */
    private static List<String> joined(SelectStmt.JoinFrom join, List<String> left,
                                       List<String> right) {
        List<String> merged = new ArrayList<String>();
        if (join.using() != null && !join.using().isEmpty()) {
            for (String using : join.using()) {
                if (contains(left, using) && contains(right, using)) merged.add(using);
            }
        } else if (join.joinType() != null && join.joinType().name().startsWith("NATURAL")) {
            for (String name : left) {
                if (contains(right, name) && !contains(merged, name)) merged.add(name);
            }
        }
        List<String> all = new ArrayList<String>(merged);
        for (String name : left) if (!contains(merged, name)) all.add(name);
        for (String name : right) if (!contains(merged, name)) all.add(name);
        return all;
    }

    private static boolean contains(List<String> names, String name) {
        for (String held : names) {
            if (held != null && held.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /**
     * The columns a WITH item answers with: the ones its own name list gives it, or the ones the
     * query it was written as produces.
     */
    private List<String> cteColumns(SelectStmt.CommonTableExpr cte,
                                    Map<String, SelectStmt.CommonTableExpr> ctes) {
        List<String> own = queryColumnsOf(cte, ctes);
        if (cte.columnNames() != null && !cte.columnNames().isEmpty()) {
            // A name list stands for the item's columns one for one. One of another length is a
            // fault of its own — PostgreSQL says how many columns the query has and how many were
            // named — so what the item then answers with is the reader's to say, not this one's.
            int written = own != null ? own.size() : SelectStmt.writtenWidth(cte.query());
            if (written != cte.columnNames().size()) return null;
            own = cte.columnNames();
        }
        if (own == null) return null;
        // SEARCH and CYCLE each write a column of their own after the item's, whether the item
        // named its columns or left them to its query: the sequence the recursion was walked in,
        // whether the row closed a cycle, and the path taken to it.
        if (cte.searchColumn() == null && cte.cycleColumn() == null
                && cte.cyclePathColumn() == null) {
            return own;
        }
        List<String> all = new ArrayList<String>(own);
        if (cte.searchColumn() != null) all.add(cte.searchColumn());
        if (cte.cycleColumn() != null) all.add(cte.cycleColumn());
        if (cte.cyclePathColumn() != null) all.add(cte.cyclePathColumn());
        return all;
    }

    /**
     * A WITH item that writes answers with rows only if it was asked to.
     *
     * <p>A statement that writes has no result of its own; RETURNING is what gives it one. So a
     * query reading from a WITH item that writes without RETURNING is asking for rows that were
     * never produced, and PostgreSQL refuses it rather than handing back a relation of no columns
     * — and refuses it while analysing, so the write does not happen either. Answering with an
     * empty relation instead let the write take effect under a query PG would not have run.
     */
    static void requireReturning(SelectStmt.CommonTableExpr cte) {
        Statement query = cte.query();
        List<SelectStmt.SelectTarget> returning;
        if (query instanceof InsertStmt) returning = ((InsertStmt) query).returning();
        else if (query instanceof UpdateStmt) returning = ((UpdateStmt) query).returning();
        else if (query instanceof DeleteStmt) returning = ((DeleteStmt) query).returning();
        else if (query instanceof MergeStmt) returning = ((MergeStmt) query).returning();
        else return;
        if (returning == null || returning.isEmpty()) {
            throw new MemgresException("WITH query \"" + cte.name()
                    + "\" does not have a RETURNING clause", "0A000");
        }
    }

    /**
     * What SEARCH and CYCLE named, against the columns the WITH item itself has.
     *
     * <p>PostgreSQL settles this while it analyses the statement, so an item carrying a SEARCH or
     * CYCLE clause is held to it whether or not the query goes on to read the item. Running the
     * checks only from the item's own execution let an unread item say nothing at all — the
     * statement answered, and the clause that could not have worked went unmentioned.
     */
    private void checkSearchAndCycleNames(SelectStmt.CommonTableExpr cte,
                                          Map<String, SelectStmt.CommonTableExpr> ctes) {
        if (cte.searchColumn() == null && cte.cycleColumn() == null) return;
        List<String> columns = queryColumnsOf(cte, ctes);
        if (cte.columnNames() != null && !cte.columnNames().isEmpty()) {
            int written = columns != null ? columns.size() : SelectStmt.writtenWidth(cte.query());
            if (written != cte.columnNames().size()) return;
            columns = cte.columnNames();
        }
        if (columns == null) return;
        rejectMissingColumn(columns, cte.searchByColumns(), "search");
        RecursiveCteCheck.rejectAddedColumn(cte, columns, cte.searchColumn(),
                "search sequence column name");
        rejectMissingColumn(columns, cte.cycleByColumns(), "cycle");
        RecursiveCteCheck.rejectAddedColumn(cte, columns, cte.cycleColumn(),
                "cycle mark column name");
        RecursiveCteCheck.rejectAddedColumn(cte, columns, cte.cyclePathColumn(),
                "cycle path column name");
    }

    /** A SEARCH BY or CYCLE list may only name columns the WITH item has. */
    private static void rejectMissingColumn(List<String> columns, List<String> named,
                                            String clause) {
        if (named == null) return;
        for (String by : named) {
            if (!holdsColumn(columns, by)) {
                throw new MemgresException(
                        clause + " column \"" + by + "\" not in WITH query column list", "42601");
            }
        }
    }

    private static boolean holdsColumn(List<String> columns, String name) {
        for (String column : columns) {
            if (column != null && column.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /** The columns the query a WITH item was written as answers with. */
    private List<String> queryColumnsOf(SelectStmt.CommonTableExpr cte,
                                        Map<String, SelectStmt.CommonTableExpr> ctes) {
        // A recursive item reads itself, so working its columns out from its own query would ask
        // the same question again; the answer for the item being worked out is that it has none.
        if (cteInProgress.containsKey(cte)) return null;
        cteInProgress.put(cte, Boolean.TRUE);
        try {
            return outputOf(cte.query(), ctes);
        } finally {
            cteInProgress.remove(cte);
        }
    }

    /**
     * The names a query answers under, or null where reading the text does not settle them.
     *
     * <p>PostgreSQL names an output column after the column it read, after the alias the writer
     * gave it, or after the call that produced it, and calls whatever it can name none of those
     * ways {@code ?column?}. All of it is settled by the text: the name a call's column carries is
     * the call's own, whatever its arguments turn out to be. The names are taken from the same
     * labeller the executor names its own output with, so a query enumerated here answers to
     * exactly the columns it would answer to when it runs.
     */
    private List<String> outputOf(Statement query, Map<String, SelectStmt.CommonTableExpr> ctes) {
        if (query instanceof SetOpStmt) return outputOf(((SetOpStmt) query).left(), ctes);
        if (query instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) query;
            return returningNames(ins.returning, ins.schema, ins.table);
        }
        if (query instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) query;
            return returningNames(upd.returning, upd.schema(), upd.table());
        }
        if (query instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) query;
            return returningNames(del.returning, del.schema(), del.table());
        }
        if (!(query instanceof SelectStmt)) return null;
        SelectStmt sel = (SelectStmt) query;
        List<SelectStmt.SelectTarget> targets = sel.targets();
        if (targets == null || targets.isEmpty()) return null;
        // A VALUES list is a query over nothing at all, and PostgreSQL names what it answers with
        // by position rather than after anything written in it.
        if (sel.fromValues()) {
            List<String> byPosition = new ArrayList<String>();
            for (int i = 0; i < targets.size(); i++) byPosition.add("column" + (i + 1));
            return byPosition;
        }
        Map<String, SelectStmt.CommonTableExpr> inner = withCtes(sel.withClauses(), ctes);
        List<String> names = new ArrayList<String>();
        for (SelectStmt.SelectTarget target : targets) {
            if (target.alias() != null) {
                names.add(target.alias());
                continue;
            }
            Expression e = target.expr();
            if (e instanceof ColumnRef) {
                String column = ((ColumnRef) e).column();
                if (column == null || column.equals("*")) return null;
                names.add(column);
                continue;
            }
            if (e instanceof WildcardExpr) {
                List<String> expanded = starColumns((WildcardExpr) e, sel.from(), inner);
                if (expanded == null) return null;
                names.addAll(expanded);
                continue;
            }
            String label = outputName(e);
            if (label == null) return null;
            names.add(label);
        }
        return names;
    }

    /**
     * The name one target answers under, or null where reading it does not settle that.
     *
     * <p>A field written {@code (x).*} stands for as many columns as the composite has fields, and
     * how many that is is the type resolution this reader does not do, so a query holding one is
     * not enumerated at all. A sub-query used as a value is labelled after whatever it answers
     * with, and working that out means resolving the relations it reads — running the very thing
     * this reader exists to avoid — so a target holding one is left unnamed as well.
     */
    private String outputName(Expression e) {
        if (e instanceof CompositeStarExpr || holdsSubquery(e)) return null;
        try {
            return executor.exprEvaluator.exprToAlias(e);
        } catch (RuntimeException | StackOverflowError unreadable) {
            return null;
        }
    }

    /** Whether naming this would have to name a sub-query, following the labeller's own path. */
    private static boolean holdsSubquery(Expression e) {
        if (e instanceof SubqueryExpr) return true;
        if (e instanceof CastExpr) return holdsSubquery(((CastExpr) e).expr());
        if (e instanceof CollateExpr) return holdsSubquery(((CollateExpr) e).expr());
        if (e instanceof ArraySliceExpr) return holdsSubquery(((ArraySliceExpr) e).array());
        if (e instanceof SubscriptExpr) return holdsSubquery(((SubscriptExpr) e).base());
        return false;
    }

    /** The names a RETURNING list answers under, or null where the text does not settle them. */
    private List<String> returningNames(List<SelectStmt.SelectTarget> returning, String schema,
                                        String table) {
        if (returning == null || returning.isEmpty()) return null;
        List<String> names = new ArrayList<String>();
        for (SelectStmt.SelectTarget item : returning) {
            if (item.alias() != null) {
                names.add(item.alias());
                continue;
            }
            Expression e = item.expr();
            if (e instanceof ColumnRef) {
                String column = ((ColumnRef) e).column();
                if (column == null || column.equals("*")) return null;
                names.add(column);
                continue;
            }
            if (e instanceof WildcardExpr) {
                if (((WildcardExpr) e).table() != null) return null;
                Table target = resolveQuietly(schema, table);
                if (target == null) return null;
                names.addAll(namesOf(target));
                continue;
            }
            return null;
        }
        return names;
    }

    /** The columns a star stands for, or null where the relations it reads are not all in hand. */
    private List<String> starColumns(WildcardExpr star, List<SelectStmt.FromItem> from,
                                     Map<String, SelectStmt.CommonTableExpr> ctes) {
        if (from == null || from.isEmpty()) return null;
        if (star.catalog() != null || star.schema() != null) return null;
        if (star.table() != null) {
            List<NamedRelation> named = new ArrayList<NamedRelation>();
            for (SelectStmt.FromItem item : from) {
                if (!collectNamed(item, ctes, named)) return null;
            }
            List<String> found = null;
            for (NamedRelation relation : named) {
                if (!relation.name.equalsIgnoreCase(star.table())) continue;
                if (found != null) return null;
                found = relation.columns;
            }
            return found;
        }
        List<String> all = new ArrayList<String>();
        for (SelectStmt.FromItem item : from) {
            List<String> columns = columnsOf(item, ctes);
            if (columns == null) return null;
            all.addAll(columns);
        }
        return all;
    }

    /** A FROM item under the name the query reads it by. */
    private static final class NamedRelation {
        final String name;
        final List<String> columns;

        NamedRelation(String name, List<String> columns) {
            this.name = name;
            this.columns = columns;
        }
    }

    /**
     * The FROM items a qualified star may be naming. A join is not one of them: what stands under
     * a name is always one of the relations the join was built from.
     */
    private boolean collectNamed(SelectStmt.FromItem item,
                                 Map<String, SelectStmt.CommonTableExpr> ctes,
                                 List<NamedRelation> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            return collectNamed(join.left(), ctes, out) && collectNamed(join.right(), ctes, out);
        }
        String name = itemName(item);
        if (name == null) return false;
        List<String> columns = columnsOf(item, ctes);
        if (columns == null) return false;
        out.add(new NamedRelation(name, columns));
        return true;
    }

    /** The name a FROM item is read under: the alias where one was written, else its own. */
    private static String itemName(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            return ref.alias() != null ? ref.alias() : ref.table();
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            return ((SelectStmt.SubqueryFrom) item).alias();
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom call = (SelectStmt.FunctionFrom) item;
            // A call answers to the function's own name, and to nothing of the schema it was
            // written with: a qualifier names where the function was looked up rather than what
            // the FROM clause is now holding.
            return call.alias() != null ? call.alias()
                    : QueryLevelScope.bareName(call.functionName());
        }
        return null;
    }

    /** Records every relation a sub-select reads under the name the sub-select reads it by. */
    private void noteHidden(Statement query) {
        if (query instanceof SetOpStmt) {
            noteHidden(((SetOpStmt) query).left());
            noteHidden(((SetOpStmt) query).right());
            return;
        }
        if (!(query instanceof SelectStmt)) return;
        SelectStmt sel = (SelectStmt) query;
        if (sel.from() == null) return;
        for (SelectStmt.FromItem item : sel.from()) noteHidden(item);
    }

    private void noteHidden(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.JoinFrom) {
            noteHidden(((SelectStmt.JoinFrom) item).left());
            noteHidden(((SelectStmt.JoinFrom) item).right());
            return;
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            noteHidden(((SelectStmt.SubqueryFrom) item).subquery());
        }
        String name = itemName(item);
        if (name != null) hidden.add(name.toLowerCase(java.util.Locale.ROOT));
        if (item instanceof SelectStmt.TableRef) {
            hidden.add(((SelectStmt.TableRef) item).table().toLowerCase(java.util.Locale.ROOT));
        }
    }

    /**
     * Whether anything answers to the name as a call: a type, whose name is the cast to it, or a
     * function the database holds. Both are things {@code t.name} may mean where the relation has
     * no column of that name.
     */
    private boolean namesSomethingCallable(String name) {
        if (name == null) return false;
        if (DataType.fromPgName(name) != null) return true;
        if (executor.database.getFunction(name.toLowerCase(java.util.Locale.ROOT)) != null) return true;
        if (executor.database.getCustomEnum(name) != null) return true;
        if (executor.database.getDomain(name) != null) return true;
        return executor.database.getCompositeTypes().containsKey(name.toLowerCase(java.util.Locale.ROOT));
    }

    /** The view a name reaches, or null when it reaches none. */
    private Database.ViewDef viewDefinition(String schema, String name) {
        return schema != null
                ? executor.database.getView(schema, name)
                : executor.database.getView(name);
    }

    /** The columns a view answers with, as the catalogue records them. */
    private static List<String> viewColumns(Database.ViewDef view) {
        List<Column> cached = view.cachedColumns();
        if (cached == null || cached.isEmpty()) return null;
        List<String> names = new ArrayList<String>();
        for (Column column : cached) names.add(column.getName());
        return names;
    }

    /** The relation a name reaches, or null when reading does not reach one. */
    private Table resolveQuietly(String schema, String name) {
        if (name == null) return null;
        try {
            return executor.resolveTable(schema != null ? schema : executor.defaultSchema(), name,
                    schema != null);
        } catch (MemgresException notThere) {
            try {
                return executor.systemCatalog.resolve(schema, name, executor.session);
            } catch (RuntimeException unreadable) {
                return null;
            }
        }
    }

    /**
     * Resolve a relation, or raise 42P01 naming it the way it was written — schema and all, since
     * that is the name the reader used.
     */
    private Table requireRelation(String schema, String name,
                                  Map<String, SelectStmt.CommonTableExpr> ctes) {
        if (name == null) return null;
        if (schema == null && ctes.containsKey(name.toLowerCase(java.util.Locale.ROOT))) return null;
        // A view is a relation every query may read, and resolution here is the write path's:
        // it hands back the base table a write would be rewritten onto, or refuses the view
        // outright when no write can go through it. Neither answers what a reader asked — the
        // base table's columns are not the view's, and a materialized view or a view over a join
        // was reported missing although it is sitting right there. So a name that reaches a view
        // is left alone: it exists, and this analyzer cannot enumerate what it holds.
        if (viewNamed(schema, name)) return null;
        String written = schema == null ? name : schema + "." + name;
        try {
            // A written qualifier names one schema's relation and no other's, which is also what
            // decides whether a sequence in that schema is reached at all.
            return executor.resolveTable(schema != null ? schema : executor.defaultSchema(), name,
                    schema != null);
        } catch (MemgresException e) {
            // The catalogues are relations too, and a query may read them by name.
            Table catalog = executor.systemCatalog.resolve(schema, name, executor.session);
            if (catalog != null) return catalog;
            // A name that reaches a relation of a kind no query can read — an index, a composite
            // type — is refused for what it is: PostgreSQL opens the relation and then complains,
            // and reporting that as a missing relation sent the reader looking for one that is
            // sitting right there.
            if (!"42P01".equals(e.getSqlState()) && !"3F000".equals(e.getSqlState())) throw e;
            // Nor is a relation this analyzer cannot open a relation that is not there. A foreign
            // table is reached by name and answered for by its wrapper, so what a read of one is
            // told is the wrapper's to say.
            if (relationNamed(schema, name)) return null;
            throw new MemgresException("relation \"" + written + "\" does not exist", "42P01");
        }
    }

    /**
     * Every column a write names has to be one the relation has. PostgreSQL settles that while it
     * reads the statement, and names the relation alongside the column, because a column list is
     * only wrong relative to the relation it was written for.
     */
    private void requireColumns(Table table, String relation, List<String> columns) {
        if (table == null || columns == null) return;
        Set<String> known = new HashSet<String>();
        for (Column c : table.getColumns()) known.add(c.getName().toLowerCase(java.util.Locale.ROOT));
        for (String column : columns) {
            if (column == null || known.contains(column.toLowerCase(java.util.Locale.ROOT))) continue;
            // A system column is there whether or not it was declared, and writing to one is
            // refused for what it is rather than reported as a column that does not exist.
            if (SYSTEM_COLUMNS.contains(column.toLowerCase(java.util.Locale.ROOT))) continue;
            throw new MemgresException("column \"" + column + "\" of relation \"" + relation
                    + "\" does not exist", "42703");
        }
    }

    /** Whether the name reaches a relation of any kind, whatever a read of it would be told. */
    private boolean relationNamed(String schema, String name) {
        if (schema != null) {
            return RelationNamespace.kindOf(executor.database, schema, name) != null;
        }
        for (String onPath : executor.relationSearchPath()) {
            if (RelationNamespace.kindOf(executor.database, onPath, name) != null) return true;
        }
        return false;
    }

    /** Whether a view of this name is defined, whatever a write against it would be told. */
    private boolean viewNamed(String schema, String name) {
        return viewDefinition(schema, name) != null;
    }

    /**
     * The columns a relation has without declaring them. {@code oid} is not one of them: it stopped
     * being a system column, and a relation that answers to the name now declares it like any other
     * column — which is why {@code pg_class.oid} reads and a table's does not.
     */
    private static final Set<String> SYSTEM_COLUMNS = new HashSet<String>(java.util.Arrays.asList(
            "tableoid", "ctid", "xmin", "xmax", "cmin", "cmax"));

    /**
     * How many columns the statement answers with, or -1 when reading it does not settle that.
     *
     * <p>PostgreSQL knows this before a row is read, because it plans a statement while it reads
     * it, and the protocol holds a client's list of result formats against the count at Bind — one
     * message before anything runs. Where the text does not settle the width this says -1 and the
     * message is taken as it stands, which is what a statement answering with no rows at all gets
     * too: there is no row description for the formats to apply to.
     */
    int resultColumns(Statement stmt) {
        if (stmt instanceof SetOpStmt) return resultColumns(((SetOpStmt) stmt).left());
        // A plan is written out down one column, whatever it is a plan for and however it is
        // formatted, so EXPLAIN settles its width without anything being read.
        if (stmt instanceof ExplainStmt) return 1;
        // SHOW writes the setting's value down one column, and SHOW ALL the name, the setting and
        // the sentence describing it — three columns, whatever the server has been set to. A name
        // no setting answers to is not a width at all: that statement is refused rather than
        // answered, and refusing it for the shape of a format list would be a different complaint
        // from the one it has coming.
        if (stmt instanceof SetStmt) {
            SetStmt utility = (SetStmt) stmt;
            if (!"show".equals(utility.name())) return -1;
            if ("ALL".equalsIgnoreCase(utility.value())) return 3;
            GucSettings settings = executor.session == null
                    ? null : executor.session.getGucSettings();
            return settings != null && settings.isKnown(utility.value()) ? 1 : -1;
        }
        if (stmt instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) stmt;
            return returnedColumns(ins.returning, ins.schema, ins.table);
        }
        if (stmt instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) stmt;
            return returnedColumns(upd.returning, upd.schema(), upd.table());
        }
        if (stmt instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) stmt;
            return returnedColumns(del.returning, del.schema(), del.table());
        }
        if (!(stmt instanceof SelectStmt)) return -1;
        SelectStmt sel = (SelectStmt) stmt;
        if (sel.targets() == null || sel.targets().isEmpty()) return -1;
        int written = SelectStmt.writtenWidth(sel);
        if (written >= 0) return written;
        // A star stands for the columns of the relations read, so it settles a width only where
        // every one of them is a relation this analyzer can enumerate.
        Map<String, SelectStmt.CommonTableExpr> ctes = withCtes(sel.withClauses(), noCtes());
        int total = 0;
        for (SelectStmt.SelectTarget target : sel.targets()) {
            Expression e = target.expr();
            if (!(e instanceof WildcardExpr)) {
                if (e instanceof CompositeStarExpr) return -1;
                total++;
                continue;
            }
            List<String> columns = starColumns((WildcardExpr) e, sel.from(), ctes);
            if (columns == null) return -1;
            total += columns.size();
        }
        return total;
    }

    /**
     * The width of a RETURNING list. A write with none of it answers with no rows at all, which
     * is not a width of zero but nothing to measure a format list against, so it reads as -1 too.
     */
    private int returnedColumns(List<SelectStmt.SelectTarget> returning, String schema,
                                String table) {
        List<String> names = returningNames(returning, schema, table);
        return names == null ? -1 : names.size();
    }

    /** Every bare column named in the query has to be a column of one of the relations read. */
    private void checkColumns(SelectStmt sel, List<RowContext.TableBinding> relations,
                              Map<String, SelectStmt.CommonTableExpr> ctes) {
        scopes.add(relations);
        Set<String> known = new HashSet<String>();
        boolean anyStored = false;
        for (RowContext.TableBinding b : inScope()) {
            for (Column c : b.table().getColumns()) known.add(c.getName().toLowerCase(java.util.Locale.ROOT));
            // A relation's own name stands for a row of it, so a bare name matching one of the
            // relations read is a whole-row reference rather than a column that is not there.
            known.add(b.table().getName().toLowerCase(java.util.Locale.ROOT));
            if (b.alias() != null) known.add(b.alias().toLowerCase(java.util.Locale.ROOT));
            if (!queryRelations.containsKey(b.table())) anyStored = true;
        }
        // The system columns belong to the tuple a relation stores, so they are names to reckon
        // with only while something in scope has tuples of its own: over a view, a WITH item or a
        // sub-select alone, PostgreSQL answers that there is no column called ctid.
        if (anyStored) known.addAll(SYSTEM_COLUMNS);
        // GROUP BY reads the select list's output names, so an alias defined there is a name the
        // query may group by even though no relation has a column of that name.
        if (sel.targets != null) {
            for (SelectStmt.SelectTarget t : sel.targets) {
                if (t.alias() != null) known.add(t.alias().toLowerCase(java.util.Locale.ROOT));
            }
        }
        // In the order PostgreSQL settles them, because that is the order it reports them in: the
        // select list, then WHERE, then HAVING, then the sort clause, and the grouping items last
        // of all — which is why an unresolvable grouping item is not the answer to a query whose
        // sort clause is unusable too. The walk stops at the first thing this analyzer cannot
        // judge, so it never names a fault PostgreSQL would reach only after one it cannot see.
        List<Expression> toCheck = new ArrayList<Expression>();
        if (sel.targets != null) {
            for (SelectStmt.SelectTarget t : sel.targets) {
                if (t.expr() != null) toCheck.add(t.expr());
            }
        }
        if (sel.where() != null) toCheck.add(sel.where());
        if (sel.having() != null) toCheck.add(sel.having());
        boolean sortable = true;
        if (sel.orderBy() != null) {
            for (SelectStmt.OrderByItem item : sel.orderBy()) {
                // A sort position is settled against the select list a wildcard has been expanded
                // into, which this analyzer does not do, so it says nothing past one.
                if (item.expr() instanceof Literal) { sortable = false; break; }
                if (item.expr() != null) toCheck.add(item.expr());
                rejectSortOperatorWithNoEntry(item);
            }
        }
        if (sortable && sel.groupBy() != null) toCheck.addAll(sel.groupBy());
        for (Expression e : toCheck) {
            if (!checkColumnsIn(e, known, ctes)) break;
        }
        scopes.remove(scopes.size() - 1);
    }

    /**
     * An ordering operator has to be an operator at all before it has to order, and the two are
     * different complaints: {@code ORDER BY a USING @@} over an integer is refused because nothing
     * defines {@code @@} for two integers, while over text it is refused because {@code @@} orders
     * nothing. The parser cannot tell them apart, having no idea what is being sorted, so the
     * question is asked here, where the relations the sort reads from are known. Left unasked, the
     * operator was consumed and the rows came back in ascending order.
     *
     * <p>Only the operators spelled as symbols reach this, and none of them is a {@code <} or
     * {@code >} member of a btree operator family for any type, so an operator that does exist has
     * still not got the one property a sort asks of it.
     */
    private void rejectSortOperatorWithNoEntry(SelectStmt.OrderByItem item) {
        BinaryExpr.BinOp op = item.usingOperator();
        if (op == null || item.expr() == null) return;
        executor.binaryOpEvaluator.rejectUnresolvableOperator(
                new BinaryExpr(item.expr(), op, item.expr()), new RowContext(inScope()));
        MemgresException e = new MemgresException("operator "
                + BinaryOpEvaluator.spellingOf(op) + " is not a valid ordering operator", "42809");
        e.setHint("Ordering operators must be \"<\" or \">\" members of btree operator families.");
        throw e;
    }

    /**
     * Check one expression, and say whether the walk may go on.
     *
     * <p>It may not once it has met something this analyzer cannot resolve — a call, a cast, a
     * subquery. PostgreSQL resolves those where they stand and raises its own error there, so a
     * column fault written after one of them is not the fault it would report: {@code SELECT
     * nosuchfunc(1), nosuchcol FROM t} is a function that does not exist, not a column.
     */
    private boolean checkColumnsIn(Expression expr, Set<String> known,
                                   Map<String, SelectStmt.CommonTableExpr> ctes) {
        if (expr == null) return true;
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            if (ref.table() != null) {
                // A name carrying a database in front of it is a cross-database reference, and
                // what a query may do with one is the reader's to say rather than this one's.
                if (ref.catalog() != null) return true;
                return checkQualified(ref);
            }
            String name = ref.column();
            if (name == null || name.equals("*")) return true;
            if (!known.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                MemgresException e =
                        new MemgresException("column \"" + name + "\" does not exist", "42703");
                // A name close to one of the columns in scope is worth suggesting, which is the
                // same suggestion the executor makes when it reaches the reference itself.
                String hint = RowContext.suggestClosestColumn(name, suggestionOrder());
                if (hint != null) e.setHint(hint);
                throw e;
            }
            return true;
        }
        // A literal, a wildcard and a parameter carry nothing to resolve, so the walk goes past
        // them; anything else may be raising an error of its own where it stands.
        if (expr instanceof Literal) {
            // Except DEFAULT, which is not a value: nowhere a query reads from is a place it may
            // stand, so every one that gets here is misplaced. Refusing it as the walk passes it
            // is what puts it in order against the columns, because PostgreSQL reads an
            // expression's operands left to right and reports whichever fault it meets first.
            if (MisplacedDefault.isKeyword(expr)) {
                throw MisplacedDefault.error((Literal) expr, textOffset);
            }
            return true;
        }
        if (expr instanceof WildcardExpr || expr instanceof ParamRef) {
            return true;
        }
        if (expr instanceof BinaryExpr) {
            return checkColumnsIn(((BinaryExpr) expr).left(), known, ctes)
                    && checkColumnsIn(((BinaryExpr) expr).right(), known, ctes);
        }
        if (expr instanceof UnaryExpr) {
            return checkColumnsIn(((UnaryExpr) expr).operand(), known, ctes);
        }
        // A subquery is a query of its own, read the way any other is, and the columns of the
        // query it stands in are in scope inside it. PostgreSQL settles it where it stands, so
        // nothing written after it is the fault it would report first.
        if (expr instanceof SubqueryExpr) {
            analyze(((SubqueryExpr) expr).subquery(), ctes);
            return false;
        }
        if (expr instanceof ExistsExpr) {
            analyze(((ExistsExpr) expr).subquery(), ctes);
            return false;
        }
        if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            if (!checkColumnsIn(in.expr(), known, ctes)) return false;
            if (in.values() != null) {
                for (Expression value : in.values()) {
                    if (!checkColumnsIn(value, known, ctes)) return false;
                }
            }
            return false;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) expr;
            List<Expression> args = call.args();
            // PostgreSQL reads a call's arguments before it looks the call itself up, which is why
            // a missing column inside one is reported and not the function around it.
            if (args != null) {
                // NORMALIZE(text, NFKC) writes its normalization form as a keyword, and the parser
                // hands that over as a bare name like any other. It is the one argument that is
                // not a value, so reading it as a column refused a call PostgreSQL accepts.
                boolean formIsSecondArgument = isNormalizeCall(call.name());
                for (int i = 0; i < args.size(); i++) {
                    if (formIsSecondArgument && i == 1) continue;
                    if (!checkColumnsIn(args.get(i), known, ctes)) return false;
                }
            }
            // count(*) is the one call PostgreSQL settles without consulting anything: it is
            // written with no argument to resolve against and one signature answers to the name,
            // so a fault written after it is still the fault PostgreSQL reports first. Every other
            // call is resolved against the types of its arguments, which this analyzer does not
            // work out, and stops the walk where it stands.
            return isCountStar(call);
        }
        return false;
    }

    /** Whether the call is {@code count(*)} and nothing is attached to it. */
    private static boolean isCountStar(FunctionCallExpr call) {
        return call.star() && call.filter() == null && call.orderBy() == null && !call.distinct()
                && "count".equalsIgnoreCase(call.name());
    }

    /**
     * A qualified name reaches one entry of the FROM clause and no other, so what it reads is
     * settled by that clause alone. PostgreSQL words a qualifier no entry answers to differently
     * from a column an entry has not got, and differently again when the qualifier is the
     * relation's own name and a clause has since renamed it — which is the reader's own mistake
     * and the one worth naming, because the relation is sitting right there.
     */
    private boolean checkQualified(ColumnRef ref) {
        if (!scopeComplete) return true;
        String qualifier = ref.table();
        List<RowContext.TableBinding> scope = inScope();
        RowContext.TableBinding found = null;
        for (RowContext.TableBinding b : scope) {
            boolean matches = b.alias() != null
                    ? b.alias().equalsIgnoreCase(qualifier)
                    : b.table().getName().equalsIgnoreCase(qualifier);
            if (!matches) continue;
            // A qualifier two entries answer to is ambiguous, which is a fault of its own and one
            // this analyzer leaves to the reader that raises it.
            if (found != null) return true;
            found = b;
        }
        if (found == null) {
            for (RowContext.TableBinding b : scope) {
                if (b.alias() == null || b.alias().equalsIgnoreCase(b.table().getName())) continue;
                if (!b.table().getName().equalsIgnoreCase(qualifier)) continue;
                MemgresException aliased = new MemgresException("invalid reference to FROM-clause"
                        + " entry for table \"" + qualifier + "\"", "42P01");
                aliased.setHint("Perhaps you meant to reference the table alias \""
                        + b.alias() + "\".");
                throw aliased;
            }
            // A relation the query reads inside a sub-select is in the query without being in this
            // part of it, and PostgreSQL says so rather than calling it missing. Which of the two
            // it is is settled by the whole range table, and the reader that has that in front of
            // it is the one that runs the query, so a name one of them holds is left to it.
            if (hidden.contains(qualifier.toLowerCase(java.util.Locale.ROOT))) return true;
            throw new MemgresException(
                    "missing FROM-clause entry for table \"" + qualifier + "\"", "42P01");
        }
        // A schema written in front of the qualifier picks the entry that lives in that schema and
        // no other, so an entry of the right name held somewhere else is one this part of the
        // query cannot reach — and a WITH item or a sub-select, which live in no schema at all,
        // are never reachable that way. Where the schema an entry lives in is not in hand this
        // says nothing, because a qualifier that may still be right is not a fault.
        if (ref.schema() != null) {
            String schema = relationSchemas.get(found.table());
            if (schema == null) return true;
            if (!schema.equalsIgnoreCase(ref.schema())) {
                MemgresException elsewhere = new MemgresException("invalid reference to FROM-clause"
                        + " entry for table \"" + qualifier + "\"", "42P01");
                elsewhere.setDetail("There is an entry for table \"" + qualifier + "\", but it"
                        + " cannot be referenced from this part of the query.");
                throw elsewhere;
            }
        }
        String name = ref.column();
        if (name == null || name.equals("*")) return true;
        // The system columns belong to the tuple a relation stores, so a relation standing for a
        // query's own output has none of them.
        if (!queryRelations.containsKey(found.table())
                && SYSTEM_COLUMNS.contains(name.toLowerCase(java.util.Locale.ROOT))) {
            return true;
        }
        if (found.table().getColumnIndex(name) >= 0) return true;
        // A name a relation has not got may not be a column at all: written after a relation that
        // answers with one value per row, a type's name or a function's is a call on that value —
        // PostgreSQL reads t.name as name(t) wherever the column is not there.
        if (functionRelations.containsKey(found.table())
                && found.table().getColumns().size() == 1
                && namesSomethingCallable(name)) {
            return true;
        }
        MemgresException e = new MemgresException(
                "column " + qualifier + "." + name + " does not exist", "42703");
        String hint = RowContext.suggestClosestColumn(name,
                java.util.Collections.singletonList(found));
        if (hint != null) e.setHint(hint);
        throw e;
    }

    /** The calls whose second argument is a Unicode normalization form rather than a value. */
    private static boolean isNormalizeCall(String name) {
        return name != null
                && (name.equalsIgnoreCase("normalize") || name.equalsIgnoreCase("is_normalized"));
    }
}
