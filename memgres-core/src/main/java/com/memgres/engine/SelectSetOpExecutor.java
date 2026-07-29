package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles UNION, INTERSECT, EXCEPT set operations.
 * Extracted from SelectExecutor to separate concerns.
 */
class SelectSetOpExecutor {
    private final SelectExecutor select;
    private final AstExecutor executor;

    SelectSetOpExecutor(SelectExecutor select) {
        this.select = select;
        this.executor = select.executor;
    }

    QueryResult executeSetOp(SetOpStmt stmt) {
        rejectLock(stmt);
        // If the left side has CTEs, they should be available to both sides
        if (stmt.left() instanceof SelectStmt && ((SelectStmt) stmt.left()).withClauses() != null && !((SelectStmt) stmt.left()).withClauses().isEmpty()) {
            SelectStmt sel = (SelectStmt) stmt.left();
            Map<String, SelectStmt.CommonTableExpr> cteMap = new LinkedHashMap<>();
            for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                cteMap.put(cte.name().toLowerCase(), cte);
            }
            executor.cteStack.push(cteMap);
            // Execute left without its own CTE push (it would double-push)
            SelectStmt stripped = new SelectStmt(sel.distinct(), sel.targets(), sel.from(), sel.where(),
                    sel.groupBy(), sel.having(), sel.orderBy(), sel.limit(), sel.offset(), null);
            QueryResult leftResult = executor.executeStatement(stripped);
            QueryResult rightResult = executor.executeStatement(stmt.right());
            try {
                return executeSetOpInner(stmt, leftResult, rightResult);
            } finally {
                executor.cteStack.pop();
            }
        }

        QueryResult leftResult = executor.executeStatement(stmt.left());
        QueryResult rightResult = executor.executeStatement(stmt.right());
        return executeSetOpInner(stmt, leftResult, rightResult);
    }

    /**
     * A row lock points at the base-table row behind an output row, and a set operation has
     * already combined rows from different relations by the time it has an output row to point
     * at. PostgreSQL rejects the lock wherever in the set operation it was written — on an arm,
     * parenthesised or not, or after the whole thing.
     */
    private void rejectLock(SetOpStmt stmt) {
        SelectStmt.LockClause lock = lockIn(stmt.left());
        if (lock == null) lock = lockIn(stmt.right());
        if (lock == null) return;
        throw new MemgresException("FOR " + (lock.mode() == null ? "UPDATE" : lock.mode())
                + " is not allowed with UNION/INTERSECT/EXCEPT", "0A000");
    }

    private SelectStmt.LockClause lockIn(Statement arm) {
        if (arm instanceof SelectStmt) return ((SelectStmt) arm).lockClause();
        if (arm instanceof SetOpStmt) {
            SelectStmt.LockClause left = lockIn(((SetOpStmt) arm).left());
            return left != null ? left : lockIn(((SetOpStmt) arm).right());
        }
        return null;
    }

    /**
     * True when a branch writes a text type down for column {@code ci} -- {@code 'a'::text} and
     * nothing else. Such a branch really is text and PostgreSQL reports the mismatch rather than
     * coercing it away, where a bare literal is {@code unknown} and takes the other branch's type.
     *
     * <p>Only an explicit cast counts. Anything the engine merely inferred as text -- a function
     * call, a subquery column, an expression -- keeps the coercion path, because refusing those
     * on the strength of a defaulted type rejects SQL PostgreSQL runs.
     */
    private boolean writesTextTypeDown(Statement branch, int ci) {
        if (!(branch instanceof SelectStmt)) return false;
        List<SelectStmt.SelectTarget> targets = ((SelectStmt) branch).targets();
        if (targets == null || ci >= targets.size()) return false;
        Expression expr = targets.get(ci).expr();
        if (!(expr instanceof CastExpr)) return false;
        String name = ((CastExpr) expr).typeName();
        if (name == null) return false;
        DataType dt = DataType.fromPgName(DataType.canonicalName(name));
        return dt == DataType.TEXT || dt == DataType.VARCHAR || dt == DataType.CHAR
                || dt == DataType.NAME;
    }

    private QueryResult executeSetOpInner(SetOpStmt stmt, QueryResult leftResult, QueryResult rightResult) {
        List<Column> columns = new ArrayList<>(leftResult.getColumns());
        if (columns.size() != rightResult.getColumns().size()) {
            throw new MemgresException("each " + stmt.op().name() + " query must have the same number of columns", "42601");
        }
        // Check type compatibility and widen column types between corresponding columns
        for (int ci = 0; ci < columns.size(); ci++) {
            DataType leftType = columns.get(ci).getType();
            DataType rightType = rightResult.getColumns().get(ci).getType();
            // A bare NULL branch has no type of its own; PG takes the type of the branch
            // that has one, so SELECT NULL UNION ALL SELECT 1 is integer, not unknown.
            if (leftType == null && rightType != null) {
                Column orig = columns.get(ci);
                columns.set(ci, new Column(orig.getName(), rightType, true, false, orig.getDefaultValue()));
                continue;
            }
            if (leftType != null && rightType != null && leftType != rightType) {
                TypeCoercion.TypeCategory leftCat = TypeCoercion.categoryOf(leftType);
                TypeCoercion.TypeCategory rightCat = TypeCoercion.categoryOf(rightType);
                if (leftCat == rightCat && leftCat == TypeCoercion.TypeCategory.NUMERIC) {
                    DataType wider = widenNumericSetOp(leftType, rightType);
                    if (wider != leftType) {
                        Column orig = columns.get(ci);
                        columns.set(ci, new Column(orig.getName(), wider, orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
                    }
                } else if (leftCat == rightCat
                        && leftCat == TypeCoercion.TypeCategory.DATETIME) {
                    // A set operation gives its column one type and reads every arm as that type,
                    // so a date and a timestamp of the same instant are the same row. Leaving each
                    // arm the type it came with described the column as date and left the two
                    // values unequal, so UNION kept both of them.
                    DataType wider = widenDatetimeSetOp(leftType, rightType);
                    // A date and a time of day have no type that holds both. PostgreSQL settles a
                    // set operation's column on the first arm's type and reads the rest as that,
                    // so it reports the reading it could not do. Letting the two stand described
                    // the column as one of them and answered rows of the other, which the client
                    // then failed to decode.
                    if (wider == null) {
                        throw new MemgresException(stmt.op().name() + " could not convert type "
                                + rightType.toRegtypeDisplay() + " to "
                                + leftType.toRegtypeDisplay(), "42846");
                    }
                    if (leftType != wider) {
                        coerceColumnValues(leftResult, ci, wider);
                        Column orig = columns.get(ci);
                        columns.set(ci, new Column(orig.getName(), wider, orig.isNullable(),
                                orig.isPrimaryKey(), orig.getDefaultValue()));
                    }
                    if (rightType != wider) {
                        coerceColumnValues(rightResult, ci, wider);
                    }
                } else if (leftCat == rightCat) {
                    // Same category, different types (e.g., varchar vs text) - allow
                } else if (leftCat != rightCat) {
                    boolean leftIsText = leftType == DataType.TEXT || leftType == DataType.VARCHAR
                            || leftType == DataType.CHAR || leftType == DataType.NAME;
                    boolean rightIsText = rightType == DataType.TEXT || rightType == DataType.VARCHAR
                            || rightType == DataType.CHAR || rightType == DataType.NAME;
                    if (!leftIsText && !rightIsText) {
                        throw new MemgresException(stmt.op().name() + " types "
                                + leftType.toRegtypeDisplay() + " and "
                                + rightType.toRegtypeDisplay() + " cannot be matched", "42804");
                    }
                    // One side is text-category, the other is not. In PostgreSQL, string
                    // literals and NULL have type "unknown" which is implicitly coercible to
                    // any type. Our engine uses TEXT for both unknown literals and actual text
                    // columns. To distinguish: columns from real tables have tableOid > 0,
                    // and a branch whose query writes the type down -- 'a'::text -- is text in
                    // its own right, so PostgreSQL reports the mismatch instead of coercing it.
                    Column textCol = leftIsText ? columns.get(ci) : rightResult.getColumns().get(ci);
                    boolean writtenAsText = leftIsText
                            ? writesTextTypeDown(stmt.left(), ci) : writesTextTypeDown(stmt.right(), ci);
                    if (textCol.getTableOid() != 0 || writtenAsText) {
                        throw new MemgresException(stmt.op().name() + " types "
                                + leftType.toRegtypeDisplay() + " and "
                                + rightType.toRegtypeDisplay() + " cannot be matched", "42804");
                    }
                    // TEXT from a computed expression or literal — attempt coercion
                    DataType targetType = leftIsText ? rightType : leftType;
                    List<Object[]> textRows = leftIsText ? leftResult.getRows() : rightResult.getRows();
                    for (Object[] row : textRows) {
                        Object val = row[ci];
                        if (val != null) {
                            try {
                                executor.castEvaluator.applyCast(val, targetType.getPgName());
                            } catch (Exception e) {
                                // PG returns 22P02 (invalid_text_representation) when a
                                // literal/unknown value cannot be coerced to the target type
                                // in a set operation (e.g., SELECT 1 UNION ALL SELECT 'x').
                                // The type is named the way PostgreSQL names it in a message --
                                // "integer", not the catalog's "int4" -- as the mismatch error
                                // just above already does.
                                throw new MemgresException(
                                        "invalid input syntax for type " + targetType.toRegtypeDisplay()
                                                + ": \"" + val + "\"", "22P02");
                            }
                        }
                    }
                    // Coercion succeeded for all values — use the non-text type
                    if (leftIsText && !rightIsText) {
                        Column orig = columns.get(ci);
                        columns.set(ci, new Column(orig.getName(), rightType, orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
                    }
                }
            }
        }
        // UNION, INTERSECT and EXCEPT tell one row from another, and INTERSECT and EXCEPT do so
        // even with ALL, so every output column needs an equality to be compared by. A few types
        // have none, and PostgreSQL refuses the whole operation rather than comparing them some
        // other way; only UNION ALL, which never compares anything, takes them.
        if (stmt.op() != SetOpStmt.SetOpType.UNION || !stmt.all()) {
            rejectColumnWithoutEquality(columns);
        }
        List<Object[]> resultRows = new ArrayList<>();

        switch (stmt.op()) {
            case UNION: {
                resultRows.addAll(leftResult.getRows());
                resultRows.addAll(rightResult.getRows());
                if (!stmt.all()) {
                    resultRows = deduplicateRows(resultRows);
                }
                break;
            }
            case INTERSECT: {
                if (stmt.all()) {
                    // Multiset semantics: each left row matches at most once per right occurrence.
                    Map<RowKey, Integer> rightCounts = new HashMap<>();
                    for (Object[] row : rightResult.getRows()) {
                        rightCounts.merge(new RowKey(row), 1, Integer::sum);
                    }
                    for (Object[] row : leftResult.getRows()) {
                        RowKey key = new RowKey(row);
                        int remaining = rightCounts.getOrDefault(key, 0);
                        if (remaining > 0) {
                            resultRows.add(row);
                            rightCounts.put(key, remaining - 1);
                        }
                    }
                } else {
                    Set<RowKey> rightKeys = new HashSet<>();
                    for (Object[] row : rightResult.getRows()) {
                        rightKeys.add(new RowKey(row));
                    }
                    Set<RowKey> seen = new HashSet<>();
                    for (Object[] row : leftResult.getRows()) {
                        RowKey key = new RowKey(row);
                        if (rightKeys.contains(key) && seen.add(key)) {
                            resultRows.add(row);
                        }
                    }
                }
                break;
            }
            case EXCEPT: {
                Map<RowKey, Integer> rightCounts = new HashMap<>();
                for (Object[] row : rightResult.getRows()) {
                    rightCounts.merge(new RowKey(row), 1, Integer::sum);
                }
                Set<RowKey> seen = new HashSet<>();
                for (Object[] row : leftResult.getRows()) {
                    RowKey key = new RowKey(row);
                    if (stmt.all()) {
                        int remaining = rightCounts.getOrDefault(key, 0);
                        if (remaining > 0) {
                            rightCounts.put(key, remaining - 1);
                        } else {
                            resultRows.add(row);
                        }
                    } else {
                        if (!rightCounts.containsKey(key) && seen.add(key)) {
                            resultRows.add(row);
                        }
                    }
                }
                break;
            }
        }

        final List<Column> orderColumns = columns;
        // ORDER BY on set operation result
        final List<SelectStmt.OrderByItem> orderBy = validateOrderBy(stmt, columns);
        if (orderBy != null && !orderBy.isEmpty()) {
            resultRows.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : orderBy) {
                    int colIdx = -1;
                    if (item.expr() instanceof Literal && ((Literal) item.expr()).literalType() == Literal.LiteralType.INTEGER) {
                        Literal lit = (Literal) item.expr();
                        colIdx = Integer.parseInt(lit.value()) - 1;
                    } else if (item.expr() instanceof ColumnRef && ((ColumnRef) item.expr()).table() == null) {
                        ColumnRef ref = (ColumnRef) item.expr();
                        for (int i = 0; i < columns.size(); i++) {
                            if (columns.get(i).getName().equalsIgnoreCase(ref.column())) {
                                colIdx = i;
                                break;
                            }
                        }
                    }
                    if (colIdx < 0 || colIdx >= a.length) continue;

                    Object va = a[colIdx], vb = b[colIdx];
                    if (va == null && vb == null) continue;
                    if (va == null || vb == null) {
                        boolean nullsFirst = item.nullsFirst() != null ? item.nullsFirst() : item.descending();
                        if (va == null) return nullsFirst ? -1 : 1;
                        else return nullsFirst ? 1 : -1;
                    }
                    int cmp = executor.compareValues(va, vb);
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        // OFFSET + LIMIT. A set operation reads them exactly as a plain SELECT does, so it uses
        // the same reader: a negative one is refused rather than silently reordering the result,
        // a fractional one is rounded, and one too large for an int no longer wraps.
        if (stmt.offset() != null) {
            long off = select.limitOffsetValue(stmt.offset(), false);
            if (off >= resultRows.size()) {
                resultRows = Cols.listOf();
            } else if (off > 0) {
                resultRows = new ArrayList<>(resultRows.subList((int) off, resultRows.size()));
            }
        }
        if (stmt.limit() != null) {
            long lim = select.limitOffsetValue(stmt.limit(), true);
            if (lim >= 0 && lim < resultRows.size()) {
                int end = (int) lim;
                // WITH TIES keeps going past the count for as long as the rows are equal to the
                // last one under the ORDER BY — cutting there would answer with an arbitrary one
                // of a group the query said was indistinguishable.
                if (stmt.withTies() && stmt.orderBy() != null && !stmt.orderBy().isEmpty()
                        && end > 0) {
                    int[] keys = new int[stmt.orderBy().size()];
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = orderByColumnIndex(stmt.orderBy().get(i), orderColumns);
                    }
                    Object[] last = resultRows.get(end - 1);
                    while (end < resultRows.size()
                            && tiedWith(last, resultRows.get(end), keys)) {
                        end++;
                    }
                }
                resultRows = new ArrayList<>(resultRows.subList(0, end));
            }
        }

        return QueryResult.select(columns, resultRows);
    }

    /** The output column an ORDER BY item names, by position or by name, or -1. */
    private static int orderByColumnIndex(SelectStmt.OrderByItem item, List<Column> columns) {
        Expression expr = item.expr();
        if (expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
            return Integer.parseInt(((Literal) expr).value()) - 1;
        }
        if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
            String want = ((ColumnRef) expr).column();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equalsIgnoreCase(want)) return i;
            }
        }
        return -1;
    }

    /** True when two rows carry the same values in every ORDER BY key. */
    private static boolean tiedWith(Object[] a, Object[] b, int[] keys) {
        for (int key : keys) {
            if (key < 0 || key >= a.length || key >= b.length) continue;
            if (!java.util.Objects.deepEquals(a[key], b[key])) return false;
        }
        return true;
    }

    /**
     * The ORDER BY of a set operation sees the output columns and nothing else: there is no
     * relation left to read a name from once the arms have been combined, so PostgreSQL accepts
     * only an output column name or its position. Anything else it refuses, and the refusals
     * differ by shape -- an ordinal out of range is out of range, a bare non-integer constant
     * names no column at all, a qualified name has no FROM entry to qualify against, an unknown
     * bare name is a missing column, and everything else is an expression the clause does not
     * take. Sorting by whatever happened to match and ignoring the rest let ORDER BY 5 pass; and
     * because the OFFSET beside it was read with neither a sign check nor a rounding, OFFSET -1
     * changed the row order instead of raising.
     *
     * <p>"Only an output column name" is the shape of what PostgreSQL accepts, not the test it
     * applies. What it does is analyse the item and then look for the result among the output
     * columns it already has, refusing only when the item added one. So an expression that
     * analyses back to an output column is accepted, and the one such expression is a cast to the
     * type the column already has: {@code ORDER BY a::int} over an integer {@code a} is a relabel
     * PostgreSQL elides, and it sorts. {@code a::bigint} is a real coercion and is refused, and so
     * are {@code b::varchar} over text, {@code +a}, {@code abs(a)} and {@code 1::int}. Refusing
     * every cast alike refused SQL PostgreSQL runs.
     */
    private List<SelectStmt.OrderByItem> validateOrderBy(SetOpStmt stmt, List<Column> columns) {
        if (stmt.orderBy() == null) return null;
        List<SelectStmt.OrderByItem> resolved = new ArrayList<>();
        for (SelectStmt.OrderByItem item : stmt.orderBy()) {
            Expression expr = item.expr();
            boolean collated = expr instanceof CollateExpr;
            if (collated) expr = ((CollateExpr) expr).expr();

            // A cast that changes nothing is not there once the item is analysed, so peel it off
            // and judge what it wrapped -- and sort by what it wrapped, since the peeled item is
            // the one the sort below knows how to read. COLLATE is a node of its own and never
            // peels away.
            if (!collated) expr = stripNoOpCasts(expr, columns);
            resolved.add(new SelectStmt.OrderByItem(expr, item.descending(), item.nullsFirst()));

            Integer pos = GroupByValidator.integerConstant(expr);
            if (pos != null) {
                // COLLATE binds to the constant itself, not to the column it would have named,
                // so the collation is asked of an integer and PostgreSQL says integers have none.
                if (collated) {
                    throw new MemgresException("collations are not supported by type integer", "42804");
                }
                if (pos < 1 || pos > columns.size()) {
                    throw new MemgresException(
                            "ORDER BY position " + pos + " is not in select list", "42P10");
                }
                continue;
            }
            if (expr instanceof Literal) {
                throw new MemgresException("non-integer constant in ORDER BY", "42601");
            }
            // Every name written anywhere in the item is looked up before the clause is judged as
            // a whole, so ORDER BY a + 1 over arms that have no a is a missing column and not an
            // unsupported expression.
            ColumnRef unresolved = unresolvedColumnRef(expr, columns);
            if (unresolved != null) {
                if (unresolved.table() != null) {
                    throw new MemgresException("missing FROM-clause entry for table \""
                            + unresolved.table() + "\"", "42P01");
                }
                MemgresException missing = new MemgresException(
                        "column \"" + unresolved.column() + "\" does not exist", "42703");
                nameAnArmThatHasIt(missing, stmt, unresolved.column());
                throw missing;
            }
            if (expr instanceof ColumnRef) {
                rejectAmbiguousOutputName(columns, ((ColumnRef) expr).column());
                Column named = namedColumn(columns, ((ColumnRef) expr).column());
                if (!collated) continue;
                if (named != null && !isCollatable(named.getType())) {
                    throw new MemgresException("collations are not supported by type "
                            + named.getType().toRegtypeDisplay(), "42804");
                }
            }
            throw notAnOutputColumn();
        }
        return resolved;
    }

    /**
     * The error PostgreSQL raises for an ORDER BY item over a set operation that is not one of the
     * output columns, carrying the Detail and the Hint it sends with it.
     */
    static MemgresException notAnOutputColumn() {
        MemgresException e = new MemgresException(
                "invalid UNION/INTERSECT/EXCEPT ORDER BY clause", "0A000");
        e.setDetail("Only result column names can be used, not expressions or functions.");
        e.setHint("Add the expression/function to every SELECT, "
                + "or move the UNION into a FROM clause.");
        return e;
    }

    /**
     * {@code expr} with any casts peeled off that cast an output column to the type it already
     * has, or {@code expr} unchanged. Only a chain of such casts over a named output column peels:
     * a cast of anything else, or to any other type, is a coercion the analysed item keeps, and
     * keeping it here is what makes the item fail the output-column test below.
     */
    private static Expression stripNoOpCasts(Expression expr, List<Column> columns) {
        Expression inner = expr;
        while (inner instanceof CastExpr) inner = ((CastExpr) inner).expr();
        if (!(inner instanceof ColumnRef) || ((ColumnRef) inner).table() != null) return expr;
        Column named = namedColumn(columns, ((ColumnRef) inner).column());
        if (named == null || named.getType() == null) return expr;
        Expression walk = expr;
        while (walk instanceof CastExpr) {
            String typeName = ((CastExpr) walk).typeName();
            if (typeName == null) return expr;
            // A cast that carries a length or a precision is a coercion whatever the type is:
            // numeric(10,2) rounds and varchar(4) truncates, so neither is the relabel that peels
            // away. Reading the type name past the parenthesis made both look like no cast at all.
            if (typeName.indexOf('(') >= 0) return expr;
            if (DataType.fromPgName(DataType.canonicalName(typeName)) != named.getType()) return expr;
            walk = ((CastExpr) walk).expr();
        }
        return inner;
    }

    /**
     * A name the set operation does not answer to may still be a column of one of its arms, and
     * PostgreSQL says which arm rather than leaving the writer to guess. An arm has no name of its
     * own, so it is called {@code *SELECT* n} by its place in the operation, counted left to right
     * from one. A name that reads the same but for its case gets the same account as a suggestion
     * instead, which is how a quoted alias written unquoted in the ORDER BY is explained.
     */
    private void nameAnArmThatHasIt(MemgresException e, SetOpStmt stmt, String name) {
        List<SelectStmt> arms = new ArrayList<>();
        collectArms(stmt, arms);
        for (int i = 0; i < arms.size(); i++) {
            for (String output : outputNames(arms.get(i))) {
                if (!output.equals(name)) continue;
                e.setDetail("There is a column named \"" + name + "\" in table \"*SELECT* "
                        + (i + 1) + "\", but it cannot be referenced from this part of the query.");
                return;
            }
        }
        for (int i = 0; i < arms.size(); i++) {
            for (String output : outputNames(arms.get(i))) {
                if (!output.equalsIgnoreCase(name)) continue;
                e.setHint("Perhaps you meant to reference the column \"*SELECT* "
                        + (i + 1) + "." + output + "\".");
                return;
            }
        }
    }

    /** The arms of a set operation, left to right, however they are nested. */
    private static void collectArms(Statement stmt, List<SelectStmt> out) {
        if (stmt instanceof SetOpStmt) {
            collectArms(((SetOpStmt) stmt).left(), out);
            collectArms(((SetOpStmt) stmt).right(), out);
            return;
        }
        if (stmt instanceof SelectStmt) out.add((SelectStmt) stmt);
    }

    /** The names one arm writes for its columns, as written -- an alias, or the implicit name. */
    private List<String> outputNames(SelectStmt arm) {
        List<String> names = new ArrayList<>();
        if (arm.targets() == null) return names;
        for (SelectStmt.SelectTarget target : arm.targets()) {
            String name = target.alias() != null
                    ? target.alias() : executor.exprToAlias(target.expr());
            if (name != null) names.add(name);
        }
        return names;
    }

    /**
     * The types a set operation cannot compare rows by, because PostgreSQL defines no equality
     * over them: json is compared by no operator at all (jsonb, which has one, is unaffected), xml
     * likewise, and a point has ordering operators but no equality. Only these three are listed,
     * because these are the three the reference server was measured refusing.
     */
    private static void rejectColumnWithoutEquality(List<Column> columns) {
        for (Column column : columns) {
            DataType type = column.getType();
            if (type != DataType.JSON && type != DataType.XML && type != DataType.POINT) continue;
            throw new MemgresException("could not identify an equality operator for type "
                    + type.toRegtypeDisplay(), "42883");
        }
    }

    /**
     * A name that two output columns answer to names neither. The ORDER BY of a set operation
     * reaches its output columns through the set operation, where each of them is a column of its
     * own however it was written -- so two of them called the same thing leave the name with no
     * one referent, and PostgreSQL refuses it. (Written on a plain SELECT the same name is not
     * ambiguous when the two columns are the same expression, because there it reaches the select
     * list itself; a set operation has no select list left to reach.) Taking the first match
     * sorted by one of them.
     */
    private static void rejectAmbiguousOutputName(List<Column> columns, String name) {
        int matches = 0;
        for (Column column : columns) {
            if (column.getName().equals(name)) matches++;
        }
        if (matches > 1) {
            throw new MemgresException("ORDER BY \"" + name + "\" is ambiguous", "42702");
        }
    }

    /**
     * An output column of exactly this name. A quoted alias keeps the case it was written with and
     * an unquoted name is folded down, so the two are the same name only when they read the same:
     * matching without regard to case let {@code ORDER BY foo} reach a column called {@code "Foo"},
     * which PostgreSQL says does not exist.
     */
    private static Column namedColumn(List<Column> columns, String name) {
        for (Column column : columns) {
            if (column.getName().equals(name)) return column;
        }
        return null;
    }

    /**
     * The first name in {@code node} that the output columns do not account for, or null. A
     * qualified name never does, because a set operation leaves no relation to qualify against.
     * A subquery written inside the item brings its own scope, so nothing under one is judged
     * here -- ORDER BY (SELECT max(a) FROM t) is an unsupported expression, not a missing a.
     */
    private static ColumnRef unresolvedColumnRef(Object node, final List<Column> columns) {
        if (node == null) return null;
        if (node instanceof SubqueryExpr || node instanceof ExistsExpr
                || node instanceof ArraySubqueryExpr || node instanceof Statement) {
            return null;
        }
        if (node instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) node;
            if (ref.table() != null) return ref;
            return namedColumn(columns, ref.column()) == null ? ref : null;
        }
        final ColumnRef[] found = new ColumnRef[1];
        AstWalk.forEachChild(node, new java.util.function.Consumer<Object>() {
            @Override
            public void accept(Object child) {
                if (found[0] != null) return;
                found[0] = unresolvedColumnRef(child, columns);
            }
        });
        return found[0];
    }

    /** Only the string types carry a collation; a collation asked of any other type is an error. */
    private static boolean isCollatable(DataType type) {
        return type == DataType.TEXT || type == DataType.VARCHAR || type == DataType.CHAR
                || type == DataType.NAME;
    }

    /**
     * The one date/time type two arms can both be read as, or null when there is none. A date is
     * a timestamp of midnight and a timestamp is a timestamptz in the session's zone, so each of
     * those pairs has a wider type both fit; a time and a date have none, and are left to the
     * mismatch the caller reports.
     */
    private static DataType widenDatetimeSetOp(DataType a, DataType b) {
        int ra = datetimeRank(a), rb = datetimeRank(b);
        if (ra == 0 || rb == 0) return null;
        boolean aIsDateLike = ra <= 3, bIsDateLike = rb <= 3;
        if (aIsDateLike != bIsDateLike) return null;
        return rb > ra ? b : a;
    }

    /** Date-like types rank 1..3 and time-like types 4..5; anything else is 0. */
    private static int datetimeRank(DataType dt) {
        if (dt == DataType.DATE) return 1;
        if (dt == DataType.TIMESTAMP) return 2;
        if (dt == DataType.TIMESTAMPTZ) return 3;
        if (dt == DataType.TIME) return 4;
        if (dt == DataType.TIMETZ) return 5;
        return 0;
    }

    /** Read every value of one column of one arm as {@code target}. */
    private void coerceColumnValues(QueryResult result, int ci, DataType target) {
        for (Object[] row : result.getRows()) {
            if (row[ci] == null) continue;
            row[ci] = executor.castEvaluator.applyCast(row[ci], target.getPgName());
        }
    }

    static DataType widenNumericSetOp(DataType a, DataType b) {
        int ra = numericRank(a), rb = numericRank(b);
        return rb > ra ? b : a;
    }

    static int numericRank(DataType dt) {
        switch (dt) {
            case SMALLINT:
            case SMALLSERIAL:
                return 1;
            case INTEGER:
            case SERIAL:
                return 2;
            case BIGINT:
            case BIGSERIAL:
                return 3;
            case NUMERIC:
                return 4;
            case REAL:
                return 5;
            case DOUBLE_PRECISION:
                return 6;
            default:
                return 0;
        }
    }

    static List<Object[]> deduplicateRows(List<Object[]> rows) {
        Set<RowKey> seen = new LinkedHashSet<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] row : rows) {
            if (seen.add(new RowKey(row))) {
                result.add(row);
            }
        }
        return result;
    }
}
