package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * A stored query written back out the way pg_get_viewdef writes it.
 *
 * <p>PostgreSQL lays a definition out while it deparses rather than afterwards: it carries an
 * indentation level through the tree, and every clause keyword moves that level and then starts a
 * line at it. That is why a sub-select's FROM sits eight columns further in than the FROM around
 * it, and why a CASE inside a select list begins a line of its own. Formatting a finished
 * one-line string cannot reach inside a sub-select or a CASE, so the layout is produced here, at
 * the point each piece is written.
 *
 * <p>The second argument of pg_get_viewdef does not choose between a laid-out form and a flat
 * one — both forms are laid out. What it chooses is whether the parentheses precedence makes
 * unnecessary are dropped; without it every operator, every join and every cast carries its own
 * pair.
 */
public final class ViewDeparser {

    /** The indentation steps PostgreSQL moves by: a clause, a join, a select-list continuation. */
    private static final int STD = 8;
    private static final int JOIN_STEP = 4;
    private static final int VAR_STEP = 4;

    /** Past this depth PostgreSQL folds the indentation back rather than run off the page. */
    private static final int INDENT_LIMIT = 40;

    private final boolean prettyParen;
    private final int wrapColumn;
    private final SqlUnparser.ColumnTypes types;
    private final String qualifyingSchema;

    private StringBuilder out = new StringBuilder();
    private int indentLevel;

    /** Whether the query being written sits inside another one, which is what makes it qualify. */
    private boolean nested;
    /** The relation a column of this query can only have come from, or null when more than one. */
    private String soleRelation;
    /** Whether a column of this query is written with the relation it comes from. */
    private boolean qualify;
    /** The type a bare literal in the position being written resolves to, or null when unknown. */
    private DataType literalType;
    /** The relations this query reads, in the order written, for resolving a bare column to one. */
    private List<String> scopeRelations = new ArrayList<String>();
    /** The name each of those relations is written under here, in the same order. */
    private List<String> scopeAliases = new ArrayList<String>();
    /** Every name a query around this one already writes a relation under. */
    private List<String> takenRelations = new ArrayList<String>();
    /** Whether a reader sees the names this query gives its columns. */
    private boolean colNamesVisible = true;
    /** The names this query's columns are published under, or null when nothing publishes them. */
    private List<String> resultNames;

    /** What PostgreSQL calls a column the query gave no name of its own. */
    private static final String UNNAMED = "?column?";

    private ViewDeparser(boolean prettyParen, int wrapColumn, SqlUnparser.ColumnTypes types,
                         String qualifyingSchema) {
        this.prettyParen = prettyParen;
        this.wrapColumn = wrapColumn;
        this.types = types;
        this.qualifyingSchema = qualifyingSchema;
    }

    /**
     * The definition text pg_get_viewdef answers with, without its closing semicolon.
     *
     * @param prettyParen      true to keep only the parentheses precedence needs
     * @param wrapColumn       the column a select list wraps at; zero breaks after every item
     * @param types            the declared types of the columns the query reads, or null
     * @param qualifyingSchema the schema to write an unqualified relation under, or null
     */
    public static String viewDef(Statement stmt, boolean prettyParen, int wrapColumn,
                                 SqlUnparser.ColumnTypes types, String qualifyingSchema) {
        if (!(stmt instanceof SelectStmt) && !(stmt instanceof SetOpStmt)) {
            return SqlUnparser.prettyViewDef(SqlUnparser.toSql(stmt), wrapColumn);
        }
        ViewDeparser deparser = new ViewDeparser(prettyParen, wrapColumn, types, qualifyingSchema);
        deparser.queryDef(stmt, 0, false);
        return deparser.out.toString();
    }

    /**
     * A query standing inside a rule, the way pg_get_ruledef writes one.
     *
     * <p>PostgreSQL analyses a rule against a range table holding OLD and NEW beside whatever the
     * action itself reads, and a query reading more than one relation writes the relation in front
     * of every column -- which is why an action names it where a view over one relation does not.
     * The layout begins at {@code startIndent} because a query feeding an INSERT, or standing
     * inside a clause, is written one step further in than the statement holding it.
     *
     * @param namesVisible whether a reader sees the names this query gives its columns: true of an
     *                     action standing on its own, false of one nothing outside it reads
     */
    public static String ruleQuery(Statement stmt, int startIndent, boolean namesVisible,
                                   SqlUnparser.ColumnTypes types) {
        if (!(stmt instanceof SelectStmt) && !(stmt instanceof SetOpStmt)) {
            return " " + SqlUnparser.toSql(stmt);
        }
        ViewDeparser deparser = new ViewDeparser(false, 0, types, null);
        deparser.queryDef(stmt, startIndent, true, namesVisible, null);
        return deparser.out.toString();
    }

    /**
     * The relations a rule's UPDATE action reads beside the one it writes to, each on the line a
     * query's FROM would put it on: PostgreSQL lays an action out exactly as it lays a query out.
     */
    public static String ruleFromClause(List<SelectStmt.FromItem> items) {
        ViewDeparser deparser = new ViewDeparser(false, 0, null, null);
        deparser.indentLevel = STD;
        deparser.nested = true;
        deparser.qualify = true;
        List<String> names = new ArrayList<String>();
        collectItemNames(items, names);
        deparser.scopeRelations = names;
        deparser.scopeAliases = deparser.assignNames(names);
        deparser.fromClause(items);
        return deparser.out.toString();
    }

    /**
     * The declared types of the columns a stored query reads, so a constant that still had no
     * type of its own when the query was written is printed as the one PostgreSQL resolved it to.
     * Every relation the query names is gathered, sub-selects and CTE bodies included; a column of
     * anything else is left unanswered and printed as it was written.
     */
    public static SqlUnparser.ColumnTypes columnTypesOf(Database database, Database.ViewDef view) {
        return columnTypesOf(database, view.query(),
                view.schemaName() == null ? "public" : view.schemaName());
    }

    /**
     * The same lookup for a query that belongs to no view: the body of a rule's action, which is
     * kept as the statement it was written as rather than as a relation of its own.
     */
    public static SqlUnparser.ColumnTypes columnTypesOf(Database database, Statement query,
                                                        String schemaName) {
        final java.util.Map<String, Table> byName = new java.util.LinkedHashMap<String, Table>();
        collectQueryRelations(database, query, schemaName, byName);
        return new SqlUnparser.ColumnTypes() {
            @Override
            public String typeOf(String relation, String column) {
                for (java.util.Map.Entry<String, Table> entry : byName.entrySet()) {
                    if (relation != null && !relation.equalsIgnoreCase(entry.getKey())) continue;
                    for (Column col : entry.getValue().getColumns()) {
                        if (!col.getName().equalsIgnoreCase(column)) continue;
                        // A domain, an enum or a composite is named in its own terms; only a
                        // column whose type this deparser can name settles a constant's form.
                        if (col.getEnumTypeName() != null || col.getDomainTypeName() != null
                                || col.getCompositeTypeName() != null) {
                            return null;
                        }
                        // An array is named by the type of its elements, and a column declared
                        // with a width states it: a cast that only drops the width has work to do,
                        // and stays in the definition where a cast to the same type does not.
                        if (col.getArrayElementType() != null) {
                            return col.getArrayElementType().toRegtypeDisplay() + "[]";
                        }
                        if (col.getType() == null) return null;
                        String display = col.getType().toRegtypeDisplay();
                        if (col.getPrecision() == null) return display;
                        return display + "(" + col.getPrecision()
                                + (col.getScale() == null ? "" : "," + col.getScale()) + ")";
                    }
                }
                return null;
            }
        };
    }

    private static void collectQueryRelations(Database database, Statement stmt, String schemaName,
                                              java.util.Map<String, Table> out) {
        if (stmt instanceof SetOpStmt) {
            collectQueryRelations(database, ((SetOpStmt) stmt).left(), schemaName, out);
            collectQueryRelations(database, ((SetOpStmt) stmt).right(), schemaName, out);
            return;
        }
        if (!(stmt instanceof SelectStmt)) return;
        SelectStmt select = (SelectStmt) stmt;
        collectFromRelations(database, select.from(), schemaName, out);
        List<SelectStmt.CommonTableExpr> ctes = select.withClauses();
        for (int i = 0; ctes != null && i < ctes.size(); i++) {
            collectQueryRelations(database, ctes.get(i).query(), schemaName, out);
        }
    }

    private static void collectFromRelations(Database database, List<SelectStmt.FromItem> items,
                                             String schemaName, java.util.Map<String, Table> out) {
        if (items == null) return;
        for (SelectStmt.FromItem item : items) {
            if (item instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
                String schema = ref.schema() != null ? ref.schema() : schemaName;
                Schema found = database.getSchema(schema);
                Table table = found != null ? found.getTable(ref.table()) : null;
                if (table != null) out.put(ref.alias() != null ? ref.alias() : ref.table(), table);
            } else if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
                collectFromRelations(database, java.util.Arrays.asList(join.left(), join.right()),
                        schemaName, out);
            } else if (item instanceof SelectStmt.SubqueryFrom) {
                collectQueryRelations(database, ((SelectStmt.SubqueryFrom) item).subquery(),
                        schemaName, out);
            }
        }
    }

    // ---- layout ------------------------------------------------------------------------------

    /**
     * Start a line at the current level and write {@code text} on it.
     *
     * @param before how far the level moves before the line is measured
     * @param after  how far it moves once the line has started
     * @param plus   extra columns for this line alone, which the level does not keep
     */
    private void keyword(String text, int before, int after, int plus) {
        indentLevel += before;
        trimTrailingSpaces();
        int amount;
        if (indentLevel < INDENT_LIMIT) {
            amount = Math.max(indentLevel, 0) + plus;
        } else {
            amount = INDENT_LIMIT + (indentLevel - INDENT_LIMIT) / (STD / 2);
            amount %= INDENT_LIMIT;
            amount += plus;
        }
        out.append('\n');
        spaces(amount);
        out.append(text);
        indentLevel += after;
        if (indentLevel < 0) indentLevel = 0;
    }

    private void spaces(int n) {
        for (int i = 0; i < n; i++) out.append(' ');
    }

    /**
     * Drop the spaces at the end of the line being left. The separator that stood between two
     * items belongs to neither once a newline does the separating, and leaving it behind turns
     * into trailing whitespace at end of line.
     */
    private void trimTrailingSpaces() {
        int end = out.length();
        while (end > 0 && out.charAt(end - 1) == ' ') end--;
        out.setLength(end);
    }

    /** How wide the line being written already is, which is what a wrap column is measured against. */
    private int currentLineLength() {
        int nl = out.lastIndexOf("\n");
        return out.length() - (nl + 1);
    }

    // ---- statements --------------------------------------------------------------------------

    /**
     * Write a whole query at {@code startIndent}. Every query keeps its own indentation, its own
     * idea of which relation its columns may be left bare against, and its own nesting: a
     * sub-select restores all three when it is done.
     */
    private void queryDef(Statement stmt, int startIndent, boolean nestedHere) {
        queryDef(stmt, startIndent, nestedHere, true, null);
    }

    /**
     * Write a query that stands inside an expression. PostgreSQL does not show the names such a
     * query gives its columns, because nothing outside the expression can read them: a column
     * whose name would be the placeholder is written without a name at all.
     */
    private void sublinkDef(Statement stmt) {
        queryDef(stmt, indentLevel, true, false, null);
    }

    private void queryDef(Statement stmt, int startIndent, boolean nestedHere,
                          boolean namesVisible, List<String> published) {
        int saveIndent = indentLevel;
        boolean saveNested = nested;
        String saveSole = soleRelation;
        boolean saveQualify = qualify;
        List<String> saveScope = scopeRelations;
        List<String> saveAliases = scopeAliases;
        List<String> saveTaken = takenRelations;
        boolean saveVisible = colNamesVisible;
        List<String> savePublished = resultNames;
        DataType saveLiteral = literalType;
        indentLevel = startIndent;
        nested = nestedHere;
        literalType = null;
        colNamesVisible = namesVisible;
        resultNames = published;
        try {
            if (stmt instanceof SetOpStmt) {
                SetOpStmt setOp = (SetOpStmt) stmt;
                // Only the query a reader reads by name publishes any: an operation written
                // inside another query takes the names of whatever holds it.
                setOpNode(setOp, namesVisible, nestedHere ? null : publishedNames(setOp));
                queryTail(setOp.orderBy(), setOp.offset(), setOp.limit());
            } else if (stmt instanceof SelectStmt) {
                SelectStmt select = (SelectStmt) stmt;
                // The names this query's relations are written under are settled before anything
                // is written, because a WITH clause is deparsed inside them: a CTE body may not
                // reuse a name the query reading it has already taken.
                enterScope(select);
                withClause(select);
                basicSelect(select);
                queryTail(select.orderBy(), select.offset(), select.limit());
            } else {
                out.append(SqlUnparser.toSql(stmt));
            }
        } finally {
            indentLevel = saveIndent;
            nested = saveNested;
            soleRelation = saveSole;
            qualify = saveQualify;
            scopeRelations = saveScope;
            scopeAliases = saveAliases;
            takenRelations = saveTaken;
            colNamesVisible = saveVisible;
            resultNames = savePublished;
            literalType = saveLiteral;
        }
    }

    private void withClause(SelectStmt select) {
        List<SelectStmt.CommonTableExpr> ctes = select.withClauses();
        if (ctes == null || ctes.isEmpty()) return;
        indentLevel += STD;
        out.append(' ').append("WITH ");
        for (int i = 0; i < ctes.size(); i++) {
            if (ctes.get(i).recursive()) { out.append("RECURSIVE "); break; }
        }
        for (int i = 0; i < ctes.size(); i++) {
            SelectStmt.CommonTableExpr cte = ctes.get(i);
            if (i > 0) out.append(", ");
            out.append(ident(cte.name()));
            appendNameList(cte.columnNames());
            out.append(" AS (");
            keyword("", 0, 0, 0);
            queryDef(cte.query(), indentLevel, true);
            keyword("", 0, 0, 0);
            out.append(')');
        }
        indentLevel -= STD;
        keyword("", 0, 0, 0);
    }

    private void basicSelect(SelectStmt select) {
        indentLevel += STD;
        out.append(' ');
        out.append("SELECT");
        List<Expression> on = select.distinctOn();
        if (on != null && !on.isEmpty()) {
            out.append(" DISTINCT ON (");
            for (int i = 0; i < on.size(); i++) {
                if (i > 0) out.append(", ");
                sortKey(on.get(i));
            }
            out.append(')');
        } else if (select.distinct()) {
            out.append(" DISTINCT");
        }
        targetList(select);
        fromClause(select);
        if (select.where() != null) {
            keyword(" WHERE ", -STD, STD, 1);
            expr(select.where(), 0);
        }
        List<SelectStmt.GroupingElement> written = select.groupingElements();
        if (written != null && !written.isEmpty()) {
            // ROLLUP, CUBE and GROUPING SETS each fold to a list of sets, and several spellings
            // fold to the same list. What is read back is the spelling, so it is written from
            // what the reader wrote rather than from what it folded to -- otherwise every one of
            // them read back as a plain GROUP BY, which groups differently from what it says.
            keyword(" GROUP BY ", -STD, STD, 1);
            if (select.groupByDistinct()) out.append("DISTINCT ");
            for (int i = 0; i < written.size(); i++) {
                if (i > 0) out.append(", ");
                groupingElement(written.get(i));
            }
        } else {
            List<Expression> groupBy = select.groupBy();
            if (groupBy != null && !groupBy.isEmpty()) {
                keyword(" GROUP BY ", -STD, STD, 1);
                for (int i = 0; i < groupBy.size(); i++) {
                    if (i > 0) out.append(", ");
                    sortKey(groupBy.get(i));
                }
            }
        }
        if (select.having() != null) {
            keyword(" HAVING ", -STD, STD, 0);
            expr(select.having(), 0);
        }
        List<SelectStmt.WindowDef> windows = select.windowDefs();
        if (windows != null && !windows.isEmpty()) {
            keyword(" WINDOW ", -STD, STD, 1);
            for (int i = 0; i < windows.size(); i++) {
                SelectStmt.WindowDef def = windows.get(i);
                if (i > 0) out.append(", ");
                out.append(def.name()).append(" AS (");
                windowSpec(def.refName(), def.partitionBy(), def.orderBy(), def.frame());
                out.append(')');
            }
        }
    }

    private void queryTail(List<SelectStmt.OrderByItem> orderBy, Expression offset, Expression limit) {
        if (orderBy != null && !orderBy.isEmpty()) {
            keyword(" ORDER BY ", -STD, STD, 1);
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) out.append(", ");
                sortItem(orderBy.get(i));
            }
        }
        // PostgreSQL writes OFFSET before LIMIT whatever order they were written in, because it
        // deparses the analysed query, which holds the two as separate fields in that order.
        if (offset != null) {
            keyword(" OFFSET ", -STD, STD, 0);
            expr(offset, 0);
        }
        if (limit != null) {
            keyword(" LIMIT ", -STD, STD, 0);
            expr(limit, 0);
        }
    }

    private void sortItem(SelectStmt.OrderByItem item) {
        sortKey(item.expr());
        if (item.descending()) out.append(" DESC");
        // Descending order already puts nulls first and ascending order puts them last, so
        // PostgreSQL writes the clause only where it asks for the other one.
        if (item.nullsFirst() != null && item.nullsFirst().booleanValue() != item.descending()) {
            out.append(item.nullsFirst().booleanValue() ? " NULLS FIRST" : " NULLS LAST");
        }
    }

    /**
     * A key a query is sorted, grouped or made distinct by. PostgreSQL brackets every one of them
     * that is not a plain column, a call included, so that a key which is only a number cannot be
     * read back as the position of a column instead.
     */
    private void sortKey(Expression key) {
        boolean paren = !(key instanceof ColumnRef) && !(key instanceof Literal);
        if (paren) out.append('(');
        expr(key, 0);
        if (paren) out.append(')');
    }

    /**
     * One element of a GROUP BY, written as PostgreSQL writes it: ROLLUP and CUBE take their
     * columns straight after the word, GROUPING SETS takes a space and then its members, and
     * every member of a GROUPING SETS is parenthesised whether the reader wrote it that way or
     * not -- which is why {@code GROUPING SETS (a, b)} reads back as {@code ((a), (b))}.
     */
    private void groupingElement(SelectStmt.GroupingElement element) {
        if (element == null) return;
        switch (element.kind()) {
            case SIMPLE:
                expr(element.expr(), 0);
                break;
            case LIST:
                groupingColumns(element.columns());
                break;
            case ROLLUP:
                out.append("ROLLUP");
                groupingColumns(element.columns());
                break;
            case CUBE:
                out.append("CUBE");
                groupingColumns(element.columns());
                break;
            case SETS:
            default: {
                out.append("GROUPING SETS (");
                List<SelectStmt.GroupingElement> members = element.members();
                for (int i = 0; i < members.size(); i++) {
                    if (i > 0) out.append(", ");
                    groupingElement(members.get(i));
                }
                out.append(')');
                break;
            }
        }
    }

    private void groupingColumns(List<Expression> columns) {
        out.append('(');
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) out.append(", ");
            expr(columns.get(i), 0);
        }
        out.append(')');
    }

    /**
     * Write a set operation, its operator on a line of its own at the level of the arms.
     *
     * <p>A left arm that is itself a set operation of the same kind is written flat, because
     * {@code a UNION b UNION c} means the same however it associates; any other nesting keeps its
     * parentheses.
     */
    private void setOpNode(Statement node, boolean namesVisible, List<String> published) {
        if (!(node instanceof SetOpStmt)) {
            boolean paren = armNeedsParens(node);
            if (paren) out.append('(');
            queryDef(node, indentLevel, true, namesVisible, published);
            if (paren) out.append(')');
            return;
        }
        SetOpStmt op = (SetOpStmt) node;
        boolean needParen = false;
        if (op.left() instanceof SetOpStmt) {
            SetOpStmt left = (SetOpStmt) op.left();
            needParen = left.op() != op.op() || left.all() != op.all();
        }
        int subindent = 0;
        if (needParen) {
            out.append('(');
            subindent = STD;
            keyword("", subindent, 0, 0);
        }
        setOpNode(op.left(), namesVisible, published);
        if (needParen) keyword(") ", -subindent, 0, 0);
        else keyword("", 0, 0, 0);
        out.append(op.op().name());
        if (op.all()) out.append(" ALL");
        indentLevel += subindent;
        out.append('\n');
        spaces(Math.max(indentLevel, 0));
        // A set operation takes its column names from its first arm, so only that arm shows names
        // of its own; every later arm is written against the names already settled.
        setOpNode(op.right(), false, published);
    }

    /** The names a set operation publishes: the ones its first arm gives its columns. */
    private List<String> publishedNames(Statement node) {
        Statement first = node;
        while (first instanceof SetOpStmt) first = ((SetOpStmt) first).left();
        if (!(first instanceof SelectStmt)) return null;
        List<SelectStmt.SelectTarget> targets = ((SelectStmt) first).targets();
        if (targets == null || targets.isEmpty()) return null;
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < targets.size(); i++) {
            SelectStmt.SelectTarget target = targets.get(i);
            // A star stands for however many columns the relation has, so an arm that writes one
            // settles no names this deparser can count off against the other arms' columns.
            if (target.expr() instanceof WildcardExpr) return null;
            names.add(target.alias() != null ? target.alias() : figuredName(target.expr()));
        }
        return names;
    }

    /** A set-operation arm keeps its parentheses when it carries a clause the operator cannot. */
    private static boolean armNeedsParens(Statement arm) {
        if (!(arm instanceof SelectStmt)) return false;
        SelectStmt select = (SelectStmt) arm;
        return (select.orderBy() != null && !select.orderBy().isEmpty())
                || select.limit() != null
                || select.offset() != null
                || (select.withClauses() != null && !select.withClauses().isEmpty());
    }

    // ---- select list -------------------------------------------------------------------------

    private void targetList(SelectStmt select) {
        List<SelectStmt.SelectTarget> targets = select.targets();
        if (targets == null || targets.isEmpty()) return;
        String sep = " ";
        boolean lastWasMultiline = false;
        for (int i = 0; i < targets.size(); i++) {
            SelectStmt.SelectTarget target = targets.get(i);
            out.append(sep);
            sep = ", ";
            StringBuilder item = new StringBuilder();
            StringBuilder outer = out;
            out = item;
            try {
                expr(target.expr(), 0);
                String label = targetLabel(target, i);
                if (label != null) out.append(" AS ").append(label);
            } finally {
                out = outer;
            }
            // An item that starts a line of its own — a CASE does — needs no separator line, and
            // the space that would have separated it belongs to the line it is leaving.
            if (item.length() > 0 && item.charAt(0) == '\n') {
                trimTrailingSpaces();
            } else if (i > 0
                    && (lastWasMultiline || currentLineLength() + item.length() > wrapColumn)) {
                keyword("", -STD, STD, VAR_STEP);
            }
            lastWasMultiline = item.indexOf("\n") >= 0;
            out.append(item);
        }
    }

    /**
     * The name a select item is published under, or null when the item already reads as its own
     * name. PostgreSQL always shows the name a column will be known by where a reader can read the
     * names at all, so an unlabelled call comes back as {@code count(*) AS count} and a constant
     * that never had a name comes back as {@code 1 AS "?column?"}; only a bare column reference,
     * whose name is already what it is called, is written without one. Inside an expression the
     * names go unread, and there the placeholder is left off again.
     */
    private String targetLabel(SelectStmt.SelectTarget target, int position) {
        Expression expr = target.expr();
        if (expr instanceof WildcardExpr) return null;
        String colname = null;
        if (resultNames != null && position < resultNames.size()) colname = resultNames.get(position);
        if (colname == null) colname = target.alias();
        if (colname == null) colname = figuredName(expr);
        if (colname == null) return null;
        String attname;
        if (expr instanceof ColumnRef) attname = ((ColumnRef) expr).column();
        else attname = colNamesVisible ? null : UNNAMED;
        if (attname != null && attname.equals(colname)) return null;
        return ident(colname);
    }

    /** What PostgreSQL calls an unlabelled item, the placeholder included. */
    private static String figuredName(Expression expr) {
        String own = ownName(expr);
        return own == null ? UNNAMED : own;
    }

    /** The name an expression carries of itself, or null where it carries none. */
    private static String ownName(Expression expr) {
        if (expr instanceof ColumnRef) return ((ColumnRef) expr).column();
        if (expr instanceof FunctionCallExpr) return ((FunctionCallExpr) expr).name().toLowerCase(java.util.Locale.ROOT);
        if (expr instanceof WindowFuncExpr) return ((WindowFuncExpr) expr).name().toLowerCase(java.util.Locale.ROOT);
        if (expr instanceof CaseExpr) return "case";
        if (expr instanceof CollateExpr) return ownName(((CollateExpr) expr).expr());
        if (expr instanceof CastExpr) {
            String inner = ownName(((CastExpr) expr).expr());
            if (inner != null) return inner;
            return baseTypeName(((CastExpr) expr).typeName());
        }
        return null;
    }

    // ---- FROM --------------------------------------------------------------------------------

    /**
     * Decide, for the query about to be written, whether its columns carry the relation they come
     * from. PostgreSQL leaves the relation off only where naming it would say nothing: a query
     * that reads exactly one relation and is not itself inside another. Everything else — a join,
     * a comma list, a sub-select, a CTE body, a set-operation arm — names it, because the column
     * could otherwise have come from the query around this one.
     */
    private void enterScope(SelectStmt select) {
        List<SelectStmt.FromItem> from = select.from();
        boolean single = from != null && from.size() == 1
                && !(from.get(0) instanceof SelectStmt.JoinFrom);
        soleRelation = single ? itemName(from.get(0)) : null;
        qualify = nested || !single;
        List<String> names = new ArrayList<String>();
        collectItemNames(from, names);
        scopeRelations = names;
        scopeAliases = assignNames(names);
    }

    /**
     * The name each relation of this query is written under.
     *
     * <p>PostgreSQL settles the names of a query's relations before it writes any of them, and a
     * name a query around this one already writes a relation under is not one this query may
     * reuse: the two would read as the same relation. The one it cannot keep is given a counter,
     * which is why a recursive query's reference to itself comes back as {@code FROM t t_1}.
     */
    private List<String> assignNames(List<String> written) {
        List<String> taken = new ArrayList<String>(takenRelations);
        List<String> assigned = new ArrayList<String>();
        for (int i = 0; i < written.size(); i++) {
            String base = written.get(i);
            String name = base;
            for (int counter = 1; base != null && taken.contains(name); counter++) {
                name = base + "_" + counter;
            }
            assigned.add(name);
            if (name != null) taken.add(name);
        }
        takenRelations = taken;
        return assigned;
    }

    /** The name a relation this query reads is written under, once collisions have been settled. */
    private String printedRelation(String written) {
        for (int i = 0; i < scopeRelations.size() && i < scopeAliases.size(); i++) {
            if (written.equals(scopeRelations.get(i))) return scopeAliases.get(i);
        }
        return written;
    }

    /** Every relation the FROM tree names, in the order a column reference would be resolved. */
    private static void collectItemNames(List<SelectStmt.FromItem> items, List<String> into) {
        if (items == null) return;
        for (SelectStmt.FromItem item : items) {
            if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
                collectItemNames(java.util.Arrays.asList(join.left(), join.right()), into);
            } else {
                String name = itemName(item);
                if (name != null) into.add(name);
            }
        }
    }

    /** The name a column of this FROM item is written with: its alias, else the relation's name. */
    private static String itemName(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            return ref.alias() != null ? ref.alias() : ref.table();
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            return ((SelectStmt.SubqueryFrom) item).alias();
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom fn = (SelectStmt.FunctionFrom) item;
            return fn.alias() != null ? fn.alias() : fn.functionName();
        }
        return null;
    }

    private void fromClause(SelectStmt select) {
        fromClause(select.from());
    }

    private void fromClause(List<SelectStmt.FromItem> from) {
        if (from == null || from.isEmpty()) return;
        for (int i = 0; i < from.size(); i++) {
            if (i == 0) {
                keyword(" FROM ", -STD, STD, 2);
                fromItem(from.get(i));
                continue;
            }
            out.append(", ");
            StringBuilder item = new StringBuilder();
            StringBuilder outer = out;
            out = item;
            try {
                fromItem(from.get(i));
            } finally {
                out = outer;
            }
            if (item.length() > 0 && item.charAt(0) == '\n') {
                trimTrailingSpaces();
            } else if (currentLineLength() + item.length() > wrapColumn) {
                keyword("", -STD, STD, VAR_STEP);
            }
            out.append(item);
        }
    }

    private void fromItem(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            if (ref.only()) out.append("ONLY ");
            // The schema is part of which relation this is, so a definition that has to name one
            // keeps it: dropping it wrote a view over one schema's relation as though it read the
            // relation of whatever schema the reader happens to be in.
            String schema = ref.schema() != null ? ref.schema() : qualifyingSchema;
            if (schema != null) out.append(ident(schema)).append('.');
            out.append(ident(ref.table()));
            String written = ref.alias() != null ? ref.alias() : ref.table();
            String shown = printedRelation(written);
            if (ref.alias() != null || !shown.equals(written)) out.append(' ').append(ident(shown));
            appendColumnAliases(ref.columnAliases());
            return;
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            if (sub.lateral()) out.append("LATERAL ");
            out.append('(');
            queryDef(sub.subquery(), indentLevel, true);
            out.append(')');
            if (sub.alias() != null) out.append(' ').append(ident(printedRelation(sub.alias())));
            appendColumnAliases(sub.columnAliases());
            return;
        }
        if (item instanceof SelectStmt.JoinFrom) {
            joinItem((SelectStmt.JoinFrom) item);
            return;
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom fn = (SelectStmt.FunctionFrom) item;
            out.append(fn.functionName()).append('(');
            List<Expression> args = fn.args();
            for (int i = 0; args != null && i < args.size(); i++) {
                if (i > 0) out.append(", ");
                expr(args.get(i), 0);
            }
            out.append(')');
            if (fn.withOrdinality()) out.append(" WITH ORDINALITY");
            if (fn.alias() != null) out.append(' ').append(ident(printedRelation(fn.alias())));
            appendColumnAliases(fn.columnAliases());
            return;
        }
        out.append(item.toString());
    }

    private void appendColumnAliases(List<String> aliases) {
        appendNameList(aliases);
    }

    /** A parenthesised list of names, each written so that it reads back as the same name. */
    private void appendNameList(List<String> names) {
        if (names == null || names.isEmpty()) return;
        out.append('(');
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) out.append(", ");
            out.append(ident(names.get(i)));
        }
        out.append(')');
    }

    private void joinItem(SelectStmt.JoinFrom join) {
        // Without the pruning flag PostgreSQL brackets the whole join tree, one pair per join, so
        // that the text reads back as the same tree whatever the reader's operator precedence.
        if (!prettyParen) out.append('(');
        fromItem(join.left());
        keyword(joinKeyword(join), -STD, STD, JOIN_STEP);
        fromItem(join.right());
        List<String> using = join.using();
        if (using != null && !using.isEmpty()) {
            out.append(" USING ");
            appendNameList(using);
        } else if (join.on() != null) {
            out.append(" ON ");
            if (!prettyParen) out.append('(');
            expr(join.on(), 0);
            if (!prettyParen) out.append(')');
        }
        if (!prettyParen) out.append(')');
    }

    private static String joinKeyword(SelectStmt.JoinFrom join) {
        switch (join.joinType()) {
            case INNER:
                boolean qualified = join.on() != null
                        || (join.using() != null && !join.using().isEmpty());
                return qualified ? " JOIN " : " CROSS JOIN ";
            case LEFT: return " LEFT JOIN ";
            case RIGHT: return " RIGHT JOIN ";
            case FULL: return " FULL JOIN ";
            case CROSS: return " CROSS JOIN ";
            case NATURAL: return " NATURAL JOIN ";
            case NATURAL_LEFT: return " NATURAL LEFT JOIN ";
            case NATURAL_RIGHT: return " NATURAL RIGHT JOIN ";
            case NATURAL_FULL: return " NATURAL FULL JOIN ";
            default: return " JOIN ";
        }
    }

    // ---- expressions -------------------------------------------------------------------------

    /** Where an operator binds, so a child that binds tighter needs no parentheses of its own. */
    private static int precedenceOf(BinaryExpr.BinOp op) {
        switch (op) {
            case OR: return 1;
            case AND: return 2;
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
            case LESS_EQUAL: case GREATER_EQUAL:
                return 4;
            case ADD: case SUBTRACT: return 6;
            case MULTIPLY: case DIVIDE: case MODULO: return 7;
            case POWER: return 8;
            default: return 5;
        }
    }

    /** True for the operators that resolve an untyped literal against the other operand's type. */
    private static boolean isComparison(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
            case LESS_EQUAL: case GREATER_EQUAL:
                return true;
            default:
                return false;
        }
    }

    private void expr(Expression node, int minPrecedence) {
        if (node == null) { out.append("NULL"); return; }
        if (node instanceof Literal) { literal((Literal) node); return; }
        if (node instanceof ColumnRef) { out.append(columnText((ColumnRef) node)); return; }
        if (node instanceof WildcardExpr) {
            WildcardExpr wildcard = (WildcardExpr) node;
            out.append(wildcard.table() != null ? ident(wildcard.table()) + ".*" : "*");
            return;
        }
        if (node instanceof BinaryExpr) { binary((BinaryExpr) node, minPrecedence); return; }
        if (node instanceof UnaryExpr) { unary((UnaryExpr) node, minPrecedence); return; }
        if (node instanceof IsNullExpr) {
            IsNullExpr isNull = (IsNullExpr) node;
            boolean paren = !prettyParen || 4 < minPrecedence;
            if (paren) out.append('(');
            bracketed(isNull.expr(), null, true);
            out.append(isNull.negated() ? " IS NOT NULL" : " IS NULL");
            if (paren) out.append(')');
            return;
        }
        if (isSimilarToEscape(node)) {
            List<Expression> args = ((FunctionCallExpr) node).args();
            similarTo(args.get(0), args.get(1), args.get(2), false, minPrecedence);
            return;
        }
        if (node instanceof FunctionCallExpr) { functionCall((FunctionCallExpr) node); return; }
        if (node instanceof WindowFuncExpr) { windowFunction((WindowFuncExpr) node); return; }
        if (node instanceof CastExpr) {
            CastExpr cast = (CastExpr) node;
            DataType castType = DataType.fromPgName(baseTypeName(cast.typeName()));
            // A string or a null written with a cast is one labelled constant after parse
            // analysis rather than a cast of a constant, so it carries a single label and no
            // brackets under either flag -- and no label of its own on top of the written one.
            if (cast.expr() instanceof Literal) {
                Literal inner = (Literal) cast.expr();
                if (inner.literalType() == Literal.LiteralType.NULL) {
                    out.append("NULL::").append(castTypeName(cast.typeName()));
                    return;
                }
                if (inner.literalType() == Literal.LiteralType.STRING) {
                    if (castType != null) {
                        constant(canonicalText(inner.value(), castType), castType,
                                castTypeName(cast.typeName()));
                    } else {
                        out.append(quoted(inner.value()));
                        out.append("::").append(castTypeName(cast.typeName()));
                    }
                    return;
                }
                // A number cast to the type it already reads back as is one constant, not a cast
                // of one: the cast is gone by the time the definition is stored, and only a
                // written width survives it as the constant's own label.
                if (castOfOwnType(inner, cast.typeName())) {
                    DataType own = writtenNumberType(inner);
                    constant(canonicalText(inner.value(), own), own,
                            castTypeName(cast.typeName()));
                    return;
                }
            }
            // A cast to the type the expression already has is not in the stored query at all:
            // parse analysis drops a coercion with nothing to do, so upper(name)::text is stored,
            // and written back, as upper(name). A width the cast states is work of its own.
            if (castType != null && cast.typeName().indexOf('(') < 0
                    && castType == settledType(cast.expr()) && !hasDeclaredWidth(cast.expr())) {
                expr(cast.expr(), minPrecedence);
                return;
            }
            // The cast settles the type of everything under it, so a constant inside it takes no
            // label from the column it is being compared with further out.
            DataType outerType = literalType;
            literalType = null;
            try {
                if (!prettyParen) out.append('(');
                expr(cast.expr(), prettyParen ? 9 : 0);
                if (!prettyParen) out.append(')');
            } finally {
                literalType = outerType;
            }
            out.append("::").append(castTypeName(cast.typeName()));
            return;
        }
        if (node instanceof CaseExpr) { caseExpr((CaseExpr) node); return; }
        if (node instanceof SubqueryExpr) {
            boolean paren = minPrecedence > 0;
            if (paren) out.append('(');
            out.append('(');
            sublinkDef(((SubqueryExpr) node).subquery());
            out.append(')');
            if (paren) out.append(')');
            return;
        }
        if (node instanceof ExistsExpr) {
            // PostgreSQL brackets every sub-link, and then brackets the query inside it again
            // for the forms that read as an operator, which EXISTS and IN both do.
            out.append("(EXISTS (");
            sublinkDef(((ExistsExpr) node).subquery());
            out.append("))");
            return;
        }
        if (node instanceof InExpr) { inExpr((InExpr) node, minPrecedence); return; }
        if (node instanceof BetweenExpr) { between((BetweenExpr) node, minPrecedence); return; }
        if (node instanceof LikeExpr) { like((LikeExpr) node, minPrecedence); return; }
        if (node instanceof AnyAllExpr) {
            AnyAllExpr any = (AnyAllExpr) node;
            out.append('(');
            bracketed(any.left(), null, true);
            out.append(' ').append(binOpText(any.op())).append(any.isAll() ? " ALL (" : " ANY (");
            sublinkDef(any.subquery());
            out.append("))");
            return;
        }
        if (node instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr any = (AnyAllArrayExpr) node;
            boolean paren = !prettyParen || minPrecedence > 0;
            if (paren) out.append('(');
            // The operator is resolved over the element type, not over the array, which is what
            // puts a conversion on the value being tested.
            int element = arrayElementOf(any.array());
            int[] readAs = element <= 0 ? null
                    : OperandTypes.forOperator(binOpText(any.op()), oidOf(any.left()), element);
            bracketedOperand(any.left(), readAs == null ? 0 : readAs[0], null, null, true);
            out.append(' ').append(binOpText(any.op())).append(any.isAll() ? " ALL (" : " ANY (");
            expr(any.array(), 0);
            if (readAs != null && readAs[1] != element
                    && any.array() instanceof ArrayExpr) {
                DataType wanted = DataType.arrayOf(DataType.fromOid(readAs[1]));
                if (wanted != null) out.append("::").append(typeDisplay(wanted));
            }
            out.append(')');
            if (paren) out.append(')');
            return;
        }
        if (node instanceof ArrayExpr) {
            ArrayExpr array = (ArrayExpr) node;
            out.append(array.isRow() ? "ROW(" : "ARRAY[");
            List<Expression> elements = array.elements();
            DataType outerType = literalType;
            literalType = null;
            // Every element of an array constructor is read as the one type they settle on
            // together, so a bare string beside a column takes that column's type.
            int element = array.isRow() ? 0 : elementTypeOf(elements);
            try {
                for (int i = 0; elements != null && i < elements.size(); i++) {
                    if (i > 0) out.append(", ");
                    if (element > 0) operandAs(elements.get(i), element, 0);
                    else expr(elements.get(i), 0);
                }
            } finally {
                literalType = outerType;
            }
            out.append(array.isRow() ? ")" : "]");
            return;
        }
        if (node instanceof SubscriptExpr) {
            SubscriptExpr sub = (SubscriptExpr) node;
            // PostgreSQL brackets the subscripted value unless it is a column or another
            // subscript, so ARRAY['x','y'][1] reads back as (ARRAY['x','y'])[1].
            boolean paren = !(sub.base() instanceof ColumnRef)
                    && !(sub.base() instanceof SubscriptExpr);
            if (paren) out.append('(');
            expr(sub.base(), 9);
            if (paren) out.append(')');
            for (SubscriptExpr.Subscript one : sub.subscripts()) {
                out.append('[');
                if (one.lower() != null) expr(one.lower(), 0);
                if (one.slice()) {
                    out.append(':');
                    if (one.upper() != null) expr(one.upper(), 0);
                }
                out.append(']');
            }
            return;
        }
        if (node instanceof ParamRef) {
            out.append('$').append(((ParamRef) node).index());
            return;
        }
        if (node instanceof AtTimeZoneExpr) {
            // AT TIME ZONE is a call SQL gives a syntax of its own, and PostgreSQL writes it back
            // in that syntax, parentheses and all. A zone written as a bare string is a constant
            // of no type until the call resolves it, and the call it resolves to takes text.
            AtTimeZoneExpr zoned = (AtTimeZoneExpr) node;
            out.append('(');
            expr(zoned.expr(), 0);
            out.append(" AT TIME ZONE ");
            DataType savedZone = literalType;
            literalType = DataType.TEXT;
            try {
                expr(zoned.zone(), 0);
            } finally {
                literalType = savedZone;
            }
            out.append(')');
            return;
        }
        if (node instanceof NamedArgExpr) {
            // An argument written under a parameter's name is written back under it, because
            // which parameter it fills is part of what the call says.
            NamedArgExpr named = (NamedArgExpr) node;
            out.append(ident(named.name())).append(" => ");
            expr(named.value(), 0);
            return;
        }
        out.append(SqlUnparser.exprToSql(node));
    }

    private void binary(BinaryExpr node, int minPrecedence) {
        BinaryExpr.BinOp op = node.op();
        if (op == BinaryExpr.BinOp.SIMILAR_TO) {
            similarTo(node.left(), node.right(), null, false, minPrecedence);
            return;
        }
        int precedence = precedenceOf(op);
        boolean paren = !prettyParen || precedence < minPrecedence;
        if (op == BinaryExpr.BinOp.AND || op == BinaryExpr.BinOp.OR) {
            // PostgreSQL holds a chain of the same connective as one node with many arms, so
            // a AND b AND c reads back flat rather than as a AND b, and then AND c.
            List<Expression> arms = new ArrayList<Expression>();
            flattenBoolean(node, op, arms);
            if (paren) out.append('(');
            for (int i = 0; i < arms.size(); i++) {
                if (i > 0) out.append(op == BinaryExpr.BinOp.AND ? " AND " : " OR ");
                expr(arms.get(i), precedence);
            }
            if (paren) out.append(')');
            return;
        }
        if (paren) out.append('(');
        int[] readAs = operandTypesOf(node);
        operandSide(node, node.left(), node.right(), readAs == null ? 0 : readAs[0], true);
        out.append(' ').append(binOpText(op)).append(' ');
        // The right operand of a left-associative operator has to say so when it is another
        // operator of the same strength: a - (b - c) is not a - b - c.
        operandSide(node, node.right(), node.left(), readAs == null ? 0 : readAs[1], false);
        if (paren) out.append(')');
    }

    /**
     * Write one operand of an operator, in the parentheses PostgreSQL puts around it.
     *
     * <p>Which operand carries parentheses is decided by what reads it rather than by the operand
     * itself, and PostgreSQL leaves them off only where the operand already reads as one thing --
     * a name, a constant, a call, a CASE -- or where it is arithmetic standing inside arithmetic
     * of its own strength or weaker. Everything else carries a pair, which is why a sum compared
     * with a number comes back as {@code (a + b) = 1} although nothing could read it another way.
     */
    private void operandSide(BinaryExpr node, Expression self, Expression sibling, int readAs,
                             boolean left) {
        boolean paren = prettyParen && !readsAsOneThing(self, binOpText(node.op()), left);
        if (paren) out.append('(');
        operand(node, self, sibling, 0, readAs);
        if (paren) out.append(')');
    }

    /**
     * Write an expression that something other than an operator reads -- a null test, an ANY, a
     * pattern match -- in the parentheses PostgreSQL puts around it. Only what already reads as
     * one thing goes without.
     */
    private void bracketed(Expression node, String parentOp, boolean left) {
        boolean paren = prettyParen && !readsAsOneThing(node, parentOp, left);
        if (paren) out.append('(');
        expr(node, 0);
        if (paren) out.append(')');
    }

    /** As above, for an operand whose type the operator reading it settled. */
    private void bracketedOperand(Expression node, int readAs, DataType fallback, String parentOp,
                                  boolean left) {
        boolean paren = prettyParen && !readsAsOneThing(node, parentOp, left);
        if (paren) out.append('(');
        operandOr(node, readAs, fallback, 0);
        if (paren) out.append(')');
    }

    /** Whether an expression standing inside {@code parentOp} needs no parentheses of its own. */
    private static boolean readsAsOneThing(Expression node, String parentOp, boolean left) {
        if (node instanceof Literal || node instanceof ColumnRef || node instanceof ParamRef
                || node instanceof WildcardExpr || node instanceof FunctionCallExpr
                || node instanceof WindowFuncExpr || node instanceof CaseExpr
                || node instanceof ArrayExpr || node instanceof SubscriptExpr) {
            return true;
        }
        // A conversion reads as whatever it converts: vc::text is as much one thing as vc is,
        // and (a + b)::numeric is as little one as a + b.
        if (node instanceof CastExpr) return readsAsOneThing(((CastExpr) node).expr(), parentOp, left);
        if (node instanceof CollateExpr) {
            return readsAsOneThing(((CollateExpr) node).expr(), parentOp, left);
        }
        if (node instanceof BinaryExpr) {
            BinaryExpr inner = (BinaryExpr) node;
            if (inner.op() == BinaryExpr.BinOp.AND || inner.op() == BinaryExpr.BinOp.OR) return false;
            return sameStrength(binOpText(inner.op()), parentOp, left);
        }
        // A prefix operator never stands bare inside another operator, and nothing stands bare
        // inside a prefix one: the two spellings would run into each other as a third.
        return false;
    }

    /**
     * Whether an operator standing inside another needs no parentheses. PostgreSQL asks this of
     * the two spellings rather than of a table of precedences: addition inside addition stands
     * bare on the left and bracketed on the right, multiplication stands bare inside addition and
     * on the left of multiplication, and nothing else stands bare at all -- not even an operator
     * that binds tighter, which is why a sum inside a comparison is bracketed.
     */
    private static boolean sameStrength(String op, String parentOp, boolean left) {
        if (op == null || parentOp == null || op.isEmpty() || parentOp.isEmpty()) return false;
        char self = op.charAt(0);
        char outer = parentOp.charAt(0);
        if (self == '+' || self == '-') {
            return (outer == '+' || outer == '-') && left;
        }
        if (self == '*' || self == '/' || self == '%') {
            if (outer == '+' || outer == '-') return true;
            return (outer == '*' || outer == '/' || outer == '%') && left;
        }
        return false;
    }

    /** The types the operator this comparison resolved to reads its two operands as, or null. */
    private int[] operandTypesOf(BinaryExpr node) {
        return OperandTypes.forOperator(resolvedSpelling(node.op()),
                oidOf(node.left()), oidOf(node.right()));
    }

    /**
     * The spelling an operator is registered under, which is what resolves it. A distinctness test
     * is not an operator of its own: PostgreSQL settles it against equality's, which is why the
     * operands of one carry the same conversions equality's operands carry.
     */
    private static String resolvedSpelling(BinaryExpr.BinOp op) {
        if (op == BinaryExpr.BinOp.IS_DISTINCT_FROM
                || op == BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM) {
            return "=";
        }
        if (op == BinaryExpr.BinOp.AND || op == BinaryExpr.BinOp.OR
                || op == BinaryExpr.BinOp.SIMILAR_TO) {
            return null;
        }
        return binOpText(op);
    }

    /**
     * The type an expression already carries, as an OID. A written constant answers
     * {@link OperandTypes#UNKNOWN}, because that is what it has until something reads it; anything
     * whose type cannot be said answers 0, and nothing is then resolved against it.
     */
    private int oidOf(Expression node) {
        if (node instanceof Literal) {
            switch (((Literal) node).literalType()) {
                case STRING:
                case NULL:
                    return OperandTypes.UNKNOWN;
                case INTEGER:
                case FLOAT:
                    return writtenNumberType((Literal) node).getOid();
                case BOOLEAN:
                    return DataType.BOOLEAN.getOid();
                case BIT_STRING:
                    return DataType.BIT.getOid();
                default:
                    return 0;
            }
        }
        DataType settled = settledType(node);
        // A value function names its own type where nothing else does, and a constant written
        // beside one is stored as a constant of it.
        if (settled == null) settled = typeOf(node);
        return settled == null ? 0 : settled.getOid();
    }

    /**
     * The types the built-in a call names reads its arguments as, or null where nothing settles
     * them. An argument whose own type cannot be said settles nothing either, so such a call is
     * left as it was written rather than resolved against a signature it may not have.
     */
    private int[] callArgTypes(FunctionCallExpr node) {
        List<Expression> args = node.args();
        if (args == null || args.isEmpty()) return null;
        int[] written = new int[args.size()];
        for (int i = 0; i < args.size(); i++) {
            int own = oidOf(args.get(i));
            if (own == 0) return null;
            written[i] = own == OperandTypes.UNKNOWN ? BuiltinCallTypes.UNKNOWN : own;
        }
        try {
            return BuiltinCallTypes.argumentTypes(node.name(), written);
        } catch (RuntimeException unsettled) {
            return null;
        }
    }

    /**
     * The types one of the constructs the standard spells as a keyword reads its arguments as.
     * COALESCE and its kin settle on one type for every arm; NULLIF is written with an operator
     * underneath it, so its two arguments carry whatever equality over them resolved to.
     */
    private int[] constructArgTypes(String construct, List<Expression> args) {
        if (args == null || args.isEmpty()) return null;
        int[] written = new int[args.size()];
        for (int i = 0; i < args.size(); i++) written[i] = oidOf(args.get(i));
        if ("NULLIF".equals(construct)) {
            return args.size() == 2
                    ? OperandTypes.forOperator("=", written[0], written[1]) : null;
        }
        int common = OperandTypes.commonType(written);
        if (common <= 0) return null;
        int[] readAs = new int[args.size()];
        for (int i = 0; i < args.size(); i++) readAs[i] = common;
        return readAs;
    }

    /** The element type of whatever an ANY or an ALL was written over, or 0 when it has none. */
    private int arrayElementOf(Expression array) {
        if (array instanceof ArrayExpr && !((ArrayExpr) array).isRow()) {
            return elementTypeOf(((ArrayExpr) array).elements());
        }
        DataType element = DataType.elementOf(settledType(array));
        return element == null ? 0 : element.getOid();
    }

    /** The one type the elements of an array constructor settle on, or 0 when they settle none. */
    private int elementTypeOf(List<Expression> elements) {
        if (elements == null || elements.isEmpty()) return 0;
        int[] written = new int[elements.size()];
        for (int i = 0; i < elements.size(); i++) written[i] = oidOf(elements.get(i));
        return OperandTypes.commonType(written);
    }

    /**
     * Write an expression as the type whatever reads it takes there.
     *
     * <p>PostgreSQL stores the conversion parse analysis put in front of an operand and prints it,
     * so a column of a type the chosen operator does not declare comes back with the conversion
     * on it. A constant carries the same conversion in its own label instead, which is why only a
     * value that already had a type of its own is written with one in front.
     */
    private void operandAs(Expression node, int readAs, int minPrecedence) {
        DataType wanted = readAs <= 0 ? null : DataType.fromOid(readAs);
        if (wanted == null) {
            expr(node, minPrecedence);
            return;
        }
        DataType saved = literalType;
        literalType = wanted;
        try {
            if (!convertedHere(node, readAs)) {
                expr(node, minPrecedence);
                return;
            }
            if (!prettyParen) out.append('(');
            expr(node, prettyParen ? 9 : 0);
            if (!prettyParen) out.append(')');
            out.append("::").append(typeDisplay(wanted));
        } finally {
            literalType = saved;
        }
    }

    /** Whether a conversion stands in front of this operand rather than inside its own label. */
    private boolean convertedHere(Expression node, int readAs) {
        if (node instanceof Literal) {
            // A number and a string are written as constants of the type they are read as, so the
            // conversion is their label. A bit string is not: it is a constant of the bit type
            // whatever reads it, and a conversion to bit varying stands in front of it.
            if (((Literal) node).literalType() != Literal.LiteralType.BIT_STRING) return false;
        }
        int own = oidOf(node);
        return own > 0 && own != OperandTypes.UNKNOWN && own != readAs;
    }

    private static void flattenBoolean(Expression node, BinaryExpr.BinOp op, List<Expression> into) {
        if (node instanceof BinaryExpr && ((BinaryExpr) node).op() == op) {
            flattenBoolean(((BinaryExpr) node).left(), op, into);
            into.add(((BinaryExpr) node).right());
            return;
        }
        into.add(node);
    }

    private void operand(BinaryExpr node, Expression self, Expression sibling, int minPrecedence,
                         int readAs) {
        // What the operator declares is what the operand is read as, wherever the operator could
        // be settled; the rules below stand in for it where it could not.
        if (readAs > 0 && DataType.fromOid(readAs) != null) {
            operandAs(self, readAs, minPrecedence);
            return;
        }
        DataType saved = literalType;
        if (isComparison(node.op())) literalType = constantTypeOf(typeOf(sibling));
        else if (node.op() == BinaryExpr.BinOp.CONCAT) literalType = DataType.TEXT;
        // Arithmetic between two numeric types is done in the wider of them, so a whole number
        // written beside a numeric column is a cast of a constant by the time it is stored.
        else if (isArithmetic(node.op()) && isNumeric(typeOf(sibling))) literalType = typeOf(sibling);
        // The containment and overlap operators are written between two arrays and nothing else,
        // so a constant beside an array column is stored as a constant of that array's type.
        else if (isContainment(node.op()) && DataType.elementOf(typeOf(sibling)) != null) {
            literalType = typeOf(sibling);
        }
        else literalType = null;
        try {
            expr(self, minPrecedence);
        } finally {
            literalType = saved;
        }
    }

    private void unary(UnaryExpr node, int minPrecedence) {
        // A negated pattern match is an operator of its own to PostgreSQL rather than a NOT
        // around the plain one, and the operator is what a stored definition reads back as.
        if (node.op() == UnaryExpr.UnaryOp.NOT && node.operand() instanceof BinaryExpr) {
            BinaryExpr inner = (BinaryExpr) node.operand();
            String negated = null;
            if (inner.op() == BinaryExpr.BinOp.LIKE) negated = "!~~";
            else if (inner.op() == BinaryExpr.BinOp.ILIKE) negated = "!~~*";
            if (negated != null) {
                operatorPair(inner.left(), inner.right(), negated, minPrecedence);
                return;
            }
            if (inner.op() == BinaryExpr.BinOp.SIMILAR_TO) {
                similarTo(inner.left(), inner.right(), null, true, minPrecedence);
                return;
            }
        }
        if (node.op() == UnaryExpr.UnaryOp.NOT && isSimilarToEscape(node.operand())) {
            List<Expression> args = ((FunctionCallExpr) node.operand()).args();
            similarTo(args.get(0), args.get(1), args.get(2), true, minPrecedence);
            return;
        }
        switch (node.op()) {
            case NOT: {
                boolean paren = !prettyParen || 3 < minPrecedence;
                if (paren) out.append('(');
                out.append("NOT ");
                expr(node.operand(), 4);
                if (paren) out.append(')');
                return;
            }
            default: {
                // Every prefix operator is written with a space after it, because the spelling and
                // the operand would otherwise run together into a spelling of their own.
                String prefix = prefixOpText(node.op());
                if (prefix == null) {
                    expr(node.operand(), minPrecedence);
                    return;
                }
                if (!prettyParen) out.append('(');
                out.append(prefix).append(' ');
                DataType saved = literalType;
                literalType = null;
                try {
                    boolean inner = prettyParen && !readsAsOneThing(node.operand(), null, true);
                    if (inner) out.append('(');
                    operandAs(node.operand(),
                            OperandTypes.forPrefixOperator(prefix, oidOf(node.operand())),
                            inner ? 0 : 9);
                    if (inner) out.append(')');
                } finally {
                    literalType = saved;
                }
                if (!prettyParen) out.append(')');
            }
        }
    }

    /** The spelling PostgreSQL registers a prefix operator under, or null where it has none. */
    private static String prefixOpText(UnaryExpr.UnaryOp op) {
        switch (op) {
            case NEGATE: return "-";
            case POSITIVE: return "+";
            case BIT_NOT: return "~";
            case ABS: return "@";
            case SQRT: return "|/";
            case CBRT: return "||/";
            case GEO_CENTER: return "@@";
            case GEO_LENGTH: return "@-@";
            case GEO_NPOINTS: return "#";
            case GEO_IS_HORIZONTAL: return "?-";
            case GEO_IS_VERTICAL: return "?|";
            default: return null;
        }
    }

    /**
     * PostgreSQL rewrites a value list into a comparison against an array and deparses that, so a
     * view built with IN reads back as {@code = ANY}. A sub-query is not rewritten: it stays the
     * sub-link it was, and a negated one is written as a NOT around the plain form.
     */
    private void inExpr(InExpr node, int minPrecedence) {
        List<Expression> values = node.values();
        if (values != null && values.size() == 1 && values.get(0) instanceof SubqueryExpr) {
            if (node.negated()) out.append(prettyParen ? "NOT " : "(NOT ");
            out.append('(');
            bracketed(node.expr(), null, true);
            out.append(" IN (");
            sublinkDef(((SubqueryExpr) values.get(0)).subquery());
            out.append("))");
            if (node.negated() && !prettyParen) out.append(')');
            return;
        }
        List<Expression> listed = values == null ? new ArrayList<Expression>() : values;
        if (node.fromAny()) {
            // Written as ANY over an array rather than as a list: the array settles its own
            // element type, with no say for the value being tested, and stays an array however
            // few elements it holds.
            int[] elements = new int[listed.size()];
            for (int i = 0; i < listed.size(); i++) elements[i] = oidOf(listed.get(i));
            arrayComparison(node.expr(), listed, OperandTypes.commonType(elements),
                    node.negated(), minPrecedence);
            return;
        }
        List<Expression> columns = new ArrayList<Expression>();
        List<Expression> constants = new ArrayList<Expression>();
        for (int i = 0; i < listed.size(); i++) {
            if (listed.get(i) instanceof ColumnRef) columns.add(listed.get(i));
            else constants.add(listed.get(i));
        }
        // An array is worth building only for more than one item that is not itself a column;
        // anything else PostgreSQL writes as the comparisons the list stands for, in the order
        // they were written, with the array -- where there is one -- in front of them.
        boolean array = constants.size() > 1;
        List<Expression> singly = array ? columns : listed;
        int pieces = (array ? 1 : 0) + singly.size();
        BinaryExpr.BinOp op = node.negated()
                ? BinaryExpr.BinOp.NOT_EQUAL : BinaryExpr.BinOp.EQUAL;
        int connective = node.negated() ? 2 : 1;
        boolean chain = pieces > 1;
        boolean paren = chain && (!prettyParen || connective < minPrecedence);
        if (paren) out.append('(');
        int inner = chain ? connective : minPrecedence;
        if (array) {
            // Every item of the list is read as the one type the list and the value being tested
            // settle on, and the comparison itself is resolved over that type.
            int[] written = new int[constants.size() + 1];
            written[0] = oidOf(node.expr());
            for (int i = 0; i < constants.size(); i++) written[i + 1] = oidOf(constants.get(i));
            arrayComparison(node.expr(), constants, OperandTypes.commonType(written),
                    node.negated(), inner);
        }
        for (int i = 0; i < singly.size(); i++) {
            if (i > 0 || array) out.append(node.negated() ? " AND " : " OR ");
            expr(new BinaryExpr(node.expr(), op, singly.get(i)), inner);
        }
        if (paren) out.append(')');
    }

    /**
     * A value tested against an array of the items it was written against. The operator is
     * resolved over the type the items settled on, so the value carries whatever conversion that
     * operator wants -- and the array itself carries one where the operator reads its elements as
     * something else again.
     */
    private void arrayComparison(Expression tested, List<Expression> items, int element,
                                 boolean negated, int minPrecedence) {
        boolean paren = !prettyParen || minPrecedence > 0;
        if (paren) out.append('(');
        int[] readAs = element <= 0 ? null
                : OperandTypes.forOperator(negated ? "<>" : "=", oidOf(tested), element);
        bracketedOperand(tested, readAs == null ? 0 : readAs[0], typeOf(tested), null, true);
        out.append(negated ? " <> ALL (ARRAY[" : " = ANY (ARRAY[");
        DataType saved = literalType;
        literalType = element <= 0 ? constantTypeOf(typeOf(tested)) : DataType.fromOid(element);
        try {
            for (int i = 0; i < items.size(); i++) {
                if (i > 0) out.append(", ");
                if (element > 0) operandAs(items.get(i), element, 0);
                else expr(items.get(i), 0);
            }
        } finally {
            literalType = saved;
        }
        out.append(']');
        if (readAs != null && readAs[1] != element) {
            DataType wanted = DataType.arrayOf(DataType.fromOid(readAs[1]));
            if (wanted != null) out.append("::").append(typeDisplay(wanted));
        }
        out.append(')');
        if (paren) out.append(')');
    }

    /**
     * Write an operand as the type the operator reads it as, where the operator could be settled,
     * and under the rule that stands in for it where it could not.
     */
    private void operandOr(Expression node, int readAs, DataType fallback, int minPrecedence) {
        if (readAs > 0 && DataType.fromOid(readAs) != null) {
            operandAs(node, readAs, minPrecedence);
            return;
        }
        DataType saved = literalType;
        literalType = constantTypeOf(fallback);
        try {
            expr(node, minPrecedence);
        } finally {
            literalType = saved;
        }
    }

    /** A comparison PostgreSQL keeps as an operator, its operands read as that operator's. */
    private void operatorPair(Expression left, Expression right, String spelling,
                              int minPrecedence) {
        boolean paren = !prettyParen || 4 < minPrecedence;
        if (paren) out.append('(');
        int[] readAs = OperandTypes.forOperator(spelling, oidOf(left), oidOf(right));
        bracketedOperand(left, readAs == null ? 0 : readAs[0], null, spelling, true);
        out.append(' ').append(spelling).append(' ');
        bracketedOperand(right, readAs == null ? 0 : readAs[1], typeOf(left), spelling, false);
        if (paren) out.append(')');
    }

    /**
     * SIMILAR TO is not an operator PostgreSQL keeps. The parser rewrites it into a regular
     * expression match against the pattern that {@code similar_to_escape} translates, and the
     * rewritten form is what a stored definition reads back as -- {@code !~} for the negated one.
     */
    private void similarTo(Expression left, Expression pattern, Expression escape,
                           boolean negated, int minPrecedence) {
        String spelling = negated ? "!~" : "~";
        boolean paren = !prettyParen || 4 < minPrecedence;
        if (paren) out.append('(');
        int[] readAs = OperandTypes.forOperator(spelling, oidOf(left), DataType.TEXT.getOid());
        bracketedOperand(left, readAs == null ? 0 : readAs[0], null, spelling, true);
        out.append(' ').append(spelling).append(" similar_to_escape(");
        operandAs(pattern, DataType.TEXT.getOid(), 0);
        if (escape != null) {
            out.append(", ");
            operandAs(escape, DataType.TEXT.getOid(), 0);
        }
        out.append(')');
        if (paren) out.append(')');
    }

    /** Whether a call is the one the parser builds for SIMILAR TO ... ESCAPE. */
    private static boolean isSimilarToEscape(Expression node) {
        if (!(node instanceof FunctionCallExpr)) return false;
        FunctionCallExpr call = (FunctionCallExpr) node;
        return "__similar_to_escape__".equals(call.name())
                && call.args() != null && call.args().size() == 3;
    }

    /**
     * PostgreSQL has no BETWEEN node: the parser expands it into the two comparisons it stands
     * for, and that is what a stored definition reads back as.
     */
    private void between(BetweenExpr node, int minPrecedence) {
        BinaryExpr.BinOp low = node.negated()
                ? BinaryExpr.BinOp.LESS_THAN : BinaryExpr.BinOp.GREATER_EQUAL;
        BinaryExpr.BinOp high = node.negated()
                ? BinaryExpr.BinOp.GREATER_THAN : BinaryExpr.BinOp.LESS_EQUAL;
        BinaryExpr.BinOp connective = node.negated()
                ? BinaryExpr.BinOp.OR : BinaryExpr.BinOp.AND;
        Expression expanded = new BinaryExpr(
                new BinaryExpr(node.expr(), low, node.low()),
                connective,
                new BinaryExpr(node.expr(), high, node.high()));
        expr(expanded, minPrecedence);
    }

    /** A pattern match is an operator to PostgreSQL, and the operator is what it prints. */
    private void like(LikeExpr node, int minPrecedence) {
        String operator;
        if (node.caseInsensitive()) operator = node.negated() ? "!~~*" : "~~*";
        else operator = node.negated() ? "!~~" : "~~";
        if (node.escape() == null) {
            operatorPair(node.left(), node.pattern(), operator, minPrecedence);
            return;
        }
        // An escape character is not something the operator carries: the parser folds it into the
        // pattern by way of like_escape, and the call is what the definition holds.
        boolean paren = !prettyParen || 4 < minPrecedence;
        if (paren) out.append('(');
        int[] readAs = OperandTypes.forOperator(operator, oidOf(node.left()),
                DataType.TEXT.getOid());
        bracketedOperand(node.left(), readAs == null ? 0 : readAs[0], null, operator, true);
        out.append(' ').append(operator).append(" like_escape(");
        operandAs(node.pattern(), DataType.TEXT.getOid(), 0);
        out.append(", ");
        operandAs(node.escape(), DataType.TEXT.getOid(), 0);
        out.append(")");
        if (paren) out.append(')');
    }

    /** Writes an XMLSERIALIZE back in SQL's own syntax; false when the call is not one. */
    private boolean xmlSerialize(FunctionCallExpr node) {
        if (!"xmlserialize".equalsIgnoreCase(node.name())) return false;
        List<Expression> args = node.args();
        int count = args == null ? 0 : args.size();
        if ((count != 3 && count != 4) || !(args.get(0) instanceof Literal)
                || !(args.get(2) instanceof Literal)) {
            return false;
        }
        boolean indent = count == 4 && args.get(3) instanceof Literal
                && "indent".equalsIgnoreCase(((Literal) args.get(3)).value());
        out.append("XMLSERIALIZE(")
           .append("document".equalsIgnoreCase(((Literal) args.get(0)).value())
                   ? "DOCUMENT " : "CONTENT ");
        DataType saved = literalType;
        literalType = null;
        try {
            expr(args.get(1), 0);
        } finally {
            literalType = saved;
        }
        out.append(" AS ")
           .append(RuleDeparser.formatType(
                   RuleDeparser.parseTypeName(((Literal) args.get(2)).value())))
           .append(indent ? " INDENT" : " NO INDENT")
           .append(')');
        return true;
    }

    private void functionCall(FunctionCallExpr node) {
        // A value function is a keyword of the grammar rather than a call, so it is written back
        // as the keyword: no parentheses, and in capitals.
        String valueFunction = valueFunctionName(node);
        if (valueFunction != null) {
            out.append(SqlValueFunctions.keywordOf(valueFunction, false));
            return;
        }
        // XMLSERIALIZE has no other spelling — SQL gives it a syntax and PostgreSQL declares no
        // function of that name — so the record of the call is read back in that syntax. Which of
        // CONTENT and DOCUMENT was asked for and which type the result was wanted as are part of
        // what it says, and so is whether the output is laid out: an unindented serialisation
        // reads back as NO INDENT rather than as nothing at all.
        if (xmlSerialize(node)) return;
        // PostgreSQL writes the constructs the SQL standard spells as keywords in capitals, and
        // every ordinary function under the name it is registered with.
        String name = node.name();
        String upper = name.toUpperCase(java.util.Locale.ROOT);
        boolean sqlConstruct = "COALESCE".equals(upper) || "NULLIF".equals(upper)
                || "GREATEST".equals(upper) || "LEAST".equals(upper);
        out.append(sqlConstruct ? upper : name).append('(');
        if (node.distinct()) out.append("DISTINCT ");
        if (node.star()) {
            out.append('*');
        } else {
            // A function's arguments are read against its own signature, not against whatever the
            // call is being compared with; only the constructs that take a type from a sibling
            // argument -- COALESCE and its kin -- carry one down.
            DataType saved = literalType;
            literalType = sqlConstruct ? firstColumnType(node.args()) : null;
            try {
                List<Expression> args = node.args();
                int[] readAs = sqlConstruct ? constructArgTypes(upper, args) : callArgTypes(node);
                for (int i = 0; args != null && i < args.size(); i++) {
                    if (i > 0) out.append(", ");
                    if (readAs != null && i < readAs.length && readAs[i] > 0) {
                        operandAs(args.get(i), readAs[i], 0);
                    } else {
                        expr(args.get(i), 0);
                    }
                }
            } finally {
                literalType = saved;
            }
        }
        List<SelectStmt.OrderByItem> orderBy = node.orderBy();
        if (orderBy != null && !orderBy.isEmpty()) {
            out.append(" ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) out.append(", ");
                sortItem(orderBy.get(i));
            }
        }
        out.append(')');
        if (node.filter() != null) {
            out.append(" FILTER (WHERE ");
            expr(node.filter(), 0);
            out.append(')');
        }
    }

    private void windowFunction(WindowFuncExpr node) {
        out.append(node.name()).append('(');
        if (node.distinct()) out.append("DISTINCT ");
        if (node.star()) {
            out.append('*');
        } else {
            List<Expression> args = node.args();
            for (int i = 0; args != null && i < args.size(); i++) {
                if (i > 0) out.append(", ");
                expr(args.get(i), 0);
            }
        }
        out.append(')');
        if (node.ignoreNulls()) out.append(" IGNORE NULLS");
        if (node.filter() != null) {
            out.append(" FILTER (WHERE ");
            expr(node.filter(), 0);
            out.append(')');
        }
        out.append(" OVER ");
        if (node.windowName() != null && !node.copiedWindow()) {
            out.append(node.windowName());
            return;
        }
        out.append('(');
        windowSpec(node.copiedWindow() ? node.windowName() : null,
                node.partitionBy(), node.orderBy(), node.frame());
        out.append(')');
    }

    private void windowSpec(String base, List<Expression> partitionBy,
                            List<SelectStmt.OrderByItem> orderBy,
                            WindowFuncExpr.FrameClause frame) {
        boolean written = false;
        if (base != null) { out.append(base); written = true; }
        if (partitionBy != null && !partitionBy.isEmpty()) {
            if (written) out.append(' ');
            out.append("PARTITION BY ");
            for (int i = 0; i < partitionBy.size(); i++) {
                if (i > 0) out.append(", ");
                sortKey(partitionBy.get(i));
            }
            written = true;
        }
        if (orderBy != null && !orderBy.isEmpty()) {
            if (written) out.append(' ');
            out.append("ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) out.append(", ");
                sortItem(orderBy.get(i));
            }
            written = true;
        }
        if (frame != null) {
            if (written) out.append(' ');
            out.append(frame.type().name()).append(' ');
            if (frame.end() != null) {
                out.append("BETWEEN ");
                frameBound(frame.start());
                out.append(" AND ");
                frameBound(frame.end());
            } else {
                frameBound(frame.start());
            }
            if (frame.excludeMode() != null) {
                out.append(" EXCLUDE ").append(frame.excludeMode().name().replace('_', ' '));
            }
        }
    }

    private void frameBound(WindowFuncExpr.FrameBound bound) {
        if (bound == null) { out.append("CURRENT ROW"); return; }
        switch (bound.boundType()) {
            case UNBOUNDED_PRECEDING: out.append("UNBOUNDED PRECEDING"); return;
            case UNBOUNDED_FOLLOWING: out.append("UNBOUNDED FOLLOWING"); return;
            case CURRENT_ROW: out.append("CURRENT ROW"); return;
            case PRECEDING: expr(bound.offset(), 0); out.append(" PRECEDING"); return;
            default: expr(bound.offset(), 0); out.append(" FOLLOWING");
        }
    }

    /**
     * A CASE is laid out over lines of its own: the keyword four columns in from the clause, its
     * arms four further, and END back where CASE started. PostgreSQL also writes the ELSE that
     * was left out, because a CASE with no match answers NULL of the type its arms settled on.
     */
    private void caseExpr(CaseExpr node) {
        keyword("CASE", 0, VAR_STEP, 0);
        DataType saved = literalType;
        try {
            literalType = null;
            if (node.operand() != null) {
                out.append(' ');
                expr(node.operand(), 0);
            }
            int operandOid = node.operand() == null ? 0 : oidOf(node.operand());
            // Every arm of a CASE answers the one type they settle on together, and the value a
            // CASE with an operand is tested against is read as whatever equality resolved to.
            int settled = caseResultOid(node);
            DataType resultType = settled > 0 && DataType.fromOid(settled) != null
                    ? DataType.fromOid(settled) : caseResultType(node);
            for (CaseExpr.WhenClause when : node.whenClauses()) {
                keyword("WHEN ", 0, 0, 0);
                if (node.operand() != null) {
                    int[] readAs = OperandTypes.forOperator("=", operandOid,
                            oidOf(when.condition()));
                    operandOr(when.condition(), readAs == null ? 0 : readAs[1],
                            typeOf(node.operand()), 0);
                } else {
                    literalType = null;
                    expr(when.condition(), 0);
                }
                out.append(" THEN ");
                caseArm(when.result(), settled, resultType);
            }
            literalType = resultType;
            if (node.elseExpr() != null) {
                keyword("ELSE ", 0, 0, 0);
                caseArm(node.elseExpr(), settled, resultType);
            } else if (resultType != null) {
                keyword("ELSE ", 0, 0, 0);
                out.append("NULL::").append(typeDisplay(resultType));
            }
        } finally {
            literalType = saved;
        }
        keyword("END", -VAR_STEP, 0, 0);
    }

    /** Write one arm of a CASE as the type every arm of it is read as. */
    private void caseArm(Expression arm, int settled, DataType fallback) {
        if (settled > 0 && DataType.fromOid(settled) != null) {
            operandAs(arm, settled, 0);
            return;
        }
        literalType = fallback;
        expr(arm, 0);
    }

    /** The one type every arm of a CASE settles on, or 0 where they settle none this can name. */
    private int caseResultOid(CaseExpr node) {
        List<Expression> arms = new ArrayList<Expression>();
        for (CaseExpr.WhenClause when : node.whenClauses()) arms.add(when.result());
        if (node.elseExpr() != null) arms.add(node.elseExpr());
        int[] written = new int[arms.size()];
        for (int i = 0; i < arms.size(); i++) written[i] = oidOf(arms.get(i));
        return OperandTypes.commonType(written);
    }

    /**
     * The type a CASE answers, or null when its arms do not settle one. A column or a written
     * cast names a type outright, so those are read first; failing that the arms are all
     * constants, and the type is the one the first of them reads back as.
     */
    private DataType caseResultType(CaseExpr node) {
        for (CaseExpr.WhenClause when : node.whenClauses()) {
            DataType type = typeOf(when.result());
            if (type != null) return type;
        }
        DataType elseType = typeOf(node.elseExpr());
        if (elseType != null) return elseType;
        for (CaseExpr.WhenClause when : node.whenClauses()) {
            DataType type = constantType(when.result());
            if (type != null) return type;
        }
        return constantType(node.elseExpr());
    }

    /** The type a constant reads back as on its own, or null when the expression is not one. */
    private static DataType constantType(Expression node) {
        if (!(node instanceof Literal)) return null;
        switch (((Literal) node).literalType()) {
            case STRING: return DataType.TEXT;
            case INTEGER: return DataType.INTEGER;
            case FLOAT: return DataType.NUMERIC;
            case BOOLEAN: return DataType.BOOLEAN;
            default: return null;
        }
    }

    // ---- names, types and constants ----------------------------------------------------------

    private String columnText(ColumnRef ref) {
        String relation = ref.table();
        if (qualify) {
            if (relation == null) relation = relationSupplying(ref.column());
        } else if (relation != null && soleRelation != null
                && relation.equalsIgnoreCase(soleRelation)) {
            relation = null;
        }
        if (relation != null) relation = printedRelation(relation);
        return relation != null ? ident(relation) + "." + ident(ref.column()) : ident(ref.column());
    }

    /** A name written so that reading the definition back names the same thing again. */
    private static String ident(String name) {
        return name == null ? null : Quoting.identifier(name);
    }

    /**
     * Which of the relations this query reads a bare column came from. A stored query names the
     * relation of every column it prints once the prefix says something, so a column written
     * without one is put back under the first relation that has a column of that name.
     */
    private String relationSupplying(String column) {
        if (soleRelation != null) return soleRelation;
        if (types == null) return null;
        for (int i = 0; i < scopeRelations.size(); i++) {
            String name = scopeRelations.get(i);
            if (types.typeOf(name, column) != null) return name;
        }
        return null;
    }

    /** The declared type of whatever an expression reads, or null when it cannot be named. */
    private DataType typeOf(Expression node) {
        if (node instanceof ColumnRef && types != null) {
            ColumnRef ref = (ColumnRef) node;
            String name = types.typeOf(ref.table(), ref.column());
            return name == null ? null : DataType.fromPgName(baseTypeName(name));
        }
        if (node instanceof CastExpr) {
            return DataType.fromPgName(baseTypeName(((CastExpr) node).typeName()));
        }
        // A value function names its own type as plainly as a column does, so a constant written
        // beside one is stored as a constant of that type rather than as an untyped string.
        if (node instanceof FunctionCallExpr) {
            return SqlValueFunctions.typeOf(valueFunctionName((FunctionCallExpr) node));
        }
        return null;
    }

    /**
     * The name of the value function a call stands for, or null where the call is an ordinary
     * one. Anything a call can carry that a keyword cannot -- a star, DISTINCT, a filter, an
     * ordering -- makes it an ordinary call whatever it is named.
     */
    private static String valueFunctionName(FunctionCallExpr node) {
        if (node.star() || node.distinct() || node.filter() != null) return null;
        if (node.orderBy() != null && !node.orderBy().isEmpty()) return null;
        boolean hasArgs = node.args() != null && !node.args().isEmpty();
        return SqlValueFunctions.keywordOf(node.name(), hasArgs) == null
                ? null : node.name().toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * The type an expression already has, where that can be said with certainty. A cast to a type
     * an expression already has is not in the stored query at all, so knowing the type is what
     * lets the cast be left out here too; anything this cannot answer for keeps its cast, which
     * is the safe way to be wrong.
     */
    private DataType settledType(Expression node) {
        if (node instanceof ColumnRef || node instanceof CastExpr) return typeOf(node);
        if (node instanceof Literal) {
            switch (((Literal) node).literalType()) {
                case INTEGER: return DataType.INTEGER;
                case FLOAT: return DataType.NUMERIC;
                case BOOLEAN: return DataType.BOOLEAN;
                // A string still has no type of its own, which is the whole point of writing one.
                default: return null;
            }
        }
        if (node instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) node;
            List<Expression> args = call.args();
            int count = call.star() || args == null ? 0 : args.size();
            int[] written = new int[count];
            for (int i = 0; i < count; i++) {
                DataType argType = settledType(args.get(i));
                written[i] = argType == null ? BuiltinCallTypes.UNKNOWN : argType.getOid();
            }
            return DataType.fromOid(BuiltinCallTypes.resultType(call.name(), written));
        }
        if (node instanceof BinaryExpr) {
            BinaryExpr binary = (BinaryExpr) node;
            if (!isArithmetic(binary.op())) return null;
            DataType left = settledType(binary.left());
            DataType right = settledType(binary.right());
            if (!isNumeric(left) || !isNumeric(right)) return null;
            return TypeCoercion.promoteNumeric(left, right);
        }
        return null;
    }

    /** Whether the column an expression reads was declared with a width a cast has to apply. */
    private boolean hasDeclaredWidth(Expression node) {
        if (node instanceof CastExpr) return ((CastExpr) node).typeName().indexOf('(') >= 0;
        if (!(node instanceof ColumnRef) || types == null) return false;
        ColumnRef ref = (ColumnRef) node;
        String name = types.typeOf(ref.table(), ref.column());
        return name != null && name.indexOf('(') >= 0;
    }

    /** The operators an array is compared with another array by. */
    private static boolean isContainment(BinaryExpr.BinOp op) {
        switch (op) {
            case CONTAINS: case CONTAINED_BY: case OVERLAP:
                return true;
            default:
                return false;
        }
    }

    /** The operators that answer in the wider of the two numeric types they were written with. */
    private static boolean isArithmetic(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO:
                return true;
            default:
                return false;
        }
    }

    private static boolean isNumeric(DataType type) {
        if (type == null) return false;
        switch (type) {
            case SMALLINT: case INTEGER: case BIGINT:
            case REAL: case DOUBLE_PRECISION: case NUMERIC:
                return true;
            default:
                return false;
        }
    }

    private DataType firstColumnType(List<Expression> args) {
        for (int i = 0; args != null && i < args.size(); i++) {
            DataType type = typeOf(args.get(i));
            if (type != null) return type;
        }
        return null;
    }

    /**
     * How PostgreSQL names a type in a definition. An array is named by its element type; a
     * blank-padded character keeps the catalogue name it is stored under; and a bit whose width
     * was never stated is written in quotes, because an unadorned {@code bit} means {@code bit(1)}
     * to the grammar and the name has to survive being read again.
     */
    private static String typeDisplay(DataType type) {
        DataType element = DataType.elementOf(type);
        if (element != null) return element.toRegtypeDisplay() + "[]";
        if (type == DataType.CHAR) return "bpchar";
        if (type == DataType.BIT) return "\"bit\"";
        return type.toRegtypeDisplay();
    }

    /**
     * The type a constant written with no type of its own settles on beside a value of
     * {@code declared}, where which operator reads the two could not be worked out. A character
     * varying and a cidr have no operators of their own -- text's and inet's are what a comparison
     * with one resolves to -- so the constant beside one is stored as a constant of that other
     * type.
     */
    private static DataType constantTypeOf(DataType declared) {
        if (declared == DataType.VARCHAR) return DataType.TEXT;
        if (declared == DataType.CIDR) return DataType.INET;
        return declared;
    }

    /** The written type name with any width dropped, which is what names the type itself. */
    private static String baseTypeName(String typeName) {
        if (typeName == null) return null;
        int paren = typeName.indexOf('(');
        return paren < 0 ? typeName.trim() : typeName.substring(0, paren).trim();
    }

    /** A written cast's type under PostgreSQL's own name for it, width and all. */
    private static String castTypeName(String typeName) {
        if (typeName == null) return null;
        String base = baseTypeName(typeName);
        DataType type = DataType.fromPgName(base);
        if (type == null) return typeName;
        int paren = typeName.indexOf('(');
        String width = paren < 0 ? "" : typeName.substring(paren).trim();
        if (type == DataType.CHAR && width.isEmpty()) return "bpchar";
        return type.toRegtypeDisplay() + width;
    }

    /**
     * A constant as PostgreSQL prints it after parse analysis.
     *
     * <p>A bare string literal has no type of its own: read beside a column it becomes a value of
     * that column's type, and PostgreSQL prints the constant that came of it, labelled with the
     * type unless reading the text back gives the same type again. A number written where another
     * numeric type was wanted is not a constant at all by then but a cast of one, which is why it
     * comes back bracketed when the bracket-pruning flag is off.
     */
    private void literal(Literal node) {
        switch (node.literalType()) {
            case STRING: {
                DataType type = literalType == null ? DataType.TEXT : literalType;
                constant(canonicalText(node.value(), type), type, typeDisplay(type));
                return;
            }
            case INTEGER:
            case FLOAT: {
                DataType written = writtenNumberType(node);
                String text = canonicalText(node.value(), written);
                String label = numericLabel(node.literalType(), literalType);
                if (label == null) { constant(text, written, typeDisplay(written)); return; }
                if (prettyParen) out.append(text);
                else out.append('(').append(text).append(')');
                out.append("::").append(label);
                return;
            }
            case BIT_STRING:
                // A bit-string literal is a constant of the bit type by the time it is stored, and
                // it is written back as one: the B in front of the quotes is grammar, not value.
                constant(node.value(), DataType.BIT, typeDisplay(DataType.BIT));
                return;
            case NULL:
                // A null constant has no type of its own to read back, so PostgreSQL always
                // labels it with the one parse analysis gave it.
                out.append("NULL::").append(typeDisplay(
                        literalType == null ? DataType.TEXT : literalType));
                return;
            case DEFAULT:
                out.append("DEFAULT");
                return;
            default:
                out.append(node.value());
        }
    }

    /**
     * A constant written back the way PostgreSQL writes one.
     *
     * <p>Two types are written without quotes, because reading the text again gives a value of the
     * same type: a whole number, and a numeric that reads as a fraction. Those two need no label
     * either -- everything else carries one, since the text alone would be read as something else.
     * A negative number is quoted even so, or it would read as a minus sign applied to a constant
     * rather than as one constant.
     */
    private void constant(String text, DataType type, String display) {
        boolean bare;
        boolean labelled;
        if (type == DataType.BOOLEAN) {
            bare = "true".equals(text) || "false".equals(text);
            labelled = !bare;
        } else if (type == DataType.INTEGER) {
            bare = isDigitsOnly(text);
            labelled = !bare;
        } else if (type == DataType.NUMERIC) {
            bare = looksLikeAFraction(text);
            labelled = !bare || (display != null && display.indexOf('(') >= 0);
        } else {
            bare = false;
            labelled = true;
        }
        out.append(bare ? text : quoted(text));
        if (labelled && display != null) out.append("::").append(display);
    }

    private static boolean isDigitsOnly(String text) {
        if (text.isEmpty()) return false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }

    /** Whether a number reads back as a numeric on its own: a digit first, then a point or an exponent. */
    private static boolean looksLikeAFraction(String text) {
        if (text.isEmpty()) return false;
        char first = text.charAt(0);
        if (first < '0' || first > '9') return false;
        boolean pointed = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '.' || c == 'e' || c == 'E') pointed = true;
            else if ((c < '0' || c > '9') && c != '+' && c != '-') return false;
        }
        return pointed;
    }

    /**
     * The type a number is read as where it stands. The grammar reads a whole number as an integer
     * of the narrowest width that holds it, and anything else as a numeric.
     */
    private static DataType writtenNumberType(Literal node) {
        if (node.literalType() == Literal.LiteralType.FLOAT) return DataType.NUMERIC;
        try {
            long value = Long.parseLong(node.value().trim());
            return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE
                    ? DataType.INTEGER : DataType.BIGINT;
        } catch (NumberFormatException tooWide) {
            return DataType.NUMERIC;
        }
    }

    /**
     * What the type's own reader and writer make of the text a constant was written with.
     *
     * <p>A stored query holds the value, not the text: a timestamp written {@code '2020-01-01'}
     * comes back as {@code '2020-01-01 00:00:00'}, and a number written {@code '007'} as
     * {@code 7}. A text the type cannot read is not a constant PostgreSQL would have stored at
     * all, so there is nothing else to print and the text stands as it was written.
     */
    private static String canonicalText(String written, DataType type) {
        if (written == null || type == null) return written;
        // Money is written in the server's own locale, which is not something a definition read
        // somewhere else can reproduce, so the text it was written with is left alone.
        if (type == DataType.MONEY) return written;
        try {
            // An array is read and written element by element, by the same pair of readers a cast
            // to the array type goes through, so '{ 1, 2 , 3 }' comes back as the '{1,2,3}' that
            // reading the value again would produce.
            DataType element = DataType.elementOf(type);
            if (element != null) return arrayText(written, element);
            Object value = TypeCoercion.coerce(written, type);
            if (value == null) return written;
            String endOfTime = TypeCoercion.infinityText(value);
            if (endOfTime != null) return endOfTime;
            if (value instanceof Boolean) {
                return ((Boolean) value).booleanValue() ? "true" : "false";
            }
            if (value instanceof java.time.OffsetDateTime) {
                return zonedTimestampText((java.time.OffsetDateTime) value);
            }
            return TypeCoercion.toString(value);
        } catch (RuntimeException unreadable) {
            return written;
        }
    }

    /**
     * An array constant with every element read by its own type and written back by it. The
     * braces themselves are read by the one reader that knows PostgreSQL's rules for them, so an
     * element quoted, escaped or padded out with spaces means here what it means to a cast.
     *
     * <p>A default and a check constraint print a constant by the same rule, so RuleDeparser
     * reads an array one through this too rather than growing a second reader beside it.
     */
    static String arrayText(String written, DataType element) {
        PgArray array = PgArray.from(written);
        if (array == null) return written;
        return TypeCoercion.formatPgArray(PgArray.like(array, readElements(array, element)));
    }

    private static List<Object> readElements(List<?> elements, DataType element) {
        List<Object> read = new ArrayList<Object>();
        for (int i = 0; i < elements.size(); i++) {
            Object one = elements.get(i);
            if (one instanceof List<?>) read.add(readElements((List<?>) one, element));
            else read.add(one == null ? null : TypeCoercion.coerce(one, element));
        }
        return read;
    }

    /** A timestamptz as its own writer produces it: the instant, told in the reader's zone. */
    private static String zonedTimestampText(java.time.OffsetDateTime moment) {
        String date = String.format("%04d-%02d-%02d",
                Integer.valueOf(TypeCoercion.displayYear(moment.getYear())),
                Integer.valueOf(moment.getMonthValue()), Integer.valueOf(moment.getDayOfMonth()));
        String time = moment.getNano() != 0
                ? withoutTrailingZeros(moment.format(
                        java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                : moment.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        int seconds = moment.getOffset().getTotalSeconds();
        int hours = seconds / 3600;
        int minutes = Math.abs((seconds % 3600) / 60);
        String zone = minutes == 0
                ? String.format("%+03d", Integer.valueOf(hours))
                : String.format("%+03d:%02d", Integer.valueOf(hours), Integer.valueOf(minutes));
        return date + " " + time + zone + TypeCoercion.eraSuffix(moment.getYear());
    }

    /** A fraction of a second carries only the digits it has, however wide the field was written. */
    private static String withoutTrailingZeros(String text) {
        int end = text.length();
        while (end > 0 && text.charAt(end - 1) == '0') end--;
        if (end > 0 && text.charAt(end - 1) == '.') end--;
        return text.substring(0, end);
    }

    /** Whether a cast names the very type the number written under it already reads back as. */
    private static boolean castOfOwnType(Literal number, String typeName) {
        if (typeName == null) return false;
        DataType wanted = DataType.fromPgName(baseTypeName(typeName));
        if (number.literalType() == Literal.LiteralType.INTEGER) return wanted == DataType.INTEGER;
        if (number.literalType() == Literal.LiteralType.FLOAT) return wanted == DataType.NUMERIC;
        return false;
    }

    /** A string constant in the quotes PostgreSQL writes it with, its own quotes doubled. */
    private static String quoted(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** The type a number written beside a column of another numeric type is cast to, or null. */
    private static String numericLabel(Literal.LiteralType written, DataType wanted) {
        if (wanted == null) return null;
        if (wanted == DataType.REAL || wanted == DataType.DOUBLE_PRECISION) {
            return "double precision";
        }
        if (wanted == DataType.NUMERIC) {
            return written == Literal.LiteralType.INTEGER ? "numeric" : null;
        }
        // A whole number is read as an integer wherever one fits, so a bigint wanted where one
        // stands is a conversion of the constant and carries its name.
        if (wanted == DataType.BIGINT) {
            return written == Literal.LiteralType.INTEGER ? "bigint" : null;
        }
        return null;
    }

    private static String binOpText(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD: return "+";
            case SUBTRACT: return "-";
            case MULTIPLY: return "*";
            case DIVIDE: return "/";
            case MODULO: return "%";
            case EQUAL: return "=";
            case NOT_EQUAL: return "<>";
            case LESS_THAN: return "<";
            case GREATER_THAN: return ">";
            case LESS_EQUAL: return "<=";
            case GREATER_EQUAL: return ">=";
            case AND: return "AND";
            case OR: return "OR";
            case CONCAT: return "||";
            case LIKE: return "~~";
            case ILIKE: return "~~*";
            case JSON_ARROW:
            case JSON_SUBSCRIPT: return "->";
            case JSON_ARROW_TEXT: return "->>";
            case POWER: return "^";
            case CONTAINS: return "@>";
            case CONTAINED_BY: return "<@";
            case OVERLAP: return "&&";
            case IS_DISTINCT_FROM: return "IS DISTINCT FROM";
            case IS_NOT_DISTINCT_FROM: return "IS NOT DISTINCT FROM";
            case REGEX_MATCH: return "~";
            case REGEX_IMATCH: return "~*";
            case NOT_REGEX_MATCH: return "!~";
            case NOT_REGEX_IMATCH: return "!~*";
            case JSON_HASH_ARROW: return "#>";
            case JSON_HASH_ARROW_TEXT: return "#>>";
            case JSON_DELETE_PATH: return "#-";
            case JSONB_EXISTS: return "?";
            case JSONB_EXISTS_ANY: return "?|";
            case JSONB_EXISTS_ALL: return "?&";
            case JSONB_PATH_EXISTS_OP: return "@?";
            case TS_MATCH: return "@@";
            case BIT_AND: return "&";
            case BIT_OR: return "|";
            case BIT_XOR: return "#";
            case SHIFT_LEFT: return "<<";
            case SHIFT_RIGHT: return ">>";
            case INET_CONTAINS_EQUALS: return ">>=";
            case INET_CONTAINED_BY_EQUALS: return "<<=";
            case DISTANCE: return "<->";
            case APPROX_EQUAL: return "~=";
            case GEO_BELOW: return "<<|";
            case GEO_ABOVE: return "|>>";
            case GEO_NOT_EXTEND_RIGHT: return "&<";
            case GEO_NOT_EXTEND_LEFT: return "&>";
            case GEO_NOT_EXTEND_ABOVE: return "&<|";
            case GEO_NOT_EXTEND_BELOW: return "|&>";
            case GEO_INTERSECTS: return "?#";
            case GEO_CLOSEST_POINT: return "##";
            case GEO_PARALLEL: return "?||";
            case GEO_PERPENDICULAR: return "?-|";
            case GEO_HORIZONTAL: return "?-";
            case GEO_VERTICAL: return "?|";
            case RANGE_ADJACENT: return "-|-";
            default: return op.name();
        }
    }
}
