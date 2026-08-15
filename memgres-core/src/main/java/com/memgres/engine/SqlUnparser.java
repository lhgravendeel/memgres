package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts AST Statement nodes back to SQL text.
 * Used by pg_get_viewdef(), information_schema.views, and similar introspection functions.
 */
public class SqlUnparser {

    /**
     * The schema an unqualified relation is written under while a definition is being deparsed.
     *
     * <p>PostgreSQL writes a stored definition with every relation qualified that the reader's
     * search path would not find by its bare name — which is why a dump, whose search path is
     * empty, carries fully qualified view bodies and restores into any path at all. Echoing the
     * text the view was written with produced bodies that only restored under the path they were
     * created in.
     */
    private static final ThreadLocal<String> qualifyingSchema = new ThreadLocal<String>();

    /**
     * The relation whose name may be left off a column reference, or null when every reference is
     * written with the relation it comes from.
     *
     * <p>PostgreSQL prefixes a column with its relation only where the prefix says something: a
     * query that reads exactly one relation, and is not itself inside another query, needs none,
     * so {@code SELECT * FROM t} is stored as {@code SELECT i, v FROM t} and not as
     * {@code SELECT t.i, t.v FROM t}. Everything else keeps the prefix — a join, a sub-select
     * (whose columns may come from the query around it), a set operation's arms.
     */
    private static final ThreadLocal<String> bareRelation = new ThreadLocal<String>();

    /** Whether a query is already being deparsed, which makes the next one a sub-select. */
    private static final ThreadLocal<Boolean> insideQuery = new ThreadLocal<Boolean>();

    /**
     * The relation a query's columns can only have come from, or null when there is more than one
     * or when the single item is not a plain relation.
     */
    private static String soleRelationAlias(SelectStmt sel) {
        if (sel == null || sel.from() == null || sel.from().size() != 1) return null;
        SelectStmt.FromItem only = sel.from().get(0);
        if (!(only instanceof SelectStmt.TableRef)) return null;
        SelectStmt.TableRef ref = (SelectStmt.TableRef) only;
        return ref.alias() != null ? ref.alias() : ref.table();
    }

    /** The state to restore once this query has been deparsed. */
    private static Object[] enterQuery(SelectStmt sel) {
        Object[] prior = new Object[]{bareRelation.get(), insideQuery.get()};
        bareRelation.set(prior[1] == null ? soleRelationAlias(sel) : null);
        insideQuery.set(Boolean.TRUE);
        return prior;
    }

    private static void leaveQuery(Object[] prior) {
        bareRelation.set((String) prior[0]);
        insideQuery.set((Boolean) prior[1]);
    }

    /**
     * The declared type of a column of one of the relations a stored query reads, or null when
     * the caller cannot say. Supplied only where the relations can be looked up; without it an
     * untyped literal is printed as it was written.
     */
    public interface ColumnTypes {
        /** @param relation the alias or relation name written before the column, or null */
        String typeOf(String relation, String column);
    }

    /** The column types of the query being deparsed, or null when the caller supplied none. */
    private static final ThreadLocal<ColumnTypes> readColumnTypes = new ThreadLocal<ColumnTypes>();

    /**
     * Deparse a stored query with the types of the relations it reads to hand, so that a literal
     * that was still of type {@code unknown} when the query was written is printed as the constant
     * PostgreSQL resolved it to.
     */
    public static String toSqlPretty(Statement stmt, ColumnTypes types) {
        readColumnTypes.set(types);
        try {
            return toSqlPretty(stmt);
        } finally {
            readColumnTypes.remove();
        }
    }

    /**
     * An operand of an operator, as PostgreSQL prints it after parse analysis.
     *
     * <p>A bare string literal has no type of its own: compared with a column, it is read as a
     * value of that column's type, and PostgreSQL prints the constant that came of it. The label
     * is dropped only where reading the text back gives the same type again — {@code x = 4} for an
     * integer column, but {@code v = 'v'::text} for a text one, which is the rule
     * {@code get_const_expr} follows.
     */
    private static String comparedToSql(BinaryExpr op, Expression self, Expression sibling,
                                        String written) {
        ColumnTypes types = readColumnTypes.get();
        if (types == null || !isComparison(op.op())) return written;
        if (!(self instanceof Literal)
                || ((Literal) self).literalType() != Literal.LiteralType.STRING
                || !(sibling instanceof ColumnRef)) {
            return written;
        }
        ColumnRef ref = (ColumnRef) sibling;
        String typeName = types.typeOf(ref.table(), ref.column());
        DataType dt = typeName == null ? null : DataType.fromPgName(typeName);
        if (dt == null) return written;
        String value = ((Literal) self).value();
        if (dt == DataType.INTEGER) {
            try {
                return String.valueOf(Integer.parseInt(value.trim()));
            } catch (NumberFormatException notThisType) {
                return written;
            }
        }
        if (dt == DataType.BOOLEAN) return written;
        return written + "::" + dt.toRegtypeDisplay();
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

    /** Deparse a stored definition with unqualified relations written under {@code schema}. */
    public static String toSqlQualified(com.memgres.engine.parser.ast.Statement stmt, String schema,
                                        boolean pretty) {
        qualifyingSchema.set(schema);
        try {
            return pretty ? toSqlPretty(stmt) : toSql(stmt);
        } finally {
            qualifyingSchema.remove();
        }
    }


    /**
     * Reformat a single-line SELECT into PG's pg_get_viewdef "pretty" multi-line
     * layout: leading space before SELECT, each column indented 4 spaces, and
     * FROM/WHERE on their own indented lines. (M19)
     */
    public static String prettyViewDef(String sql) {
        return prettyViewDef(sql, 0);
    }

    /**
     * The clauses pg_get_viewdef starts a line with, and the indent it right-aligns each to.
     * Written out rather than derived: PostgreSQL pads LIMIT and OFFSET differently from the rest.
     */
    private static final String[][] CLAUSE_INDENTS = {
            {"FROM", "\n   "},
            {"WHERE", "\n  "},
            {"GROUP BY", "\n  "},
            {"HAVING", "\n "},
            {"WINDOW", "\n  "},
            {"ORDER BY", "\n  "},
            {"LIMIT", "\n "},
            {"OFFSET", "\n "},
    };

    /**
     * As {@link #prettyViewDef(String)}, but keeping the select list on one line when it fits
     * inside {@code wrapColumn} — the layout pg_get_viewdef(oid, int) produces. A wrap column of
     * zero means the list always breaks, which is the layout of the other two forms.
     */
    public static String prettyViewDef(String sql, int wrapColumn) {
        if (sql == null) return null;
        if (!sql.regionMatches(true, 0, "SELECT", 0, 6)) return sql;
        // Find where each top-level clause starts. Scanning rather than matching on the text means
        // an ORDER BY inside a window frame, or the word FROM inside a string literal, is left
        // where it is instead of being pulled out onto a line of its own.
        int[] cut = clauseStarts(sql);
        int listStart = "SELECT".length();
        boolean distinct = sql.regionMatches(true, listStart + 1, "DISTINCT", 0, 8);
        if (distinct) listStart += " DISTINCT".length();
        int listEnd = sql.length();
        for (int c : cut) {
            if (c >= 0) { listEnd = c; break; }
        }
        String head = " " + sql.substring(0, listStart);
        String[] columns = splitTopLevel(sql.substring(listStart, listEnd));
        StringBuilder cols = new StringBuilder();
        String oneLine = head + " " + joinTrimmed(columns, ", ");
        if (wrapColumn > 0 && oneLine.length() <= wrapColumn) {
            cols.append(joinTrimmed(columns, ", "));
        } else {
            cols.append(columns[0].trim());
            for (int ci = 1; ci < columns.length; ci++) cols.append(",\n    ").append(columns[ci].trim());
        }
        StringBuilder out = new StringBuilder(head).append(' ').append(cols);
        for (int i = 0; i < cut.length; i++) {
            if (cut[i] < 0) continue;
            int end = sql.length();
            for (int j = i + 1; j < cut.length; j++) {
                if (cut[j] >= 0) { end = cut[j]; break; }
            }
            // The space that separated this clause from the one before belongs to neither now
            // that a newline does; leaving it turns into trailing whitespace at end of line.
            while (end > cut[i] && Character.isWhitespace(sql.charAt(end - 1))) end--;
            String clause = sql.substring(cut[i], end);
            // PostgreSQL gives every relation of a comma-separated FROM list a line of its own,
            // lined up under the first one. Only the commas between the items break: the ones
            // inside a function call's arguments or a sub-select belong to those.
            if (i == 0 && clause.indexOf(',') >= 0) {
                clause = joinTrimmed(splitTopLevel(clause), ",\n    ");
            }
            out.append(CLAUSE_INDENTS[i][1]).append(clause);
        }
        return out.toString();
    }

    private static String joinTrimmed(String[] parts, String sep) {
        StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            if (sb.length() > 0) sb.append(sep);
            sb.append(p.trim());
        }
        return sb.toString();
    }

    /**
     * Where each of {@link #CLAUSE_INDENTS} begins in the statement, or -1 when it is not there.
     * Only a keyword outside every parenthesis and every string literal counts, and only one after
     * the last one already found, so a subquery's own WHERE stays inside the subquery.
     */
    private static int[] clauseStarts(String sql) {
        int[] found = new int[CLAUSE_INDENTS.length];
        java.util.Arrays.fill(found, -1);
        int depth = 0;
        boolean inString = false;
        boolean inQuotedName = false;
        int next = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inString) {
                if (c == '\'') inString = false;
                continue;
            }
            if (inQuotedName) {
                if (c == '"') inQuotedName = false;
                continue;
            }
            if (c == '\'') { inString = true; continue; }
            if (c == '"') { inQuotedName = true; continue; }
            if (c == '(') { depth++; continue; }
            if (c == ')') { depth--; continue; }
            if (depth != 0 || next >= CLAUSE_INDENTS.length) continue;
            if (i > 0 && !Character.isWhitespace(sql.charAt(i - 1))) continue;
            for (int k = next; k < CLAUSE_INDENTS.length; k++) {
                String kw = CLAUSE_INDENTS[k][0];
                if (!sql.regionMatches(true, i, kw, 0, kw.length())) continue;
                int after = i + kw.length();
                if (after < sql.length() && !Character.isWhitespace(sql.charAt(after))) continue;
                found[k] = i;
                next = k + 1;
                i = after - 1;
                break;
            }
        }
        return found;
    }

    /** The text in one pair of parentheses, adding a pair only when it does not already have one. */
    private static String wrapOnce(String text) {
        if (text.length() > 1 && text.charAt(0) == '(' && text.charAt(text.length() - 1) == ')') {
            int depth = 0;
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') {
                    depth--;
                    // The opening parenthesis closed before the end, so the outer pair is not one
                    // group: "(a) AND (b)" needs a pair of its own.
                    if (depth == 0 && i < text.length() - 1) return "(" + text + ")";
                }
            }
            return text;
        }
        return "(" + text + ")";
    }

    /** Split a comma-separated list on top-level commas only (ignoring parens). */
    private static String[] splitTopLevel(String s) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) { parts.add(s.substring(start, i)); start = i + 1; }
        }
        parts.add(s.substring(start));
        return parts.toArray(new String[0]);
    }

    /**
     * Convert a Statement AST to SQL text.
     */
    public static String toSql(Statement stmt) {
        if (stmt == null) return null;
        if (stmt instanceof SelectStmt) return selectToSql(((SelectStmt) stmt));
        if (stmt instanceof SetOpStmt) return setOpToSql(((SetOpStmt) stmt));
        if (stmt instanceof InsertStmt) return insertToSql(((InsertStmt) stmt));
        if (stmt instanceof UpdateStmt) return updateToSql(((UpdateStmt) stmt));
        if (stmt instanceof DeleteStmt) return deleteToSql(((DeleteStmt) stmt));
        return stmt.toString(); // fallback
    }

    private static String selectToSql(SelectStmt sel) {
        Object[] prior = enterQuery(sel);
        try {
            return selectBodyToSql(sel);
        } finally {
            leaveQuery(prior);
        }
    }

    private static String selectBodyToSql(SelectStmt sel) {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (sel.distinct()) sb.append("DISTINCT ");

        // Targets
        if (sel.targets() == null || sel.targets().isEmpty()) {
            // empty target list
        } else {
            sb.append(sel.targets().stream()
                    .map(SqlUnparser::targetToSql)
                    .collect(Collectors.joining(", ")));
        }

        // FROM
        if (sel.from() != null && !sel.from().isEmpty()) {
            sb.append(" FROM ");
            sb.append(sel.from().stream()
                    .map(SqlUnparser::fromItemToSql)
                    .collect(Collectors.joining(", ")));
        }

        // WHERE. PostgreSQL prints the qualification wrapped in one pair of parentheses; an
        // operator expression already brings that pair, so adding another unconditionally printed
        // a level PostgreSQL does not — and a view definition is read back and compared as text.
        if (sel.where() != null) {
            sb.append(" WHERE ").append(wrapOnce(exprToSql(sel.where())));
        }

        // GROUP BY
        if (sel.groupBy() != null && !sel.groupBy().isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(sel.groupBy().stream()
                    .map(SqlUnparser::exprToSql)
                    .collect(Collectors.joining(", ")));
        }

        // HAVING
        if (sel.having() != null) {
            sb.append(" HAVING ").append(exprToSql(sel.having()));
        }

        // ORDER BY
        if (sel.orderBy() != null && !sel.orderBy().isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(sel.orderBy().stream()
                    .map(ob -> exprToSql(ob.expr()) + (ob.descending() ? " DESC" : "")
                            + (ob.nullsFirst() != null ? (ob.nullsFirst() ? " NULLS FIRST" : " NULLS LAST") : ""))
                    .collect(Collectors.joining(", ")));
        }

        // LIMIT
        if (sel.limit() != null) {
            sb.append(" LIMIT ").append(exprToSql(sel.limit()));
        }

        // OFFSET
        if (sel.offset() != null) {
            sb.append(" OFFSET ").append(exprToSql(sel.offset()));
        }

        return sb.toString();
    }

    private static String setOpToSql(SetOpStmt setOp) {
        String left = toSql(setOp.left());
        String right = toSql(setOp.right());
        String op = setOp.op().name();
        return left + " " + op + (setOp.all() ? " ALL " : " ") + right;
    }

    private static String targetToSql(SelectStmt.SelectTarget target) {
        String expr = targetExprToSql(target.expr(), exprToSql(target.expr()));
        if (target.alias() != null) {
            return expr + " AS " + target.alias();
        }
        return expr;
    }

    /**
     * A select item as PostgreSQL prints it, which is the item after analysis rather than as it
     * was written. A bare string literal is still of type {@code unknown} while the query is being
     * read, and a column of a stored query may not stay unknown: PostgreSQL resolves it to text,
     * so {@code SELECT 'v' AS lit} comes back as {@code SELECT 'v'::text AS lit}.
     */
    private static String targetExprToSql(Expression expr, String written) {
        boolean untypedLiteral = expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
        return untypedLiteral ? written + "::text" : written;
    }

    private static String fromItemToSql(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) item;
            // The schema is part of which relation this is, so a definition that names one keeps
            // it: dropping it wrote a view over zz_cv2.b as though it read the b of whatever
            // schema the reader happens to be in.
            String schema = tr.schema() != null ? tr.schema() : qualifyingSchema.get();
            String relation = schema != null ? schema + "." + tr.table() : tr.table();
            return relation + (tr.alias() != null ? " " + tr.alias() : "");
        }
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            return fromItemToSql(join.left()) + " " + join.joinType().name().replace("_", " ") +
                    " JOIN " + fromItemToSql(join.right()) +
                    (join.on() != null ? " ON " + exprToSql(join.on()) : "") +
                    (join.using() != null ? " USING (" + String.join(", ", join.using()) + ")" : "");
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sq = (SelectStmt.SubqueryFrom) item;
            return "(" + toSql(sq.subquery()) + ")" + (sq.alias() != null ? " " + sq.alias() : "");
        }
        return item.toString();
    }

    /**
     * Convert an Expression AST to SQL text.
     */
    public static String exprToSql(Expression expr) {
        if (expr == null) return "NULL";
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case STRING:
                    return "'" + lit.value().replace("'", "''") + "'";
                case BIT_STRING:
                    return "B'" + lit.value() + "'";
                case NULL:
                    return "NULL";
                case BOOLEAN:
                    return lit.value();
                case DEFAULT:
                    return "DEFAULT";
                default:
                    return lit.value();
            }
        } else if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            String written = bareRelation.get();
            boolean prefixSaysNothing = written != null && ref.table() != null
                    && written.equalsIgnoreCase(ref.table());
            return (ref.table() != null && !prefixSaysNothing ? ref.table() + "." : "") + ref.column();
        } else if (expr instanceof WildcardExpr) {
            WildcardExpr w = (WildcardExpr) expr;
            return w.table() != null ? w.table() + ".*" : "*";
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            return "(" + comparedToSql(bin, bin.left(), bin.right(), exprToSql(bin.left()))
                    + " " + binOpToSql(bin.op()) + " "
                    + comparedToSql(bin, bin.right(), bin.left(), exprToSql(bin.right())) + ")";
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) {
                return "(" + exprToSql(cop.left()) + " " + cop.opSymbol() + " " + exprToSql(cop.right()) + ")";
            } else {
                return "(" + cop.opSymbol() + " " + exprToSql(cop.right()) + ")";
            }
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            switch (un.op()) {
                case NEGATE:
                    return "(-" + exprToSql(un.operand()) + ")";
                case NOT:
                    return "(NOT " + exprToSql(un.operand()) + ")";
                case BIT_NOT:
                    return "(~" + exprToSql(un.operand()) + ")";
                default:
                    return exprToSql(un.operand());
            }
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            // M19: handle count(*) and DISTINCT
            StringBuilder fnSb = new StringBuilder(fn.name()).append('(');
            if (fn.distinct()) fnSb.append("DISTINCT ");
            if (fn.star()) {
                fnSb.append('*');
            } else {
                fnSb.append(fn.args().stream().map(SqlUnparser::exprToSql).collect(Collectors.joining(", ")));
            }
            fnSb.append(')');
            return fnSb.toString();
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return exprToSql(cast.expr()) + "::" + cast.typeName();
        } else if (expr instanceof IsNullExpr) {
            IsNullExpr isn = (IsNullExpr) expr;
            return exprToSql(isn.expr()) + (isn.negated() ? " IS NOT NULL" : " IS NULL");
        } else if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            return caseToSql(c);
        } else if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            return "(" + toSql(sq.subquery()) + ")";
        } else if (expr instanceof ExistsExpr) {
            ExistsExpr ex = (ExistsExpr) expr;
            return "EXISTS (" + toSql(ex.subquery()) + ")";
        } else if (expr instanceof BetweenExpr) {
            BetweenExpr bet = (BetweenExpr) expr;
            return exprToSql(bet.expr()) + (bet.negated() ? " NOT" : "") +
                    " BETWEEN " + exprToSql(bet.low()) + " AND " + exprToSql(bet.high());
        } else if (expr instanceof LikeExpr) {
            LikeExpr like = (LikeExpr) expr;
            return exprToSql(like.left()) + (like.negated() ? " NOT" : "") +
                    (like.caseInsensitive() ? " ILIKE " : " LIKE ") + exprToSql(like.pattern());
        } else if (expr instanceof InExpr) {
            // PostgreSQL rewrites a value list into a scalar-array comparison and deparses that,
            // so a view built with IN reads back as = ANY. Without a case here the AST node fell
            // through to Object.toString and a Java field dump landed in the view definition.
            InExpr in = (InExpr) expr;
            String list = in.values().stream().map(SqlUnparser::exprToSql)
                    .collect(Collectors.joining(", "));
            return "(" + exprToSql(in.expr()) + (in.negated() ? " <> ALL (ARRAY[" : " = ANY (ARRAY[")
                    + list + "]))";
        } else if (expr instanceof ParamRef) {
            // A parameter is written $1 and reads back as $1. Without a case here it fell through
            // to Object.toString and a Java field dump was rendered as SQL — which then re-parsed
            // as an identifier, so a prepared statement's own body named a column nothing holds.
            return "$" + ((ParamRef) expr).index();
        } else if (expr instanceof SubscriptExpr) {
            // Brackets are written back as brackets. Writing them as the json arrow instead turned
            // a one-based array reference into a zero-based json one every time a view was read.
            SubscriptExpr sub = (SubscriptExpr) expr;
            StringBuilder subscripted = new StringBuilder(exprToSql(sub.base()));
            for (SubscriptExpr.Subscript one : sub.subscripts()) {
                subscripted.append('[');
                if (one.lower() != null) subscripted.append(exprToSql(one.lower()));
                if (one.slice()) {
                    subscripted.append(':');
                    if (one.upper() != null) subscripted.append(exprToSql(one.upper()));
                }
                subscripted.append(']');
            }
            return subscripted.toString();
        } else if (expr instanceof ArrayExpr) {
            ArrayExpr arr = (ArrayExpr) expr;
            return arr.isRow() ? "ROW(" +
                    arr.elements().stream().map(SqlUnparser::exprToSql).collect(Collectors.joining(", ")) + ")"
                    : "ARRAY[" + arr.elements().stream().map(SqlUnparser::exprToSql).collect(Collectors.joining(", ")) + "]";
        } else {
            return expr.toString();
        }
    }

    // ---- pretty (paren-minimising) rendering ------------------------------------------------

    /**
     * The statement with only the parentheses operator precedence requires, which is what
     * {@code pg_get_viewdef(oid, true)} and the wrap-column form print. The default rendering
     * parenthesises every operator, the way PostgreSQL's own non-pretty deparse does, so the two
     * forms are produced by two renderers rather than by stripping parentheses out of one.
     */
    public static String toSqlPretty(Statement stmt) {
        if (!(stmt instanceof SelectStmt)) return toSql(stmt);
        SelectStmt sel = (SelectStmt) stmt;
        Object[] prior = enterQuery((SelectStmt) stmt);
        try {
            return selectBodyToSqlPretty(sel);
        } finally {
            leaveQuery(prior);
        }
    }

    private static String selectBodyToSqlPretty(SelectStmt sel) {
        String plain = selectBodyToSql(sel);
        // Only the clauses that hold a bare expression differ; rebuilding the whole statement
        // would duplicate every clause rule for a difference that is confined to three of them.
        StringBuilder sb = new StringBuilder(plain.length());
        int listEnd = plain.indexOf(" FROM ");
        String head = listEnd < 0 ? plain : plain.substring(0, listEnd);
        if (sel.targets() != null && !sel.targets().isEmpty()) {
            head = "SELECT " + (sel.distinct() ? "DISTINCT " : "")
                    + sel.targets().stream()
                        .map(t -> targetExprToSql(t.expr(), exprToSqlPretty(t.expr(), 0))
                                + (t.alias() != null ? " AS " + t.alias() : ""))
                        .collect(Collectors.joining(", "));
        }
        sb.append(head);
        if (sel.from() != null && !sel.from().isEmpty()) {
            sb.append(" FROM ").append(sel.from().stream()
                    .map(SqlUnparser::fromItemToSqlPretty).collect(Collectors.joining(", ")));
        }
        if (sel.where() != null) sb.append(" WHERE ").append(exprToSqlPretty(sel.where(), 0));
        if (sel.groupBy() != null && !sel.groupBy().isEmpty()) {
            sb.append(" GROUP BY ").append(sel.groupBy().stream()
                    .map(e -> exprToSqlPretty(e, 0)).collect(Collectors.joining(", ")));
        }
        if (sel.having() != null) sb.append(" HAVING ").append(exprToSqlPretty(sel.having(), 0));
        if (sel.orderBy() != null && !sel.orderBy().isEmpty()) {
            sb.append(" ORDER BY ").append(sel.orderBy().stream()
                    .map(ob -> exprToSqlPretty(ob.expr(), 0) + (ob.descending() ? " DESC" : "")
                            + (ob.nullsFirst() != null ? (ob.nullsFirst() ? " NULLS FIRST" : " NULLS LAST") : ""))
                    .collect(Collectors.joining(", ")));
        }
        if (sel.limit() != null) sb.append(" LIMIT ").append(exprToSqlPretty(sel.limit(), 0));
        if (sel.offset() != null) sb.append(" OFFSET ").append(exprToSqlPretty(sel.offset(), 0));
        return sb.toString();
    }

    private static String fromItemToSqlPretty(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            String kind = join.joinType().name().replace("_", " ");
            String prefix = "INNER".equals(kind) ? "" : kind + " ";
            return fromItemToSqlPretty(join.left()) + " " + prefix + "JOIN "
                    + fromItemToSqlPretty(join.right())
                    + (join.on() != null ? " ON " + exprToSqlPretty(join.on(), 0) : "")
                    + (join.using() != null ? " USING (" + String.join(", ", join.using()) + ")" : "");
        }
        return fromItemToSql(item);
    }

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

    /**
     * An expression with parentheses only where precedence needs them.
     *
     * @param minPrecedence the binding strength of the context; anything looser gets a pair
     */
    private static String exprToSqlPretty(Expression expr, int minPrecedence) {
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            int prec = precedenceOf(bin.op());
            String text = comparedToSql(bin, bin.left(), bin.right(),
                            exprToSqlPretty(bin.left(), prec))
                    + " " + binOpToSql(bin.op()) + " "
                    // The right operand of a left-associative operator has to say so when it is
                    // another operator of the same strength: a - (b - c) is not a - b - c.
                    + comparedToSql(bin, bin.right(), bin.left(),
                            exprToSqlPretty(bin.right(), prec + 1));
            return prec < minPrecedence ? "(" + text + ")" : text;
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            if (un.op() == UnaryExpr.UnaryOp.NOT) {
                String text = "NOT " + exprToSqlPretty(un.operand(), 4);
                return 3 < minPrecedence ? "(" + text + ")" : text;
            }
            if (un.op() == UnaryExpr.UnaryOp.NEGATE) return "-" + exprToSqlPretty(un.operand(), 9);
            return exprToSql(expr);
        }
        if (expr instanceof IsNullExpr) {
            IsNullExpr isn = (IsNullExpr) expr;
            String text = exprToSqlPretty(isn.expr(), 5) + (isn.negated() ? " IS NOT NULL" : " IS NULL");
            return 4 < minPrecedence ? "(" + text + ")" : text;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            if (fn.star() || fn.distinct()) return exprToSql(expr);
            return fn.name() + "(" + fn.args().stream()
                    .map(a -> exprToSqlPretty(a, 0)).collect(Collectors.joining(", ")) + ")";
        }
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return exprToSqlPretty(cast.expr(), 9) + "::" + cast.typeName();
        }
        return exprToSql(expr);
    }

    private static String binOpToSql(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD:
                return "+";
            case SUBTRACT:
                return "-";
            case MULTIPLY:
                return "*";
            case DIVIDE:
                return "/";
            case MODULO:
                return "%";
            case EQUAL:
                return "=";
            case NOT_EQUAL:
                return "<>";
            case LESS_THAN:
                return "<";
            case GREATER_THAN:
                return ">";
            case LESS_EQUAL:
                return "<=";
            case GREATER_EQUAL:
                return ">=";
            case AND:
                return "AND";
            case OR:
                return "OR";
            case CONCAT:
                return "||";
            case LIKE:
                return "~~";
            case ILIKE:
                return "~~*";
            case JSON_ARROW:
            case JSON_SUBSCRIPT:
                return "->";
            case JSON_ARROW_TEXT:
                return "->>";
            case POWER:
                return "^";
            case CONTAINS:
                return "@>";
            case CONTAINED_BY:
                return "<@";
            case OVERLAP:
                return "&&";
            default:
                return op.name();
        }
    }

    private static String insertToSql(InsertStmt ins) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        if (ins.schema != null) sb.append(ins.schema).append(".");
        sb.append(ins.table);
        if (ins.columns != null && !ins.columns.isEmpty()) {
            sb.append(" (").append(String.join(", ", ins.columns)).append(")");
        }
        if (ins.selectStmt != null) {
            sb.append(" ").append(toSql(ins.selectStmt));
        } else if (ins.values != null && !ins.values.isEmpty()) {
            sb.append(" VALUES ");
            for (int i = 0; i < ins.values.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("(");
                List<Expression> row = ins.values.get(i);
                for (int j = 0; j < row.size(); j++) {
                    if (j > 0) sb.append(", ");
                    sb.append(exprToSql(row.get(j)));
                }
                sb.append(")");
            }
        } else {
            sb.append(" DEFAULT VALUES");
        }
        if (ins.returning != null && !ins.returning.isEmpty()) {
            sb.append(" RETURNING ");
            sb.append(ins.returning.stream().map(SqlUnparser::targetToSql).collect(Collectors.joining(", ")));
        }
        return sb.toString();
    }

    private static String updateToSql(UpdateStmt upd) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        if (upd.schema != null) sb.append(upd.schema).append(".");
        sb.append(upd.table);
        if (upd.alias != null) sb.append(" ").append(upd.alias);
        sb.append(" SET ");
        for (int i = 0; i < upd.setClauses.size(); i++) {
            if (i > 0) sb.append(", ");
            InsertStmt.SetClause sc = upd.setClauses.get(i);
            sb.append(sc.column()).append(" = ").append(exprToSql(sc.value()));
        }
        if (upd.from != null && !upd.from.isEmpty()) {
            sb.append(" FROM ");
            sb.append(upd.from.stream().map(SqlUnparser::fromItemToSql).collect(Collectors.joining(", ")));
        }
        if (upd.where != null) {
            sb.append(" WHERE ").append(exprToSql(upd.where));
        }
        if (upd.returning != null && !upd.returning.isEmpty()) {
            sb.append(" RETURNING ");
            sb.append(upd.returning.stream().map(SqlUnparser::targetToSql).collect(Collectors.joining(", ")));
        }
        return sb.toString();
    }

    private static String deleteToSql(DeleteStmt del) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        if (del.schema != null) sb.append(del.schema).append(".");
        sb.append(del.table);
        if (del.alias != null) sb.append(" ").append(del.alias);
        if (del.using != null && !del.using.isEmpty()) {
            sb.append(" USING ");
            sb.append(del.using.stream().map(SqlUnparser::fromItemToSql).collect(Collectors.joining(", ")));
        }
        if (del.where != null) {
            sb.append(" WHERE ").append(exprToSql(del.where));
        }
        if (del.returning != null && !del.returning.isEmpty()) {
            sb.append(" RETURNING ");
            sb.append(del.returning.stream().map(SqlUnparser::targetToSql).collect(Collectors.joining(", ")));
        }
        return sb.toString();
    }

    private static String caseToSql(CaseExpr c) {
        StringBuilder sb = new StringBuilder("CASE");
        if (c.operand() != null) sb.append(" ").append(exprToSql(c.operand()));
        for (CaseExpr.WhenClause w : c.whenClauses()) {
            sb.append(" WHEN ").append(exprToSql(w.condition())).append(" THEN ").append(exprToSql(w.result()));
        }
        if (c.elseExpr() != null) sb.append(" ELSE ").append(exprToSql(c.elseExpr()));
        sb.append(" END");
        return sb.toString();
    }
}
