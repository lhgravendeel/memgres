package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * SELECT [DISTINCT] targets FROM sources [WHERE ...] [GROUP BY ...] [HAVING ...]
 * [ORDER BY ...] [LIMIT n] [OFFSET n]
 *
 * Optionally preceded by WITH clause (CTEs).
 */
public final class SelectStmt implements Statement {
    public final boolean distinct;
    public final List<Expression> distinctOn;
    public final List<SelectTarget> targets;
    public final List<FromItem> from;
    public final Expression where;
    public final List<Expression> groupBy;
    public final Expression having;
    public final List<WindowDef> windowDefs;
    public final List<OrderByItem> orderBy;
    public final Expression limit;
    public final Expression offset;
    public final List<CommonTableExpr> withClauses;
    public final List<List<Expression>> groupingSets;
    public final LockClause lockClause;
    public final boolean withTies;

    public SelectStmt(
            boolean distinct,
            List<Expression> distinctOn,
            List<SelectTarget> targets,
            List<FromItem> from,
            Expression where,
            List<Expression> groupBy,
            Expression having,
            List<WindowDef> windowDefs,
            List<OrderByItem> orderBy,
            Expression limit,
            Expression offset,
            List<CommonTableExpr> withClauses,
            List<List<Expression>> groupingSets,
            LockClause lockClause,
            boolean withTies
    ) {
        this.distinct = distinct;
        this.distinctOn = distinctOn;
        this.targets = targets;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windowDefs = windowDefs;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
        this.withClauses = withClauses;
        this.groupingSets = groupingSets;
        this.lockClause = lockClause;
        this.withTies = withTies;
    }

    public SelectStmt(
            boolean distinct,
            List<Expression> distinctOn,
            List<SelectTarget> targets,
            List<FromItem> from,
            Expression where,
            List<Expression> groupBy,
            Expression having,
            List<WindowDef> windowDefs,
            List<OrderByItem> orderBy,
            Expression limit,
            Expression offset,
            List<CommonTableExpr> withClauses,
            List<List<Expression>> groupingSets,
            LockClause lockClause
    ) {
        this(distinct, distinctOn, targets, from, where, groupBy, having, windowDefs, orderBy, limit, offset, withClauses, groupingSets, lockClause, false);
    }

    /** Canonical constructor without lockClause (backward compatibility). */
    public SelectStmt(boolean distinct, List<Expression> distinctOn, List<SelectTarget> targets,
                       List<FromItem> from, Expression where, List<Expression> groupBy,
                       Expression having, List<WindowDef> windowDefs, List<OrderByItem> orderBy,
                       Expression limit, Expression offset, List<CommonTableExpr> withClauses,
                       List<List<Expression>> groupingSets) {
        this(distinct, distinctOn, targets, from, where, groupBy, having, windowDefs, orderBy, limit, offset, withClauses, groupingSets, null);
    }

    /**
     * Convenience constructor without distinctOn and WITH clauses (backward compatibility).
     */
    public SelectStmt(boolean distinct, List<SelectTarget> targets, List<FromItem> from,
                       Expression where, List<Expression> groupBy, Expression having,
                       List<OrderByItem> orderBy, Expression limit, Expression offset) {
        this(distinct, null, targets, from, where, groupBy, having, null, orderBy, limit, offset, null, null, null);
    }

    /**
     * Convenience constructor without distinctOn (backward compatibility).
     */
    public SelectStmt(boolean distinct, List<SelectTarget> targets, List<FromItem> from,
                       Expression where, List<Expression> groupBy, Expression having,
                       List<OrderByItem> orderBy, Expression limit, Expression offset,
                       List<CommonTableExpr> withClauses) {
        this(distinct, null, targets, from, where, groupBy, having, null, orderBy, limit, offset, withClauses, null, null);
    }

    /**
     * Row-level lock clause: FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE
     * with optional NOWAIT or SKIP LOCKED modifiers.
     */
        public static final class LockClause {
        public final String mode;
        public final boolean nowait;
        public final boolean skipLocked;

        public LockClause(String mode, boolean nowait, boolean skipLocked) {
            this.mode = mode;
            this.nowait = nowait;
            this.skipLocked = skipLocked;
        }

        public String mode() { return mode; }
        public boolean nowait() { return nowait; }
        public boolean skipLocked() { return skipLocked; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockClause that = (LockClause) o;
            return java.util.Objects.equals(mode, that.mode)
                && nowait == that.nowait
                && skipLocked == that.skipLocked;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(mode, nowait, skipLocked);
        }

        @Override
        public String toString() {
            return "LockClause[mode=" + mode + ", " + "nowait=" + nowait + ", " + "skipLocked=" + skipLocked + "]";
        }
    }

    /**
     * A single item in the SELECT list: expression [AS alias]
     */
        public static final class SelectTarget {
        public final Expression expr;
        public final String alias;

        public SelectTarget(Expression expr, String alias) {
            this.expr = expr;
            this.alias = alias;
        }

        public Expression expr() { return expr; }
        public String alias() { return alias; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SelectTarget that = (SelectTarget) o;
            return java.util.Objects.equals(expr, that.expr)
                && java.util.Objects.equals(alias, that.alias);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(expr, alias);
        }

        @Override
        public String toString() {
            return "SelectTarget[expr=" + expr + ", " + "alias=" + alias + "]";
        }
    }

    /**
     * An item in the ORDER BY clause.
     */
        public static final class OrderByItem {
        public final Expression expr;
        public final boolean descending;
        public final Boolean nullsFirst;

        public OrderByItem(Expression expr, boolean descending, Boolean nullsFirst) {
            this.expr = expr;
            this.descending = descending;
            this.nullsFirst = nullsFirst;
        }

        public Expression expr() { return expr; }
        public boolean descending() { return descending; }
        public Boolean nullsFirst() { return nullsFirst; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderByItem that = (OrderByItem) o;
            return java.util.Objects.equals(expr, that.expr)
                && descending == that.descending
                && java.util.Objects.equals(nullsFirst, that.nullsFirst);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(expr, descending, nullsFirst);
        }

        @Override
        public String toString() {
            return "OrderByItem[expr=" + expr + ", " + "descending=" + descending + ", " + "nullsFirst=" + nullsFirst + "]";
        }
    }

    /**
     * A source in the FROM clause.
     */
    public interface FromItem {}

    /**
     * A table reference: [ONLY] [schema.]table [AS alias]
     */
        public static final class TableRef implements FromItem {
        public final String schema;
        public final String table;
        public final String alias;
        public final boolean only;

        public TableRef(String schema, String table, String alias, boolean only) {
            this.schema = schema;
            this.table = table;
            this.alias = alias;
            this.only = only;
        }

        public TableRef(String table) {
            this(null, table, null, false);
        }

        public TableRef(String table, String alias) {
            this(null, table, alias, false);
        }

        public TableRef(String schema, String table, String alias) {
            this(schema, table, alias, false);
        }

        public String schema() { return schema; }
        public String table() { return table; }
        public String alias() { return alias; }
        public boolean only() { return only; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableRef that = (TableRef) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(table, that.table)
                && java.util.Objects.equals(alias, that.alias)
                && only == that.only;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, table, alias, only);
        }

        @Override
        public String toString() {
            return "TableRef[schema=" + schema + ", " + "table=" + table + ", " + "alias=" + alias + ", " + "only=" + only + "]";
        }
    }

    /**
     * A subquery in FROM: [LATERAL] (SELECT|VALUES ...) AS alias [(col1, col2, ...)]
     */
        public static final class SubqueryFrom implements FromItem {
        public final Statement subquery;
        public final String alias;
        public final boolean lateral;
        public final List<String> columnAliases;

        public SubqueryFrom(Statement subquery, String alias, boolean lateral, List<String> columnAliases) {
            this.subquery = subquery;
            this.alias = alias;
            this.lateral = lateral;
            this.columnAliases = columnAliases;
        }

        public SubqueryFrom(SelectStmt subquery, String alias) {
            this((Statement) subquery, alias, false, null);
        }

        public SubqueryFrom(SelectStmt subquery, String alias, boolean lateral) {
            this((Statement) subquery, alias, lateral, null);
        }

        public Statement subquery() { return subquery; }
        public String alias() { return alias; }
        public boolean lateral() { return lateral; }
        public List<String> columnAliases() { return columnAliases; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SubqueryFrom that = (SubqueryFrom) o;
            return java.util.Objects.equals(subquery, that.subquery)
                && java.util.Objects.equals(alias, that.alias)
                && lateral == that.lateral
                && java.util.Objects.equals(columnAliases, that.columnAliases);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(subquery, alias, lateral, columnAliases);
        }

        @Override
        public String toString() {
            return "SubqueryFrom[subquery=" + subquery + ", " + "alias=" + alias + ", " + "lateral=" + lateral + ", " + "columnAliases=" + columnAliases + "]";
        }
    }

    /**
     * A JOIN clause.
     */
        public static final class JoinFrom implements FromItem {
        public final FromItem left;
        public final JoinType joinType;
        public final FromItem right;
        public final Expression on;
        public final List<String> using;

        public JoinFrom(
                FromItem left,
                JoinType joinType,
                FromItem right,
                Expression on,
                List<String> using
        ) {
            this.left = left;
            this.joinType = joinType;
            this.right = right;
            this.on = on;
            this.using = using;
        }

        public FromItem left() { return left; }
        public JoinType joinType() { return joinType; }
        public FromItem right() { return right; }
        public Expression on() { return on; }
        public List<String> using() { return using; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinFrom that = (JoinFrom) o;
            return java.util.Objects.equals(left, that.left)
                && java.util.Objects.equals(joinType, that.joinType)
                && java.util.Objects.equals(right, that.right)
                && java.util.Objects.equals(on, that.on)
                && java.util.Objects.equals(using, that.using);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(left, joinType, right, on, using);
        }

        @Override
        public String toString() {
            return "JoinFrom[left=" + left + ", " + "joinType=" + joinType + ", " + "right=" + right + ", " + "on=" + on + ", " + "using=" + using + "]";
        }
    }

    /**
     * A set-returning function call in FROM: generate_series(1, 5) AS alias
     */
        public static final class FunctionFrom implements FromItem {
        public final String functionName;
        public final List<Expression> args;
        public final String alias;
        public final List<String> columnAliases;

        public FunctionFrom(String functionName, List<Expression> args, String alias, List<String> columnAliases) {
            this.functionName = functionName;
            this.args = args;
            this.alias = alias;
            this.columnAliases = columnAliases;
        }

        public FunctionFrom(String functionName, List<Expression> args, String alias) {
            this(functionName, args, alias, null);
        }

        public String functionName() { return functionName; }
        public List<Expression> args() { return args; }
        public String alias() { return alias; }
        public List<String> columnAliases() { return columnAliases; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FunctionFrom that = (FunctionFrom) o;
            return java.util.Objects.equals(functionName, that.functionName)
                && java.util.Objects.equals(args, that.args)
                && java.util.Objects.equals(alias, that.alias)
                && java.util.Objects.equals(columnAliases, that.columnAliases);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(functionName, args, alias, columnAliases);
        }

        @Override
        public String toString() {
            return "FunctionFrom[functionName=" + functionName + ", " + "args=" + args + ", " + "alias=" + alias + ", " + "columnAliases=" + columnAliases + "]";
        }
    }

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, NATURAL_LEFT, NATURAL_RIGHT, NATURAL_FULL
    }

    /**
     * A Common Table Expression: name [(col1, col2, ...)] AS (SELECT ...)
     */
        public static final class CommonTableExpr {
        public final String name;
        public final List<String> columnNames;
        public final Statement query;
        public final boolean recursive;
        public final String searchColumn;
        public final boolean searchDepthFirst;
        public final List<String> searchByColumns;
        public final String cycleColumn;
        public final String cyclePathColumn;
        public final List<String> cycleByColumns;

        public CommonTableExpr(
                String name,
                List<String> columnNames,
                Statement query,
                boolean recursive,
                String searchColumn,
                boolean searchDepthFirst,
                List<String> searchByColumns,
                String cycleColumn,
                String cyclePathColumn,
                List<String> cycleByColumns
        ) {
            this.name = name;
            this.columnNames = columnNames;
            this.query = query;
            this.recursive = recursive;
            this.searchColumn = searchColumn;
            this.searchDepthFirst = searchDepthFirst;
            this.searchByColumns = searchByColumns;
            this.cycleColumn = cycleColumn;
            this.cyclePathColumn = cyclePathColumn;
            this.cycleByColumns = cycleByColumns;
        }

        public CommonTableExpr(
                String name,
                List<String> columnNames,
                Statement query,
                boolean recursive,
                String searchColumn,
                boolean searchDepthFirst,
                List<String> searchByColumns,
                String cycleColumn,
                String cyclePathColumn
        ) {
            this(name, columnNames, query, recursive, searchColumn, searchDepthFirst, searchByColumns, cycleColumn, cyclePathColumn, null);
        }

        /** Compact constructor for backward compat */
        public CommonTableExpr(String name, List<String> columnNames, Statement query, boolean recursive) {
            this(name, columnNames, query, recursive, null, false, null, null, null, null);
        }

        public String name() { return name; }
        public List<String> columnNames() { return columnNames; }
        public Statement query() { return query; }
        public boolean recursive() { return recursive; }
        public String searchColumn() { return searchColumn; }
        public boolean searchDepthFirst() { return searchDepthFirst; }
        public List<String> searchByColumns() { return searchByColumns; }
        public String cycleColumn() { return cycleColumn; }
        public String cyclePathColumn() { return cyclePathColumn; }
        public List<String> cycleByColumns() { return cycleByColumns; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommonTableExpr that = (CommonTableExpr) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(columnNames, that.columnNames)
                && java.util.Objects.equals(query, that.query)
                && recursive == that.recursive
                && java.util.Objects.equals(searchColumn, that.searchColumn)
                && searchDepthFirst == that.searchDepthFirst
                && java.util.Objects.equals(searchByColumns, that.searchByColumns)
                && java.util.Objects.equals(cycleColumn, that.cycleColumn)
                && java.util.Objects.equals(cyclePathColumn, that.cyclePathColumn);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, columnNames, query, recursive, searchColumn, searchDepthFirst, searchByColumns, cycleColumn, cyclePathColumn);
        }

        @Override
        public String toString() {
            return "CommonTableExpr[name=" + name + ", columnNames=" + columnNames + ", query=" + query
                + ", recursive=" + recursive + ", searchColumn=" + searchColumn + ", searchDepthFirst=" + searchDepthFirst
                + ", searchByColumns=" + searchByColumns + ", cycleColumn=" + cycleColumn + ", cyclePathColumn=" + cyclePathColumn + "]";
        }
    }

    /**
     * A named window definition: WINDOW name AS (partition_by, order_by, frame)
     */
        public static final class WindowDef {
        public final String name;
        public final List<Expression> partitionBy;
        public final List<OrderByItem> orderBy;
        public final WindowFuncExpr.FrameClause frame;

        public WindowDef(String name, List<Expression> partitionBy, List<OrderByItem> orderBy, WindowFuncExpr.FrameClause frame) {
            this.name = name;
            this.partitionBy = partitionBy;
            this.orderBy = orderBy;
            this.frame = frame;
        }

        public String name() { return name; }
        public List<Expression> partitionBy() { return partitionBy; }
        public List<OrderByItem> orderBy() { return orderBy; }
        public WindowFuncExpr.FrameClause frame() { return frame; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WindowDef that = (WindowDef) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(partitionBy, that.partitionBy)
                && java.util.Objects.equals(orderBy, that.orderBy)
                && java.util.Objects.equals(frame, that.frame);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, partitionBy, orderBy, frame);
        }

        @Override
        public String toString() {
            return "WindowDef[name=" + name + ", " + "partitionBy=" + partitionBy + ", " + "orderBy=" + orderBy + ", " + "frame=" + frame + "]";
        }
    }

    public boolean distinct() { return distinct; }
    public List<Expression> distinctOn() { return distinctOn; }
    public List<SelectTarget> targets() { return targets; }
    public List<FromItem> from() { return from; }
    public Expression where() { return where; }
    public List<Expression> groupBy() { return groupBy; }
    public Expression having() { return having; }
    public List<WindowDef> windowDefs() { return windowDefs; }
    public List<OrderByItem> orderBy() { return orderBy; }
    public Expression limit() { return limit; }
    public Expression offset() { return offset; }
    public List<CommonTableExpr> withClauses() { return withClauses; }
    public List<List<Expression>> groupingSets() { return groupingSets; }
    public LockClause lockClause() { return lockClause; }
    public boolean withTies() { return withTies; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SelectStmt that = (SelectStmt) o;
        return distinct == that.distinct
            && java.util.Objects.equals(distinctOn, that.distinctOn)
            && java.util.Objects.equals(targets, that.targets)
            && java.util.Objects.equals(from, that.from)
            && java.util.Objects.equals(where, that.where)
            && java.util.Objects.equals(groupBy, that.groupBy)
            && java.util.Objects.equals(having, that.having)
            && java.util.Objects.equals(windowDefs, that.windowDefs)
            && java.util.Objects.equals(orderBy, that.orderBy)
            && java.util.Objects.equals(limit, that.limit)
            && java.util.Objects.equals(offset, that.offset)
            && java.util.Objects.equals(withClauses, that.withClauses)
            && java.util.Objects.equals(groupingSets, that.groupingSets)
            && java.util.Objects.equals(lockClause, that.lockClause);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(distinct, distinctOn, targets, from, where, groupBy, having, windowDefs, orderBy, limit, offset, withClauses, groupingSets, lockClause);
    }

    @Override
    public String toString() {
        return "SelectStmt[distinct=" + distinct + ", " + "distinctOn=" + distinctOn + ", " + "targets=" + targets + ", " + "from=" + from + ", " + "where=" + where + ", " + "groupBy=" + groupBy + ", " + "having=" + having + ", " + "windowDefs=" + windowDefs + ", " + "orderBy=" + orderBy + ", " + "limit=" + limit + ", " + "offset=" + offset + ", " + "withClauses=" + withClauses + ", " + "groupingSets=" + groupingSets + ", " + "lockClause=" + lockClause + "]";
    }
}
