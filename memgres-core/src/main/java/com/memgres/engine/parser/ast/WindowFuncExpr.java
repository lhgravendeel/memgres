package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * A window function expression: func(args) OVER (PARTITION BY ... ORDER BY ... frame)
 * Also used for aggregate functions used as window functions: SUM(x) OVER (...)
 */
public final class WindowFuncExpr implements Expression {
    public final String name;
    public final List<Expression> args;
    public final boolean distinct;
    public final boolean star;
    public final List<Expression> partitionBy;
    public final List<SelectStmt.OrderByItem> orderBy;
    public final FrameClause frame;
    public final String windowName;
    public final boolean ignoreNulls;
    public final boolean fromLast;
    public final Expression filter;

    public WindowFuncExpr(
            String name,
            List<Expression> args,
            boolean distinct,
            boolean star,
            List<Expression> partitionBy,
            List<SelectStmt.OrderByItem> orderBy,
            FrameClause frame,
            String windowName
    ) {
        this(name, args, distinct, star, partitionBy, orderBy, frame, windowName, false, false, null);
    }

    public WindowFuncExpr(
            String name,
            List<Expression> args,
            boolean distinct,
            boolean star,
            List<Expression> partitionBy,
            List<SelectStmt.OrderByItem> orderBy,
            FrameClause frame,
            String windowName,
            boolean ignoreNulls
    ) {
        this(name, args, distinct, star, partitionBy, orderBy, frame, windowName, ignoreNulls, false, null);
    }

    public WindowFuncExpr(
            String name,
            List<Expression> args,
            boolean distinct,
            boolean star,
            List<Expression> partitionBy,
            List<SelectStmt.OrderByItem> orderBy,
            FrameClause frame,
            String windowName,
            boolean ignoreNulls,
            boolean fromLast,
            Expression filter
    ) {
        this.name = name;
        this.args = args;
        this.distinct = distinct;
        this.star = star;
        this.partitionBy = partitionBy;
        this.orderBy = orderBy;
        this.frame = frame;
        this.windowName = windowName;
        this.ignoreNulls = ignoreNulls;
        this.fromLast = fromLast;
        this.filter = filter;
    }

    /**
     * Convenience constructor without windowName (backward compatibility).
     */
    public WindowFuncExpr(String name, List<Expression> args, boolean distinct, boolean star,
                           List<Expression> partitionBy, List<SelectStmt.OrderByItem> orderBy,
                           FrameClause frame) {
        this(name, args, distinct, star, partitionBy, orderBy, frame, null, false, false, null);
    }

    /**
     * Window frame specification: ROWS/RANGE BETWEEN start AND end
     */
        public static final class FrameClause {
        public final FrameType type;
        public final FrameBound start;
        public final FrameBound end;
        public final ExcludeMode excludeMode;

        public FrameClause(FrameType type, FrameBound start, FrameBound end) {
            this(type, start, end, null);
        }

        public FrameClause(FrameType type, FrameBound start, FrameBound end, ExcludeMode excludeMode) {
            this.type = type;
            this.start = start;
            this.end = end;
            this.excludeMode = excludeMode;
        }

        public FrameType type() { return type; }
        public FrameBound start() { return start; }
        public FrameBound end() { return end; }
        public ExcludeMode excludeMode() { return excludeMode; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FrameClause that = (FrameClause) o;
            return java.util.Objects.equals(type, that.type)
                && java.util.Objects.equals(start, that.start)
                && java.util.Objects.equals(end, that.end)
                && java.util.Objects.equals(excludeMode, that.excludeMode);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(type, start, end, excludeMode);
        }

        @Override
        public String toString() {
            return "FrameClause[type=" + type + ", " + "start=" + start + ", " + "end=" + end + ", " + "excludeMode=" + excludeMode + "]";
        }
    }

    public enum FrameType {
        ROWS, RANGE, GROUPS
    }

        public static final class FrameBound {
        public final FrameBoundType boundType;
        public final Expression offset;

        public FrameBound(FrameBoundType boundType, Expression offset) {
            this.boundType = boundType;
            this.offset = offset;
        }

        public FrameBoundType boundType() { return boundType; }
        public Expression offset() { return offset; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FrameBound that = (FrameBound) o;
            return java.util.Objects.equals(boundType, that.boundType)
                && java.util.Objects.equals(offset, that.offset);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(boundType, offset);
        }

        @Override
        public String toString() {
            return "FrameBound[boundType=" + boundType + ", " + "offset=" + offset + "]";
        }
    }

    public enum FrameBoundType {
        UNBOUNDED_PRECEDING,
        CURRENT_ROW,
        UNBOUNDED_FOLLOWING,
        PRECEDING,  // N PRECEDING
        FOLLOWING   // N FOLLOWING
    }

    public enum ExcludeMode {
        NO_OTHERS,     // EXCLUDE NO OTHERS (default)
        CURRENT_ROW,   // EXCLUDE CURRENT ROW
        TIES,          // EXCLUDE TIES
        GROUP          // EXCLUDE GROUP
    }

    public String name() { return name; }
    public List<Expression> args() { return args; }
    public boolean distinct() { return distinct; }
    public boolean star() { return star; }
    public List<Expression> partitionBy() { return partitionBy; }
    public List<SelectStmt.OrderByItem> orderBy() { return orderBy; }
    public FrameClause frame() { return frame; }
    public String windowName() { return windowName; }
    public boolean ignoreNulls() { return ignoreNulls; }
    public boolean fromLast() { return fromLast; }
    public Expression filter() { return filter; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowFuncExpr that = (WindowFuncExpr) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(args, that.args)
            && distinct == that.distinct
            && star == that.star
            && java.util.Objects.equals(partitionBy, that.partitionBy)
            && java.util.Objects.equals(orderBy, that.orderBy)
            && java.util.Objects.equals(frame, that.frame)
            && java.util.Objects.equals(windowName, that.windowName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, args, distinct, star, partitionBy, orderBy, frame, windowName);
    }

    @Override
    public String toString() {
        return "WindowFuncExpr[name=" + name + ", " + "args=" + args + ", " + "distinct=" + distinct + ", " + "star=" + star + ", " + "partitionBy=" + partitionBy + ", " + "orderBy=" + orderBy + ", " + "frame=" + frame + ", " + "windowName=" + windowName + "]";
    }
}
