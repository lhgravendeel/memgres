package com.memgres.engine.plpgsql;

import java.util.List;

/**
 * AST nodes for PL/pgSQL function bodies.
 * Expressions are stored as raw text strings to allow variable substitution at runtime.
 */
public interface PlpgsqlStatement {

    // Helper records (not PlpgsqlStatement)
        public static final class VarDeclaration {
        public final String name;
        public final String typeName;
        public final boolean constant;
        public final boolean notNull;
        public final String defaultExpr;
        public final boolean isCursor;
        public final String cursorQuery;

        public VarDeclaration(
                String name,
                String typeName,
                boolean constant,
                boolean notNull,
                String defaultExpr,
                boolean isCursor,
                String cursorQuery
        ) {
            this.name = name;
            this.typeName = typeName;
            this.constant = constant;
            this.notNull = notNull;
            this.defaultExpr = defaultExpr;
            this.isCursor = isCursor;
            this.cursorQuery = cursorQuery;
        }

        public String name() { return name; }
        public String typeName() { return typeName; }
        public boolean constant() { return constant; }
        public boolean notNull() { return notNull; }
        public String defaultExpr() { return defaultExpr; }
        public boolean isCursor() { return isCursor; }
        public String cursorQuery() { return cursorQuery; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VarDeclaration that = (VarDeclaration) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(typeName, that.typeName)
                && constant == that.constant
                && notNull == that.notNull
                && java.util.Objects.equals(defaultExpr, that.defaultExpr)
                && isCursor == that.isCursor
                && java.util.Objects.equals(cursorQuery, that.cursorQuery);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, typeName, constant, notNull, defaultExpr, isCursor, cursorQuery);
        }

        @Override
        public String toString() {
            return "VarDeclaration[name=" + name + ", " + "typeName=" + typeName + ", " + "constant=" + constant + ", " + "notNull=" + notNull + ", " + "defaultExpr=" + defaultExpr + ", " + "isCursor=" + isCursor + ", " + "cursorQuery=" + cursorQuery + "]";
        }
    }

        public static final class ElsifClause {
        public final String condition;
        public final List<PlpgsqlStatement> body;

        public ElsifClause(String condition, List<PlpgsqlStatement> body) {
            this.condition = condition;
            this.body = body;
        }

        public String condition() { return condition; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ElsifClause that = (ElsifClause) o;
            return java.util.Objects.equals(condition, that.condition)
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(condition, body);
        }

        @Override
        public String toString() {
            return "ElsifClause[condition=" + condition + ", " + "body=" + body + "]";
        }
    }

        public static final class ExceptionHandler {
        public final List<String> conditionNames;
        public final List<PlpgsqlStatement> body;

        public ExceptionHandler(List<String> conditionNames, List<PlpgsqlStatement> body) {
            this.conditionNames = conditionNames;
            this.body = body;
        }

        public List<String> conditionNames() { return conditionNames; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExceptionHandler that = (ExceptionHandler) o;
            return java.util.Objects.equals(conditionNames, that.conditionNames)
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(conditionNames, body);
        }

        @Override
        public String toString() {
            return "ExceptionHandler[conditionNames=" + conditionNames + ", " + "body=" + body + "]";
        }
    }

        public static final class DiagItem {
        public final String varName;
        public final String itemName;

        public DiagItem(String varName, String itemName) {
            this.varName = varName;
            this.itemName = itemName;
        }

        public String varName() { return varName; }
        public String itemName() { return itemName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DiagItem that = (DiagItem) o;
            return java.util.Objects.equals(varName, that.varName)
                && java.util.Objects.equals(itemName, that.itemName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(varName, itemName);
        }

        @Override
        public String toString() {
            return "DiagItem[varName=" + varName + ", " + "itemName=" + itemName + "]";
        }
    }

    // Statements

        public static final class Block implements PlpgsqlStatement {
        public final List<VarDeclaration> declarations;
        public final List<PlpgsqlStatement> body;
        public final List<ExceptionHandler> exceptionHandlers;

        public Block(List<VarDeclaration> declarations, List<PlpgsqlStatement> body, List<ExceptionHandler> exceptionHandlers) {
            this.declarations = declarations;
            this.body = body;
            this.exceptionHandlers = exceptionHandlers;
        }

        public List<VarDeclaration> declarations() { return declarations; }
        public List<PlpgsqlStatement> body() { return body; }
        public List<ExceptionHandler> exceptionHandlers() { return exceptionHandlers; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Block that = (Block) o;
            return java.util.Objects.equals(declarations, that.declarations)
                && java.util.Objects.equals(body, that.body)
                && java.util.Objects.equals(exceptionHandlers, that.exceptionHandlers);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(declarations, body, exceptionHandlers);
        }

        @Override
        public String toString() {
            return "Block[declarations=" + declarations + ", " + "body=" + body + ", " + "exceptionHandlers=" + exceptionHandlers + "]";
        }
    }

        public static final class Assignment implements PlpgsqlStatement {
        public final String target;
        public final String valueExpr;

        public Assignment(String target, String valueExpr) {
            this.target = target;
            this.valueExpr = valueExpr;
        }

        public String target() { return target; }
        public String valueExpr() { return valueExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Assignment that = (Assignment) o;
            return java.util.Objects.equals(target, that.target)
                && java.util.Objects.equals(valueExpr, that.valueExpr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(target, valueExpr);
        }

        @Override
        public String toString() {
            return "Assignment[target=" + target + ", " + "valueExpr=" + valueExpr + "]";
        }
    }

        public static final class IfStmt implements PlpgsqlStatement {
        public final String condition;
        public final List<PlpgsqlStatement> thenBody;
        public final List<ElsifClause> elsifClauses;
        public final List<PlpgsqlStatement> elseBody;

        public IfStmt(String condition, List<PlpgsqlStatement> thenBody, List<ElsifClause> elsifClauses, List<PlpgsqlStatement> elseBody) {
            this.condition = condition;
            this.thenBody = thenBody;
            this.elsifClauses = elsifClauses;
            this.elseBody = elseBody;
        }

        public String condition() { return condition; }
        public List<PlpgsqlStatement> thenBody() { return thenBody; }
        public List<ElsifClause> elsifClauses() { return elsifClauses; }
        public List<PlpgsqlStatement> elseBody() { return elseBody; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IfStmt that = (IfStmt) o;
            return java.util.Objects.equals(condition, that.condition)
                && java.util.Objects.equals(thenBody, that.thenBody)
                && java.util.Objects.equals(elsifClauses, that.elsifClauses)
                && java.util.Objects.equals(elseBody, that.elseBody);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(condition, thenBody, elsifClauses, elseBody);
        }

        @Override
        public String toString() {
            return "IfStmt[condition=" + condition + ", " + "thenBody=" + thenBody + ", " + "elsifClauses=" + elsifClauses + ", " + "elseBody=" + elseBody + "]";
        }
    }

        public static final class LoopStmt implements PlpgsqlStatement {
        public final String label;
        public final List<PlpgsqlStatement> body;

        public LoopStmt(String label, List<PlpgsqlStatement> body) {
            this.label = label;
            this.body = body;
        }

        public String label() { return label; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LoopStmt that = (LoopStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, body);
        }

        @Override
        public String toString() {
            return "LoopStmt[label=" + label + ", " + "body=" + body + "]";
        }
    }

        public static final class WhileStmt implements PlpgsqlStatement {
        public final String label;
        public final String condition;
        public final List<PlpgsqlStatement> body;

        public WhileStmt(String label, String condition, List<PlpgsqlStatement> body) {
            this.label = label;
            this.condition = condition;
            this.body = body;
        }

        public String label() { return label; }
        public String condition() { return condition; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WhileStmt that = (WhileStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(condition, that.condition)
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, condition, body);
        }

        @Override
        public String toString() {
            return "WhileStmt[label=" + label + ", " + "condition=" + condition + ", " + "body=" + body + "]";
        }
    }

        public static final class ForStmt implements PlpgsqlStatement {
        public final String label;
        public final String varName;
        public final String lower;
        public final String upper;
        public final String step;
        public final boolean reverse;
        public final List<PlpgsqlStatement> body;

        public ForStmt(
                String label,
                String varName,
                String lower,
                String upper,
                String step,
                boolean reverse,
                List<PlpgsqlStatement> body
        ) {
            this.label = label;
            this.varName = varName;
            this.lower = lower;
            this.upper = upper;
            this.step = step;
            this.reverse = reverse;
            this.body = body;
        }

        public String label() { return label; }
        public String varName() { return varName; }
        public String lower() { return lower; }
        public String upper() { return upper; }
        public String step() { return step; }
        public boolean reverse() { return reverse; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForStmt that = (ForStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(varName, that.varName)
                && java.util.Objects.equals(lower, that.lower)
                && java.util.Objects.equals(upper, that.upper)
                && java.util.Objects.equals(step, that.step)
                && reverse == that.reverse
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, varName, lower, upper, step, reverse, body);
        }

        @Override
        public String toString() {
            return "ForStmt[label=" + label + ", " + "varName=" + varName + ", " + "lower=" + lower + ", " + "upper=" + upper + ", " + "step=" + step + ", " + "reverse=" + reverse + ", " + "body=" + body + "]";
        }
    }

        public static final class ForQueryStmt implements PlpgsqlStatement {
        public final String label;
        public final String varName;
        public final String sql;
        public final List<PlpgsqlStatement> body;

        public ForQueryStmt(String label, String varName, String sql, List<PlpgsqlStatement> body) {
            this.label = label;
            this.varName = varName;
            this.sql = sql;
            this.body = body;
        }

        public String label() { return label; }
        public String varName() { return varName; }
        public String sql() { return sql; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForQueryStmt that = (ForQueryStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(varName, that.varName)
                && java.util.Objects.equals(sql, that.sql)
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, varName, sql, body);
        }

        @Override
        public String toString() {
            return "ForQueryStmt[label=" + label + ", " + "varName=" + varName + ", " + "sql=" + sql + ", " + "body=" + body + "]";
        }
    }

        public static final class ForeachStmt implements PlpgsqlStatement {
        public final String label;
        public final String varName;
        public final String arrayExpr;
        public final List<PlpgsqlStatement> body;

        public ForeachStmt(String label, String varName, String arrayExpr, List<PlpgsqlStatement> body) {
            this.label = label;
            this.varName = varName;
            this.arrayExpr = arrayExpr;
            this.body = body;
        }

        public String label() { return label; }
        public String varName() { return varName; }
        public String arrayExpr() { return arrayExpr; }
        public List<PlpgsqlStatement> body() { return body; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForeachStmt that = (ForeachStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(varName, that.varName)
                && java.util.Objects.equals(arrayExpr, that.arrayExpr)
                && java.util.Objects.equals(body, that.body);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, varName, arrayExpr, body);
        }

        @Override
        public String toString() {
            return "ForeachStmt[label=" + label + ", " + "varName=" + varName + ", " + "arrayExpr=" + arrayExpr + ", " + "body=" + body + "]";
        }
    }

        public static final class ExitStmt implements PlpgsqlStatement {
        public final String label;
        public final String whenCondition;

        public ExitStmt(String label, String whenCondition) {
            this.label = label;
            this.whenCondition = whenCondition;
        }

        public String label() { return label; }
        public String whenCondition() { return whenCondition; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExitStmt that = (ExitStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(whenCondition, that.whenCondition);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, whenCondition);
        }

        @Override
        public String toString() {
            return "ExitStmt[label=" + label + ", " + "whenCondition=" + whenCondition + "]";
        }
    }

        public static final class ContinueStmt implements PlpgsqlStatement {
        public final String label;
        public final String whenCondition;

        public ContinueStmt(String label, String whenCondition) {
            this.label = label;
            this.whenCondition = whenCondition;
        }

        public String label() { return label; }
        public String whenCondition() { return whenCondition; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ContinueStmt that = (ContinueStmt) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(whenCondition, that.whenCondition);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, whenCondition);
        }

        @Override
        public String toString() {
            return "ContinueStmt[label=" + label + ", " + "whenCondition=" + whenCondition + "]";
        }
    }

        public static final class ReturnStmt implements PlpgsqlStatement {
        public final String valueExpr;

        public ReturnStmt(String valueExpr) {
            this.valueExpr = valueExpr;
        }

        public String valueExpr() { return valueExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReturnStmt that = (ReturnStmt) o;
            return java.util.Objects.equals(valueExpr, that.valueExpr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(valueExpr);
        }

        @Override
        public String toString() {
            return "ReturnStmt[valueExpr=" + valueExpr + "]";
        }
    }

        public static final class ReturnNextStmt implements PlpgsqlStatement {
        public final String valueExpr;

        public ReturnNextStmt(String valueExpr) {
            this.valueExpr = valueExpr;
        }

        public String valueExpr() { return valueExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReturnNextStmt that = (ReturnNextStmt) o;
            return java.util.Objects.equals(valueExpr, that.valueExpr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(valueExpr);
        }

        @Override
        public String toString() {
            return "ReturnNextStmt[valueExpr=" + valueExpr + "]";
        }
    }

        public static final class ReturnQueryStmt implements PlpgsqlStatement {
        public final String sql;

        public ReturnQueryStmt(String sql) {
            this.sql = sql;
        }

        public String sql() { return sql; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReturnQueryStmt that = (ReturnQueryStmt) o;
            return java.util.Objects.equals(sql, that.sql);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql);
        }

        @Override
        public String toString() {
            return "ReturnQueryStmt[sql=" + sql + "]";
        }
    }

        public static final class RaiseStmt implements PlpgsqlStatement {
        public final String level;
        public final String format;
        public final List<String> argExprs;
        public final String errcode;
        public final String hint;

        public RaiseStmt(
                String level,
                String format,
                List<String> argExprs,
                String errcode,
                String hint
        ) {
            this.level = level;
            this.format = format;
            this.argExprs = argExprs;
            this.errcode = errcode;
            this.hint = hint;
        }

        /** Backwards-compatible constructor without hint. */
        RaiseStmt(String level, String format, List<String> argExprs, String errcode) {
            this(level, format, argExprs, errcode, null);
        }

        public String level() { return level; }
        public String format() { return format; }
        public List<String> argExprs() { return argExprs; }
        public String errcode() { return errcode; }
        public String hint() { return hint; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RaiseStmt that = (RaiseStmt) o;
            return java.util.Objects.equals(level, that.level)
                && java.util.Objects.equals(format, that.format)
                && java.util.Objects.equals(argExprs, that.argExprs)
                && java.util.Objects.equals(errcode, that.errcode)
                && java.util.Objects.equals(hint, that.hint);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(level, format, argExprs, errcode, hint);
        }

        @Override
        public String toString() {
            return "RaiseStmt[level=" + level + ", " + "format=" + format + ", " + "argExprs=" + argExprs + ", " + "errcode=" + errcode + ", " + "hint=" + hint + "]";
        }
    }

        public static final class PerformStmt implements PlpgsqlStatement {
        public final String sql;

        public PerformStmt(String sql) {
            this.sql = sql;
        }

        public String sql() { return sql; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PerformStmt that = (PerformStmt) o;
            return java.util.Objects.equals(sql, that.sql);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql);
        }

        @Override
        public String toString() {
            return "PerformStmt[sql=" + sql + "]";
        }
    }

        public static final class ExecuteStmt implements PlpgsqlStatement {
        public final String sqlExpr;
        public final List<String> usingExprs;
        public final List<String> intoVars;
        public final boolean strict;

        public ExecuteStmt(String sqlExpr, List<String> usingExprs, List<String> intoVars, boolean strict) {
            this.sqlExpr = sqlExpr;
            this.usingExprs = usingExprs;
            this.intoVars = intoVars;
            this.strict = strict;
        }

        public String sqlExpr() { return sqlExpr; }
        public List<String> usingExprs() { return usingExprs; }
        public List<String> intoVars() { return intoVars; }
        public boolean strict() { return strict; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExecuteStmt that = (ExecuteStmt) o;
            return java.util.Objects.equals(sqlExpr, that.sqlExpr)
                && java.util.Objects.equals(usingExprs, that.usingExprs)
                && java.util.Objects.equals(intoVars, that.intoVars)
                && strict == that.strict;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sqlExpr, usingExprs, intoVars, strict);
        }

        @Override
        public String toString() {
            return "ExecuteStmt[sqlExpr=" + sqlExpr + ", " + "usingExprs=" + usingExprs + ", " + "intoVars=" + intoVars + ", " + "strict=" + strict + "]";
        }
    }

        public static final class SqlStmt implements PlpgsqlStatement {
        public final String sql;
        public final List<String> intoVars;
        public final boolean strict;

        public SqlStmt(String sql, List<String> intoVars, boolean strict) {
            this.sql = sql;
            this.intoVars = intoVars;
            this.strict = strict;
        }

        public String sql() { return sql; }
        public List<String> intoVars() { return intoVars; }
        public boolean strict() { return strict; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SqlStmt that = (SqlStmt) o;
            return java.util.Objects.equals(sql, that.sql)
                && java.util.Objects.equals(intoVars, that.intoVars)
                && strict == that.strict;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql, intoVars, strict);
        }

        @Override
        public String toString() {
            return "SqlStmt[sql=" + sql + ", " + "intoVars=" + intoVars + ", " + "strict=" + strict + "]";
        }
    }

        public static final class NullStmt implements PlpgsqlStatement {
        public NullStmt() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "NullStmt[]";
        }
    }

        public static final class GetDiagnosticsStmt implements PlpgsqlStatement {
        public final List<DiagItem> items;
        public final boolean stacked;

        public GetDiagnosticsStmt(List<DiagItem> items) {
            this(items, false);
        }

        public GetDiagnosticsStmt(List<DiagItem> items, boolean stacked) {
            this.items = items;
            this.stacked = stacked;
        }

        public List<DiagItem> items() { return items; }
        public boolean stacked() { return stacked; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetDiagnosticsStmt that = (GetDiagnosticsStmt) o;
            return stacked == that.stacked && java.util.Objects.equals(items, that.items);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(items, stacked);
        }

        @Override
        public String toString() {
            return "GetDiagnosticsStmt[items=" + items + ", stacked=" + stacked + "]";
        }
    }

        public static final class OpenCursorStmt implements PlpgsqlStatement {
        public final String cursorName;
        public final String sql;

        public OpenCursorStmt(String cursorName, String sql) {
            this.cursorName = cursorName;
            this.sql = sql;
        }

        public String cursorName() { return cursorName; }
        public String sql() { return sql; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OpenCursorStmt that = (OpenCursorStmt) o;
            return java.util.Objects.equals(cursorName, that.cursorName)
                && java.util.Objects.equals(sql, that.sql);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(cursorName, sql);
        }

        @Override
        public String toString() {
            return "OpenCursorStmt[cursorName=" + cursorName + ", " + "sql=" + sql + "]";
        }
    }

        public static final class FetchStmt implements PlpgsqlStatement {
        public final String cursorName;
        public final List<String> intoVars;

        public FetchStmt(String cursorName, List<String> intoVars) {
            this.cursorName = cursorName;
            this.intoVars = intoVars;
        }

        public String cursorName() { return cursorName; }
        public List<String> intoVars() { return intoVars; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchStmt that = (FetchStmt) o;
            return java.util.Objects.equals(cursorName, that.cursorName)
                && java.util.Objects.equals(intoVars, that.intoVars);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(cursorName, intoVars);
        }

        @Override
        public String toString() {
            return "FetchStmt[cursorName=" + cursorName + ", " + "intoVars=" + intoVars + "]";
        }
    }

        public static final class CloseCursorStmt implements PlpgsqlStatement {
        public final String cursorName;

        public CloseCursorStmt(String cursorName) {
            this.cursorName = cursorName;
        }

        public String cursorName() { return cursorName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CloseCursorStmt that = (CloseCursorStmt) o;
            return java.util.Objects.equals(cursorName, that.cursorName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(cursorName);
        }

        @Override
        public String toString() {
            return "CloseCursorStmt[cursorName=" + cursorName + "]";
        }
    }
}
