package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Orchestrator for DDL (Data Definition Language) and admin statement execution.
 * Delegates to specialized executors for each category of DDL operation.
 */
class DdlExecutor {

    final AstExecutor executor;

    // Delegate executors
    final DdlTableExecutor tableExecutor;
    private final DdlAlterTableExecutor alterTableExecutor;
    private final DdlObjectExecutor objectExecutor;
    private final DdlViewExecutor viewExecutor;
    private final DdlAdminExecutor adminExecutor;

    DdlExecutor(AstExecutor executor) {
        this.executor = executor;
        this.tableExecutor = new DdlTableExecutor(this);
        this.alterTableExecutor = new DdlAlterTableExecutor(this);
        this.objectExecutor = new DdlObjectExecutor(this);
        this.viewExecutor = new DdlViewExecutor(this);
        this.adminExecutor = new DdlAdminExecutor(this);
    }

    // ---- Delegation methods ----

    QueryResult executeCreateTable(CreateTableStmt stmt) { return tableExecutor.executeCreateTable(stmt); }
    QueryResult executeDropTable(DropTableStmt stmt) { return tableExecutor.executeDropTable(stmt); }
    QueryResult executeTruncate(TruncateStmt stmt) { return tableExecutor.executeTruncate(stmt); }
    QueryResult executeCreateTableAs(CreateTableAsStmt stmt) { return tableExecutor.executeCreateTableAs(stmt); }

    QueryResult executeAlterTable(AlterTableStmt stmt) { return alterTableExecutor.executeAlterTable(stmt); }

    QueryResult executeCreateType(CreateTypeStmt stmt) { return objectExecutor.executeCreateType(stmt); }
    QueryResult executeAlterType(AlterTypeStmt stmt) { return objectExecutor.executeAlterType(stmt); }
    QueryResult executeCreateFunction(CreateFunctionStmt stmt) { return objectExecutor.executeCreateFunction(stmt); }
    QueryResult executeCall(CallStmt stmt) { return objectExecutor.executeCall(stmt); }
    QueryResult executeCreateTrigger(CreateTriggerStmt stmt) { return objectExecutor.executeCreateTrigger(stmt); }
    QueryResult executeDropStmt(DropStmt stmt) { return objectExecutor.executeDropStmt(stmt); }
    QueryResult executeCreateSequence(CreateSequenceStmt stmt) { return objectExecutor.executeCreateSequence(stmt); }
    QueryResult executeAlterSequence(AlterSequenceStmt stmt) { return objectExecutor.executeAlterSequence(stmt); }
    QueryResult executeCreateDomain(CreateDomainStmt stmt) { return objectExecutor.executeCreateDomain(stmt); }
    QueryResult executeAlterDomain(AlterDomainStmt stmt) { return objectExecutor.executeAlterDomain(stmt); }
    QueryResult executeCreateIndex(CreateIndexStmt stmt) { return objectExecutor.executeCreateIndex(stmt); }
    QueryResult executeCreateAggregate(CreateAggregateStmt stmt) { return objectExecutor.executeCreateAggregate(stmt); }
    QueryResult executeCreateOperator(CreateOperatorStmt stmt) { return objectExecutor.executeCreateOperator(stmt); }
    QueryResult executeCreateOperatorFamily(CreateOperatorFamilyStmt stmt) { return objectExecutor.executeCreateOperatorFamily(stmt); }
    QueryResult executeCreateOperatorClass(CreateOperatorClassStmt stmt) { return objectExecutor.executeCreateOperatorClass(stmt); }
    QueryResult executeAlterOperator(AlterOperatorStmt stmt) { return objectExecutor.executeAlterOperator(stmt); }

    QueryResult executeCreateView(CreateViewStmt stmt) { return viewExecutor.executeCreateView(stmt); }
    QueryResult executeAlterView(AlterViewStmt stmt) { return viewExecutor.executeAlterView(stmt); }
    QueryResult executeRefreshMaterializedView(RefreshMaterializedViewStmt stmt) { return viewExecutor.executeRefreshMaterializedView(stmt); }

    QueryResult executeCreateRule(CreateRuleStmt stmt) { return adminExecutor.executeCreateRule(stmt); }
    QueryResult executeCreateSchema(CreateSchemaStmt stmt) { return adminExecutor.executeCreateSchema(stmt); }
    QueryResult executeTransaction(TransactionStmt stmt) { return adminExecutor.executeTransaction(stmt); }
    QueryResult executeExplain(ExplainStmt stmt) { return adminExecutor.executeExplain(stmt); }
    QueryResult executeListen(ListenStmt stmt) { return adminExecutor.executeListen(stmt); }
    QueryResult executeNotify(NotifyStmt stmt) { return adminExecutor.executeNotify(stmt); }
    QueryResult executeUnlisten(UnlistenStmt stmt) { return adminExecutor.executeUnlisten(stmt); }
    QueryResult executeCreatePolicy(CreatePolicyStmt stmt) { return adminExecutor.executeCreatePolicy(stmt); }
    QueryResult executeAlterPolicy(AlterPolicyStmt stmt) { return adminExecutor.executeAlterPolicy(stmt); }
    QueryResult executeCreateRole(CreateRoleStmt stmt) { return adminExecutor.executeCreateRole(stmt); }
    QueryResult executeAlterRole(AlterRoleStmt stmt) { return adminExecutor.executeAlterRole(stmt); }
    QueryResult executeDropRole(DropRoleStmt stmt) { return adminExecutor.executeDropRole(stmt); }
    void executeDropOwned(String roleName) { adminExecutor.executeDropOwned(roleName); }

    // ---- Shared helpers used by multiple delegates ----

    /** Throws if the effective schema is pg_catalog or information_schema. */
    void checkPgCatalogWriteProtection() {
        String effectiveSchema = executor.defaultSchema();
        if ("pg_catalog".equalsIgnoreCase(effectiveSchema) || "information_schema".equalsIgnoreCase(effectiveSchema)) {
            throw new MemgresException("permission denied for schema " + effectiveSchema, "42501");
        }
    }

    /** Resolve owner name, handling current_user/session_user/current_role. */
    String resolveOwnerName(String name) {
        if ("current_user".equalsIgnoreCase(name) || "session_user".equalsIgnoreCase(name)
                || "current_role".equalsIgnoreCase(name)) {
            return executor.sessionUser();
        }
        return name;
    }

    /** Resolve a table by name without throwing. Searches default schema first, then all schemas. */
    Table resolveTableOrNull(String name) {
        String defSchema = executor.defaultSchema();
        if (defSchema != null) {
            Schema ds = executor.database.getSchema(defSchema);
            if (ds != null) {
                Table t = ds.getTable(name);
                if (t != null) return t;
            }
        }
        Schema pub = executor.database.getSchema("public");
        if (pub != null) {
            Table t = pub.getTable(name);
            if (t != null) return t;
        }
        for (Schema schema : executor.database.getSchemas().values()) {
            Table t = schema.getTable(name);
            if (t != null) return t;
        }
        return null;
    }

    /** Convert a TableConstraint AST node to a StoredConstraint. */
    StoredConstraint convertTableConstraint(String tableName, TableConstraint tc) {
        String name = tc.name();
        switch (tc.type()) {
            case PRIMARY_KEY: {
                if (name == null) name = tableName + "_pkey";
                StoredConstraint pk = StoredConstraint.primaryKey(name, resolveConstraintColumns(tc.columns()));
                if (tc.deferrable()) {
                    pk.setDeferrable(true);
                    pk.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return pk;
            }
            case UNIQUE: {
                List<String> cols = resolveConstraintColumns(tc.columns());
                if (name == null) name = tableName + "_" + String.join("_", cols) + "_key";
                StoredConstraint sc = StoredConstraint.unique(name, cols);
                if (tc.nullsNotDistinct()) sc.setNullsNotDistinct(true);
                if (tc.deferrable()) {
                    sc.setDeferrable(true);
                    sc.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return sc;
            }
            case CHECK: {
                if (name == null) name = tableName + "_check";
                StoredConstraint chk = StoredConstraint.check(name, tc.checkExpr());
                if (tc.notEnforced()) chk.setNotEnforced(true);
                if (tc.deferrable()) {
                    chk.setDeferrable(true);
                    chk.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return chk;
            }
            case FOREIGN_KEY: {
                if (name == null) name = tableName + "_" + String.join("_", tc.columns()) + "_fkey";
                StoredConstraint fk = StoredConstraint.foreignKey(name, tc.columns(),
                        tc.referencesTable(), tc.referencesColumns(),
                        StoredConstraint.parseFkAction(tc.onDelete()),
                        StoredConstraint.parseFkAction(tc.onUpdate()));
                if (tc.deferrable()) {
                    fk.setDeferrable(true);
                    fk.setInitiallyDeferred(tc.initiallyDeferred());
                }
                if (tc.notEnforced()) fk.setNotEnforced(true);
                return fk;
            }
            case EXCLUDE: {
                if (name == null) name = tableName + "_excl";
                StoredConstraint excl = new StoredConstraint(name, StoredConstraint.Type.EXCLUDE,
                        tc.columns(), null, null, null, null, null);
                if (tc.excludeElements() != null) {
                    excl.setExcludeElements(tc.excludeElements().stream()
                            .map(e -> new StoredConstraint.ExcludeElement(e.column(), e.operator()))
                            .collect(Collectors.toList()));
                }
                if (tc.deferrable()) {
                    excl.setDeferrable(true);
                    excl.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return excl;
            }
            case NOT_NULL:
                return null;
            default:
                throw new IllegalStateException("Unknown constraint type: " + tc.type());
        }
    }

    /** Recursively validate that all column references in an expression exist in the given table. */
    void validateExprColumnRefs(Expression expr, Table table, String newColName) {
        if (expr == null) return;
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            String col = ref.column();
            if (table.getColumnIndex(col) < 0 && !col.equalsIgnoreCase(newColName)) {
                throw new MemgresException("column \"" + col + "\" does not exist", "42703");
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            validateExprColumnRefs(bin.left(), table, newColName);
            validateExprColumnRefs(bin.right(), table, newColName);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) validateExprColumnRefs(cop.left(), table, newColName);
            validateExprColumnRefs(cop.right(), table, newColName);
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            validateExprColumnRefs(un.operand(), table, newColName);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            if (fn.args() != null) {
                for (Expression arg : fn.args()) validateExprColumnRefs(arg, table, newColName);
            }
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            validateExprColumnRefs(cast.expr(), table, newColName);
        }
    }

    // ---- DRY: Shared type resolution ----

    /** Result of resolving a column type name. */
        public static final class ResolvedType {
        public final DataType dataType;
        public final String enumTypeName;
        public final String domainTypeName;
        public final String compositeTypeName;
        public final DataType arrayElementType;
        public final boolean domainNotNull;

        public ResolvedType(
                DataType dataType,
                String enumTypeName,
                String domainTypeName,
                String compositeTypeName,
                DataType arrayElementType,
                boolean domainNotNull
        ) {
            this.dataType = dataType;
            this.enumTypeName = enumTypeName;
            this.domainTypeName = domainTypeName;
            this.compositeTypeName = compositeTypeName;
            this.arrayElementType = arrayElementType;
            this.domainNotNull = domainNotNull;
        }

        public DataType dataType() { return dataType; }
        public String enumTypeName() { return enumTypeName; }
        public String domainTypeName() { return domainTypeName; }
        public String compositeTypeName() { return compositeTypeName; }
        public DataType arrayElementType() { return arrayElementType; }
        public boolean domainNotNull() { return domainNotNull; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedType that = (ResolvedType) o;
            return java.util.Objects.equals(dataType, that.dataType)
                && java.util.Objects.equals(enumTypeName, that.enumTypeName)
                && java.util.Objects.equals(domainTypeName, that.domainTypeName)
                && java.util.Objects.equals(compositeTypeName, that.compositeTypeName)
                && java.util.Objects.equals(arrayElementType, that.arrayElementType)
                && domainNotNull == that.domainNotNull;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(dataType, enumTypeName, domainTypeName, compositeTypeName, arrayElementType, domainNotNull);
        }

        @Override
        public String toString() {
            return "ResolvedType[dataType=" + dataType + ", " + "enumTypeName=" + enumTypeName + ", " + "domainTypeName=" + domainTypeName + ", " + "compositeTypeName=" + compositeTypeName + ", " + "arrayElementType=" + arrayElementType + ", " + "domainNotNull=" + domainNotNull + "]";
        }
    }

    /**
     * Resolve a type name string to a DataType, handling enums, domains, composites, and arrays.
     * Used by CREATE TABLE and ALTER TABLE ADD COLUMN.
     */
    ResolvedType resolveColumnType(String typeName, Integer precision) {
        String fullTypeName = typeName.replaceAll("\\(.*\\)", "").trim();
        boolean isArray = fullTypeName.endsWith("[]");
        DataType arrayElementType = null;
        String baseType = fullTypeName.replace("[]", "").trim();
        if (isArray) {
            try { arrayElementType = DataType.fromPgName(baseType); } catch (Exception ignored) {}
        }

        DataType dataType;
        if (isArray) {
            DataType arrayDataType = DataType.fromPgName(fullTypeName);
            dataType = (arrayDataType != null) ? arrayDataType : DataType.fromPgName(baseType);
        } else {
            dataType = DataType.fromPgName(baseType);
        }
        // FLOAT(p): p <= 24 -> REAL, p >= 25 -> DOUBLE PRECISION
        if (baseType.equalsIgnoreCase("float") && precision != null && precision <= 24) {
            dataType = DataType.REAL;
        }

        String enumTypeName = null;
        String domainTypeName = null;
        String compositeTypeName = null;
        boolean domainNotNull = false;

        if (dataType == null) {
            if (executor.database.isCustomEnum(baseType)) {
                dataType = DataType.ENUM;
                enumTypeName = baseType;
            } else if (executor.database.isDomain(baseType)) {
                DomainType domain = executor.database.getDomain(baseType);
                dataType = domain.getBaseType();
                domainTypeName = baseType;
                domainNotNull = domain.isNotNull();
            } else if (executor.database.isCompositeType(baseType)) {
                dataType = DataType.TEXT;
                compositeTypeName = baseType;
            } else {
                throw new MemgresException("type \"" + baseType + "\" does not exist", "42704");
            }
        }

        return new ResolvedType(dataType, enumTypeName, domainTypeName, compositeTypeName, arrayElementType, domainNotNull);
    }

    // ---- Static helpers ----

    /** Convert an Expression AST to a default-value string representation. */
    static String exprToDefaultString(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            StringBuilder sb = new StringBuilder(fn.name()).append("(");
            for (int i = 0; i < fn.args().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(exprToDefaultString(fn.args().get(i)));
            }
            sb.append(")");
            return sb.toString();
        } else if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            if (lit.value() == null) return "null";
            return lit.literalType() == Literal.LiteralType.STRING
                    ? "'" + lit.value().replace("'", "''") + "'"
                    : lit.value();
        } else if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            return ref.column();
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return exprToDefaultString(cast.expr()) + "::" + cast.typeName();
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) {
                return exprToDefaultString(cop.left()) + " " + cop.opSymbol() + " " + exprToDefaultString(cop.right());
            } else {
                return cop.opSymbol() + " " + exprToDefaultString(cop.right());
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            String op;
            switch (bin.op()) {
                case ADD:
                    op = "+";
                    break;
                case SUBTRACT:
                    op = "-";
                    break;
                case MULTIPLY:
                    op = "*";
                    break;
                case DIVIDE:
                    op = "/";
                    break;
                case MODULO:
                    op = "%";
                    break;
                case POWER:
                    op = "^";
                    break;
                case CONCAT:
                    op = "||";
                    break;
                case AND:
                    op = "AND";
                    break;
                case OR:
                    op = "OR";
                    break;
                case EQUAL:
                    op = "=";
                    break;
                case NOT_EQUAL:
                    op = "<>";
                    break;
                case LESS_THAN:
                    op = "<";
                    break;
                case GREATER_THAN:
                    op = ">";
                    break;
                case LESS_EQUAL:
                    op = "<=";
                    break;
                case GREATER_EQUAL:
                    op = ">=";
                    break;
                default:
                    op = bin.op().name();
                    break;
            }
            return exprToDefaultString(bin.left()) + " " + op + " " + exprToDefaultString(bin.right());
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            String op;
            switch (un.op()) {
                case NEGATE:
                    op = "-";
                    break;
                case NOT:
                    op = "NOT ";
                    break;
                case BIT_NOT:
                    op = "~";
                    break;
                default:
                    op = un.op().name();
                    break;
            }
            return op + exprToDefaultString(un.operand());
        }
        return "null";
    }

    /** Parse a partition bound value string to an appropriate type. */
    static Object parseBoundValue(String val) {
        if (val.equalsIgnoreCase("MINVALUE")) return Long.MIN_VALUE;
        if (val.equalsIgnoreCase("MAXVALUE")) return Long.MAX_VALUE;
        if (val.startsWith("'") && val.endsWith("'")) return val.substring(1, val.length() - 1);
        try { return Long.parseLong(val); } catch (NumberFormatException e) { /* ignore */ }
        try { return Double.parseDouble(val); } catch (NumberFormatException e) { /* ignore */ }
        return val;
    }

    /** Compare two partition bound values. */
    static int comparePartitionBound(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Number && b instanceof Number) {
            Number nb = (Number) b;
            Number na = (Number) a;
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    /** Extract marker value from a quoted string like "'__marker__:value'". */
    static String extractMarker(String defaultVal) {
        int q1 = defaultVal.indexOf("'");
        int q2 = defaultVal.lastIndexOf("'");
        if (q1 >= 0 && q2 > q1) return defaultVal.substring(q1 + 1, q2);
        return defaultVal;
    }

    /** Extract bare identifier tokens from a SQL expression string (simple lexical scan). */
    static List<String> extractIdentifiers(String expr) {
        List<String> result = new ArrayList<>();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (c == '"') {
                int j = i + 1;
                while (j < expr.length() && expr.charAt(j) != '"') j++;
                result.add(expr.substring(i + 1, j));
                i = j + 1;
                continue;
            }
            if (c == '\'') {
                int j = i + 1;
                while (j < expr.length()) {
                    if (expr.charAt(j) == '\'' && (j + 1 >= expr.length() || expr.charAt(j + 1) != '\'')) break;
                    if (expr.charAt(j) == '\'') j++;
                    j++;
                }
                i = j + 1;
                continue;
            }
            if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < expr.length() && (Character.isLetterOrDigit(expr.charAt(j)) || expr.charAt(j) == '_')) j++;
                int k = j;
                while (k < expr.length() && Character.isWhitespace(expr.charAt(k))) k++;
                if (k < expr.length() && expr.charAt(k) == '(') {
                    i = j;
                    continue;
                }
                result.add(expr.substring(i, j));
                i = j;
                continue;
            }
            i++;
        }
        return result;
    }

    private static final Set<String> SQL_KEYWORDS_AND_FUNCTIONS = new HashSet<>(Arrays.asList(
            "and", "or", "not", "null", "true", "false", "is", "in", "between", "like", "case", "when", "then",
            "else", "end", "as", "cast", "extract", "epoch", "year", "month", "day", "hour", "minute", "second",
            "from", "at", "time", "zone",
            "abs", "ceil", "ceiling", "floor", "round", "trunc", "sqrt", "power", "exp", "ln", "log",
            "mod", "sign", "greatest", "least",
            "upper", "lower", "length", "substr", "substring", "trim", "ltrim", "rtrim", "lpad", "rpad",
            "concat", "replace", "position", "overlay", "char_length", "octet_length", "left", "right",
            "repeat", "reverse", "split_part", "strpos", "to_char", "to_number", "to_date",
            "date_part", "date_trunc", "age", "now", "current_timestamp", "current_date", "current_time",
            "make_date", "make_time", "make_interval",
            "int", "integer", "bigint", "smallint", "numeric", "decimal", "real", "float", "double",
            "boolean", "bool", "text", "varchar", "char", "date", "timestamp", "interval",
            "coalesce", "nullif", "ifnull",
            "count", "sum", "min", "max", "avg"
    ));

    static boolean isSqlKeywordOrFunction(String ident) {
        return SQL_KEYWORDS_AND_FUNCTIONS.contains(ident.toLowerCase());
    }

    private List<String> resolveConstraintColumns(List<String> columns) {
        if (columns.size() == 1 && columns.get(0).startsWith("__using_index__:")) {
            String indexName = columns.get(0).substring("__using_index__:".length());
            List<String> indexCols = executor.database.getIndexColumns(indexName);
            if (indexCols != null) return indexCols;
            return Cols.listOf();
        }
        return columns;
    }
}
