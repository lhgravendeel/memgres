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
    QueryResult executeCreateEventTrigger(CreateEventTriggerStmt stmt) { return objectExecutor.executeCreateEventTrigger(stmt); }
    QueryResult executeAlterEventTrigger(AlterEventTriggerStmt stmt) { return objectExecutor.executeAlterEventTrigger(stmt); }
    QueryResult executeDropEventTrigger(DropEventTriggerStmt stmt) { return objectExecutor.executeDropEventTrigger(stmt); }
    QueryResult executeDropStmt(DropStmt stmt) { return objectExecutor.executeDropStmt(stmt); }
    QueryResult executeAlterObject(String payload) { return objectExecutor.executeAlterObject(payload); }
    QueryResult executeDropObject(String payload) { return objectExecutor.executeDropObject(payload); }
    void requireObjectExists(String kind, String name) { objectExecutor.requireObjectExists(kind, name); }
    void requireSchemaExists(String schemaName) { objectExecutor.requireSchemaExists(schemaName); }
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

    QueryResult executeCreateCollation(CreateCollationStmt stmt) { return objectExecutor.executeCreateCollation(stmt); }
    QueryResult executeCreateCast(CreateCastStmt stmt) { return objectExecutor.executeCreateCast(stmt); }

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

    /**
     * Resolve the relation a FOREIGN KEY points at, then run PostgreSQL's definition-time checks
     * on the constraint. Shared by CREATE TABLE (column-level REFERENCES and table-level FOREIGN
     * KEY) and ALTER TABLE ... ADD CONSTRAINT so that all three reject the same definitions.
     *
     * @param schemaName schema of the table carrying the constraint, used when the reference is
     *                   written unqualified
     */
    void validateForeignKeyDefinition(StoredConstraint fk, Table table, String schemaName) {
        String refTableName = fk.getReferencesTable();
        if (refTableName == null) return;
        String refSchemaName = fk.getReferencesSchema();
        // A view resolves to its base table, which would silently point the key somewhere the
        // user never named; PostgreSQL refuses instead.
        Database.ViewDef view = executor.database.getView(refTableName);
        if (view != null && (refSchemaName == null || refSchemaName.equalsIgnoreCase(view.schemaName))) {
            throw PgErrors.wrongObjectType("referenced relation \"" + refTableName + "\" is not a table");
        }
        Table refTable = null;
        if (refSchemaName != null) {
            Schema s = executor.database.getSchema(refSchemaName);
            if (s != null) refTable = s.getTable(refTableName);
        }
        if (refTable == null && schemaName != null) {
            Schema s = executor.database.getSchema(schemaName);
            if (s != null) refTable = s.getTable(refTableName);
        }
        if (refTable == null) refTable = resolveTableOrNull(refTableName);
        if (refTable == null) {
            throw new MemgresException("relation \"" + refTableName + "\" does not exist", "42P01");
        }
        // A key across the temp/permanent boundary outlives one of its two ends, so PostgreSQL
        // refuses it. Checked here so all three ways of declaring a key are covered.
        tableExecutor.checkTempPermanentReference(schemaName, refTable);
        ConstraintValidator.validateForeignKeyDefinition(table, refTable, refTableName, fk);
    }

    /**
     * PostgreSQL backs every EXCLUDE constraint with a real index — that is what
     * pg_class, pg_index, pg_indexes and pg_constraint.conindid all point at. Register the
     * same index metadata so those catalogs agree with PG.
     */
    void registerExcludeIndex(String schemaName, String tableName, StoredConstraint sc) {
        if (sc == null || sc.getType() != StoredConstraint.Type.EXCLUDE) return;
        List<String> cols = new ArrayList<>();
        if (sc.getExcludeElements() != null) {
            for (StoredConstraint.ExcludeElement e : sc.getExcludeElements()) cols.add(e.column());
        }
        if (cols.isEmpty()) return;
        String method = sc.getExcludeMethod() != null ? sc.getExcludeMethod() : "btree";
        String idxSchema = schemaName != null ? schemaName : "public";
        executor.database.addIndex(idxSchema, sc.getName(), cols);
        executor.database.addIndexMeta(idxSchema, sc.getName(),
                idxSchema + "." + tableName, false, method, null);
    }

    /** Convert a TableConstraint AST node to a StoredConstraint. */
    StoredConstraint convertTableConstraint(String tableName, TableConstraint tc) {
        return convertTableConstraint(tableName, tc, null);
    }

    /**
     * Convert a TableConstraint AST node to a StoredConstraint. When {@code existing} is given,
     * a name PostgreSQL would have chosen is made unique against the constraints already on the
     * table the way PG does it — by appending 1, 2, ... — so two unnamed CHECKs on the same
     * table do not end up sharing a name that DROP CONSTRAINT could not tell apart.
     */
    StoredConstraint convertTableConstraint(String tableName, TableConstraint tc, Table existing) {
        String name = tc.name();
        switch (tc.type()) {
            case PRIMARY_KEY: {
                if (name == null) name = uniqueConstraintName(tableName + "_pkey", existing);
                // A key is stored as an index, so it is bounded by what an index tuple holds.
                DdlIndexValidator.checkIndexColumnCount(tc.columns(), null);
                StoredConstraint pk = StoredConstraint.primaryKey(name, resolveConstraintColumns(tc.columns()));
                if (tc.deferrable()) {
                    pk.setDeferrable(true);
                    pk.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return pk;
            }
            case UNIQUE: {
                DdlIndexValidator.checkIndexColumnCount(tc.columns(), null);
                List<String> cols = resolveConstraintColumns(tc.columns());
                if (name == null) {
                    name = uniqueConstraintName(
                            tableName + "_" + String.join("_", cols) + "_key", existing);
                }
                StoredConstraint sc = StoredConstraint.unique(name, cols);
                // For expression-based UNIQUE constraints (e.g. UNIQUE (id, (data->>'k'))),
                // parse every column entry (plain identifiers included) as an expression so
                // uniqueness enforcement and ON CONFLICT matching can evaluate/compare them
                // structurally. Mirrors the detection used for CREATE UNIQUE INDEX.
                boolean hasExprCols = cols.stream().anyMatch(c ->
                        c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                        || c.contains("*") || c.contains("/") || c.contains("||"));
                if (hasExprCols) {
                    List<Expression> exprCols = new ArrayList<>();
                    for (String col : cols) {
                        try {
                            exprCols.add(com.memgres.engine.parser.Parser.parseExpression(col));
                        } catch (Exception e) {
                            exprCols = null;
                            break;
                        }
                    }
                    if (exprCols != null) {
                        sc.setExpressionColumns(exprCols);
                    }
                }
                if (tc.nullsNotDistinct()) sc.setNullsNotDistinct(true);
                if (tc.deferrable()) {
                    sc.setDeferrable(true);
                    sc.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return sc;
            }
            case CHECK: {
                if (name == null) {
                    // PG names an unnamed CHECK after the one column its expression mentions —
                    // {table}_{col}_check — and after the table alone when it mentions none or
                    // several. Which columns those are comes from the expression, not from where
                    // the constraint was written, so a table-level CHECK on one column is named
                    // the same as if it had been written on the column.
                    List<String> checkCols = referencedColumnNames(tc.checkExpr());
                    name = uniqueConstraintName(checkCols.size() == 1
                            ? tableName + "_" + checkCols.get(0) + "_check"
                            : tableName + "_check", existing);
                }
                StoredConstraint chk = StoredConstraint.check(name, tc.checkExpr());
                if (tc.notEnforced()) chk.setNotEnforced(true);
                if (tc.noInherit()) chk.setNoInherit(true);
                if (tc.deferrable()) {
                    chk.setDeferrable(true);
                    chk.setInitiallyDeferred(tc.initiallyDeferred());
                }
                return chk;
            }
            case FOREIGN_KEY: {
                if (name == null) name = tableName + "_" + String.join("_", tc.columns()) + "_fkey";
                String fkRefTable = tc.referencesTable();
                String fkRefSchema = null;
                if (fkRefTable != null && fkRefTable.contains(".")) {
                    int dot = fkRefTable.indexOf('.');
                    fkRefSchema = fkRefTable.substring(0, dot);
                    fkRefTable = fkRefTable.substring(dot + 1);
                }
                StoredConstraint fk = StoredConstraint.foreignKey(name, tc.columns(),
                        fkRefTable, tc.referencesColumns(),
                        StoredConstraint.parseFkAction(tc.onDelete()),
                        StoredConstraint.parseFkAction(tc.onUpdate()));
                if (fkRefSchema != null) fk.setReferencesSchema(fkRefSchema);
                if (tc.deferrable()) {
                    fk.setDeferrable(true);
                    fk.setInitiallyDeferred(tc.initiallyDeferred());
                }
                if (tc.notEnforced()) fk.setNotEnforced(true);
                if (tc.period()) fk.setPeriod(true);
                if (tc.matchType() != null) fk.setMatchType(tc.matchType());
                fk.setOnDeleteSetNullColumns(StoredConstraint.parseSetNullColumns(tc.onDelete()));
                fk.setOnUpdateSetNullColumns(StoredConstraint.parseSetNullColumns(tc.onUpdate()));
                return fk;
            }
            case EXCLUDE: {
                if (name == null) {
                    // PG names it <table>_<col>..._excl.
                    List<String> exCols = tc.columns();
                    name = exCols == null || exCols.isEmpty()
                            ? tableName + "_excl"
                            : tableName + "_" + String.join("_", exCols) + "_excl";
                }
                StoredConstraint excl = new StoredConstraint(name, StoredConstraint.Type.EXCLUDE,
                        tc.columns(), null, null, null, null, null);
                excl.setExcludeMethod(tc.excludeMethod() != null ? tc.excludeMethod() : "btree");
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

        /** The base type's modifier when the column was declared with a domain, else null. */
        private Integer domainPrecision;
        private Integer domainScale;
        private String domainIntervalQualifier;

        void setDomainTypmod(Integer precision, Integer scale, String intervalQualifier) {
            this.domainPrecision = precision;
            this.domainScale = scale;
            this.domainIntervalQualifier = intervalQualifier;
        }

        public Integer domainPrecision() { return domainPrecision; }
        public Integer domainScale() { return domainScale; }
        public String domainIntervalQualifier() { return domainIntervalQualifier; }

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
        TypeCoercion.checkDeclaredTypeLimits(typeName);
        String fullTypeName = typeName.replaceAll("\\(.*\\)", "").trim();
        boolean isArray = fullTypeName.endsWith("[]");
        DataType arrayElementType = null;
        // Which type a written name denotes is settled once, here, and the column records the
        // answer: with two schemas each holding an e, a column declared a.e has to keep reading
        // a.e's definition however the search path moves afterwards.
        String baseType = TypeNamespace.qualify(executor.database, executor.session,
                fullTypeName.replace("[]", "").trim());
        if (!baseType.equals(fullTypeName.replace("[]", "").trim())) {
            fullTypeName = isArray ? baseType + "[]" : baseType;
        }
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

        // Extension-gated types: hstore requires CREATE EXTENSION hstore
        if (dataType == DataType.HSTORE && !executor.database.hasExtension("hstore")) {
            throw new MemgresException("type \"hstore\" does not exist\n"
                    + "  Hint: You need to install the hstore extension: CREATE EXTENSION hstore;", "42704");
        }
        // FLOAT(p): p <= 24 -> REAL, p >= 25 -> DOUBLE PRECISION
        if (baseType.equalsIgnoreCase("float") && precision != null && precision <= 24) {
            dataType = DataType.REAL;
        }

        String enumTypeName = null;
        String domainTypeName = null;
        String compositeTypeName = null;
        boolean domainNotNull = false;
        Integer domainPrecision = null;
        Integer domainScale = null;
        String domainInterval = null;

        if (dataType == null) {
            if (executor.database.isCustomEnum(baseType)) {
                dataType = DataType.ENUM;
                enumTypeName = baseType;
                // A custom enum's base name never matches DataType.fromPgName, so the isArray
                // branch above left arrayElementType null for "enum_type[]" columns -- making
                // them indistinguishable from a scalar enum column of the same type. Mark them
                // as arrays too (mirrors the built-in-array convention used elsewhere: dataType
                // == arrayElementType, non-null arrayElementType means "this column is an
                // array"), so the wire layer can advertise a distinct array-type OID instead of
                // reusing the element's OID for both.
                if (isArray) {
                    arrayElementType = DataType.ENUM;
                }
            } else if (executor.database.isDomain(baseType)) {
                DomainType domain = executor.database.getDomain(baseType);
                dataType = domain.getBaseType();
                domainTypeName = baseType;
                domainNotNull = domain.isNotNull();
                // A column declared with a domain is a column of the domain's base type, width
                // and all: varchar(9) refuses a tenth character whether the nine were written
                // on the column or on the domain it was declared with.
                domainPrecision = domain.getPrecision();
                domainScale = domain.getScale();
                domainInterval = domain.getIntervalQualifier();
                if (domain.getArrayElementType() != null) arrayElementType = domain.getArrayElementType();
            } else if (executor.database.isCompositeType(baseType)) {
                dataType = DataType.TEXT;
                compositeTypeName = baseType;
                // As for an enum above, a composite's name never matches a built-in, so an array
                // of one was left indistinguishable from a single one of it.
                if (isArray) {
                    arrayElementType = DataType.TEXT;
                }
            } else if (executor.database.isShellType(baseType)) {
                // A shell has no representation yet, so nothing can be declared as one
                throw new MemgresException("type \""
                        + TypeNamespace.display(executor.database, executor.session, baseType)
                        + "\" is only a shell", "42704");
            } else {
                // A qualifier that turned out to name no type of this engine's own is dropped and
                // the bare name read as the built-in it spells, which is what information_schema's
                // domains and an extension's types are written as. Whether the schema was entitled
                // to hold that name is the statement-level check's business, not this one's.
                DataType builtin = TypeNamespace.writtenSchema(baseType) == null ? null
                        : DataType.fromPgName(isArray
                                ? TypeNamespace.bare(baseType) + "[]" : TypeNamespace.bare(baseType));
                if (builtin == null && TypeNamespace.writtenSchema(baseType) != null) {
                    builtin = DataType.fromPgName(TypeNamespace.bare(baseType));
                }
                if (builtin == null) {
                    throw new MemgresException("type \"" + baseType + "\" does not exist", "42704");
                }
                dataType = builtin;
                if (isArray && arrayElementType == null) {
                    arrayElementType = DataType.fromPgName(TypeNamespace.bare(baseType));
                }
            }
        }

        ResolvedType resolved = new ResolvedType(dataType, enumTypeName, domainTypeName,
                compositeTypeName, arrayElementType, domainNotNull);
        resolved.setDomainTypmod(domainPrecision, domainScale, domainInterval);
        return resolved;
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
        } else if (expr instanceof ArrayExpr) {
            // Without this an ARRAY[...] default fell through to the "null" below, so the column
            // was recorded as having no default at all and every insert that relied on it
            // silently got a null instead of the array that was written.
            ArrayExpr arr = (ArrayExpr) expr;
            StringBuilder sb = new StringBuilder(arr.isRow() ? "ROW(" : "ARRAY[");
            for (int i = 0; i < arr.elements().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(exprToDefaultString(arr.elements().get(i)));
            }
            return sb.append(arr.isRow() ? ")" : "]").toString();
        }
        return "null";
    }

    // Built-in volatile function names (these don't have PgFunction entries in the database)
    private static final Set<String> BUILTIN_VOLATILE_FUNCTIONS = Cols.setOf(
            "random", "now", "clock_timestamp", "timeofday", "gen_random_uuid", "uuidv4",
            "nextval", "currval", "setval", "txid_current", "statement_timestamp"
    );

    // Built-in volatile identifiers that appear as bare names (not function calls)
    private static final Set<String> BUILTIN_VOLATILE_IDENTIFIERS = Cols.setOf(
            "current_timestamp", "current_time", "current_date", "localtimestamp", "localtime"
    );

    /**
     * Check that an expression is immutable — rejects VOLATILE and STABLE functions/operators.
     * Used for CREATE INDEX expressions and VIRTUAL generated columns.
     * Walks the expression AST recursively, checking:
     * - FunctionCallExpr: looks up PgFunction.getVolatility()
     * - CustomOperatorExpr: resolves operator → backing function → checks volatility
     * - Built-in volatile functions by name (hardcoded list, no PgFunction entries)
     *
     * PG trusts declared volatility (no transitive checking of function bodies).
     *
     * @throws MemgresException with sqlState 42P17 if expression is not immutable
     */
    /**
     * A SQL-language function whose body is a single {@code SELECT expr} is inlined by the planner
     * before the expression is ever judged, so what governs is the body, not the declared
     * volatility: {@code CREATE FUNCTION f(int) RETURNS int LANGUAGE sql STABLE AS 'SELECT $1 + 1'}
     * leaves an immutable expression behind and PostgreSQL accepts it in a partition key. A body
     * that is not a single expression is not inlinable, and its declaration is believed.
     *
     * @return true when the call was judged here — by its body — and needs no volatility check
     */
    private static boolean checkInlinedSqlBody(PgFunction fn, Database db, String errorMsg) {
        if (fn.getLanguage() == null || !"sql".equalsIgnoreCase(fn.getLanguage())) return false;
        String body = fn.getBody();
        if (body == null) return false;
        String trimmed = body.trim();
        while (trimmed.endsWith(";")) trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        if (!trimmed.regionMatches(true, 0, "select", 0, 6)) return false;
        String selectList = trimmed.substring(6).trim();
        // Anything past the select list — FROM, WHERE, a second statement — is not a bare
        // expression, so the body is not inlinable in the sense that matters here.
        if (selectList.isEmpty() || containsBareSemicolon(selectList)) return false;
        Expression inlined;
        try {
            inlined = com.memgres.engine.parser.Parser.parseExpression(selectList);
        } catch (RuntimeException notAnExpression) {
            return false;
        }
        checkExpressionImmutability(inlined, db, errorMsg);
        return true;
    }

    /** True when the text holds a {@code ;} outside quotes and parentheses. */
    private static boolean containsBareSemicolon(String text) {
        int depth = 0;
        boolean inString = false;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (inString) {
                if (ch == '\'') inString = false;
            } else if (ch == '\'') {
                inString = true;
            } else if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
            } else if (ch == ';' && depth == 0) {
                return true;
            }
        }
        return false;
    }

    static void checkExpressionImmutability(Expression expr, Database db, String errorMsg) {
        if (expr == null) return;

        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String fnName = fn.name().toLowerCase();
            // Check built-in volatile functions
            if (BUILTIN_VOLATILE_FUNCTIONS.contains(fnName)) {
                throw new MemgresException(errorMsg, "42P17");
            }
            // Check user-defined function volatility
            PgFunction pgFunc = db.getFunction(fn.name());
            if (pgFunc != null && !checkInlinedSqlBody(pgFunc, db, errorMsg)) {
                String vol = pgFunc.getVolatility();
                if (vol == null || "VOLATILE".equalsIgnoreCase(vol) || "STABLE".equalsIgnoreCase(vol)) {
                    throw new MemgresException(errorMsg, "42P17");
                }
            }
            // Recurse into function arguments
            if (fn.args() != null) {
                for (Expression arg : fn.args()) {
                    checkExpressionImmutability(arg, db, errorMsg);
                }
            }
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            // Resolve operator → backing function → check volatility
            java.util.List<PgOperator> ops = db.getOperatorsByName(cop.opSymbol());
            for (PgOperator op : ops) {
                if (op.getFunction() != null) {
                    PgFunction pgFunc = db.getFunction(op.getFunction());
                    if (pgFunc != null) {
                        String vol = pgFunc.getVolatility();
                        if (vol == null || "VOLATILE".equalsIgnoreCase(vol) || "STABLE".equalsIgnoreCase(vol)) {
                            throw new MemgresException(errorMsg, "42P17");
                        }
                    }
                    break; // Only check the first matching operator
                }
            }
            // Recurse into operands
            if (cop.left() != null) checkExpressionImmutability(cop.left(), db, errorMsg);
            if (cop.right() != null) checkExpressionImmutability(cop.right(), db, errorMsg);
        } else if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            // Check bare volatile identifiers like current_timestamp, localtime, etc.
            if (cr.table() == null && BUILTIN_VOLATILE_IDENTIFIERS.contains(cr.column().toLowerCase())) {
                throw new MemgresException(errorMsg, "42P17");
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            checkExpressionImmutability(bin.left(), db, errorMsg);
            checkExpressionImmutability(bin.right(), db, errorMsg);
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            checkExpressionImmutability(un.operand(), db, errorMsg);
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            checkExpressionImmutability(cast.expr(), db, errorMsg);
        } else if (expr instanceof CaseExpr) {
            CaseExpr ce = (CaseExpr) expr;
            if (ce.operand() != null) checkExpressionImmutability(ce.operand(), db, errorMsg);
            if (ce.whenClauses() != null) {
                for (CaseExpr.WhenClause wc : ce.whenClauses()) {
                    checkExpressionImmutability(wc.condition, db, errorMsg);
                    checkExpressionImmutability(wc.result, db, errorMsg);
                }
            }
            if (ce.elseExpr() != null) checkExpressionImmutability(ce.elseExpr(), db, errorMsg);
        } else if (expr instanceof IsNullExpr) {
            checkExpressionImmutability(((IsNullExpr) expr).expr(), db, errorMsg);
        } else if (expr instanceof BetweenExpr) {
            BetweenExpr be = (BetweenExpr) expr;
            checkExpressionImmutability(be.expr(), db, errorMsg);
            checkExpressionImmutability(be.low(), db, errorMsg);
            checkExpressionImmutability(be.high(), db, errorMsg);
        } else if (expr instanceof InExpr) {
            InExpr ie = (InExpr) expr;
            checkExpressionImmutability(ie.expr(), db, errorMsg);
            if (ie.values() != null) {
                for (Expression v : ie.values()) {
                    checkExpressionImmutability(v, db, errorMsg);
                }
            }
        }
        // Literals, parameters, etc. are always immutable — no action needed
    }

    /**
     * PG 18: Virtual generated columns cannot use user-defined functions at all.
     * Even IMMUTABLE UDFs are rejected with SQLSTATE 0A000.
     */
    static void checkVirtualColumnUdf(String exprStr, Database db) {
        try {
            Expression parsed = com.memgres.engine.parser.Parser.parseExpression(exprStr);
            checkVirtualColumnUdfExpr(parsed, db);
        } catch (MemgresException e) {
            throw e;
        } catch (Exception ignored) {}
    }

    private static void checkVirtualColumnUdfExpr(Expression expr, Database db) {
        if (expr == null) return;
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            PgFunction pgFunc = db.getFunction(fn.name());
            if (pgFunc != null) {
                throw new MemgresException("generation expression uses user-defined function", "0A000");
            }
            if (fn.args() != null) {
                for (Expression arg : fn.args()) checkVirtualColumnUdfExpr(arg, db);
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            checkVirtualColumnUdfExpr(bin.left(), db);
            checkVirtualColumnUdfExpr(bin.right(), db);
        } else if (expr instanceof UnaryExpr) {
            checkVirtualColumnUdfExpr(((UnaryExpr) expr).operand(), db);
        } else if (expr instanceof CastExpr) {
            checkVirtualColumnUdfExpr(((CastExpr) expr).expr(), db);
        } else if (expr instanceof CaseExpr) {
            CaseExpr ce = (CaseExpr) expr;
            if (ce.operand() != null) checkVirtualColumnUdfExpr(ce.operand(), db);
            if (ce.whenClauses() != null) {
                for (CaseExpr.WhenClause wc : ce.whenClauses()) {
                    checkVirtualColumnUdfExpr(wc.condition, db);
                    checkVirtualColumnUdfExpr(wc.result, db);
                }
            }
            if (ce.elseExpr() != null) checkVirtualColumnUdfExpr(ce.elseExpr(), db);
        }
    }

    /**
     * Check only built-in volatile functions/identifiers in an expression string.
     * Used for CREATE INDEX — PG enforces immutability for built-in volatile functions
     * but allows user-defined volatile functions in expression indexes.
     */
    static void checkBuiltinVolatileInExpression(String exprStr, Database db, String errorMsg) {
        String norm = exprStr.toLowerCase().replaceAll("\\s+", "");
        for (String fn : BUILTIN_VOLATILE_FUNCTIONS) {
            if (norm.contains(fn + "(")) {
                throw new MemgresException(errorMsg, "42P17");
            }
        }
        for (String id : BUILTIN_VOLATILE_IDENTIFIERS) {
            if (norm.contains(id)) {
                throw new MemgresException(errorMsg, "42P17");
            }
        }
    }

    /**
     * Check immutability using string-based expression (parses first, then walks AST).
     * Falls back to string matching if parsing fails.
     */
    static void checkExpressionImmutability(String exprStr, Database db, String errorMsg) {
        // Fast path: check for built-in volatile names in the raw string
        String norm = exprStr.toLowerCase().replaceAll("\\s+", "");
        for (String fn : BUILTIN_VOLATILE_FUNCTIONS) {
            if (norm.contains(fn + "(")) {
                throw new MemgresException(errorMsg, "42P17");
            }
        }
        for (String id : BUILTIN_VOLATILE_IDENTIFIERS) {
            if (norm.contains(id)) {
                throw new MemgresException(errorMsg, "42P17");
            }
        }

        // Parse expression and do AST-based checking for user-defined functions/operators
        try {
            Expression parsed = com.memgres.engine.parser.Parser.parseExpression(exprStr);
            checkExpressionImmutability(parsed, db, errorMsg);
        } catch (MemgresException e) {
            throw e; // Re-throw volatility errors
        } catch (Exception ignored) {
            // If parsing fails, the string-based check above is sufficient
        }
    }

    /** Parse a partition bound value string to an appropriate type. */
    static Object parseBoundValue(String val) {
        // Quoted values are always string literals ('MINVALUE' is the text, MINVALUE the keyword)
        if (val.length() >= 2 && val.startsWith("'") && val.endsWith("'")) {
            return val.substring(1, val.length() - 1);
        }
        if (val.equalsIgnoreCase("MINVALUE")) return PartitionBound.MINVALUE;
        if (val.equalsIgnoreCase("MAXVALUE")) return PartitionBound.MAXVALUE;
        if (val.equalsIgnoreCase("NULL")) return null;
        if (val.equalsIgnoreCase("TRUE")) return Boolean.TRUE;
        if (val.equalsIgnoreCase("FALSE")) return Boolean.FALSE;
        try { return Long.parseLong(val); } catch (NumberFormatException e) { /* ignore */ }
        try { return Double.parseDouble(val); } catch (NumberFormatException e) { /* ignore */ }
        return val;
    }

    /** Compare two partition bound values (sentinel-, tuple-, and NULL-aware). */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static int comparePartitionBound(Object a, Object b) {
        if (a == b) return 0;
        // MINVALUE sorts below everything, MAXVALUE above everything, regardless of key type
        if (a == PartitionBound.MINVALUE) return -1;
        if (b == PartitionBound.MINVALUE) return 1;
        if (a == PartitionBound.MAXVALUE) return 1;
        if (b == PartitionBound.MAXVALUE) return -1;
        if (a == null) return -1;
        if (b == null) return 1;
        // Multi-column bounds compare lexicographically, element by element
        if (a instanceof List && b instanceof List) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;
            int minLen = Math.min(la.size(), lb.size());
            for (int i = 0; i < minLen; i++) {
                int cmp = comparePartitionBound(la.get(i), lb.get(i));
                if (cmp != 0) return cmp;
            }
            return Integer.compare(la.size(), lb.size());
        }
        if (a instanceof Number && b instanceof Number) {
            Number nb = (Number) b;
            Number na = (Number) a;
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a.getClass() == b.getClass() && a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
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
            "count", "sum", "min", "max", "avg",
            "returning", "passing", "json_value", "json_query", "json_exists",
            "json_object", "json_array", "json_serialize", "json_scalar",
            "json_table", "json_arrayagg", "json_objectagg",
            "path", "wrapper", "conditional", "unconditional",
            "keep", "omit", "quotes", "format", "json", "jsonb",
            "value", "key", "columns", "nested", "ordinality",
            "empty", "error", "object", "array", "scalar",
            "on", "with", "without", "unique", "keys",
            "absent", "default", "exists"
    ));

    static boolean isSqlKeywordOrFunction(String ident) {
        return SQL_KEYWORDS_AND_FUNCTIONS.contains(ident.toLowerCase());
    }

    /** {@code base}, or base with the lowest suffix from 1 upwards that the table does not use. */
    private static String uniqueConstraintName(String base, Table existing) {
        if (existing == null) return base;
        String candidate = base;
        for (int n = 1; taken(candidate, existing); n++) {
            candidate = base + n;
        }
        return candidate;
    }

    private static boolean taken(String name, Table table) {
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getName() != null && sc.getName().equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /** The distinct column names an expression mentions, in the order they first appear. */
    static List<String> referencedColumnNames(Expression expr) {
        final List<String> names = new ArrayList<>();
        if (expr == null) return names;
        AstWalk.forEach(expr, new java.util.function.Consumer<Object>() {
            @Override public void accept(Object node) {
                if (!(node instanceof ColumnRef)) return;
                String column = ((ColumnRef) node).column;
                if (column != null && !names.contains(column)) names.add(column);
            }
        });
        return names;
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
