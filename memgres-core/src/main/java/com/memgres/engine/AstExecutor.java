package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.Lexer;
import com.memgres.engine.parser.Parser;
import com.memgres.engine.parser.ast.*;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;
import com.memgres.engine.plpgsql.PlpgsqlParser;
import com.memgres.engine.plpgsql.PlpgsqlStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * AST-based SQL executor. Walks parsed AST nodes and executes them against the database.
 */
public class AstExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AstExecutor.class);

    final Database database;
    final Session session; // null when no session
    final SystemCatalog systemCatalog;
    final ArrayOperationHandler arrayOperationHandler = new ArrayOperationHandler(this);
    final BinaryOpEvaluator binaryOpEvaluator = new BinaryOpEvaluator(this);
    final CompositeTypeHandler compositeTypeHandler = new CompositeTypeHandler(this);
    final DateTimeArithmetic dateTimeArithmetic = new DateTimeArithmetic(this);
    final FunctionEvaluator functionEvaluator = new FunctionEvaluator(this);
    final CastEvaluator castEvaluator = new CastEvaluator(this);
    final ConstraintValidator constraintValidator = new ConstraintValidator(this);
    final SessionExecutor sessionExecutor = new SessionExecutor(this);
    final DmlExecutor dmlExecutor = new DmlExecutor(this);
    final DdlExecutor ddlExecutor = new DdlExecutor(this);
    Long lastSequenceValue = null; // for lastval()
    final FromResolver fromResolver = new FromResolver(this);
    final ExprEvaluator exprEvaluator = new ExprEvaluator(this);
    final SelectExecutor selectExecutor = new SelectExecutor(this);
    // Stack of outer row contexts for correlated subqueries
    final Deque<RowContext> outerContextStack = new ArrayDeque<>();
    // CTE registry: name -> query body (scoped per top-level query)
    final Deque<Map<String, SelectStmt.CommonTableExpr>> cteStack = new ArrayDeque<>();
    // CTE result cache: prevents double execution of CTE bodies
    final Map<String, QueryResult> cteResultCache = new HashMap<>();
    // CTEs currently being executed (to prevent infinite recursion in recursive CTEs)
    final Set<String> executingCtes = new HashSet<>();
    // Bound parameter values for extended query protocol ($1, $2, ...)
    List<Object> boundParameters = new ArrayList<>();
    // Statement timestamp: frozen at statement start for now()/statement_timestamp()
    OffsetDateTime currentStatementTimestamp = null;
    // Current MERGE action for merge_action() function in RETURNING clause (PG 17+)
    String currentMergeAction = null;
    // Raw SQL text of the current top-level statement (for pg_prepared_statements/pg_cursors verbatim display)
    String currentRawSql = null;
    // When true, column references with no context throw instead of returning column name as string
    private boolean strictColumnRefs = false;

    public void setStrictColumnRefs(boolean strict) { this.strictColumnRefs = strict; }
    public boolean isStrictColumnRefs() { return strictColumnRefs; }

    public AstExecutor(Database database) {
        this(database, null);
    }

    public AstExecutor(Database database, Session session) {
        this.database = database;
        this.session = session;
        this.systemCatalog = new SystemCatalog(database);
    }

    public QueryResult execute(String sql) {
        return execute(sql, Cols.listOf());
    }

    public QueryResult execute(String sql, List<Object> parameters) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        if (sql.isEmpty()) {
            return QueryResult.empty();
        }

        LOG.debug("Executing: {}", sql);

        List<Object> previousParams = this.boundParameters;
        this.boundParameters = parameters != null ? new ArrayList<>(parameters) : new ArrayList<>();
        cteResultCache.clear(); // Clear CTE cache between top-level statements
        currentStatementTimestamp = OffsetDateTime.now();
        String previousRawSql = this.currentRawSql;
        this.currentRawSql = sql;
        try {
            Statement stmt = Parser.parse(sql);
            if (stmt == null) return QueryResult.empty(); // empty input (only comments)
            return executeStatement(stmt);
        } finally {
            this.boundParameters = previousParams;
            this.currentRawSql = previousRawSql;
            currentStatementTimestamp = null;
        }
    }

    public QueryResult executeStatement(Statement stmt) {
        if (stmt instanceof SelectStmt) return selectExecutor.executeSelect(((SelectStmt) stmt));
        if (stmt instanceof SetOpStmt) return selectExecutor.executeSetOp(((SetOpStmt) stmt));
        if (stmt instanceof InsertStmt) return dmlExecutor.executeInsert(((InsertStmt) stmt));
        if (stmt instanceof UpdateStmt) return dmlExecutor.executeUpdate(((UpdateStmt) stmt));
        if (stmt instanceof DeleteStmt) return dmlExecutor.executeDelete(((DeleteStmt) stmt));
        if (stmt instanceof CreateTableStmt) return ddlExecutor.executeCreateTable(((CreateTableStmt) stmt));
        if (stmt instanceof DropTableStmt) return ddlExecutor.executeDropTable(((DropTableStmt) stmt));
        if (stmt instanceof CreateTypeStmt) return ddlExecutor.executeCreateType(((CreateTypeStmt) stmt));
        if (stmt instanceof CreateFunctionStmt) return ddlExecutor.executeCreateFunction(((CreateFunctionStmt) stmt));
        if (stmt instanceof CreateAggregateStmt) return ddlExecutor.executeCreateAggregate(((CreateAggregateStmt) stmt));
        if (stmt instanceof CreateOperatorStmt) return ddlExecutor.executeCreateOperator(((CreateOperatorStmt) stmt));
        if (stmt instanceof CreateOperatorFamilyStmt) return ddlExecutor.executeCreateOperatorFamily(((CreateOperatorFamilyStmt) stmt));
        if (stmt instanceof CreateOperatorClassStmt) return ddlExecutor.executeCreateOperatorClass(((CreateOperatorClassStmt) stmt));
        if (stmt instanceof AlterOperatorStmt) return ddlExecutor.executeAlterOperator(((AlterOperatorStmt) stmt));
        if (stmt instanceof CreateTriggerStmt) return ddlExecutor.executeCreateTrigger(((CreateTriggerStmt) stmt));
        if (stmt instanceof CreateExtensionStmt) return QueryResult.message(QueryResult.Type.SET, "CREATE EXTENSION");
        if (stmt instanceof CreateRuleStmt) return ddlExecutor.executeCreateRule(((CreateRuleStmt) stmt));
        if (stmt instanceof DropStmt) return ddlExecutor.executeDropStmt(((DropStmt) stmt));
        if (stmt instanceof AlterTableStmt) return ddlExecutor.executeAlterTable(((AlterTableStmt) stmt));
        if (stmt instanceof TruncateStmt) return ddlExecutor.executeTruncate(((TruncateStmt) stmt));
        if (stmt instanceof SetStmt) return sessionExecutor.executeSetStmt(((SetStmt) stmt));
        if (stmt instanceof DiscardStmt) return sessionExecutor.executeDiscard(((DiscardStmt) stmt));
        if (stmt instanceof TransactionStmt) return ddlExecutor.executeTransaction(((TransactionStmt) stmt));
        if (stmt instanceof CreateIndexStmt) return ddlExecutor.executeCreateIndex(((CreateIndexStmt) stmt));
        if (stmt instanceof CreateViewStmt) return ddlExecutor.executeCreateView(((CreateViewStmt) stmt));
        if (stmt instanceof CreateSequenceStmt) return ddlExecutor.executeCreateSequence(((CreateSequenceStmt) stmt));
        if (stmt instanceof ExplainStmt) return ddlExecutor.executeExplain(((ExplainStmt) stmt));
        if (stmt instanceof CreateDomainStmt) return ddlExecutor.executeCreateDomain(((CreateDomainStmt) stmt));
        if (stmt instanceof CopyStmt) return dmlExecutor.executeCopy(((CopyStmt) stmt));
        if (stmt instanceof CallStmt) return ddlExecutor.executeCall(((CallStmt) stmt));
        if (stmt instanceof ListenStmt) return ddlExecutor.executeListen(((ListenStmt) stmt));
        if (stmt instanceof NotifyStmt) return ddlExecutor.executeNotify(((NotifyStmt) stmt));
        if (stmt instanceof UnlistenStmt) return ddlExecutor.executeUnlisten(((UnlistenStmt) stmt));
        if (stmt instanceof CreatePolicyStmt) return ddlExecutor.executeCreatePolicy(((CreatePolicyStmt) stmt));
        if (stmt instanceof RefreshMaterializedViewStmt) return ddlExecutor.executeRefreshMaterializedView(((RefreshMaterializedViewStmt) stmt));
        if (stmt instanceof MergeStmt) return dmlExecutor.executeMerge(((MergeStmt) stmt));
        if (stmt instanceof CreateTableAsStmt) return ddlExecutor.executeCreateTableAs(((CreateTableAsStmt) stmt));
        if (stmt instanceof AlterTypeStmt) return ddlExecutor.executeAlterType(((AlterTypeStmt) stmt));
        if (stmt instanceof AlterSequenceStmt) return ddlExecutor.executeAlterSequence(((AlterSequenceStmt) stmt));
        if (stmt instanceof CreateSchemaStmt) return ddlExecutor.executeCreateSchema(((CreateSchemaStmt) stmt));
        if (stmt instanceof PrepareStmt) return sessionExecutor.executePrepare(((PrepareStmt) stmt));
        if (stmt instanceof ExecuteStmt) return sessionExecutor.executeExecuteStmt(((ExecuteStmt) stmt));
        if (stmt instanceof DeallocateStmt) return sessionExecutor.executeDeallocate(((DeallocateStmt) stmt));
        if (stmt instanceof DeclareCursorStmt) return sessionExecutor.executeDeclareCursor(((DeclareCursorStmt) stmt));
        if (stmt instanceof FetchStmt) return sessionExecutor.executeFetch(((FetchStmt) stmt));
        if (stmt instanceof CloseStmt) return sessionExecutor.executeClose(((CloseStmt) stmt));
        if (stmt instanceof LockStmt) return sessionExecutor.executeLock(((LockStmt) stmt));
        if (stmt instanceof CreateRoleStmt) return ddlExecutor.executeCreateRole(((CreateRoleStmt) stmt));
        if (stmt instanceof AlterRoleStmt) return ddlExecutor.executeAlterRole(((AlterRoleStmt) stmt));
        if (stmt instanceof DropRoleStmt) return ddlExecutor.executeDropRole(((DropRoleStmt) stmt));
        if (stmt instanceof GrantStmt) return sessionExecutor.executeGrant(((GrantStmt) stmt));
        if (stmt instanceof RevokeStmt) return sessionExecutor.executeRevoke(((RevokeStmt) stmt));
        if (stmt instanceof AlterPolicyStmt) return ddlExecutor.executeAlterPolicy(((AlterPolicyStmt) stmt));
        if (stmt instanceof AlterDefaultPrivilegesStmt) {
            AlterDefaultPrivilegesStmt s = (AlterDefaultPrivilegesStmt) stmt;
            if (s.isGrant()) {
                database.addDefaultAcl(new Database.DefaultAclEntry(
                        null, s.inSchema(), s.objectType(),
                        s.privileges(), s.grantees(), true));
            } else {
                database.removeDefaultAcl(s.inSchema(), s.objectType(), s.grantees());
            }
            return QueryResult.message(QueryResult.Type.SET, "ALTER DEFAULT PRIVILEGES");
        }
        if (stmt instanceof AlterViewStmt) return ddlExecutor.executeAlterView(((AlterViewStmt) stmt));
        if (stmt instanceof AlterDomainStmt) return ddlExecutor.executeAlterDomain(((AlterDomainStmt) stmt));
        if (stmt instanceof AlterFunctionOwnerStmt) {
            // Legacy path — kept for backward compatibility with any code that creates this node directly
            AlterFunctionOwnerStmt s = (AlterFunctionOwnerStmt) stmt;
            String newOwner = ddlExecutor.resolveOwnerName(s.newOwner());
            if (!database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            database.setObjectOwner("function:" + s.name(), newOwner);
            return QueryResult.message(QueryResult.Type.SET, "ALTER FUNCTION");
        }
        if (stmt instanceof AlterFunctionStmt) {
            return executeAlterFunction((AlterFunctionStmt) stmt);
        }
        if (stmt instanceof AlterIndexStmt) {
            return executeAlterIndex((AlterIndexStmt) stmt);
        }
        if (stmt instanceof AlterSchemaOwnerStmt) {
            AlterSchemaOwnerStmt s = (AlterSchemaOwnerStmt) stmt;
            String newOwner = ddlExecutor.resolveOwnerName(s.newOwner());
            if (!database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            database.setObjectOwner("schema:" + s.name(), newOwner);
            return QueryResult.message(QueryResult.Type.SET, "ALTER SCHEMA");
        }
        if (stmt instanceof ReassignOwnedStmt) {
            ReassignOwnedStmt s = (ReassignOwnedStmt) stmt;
            String oldRole = ddlExecutor.resolveOwnerName(s.oldRole());
            String newRole = ddlExecutor.resolveOwnerName(s.newRole());
            if (!database.hasRole(oldRole)) {
                throw new MemgresException("role \"" + s.oldRole() + "\" does not exist", "42704");
            }
            if (!database.hasRole(newRole)) {
                throw new MemgresException("role \"" + s.newRole() + "\" does not exist", "42704");
            }
            database.reassignOwned(oldRole, newRole);
            return QueryResult.message(QueryResult.Type.SET, "REASSIGN OWNED");
        }
        if (stmt instanceof DropOwnedStmt) {
            DropOwnedStmt s = (DropOwnedStmt) stmt;
            String role = ddlExecutor.resolveOwnerName(s.role());
            if (!database.hasRole(role)) {
                throw new MemgresException("role \"" + s.role() + "\" does not exist", "42704");
            }
            ddlExecutor.executeDropOwned(role);
            return QueryResult.message(QueryResult.Type.SET, "DROP OWNED");
        }
        throw new MemgresException("unsupported statement type: " + stmt.getClass().getSimpleName(), "0A000");
    }

    // ---- SELECT (delegated to SelectExecutor) ----

    QueryResult executeSelect(SelectStmt stmt) {
        return selectExecutor.executeSelect(stmt);
    }

    // ---- Constraint & DML delegates ----

    void validateConstraints(Table table, Object[] row, Object[] excludeRow) {
        constraintValidator.validateConstraints(table, row, excludeRow);
    }

    void validateForeignKeyDeferred(Table table, Object[] row, StoredConstraint sc) {
        constraintValidator.validateForeignKeyDeferred(table, row, sc);
    }

    void handleFkOnDelete(Table parentTable, Object[] deletedRow) {
        constraintValidator.handleFkOnDelete(parentTable, deletedRow);
    }

    void handleFkOnUpdate(Table parentTable, Object[] oldRow, Object[] newRow) {
        constraintValidator.handleFkOnUpdate(parentTable, oldRow, newRow);
    }

    boolean valuesEqual(Object a, Object b) {
        return constraintValidator.valuesEqual(a, b);
    }

    static String pgTypeNameOf(Object value) {
        return ConstraintValidator.pgTypeNameOf(value);
    }

    // ---- Expression evaluation (delegated to ExprEvaluator) ----

    public Object evalExpr(Expression expr, RowContext ctx) {
        return exprEvaluator.evalExpr(expr, ctx);
    }

    // ---- Composite type operations (delegated to CompositeTypeHandler) ----

    String resolveCompositeTypeName(Expression expr, RowContext ctx) {
        return compositeTypeHandler.resolveCompositeTypeName(expr, ctx);
    }

    String resolveCompositeTypeNamePublic(Expression expr, RowContext ctx) {
        return compositeTypeHandler.resolveCompositeTypeName(expr, ctx);
    }

    Object extractCompositeField(Object val, String fieldName, String typeName) {
        return compositeTypeHandler.extractCompositeField(val, fieldName, typeName);
    }

    String[] splitCompositeString(String inner) {
        return compositeTypeHandler.splitCompositeString(inner);
    }

    PgRow parseCompositeToRow(String s, String typeName) {
        return compositeTypeHandler.parseCompositeToRow(s, typeName);
    }

    Object coerceFieldValue(String val, String typeName) {
        return compositeTypeHandler.coerceFieldValue(val, typeName);
    }

    void validateOperatorTypes(BinaryExpr.BinOp op, Object left, Object right) {
        constraintValidator.validateOperatorTypes(op, left, right);
    }

    void validateWhereTypesAgainstTable(Expression where, Table table) {
        constraintValidator.validateWhereTypesAgainstTable(where, table);
    }

    Object applyCast(Object val, String typeSpec) {
        return castEvaluator.applyCast(val, typeSpec);
    }

    Object evalBinaryValues(BinaryExpr.BinOp op, Object left, Object right) {
        return binaryOpEvaluator.evalBinaryValues(op, left, right);
    }

    String formatArrayForOutput(List<?> elements) {
        return arrayOperationHandler.formatArrayForOutput(elements);
    }

    Object evalUnaryValue(UnaryExpr.UnaryOp op, Object val) {
        return exprEvaluator.evalUnaryValue(op, val);
    }

    List<Object> parsePostgresArrayLiteral(String s) {
        return arrayOperationHandler.parsePostgresArrayLiteral(s);
    }

    void validateCaseBranchTypesForPrepare(CaseExpr c) {
        exprEvaluator.validateCaseBranchTypesForPrepare(c);
    }

    /** Marker record for ROW(...) values, formatted as (v1,v2,...) instead of {v1,v2,...}. */
        public static final class PgRow {
        public final List<Object> values;

        public PgRow(List<Object> values) {
            this.values = values;
        }

        public List<Object> values() { return values; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgRow that = (PgRow) o;
            return java.util.Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(values);
        }

        @Override
        public String toString() {
            return "PgRow[values=" + values + "]";
        }
    }

    /** Marker record for bit string values, e.g., B'1010'. Prevents implicit coercion with other types. */
        public static final class PgBitString {
        public final String bits;

        public PgBitString(String bits) {
            this.bits = bits;
        }

        @Override public String toString() { return bits; }

        public String bits() { return bits; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgBitString that = (PgBitString) o;
            return java.util.Objects.equals(bits, that.bits);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(bits);
        }
    }

    /** Marker record for ENUM values, compared by ordinal position, not alphabetically. */
        public static final class PgEnum implements Comparable<PgEnum> {
        public final String label;
        public final String typeName;
        public final int ordinal;

        public PgEnum(String label, String typeName, int ordinal) {
            this.label = label;
            this.typeName = typeName;
            this.ordinal = ordinal;
        }

        @Override public int compareTo(PgEnum other) { return Integer.compare(ordinal, other.ordinal); }
        @Override public String toString() { return label; }

        public String label() { return label; }
        public String typeName() { return typeName; }
        public int ordinal() { return ordinal; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgEnum that = (PgEnum) o;
            return java.util.Objects.equals(label, that.label)
                && java.util.Objects.equals(typeName, that.typeName)
                && ordinal == that.ordinal;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(label, typeName, ordinal);
        }
    }

    // ---- Helpers ----

    void recordUndo(Session.UndoEntry entry) {
        if (session != null) {
            session.recordUndo(entry);
        }
    }

    static String toBitStringOrNull(Object val) {
        return ExprEvaluator.toBitStringOrNull(val);
    }

    static String bitwiseBitString(String a, String b, char op) {
        return ExprEvaluator.bitwiseBitString(a, b, op);
    }

    long toLong(Object val) {
        return exprEvaluator.toLong(val);
    }

    String defaultSchema() {
        return session != null ? session.getEffectiveSchema() : "public";
    }

    String sessionUser() {
        if (session != null && session.getConnectingUser() != null) {
            return session.getConnectingUser();
        }
        return "memgres";
    }

    Table resolveTable(String schemaName, String tableName) {
        String tempSchemaName = session != null ? session.getTempSchemaName() : "pg_temp";
        // Resolve pg_temp alias to the actual session temp schema
        if ("pg_temp".equalsIgnoreCase(schemaName)) {
            Schema pgTemp = database.getSchema(tempSchemaName);
            if (pgTemp != null) {
                Table tempTable = pgTemp.getTable(tableName);
                if (tempTable != null) return tempTable;
            }
            throw new MemgresException("relation \"" + schemaName + "." + tableName + "\" does not exist", "42P01");
        }
        Schema pgTemp = database.getSchema(tempSchemaName);
        if (pgTemp != null) {
            Table tempTable = pgTemp.getTable(tableName);
            if (tempTable != null) return tempTable;
        }
        Schema schema = schemaName != null ? database.getSchema(schemaName) : null;
        if (schema != null) {
            Table table = schema.getTable(tableName);
            if (table != null) return table;
        }
        if (session != null) {
            String searchPath = session.getGucSettings().get("search_path");
            if (searchPath != null) {
                for (String sp : searchPath.split(",")) {
                    String s = sp.trim().replace("\"", "").replace("'", "");
                    if (s.equals("$user")) continue;
                    Schema spSchema = database.getSchema(s);
                    if (spSchema != null) {
                        Table table = spSchema.getTable(tableName);
                        if (table != null) return table;
                    }
                }
            }
        }
        if (schema == null && schemaName != null && !"pg_catalog".equalsIgnoreCase(schemaName)
                && !"information_schema".equalsIgnoreCase(schemaName)) {
            throw new MemgresException("schema \"" + schemaName + "\" does not exist", "3F000");
        }
        Database.ViewDef view = database.getView(tableName);
        if (view != null) {
            Table underlying = resolveViewToBaseTable(view);
            if (underlying != null) return underlying;
            throw new MemgresException("cannot insert into view \"" + tableName + "\"", "55000");
        }
        throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
    }

    Table resolveTableSafe(String tableName) {
        try {
            return resolveTable(defaultSchema(), tableName);
        } catch (MemgresException e) {
            return null;
        }
    }

    private Table resolveViewToBaseTable(Database.ViewDef view) {
        if (!(view.query() instanceof SelectStmt)) return null;
        SelectStmt sel = (SelectStmt) view.query();
        if (sel.from() == null || sel.from().size() != 1) return null;
        if (!(sel.from().get(0) instanceof SelectStmt.TableRef)) return null;
        SelectStmt.TableRef ref = (SelectStmt.TableRef) sel.from().get(0);
        if (sel.distinct()) return null;
        if (sel.groupBy() != null && !sel.groupBy().isEmpty()) return null;
        if (sel.having() != null) return null;
        if (sel.limit() != null || sel.offset() != null) return null;
        String refSchema = ref.schema() != null ? ref.schema() : defaultSchema();
        try { return resolveTable(refSchema, ref.table()); } catch (MemgresException e) { return null; }
    }

    Table resolveTableAnySchema(String tableName) {
        String defSchema = defaultSchema();
        if (defSchema != null) {
            Schema ds = database.getSchema(defSchema);
            if (ds != null) {
                Table t = ds.getTable(tableName);
                if (t != null) return t;
            }
        }
        Schema pub = database.getSchema("public");
        if (pub != null) {
            Table t = pub.getTable(tableName);
            if (t != null) return t;
        }
        for (Schema schema : database.getSchemas().values()) {
            Table t = schema.getTable(tableName);
            if (t != null) return t;
        }
        throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
    }

    Object evaluateDefault(String defaultExpr, DataType type) {
        return evaluateDefault(defaultExpr, type, null);
    }

    Object evaluateDefault(String defaultExpr, DataType type, Expression parsedExpr) {
        if (defaultExpr != null) {
            String lower = defaultExpr.toLowerCase().trim();
            if (lower.equals("uuid_generate_v4()") || lower.equals("gen_random_uuid()")) {
                return java.util.UUID.randomUUID();
            }
            if (lower.equals("now()") || lower.equals("current_timestamp")) {
                if (type == DataType.DATE) return LocalDate.now();
                if (type == DataType.TIMESTAMP) return LocalDateTime.now();
                return OffsetDateTime.now();
            }
        }
        if (parsedExpr != null) {
            try {
                return evalExpr(parsedExpr, null);
            } catch (Exception e) {
                // Fall through to string-based parsing
            }
        }
        if (defaultExpr != null) {
            try {
                Expression expr = new Parser(new Lexer(defaultExpr).tokenize()).parseExpression();
                return evalExpr(expr, null);
            } catch (Exception e) {
                return defaultExpr;
            }
        }
        return null;
    }

    // ---- Expression alias & type inference (delegated to ExprEvaluator) ----

    String exprToAlias(Expression expr) {
        return exprEvaluator.exprToAlias(expr);
    }

    DataType inferTypeFromContext(Expression expr, List<RowContext.TableBinding> bindings) {
        return exprEvaluator.inferTypeFromContext(expr, bindings);
    }

    DataType inferExprType(Expression expr) {
        return exprEvaluator.inferExprType(expr);
    }

    @SuppressWarnings("unchecked")
    int compareValues(Object a, Object b) {
        return exprEvaluator.compareValues(a, b);
    }

    boolean isTruthy(Object val) {
        return exprEvaluator.isTruthy(val);
    }

    static String likeToRegex(String likePattern) {
        return ExprEvaluator.likeToRegex(likePattern);
    }

    boolean isTruthyStrict(Object val) {
        return exprEvaluator.isTruthyStrict(val);
    }

    Object numericOp(Object left, Object right,
                             java.util.function.BiFunction<Double, Double, Double> doubleOp,
                             java.util.function.BiFunction<Long, Long, Long> longOp) {
        return exprEvaluator.numericOp(left, right, doubleOp, longOp);
    }

    Object numericOp(Object left, Object right,
                             java.util.function.BiFunction<Double, Double, Double> doubleOp,
                             java.util.function.BiFunction<Long, Long, Long> longOp,
                             java.util.function.BiFunction<java.math.BigDecimal, java.math.BigDecimal, java.math.BigDecimal> bdOp) {
        return exprEvaluator.numericOp(left, right, doubleOp, longOp, bdOp);
    }

    double toDouble(Object val) {
        return exprEvaluator.toDouble(val);
    }

    int toInt(Object val) {
        return exprEvaluator.toInt(val);
    }

    // ---- Date/time arithmetic (delegated to DateTimeArithmetic) ----

    Object dateTimeAdd(Object left, Object right) {
        return dateTimeArithmetic.dateTimeAdd(left, right);
    }

    Object dateTimeSubtract(Object left, Object right) {
        return dateTimeArithmetic.dateTimeSubtract(left, right);
    }

    Object numericOrIntervalMul(Object left, Object right) {
        return dateTimeArithmetic.numericOrIntervalMul(left, right);
    }

    List<String> parseJsonPathArg(Object right) {
        return exprEvaluator.parseJsonPathArg(right);
    }

    // ---- ALTER FUNCTION / ALTER PROCEDURE ----

    /**
     * Resolve the function/procedure targeted by an ALTER FUNCTION/PROCEDURE statement,
     * matching by name and optionally by parameter type signature.
     */
    private PgFunction resolveAlterFunction(AlterFunctionStmt stmt) {
        java.util.List<String> paramTypes = stmt.paramTypes();
        if (paramTypes != null) {
            // Signature-based resolution: match by name and parameter types
            java.util.List<PgFunction> overloads = stmt.schema() != null
                ? database.getFunctionOverloads(stmt.name()).stream()
                    .filter(f -> stmt.schema().equalsIgnoreCase(f.getSchemaName()))
                    .collect(java.util.stream.Collectors.toList())
                : database.getFunctionOverloads(stmt.name());
            for (PgFunction f : overloads) {
                java.util.List<String> fTypes = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(java.util.stream.Collectors.toList());
                if (fTypes.size() != paramTypes.size()) continue;
                boolean match = true;
                for (int i = 0; i < fTypes.size(); i++) {
                    if (!database.typesCompatible(fTypes.get(i), paramTypes.get(i))) {
                        match = false;
                        break;
                    }
                }
                if (match) return f;
            }
            return null; // no matching overload
        }
        return stmt.schema() != null
            ? database.getFunction(stmt.schema(), stmt.name())
            : database.getFunction(stmt.name());
    }

    private QueryResult executeAlterFunction(AlterFunctionStmt stmt) {
        String tag = stmt.commandTag();
        String kind = stmt.isProcedure() ? "procedure" : "function";

        switch (stmt.action()) {
            case RENAME_TO: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + stmt.name() + " does not exist", "42883");
                }
                // Check for name conflict: target name must not already exist with compatible signature
                java.util.List<PgFunction> existingTarget = database.getFunctionOverloads(stmt.targetValue());
                if (!existingTarget.isEmpty()) {
                    // Check if there's a conflict (same param types)
                    java.util.List<String> funcParamTypes = func.getParams().stream()
                        .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                        .map(PgFunction.Param::typeName)
                        .collect(java.util.stream.Collectors.toList());
                    for (PgFunction existing : existingTarget) {
                        java.util.List<String> existingParamTypes = existing.getParams().stream()
                            .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                            .map(PgFunction.Param::typeName)
                            .collect(java.util.stream.Collectors.toList());
                        if (funcParamTypes.size() == existingParamTypes.size()) {
                            boolean match = true;
                            for (int i = 0; i < funcParamTypes.size(); i++) {
                                if (!database.typesCompatible(funcParamTypes.get(i), existingParamTypes.get(i))) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match) {
                                throw new MemgresException(kind + " " + stmt.targetValue() + " already exists", "42723");
                            }
                        }
                    }
                }
                // Rename only this specific overload, not all overloads
                database.renameFunctionOverload(func, stmt.targetValue());
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case SET_SCHEMA: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + stmt.name() + " does not exist", "42883");
                }
                String oldSchema = func.getSchemaName() != null ? func.getSchemaName() : "public";
                String newSchema = stmt.targetValue();
                if (database.getSchema(newSchema) == null) {
                    throw new MemgresException("schema \"" + newSchema + "\" does not exist", "3F000");
                }
                func.setSchemaName(newSchema);
                // Update schema registry
                Set<String> oldObjects = database.getSchemaObjects(oldSchema);
                oldObjects.remove("function:" + stmt.name().toLowerCase());
                database.registerSchemaObject(newSchema, "function", stmt.name());
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case OWNER_TO: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + stmt.name() + " does not exist", "42883");
                }
                String newOwner = ddlExecutor.resolveOwnerName(stmt.targetValue());
                if (!database.hasRole(newOwner)) {
                    throw new MemgresException("role \"" + stmt.targetValue() + "\" does not exist", "42704");
                }
                database.setObjectOwner("function:" + stmt.name(), newOwner);
                func.setOwner(newOwner);
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case SET_ATTRIBUTES: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + stmt.name() + " does not exist", "42883");
                }
                // Record undo for transactional rollback
                if (session != null && session.getStatus() == Session.TransactionStatus.IN_TRANSACTION) {
                    final String oldVolatility = func.getVolatility();
                    final boolean oldStrict = func.isStrict();
                    final boolean oldSecDef = func.isSecurityDefiner();
                    final boolean oldLeakproof = func.isLeakproof();
                    final double oldCost = func.getCost();
                    final double oldRows = func.getRows();
                    final String oldParallel = func.getParallel();
                    final java.util.Map<String, String> oldSetClauses = func.getSetClauses() != null
                        ? new java.util.LinkedHashMap<>(func.getSetClauses()) : null;
                    final PgFunction undoFunc = func;
                    session.recordUndo(db -> {
                        undoFunc.setVolatility(oldVolatility);
                        undoFunc.setStrict(oldStrict);
                        undoFunc.setSecurityDefiner(oldSecDef);
                        undoFunc.setLeakproof(oldLeakproof);
                        undoFunc.setCost(oldCost);
                        undoFunc.setRows(oldRows);
                        undoFunc.setParallel(oldParallel);
                        undoFunc.setSetClauses(oldSetClauses);
                    });
                }
                if (stmt.volatility() != null) func.setVolatility(stmt.volatility());
                if (stmt.strict() != null) func.setStrict(stmt.strict());
                if (stmt.securityDefiner() != null) func.setSecurityDefiner(stmt.securityDefiner());
                if (stmt.leakproof() != null) func.setLeakproof(stmt.leakproof());
                if (stmt.cost() != null) func.setCost(stmt.cost());
                // ROWS: only store for set-returning functions (proretset=true); PG ignores ROWS for scalar functions
                if (stmt.rows() != null && func.getReturnType() != null
                        && (func.getReturnType().toUpperCase().startsWith("SETOF")
                            || func.getReturnType().toUpperCase().contains("TABLE"))) {
                    func.setRows(stmt.rows());
                }
                if (stmt.parallel() != null) func.setParallel(stmt.parallel());
                if (stmt.setClauses() != null) {
                    java.util.Map<String, String> existing = func.getSetClauses();
                    if (existing == null) existing = new java.util.LinkedHashMap<>();
                    existing.putAll(stmt.setClauses());
                    func.setSetClauses(existing);
                }
                if (stmt.resetParams() != null) {
                    java.util.Map<String, String> existing = func.getSetClauses();
                    if (existing != null) {
                        for (String p : stmt.resetParams()) {
                            if ("ALL".equals(p)) {
                                existing.clear();
                            } else {
                                existing.remove(p);
                            }
                        }
                        func.setSetClauses(existing.isEmpty() ? null : existing);
                    }
                }
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            default:
                return QueryResult.message(QueryResult.Type.SET, tag);
        }
    }

    /** Rename a PK/UNIQUE constraint-backed index. Returns true if found and renamed. */
    private boolean renameConstraintIndex(String oldName, String newName) {
        String oldLower = oldName.toLowerCase();
        for (java.util.Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
            for (java.util.Map.Entry<String, Table> te : se.getValue().getTables().entrySet()) {
                for (StoredConstraint sc : te.getValue().getConstraints()) {
                    if ((sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE)
                            && sc.getName().toLowerCase().equals(oldLower)) {
                        sc.setName(newName);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // ---- ALTER INDEX ----

    private QueryResult executeAlterIndex(AlterIndexStmt stmt) {
        switch (stmt.action()) {
            case RENAME_TO: {
                boolean found = database.hasIndex(stmt.name());
                if (!found) {
                    // Also check PK/UNIQUE constraint-backed indexes
                    found = renameConstraintIndex(stmt.name(), stmt.targetValue());
                }
                if (!found) {
                    if (database.hasIndex(stmt.name())) found = true;
                }
                if (!found) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                if (database.hasIndex(stmt.name())) {
                    if (database.hasIndex(stmt.targetValue())) {
                        throw new MemgresException("relation \"" + stmt.targetValue() + "\" already exists", "42P07");
                    }
                    database.renameIndex(stmt.name(), stmt.targetValue());
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
            }
            case SET_PARAMS: {
                if (!database.hasIndex(stmt.name())) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                if (stmt.params != null && !stmt.params.isEmpty()) {
                    java.util.Map<String, String> existing = database.getIndexReloptions(stmt.name());
                    java.util.Map<String, String> merged = existing != null ? new java.util.LinkedHashMap<>(existing) : new java.util.LinkedHashMap<>();
                    merged.putAll(stmt.params);
                    database.setIndexReloptions(stmt.name(), merged);
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
            }
            default:
                // All other actions (SET TABLESPACE, ATTACH PARTITION, etc.) are accepted no-ops
                if (stmt.action() != AlterIndexStmt.Action.NO_OP
                        && !database.hasIndex(stmt.name()) && !stmt.ifExists()) {
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
        }
    }
}
