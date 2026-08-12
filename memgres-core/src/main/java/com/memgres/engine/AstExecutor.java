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
    /**
     * Where a select leaves the stored rows each of its answers came from, when someone has asked
     * for them. DECLARE CURSOR asks, because WHERE CURRENT OF names a row and not a set of values:
     * a cursor over {@code SELECT nm FROM t ORDER BY id DESC} is on one particular row of t, and
     * two rows with the same nm are still two rows.
     */
    List<List<RowContext.TableBinding>> cursorRowProvenance;

    final SystemCatalog systemCatalog;
    public SystemCatalog getSystemCatalog() { return systemCatalog; }
    final ArrayOperationHandler arrayOperationHandler = new ArrayOperationHandler(this);
    final BinaryOpEvaluator binaryOpEvaluator = new BinaryOpEvaluator(this);
    final SubscriptEvaluator subscriptEvaluator = new SubscriptEvaluator(this);
    final SubscriptAssign subscriptAssign = new SubscriptAssign(this);
    final CompositeTypeHandler compositeTypeHandler = new CompositeTypeHandler(this);
    final DateTimeArithmetic dateTimeArithmetic = new DateTimeArithmetic(this);
    final FunctionEvaluator functionEvaluator = new FunctionEvaluator(this);
    final CastEvaluator castEvaluator = new CastEvaluator(this);
    final ConstraintValidator constraintValidator = new ConstraintValidator(this);
    final SessionExecutor sessionExecutor = new SessionExecutor(this);
    final DmlExecutor dmlExecutor = new DmlExecutor(this);
    final DdlExecutor ddlExecutor = new DdlExecutor(this);
    Long lastSequenceValue = null; // for lastval()
    /** Which sequence produced {@link #lastSequenceValue}, so lastval can tell whether it is gone. */
    long lastSequenceInstanceId = 0;
    /**
     * The value each sequence last produced <em>for this session</em>. currval reports what this
     * connection drew, so it cannot be answered from state the sequence shares with every other
     * connection — doing so hands one caller another caller's generated key.
     *
     * <p>Keyed by the sequence's identity rather than its name, as PostgreSQL keys it by relid: a
     * rename carries the entry along, and a DROP followed by a CREATE of the same name starts over
     * with currval undefined instead of inheriting the dropped sequence's value.
     */
    final java.util.Map<Long, Long> sessionSequenceValues = new java.util.HashMap<>();

    /**
     * Forget every sequence value this session drew, which is what DISCARD SEQUENCES drops:
     * currval and lastval are undefined again and no reserved CACHE block is still held.
     */
    void clearSequenceState() {
        sessionSequenceValues.clear();
        lastSequenceValue = null;
        lastSequenceInstanceId = 0;
        if (session != null) session.clearSequenceCache();
    }
    final FromResolver fromResolver = new FromResolver(this);
    /** The types a relation's definition settles for its columns, read from the definition. */
    final DefinedTypes definedTypes = new DefinedTypes(this);
    /** How deep a statement run from inside another statement is. */
    private int executeDepth;
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
    // The same items by identity: a nested WITH clause may declare the same name for a different
    // item, and that inner item is readable while the outer one of that name is still running.
    final Set<SelectStmt.CommonTableExpr> executingCteNodes =
            Collections.newSetFromMap(new IdentityHashMap<SelectStmt.CommonTableExpr, Boolean>());
    // Bound parameter values for extended query protocol ($1, $2, ...)
    List<Object> boundParameters = new ArrayList<>();
    // Statement timestamp: frozen at statement start for now()/statement_timestamp()
    OffsetDateTime currentStatementTimestamp = null;

    /**
     * The instant every "current" date/time reads from. PG derives CURRENT_DATE, CURRENT_TIME,
     * LOCALTIMESTAMP and now() from one transaction timestamp, so they cannot disagree with each
     * other part-way through a transaction that spans midnight.
     */
    OffsetDateTime currentInstant() {
        if (session != null && session.getTransactionTimestamp() != null) {
            return session.getTransactionTimestamp();
        }
        return currentStatementTimestamp != null ? currentStatementTimestamp : OffsetDateTime.now();
    }
    // Current MERGE action for merge_action() function in RETURNING clause (PG 17+)
    String currentMergeAction = null;
    // Raw SQL text of the current top-level statement (for pg_prepared_statements/pg_cursors verbatim display)
    String currentRawSql = null;
    // View column mapping: view_column_name -> base_table_column_name (set by resolveViewToBaseTable)
    Map<String, String> lastViewColumnMapping = null;
    // Ordered base-table column names, one per view-column position (set by resolveViewToBaseTable).
    // Used to map a positional INSERT through a reordered/renamed/subset view onto the base table.
    // Null when the view target list has any non-simple-column expression (e.g. SELECT *).
    List<String> lastViewColumnOrder = null;

    /**
     * The view's own column names, one per view-column position, the computed ones included. A
     * positional write names nothing, so this is the only list that can say which view column a
     * value was aimed at.
     */
    List<String> lastViewColumnNames = null;

    /**
     * View columns whose target is an expression rather than a plain column reference.
     * PG allows such a view to be updatable but rejects assigning to those columns with
     * 0A000 ("cannot update column ... of view ...").
     */
    Set<String> lastViewExpressionColumns = null;

    /**
     * The qualification of each view a write is being rewritten through, innermost first.
     * PostgreSQL rewrites an UPDATE or a DELETE on an auto-updatable view into one on the base
     * relation with the view's own WHERE added to the statement's, so a write through the view
     * reaches only the rows the view shows. Null when the target is a plain table.
     */
    List<ViewQual> lastViewQuals = null;

    /**
     * One view's WHERE, kept with the name its own FROM item answers to and the column renaming
     * in force below it. A view over a view writes its condition in the names the relation it
     * reads exposes, which are not always the base table's.
     */
    static final class ViewQual {
        final Expression expr;
        final String relationName;
        final Map<String, String> columnNames;

        ViewQual(Expression expr, String relationName, Map<String, String> columnNames) {
            this.expr = expr;
            this.relationName = relationName;
            this.columnNames = columnNames;
        }
    }

    /**
     * The phrase PG uses when refusing DML on a non-updatable view: "insert into",
     * "update" or "delete from". Set by the DML executor before it resolves the target.
     */
    String viewDmlVerb = "insert into";

    /**
     * Whether that write is a MERGE. PostgreSQL's advice for a MERGE names an INSTEAD OF trigger
     * and nothing else, because no rule can stand in for a MERGE the way one stands in for an
     * INSERT or an UPDATE, so the verb alone does not say enough to word the Hint.
     */
    boolean viewDmlByMerge;

    /** Views whose body is currently being expanded, and rules currently being applied. */
    private final Set<String> expansionsInProgress = new HashSet<>();

    /**
     * Run a view's body, refusing a view that is defined — directly or through other views — in
     * terms of itself. Expanding it again would never finish, and PostgreSQL names the relation
     * it came back to rather than letting the recursion run.
     */
    QueryResult executeViewQuery(String viewName, Statement viewQuery) {
        String key = "view:" + viewName.toLowerCase();
        if (!expansionsInProgress.add(key)) {
            throw PgErrors.infiniteRecursionInRules(viewName);
        }
        try {
            return executeStatement(viewQuery);
        } finally {
            expansionsInProgress.remove(key);
        }
    }

    /**
     * Claim the right to apply a rule for this relation and event. A rule that rewrites onto its
     * own table would otherwise re-enter itself for as long as the stack lasted.
     */
    boolean enterRuleExpansion(String relation, String event) {
        return expansionsInProgress.add("rule:" + event + ":" + relation.toLowerCase());
    }

    void exitRuleExpansion(String relation, String event) {
        expansionsInProgress.remove("rule:" + event + ":" + relation.toLowerCase());
    }

    boolean isRuleExpanding(String relation, String event) {
        return expansionsInProgress.contains("rule:" + event + ":" + relation.toLowerCase());
    }

    /**
     * The schema holding the relation a name reaches, which is what says which relation a rule
     * or a trigger written on that name belongs to.
     *
     * <p>A rule belongs to the relation rather than to its name: two schemas may each hold a
     * relation called {@code t} and each carries its own rules, so a write has to look for them
     * where the name it wrote reaches. A name written with no schema reaches the relation the
     * search path reaches, and the temporary schema comes first for a relation unless the path
     * says where it stands -- the same order the name itself is resolved in.
     */
    String relationSchemaOf(String writtenSchema, String relation) {
        String bare = relation == null ? "" : RelationNamespace.bareName(relation);
        int dot = relation == null ? -1 : relation.lastIndexOf('.');
        String written = writtenSchema != null ? writtenSchema
                : (dot > 0 ? relation.substring(0, dot) : null);
        String temp = session != null ? session.getTempSchemaName() : null;
        if (written != null) {
            return "pg_temp".equalsIgnoreCase(written) && temp != null ? temp : written;
        }
        List<String> order = new ArrayList<>();
        if (temp != null && !searchPathNamesTemp()) order.add(temp);
        for (String entry : searchPathSchemas()) {
            order.add("pg_temp".equalsIgnoreCase(entry) && temp != null ? temp : entry);
        }
        for (String schemaName : order) {
            if (RelationNamespace.kindOf(database, schemaName, bare) != null) return schemaName;
        }
        return defaultSchema();
    }

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
        // Where the statement begins in the text handed over. An error's position is reported
        // against that text, and the parser is given only what is left after the trim.
        int textOffset = 0;
        while (textOffset < sql.length() && sql.charAt(textOffset) <= ' ') textOffset++;
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
        // H37: publish the session DateStyle field order so date input parsing honors DMY/YMD/MDY
        String previousDateOrder = TypeCoercion.getDateOrder();
        TypeCoercion.setDateOrder(currentDateStyleOrder());
        // ... and the session TimeZone, which decides what "today" and CURRENT_DATE mean
        java.time.ZoneId previousZone = TypeCoercion.rawSessionZone();
        TypeCoercion.setSessionZone(currentSessionZone());
        OffsetDateTime previousInstant = TypeCoercion.rawSessionInstant();
        TypeCoercion.setSessionInstant(currentInstant());
        try {
            List<String> typeSchemas = new ArrayList<String>();
            Statement stmt = Parser.parse(sql, typeSchemas);
            if (stmt == null) return QueryResult.empty(); // empty input (only comments)
            rejectQualifiedTypeSchemas(stmt, typeSchemas);
            rejectNestedDataModifyingCtes(stmt);
            rejectMisplacedDefault(stmt, textOffset);
            // The FULL JOIN restriction is asked of the statement the client sent and of nothing
            // else; a statement run from inside one — a function body, a catalog lookup — is not
            // the outermost query, so what it holds is left alone. See FullJoinAdmissibility.
            Statement priorOutermost = fromResolver.outermostQuery;
            boolean outermost = executeDepth == 0;
            if (outermost) fromResolver.outermostQuery = stmt instanceof SelectStmt ? stmt : null;
            // A catalog relation is the same relation everywhere in one statement, so it is
            // built once and reused; without this a correlated subquery over pg_proc rebuilds
            // the whole of it for every outer row.
            if (outermost) {
                systemCatalog.beginStatement();
                existsIndexes.clear();
                baseTablesByName.clear();
                scalarSubqueries.clear();
            }
            executeDepth++;
            try {
                return executeStatement(stmt);
            } finally {
                executeDepth--;
                if (outermost) {
                    fromResolver.outermostQuery = priorOutermost;
                    systemCatalog.endStatement();
                    existsIndexes.clear();
                    baseTablesByName.clear();
                    scalarSubqueries.clear();
                }
            }
        } finally {
            this.boundParameters = previousParams;
            this.currentRawSql = previousRawSql;
            currentStatementTimestamp = null;
            TypeCoercion.setDateOrder(previousDateOrder);
            TypeCoercion.setSessionZone(previousZone);
            TypeCoercion.setSessionInstant(previousInstant);
        }
    }

    /**
     * A type name written under a schema that does not exist, which PostgreSQL refuses before it
     * runs anything — wherever the type stands, and whether or not the expression holding it would
     * ever have been evaluated. {@code CASE WHEN false THEN 1::nosuch.int4 END} and a WITH item
     * nothing selects from are both refused, because the type is resolved while the statement is
     * analysed rather than while it runs.
     *
     * <p>The range table is built first, though, so a relation the statement names and does not
     * have is still what {@code SELECT 1::nosuch.int4 FROM nosuchtable} reports. That lookup is
     * asked for here only when a qualifier has already turned out to be missing, so a statement
     * whose type names all resolve — which is every statement that writes no qualifier at all —
     * reaches the engine by exactly the path it did before.
     */
    private void rejectQualifiedTypeSchemas(Statement stmt, List<String> typeSchemas) {
        String missing = SchemaQualifier.firstMissing(database, session, typeSchemas);
        if (missing != null) {
            fromResolver.checkRelationNamesExist(stmt);
            throw SchemaQualifier.missing(missing);
        }
        // The schema is there; whether it holds the type is the second half of the same lookup.
        String unknown = SchemaQualifier.firstUnknownType(database, systemCatalog, typeSchemas);
        if (unknown == null) return;
        fromResolver.checkRelationNamesExist(stmt);
        throw SchemaQualifier.noSuchType(unknown);
    }

    /**
     * A data-modifying statement may only appear in the statement's outermost WITH list. Deeper
     * down neither when the write happens nor what the surrounding query sees is defined, so PG
     * refuses to run the query at all rather than pick an order.
     */
    private static void rejectNestedDataModifyingCtes(Statement root) {
        Set<Object> topLevel = Collections.newSetFromMap(new java.util.IdentityHashMap<Object, Boolean>());
        collectTopLevelCtes(root, topLevel);
        Object nested = AstWalk.findFirst(root, node -> {
            List<SelectStmt.CommonTableExpr> ctes = ownCtes(node);
            if (ctes == null || topLevel.contains(ctes)) return false;
            for (SelectStmt.CommonTableExpr cte : ctes) {
                if (isDataModifying(cte.query())) return true;
            }
            return false;
        });
        if (nested != null) {
            throw PgErrors.notImplemented(
                    "WITH clause containing a data-modifying statement must be at the top level");
        }
    }

    /**
     * A DEFAULT written where it does not ask a column for its default.
     *
     * <p>The keyword is not a value. It may only be the whole of a value an INSERT supplies for a
     * column or the whole of what an assignment writes — parentheses around it change nothing, and
     * one element of a multi-column assignment counts as the whole of what that element writes.
     * PostgreSQL refuses it anywhere else while it analyses the statement, so an empty relation is
     * refused exactly as a full one is. Judged only where the expression came to be evaluated, the
     * same statement quietly did nothing whenever no row reached the evaluator.
     *
     * <p>Which of a statement's faults it is reported for is a question of order. PostgreSQL reads
     * a statement one clause at a time and an expression's operands left to right, and answers for
     * whatever it meets first — so {@code WHERE nosuchcol = DEFAULT} is a column that is not
     * there, while {@code WHERE DEFAULT = nosuchcol} is this. The reading the protocol layer does
     * before it answers Parse is asked for here too, which is what puts the two complaints in one
     * order however the client sent the statement.
     */
    private void rejectMisplacedDefault(Statement stmt, int textOffset) {
        if (AstWalk.findFirst(stmt, node -> MisplacedDefault.isKeyword(node)) == null) return;
        Set<Object> standing = MisplacedDefault.standingPlaces(stmt);
        Literal keyword = MisplacedDefault.anywhere(stmt, standing);
        if (keyword == null) return;
        Literal inReadingOrder = MisplacedDefault.first(stmt, standing);
        if (inReadingOrder != null) keyword = inReadingOrder;
        // The range table is built before the statement's expressions are read, and the columns of
        // a clause are resolved as the clause is read, so a name that reaches nothing is what
        // PostgreSQL reports about whenever it stands earlier in the reading than the keyword.
        try {
            analyzeWithoutRunning(stmt, textOffset);
            fromResolver.checkRelationNamesExist(stmt);
        } catch (MemgresException reported) {
            throw reported;
        } catch (RuntimeException | StackOverflowError unreadable) {
            // Reading a statement is not the place to invent a failure. Whatever the reading could
            // not make sense of leaves the keyword itself as the answer, which is what it was.
        }
        throw MisplacedDefault.error(keyword, textOffset);
    }

    /** The WITH lists that count as the statement's own, including each arm of a set operation. */
    private static void collectTopLevelCtes(Statement stmt, Set<Object> out) {
        if (stmt == null) return;
        // EXPLAIN describes a statement; it does not put one inside another. The statement it
        // describes is still the outermost one, and reading it as nested refused a query
        // PostgreSQL explains happily.
        if (stmt instanceof ExplainStmt) {
            collectTopLevelCtes(((ExplainStmt) stmt).statement, out);
            return;
        }
        if (stmt instanceof SetOpStmt) {
            collectTopLevelCtes(((SetOpStmt) stmt).left, out);
            collectTopLevelCtes(((SetOpStmt) stmt).right, out);
            return;
        }
        List<SelectStmt.CommonTableExpr> ctes = ownCtes(stmt);
        if (ctes != null) out.add(ctes);
    }

    private static List<SelectStmt.CommonTableExpr> ownCtes(Object node) {
        if (node instanceof SelectStmt) return ((SelectStmt) node).withClauses();
        if (node instanceof InsertStmt) return ((InsertStmt) node).withClauses();
        if (node instanceof UpdateStmt) return ((UpdateStmt) node).withClauses();
        if (node instanceof DeleteStmt) return ((DeleteStmt) node).withClauses();
        return null;
    }

    private static boolean isDataModifying(Statement stmt) {
        return stmt instanceof InsertStmt || stmt instanceof UpdateStmt || stmt instanceof DeleteStmt
                || stmt instanceof MergeStmt;
    }

    /**
     * Whether this statement has a relation it writes rows to. A SELECT that locks the rows it
     * reads is one of them: PostgreSQL writes the lock into each tuple it takes, so FOR UPDATE
     * moves the command counter exactly as a write does. See {@link #executeStatement}.
     */
    private static boolean writesRows(Statement stmt) {
        if (stmt instanceof ExplainStmt) return false;
        if (stmt instanceof SelectStmt) return ((SelectStmt) stmt).lockClause() != null;
        return isDataModifying(stmt) || stmt instanceof CopyStmt;
    }

    /**
     * How many rows a statement of this shape writes into the catalogue.
     *
     * <p>PostgreSQL spends a command identifier on each of them, so a statement that takes a
     * relation down spends more than one: dropping a table retires the relation, its composite
     * type and that type's array type, and dropping a view retires its rewrite rule beside those
     * three. Creating a view writes the relation and the rule; creating a sequence writes the
     * relation and the row holding its state. A name nothing answers to costs nothing, which is
     * why DROP ... IF EXISTS over a relation that is not there leaves the counter alone. Zero
     * here leaves the statement to the rule that a write takes exactly one.
     */
    private int catalogRowsWrittenBy(Statement stmt) {
        if (stmt instanceof CreateViewStmt) {
            return ((CreateViewStmt) stmt).materialized ? 0 : 2;
        }
        if (stmt instanceof CreateSequenceStmt) {
            CreateSequenceStmt create = (CreateSequenceStmt) stmt;
            if (create.ifNotExists() && database.getSequence(create.name()) != null) return 0;
            return 2;
        }
        if (stmt instanceof DropTableStmt) {
            DropTableStmt drop = (DropTableStmt) stmt;
            int relations = relationsBehind(drop.schema(), drop.name());
            for (String also : drop.additionalTables()) relations += relationsBehind(null, also);
            return 3 * relations;
        }
        if (stmt instanceof DropStmt) {
            int rows = 0;
            for (DropStmt one : DropStmt.allOf((DropStmt) stmt)) {
                if (one.objectType() != DropStmt.ObjectType.VIEW) continue;
                if (database.getView(one.schema(), one.name()) != null) rows += 4;
            }
            return rows;
        }
        return 0;
    }

    /**
     * How many relations a DROP TABLE of this name takes down: the relation itself and, where it
     * is partitioned, every partition below it, because those go with it.
     */
    private int relationsBehind(String schema, String name) {
        if (name == null) return 0;
        String bare = name;
        String where = schema;
        int dot = bare.indexOf('.');
        if (where == null && dot > 0) {
            where = bare.substring(0, dot);
            bare = bare.substring(dot + 1);
        }
        Table found = null;
        if (where != null) {
            Schema holder = database.getSchema(where.toLowerCase());
            found = holder == null ? null : holder.getTable(bare.toLowerCase());
        } else {
            for (String path : relationSearchPath()) {
                Schema holder = database.getSchema(path);
                found = holder == null ? null : holder.getTable(bare.toLowerCase());
                if (found != null) break;
            }
        }
        return found == null ? 0 : countWithPartitions(found);
    }

    private static int countWithPartitions(Table relation) {
        int total = 1;
        for (Table partition : relation.getPartitions()) total += countWithPartitions(partition);
        return total;
    }

    /**
     * H37: extract the DateStyle field order ("MDY"/"DMY"/"YMD") from the session GUC.
     * The stored value is normalized (e.g. "ISO, DMY"); default is "MDY".
     */
    private String currentDateStyleOrder() {
        if (session == null) return "MDY";
        String ds = session.getGucSettings().get("datestyle");
        if (ds == null) return "MDY";
        String lower = ds.toLowerCase();
        if (lower.contains("dmy")) return "DMY";
        if (lower.contains("ymd")) return "YMD";
        return "MDY";
    }

    /**
     * The session TimeZone GUC as a zone. An unset or unrecognised value falls back to the JVM's
     * zone, which is what the engine used before the setting was consulted at all.
     */
    private java.time.ZoneId currentSessionZone() {
        if (session == null) return java.time.ZoneId.systemDefault();
        String tz = session.getGucSettings().get("timezone");
        if (tz == null || tz.isEmpty()) return java.time.ZoneId.systemDefault();
        try {
            return java.time.ZoneId.of(tz);
        } catch (RuntimeException e) {
            return java.time.ZoneId.systemDefault();
        }
    }

    public QueryResult executeStatement(Statement stmt) {
        // Outside a transaction a statement is a transaction of its own, so what it writes has to
        // be undone if it does not finish. Nested calls join this scope rather than opening one.
        if (session == null) return executeStatementInner(stmt);
        session.beginStatementScope();
        // PostgreSQL takes the command identifier when a statement opens the relation it writes to,
        // so a write consumes one whether or not any row turned out to match: an UPDATE that found
        // nothing still moves the counter that the next statement's cmin reports.
        if (writesRows(stmt)) session.noteCommandIdUsed();
        session.noteCatalogRowsWritten(catalogRowsWrittenBy(stmt));
        boolean failed = true;
        try {
            QueryResult result = executeStatementInner(stmt);
            // That transaction of its own now commits, and a DEFERRABLE INITIALLY DEFERRED
            // constraint is checked when it does: after the statement's AFTER triggers and its
            // data-modifying WITH items, never row by row as it runs. The statement is not
            // reported as having succeeded until those checks have passed.
            session.runEndOfStatementDeferredChecks();
            failed = false;
            return result;
        } finally {
            session.endStatementScope(failed);
        }
    }

    private QueryResult executeStatementInner(Statement stmt) {
        // A statement that is not a plain query can change what the catalogs report — a
        // data-modifying CTE, a function body running DDL — so what has been built for this
        // statement so far is dropped on either side of it rather than read again.
        boolean mayChangeCatalog = !(stmt instanceof SelectStmt) && !(stmt instanceof SetOpStmt);
        if (!mayChangeCatalog) return executeReadOrWrite(stmt);
        dropStatementCaches();
        try {
            return executeReadOrWrite(stmt);
        } finally {
            dropStatementCaches();
        }
    }

    /** The key sets built for correlated EXISTS subqueries of the statement now running. */
    private final java.util.IdentityHashMap<ExistsExpr, ExistsKeyIndex> existsIndexes =
            new java.util.IdentityHashMap<>();

    /** The plan for answering this EXISTS from a key set, computed once per statement. */
    ExistsKeyIndex existsKeyIndex(ExistsExpr ex) {
        ExistsKeyIndex idx = existsIndexes.get(ex);
        if (idx == null) {
            idx = ExistsKeyIndex.plan(ex);
            existsIndexes.put(ex, idx);
        }
        return idx;
    }

    /**
     * The one answer a scalar subquery of the statement now running has, once it has been asked.
     *
     * <p>Only kept for subqueries that read nothing outside themselves; see
     * {@link UncorrelatedSubquery} for what that means and how it is decided.
     */
    static final class ScalarSubqueryValue {
        boolean answered;
        Object value;
    }

    /** Stands for a subquery that has to be run again for every row. */
    private static final ScalarSubqueryValue PER_ROW = new ScalarSubqueryValue();

    private final java.util.IdentityHashMap<SubqueryExpr, ScalarSubqueryValue> scalarSubqueries =
            new java.util.IdentityHashMap<>();

    /**
     * Where this subquery's answer is kept for the statement, or null when it has to be run for
     * every row. Which of the two it is, is settled once per statement.
     */
    ScalarSubqueryValue scalarSubqueryValue(SubqueryExpr sq) {
        ScalarSubqueryValue held = scalarSubqueries.get(sq);
        if (held == null) {
            held = UncorrelatedSubquery.readsNothingOutside(sq.subquery(), this)
                    ? new ScalarSubqueryValue() : PER_ROW;
            scalarSubqueries.put(sq, held);
        }
        return held == PER_ROW ? null : held;
    }

    /** Forget everything derived from the database for the statement now running. */
    private void dropStatementCaches() {
        systemCatalog.invalidateStatementCache();
        existsIndexes.clear();
        baseTablesByName.clear();
        scalarSubqueries.clear();
    }

    /** The register that has to be told when an object is created, renamed, moved or dropped. */
    ObjectIdentity identity() {
        return systemCatalog.identity();
    }

    /**
     * What has to be done before a statement creates a relation, and the schemas that already hold
     * one of that name. Asked again once the statement has run, the one schema that has gained a
     * relation of the name is the schema the statement created it in — which is the identity the
     * OID register is told about, without this having to work out for itself where each kind of
     * CREATE puts a relation.
     *
     * @see ObjectIdentity#relationCreated
     */
    private Set<String> beforeCreatingRelation(String name) {
        if (name == null) return java.util.Collections.emptySet();
        String bare = RelationNamespace.bareName(name);
        Set<String> held = new HashSet<>();
        for (String schema : database.getSchemas().keySet()) {
            if (RelationNamespace.kindOf(database, schema, bare) != null) held.add(schema);
        }
        // Nothing anywhere answers to the name, so a comment still filed under it was said about
        // an object that has been dropped. Cleared here, before the CREATE runs, because the
        // CREATE may file comments of its own -- LIKE ... INCLUDING COMMENTS does.
        if (held.isEmpty()) identity().forgetOrphanedComments(bare);
        return held;
    }

    /**
     * Tell the OID register about a relation this statement has just created, so the number the
     * name used to answer with — the one belonging to whatever was dropped from under it — is not
     * handed to the new object. PostgreSQL never reuses an OID.
     */
    private void noteRelationCreated(String name, Set<String> heldBefore) {
        if (name == null) return;
        String bare = RelationNamespace.bareName(name);
        for (String schema : database.getSchemas().keySet()) {
            if (heldBefore.contains(schema)) continue;
            if (RelationNamespace.kindOf(database, schema, bare) == null) continue;
            identity().relationCreated(schema, bare);
        }
    }

    private QueryResult executeReadOrWrite(Statement stmt) {
        checkReadOnlyTransaction(stmt);
        // FILTER belongs to a call that accumulates rows, so a statement is refused for one
        // written anywhere in it -- select list, WHERE, a CTE, a derived table, the SET list of an
        // UPDATE -- before any of it is run. A SELECT is judged where its own relations have been
        // resolved instead (SelectExecutor.executeSelectInner), because PostgreSQL builds the
        // range table before it looks at any clause and reports a missing relation on its own.
        // The range table comes first for a data-modifying statement too, and the relation it
        // writes goes into it before the ones it reads, so both are resolved before the clauses
        // are judged. Only names are resolved and the target is only described: nothing is read
        // and nothing is written by the check itself.
        if (isDataModifying(stmt)) dmlExecutor.checkTargetsResolvable(stmt);
        if (stmt instanceof SetOpStmt || isDataModifying(stmt)) {
            FilterCheck.reject(selectExecutor, stmt, null);
        }
        if (stmt instanceof SelectStmt) return selectExecutor.executeSelect(((SelectStmt) stmt));
        if (stmt instanceof SetOpStmt) return selectExecutor.executeSetOp(((SetOpStmt) stmt));
        if (stmt instanceof InsertStmt) return dmlExecutor.executeInsert(((InsertStmt) stmt));
        if (stmt instanceof UpdateStmt) return dmlExecutor.executeUpdate(((UpdateStmt) stmt));
        if (stmt instanceof DeleteStmt) return dmlExecutor.executeDelete(((DeleteStmt) stmt));
        if (stmt instanceof CreateTableStmt) {
            String tag = getDdlTag(stmt);
            fireEventTriggers("ddl_command_start", tag);
            Set<String> held = beforeCreatingRelation(((CreateTableStmt) stmt).name());
            QueryResult result = ddlExecutor.executeCreateTable(((CreateTableStmt) stmt));
            noteRelationCreated(((CreateTableStmt) stmt).name(), held);
            fireEventTriggers("ddl_command_end", tag);
            return result;
        }
        if (stmt instanceof DropTableStmt) {
            QueryResult result = ddlExecutor.executeDropTable(((DropTableStmt) stmt));
            identity().sweepDead();
            return result;
        }
        if (stmt instanceof CreateTypeStmt) return ddlExecutor.executeCreateType(((CreateTypeStmt) stmt));
        if (stmt instanceof CreateFunctionStmt) return ddlExecutor.executeCreateFunction(((CreateFunctionStmt) stmt));
        if (stmt instanceof CreateAggregateStmt) return ddlExecutor.executeCreateAggregate(((CreateAggregateStmt) stmt));
        if (stmt instanceof CreateOperatorStmt) return ddlExecutor.executeCreateOperator(((CreateOperatorStmt) stmt));
        if (stmt instanceof CreateOperatorFamilyStmt) return ddlExecutor.executeCreateOperatorFamily(((CreateOperatorFamilyStmt) stmt));
        if (stmt instanceof CreateOperatorClassStmt) return ddlExecutor.executeCreateOperatorClass(((CreateOperatorClassStmt) stmt));
        if (stmt instanceof AlterOperatorStmt) return ddlExecutor.executeAlterOperator(((AlterOperatorStmt) stmt));
        if (stmt instanceof CreateTriggerStmt) return ddlExecutor.executeCreateTrigger(((CreateTriggerStmt) stmt));
        if (stmt instanceof CreateEventTriggerStmt) return ddlExecutor.executeCreateEventTrigger(((CreateEventTriggerStmt) stmt));
        if (stmt instanceof AlterEventTriggerStmt) return ddlExecutor.executeAlterEventTrigger(((AlterEventTriggerStmt) stmt));
        if (stmt instanceof DropEventTriggerStmt) return ddlExecutor.executeDropEventTrigger(((DropEventTriggerStmt) stmt));
        if (stmt instanceof CreateExtensionStmt) {
            CreateExtensionStmt extStmt = (CreateExtensionStmt) stmt;
            if (!extStmt.ifNotExists() || !database.hasExtension(extStmt.name())) {
                String version = extStmt.version() != null ? extStmt.version() : "1.0";
                database.addExtension(extStmt.name(), version, extStmt.schema());
                registerExtensionObjects(extStmt.name());
            }
            return QueryResult.message(QueryResult.Type.SET, "CREATE EXTENSION");
        }
        if (stmt instanceof CreateCollationStmt) return ddlExecutor.executeCreateCollation(((CreateCollationStmt) stmt));
        if (stmt instanceof CreateCastStmt) return ddlExecutor.executeCreateCast(((CreateCastStmt) stmt));
        if (stmt instanceof CreateRuleStmt) return ddlExecutor.executeCreateRule(((CreateRuleStmt) stmt));
        if (stmt instanceof DropStmt) {
            QueryResult result = ddlExecutor.executeDropStmt(((DropStmt) stmt));
            identity().sweepDead();
            return result;
        }
        if (stmt instanceof AlterTableStmt) {
            QueryResult result = ddlExecutor.executeAlterTable(((AlterTableStmt) stmt));
            // DROP CONSTRAINT and DROP COLUMN take an index down without naming it.
            identity().sweepDead();
            return result;
        }
        if (stmt instanceof TruncateStmt) return ddlExecutor.executeTruncate(((TruncateStmt) stmt));
        if (stmt instanceof SetStmt) return sessionExecutor.executeSetStmt(((SetStmt) stmt));
        if (stmt instanceof DiscardStmt) return sessionExecutor.executeDiscard(((DiscardStmt) stmt));
        if (stmt instanceof TransactionStmt) return ddlExecutor.executeTransaction(((TransactionStmt) stmt));
        if (stmt instanceof CreateIndexStmt) {
            String created = ((CreateIndexStmt) stmt).name();
            Set<String> held = beforeCreatingRelation(created);
            QueryResult result = ddlExecutor.executeCreateIndex(((CreateIndexStmt) stmt));
            noteRelationCreated(created, held);
            return result;
        }
        if (stmt instanceof CreateViewStmt) {
            String created = ((CreateViewStmt) stmt).name();
            Set<String> held = beforeCreatingRelation(created);
            QueryResult result = ddlExecutor.executeCreateView(((CreateViewStmt) stmt));
            noteRelationCreated(created, held);
            return result;
        }
        if (stmt instanceof CreateSequenceStmt) {
            String created = ((CreateSequenceStmt) stmt).name();
            Set<String> held = beforeCreatingRelation(created);
            QueryResult result = ddlExecutor.executeCreateSequence(((CreateSequenceStmt) stmt));
            noteRelationCreated(created, held);
            return result;
        }
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
        if (stmt instanceof CreateTableAsStmt) {
            String created = ((CreateTableAsStmt) stmt).name();
            Set<String> held = beforeCreatingRelation(created);
            QueryResult result = ddlExecutor.executeCreateTableAs(((CreateTableAsStmt) stmt));
            noteRelationCreated(created, held);
            return result;
        }
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
            // The schema and role are resolved to OIDs before anything is recorded, so naming one
            // that does not exist is an error rather than a default nothing will ever match.
            // The grantees are read first: PostgreSQL resolves the roles the privileges are for
            // before it looks at the schema they are in, so a missing grantee is what it names.
            if (s.grantees() != null) {
                for (String grantee : s.grantees()) {
                    String lower = grantee == null ? null : grantee.toLowerCase();
                    if (lower == null || lower.equals("public") || lower.equals("current_user")
                            || lower.equals("session_user") || lower.equals("current_role")) {
                        continue;
                    }
                    if (!database.hasRole(lower) && !lower.equalsIgnoreCase(sessionUser())) {
                        throw PgErrors.undefinedObject("role", grantee);
                    }
                }
            }
            if (s.forRole() != null && !database.hasRole(s.forRole().toLowerCase())) {
                throw PgErrors.undefinedObject("role", s.forRole());
            }
            if (s.inSchema() != null && database.getSchema(s.inSchema()) == null) {
                throw new MemgresException("schema \"" + s.inSchema() + "\" does not exist", "3F000");
            }
            if (s.isGrant()) {
                String grantor = s.forRole() != null ? s.forRole() : sessionUser();
                database.addDefaultAcl(new Database.DefaultAclEntry(
                        grantor, s.inSchema(), s.objectType(),
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
        if (stmt instanceof AlterSchemaRenameStmt) {
            AlterSchemaRenameStmt s = (AlterSchemaRenameStmt) stmt;
            database.renameSchema(s.name(), s.newName());
            return QueryResult.message(QueryResult.Type.SET, "ALTER SCHEMA");
        }
        if (stmt instanceof AlterSchemaOwnerStmt) {
            AlterSchemaOwnerStmt s = (AlterSchemaOwnerStmt) stmt;
            // A schema that was never created has no owner to change, and reporting success
            // leaves a script believing the schema is there.
            ddlExecutor.requireSchemaExists(s.name());
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
            identity().sweepDead();
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

    public PgRow parseCompositeToRow(String s, String typeName) {
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

    void checkNumericSpecialToInteger(CastExpr cast, Object val) {
        exprEvaluator.checkNumericSpecialToInteger(cast, val);
    }

    /**
     * Coerce a value to a named type the way an assignment would, so a PL/pgSQL variable of a
     * domain carries the domain's constraints rather than only its name.
     */
    public Object castValue(Object val, String typeSpec) {
        return castEvaluator.applyCast(val, typeSpec);
    }

    /**
     * The type a {@code table.column%TYPE} reference resolves to, or null when nothing typed
     * answers to it. A domain column resolves to the domain so the constraint travels with it.
     */
    public String resolveTypeReference(String ref) {
        if (ref == null) return null;
        String[] parts = ref.split("\\.");
        if (parts.length < 2) return null;
        String column = parts[parts.length - 1];
        String relation = parts[parts.length - 2];
        String schema = parts.length >= 3 ? parts[parts.length - 3] : defaultSchema();
        Table table;
        try {
            table = resolveTable(schema, relation);
        } catch (RuntimeException e) {
            return null;
        }
        if (table == null) return null;
        int idx = table.getColumnIndex(column);
        if (idx < 0) return null;
        Column col = table.getColumns().get(idx);
        // %TYPE names the column's own type as this session would write it.
        if (col.getDomainTypeName() != null) {
            return TypeNamespace.display(database, session, col.getDomainTypeName());
        }
        if (col.getEnumTypeName() != null) {
            return TypeNamespace.display(database, session, col.getEnumTypeName());
        }
        if (col.getCompositeTypeName() != null) return null;
        if (col.getArrayElementType() != null) return col.getArrayElementType().getPgName() + "[]";
        if (col.getType() == null) return null;
        // %TYPE copies the column's type exactly, length and precision included: a variable
        // declared from a varchar(3) column is itself a varchar(3)
        String name = col.getType().getPgName();
        if (col.getPrecision() != null) {
            name += col.getScale() != null
                    ? "(" + col.getPrecision() + "," + col.getScale() + ")"
                    : "(" + col.getPrecision() + ")";
        }
        return name;
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

        /**
         * Render as PostgreSQL composite text: {@code (f1,f2)}. A field is quoted when it is
         * empty or carries a character that would otherwise be read as structure, and a nested
         * composite is always quoted because its own parentheses would be ambiguous.
         */
        public String toPgText() {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sb.append(',');
                Object elem = values.get(i);
                if (elem == null) continue;
                // Each field is written by its own type's output function, the same one an array
                // element goes through: reading the Java object instead put a boolean in as
                // "true", a bytea as its identity hash and an array as a Java list.
                String text = TypeCoercion.toString(elem);
                if (needsCompositeQuoting(text)) {
                    sb.append('"').append(text.replace("\\", "\\\\").replace("\"", "\"\"")).append('"');
                } else {
                    sb.append(text);
                }
            }
            return sb.append(')').toString();
        }

        private static boolean needsCompositeQuoting(String text) {
            if (text.isEmpty()) return true;
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c == ',' || c == '(' || c == ')' || c == '"' || c == '\\'
                        || Character.isWhitespace(c)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Build a row from a composite held as a field map, as PL/pgSQL composite variables are.
         * Nested composites are converted too, so the row keeps its structure.
         */
        public static PgRow fromFieldMap(java.util.Map<?, ?> fields) {
            List<Object> vals = new java.util.ArrayList<>();
            for (Object v : fields.values()) {
                vals.add(v instanceof java.util.Map ? fromFieldMap((java.util.Map<?, ?>) v) : v);
            }
            return new PgRow(vals);
        }

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
            // A composite has one text form, the one record_out writes. A second, quoting-free
            // one here is what a stored composite was written with, and no reader could undo it.
            return toPgText();
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

    /** The schema a CREATE lands in; raises when search_path names no usable schema. */
    String creationSchema() {
        return session != null ? session.getCreationSchema() : "public";
    }

    /**
     * Schemas an unqualified name is looked up in, in precedence order. pg_catalog is implicitly
     * first, which is what makes a built-in win over a same-named function in a user schema.
     */
    /**
     * Schemas a relation name is looked up in, with this session's temporary schema ahead of the
     * rest. PostgreSQL puts pg_temp implicitly first for relations, which is what makes a
     * temporary sequence answer to its bare name.
     */
    /** The first table of this name along the search path, reading pg_temp where it stands. */
    private Table tableAlongSearchPath(String tableName, String tempSchemaName) {
        if (session == null) return null;
        for (String entry : session.getEffectiveSearchPath(false)) {
            String schemaName = "pg_temp".equalsIgnoreCase(entry) ? tempSchemaName : entry;
            Schema schema = database.getSchema(schemaName);
            if (schema == null) continue;
            Table table = visibleTable(schema.getTable(tableName));
            if (table != null) return table;
        }
        return null;
    }

    /** Whether the search path names the temporary schema, so its position is already decided. */
    private boolean searchPathNamesTemp() {
        if (session == null) return false;
        String temp = session.getTempSchemaName();
        for (String entry : session.getEffectiveSearchPath(false)) {
            if ("pg_temp".equalsIgnoreCase(entry) || entry.equalsIgnoreCase(temp)) return true;
        }
        return false;
    }

    /** Whether two functions take the same argument types, so one schema cannot hold both. */
    private static boolean sameArgumentTypes(PgFunction a, PgFunction b) {
        if (a.getParams().size() != b.getParams().size()) return false;
        for (int i = 0; i < a.getParams().size(); i++) {
            String left = a.getParams().get(i).typeName();
            String right = b.getParams().get(i).typeName();
            if (left == null ? right != null : !left.equalsIgnoreCase(right)) return false;
        }
        return true;
    }

    /** A function's argument types, written the way a message names them. */
    private static String argumentTypeList(PgFunction fn) {
        StringBuilder sb = new StringBuilder();
        for (PgFunction.Param p : fn.getParams()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(p.typeName() == null ? "any" : p.typeName());
        }
        return sb.toString();
    }

    /**
     * Read a statement and report the first thing wrong with it, without running it.
     *
     * <p>PostgreSQL analyses a statement when it is parsed, so a client that names a relation
     * that is not there hears about it at Parse — before it has bound anything. memgres left
     * every such error until execution, so a statement carrying a parameter reported a bind
     * mismatch where PostgreSQL reports the missing relation.
     */
    public void analyzeWithoutRunning(Statement stmt) {
        analyzeWithoutRunning(stmt, 0);
    }

    /**
     * The same reading, of a tree parsed from text beginning {@code textOffset} characters into
     * the statement the client sent. What the reading points at is pointed at in that statement,
     * not in whatever was handed to the parser.
     */
    public void analyzeWithoutRunning(Statement stmt, int textOffset) {
        // Reading a statement is a read of the catalog, and the catalog answers differently to
        // different sessions: a relation another session's open transaction dropped is still
        // there for everyone else, and one it created is there for nobody else. Analysing
        // without saying whose statement this is asked the catalog a question it cannot answer,
        // and a reader was told a live relation was missing.
        Session outerViewer = Database.bindViewer(session);
        try {
            new StatementAnalyzer(this, textOffset).analyzeAtParse(stmt);
        } finally {
            Database.bindViewer(outerViewer);
        }
    }

    /**
     * How many columns a statement answers with, or -1 when reading it does not settle that.
     *
     * <p>PostgreSQL settles this while it plans the statement, which is before Bind holds a
     * client's list of result formats against it. Running the statement to find out is the side
     * effect this reader exists to avoid, so a width it cannot work out is no width at all.
     */
    public int resultColumnsWithoutRunning(Statement stmt) {
        Session outerViewer = Database.bindViewer(session);
        try {
            return new StatementAnalyzer(this).resultColumns(stmt);
        } catch (RuntimeException | StackOverflowError e) {
            return -1;
        } finally {
            Database.bindViewer(outerViewer);
        }
    }

    java.util.List<String> relationSearchPath() {
        java.util.LinkedHashSet<String> path = new java.util.LinkedHashSet<>();
        String temp = session == null ? null : session.getTempSchemaName().toLowerCase();
        // pg_temp comes first only while the search path leaves it unsaid. A path that names it
        // puts it where it was named, so "public, pg_temp" reads the permanent table of a name
        // in preference to the temporary one — and putting temp first regardless meant a
        // temporary table shadowed a permanent one the session had asked to see instead.
        boolean named = false;
        if (session != null) {
            for (String entry : session.getEffectiveSearchPath(false)) {
                String lower = entry.toLowerCase();
                if (lower.equals("pg_temp") || lower.equals(temp)) { named = true; break; }
            }
        }
        if (temp != null && !named) path.add(temp);
        for (String schema : searchPathSchemas()) {
            path.add(temp != null && "pg_temp".equals(schema) ? temp : schema);
        }
        if (temp != null) path.add(temp);
        return new ArrayList<>(path);
    }

    java.util.List<String> searchPathSchemas() {
        java.util.LinkedHashSet<String> path = new java.util.LinkedHashSet<>();
        path.add("pg_catalog");
        if (session != null) {
            for (String s : session.getEffectiveSearchPath(false)) path.add(s.toLowerCase());
        }
        path.add("public");
        return new ArrayList<>(path);
    }

    String sessionUser() {
        if (session != null && session.getConnectingUser() != null) {
            return session.getConnectingUser();
        }
        return "memgres";
    }

    /** Returns the current effective role (respects SET ROLE), falling back to session user. */
    String currentRole() {
        if (session != null) {
            GucSettings guc = session.getGucSettings();
            if (guc.hasSessionOverride("role")) {
                String role = guc.get("role");
                if (role != null && !role.equalsIgnoreCase("NONE") && !role.equalsIgnoreCase("DEFAULT")) {
                    return role;
                }
            }
        }
        return sessionUser();
    }

    /**
     * C6: Check that the current role has the given privilege on a table.
     * Superusers and object owners bypass the check. PUBLIC grants are inherited.
     */
    /** The key privileges on a table are stored under: schema-qualified and lower-cased. */
    static String privilegeKey(String schemaName, String tableName) {
        String bare = tableName == null ? "" : tableName.toLowerCase();
        if (bare.contains(".")) return bare;
        return (schemaName == null ? "public" : schemaName.toLowerCase()) + "." + bare;
    }

    /**
     * While a view's body runs, privilege checks use the view's owner. PG reads a view
     * with the owner's rights, so granting on the view alone is enough — the querying
     * role never needs a grant on the base tables.
     */
    String viewOwnerRole = null;

    void checkTablePrivilege(String privilege, String schemaName, String tableName) {
        String role = viewOwnerRole != null ? viewOwnerRole : currentRole();
        if (role == null) return; // no session / embedded mode
        // Superuser check (by role attribute, not hardcoded names)
        Map<String, String> roleAttrs = database.getRole(role);
        if (roleAttrs != null && "true".equalsIgnoreCase(roleAttrs.get("SUPERUSER"))) return;
        // Also treat the default "memgres"/"test"/"postgres" connecting users as superuser
        // when they have no explicit role entry (backwards-compat for existing tests)
        if (roleAttrs == null) {
            String lower = role.toLowerCase();
            if ("memgres".equals(lower) || "test".equals(lower) || "postgres".equals(lower)) return;
        }
        // Owner check — try both "table:" and "view:" keys since views are DML-capable
        String qualName = schemaName.toLowerCase() + "." + tableName.toLowerCase();
        String owner = database.getObjectOwner("table:" + qualName);
        if (owner != null && owner.equalsIgnoreCase(role)) return;
        owner = database.getObjectOwner("view:" + qualName);
        if (owner != null && owner.equalsIgnoreCase(role)) return;
        // Direct or inherited privilege check (including PUBLIC grants). Privileges are
        // keyed by schema-qualified name so a grant on s1.t cannot open s2.t.
        if (hasPrivilegeDirectOrInherited(role, privilege, "TABLE", qualName)) return;
        // Also check PUBLIC grants
        if (hasPrivilegeDirectOrInherited("public", privilege, "TABLE", qualName)) return;
        // PostgreSQL leaves the name unquoted here, unlike the messages that report a relation
        // that is missing or of the wrong kind.
        throw new MemgresException("permission denied for table " + tableName, "42501");
    }

    /** Check if role has a privilege directly or through role membership. */
    boolean hasPrivilegeDirectOrInherited(String roleName, String privilege,
            String objectType, String objectName) {
        return hasPrivilegeDirectOrInherited(roleName, privilege, objectType, objectName, new java.util.HashSet<>());
    }

    private boolean hasPrivilegeDirectOrInherited(String roleName, String privilege,
            String objectType, String objectName, java.util.Set<String> visited) {
        String roleNameLower = roleName.toLowerCase();
        if (visited.contains(roleNameLower)) return false;
        visited.add(roleNameLower);
        java.util.Set<String> privs = database.getRolePrivileges(roleNameLower);
        String checkKey = privilege.toUpperCase() + ":" + objectType.toUpperCase() + ":" + objectName.toLowerCase();
        String allKey = "ALL:" + objectType.toUpperCase() + ":" + objectName.toLowerCase();
        if (privs.contains(checkKey) || privs.contains(allKey)) return true;
        // Traverse role memberships
        java.util.Map<String, java.util.Set<String>> memberships = database.getRoleMemberships();
        for (java.util.Map.Entry<String, java.util.Set<String>> entry : memberships.entrySet()) {
            if (entry.getValue().contains(roleNameLower) && !visited.contains(entry.getKey())) {
                if (hasPrivilegeDirectOrInherited(entry.getKey(), privilege, objectType, objectName, visited)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if a role is a superuser (by rolsuper attribute, or backwards-compat for default connecting users).
     */
    boolean isRoleSuperuser(String role) {
        if (role == null) return true; // no session / embedded mode
        Map<String, String> roleAttrs = database.getRole(role);
        if (roleAttrs != null && "true".equalsIgnoreCase(roleAttrs.get("SUPERUSER"))) return true;
        if (roleAttrs == null) {
            String lower = role.toLowerCase();
            return "memgres".equals(lower) || "test".equals(lower) || "postgres".equals(lower);
        }
        return false;
    }

    /**
     * Check if the given role owns a table (by schema.table lookup in object owners).
     */
    boolean isTableOwner(String role, String schemaName, String tableName) {
        String ownerKey = "table:" + schemaName.toLowerCase() + "." + tableName.toLowerCase();
        String owner = database.getObjectOwner(ownerKey);
        return owner != null && owner.equalsIgnoreCase(role);
    }

    /**
     * A table is reshaped and removed by whoever owns it. Every role could do both, so a session
     * that had only set a role to one with no rights over a table still altered and dropped it.
     *
     * <p>Membership counts: a role that is a member of the owning role owns what it owns. So does
     * being a superuser. A table whose owner was never recorded is left alone — nothing is known
     * about it to refuse on.
     */
    void requireTableOwner(String schemaName, String tableName) {
        requireRelationOwner(schemaName, tableName, "table");
    }

    /**
     * The same rule, worded the way PostgreSQL words it for the objects that hang off a relation:
     * DROP POLICY says relation where ALTER TABLE and DROP TABLE say table.
     */
    void requireRelationOwner(String schemaName, String relationName) {
        requireRelationOwner(schemaName, relationName, "relation");
    }

    private void requireRelationOwner(String schemaName, String relationName, String noun) {
        if (session == null || schemaName == null || relationName == null) return;
        String owner = database.getObjectOwner(
                "table:" + schemaName.toLowerCase() + "." + relationName.toLowerCase());
        if (owner == null) return;
        String role = currentRole();
        if (role == null || role.equalsIgnoreCase(owner)) return;
        if (isRoleSuperuser(role)) return;
        if (database.isRoleMemberOf(role, owner)) return;
        throw new MemgresException("must be owner of " + noun + " " + relationName, "42501");
    }

    /**
     * Determine if RLS should be bypassed for the current role on a table.
     * Returns true if the current role can bypass RLS (superuser/owner without FORCE, or row_security=off).
     * Throws MemgresException if row_security=off but user has no bypass privilege.
     */
    boolean shouldBypassRls(Table table, String schemaName) {
        String role = currentRole();
        boolean isSuperuser = isRoleSuperuser(role);
        boolean isOwner = isTableOwner(role, schemaName, table.getName());

        // Check SET row_security GUC
        String rowSecurityGuc = session != null ? session.getGucSettings().get("row_security") : "on";
        if ("off".equalsIgnoreCase(rowSecurityGuc)) {
            if (isSuperuser || isOwner) return true;
            // non-owner/non-superuser with row_security=off: if any policies exist, error
            if (!table.getRlsPolicies().isEmpty()) {
                throw new MemgresException(
                    "query would be affected by row-level security policy for table \"" + table.getName() + "\"", "55P04");
            }
            return true; // no policies, no filtering needed
        }

        // Superuser bypasses unless FORCE RLS
        if (isSuperuser && !table.isRlsForced()) return true;
        // Owner bypasses unless FORCE RLS
        if (isOwner && !table.isRlsForced()) return true;
        return false;
    }

    Table resolveTable(String schemaName, String tableName) {
        return resolveTable(schemaName, tableName, false);
    }

    /**
     * Hide a relation another session created in a transaction that has not committed. It may
     * still turn out never to have existed, so resolution behaves as if it does not yet.
     */
    private Table visibleTable(Table table) {
        return database.isObjectVisibleTo(table, session) ? table : null;
    }

    /**
     * @param userQualified true when the user explicitly wrote schema.table in SQL
     *                      (prevents temp tables from shadowing the explicit schema)
     */
    Table resolveTable(String schemaName, String tableName, boolean userQualified) {
        lastViewColumnMapping = null; // reset before each resolution
        lastViewColumnOrder = null;
        lastViewColumnNames = null;
        lastViewExpressionColumns = null;
        // A plain table carries no qualification of its own, so nothing a view resolved earlier
        // said may be left standing here: it would hold rows back from a write that is not going
        // through any view at all.
        lastViewQuals = null;
        String tempSchemaName = session != null ? session.getTempSchemaName() : "pg_temp";
        // Resolve pg_temp alias to the actual session temp schema
        if ("pg_temp".equalsIgnoreCase(schemaName)) {
            Schema pgTemp = database.getSchema(tempSchemaName);
            if (pgTemp != null) {
                Table tempTable = visibleTable(pgTemp.getTable(tableName));
                if (tempTable != null) return tempTable;
            }
            throw new MemgresException("relation \"" + schemaName + "." + tableName + "\" does not exist", "42P01");
        }
        // H35: When user explicitly wrote schema.table, do NOT let temp tables shadow it —
        // resolve directly in the specified schema without checking temp first.
        if (userQualified && schemaName != null) {
            Schema schema = database.getSchema(schemaName);
            if (schema != null) {
                Table table = visibleTable(schema.getTable(tableName));
                if (table != null) return table;
            }
            // Fall through to view/sequence resolution below
        } else {
            // The temporary schema comes first only while the search path leaves it unsaid; a
            // path that names it puts it where it was named, so "public, pg_temp" reads the
            // permanent table in preference to the temporary one of the same name.
            boolean tempIsNamed = searchPathNamesTemp();
            if (!tempIsNamed) {
                Schema pgTemp = database.getSchema(tempSchemaName);
                if (pgTemp != null) {
                    Table tempTable = visibleTable(pgTemp.getTable(tableName));
                    if (tempTable != null) return tempTable;
                }
            } else {
                // The path said where the temporary schema stands, so the whole path is walked
                // in order before anything is assumed about which schema is the current one.
                Table onPath = tableAlongSearchPath(tableName, tempSchemaName);
                if (onPath != null) return onPath;
            }
            // Then check explicit/default schema
            Schema schema = schemaName != null ? database.getSchema(schemaName) : null;
            if (schema != null) {
                Table table = visibleTable(schema.getTable(tableName));
                if (table != null) return table;
            }
            // Then walk search_path
            if (session != null) {
                String searchPath = session.getGucSettings().get("search_path");
                if (searchPath != null) {
                    for (String sp : searchPath.split(",")) {
                        String s = sp.trim().replace("\"", "").replace("'", "");
                        if (s.isEmpty() || s.equals("$user")) continue;
                        if ("pg_temp".equalsIgnoreCase(s)) s = tempSchemaName;
                        Schema spSchema = database.getSchema(s);
                        if (spSchema != null) {
                            Table table = visibleTable(spSchema.getTable(tableName));
                            if (table != null) return table;
                        }
                    }
                }
            }
        }
        if (schemaName != null && database.getSchema(schemaName) == null
                && !"pg_catalog".equalsIgnoreCase(schemaName)
                && !"information_schema".equalsIgnoreCase(schemaName)) {
            // A query names a relation, not a schema: PG resolves schema.table as one name and
            // reports the whole of it missing, and a client that branches on 42P01 to mean
            // "no such table" is right either way
            if (userQualified) {
                throw new MemgresException("relation \"" + schemaName + "." + tableName
                        + "\" does not exist", "42P01");
            }
            throw new MemgresException("schema \"" + schemaName + "\" does not exist", "3F000");
        }
        // A qualified name reaches the view in the schema it names, and no other schema's.
        Database.ViewDef view = userQualified && schemaName != null
                ? database.getView(schemaName, tableName) : database.getView(tableName);
        if (view != null) {
            // Whether the write may be rewritten onto the base table is PostgreSQL's exact rule,
            // and it is the same rule the catalogs report — a caller that asked
            // information_schema.views or pg_relation_is_updatable first was told the truth.
            ViewUpdatability.Reason reason = ViewUpdatability.notAutoUpdatable(database, view);
            if (reason == null) {
                Table underlying = resolveViewToBaseTable(view);
                if (underlying != null) return underlying;
            }
            // An INSTEAD OF trigger takes the write in place of the rewrite — but only the write
            // it was declared for. A view with an INSTEAD OF UPDATE trigger and nothing else is
            // still not deletable, and PG refuses the DELETE.
            if ((ViewUpdatability.insteadOfEvents(database, tableName) & verbEvent(viewDmlVerb)) != 0) {
                // Create a virtual table from the view's column definitions
                return buildVirtualTableForView(view, tableName);
            }
            // PG blames the view that broke the rule, which for a view over a view is the inner
            // one — the caller has to be told which definition to change.
            String blamed = reason != null && reason.relation != null ? reason.relation : tableName;
            throw ViewUpdatability.cannotWrite(viewDmlVerb, blamed,
                    reason != null ? reason.detail : ViewUpdatability.DETAIL_NOT_SINGLE_RELATION,
                    viewDmlByMerge);
        }
        // Sequences are queryable as relations in PG (columns: last_value, log_cnt, is_called)
        Table seqTable = resolveSequenceAsRelation(schemaName, tableName, userQualified);
        if (seqTable != null) return seqTable;
        // An index is a relation the name reaches, so PostgreSQL opens it and then refuses it for
        // what it is. Reporting it missing sent the reader looking for a relation that is there.
        rejectRelationKindInFrom(schemaName, tableName, userQualified);
        // PG names the relation the way the query did: a qualified reference that found nothing
        // reports the qualified name, and only an unqualified one can have meant a WITH item.
        if (userQualified && schemaName != null) {
            throw new MemgresException(
                    "relation \"" + schemaName + "." + tableName + "\" does not exist", "42P01");
        }
        MemgresException notThere =
                new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
        selectExecutor.noteHiddenWithItem(notThere, tableName);
        throw notThere;
    }

    /** The event bit the statement now being resolved writes with. */
    private int verbEvent(String verb) {
        if ("update".equals(verb)) return ViewUpdatability.UPDATE;
        if ("delete from".equals(verb)) return ViewUpdatability.DELETE;
        return ViewUpdatability.INSERT;
    }

    /**
     * Resolve a sequence name to a virtual single-row table with columns
     * last_value, log_cnt, is_called — matching PG's sequence relation layout.
     */
    private Table resolveSequenceAsRelation(String schemaName, String seqName, boolean userQualified) {
        // A written qualifier names one schema's sequence and no other's. Asking for the sequence
        // of that name anywhere returned public's counter for a read of another schema's, and
        // invented a row for a sequence the named schema does not hold at all.
        Sequence seq = userQualified && schemaName != null
                ? database.getSequence(SchemaQualifier.resolveAlias(session, schemaName), seqName)
                : database.resolveSequence(relationSearchPath(), seqName);
        if (seq == null) return null;
        List<Column> cols = new java.util.ArrayList<>();
        cols.add(new Column("last_value", DataType.BIGINT, false, false, null));
        cols.add(new Column("log_cnt", DataType.BIGINT, false, false, null));
        cols.add(new Column("is_called", DataType.BOOLEAN, false, false, null));
        Table table = new Table(seqName, cols);
        table.insertRow(new Object[]{seq.currValRaw(), 0L, seq.isCalled()});
        return table;
    }

    /**
     * Refuse a read or a write against a relation whose kind cannot carry one.
     *
     * <p>PostgreSQL resolves the name first and opens the relation second, so naming an index in
     * a FROM or an INSERT is {@code 42809 cannot open relation} with a detail line saying which
     * kind is in the way — not {@code 42P01}, which says the name reaches nothing and sends the
     * reader looking for a relation that is sitting right there.
     */
    private void rejectRelationKindInFrom(String schemaName, String tableName, boolean userQualified) {
        List<String> path = userQualified && schemaName != null
                ? Collections.singletonList(SchemaQualifier.resolveAlias(session, schemaName))
                : relationSearchPath();
        for (String schema : path) {
            String kind = RelationNamespace.kindOf(database, schema, tableName);
            // A composite type owns a pg_class row too, so a query reaches it and is refused for
            // what it is rather than told the relation is missing — the same complaint an index
            // gets, worded for the kind that was found.
            String detail = RelationNamespace.INDEX.equals(kind)
                    ? "This operation is not supported for indexes."
                    : RelationNamespace.COMPOSITE.equals(kind)
                            ? "This operation is not supported for composite types." : null;
            if (detail == null) continue;
            MemgresException e = new MemgresException(
                    "cannot open relation \"" + tableName + "\"", "42809");
            e.setDetail(detail);
            e.setPositionToken(tableName);
            throw e;
        }
    }

    Table resolveTableSafe(String tableName) {
        try {
            return resolveTable(defaultSchema(), tableName);
        } catch (MemgresException e) {
            return null;
        }
    }

    /** The base tables a name reached during the statement now running, {@link #NO_TABLE} for none. */
    private final Map<String, Table> baseTablesByName = new java.util.HashMap<>();

    /** Stands in a {@link #baseTablesByName} entry for a name no schema holds a table under. */
    private static final Table NO_TABLE = new Table("", Collections.<Column>emptyList());

    /**
     * The table a schema holds under this name, or null when no schema holds one — the same walk
     * {@link #resolveTable} makes, stopping where the tables stop.
     *
     * <p>This exists to be compared with a relation the caller already has in hand, which is what
     * makes it right to leave out everything {@code resolveTable} goes on to try. A view, a
     * sequence read as a relation and the virtual table built for a view with INSTEAD OF triggers
     * are all relations a name reaches, and none of them is ever the same object as the one being
     * compared: the first two are built fresh on every resolution and the third describes a view
     * rather than a table. So the comparison answers false for them either way, and asking the
     * short question instead of the long one costs no accuracy.
     *
     * <p>It also answers without throwing. The long question reports a name it cannot resolve by
     * raising, and the caller here asks it for every column reference of every comparison of
     * every row; filling in those stack traces was, measured over the verification corpus, the
     * single largest cost in the engine.
     */
    Table baseTableNamed(String tableName) {
        if (tableName == null) return null;
        Table cached = baseTablesByName.get(tableName);
        if (cached != null) return cached == NO_TABLE ? null : cached;
        Table found = lookUpBaseTable(tableName);
        baseTablesByName.put(tableName, found == null ? NO_TABLE : found);
        return found;
    }

    private Table lookUpBaseTable(String tableName) {
        // Temp tables shadow the search path, exactly as in resolveTable.
        String tempSchemaName = session != null ? session.getTempSchemaName() : "pg_temp";
        Schema pgTemp = database.getSchema(tempSchemaName);
        if (pgTemp != null) {
            Table tempTable = visibleTable(pgTemp.getTable(tableName));
            if (tempTable != null) return tempTable;
        }
        Schema schema = database.getSchema(defaultSchema());
        if (schema != null) {
            Table table = visibleTable(schema.getTable(tableName));
            if (table != null) return table;
        }
        if (session == null) return null;
        String searchPath = session.getGucSettings().get("search_path");
        if (searchPath == null) return null;
        for (String sp : searchPath.split(",")) {
            String s = sp.trim().replace("\"", "").replace("'", "");
            if (s.isEmpty() || s.equals("$user")) continue;
            Schema spSchema = database.getSchema(s);
            if (spSchema != null) {
                Table table = visibleTable(spSchema.getTable(tableName));
                if (table != null) return table;
            }
        }
        return null;
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
        // Check for aggregate functions in SELECT targets (PG: view is not auto-updatable)
        if (sel.targets() != null) {
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (containsAggregate(target.expr())) return null;
            }
        }
        String refSchema = ref.schema() != null ? ref.schema() : defaultSchema();
        Table baseTable;
        List<ViewQual> quals;
        try {
            baseTable = resolveTable(refSchema, ref.table());
            // Resolving what this view reads from settles the qualification below it, and that
            // relation is also this view's own WHERE's frame of reference: the names it writes are
            // the ones that relation exposes, under the renaming recorded for it.
            quals = lastViewQuals != null
                    ? new ArrayList<ViewQual>(lastViewQuals) : new ArrayList<ViewQual>();
            if (sel.where() != null) {
                quals.add(new ViewQual(sel.where(),
                        ref.alias() != null ? ref.alias() : ref.table(), lastViewColumnMapping));
            }
        } catch (MemgresException e) { return null; }
        // Build column mapping: view alias → base column name, plus the ordered base-column
        // list (one entry per view-column position) used for positional INSERT remapping.
        lastViewColumnMapping = null;
        lastViewColumnOrder = null;
        lastViewColumnNames = null;
        lastViewExpressionColumns = null;
        lastViewQuals = quals.isEmpty() ? null : quals;
        if (sel.targets() != null) {
            Map<String, String> mapping = new LinkedHashMap<>();
            List<String> order = new ArrayList<>();
            List<String> viewColumnNames = new ArrayList<>();
            Set<String> exprCols = new java.util.LinkedHashSet<>();
            boolean allSimpleColumns = true;
            for (SelectStmt.SelectTarget target : sel.targets()) {
                String baseCol = null;
                if (target.expr() instanceof ColumnRef) {
                    baseCol = ((ColumnRef) target.expr()).column();
                }
                if (baseCol == null) {
                    // Non-column target (e.g. SELECT *, expression): positional remap not derivable.
                    allSimpleColumns = false;
                    if (target.alias() != null) exprCols.add(target.alias().toLowerCase());
                } else {
                    order.add(baseCol);
                }
                String viewCol = target.alias() != null ? target.alias() : baseCol;
                viewColumnNames.add(viewCol);
                if (viewCol != null && baseCol != null && !viewCol.equalsIgnoreCase(baseCol)) {
                    mapping.put(viewCol.toLowerCase(), baseCol);
                }
            }
            if (!viewColumnNames.isEmpty()) lastViewColumnNames = viewColumnNames;
            if (!mapping.isEmpty()) lastViewColumnMapping = mapping;
            if (allSimpleColumns && !order.isEmpty()) lastViewColumnOrder = order;
            if (!exprCols.isEmpty()) lastViewExpressionColumns = exprCols;
        }
        return baseTable;
    }

    /**
     * The rows of {@code table} a write going through a view may reach. PostgreSQL rewrites an
     * UPDATE or a DELETE on an auto-updatable view into one on the base relation with the view's
     * own qualification added, so a row the view does not show is not a row the write can touch.
     * With no view in the way -- the ordinary case of a write to a table -- the rows come back
     * exactly as they were given.
     */
    List<Object[]> filterByViewQuals(List<ViewQual> quals, Table table, List<Object[]> rows) {
        if (quals == null || quals.isEmpty()) return rows;
        List<Object[]> shown = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            if (viewQualsPass(quals, table, row)) shown.add(row);
        }
        return shown;
    }

    /** Whether one stored row passes every view in the chain the write is going through. */
    boolean viewQualsPass(List<ViewQual> quals, Table table, Object[] row) {
        if (quals == null || quals.isEmpty()) return true;
        for (ViewQual qual : quals) {
            RowContext ctx = new RowContext(table, qual.relationName, row);
            if (qual.columnNames != null) ctx.setColumnAliases(qual.columnNames);
            if (!isTruthy(evalExpr(qual.expr, ctx))) return false;
        }
        return true;
    }

    private Table buildVirtualTableForView(Database.ViewDef view, String viewName) {
        // Execute the view query to determine column structure AND materialize its current
        // rows. INSTEAD OF INSERT ignores the rows (it builds them from VALUES), but INSTEAD OF
        // UPDATE/DELETE need the view's rows so the WHERE clause can match and OLD is populated.
        try {
            QueryResult result = executeStatement(view.query());
            List<Column> cols = new ArrayList<>();
            for (Column c : result.getColumns()) {
                cols.add(new Column(c.getName(), c.getType(), c.isNullable(), false, null));
            }
            Table t = new Table(viewName, cols);
            t.setViewProjection(true);
            for (Object[] row : result.getRows()) {
                t.insertRow(row);
            }
            return t;
        } catch (Exception e) {
            throw new MemgresException("cannot " + viewDmlVerb + " view \"" + viewName + "\"", "55000");
        }
    }

    /**
     * True if the expression contains an aggregate or window call anywhere inside it.
     * PG's auto-updatability test looks at the whole target expression, not just a bare
     * call: {@code sum(val)+1} and {@code row_number() OVER ()} both make a view
     * read-only, so this has to walk every operand, not only function arguments.
     */
    private boolean containsAggregate(Expression expr) {
        if (expr == null) return false;
        if (expr instanceof WindowFuncExpr) return true;
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = fn.name().toLowerCase();
            if (name.contains(".")) name = name.substring(name.lastIndexOf('.') + 1);
            if (SelectExecutor.AGGREGATE_FUNCTIONS.contains(name)) return true;
            for (Expression arg : fn.args()) {
                if (containsAggregate(arg)) return true;
            }
            if (containsAggregate(fn.filter())) return true;
            return false;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) expr;
            return containsAggregate(b.left()) || containsAggregate(b.right());
        }
        if (expr instanceof UnaryExpr) return containsAggregate(((UnaryExpr) expr).operand());
        if (expr instanceof CastExpr) return containsAggregate(((CastExpr) expr).expr());
        if (expr instanceof CollateExpr) return containsAggregate(((CollateExpr) expr).expr());
        if (expr instanceof IsNullExpr) return containsAggregate(((IsNullExpr) expr).expr());
        if (expr instanceof IsBooleanExpr) return containsAggregate(((IsBooleanExpr) expr).expr());
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) expr;
            return containsAggregate(c.left()) || containsAggregate(c.right());
        }
        if (expr instanceof BetweenExpr) {
            BetweenExpr b = (BetweenExpr) expr;
            return containsAggregate(b.expr()) || containsAggregate(b.low()) || containsAggregate(b.high());
        }
        if (expr instanceof LikeExpr) {
            LikeExpr l = (LikeExpr) expr;
            return containsAggregate(l.left()) || containsAggregate(l.pattern());
        }
        if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            if (containsAggregate(in.expr())) return true;
            if (in.values() != null) {
                for (Expression v : in.values()) if (containsAggregate(v)) return true;
            }
            return false;
        }
        if (expr instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr a = (AnyAllArrayExpr) expr;
            return containsAggregate(a.left()) || containsAggregate(a.array());
        }
        if (expr instanceof ArrayExpr) {
            for (Expression e : ((ArrayExpr) expr).elements()) if (containsAggregate(e)) return true;
            return false;
        }
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            if (containsAggregate(c.operand()) || containsAggregate(c.elseExpr())) return true;
            for (CaseExpr.WhenClause w : c.whenClauses()) {
                if (containsAggregate(w.condition()) || containsAggregate(w.result())) return true;
            }
            return false;
        }
        return false;
    }

    Table resolveTableAnySchema(String tableName) {
        // Handle schema-qualified names (e.g., "ks1.parent")
        if (tableName.contains(".")) {
            int dot = tableName.indexOf('.');
            String schema = tableName.substring(0, dot);
            String bare = tableName.substring(dot + 1);
            Schema s = database.getSchema(schema);
            if (s != null) {
                Table t = visibleTable(s.getTable(bare));
                if (t != null) return t;
            }
            throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
        }
        String defSchema = defaultSchema();
        if (defSchema != null) {
            Schema ds = database.getSchema(defSchema);
            if (ds != null) {
                Table t = visibleTable(ds.getTable(tableName));
                if (t != null) return t;
            }
        }
        Schema pub = database.getSchema("public");
        if (pub != null) {
            Table t = visibleTable(pub.getTable(tableName));
            if (t != null) return t;
        }
        for (Schema schema : database.getSchemas().values()) {
            Table t = visibleTable(schema.getTable(tableName));
            if (t != null) return t;
        }
        throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
    }

    Object evaluateDefault(String defaultExpr, DataType type) {
        return evaluateDefault(defaultExpr, type, null);
    }

    Object evaluateDefault(String defaultExpr, DataType type, Expression parsedExpr) {
        if (defaultExpr != null && defaultExpr.startsWith("__identity__")) {
            return null; // identity columns handled by nextSerial() in DmlExecutor
        }
        if (defaultExpr != null) {
            String lower = defaultExpr.toLowerCase().trim();
            if (lower.equals("uuid_generate_v4()") || lower.equals("gen_random_uuid()")) {
                return java.util.UUID.randomUUID();
            }
            if (lower.equals("now()") || lower.equals("current_timestamp")) {
                OffsetDateTime instant = currentInstant();
                if (type == DataType.DATE) {
                    return instant.atZoneSameInstant(TypeCoercion.sessionZone()).toLocalDate();
                }
                if (type == DataType.TIMESTAMP) {
                    return instant.atZoneSameInstant(TypeCoercion.sessionZone()).toLocalDateTime();
                }
                return instant;
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

    String resolveEnumTypeName(Expression expr, List<RowContext.TableBinding> bindings) {
        return exprEvaluator.resolveEnumTypeName(expr, bindings);
    }

    Column buildResultColumn(String alias, Expression expr, List<RowContext.TableBinding> bindings) {
        return exprEvaluator.buildResultColumn(alias, expr, bindings);
    }

    @SuppressWarnings("unchecked")
    int compareValues(Object a, Object b) {
        return exprEvaluator.compareValues(a, b);
    }

    boolean isTruthy(Object val) {
        return exprEvaluator.isTruthy(val);
    }

    static boolean likeMatch(String text, String pattern, boolean caseInsensitive) {
        return ExprEvaluator.likeMatch(text, pattern, caseInsensitive);
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

    /**
     * The signature PostgreSQL names when a routine is missing: the name with the argument list
     * that was written, so two overloads of one name are told apart in the message.
     */
    private static String alterFunctionSignature(AlterFunctionStmt stmt) {
        if (stmt.paramTypes() == null) return stmt.name();
        return stmt.name() + "(" + DdlObjectExecutor.canonicalTypeList(stmt.paramTypes()) + ")";
    }

    private QueryResult executeAlterFunction(AlterFunctionStmt stmt) {
        String tag = stmt.commandTag();
        String kind = stmt.isProcedure() ? "procedure" : "function";

        switch (stmt.action()) {
            case RENAME_TO: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + alterFunctionSignature(stmt)
                            + " does not exist", "42883").withoutHint();
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
                                // Named the way PostgreSQL names it: the signature that clashes and
                                // the schema it clashes in, since only an overload of the same
                                // argument types in the same schema is a collision at all.
                                String where = existing.getSchemaName() != null
                                        ? existing.getSchemaName() : "public";
                                throw new MemgresException(kind + " " + stmt.targetValue() + "("
                                        + DdlObjectExecutor.canonicalTypeList(existingParamTypes)
                                        + ") already exists in schema \"" + where + "\"", "42723");
                            }
                        }
                    }
                }
                // Rename only this specific overload, not all overloads
                String nameBeforeRename = func.getName();
                database.renameFunctionOverload(func, stmt.targetValue());
                // DDL is transactional: a rolled-back rename has to leave the routine answering to
                // the name it had, with the schema registration and ownership that went with it.
                recordUndo(new Session.RenameFunctionUndo(nameBeforeRename, func));
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case SET_SCHEMA: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + alterFunctionSignature(stmt) + " does not exist", "42883");
                }
                String oldSchema = func.getSchemaName() != null ? func.getSchemaName() : "public";
                String newSchema = stmt.targetValue();
                if (database.getSchema(newSchema) == null) {
                    throw new MemgresException("schema \"" + newSchema + "\" does not exist", "3F000");
                }
                // A schema holds one function of a given name and argument list. Moving on top of
                // one that is already there replaced it silently, and the function the target
                // schema had answered under its own name no longer existed.
                for (PgFunction other : database.getFunctionOverloads(newSchema, func.getName())) {
                    if (other != func && sameArgumentTypes(other, func)) {
                        throw new MemgresException("function " + func.getName() + "("
                                + argumentTypeList(func) + ") already exists in schema \""
                                + newSchema + "\"", "42723");
                    }
                }
                func.setSchemaName(newSchema);
                // Update schema registry
                Set<String> oldObjects = database.getSchemaObjects(oldSchema);
                oldObjects.remove("function:" + stmt.name().toLowerCase());
                database.registerSchemaObject(newSchema, "function", stmt.name());
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case OWNER_TO: {
                // The role the ownership is being given to is resolved first, so a role that is
                // not there is what PostgreSQL names — before it has looked for the routine.
                String newOwner = ddlExecutor.resolveOwnerName(stmt.targetValue());
                if (!database.hasRole(newOwner)) {
                    throw new MemgresException("role \"" + stmt.targetValue() + "\" does not exist", "42704");
                }
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + alterFunctionSignature(stmt) + " does not exist", "42883");
                }
                database.setObjectOwner("function:" + stmt.name(), newOwner);
                func.setOwner(newOwner);
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case DEPENDS_ON_EXTENSION: {
                PgFunction dependent = resolveAlterFunction(stmt);
                if (dependent == null) {
                    throw new MemgresException(kind + " " + alterFunctionSignature(stmt)
                            + " does not exist", "42883");
                }
                // memgres keeps no extension dependencies, but an extension that was never
                // installed is still not something a function can be made to depend on. The ones
                // PostgreSQL ships with count as installed, which is why this asks the same
                // question every other statement naming an extension asks.
                ddlExecutor.requireObjectExists("extension", stmt.targetValue());
                return QueryResult.message(QueryResult.Type.SET, tag);
            }
            case SET_ATTRIBUTES: {
                PgFunction func = resolveAlterFunction(stmt);
                if (func == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, tag);
                    throw new MemgresException(kind + " " + alterFunctionSignature(stmt) + " does not exist", "42883");
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
                // ROWS: PG 18 rejects ROWS for non-set-returning functions with 22023
                if (stmt.rows() != null) {
                    boolean isSrf = func.getReturnType() != null
                            && (func.getReturnType().toUpperCase().startsWith("SETOF")
                                || func.getReturnType().toUpperCase().contains("TABLE"));
                    if (!isSrf) {
                        throw new MemgresException(
                                "ROWS is not applicable when function does not return a set", "22023");
                    }
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

    /**
     * Rename a PK/UNIQUE constraint-backed index. Returns true if found and renamed.
     *
     * <p>The written name is resolved the way every other relation name is: a qualifier names one
     * schema and no other, a bare name walks the search path. Comparing the whole written name
     * against bare constraint names refused {@code ALTER INDEX a.t_pkey RENAME TO ...} outright,
     * because no constraint is ever called {@code a.t_pkey}.
     */
    private boolean renameConstraintIndex(String oldName, String newName) {
        int dot = oldName.lastIndexOf('.');
        String bare = dot > 0 ? oldName.substring(dot + 1) : oldName;
        List<String> path = dot > 0
                ? Collections.singletonList(oldName.substring(0, dot))
                : relationSearchPath();
        for (String schemaName : path) {
            Schema s = database.getSchema(schemaName);
            if (s == null) continue;
            for (Table t : s.getTables().values()) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if ((sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                            || sc.getType() == StoredConstraint.Type.UNIQUE)
                            && sc.getName().equalsIgnoreCase(bare)) {
                        // A rename never moves the index, so the new name has to be free right
                        // here — including of the tables and sequences the schema already holds.
                        RelationNamespace.requireFree(database, schemaName, newName, null);
                        String was = sc.getName();
                        sc.setName(newName);
                        // The index a constraint is backed by owns a pg_class row and an OID of
                        // its own, and both stay with it across the rename.
                        identity().relationRenamed("i", schemaName, was, schemaName, newName);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Refuse an ALTER INDEX action that is not the generic relation rename against a relation
     * that is not an index. Only the rename is kind-blind in PostgreSQL; everything else opens
     * the index and says what the name really holds instead of reporting it missing.
     */
    private void requireIndexKind(String schemaName, String bareName) {
        RelationNamespace.requireKind(database, schemaName, bareName, RelationNamespace.INDEX);
    }

    // ---- ALTER INDEX ----

    private QueryResult executeAlterIndex(AlterIndexStmt stmt) {
        // A qualified name reaches the index in the schema it names; a bare one walks the search
        // path. Reaching whichever index of that name existed anywhere let ALTER INDEX other.i
        // rename an index the statement never named.
        String idxKey = database.resolveIndexName(relationSearchPath(), stmt.name());
        String bareIdx = RelationNamespace.bareName(stmt.name());
        // Where a complaint about this name belongs. With no index of the name, that is still the
        // schema the statement wrote, or the first on the path holding any relation of the name —
        // using the default schema said "does not exist" about a relation sitting elsewhere.
        String idxSchema = idxKey != null ? Database.idxSchema(idxKey)
                : SchemaQualifier.qualifierOf(stmt.name());
        if (idxSchema == null) {
            idxSchema = RelationNamespace.schemaHolding(database, relationSearchPath(), bareIdx);
        }
        if (idxSchema == null) idxSchema = defaultSchema();
        switch (stmt.action()) {
            case RENAME_TO: {
                boolean found = idxKey != null;
                if (!found) {
                    // Also check PK/UNIQUE constraint-backed indexes
                    found = renameConstraintIndex(stmt.name(), stmt.targetValue());
                }
                if (!found) {
                    // PostgreSQL renames whatever relation the name owns here: ALTER INDEX's
                    // rename runs the generic relation rename and never checks the kind, so
                    // ALTER INDEX naming a table renames the table. Refusing it turned SQL
                    // PostgreSQL accepts into a missing-relation error.
                    if (RelationNamespace.kindOf(database, idxSchema, bareIdx) != null) {
                        // The relation is in idxSchema; handing the rename the written name with
                        // its qualifier still attached made it look for a table called "a.t".
                        return ddlExecutor.executeAlterTable(new AlterTableStmt(idxSchema, bareIdx,
                                java.util.Collections.<AlterTableStmt.AlterAction>singletonList(
                                        new AlterTableStmt.RenameTable(stmt.targetValue())),
                                stmt.ifExists()));
                    }
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                if (idxKey != null) {
                    // The new name has to be free of every kind of relation, not only of another
                    // index: renaming an index onto a table left two relations answering to one
                    // name and the index unreachable under either. A rename never moves the index
                    // out of the schema it is in, so it is that schema the name has to be free in.
                    RelationNamespace.requireFree(database, idxSchema, stmt.targetValue(), null);
                    if (database.hasIndex(idxSchema, stmt.targetValue())) {
                        throw new MemgresException("relation \"" + stmt.targetValue() + "\" already exists", "42P07");
                    }
                    database.renameIndex(idxKey, stmt.targetValue());
                    // The same index under a new name: its OID and its comment stay with it.
                    identity().relationRenamed("i", idxSchema, Database.idxName(idxKey),
                            idxSchema, stmt.targetValue());
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
            }
            case SET_PARAMS: {
                if (idxKey == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
                    requireIndexKind(idxSchema, bareIdx);
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                if (stmt.params != null && !stmt.params.isEmpty()) {
                    // A parameter the access method does not have, or a value outside its range,
                    // is refused here rather than stored as written.
                    DdlIndexValidator.checkRelOptions(database.getIndexMethod(idxKey), stmt.params);
                    java.util.Map<String, String> existing = database.getIndexReloptions(idxKey);
                    java.util.Map<String, String> merged = existing != null ? new java.util.LinkedHashMap<>(existing) : new java.util.LinkedHashMap<>();
                    merged.putAll(stmt.params);
                    database.setIndexReloptions(idxKey, merged);
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
            }
            case RESET_PARAMS: {
                if (idxKey == null) {
                    if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
                    requireIndexKind(idxSchema, bareIdx);
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                if (stmt.params != null && !stmt.params.isEmpty()) {
                    java.util.Map<String, String> existing = database.getIndexReloptions(idxKey);
                    if (existing != null) {
                        java.util.Map<String, String> updated = new java.util.LinkedHashMap<>(existing);
                        for (String key : stmt.params.keySet()) {
                            updated.remove(key);
                        }
                        if (updated.isEmpty()) {
                            database.removeIndexReloptions(idxKey);
                        } else {
                            database.setIndexReloptions(idxKey, updated);
                        }
                    }
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
            }
            case ATTACH_PARTITION: {
                String childIdx = stmt.targetValue();
                String parentIdx = stmt.name();
                // The parent map is keyed by schema and name, so the written names have to be
                // resolved to those keys before they can be compared with what it holds.
                String parentKey = database.resolveIndexName(relationSearchPath(), parentIdx);
                if (parentKey == null) parentKey = Database.idxKey(idxSchema, parentIdx);
                if (childIdx != null) {
                    // Validate: reject if parent already has a child index for the same partition table
                    String childTable = database.getIndexTable(childIdx);
                    if (childTable != null) {
                        for (Map.Entry<String, String> entry : database.getIndexParentMap().entrySet()) {
                            if (entry.getValue().equalsIgnoreCase(parentKey)) {
                                String existingChildTable = database.getIndexTable(entry.getKey());
                                if (childTable.equalsIgnoreCase(existingChildTable != null ? existingChildTable : "")) {
                                    // PostgreSQL names the partition that is already covered,
                                    // which is the one the reader has to go and look at.
                                    MemgresException taken = new MemgresException(
                                            "cannot attach index \"" + childIdx
                                            + "\" as a partition of index \"" + parentIdx + "\"",
                                            "55000");
                                    // The name is the relation's own, as PostgreSQL prints it:
                                    // the schema the map keys the partition under is memgres's
                                    // bookkeeping rather than part of what the partition is called.
                                    taken.setDetail("Another index is already attached for partition \""
                                            + RelationNamespace.bareName(childTable) + "\".");
                                    throw taken;
                                }
                            }
                        }
                    }
                    database.setIndexParent(childIdx, parentIdx);
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
            }
            default:
                // All other actions (SET TABLESPACE, etc.) are accepted no-ops
                if (stmt.action() != AlterIndexStmt.Action.NO_OP
                        && idxKey == null && !stmt.ifExists()) {
                    requireIndexKind(idxSchema, bareIdx);
                    throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
                }
                return QueryResult.message(QueryResult.Type.SET, "ALTER INDEX");
        }
    }

    // ---- Event Trigger Support ----

    /**
     * A read-only transaction refuses anything that would change the database. PG draws the line
     * at the schema: every CREATE, ALTER and DROP is out, along with TRUNCATE, GRANT and REVOKE.
     * DML is checked separately, closer to the table it writes.
     */
    private void checkReadOnlyTransaction(Statement stmt) {
        if (session == null || !session.isReadOnly()) return;
        String cls = stmt.getClass().getSimpleName();
        String command;
        if (stmt instanceof TruncateStmt) command = "TRUNCATE TABLE";
        else if (stmt instanceof GrantStmt) command = "GRANT";
        else if (stmt instanceof RevokeStmt) command = "REVOKE";
        else if (cls.startsWith("Create") || cls.startsWith("Alter") || cls.startsWith("Drop")) {
            command = commandTagOf(cls);
        } else return;
        throw new MemgresException(
                "cannot execute " + command + " in a read-only transaction", "25006");
    }

    /** {@code CreateTableStmt} names the command {@code CREATE TABLE}, the way PG reports it. */
    private String commandTagOf(String className) {
        String name = className.endsWith("Stmt")
                ? className.substring(0, className.length() - 4) : className;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (i > 0 && Character.isUpperCase(c)) sb.append(' ');
            sb.append(Character.toUpperCase(c));
        }
        return sb.toString();
    }

    /**
     * Determine the DDL command tag for a statement (e.g. "CREATE TABLE", "ALTER TABLE").
     */
    private String getDdlTag(Statement stmt) {
        if (stmt instanceof CreateTableStmt) return "CREATE TABLE";
        if (stmt instanceof DropTableStmt) return "DROP TABLE";
        if (stmt instanceof AlterTableStmt) return "ALTER TABLE";
        if (stmt instanceof CreateIndexStmt) return "CREATE INDEX";
        if (stmt instanceof CreateViewStmt) return "CREATE VIEW";
        if (stmt instanceof CreateFunctionStmt) return "CREATE FUNCTION";
        if (stmt instanceof CreateTypeStmt) return "CREATE TYPE";
        if (stmt instanceof CreateSequenceStmt) return "CREATE SEQUENCE";
        if (stmt instanceof CreateTriggerStmt) return "CREATE TRIGGER";
        if (stmt instanceof DropStmt) return "DROP";
        return "DDL";
    }

    /**
     * Fire all enabled event triggers that match the given event and optional tag.
     */
    void fireEventTriggers(String event, String tag) {
        for (PgEventTrigger et : database.getAllEventTriggers().values()) {
            if (et.getEnabled() == 'D') continue; // disabled
            if (!et.getEvent().equals(event)) continue;
            // Check tag filter
            if (et.getTags() != null && !et.getTags().isEmpty()) {
                boolean matchesTag = false;
                for (String t : et.getTags()) {
                    if (t.equalsIgnoreCase(tag)) { matchesTag = true; break; }
                }
                if (!matchesTag) continue;
            }
            // Find and execute the function
            PgFunction func = database.getFunction(et.getFunctionName());
            if (func != null && func.getBody() != null) {
                try {
                    PlpgsqlExecutor plpgsql = new PlpgsqlExecutor(this, database);
                    plpgsql.executeEventTriggerFunction(func, tag, event);
                } catch (Exception e) {
                    LOG.debug("Event trigger {} function execution error: {}", et.getName(), e.getMessage());
                }
            }
        }
    }

    /**
     * Register extension-specific objects (opfamilies, functions) when an extension is created.
     */
    private void registerExtensionObjects(String extName) {
        switch (extName.toLowerCase()) {
            case "btree_gin": {
                // Register gin opfamilies for scalar types (PG uses int4_ops, not integer_ops)
                for (String typeName : new String[]{"int4_ops", "text_ops", "bool_ops", "float8_ops",
                        "numeric_ops", "timestamptz_ops", "uuid_ops"}) {
                    String key = typeName + ":gin";
                    if (!database.hasOperatorFamily(key)) {
                        PgOperatorFamily fam = new PgOperatorFamily(typeName, "gin");
                        fam.setSchemaName("pg_catalog");
                        database.addOperatorFamily(fam);
                    }
                }
                break;
            }
            case "btree_gist": {
                // Register gist opfamilies for scalar types (PG uses int4_ops, not integer_ops)
                for (String typeName : new String[]{"int4_ops", "text_ops", "bool_ops", "float8_ops",
                        "numeric_ops", "timestamptz_ops", "uuid_ops"}) {
                    String key = typeName + ":gist";
                    if (!database.hasOperatorFamily(key)) {
                        PgOperatorFamily fam = new PgOperatorFamily(typeName, "gist");
                        fam.setSchemaName("pg_catalog");
                        database.addOperatorFamily(fam);
                    }
                }
                break;
            }
            case "tablefunc": {
                // Register crosstab function overloads (PG has 3)
                if (database.getFunction("crosstab") == null) {
                    // crosstab(text) → setof record
                    PgFunction fn1 = new PgFunction("crosstab", "record", "SELECT NULL", "sql",
                            Cols.listOf(new PgFunction.Param("sql", "text", null, null)), false);
                    fn1.setSchemaName("pg_catalog");
                    database.addFunction(fn1);
                    // crosstab(text, int) → setof record
                    PgFunction fn2 = new PgFunction("crosstab", "record", "SELECT NULL", "sql",
                            Cols.listOf(new PgFunction.Param("sql", "text", null, null),
                                    new PgFunction.Param("n", "integer", null, null)), false);
                    fn2.setSchemaName("pg_catalog");
                    database.addFunction(fn2);
                    // crosstab(text, text) → setof record
                    PgFunction fn3 = new PgFunction("crosstab", "record", "SELECT NULL", "sql",
                        Cols.listOf(new PgFunction.Param("source_sql", "text", null, null),
                                    new PgFunction.Param("category_sql", "text", null, null)), false);
                    fn3.setSchemaName("pg_catalog");
                    database.addFunction(fn3);
                }
                break;
            }
            case "pgrowlocks": {
                // Register pgrowlocks function
                if (database.getFunction("pgrowlocks") == null) {
                    PgFunction fn = new PgFunction("pgrowlocks", "record", "", "c");
                    fn.setSchemaName("pg_catalog");
                    database.addFunction(fn);
                }
                break;
            }
            case "citext": {
                // citext is a case-insensitive text type; casting is handled in CastEvaluator
                // and equality comparison works through CitextValue.equals()
                break;
            }
            default:
                // no special registrations needed
                break;
        }
    }
}
