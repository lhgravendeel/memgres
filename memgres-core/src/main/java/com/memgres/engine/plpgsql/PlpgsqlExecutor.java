package com.memgres.engine.plpgsql;

import com.memgres.engine.util.Cols;

import com.memgres.engine.*;
import com.memgres.engine.*;
import com.memgres.engine.parser.Lexer;
import com.memgres.engine.parser.Token;
import com.memgres.engine.parser.TokenType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Executes PL/pgSQL function bodies.
 * Manages variable scope, control flow, and delegates SQL to AstExecutor.
 * Expressions are evaluated by substituting variables and using SELECT expr.
 */
public class PlpgsqlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PlpgsqlExecutor.class);

    private final AstExecutor astExecutor;
    private final Database database;
    private final Session session; // may be null when no client connection

    // Procedure transaction control context (PG 11+)
    private boolean isProcedureExecution;
    private int exceptionBlockDepth;
    // Track current function name for PG_EXCEPTION_CONTEXT
    private String currentFunctionName;

    /**
     * How an identifier that names both a variable and a column is resolved: PG's default is
     * "error", and the {@code #variable_conflict} pragma at the top of a body changes it.
     */
    private String variableConflict = "error";
    // Parameter names (lower-cased) of the current function. In PG, the hidden outer block
    // labeled with the function name contains ONLY the parameters, so function-name
    // qualification (funcname.x) resolves parameters but never body-DECLAREd variables.
    private java.util.Set<String> currentFunctionParams;

    // Control flow signals
    private static class ReturnSignal extends RuntimeException {
        final Object value;
        ReturnSignal(Object value) { super(null, null, true, false); this.value = value; }
    }

    private static class ExitSignal extends RuntimeException {
        final String label;
        ExitSignal(String label) { super(null, null, true, false); this.label = label; }
    }

    private static class ContinueSignal extends RuntimeException {
        final String label;
        ContinueSignal(String label) { super(null, null, true, false); this.label = label; }
    }

    /** Wrapper for a pre-formatted SQL expression that should be emitted verbatim by appendValue. */
    private static class RawSql {
        final String sql;
        RawSql(String sql) { this.sql = sql; }
    }

    // Variable scope
    class Scope {
        final Map<String, Object> variables = new LinkedHashMap<>();
        final Map<String, String> declaredTypes = new LinkedHashMap<>();
        /** Variables declared NOT NULL, mapped to the spelling the declaration used. */
        final Map<String, String> notNullVars = new LinkedHashMap<>();
        /** Variables of a domain type, so every assignment re-checks the domain's constraints. */
        final Map<String, String> domainVars = new LinkedHashMap<>();
        final java.util.Set<String> outputOnlyVars = new java.util.HashSet<>();
        final Scope parent;
        int lastRowCount = 0;
        /** Label of the block that opened this scope, so {@code label.var} can reach it. */
        String label;

        Scope(Scope parent) { this.parent = parent; }

        /** The scope opened by the block carrying this label, or null when no block has it. */
        Scope labeled(String name) {
            Scope s = this;
            while (s != null) {
                if (name.equalsIgnoreCase(s.label)) return s;
                s = s.parent;
            }
            return null;
        }

        Object get(String name) {
            String key = name.toLowerCase();
            if (variables.containsKey(key)) return variables.get(key);
            if (parent != null) return parent.get(key);
            return null;
        }

        void set(String name, Object value) {
            String key = name.toLowerCase();
            Scope s = this;
            while (s != null) {
                if (s.variables.containsKey(key)) {
                    s.variables.put(key, s.checkAssignable(key, value));
                    s.outputOnlyVars.remove(key); // once explicitly assigned, it's no longer output-only
                    return;
                }
                s = s.parent;
            }
            variables.put(key, value);
        }

        /**
         * A declaration's NOT NULL and a domain's constraints have to hold on every write, not
         * only on the initialiser — otherwise a value the declared type cannot represent moves
         * freely through the variable.
         */
        Object checkAssignable(String key, Object value) {
            String declaredName = notNullVars.get(key);
            if (declaredName != null && value == null) {
                throw new MemgresException("null value cannot be assigned to variable \""
                        + declaredName + "\" declared NOT NULL", "22004");
            }
            String domain = domainVars.get(key);
            return domain != null ? astExecutor.castValue(value, domain) : value;
        }

        boolean has(String name) {
            String key = name.toLowerCase();
            if (variables.containsKey(key)) return true;
            return parent != null && parent.has(key);
        }

        boolean isOutputOnly(String name) {
            String key = name.toLowerCase();
            if (outputOnlyVars.contains(key)) return true;
            return parent != null && parent.isOutputOnly(key);
        }

        void declare(String name, Object value) {
            variables.put(name.toLowerCase(), value);
        }

        /** Record the type the variable was declared with, so assignments can be checked. */
        void declareTyped(String name, Object value, String typeName) {
            String key = name.toLowerCase();
            variables.put(key, value);
            if (typeName != null) declaredTypes.put(key, typeName.toLowerCase().trim());
        }

        String declaredType(String name) {
            String key = name.toLowerCase();
            Scope sc = this;
            while (sc != null) {
                if (sc.declaredTypes.containsKey(key)) return sc.declaredTypes.get(key);
                if (sc.variables.containsKey(key)) return null;
                sc = sc.parent;
            }
            return null;
        }

        void declareOutputOnly(String name, Object value) {
            String key = name.toLowerCase();
            variables.put(key, value);
            outputOnlyVars.add(key);
        }
    }

    public PlpgsqlExecutor(AstExecutor astExecutor, Database database) {
        this(astExecutor, database, null);
    }

    public PlpgsqlExecutor(AstExecutor astExecutor, Database database, Session session) {
        this.astExecutor = astExecutor;
        this.database = database;
        this.session = session;
    }

    /**
     * Execute a DO block (anonymous PL/pgSQL code block).
     */
    public void executeDoBlock(String body) {
        PlpgsqlStatement.Block block;
        try {
            block = PlpgsqlParser.parse(body);
        } catch (MemgresException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MemgresException(e.getMessage() != null ? e.getMessage() : "syntax error in PL/pgSQL block", "42601");
        }
        // PG compiles a DO block before running it, so a declaration it rejects never reaches a
        // statement — nothing the block would have done takes effect.
        PlpgsqlBodyValidator.validate(astExecutor, block, null);
        // DO blocks are anonymous code blocks that support transaction control (PG 11+)
        this.isProcedureExecution = true;
        this.currentFunctionName = null;
        this.currentFunctionParams = null;
        Scope scope = new Scope(null);
        scope.declare("found", false);
        if (session != null) session.enterNestedExecution();
        try {
            executeBlock(block, scope);
        } catch (ReturnSignal rs) {
            // DO blocks can RETURN but the value is discarded
        } finally {
            if (session != null) session.exitNestedExecution();
        }
    }

    public Object executeFunction(PgFunction function, List<Object> args) {
        this.isProcedureExecution = function.isProcedure();
        this.currentFunctionName = function.getName();
        // Track function call depth: when inside a non-procedure function, transaction control is forbidden
        boolean enteredFunctionContext = false;
        if (!function.isProcedure() && session != null) {
            session.enterFunctionCall();
            enteredFunctionContext = true;
        }
        // Apply function-level SET clauses (save current values, apply overrides)
        java.util.Map<String, String> savedGuc = null;
        if (function.getSetClauses() != null && !function.getSetClauses().isEmpty() && session != null) {
            GucSettings guc = session.getGucSettings();
            savedGuc = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, String> entry : function.getSetClauses().entrySet()) {
                savedGuc.put(entry.getKey(), guc.get(entry.getKey()));
                guc.set(entry.getKey(), entry.getValue());
            }
        }

        // SECURITY DEFINER: switch current_user to function owner during execution
        boolean roleWasOverridden = false;
        String savedRole = null;
        if (function.isSecurityDefiner() && session != null && function.getOwner() != null) {
            GucSettings guc = session.getGucSettings();
            roleWasOverridden = guc.hasSessionOverride("role");
            if (roleWasOverridden) {
                savedRole = guc.get("role");
            }
            guc.set("role", function.getOwner());
        }

        if (session != null) session.enterNestedExecution();
        try {
            return executeFunctionBody(function, args);
        } finally {
            if (session != null) session.exitNestedExecution();
            // Exit function context
            if (enteredFunctionContext) {
                session.exitFunctionCall();
            }
            // Restore SECURITY DEFINER role
            if (function.isSecurityDefiner() && session != null && function.getOwner() != null) {
                GucSettings guc = session.getGucSettings();
                if (roleWasOverridden) {
                    guc.set("role", savedRole);
                } else {
                    guc.reset("role");
                }
            }

            // Restore GUC settings after function returns
            if (savedGuc != null) {
                GucSettings guc = session.getGucSettings();
                for (java.util.Map.Entry<String, String> entry : savedGuc.entrySet()) {
                    if (entry.getValue() != null) {
                        guc.set(entry.getKey(), entry.getValue());
                    } else {
                        guc.reset(entry.getKey());
                    }
                }
            }
        }
    }

    private Object executeFunctionBody(PgFunction function, List<Object> args) {
        PlpgsqlStatement.Block block;
        String lang = function.getLanguage() != null ? function.getLanguage().toLowerCase() : "plpgsql";

        // Record parameter names for function-name-qualified references (funcname.param)
        java.util.Set<String> paramNames = new java.util.HashSet<>();
        for (int i = 0; i < function.getParams().size(); i++) {
            PgFunction.Param p = function.getParams().get(i);
            paramNames.add(p.name() != null ? p.name().toLowerCase() : ("$" + (i + 1)));
        }
        this.currentFunctionParams = paramNames;

        if (lang.equals("sql")) {
            // SQL language function: execute body as SQL with param substitution
            return executeSqlFunction(function, args);
        }

        if (lang.equals("internal")) {
            return executeInternalFunction(function.getName(), args);
        }

        block = PlpgsqlParser.parse(function.getBody());
        Scope scope = new Scope(null);

        // Bind parameters: OUT params get null initial value, INOUT params get caller's value
        List<PgFunction.Param> params = function.getParams();
        List<PgFunction.Param> outParams = new ArrayList<>();
        int argIdx = 0;
        for (int i = 0; i < params.size(); i++) {
            PgFunction.Param p = params.get(i);
            String pName = p.name() != null ? p.name() : ("$" + (i + 1));
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            Object val;
            if ("OUT".equals(mode)) {
                val = null; // OUT params start as null
                outParams.add(p);
            } else if ("INOUT".equals(mode)) {
                val = argIdx < args.size() ? args.get(argIdx++) : null;
                outParams.add(p);
            } else if ("VARIADIC".equals(mode)) {
                // Collect all remaining args into an array
                List<Object> variadicArgs = new ArrayList<>();
                while (argIdx < args.size()) {
                    variadicArgs.add(args.get(argIdx++));
                }
                val = variadicArgs;
            } else {
                // IN (or default)
                if (argIdx < args.size()) {
                    val = args.get(argIdx++);
                } else if (p.defaultExpr() != null) {
                    QueryResult defaultResult = astExecutor.execute("SELECT " + p.defaultExpr());
                    val = (!defaultResult.getRows().isEmpty() && defaultResult.getRows().get(0).length > 0)
                            ? defaultResult.getRows().get(0)[0] : null;
                } else {
                    val = null;
                }
            }
            val = coerceParamValue(val, p.typeName());
            scope.declare(pName, val);
        }
        scope.declare("found", false);

        String returnType = function.getReturnType();
        boolean isSetof = returnType != null && returnType.toUpperCase().startsWith("SETOF");
        boolean isTable = returnType != null && returnType.equalsIgnoreCase("TABLE");

        // Mark OUT params of RETURNS TABLE functions as output-only to prevent
        // substituteVariables from replacing column names that match OUT param names
        if (isTable) {
            for (PgFunction.Param p : outParams) {
                String pName = p.name() != null ? p.name() : ("$" + (params.indexOf(p) + 1));
                scope.declareOutputOnly(pName, null);
            }
        }

        if (isSetof || isTable) {
            List<Object> results = new ArrayList<>();
            scope.declare("__return_next_results__", results);
            // Store out param names for RETURN NEXT with no expression
            if (isTable && !outParams.isEmpty()) {
                List<String> outNames = new ArrayList<>();
                for (PgFunction.Param p : outParams) {
                    outNames.add(p.name() != null ? p.name().toLowerCase() : ("$" + (params.indexOf(p) + 1)));
                }
                scope.declare("__return_next_out_params__", outNames);
            }
            try {
                executeBlock(block, scope);
            } catch (ReturnSignal rs) { /* done */ }
            return results;
        }

        try {
            executeBlock(block, scope);
        } catch (ReturnSignal rs) {
            // Explicit RETURN; if we also have OUT params, prefer the OUT param values
            if (outParams.isEmpty()) return rs.value;
        }

        // Collect OUT/INOUT param values as the return value
        if (!outParams.isEmpty()) {
            if (outParams.size() == 1) {
                String pName = outParams.get(0).name() != null ? outParams.get(0).name()
                        : ("$" + (params.indexOf(outParams.get(0)) + 1));
                return scope.get(pName);
            }
            // Multiple OUT params: return as Object[] (record)
            Object[] record = new Object[outParams.size()];
            for (int i = 0; i < outParams.size(); i++) {
                PgFunction.Param op = outParams.get(i);
                String pName = op.name() != null ? op.name() : ("$" + (params.indexOf(op) + 1));
                record[i] = scope.get(pName);
            }
            return record;
        }

        return null;
    }

    private Object executeInternalFunction(String name, List<Object> args) {
        // Built-in comparison functions for integer types
        switch (name) {
            case "int4eq": return toInt(args.get(0)) == toInt(args.get(1));
            case "int4ne": return toInt(args.get(0)) != toInt(args.get(1));
            case "int4lt": return toInt(args.get(0)) < toInt(args.get(1));
            case "int4gt": return toInt(args.get(0)) > toInt(args.get(1));
            case "int4le": return toInt(args.get(0)) <= toInt(args.get(1));
            case "int4ge": return toInt(args.get(0)) >= toInt(args.get(1));
            default:
                throw new MemgresException("internal function " + name + " is not implemented", "0A000");
        }
    }

    private static int toInt(Object val) {
        if (val instanceof Number) return ((Number) val).intValue();
        if (val instanceof String) return Integer.parseInt((String) val);
        throw new MemgresException("cannot convert " + val + " to integer", "22023");
    }

    private Object executeSqlFunction(PgFunction function, List<Object> args) {
        String body = function.getBody().trim();
        // Substitute parameter names (with default support)
        Scope scope = new Scope(null);
        List<PgFunction.Param> params = function.getParams();
        int argIdx = 0;
        for (int i = 0; i < params.size(); i++) {
            PgFunction.Param p = params.get(i);
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            // Skip pure OUT params (e.g., RETURNS TABLE columns) — they are output column
            // definitions, not input variables. Substituting them would corrupt the SQL body.
            if ("OUT".equalsIgnoreCase(mode)) continue;
            String pName = p.name() != null ? p.name() : ("$" + (argIdx + 1));
            Object val;
            if ("VARIADIC".equalsIgnoreCase(mode)) {
                // Collect all remaining args into a list (array)
                List<Object> variadicArgs = new ArrayList<>();
                for (int j = argIdx; j < args.size(); j++) {
                    variadicArgs.add(args.get(j));
                }
                val = variadicArgs;
                scope.declare(pName, val);
                break; // VARIADIC is always the last param
            } else if (argIdx < args.size()) {
                val = args.get(argIdx);
                // For array (List) parameters, wrap as typed array literal so that
                // substituteVariables produces valid SQL (especially for empty arrays
                // where ARRAY[] without a type cast causes "cannot determine type of empty array")
                if (val instanceof java.util.List<?> && p.typeName() != null) {
                    java.util.List<?> list = (java.util.List<?>) val;
                    StringBuilder arrSb = new StringBuilder();
                    arrSb.append("(ARRAY[");
                    for (int j = 0; j < list.size(); j++) {
                        if (j > 0) arrSb.append(",");
                        Object elem = list.get(j);
                        if (elem == null) arrSb.append("NULL");
                        else if (elem instanceof Number || elem instanceof Boolean) arrSb.append(elem);
                        else arrSb.append("'").append(elem.toString().replace("'", "''")).append("'");
                    }
                    arrSb.append("]::").append(p.typeName()).append(")");
                    val = new RawSql(arrSb.toString());
                }
            } else if (p.defaultExpr() != null) {
                QueryResult defaultResult = astExecutor.execute("SELECT " + p.defaultExpr());
                val = (!defaultResult.getRows().isEmpty() && defaultResult.getRows().get(0).length > 0)
                        ? defaultResult.getRows().get(0)[0] : null;
            } else {
                val = null;
            }
            scope.declare(pName, val);
            argIdx++;
        }
        // SQL-language functions resolve parameter/column name conflicts in favor of the column
        String substituted = substituteVariables(body, scope, true);

        // Split body into individual statements (supports multi-statement SQL bodies)
        List<String> stmts = splitSqlBody(substituted);
        QueryResult result = null;
        for (String sql : stmts) {
            result = astExecutor.execute(sql);
        }

        if (result == null) return null;

        // Check if this is a SETOF or TABLE-returning function
        String returnType = function.getReturnType();
        boolean isSetof = returnType != null && returnType.toUpperCase().startsWith("SETOF");
        boolean isTable = returnType != null && returnType.equalsIgnoreCase("TABLE");
        if (isSetof || isTable) {
            // Return all rows as a List (each row as Object[] or single value)
            List<Object> results = new ArrayList<>();
            for (Object[] row : result.getRows()) {
                results.add(row.length == 1 ? row[0] : row);
            }
            return results;
        }

        // For functions with multiple INOUT params, return all columns as Object[]
        long inoutCount = params.stream()
                .filter(p -> "INOUT".equalsIgnoreCase(p.mode()))
                .count();
        if (inoutCount > 1 && !result.getRows().isEmpty()) {
            Object[] row = result.getRows().get(0);
            if (row.length > 1) return row;
        }

        if (!result.getRows().isEmpty() && result.getRows().get(0).length > 0) {
            return result.getRows().get(0)[0];
        }
        return null;
    }

    /**
     * Execute a trigger function.
     *
     * <p>Returns the (possibly modified) NEW row. For BEFORE / INSTEAD OF row-level triggers,
     * returns {@code null} when the trigger function returned NULL, which signals that the
     * operation on this row must be skipped (PostgreSQL semantics). The return value of
     * AFTER triggers is ignored, as in PostgreSQL.
     */
    public Object[] executeTriggerFunction(PgFunction function, Object[] newRow, Object[] oldRow,
                                           Table table, PgTrigger trigger) {
        PlpgsqlStatement.Block block = PlpgsqlParser.parse(function.getBody());
        this.currentFunctionName = function.getName();
        this.currentFunctionParams = new java.util.HashSet<>(); // trigger functions take no parameters
        Scope scope = new Scope(null);

        Map<String, Object> newMap = new LinkedHashMap<>();
        Map<String, Object> oldMap = new LinkedHashMap<>();
        if (newRow != null) {
            for (int i = 0; i < table.getColumns().size(); i++)
                newMap.put(table.getColumns().get(i).getName().toLowerCase(), newRow[i]);
        }
        if (oldRow != null) {
            for (int i = 0; i < table.getColumns().size(); i++)
                oldMap.put(table.getColumns().get(i).getName().toLowerCase(), oldRow[i]);
        }
        scope.declare("new", newMap);
        scope.declare("old", oldMap);
        scope.declare("found", false);

        if (trigger != null) {
            scope.declare("tg_op", trigger.getEvent().name());
            scope.declare("tg_name", trigger.getName());
            scope.declare("tg_table_name", table.getName());
            scope.declare("tg_table_schema", "public");
            scope.declare("tg_when", trigger.getTiming().name());
            scope.declare("tg_level", trigger.isForEachStatement() ? "STATEMENT" : "ROW");
            java.util.List<String> trigArgs = trigger.getArgs();
            if (trigArgs != null && !trigArgs.isEmpty()) {
                scope.declare("tg_nargs", trigArgs.size());
                scope.declare("tg_argv", trigArgs.toArray(new String[0]));
            } else {
                scope.declare("tg_nargs", 0);
                scope.declare("tg_argv", new String[0]);
            }
            // TG_RELID: OID of the table that caused the trigger invocation
            int relid = astExecutor.getSystemCatalog().getOid("rel:public." + trigger.getTableName());
            scope.declare("tg_relid", relid);
        } else {
            scope.declare("tg_op", "INSERT");
            scope.declare("tg_name", "");
            scope.declare("tg_table_name", table.getName());
            scope.declare("tg_table_schema", "public");
            scope.declare("tg_when", "BEFORE");
            scope.declare("tg_level", "ROW");
            scope.declare("tg_nargs", 0);
            scope.declare("tg_argv", new String[0]);
            int relid = astExecutor.getSystemCatalog().getOid("rel:public." + table.getName());
            scope.declare("tg_relid", relid);
        }

        boolean rowLevel = trigger == null || !trigger.isForEachStatement();
        boolean afterTiming = trigger != null && trigger.getTiming() == PgTrigger.Timing.AFTER;
        // BEFORE and INSTEAD OF row-level triggers may skip the row by returning NULL
        boolean skipCapable = rowLevel && !afterTiming;

        try {
            executeBlock(block, scope);
        } catch (ReturnSignal rs) {
            Object retVal = rs.value;
            if (retVal == null) {
                // BEFORE/INSTEAD OF row triggers: RETURN NULL skips the row.
                // AFTER (and statement-level) triggers: the return value is ignored.
                return skipCapable ? null : newRow;
            }
            if (retVal instanceof Map) {
                // AFTER triggers: PG ignores the returned row entirely
                if (!afterTiming && newRow != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> retMap = (Map<String, Object>) retVal;
                    copyMapToRow(retMap, newRow, table);
                }
                return newRow;
            }
        }

        // Copy NEW map back (skipped for AFTER triggers: modifications to NEW are ignored, as in PG)
        if (!afterTiming) {
            @SuppressWarnings("unchecked")
            Map<String, Object> finalNew = (Map<String, Object>) scope.get("new");
            if (finalNew != null && newRow != null) {
                copyMapToRow(finalNew, newRow, table);
            }
        }
        return newRow;
    }

    /**
     * Execute an event trigger function with tg_tag and tg_event set.
     */
    public void executeEventTriggerFunction(PgFunction function, String tag, String event) {
        PlpgsqlStatement.Block block = PlpgsqlParser.parse(function.getBody());
        Scope scope = new Scope(null);
        scope.declare("tg_tag", tag);
        scope.declare("tg_event", event);
        scope.declare("found", false);
        try {
            executeBlock(block, scope);
        } catch (ReturnSignal rs) {
            // event triggers return void
        }
    }

    private void copyMapToRow(Map<String, Object> map, Object[] row, Table table) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            String colName = table.getColumns().get(i).getName().toLowerCase();
            if (map.containsKey(colName)) {
                row[i] = map.get(colName);
            }
        }
    }

    // ---- Block and statement execution ----

    /**
     * A labelled block is a target for EXIT just as a labelled loop is: leaving it means resuming
     * after its END, not unwinding the whole function.
     */
    private void executeBlock(PlpgsqlStatement.Block block, Scope scope) {
        if (block.variableConflict() != null) {
            variableConflict = block.variableConflict();
        }
        if (block.label() != null) {
            if (scope.label == null) scope.label = block.label();
            try {
                executeBlockBody(block, scope);
            } catch (ExitSignal e) {
                if (e.label == null || !e.label.equalsIgnoreCase(block.label())) throw e;
            }
            return;
        }
        executeBlockBody(block, scope);
    }

    private void executeBlockBody(PlpgsqlStatement.Block block, Scope scope) {
        for (PlpgsqlStatement.VarDeclaration decl : block.declarations()) {
            if (decl.isCursor() && decl.cursorQuery() != null && !decl.cursorQuery().isEmpty()) {
                // A bound cursor keeps its query so OPEN can run it, with or without arguments
                scope.declareTyped(decl.name(),
                        new BoundCursor(decl.name(), decl.cursorQuery(), decl.cursorParams()),
                        decl.typeName());
                continue;
            }
            String coerceType = coercionType(decl.typeName());
            Object defaultVal = null;
            if (decl.defaultExpr() != null) {
                defaultVal = evalDeclarationDefault(decl, coerceType, scope);
            } else if (coerceType != null && database.isDomain(coerceType)) {
                // A variable with no initialiser starts as NULL, which a NOT NULL domain rejects
                astExecutor.castValue(null, coerceType);
            }
            if (decl.notNull() && defaultVal == null) {
                throw new MemgresException("null value cannot be assigned to variable \""
                        + decl.name() + "\" declared NOT NULL", "22004");
            }
            String key = decl.name().toLowerCase();
            if (decl.notNull()) scope.notNullVars.put(key, decl.name());
            if (coerceType != null && database.isDomain(coerceType)) {
                scope.domainVars.put(key, coerceType);
            }
            scope.declareTyped(decl.name(), defaultVal, decl.typeName());
        }

        if (block.exceptionHandlers().isEmpty()) {
            executeStatements(block.body(), scope);
        } else {
            // PG uses a subtransaction for exception blocks — COMMIT/ROLLBACK forbidden inside
            exceptionBlockDepth++;
            // Create a savepoint to rollback changes if an exception is caught (subtransaction semantics)
            String subtxnSavepoint = "__plpgsql_subtxn_" + exceptionBlockDepth + "_" + System.nanoTime();
            // Track whether we implicitly started a transaction for the savepoint (autocommit mode)
            boolean implicitTxnStarted = false;
            if (session != null) {
                implicitTxnStarted = !session.isInTransaction();
                session.savepoint(subtxnSavepoint);
            }
            try {
                executeStatements(block.body(), scope);
                // Body succeeded — release the savepoint
                if (session != null) {
                    try { session.releaseSavepoint(subtxnSavepoint); } catch (Exception ignored) {}
                    if (implicitTxnStarted && session.isInTransaction() && !session.isExplicitTransactionBlock()) {
                        session.commit();
                    }
                }
            } catch (ReturnSignal rs) {
                // Release savepoint on normal RETURN
                if (session != null) {
                    try { session.releaseSavepoint(subtxnSavepoint); } catch (Exception ignored) {}
                    if (implicitTxnStarted && session.isInTransaction() && !session.isExplicitTransactionBlock()) {
                        session.commit();
                    }
                }
                throw rs;
            } catch (MemgresException e) {
                // Rollback to savepoint to undo changes made in the try body (subtransaction rollback)
                if (session != null) {
                    try { session.rollbackToSavepoint(subtxnSavepoint); } catch (Exception ignored) {}
                }
                String sqlState = e.getSqlState() != null ? e.getSqlState() : "P0001";
                for (PlpgsqlStatement.ExceptionHandler handler : block.exceptionHandlers()) {
                    if (matchesCondition(handler.conditionNames(), sqlState)) {
                        Scope handlerScope = new Scope(scope);
                        handlerScope.declare("sqlerrm", e.getMessage());
                        handlerScope.declare("sqlstate", sqlState);
                        populateDiagnosticScope(handlerScope, e);
                        try {
                            executeStatements(handler.body(), handlerScope);
                        } catch (ReturnSignal rs) {
                            releaseSubtxnSavepoint(subtxnSavepoint, implicitTxnStarted);
                            throw rs;
                        }
                        releaseSubtxnSavepoint(subtxnSavepoint, implicitTxnStarted);
                        return;
                    }
                }
                throw e;
            } catch (RuntimeException e) {
                // Rollback to savepoint to undo changes made in the try body (subtransaction rollback)
                if (session != null) {
                    try { session.rollbackToSavepoint(subtxnSavepoint); } catch (Exception ignored) {}
                }
                // Handle Java exceptions (like ArithmeticException for / by zero)
                String sqlState = mapJavaExceptionToSqlState(e);
                for (PlpgsqlStatement.ExceptionHandler handler : block.exceptionHandlers()) {
                    if (matchesCondition(handler.conditionNames(), sqlState)) {
                        Scope handlerScope = new Scope(scope);
                        handlerScope.declare("sqlerrm", e.getMessage());
                        handlerScope.declare("sqlstate", sqlState);
                        try {
                            executeStatements(handler.body(), handlerScope);
                        } catch (ReturnSignal rs) {
                            releaseSubtxnSavepoint(subtxnSavepoint, implicitTxnStarted);
                            throw rs;
                        }
                        releaseSubtxnSavepoint(subtxnSavepoint, implicitTxnStarted);
                        return;
                    }
                }
                throw e;
            } finally {
                exceptionBlockDepth--;
            }
        }
    }

    /**
     * PG runs every initialiser through the declared type's input function, so a value the type
     * cannot represent fails at the declaration rather than travelling on inside the variable.
     * The array case matters twice over: without the cast the variable holds the text of an
     * array, which a later subscript has nothing to index into.
     */
    private Object evalDeclarationDefault(PlpgsqlStatement.VarDeclaration decl, String coerceType,
                                          Scope scope) {
        if (coerceType != null) {
            return evalExpr("CAST(" + decl.defaultExpr() + " AS " + coerceType + ")", scope);
        }
        return evalExpr(decl.defaultExpr(), scope);
    }

    /**
     * The concrete type an initialiser and later assignments are coerced to, or null when the
     * declared type carries no coercion of its own — a record, a cursor or a whole row, where a
     * cast would say nothing the value does not already say.
     */
    private String coercionType(String typeName) {
        if (typeName == null) return null;
        String type = typeName.trim();
        if (type.isEmpty()) return null;
        String upper = type.toUpperCase();
        if (upper.endsWith("%ROWTYPE")) return null;
        if (upper.endsWith("%TYPE")) {
            String resolved = astExecutor.resolveTypeReference(
                    type.substring(0, type.length() - "%TYPE".length()));
            return resolved != null ? coercionType(resolved) : null;
        }
        String base = type.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        if (base.equalsIgnoreCase("record") || base.equalsIgnoreCase("refcursor")) return null;
        // A composite value already carries its own field names; casting it adds nothing
        if (database.isCompositeType(base) || database.getRowType(base) != null) return null;
        return type;
    }

    /**
     * Release a subtransaction savepoint and commit the implicit transaction if one was started.
     * Called after a PL/pgSQL EXCEPTION handler completes (normally or via RETURN).
     */
    private void releaseSubtxnSavepoint(String savepoint, boolean implicitTxnStarted) {
        if (session != null) {
            try { session.releaseSavepoint(savepoint); } catch (Exception ignored) {}
            if (implicitTxnStarted && session.isInTransaction() && !session.isExplicitTransactionBlock()) {
                session.commit();
            }
        }
    }

    private void executeStatements(List<PlpgsqlStatement> stmts, Scope scope) {
        for (PlpgsqlStatement stmt : stmts) {
            executeStatement(stmt, scope);
        }
    }

    private void executeStatement(PlpgsqlStatement stmt, Scope scope) {
        if (stmt instanceof PlpgsqlStatement.Block) {
            PlpgsqlStatement.Block b = (PlpgsqlStatement.Block) stmt;
            executeBlock(b, new Scope(scope));
        } else if (stmt instanceof PlpgsqlStatement.Assignment) {
            PlpgsqlStatement.Assignment a = (PlpgsqlStatement.Assignment) stmt;
            executeAssignment(a, scope);
        } else if (stmt instanceof PlpgsqlStatement.SubscriptAssignment) {
            executeSubscriptAssignment((PlpgsqlStatement.SubscriptAssignment) stmt, scope);
        } else if (stmt instanceof PlpgsqlStatement.CaseStmt) {
            PlpgsqlStatement.CaseStmt c = (PlpgsqlStatement.CaseStmt) stmt;
            executeCase(c, scope);
        } else if (stmt instanceof PlpgsqlStatement.IfStmt) {
            PlpgsqlStatement.IfStmt i = (PlpgsqlStatement.IfStmt) stmt;
            executeIf(i, scope);
        } else if (stmt instanceof PlpgsqlStatement.LoopStmt) {
            PlpgsqlStatement.LoopStmt l = (PlpgsqlStatement.LoopStmt) stmt;
            executeLoop(l, scope);
        } else if (stmt instanceof PlpgsqlStatement.WhileStmt) {
            PlpgsqlStatement.WhileStmt w = (PlpgsqlStatement.WhileStmt) stmt;
            executeWhile(w, scope);
        } else if (stmt instanceof PlpgsqlStatement.ForStmt) {
            PlpgsqlStatement.ForStmt f = (PlpgsqlStatement.ForStmt) stmt;
            executeFor(f, scope);
        } else if (stmt instanceof PlpgsqlStatement.ForQueryStmt) {
            PlpgsqlStatement.ForQueryStmt fq = (PlpgsqlStatement.ForQueryStmt) stmt;
            executeForQuery(fq, scope);
        } else if (stmt instanceof PlpgsqlStatement.ForExecuteStmt) {
            PlpgsqlStatement.ForExecuteStmt fe = (PlpgsqlStatement.ForExecuteStmt) stmt;
            executeForExecute(fe, scope);
        } else if (stmt instanceof PlpgsqlStatement.ForeachStmt) {
            PlpgsqlStatement.ForeachStmt fe = (PlpgsqlStatement.ForeachStmt) stmt;
            executeForeach(fe, scope);
        } else if (stmt instanceof PlpgsqlStatement.ExitStmt) {
            PlpgsqlStatement.ExitStmt e = (PlpgsqlStatement.ExitStmt) stmt;
            executeExit(e, scope);
        } else if (stmt instanceof PlpgsqlStatement.ContinueStmt) {
            PlpgsqlStatement.ContinueStmt c = (PlpgsqlStatement.ContinueStmt) stmt;
            executeContinue(c, scope);
        } else if (stmt instanceof PlpgsqlStatement.ReturnStmt) {
            PlpgsqlStatement.ReturnStmt r = (PlpgsqlStatement.ReturnStmt) stmt;
            executeReturn(r, scope);
        } else if (stmt instanceof PlpgsqlStatement.ReturnNextStmt) {
            PlpgsqlStatement.ReturnNextStmt rn = (PlpgsqlStatement.ReturnNextStmt) stmt;
            executeReturnNext(rn, scope);
        } else if (stmt instanceof PlpgsqlStatement.ReturnQueryStmt) {
            PlpgsqlStatement.ReturnQueryStmt rq = (PlpgsqlStatement.ReturnQueryStmt) stmt;
            executeReturnQuery(rq, scope);
        } else if (stmt instanceof PlpgsqlStatement.ReturnQueryExecuteStmt) {
            PlpgsqlStatement.ReturnQueryExecuteStmt rqe = (PlpgsqlStatement.ReturnQueryExecuteStmt) stmt;
            executeReturnQueryExecute(rqe, scope);
        } else if (stmt instanceof PlpgsqlStatement.AssertStmt) {
            PlpgsqlStatement.AssertStmt as = (PlpgsqlStatement.AssertStmt) stmt;
            executeAssert(as, scope);
        } else if (stmt instanceof PlpgsqlStatement.RaiseStmt) {
            PlpgsqlStatement.RaiseStmt ra = (PlpgsqlStatement.RaiseStmt) stmt;
            executeRaise(ra, scope);
        } else if (stmt instanceof PlpgsqlStatement.PerformStmt) {
            PlpgsqlStatement.PerformStmt p = (PlpgsqlStatement.PerformStmt) stmt;
            executePerform(p, scope);
        } else if (stmt instanceof PlpgsqlStatement.ExecuteStmt) {
            PlpgsqlStatement.ExecuteStmt ex = (PlpgsqlStatement.ExecuteStmt) stmt;
            executeExecute(ex, scope);
        } else if (stmt instanceof PlpgsqlStatement.SqlStmt) {
            PlpgsqlStatement.SqlStmt s = (PlpgsqlStatement.SqlStmt) stmt;
            executeSql(s, scope);
        } else if (stmt instanceof PlpgsqlStatement.NullStmt) {
            // no-op
        } else if (stmt instanceof PlpgsqlStatement.GetDiagnosticsStmt) {
            PlpgsqlStatement.GetDiagnosticsStmt gd = (PlpgsqlStatement.GetDiagnosticsStmt) stmt;
            executeGetDiagnostics(gd, scope);
        } else if (stmt instanceof PlpgsqlStatement.OpenCursorStmt) {
            PlpgsqlStatement.OpenCursorStmt oc = (PlpgsqlStatement.OpenCursorStmt) stmt;
            executeOpenCursor(oc, scope);
        } else if (stmt instanceof PlpgsqlStatement.FetchStmt) {
            PlpgsqlStatement.FetchStmt fs = (PlpgsqlStatement.FetchStmt) stmt;
            executeFetch(fs, scope);
        } else if (stmt instanceof PlpgsqlStatement.CloseCursorStmt) {
            PlpgsqlStatement.CloseCursorStmt cc = (PlpgsqlStatement.CloseCursorStmt) stmt;
            executeCloseCursor(cc, scope);
        } else if (stmt instanceof PlpgsqlStatement.CommitStmt) {
            executeCommit((PlpgsqlStatement.CommitStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.RollbackStmt) {
            executeRollback((PlpgsqlStatement.RollbackStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.AbortStmt) {
            // ABORT is an unsupported transaction command in PL/pgSQL (PG raises 0A000)
            throw new MemgresException("unsupported transaction command in PL/pgSQL", "0A000");
        } else if (stmt instanceof PlpgsqlStatement.SavepointStmt) {
            throw new MemgresException("unsupported transaction command in PL/pgSQL", "0A000");
        }
    }

    // ---- Procedure transaction control (PG 11+) ----

    private void executeCommit(PlpgsqlStatement.CommitStmt stmt) {
        validateTransactionControl("COMMIT");
        if (session != null) {
            String savedIsolation = null;
            String savedReadOnly = null;
            if (stmt.chain) {
                savedIsolation = session.getGucSettings().get("transaction_isolation");
                savedReadOnly = session.getGucSettings().get("transaction_read_only");
            }
            session.commit();
            // Start a new implicit transaction for remaining procedure statements
            session.begin();
            if (stmt.chain && savedIsolation != null) {
                session.getGucSettings().set("transaction_isolation", savedIsolation);
            }
            if (stmt.chain && savedReadOnly != null) {
                session.getGucSettings().set("transaction_read_only", savedReadOnly);
            }
        }
    }

    private void executeRollback(PlpgsqlStatement.RollbackStmt stmt) {
        validateTransactionControl("ROLLBACK");
        if (session != null) {
            String savedIsolation = null;
            String savedReadOnly = null;
            if (stmt.chain) {
                savedIsolation = session.getGucSettings().get("transaction_isolation");
                savedReadOnly = session.getGucSettings().get("transaction_read_only");
            }
            session.rollback();
            // Start a new implicit transaction for remaining procedure statements
            session.begin();
            if (stmt.chain && savedIsolation != null) {
                session.getGucSettings().set("transaction_isolation", savedIsolation);
            }
            if (stmt.chain && savedReadOnly != null) {
                session.getGucSettings().set("transaction_read_only", savedReadOnly);
            }
        }
    }

    private void validateTransactionControl(String command) {
        if (!isProcedureExecution) {
            throw new MemgresException("invalid transaction termination", "2D000");
        }
        // When called from within a function context (even indirectly), transaction control is forbidden
        if (session != null && session.isInFunctionContext()) {
            throw new MemgresException("invalid transaction termination", "2D000");
        }
        // When inside an explicit transaction block (user-issued BEGIN), procedure COMMIT/ROLLBACK is forbidden
        if (session != null && session.isExplicitTransactionBlock()) {
            throw new MemgresException("invalid transaction termination", "2D000");
        }
        // COMMIT is forbidden inside exception blocks (subtransactions).
        // ROLLBACK is allowed — PG permits ROLLBACK in exception handlers to roll back
        // the main transaction, after which the exception handler continues in a new transaction.
        if (exceptionBlockDepth > 0 && command.equalsIgnoreCase("COMMIT")) {
            throw new MemgresException(
                    "cannot commit while a subtransaction is active", "2D000");
        }
    }

    // ---- Control flow ----

    private void executeIf(PlpgsqlStatement.IfStmt stmt, Scope scope) {
        if (isTruthy(evalExpr(stmt.condition(), scope))) {
            executeStatements(stmt.thenBody(), scope);
            return;
        }
        for (PlpgsqlStatement.ElsifClause elsif : stmt.elsifClauses()) {
            if (isTruthy(evalExpr(elsif.condition(), scope))) {
                executeStatements(elsif.body(), scope);
                return;
            }
        }
        if (!stmt.elseBody().isEmpty()) {
            executeStatements(stmt.elseBody(), scope);
        }
    }

    private void executeCase(PlpgsqlStatement.CaseStmt stmt, Scope scope) {
        if (stmt.searchExpr() != null) {
            // Simple CASE: evaluate search expression once, compare with each WHEN value
            Object searchVal = evalExpr(stmt.searchExpr(), scope);
            // PG uses = comparison, so NULL never matches anything (NULL = NULL is NULL, not true)
            if (searchVal != null) {
                for (PlpgsqlStatement.CaseWhenClause when : stmt.whenClauses()) {
                    // WHEN may have comma-separated values (e.g. WHEN 1, 2, 3 THEN)
                    String whenExprStr = when.whenExpr().trim();
                    String[] parts = splitCommaValues(whenExprStr);
                    for (String part : parts) {
                        Object whenVal = evalExpr(part.trim(), scope);
                        if (whenVal != null && (searchVal.equals(whenVal)
                                || String.valueOf(searchVal).equals(String.valueOf(whenVal)))) {
                            executeStatements(when.body(), scope);
                            return;
                        }
                    }
                }
            }
        } else {
            // Searched CASE: each WHEN is a boolean expression
            for (PlpgsqlStatement.CaseWhenClause when : stmt.whenClauses()) {
                if (isTruthy(evalExpr(when.whenExpr(), scope))) {
                    executeStatements(when.body(), scope);
                    return;
                }
            }
        }
        // No WHEN matched
        if (!stmt.elseBody().isEmpty()) {
            executeStatements(stmt.elseBody(), scope);
        } else {
            throw new MemgresException("case not found", "20000");
        }
    }

    private String[] splitCommaValues(String expr) {
        // Split on commas that are not inside parentheses or quotes
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inQuote = false;
        int start = 0;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '\'' && !inQuote) inQuote = true;
            else if (c == '\'' && inQuote) inQuote = false;
            else if (!inQuote) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (c == ',' && depth == 0) {
                    parts.add(expr.substring(start, i));
                    start = i + 1;
                }
            }
        }
        parts.add(expr.substring(start));
        return parts.toArray(new String[0]);
    }

    private void executeLoop(PlpgsqlStatement.LoopStmt stmt, Scope scope) {
        while (true) {
            try {
                executeStatements(stmt.body(), scope);
            } catch (ExitSignal e) {
                if (e.label == null || e.label.equalsIgnoreCase(stmt.label())) break;
                throw e;
            } catch (ContinueSignal c) {
                if (c.label == null || c.label.equalsIgnoreCase(stmt.label())) continue;
                throw c;
            }
        }
    }

    private void executeWhile(PlpgsqlStatement.WhileStmt stmt, Scope scope) {
        while (isTruthy(evalExpr(stmt.condition(), scope))) {
            try {
                executeStatements(stmt.body(), scope);
            } catch (ExitSignal e) {
                if (e.label == null || e.label.equalsIgnoreCase(stmt.label())) break;
                throw e;
            } catch (ContinueSignal c) {
                if (c.label == null || c.label.equalsIgnoreCase(stmt.label())) continue;
                throw c;
            }
        }
    }

    private void executeFor(PlpgsqlStatement.ForStmt stmt, Scope scope) {
        int lower = toInt(evalExpr(stmt.lower(), scope));
        int upper = toInt(evalExpr(stmt.upper(), scope));
        int step = stmt.step() != null ? toInt(evalExpr(stmt.step(), scope)) : 1;
        // PG requires a strictly positive BY value, for both forward and REVERSE loops
        // (REVERSE negates the step internally)
        if (step <= 0) {
            throw new MemgresException("BY value of FOR loop must be greater than zero", "22023");
        }
        if (stmt.reverse()) step = -step;

        // The loop variable lives in an implicit inner block: it must not clobber a
        // same-named variable in the enclosing scope, and it vanishes after the loop (PG semantics)
        Scope loopScope = new Scope(scope);
        loopScope.declare(stmt.varName(), null);
        boolean anyIteration = false;

        for (int i = lower; stmt.reverse() ? i >= upper : i <= upper; i += step) {
            anyIteration = true;
            loopScope.declare(stmt.varName(), i);
            try {
                executeStatements(stmt.body(), loopScope);
            } catch (ExitSignal e) {
                if (e.label == null || e.label.equalsIgnoreCase(stmt.label())) break;
                throw e;
            } catch (ContinueSignal c) {
                if (c.label == null || c.label.equalsIgnoreCase(stmt.label())) continue;
                throw c;
            }
        }
        scope.set("found", anyIteration);
    }

    private void executeForQuery(PlpgsqlStatement.ForQueryStmt stmt, Scope scope) {
        String sql = substituteVariables(stmt.sql(), scope);
        QueryResult result = astExecutor.execute(sql);

        List<String> varNames = stmt.varNames();
        boolean multiVar = varNames.size() > 1;
        // PG semantics: a record target is implicitly declared in an inner block (it must not
        // clobber a same-named outer variable and vanishes after the loop), while a
        // comma-separated scalar list targets previously declared variables (values persist).
        Scope loopScope = new Scope(scope);
        if (multiVar) {
            for (String vn : varNames) {
                if (!scope.has(vn)) scope.declare(vn, null);
            }
        } else {
            loopScope.declare(varNames.get(0), null);
        }
        boolean anyIteration = false;

        for (Object[] row : result.getRows()) {
            anyIteration = true;
            if (multiVar) {
                // Destructure columns into individual scalar variables
                for (int i = 0; i < varNames.size(); i++) {
                    scope.set(varNames.get(i), i < row.length ? row[i] : null);
                }
            } else {
                // Single variable — always create a record (Map) so that field access (r.col) works
                Map<String, Object> record = new LinkedHashMap<>();
                for (int i = 0; i < result.getColumns().size(); i++) {
                    record.put(result.getColumns().get(i).getName().toLowerCase(), row[i]);
                }
                loopScope.declare(varNames.get(0), record);
            }
            try {
                executeStatements(stmt.body(), loopScope);
            } catch (ExitSignal e) {
                if (e.label == null || e.label.equalsIgnoreCase(stmt.label())) break;
                throw e;
            } catch (ContinueSignal c) {
                if (c.label == null || c.label.equalsIgnoreCase(stmt.label())) continue;
                throw c;
            }
        }
        scope.set("found", anyIteration);
    }

    private void executeForExecute(PlpgsqlStatement.ForExecuteStmt stmt, Scope scope) {
        // Evaluate the SQL expression (which may be a string literal or expression)
        Object sqlObj = evalExpr(stmt.sqlExpr(), scope);
        if (sqlObj == null) {
            throw new MemgresException("query string argument of EXECUTE is null", "22004");
        }
        String sql = String.valueOf(sqlObj);
        // Splice USING parameters ($1, $2, ...) into the SQL string
        sql = substituteUsingParams(sql, stmt.usingExprs(), scope);

        QueryResult result = astExecutor.execute(sql);

        List<String> varNames = stmt.varNames();
        boolean multiVar = varNames.size() > 1;
        // PG semantics: a record target is implicitly declared in an inner block (it must not
        // clobber a same-named outer variable and vanishes after the loop), while a
        // comma-separated scalar list targets previously declared variables (values persist).
        Scope loopScope = new Scope(scope);
        if (multiVar) {
            for (String vn : varNames) {
                if (!scope.has(vn)) scope.declare(vn, null);
            }
        } else {
            loopScope.declare(varNames.get(0), null);
        }
        boolean anyIteration = false;

        for (Object[] row : result.getRows()) {
            anyIteration = true;
            if (multiVar) {
                for (int i = 0; i < varNames.size(); i++) {
                    scope.set(varNames.get(i), i < row.length ? row[i] : null);
                }
            } else {
                Map<String, Object> record = new LinkedHashMap<>();
                for (int i = 0; i < result.getColumns().size(); i++) {
                    record.put(result.getColumns().get(i).getName().toLowerCase(), row[i]);
                }
                loopScope.declare(varNames.get(0), record);
            }
            try {
                executeStatements(stmt.body(), loopScope);
            } catch (ExitSignal e) {
                if (e.label == null || e.label.equalsIgnoreCase(stmt.label())) break;
                throw e;
            } catch (ContinueSignal c) {
                if (c.label == null || c.label.equalsIgnoreCase(stmt.label())) continue;
                throw c;
            }
        }
        scope.set("found", anyIteration);
    }

    private void executeForeach(PlpgsqlStatement.ForeachStmt stmt, Scope scope) {
        Object arrayObj = evalExpr(stmt.arrayExpr(), scope);
        if (arrayObj == null) return;
        List<?> list = arrayObj instanceof List ? (List<?>) arrayObj
                : arrayObj instanceof Object[] ? Arrays.asList((Object[]) arrayObj) : Cols.listOf();

        int sliceDepth = stmt.sliceDepth();

        // When sliceDepth > 0, iterate over sub-arrays at the given depth.
        // For example, SLICE 1 on a 2D array [[1,2],[3,4],[5,6]] yields [1,2], [3,4], [5,6].
        List<?> slices = sliceDepth > 0 ? sliceArray(list, sliceDepth) : list;

        // FOREACH targets an existing declared variable (it keeps its last value after the loop)
        if (!scope.has(stmt.varName())) {
            scope.declare(stmt.varName(), null);
        }
        boolean anyIteration = false;
        for (Object element : slices) {
            anyIteration = true;
            scope.set(stmt.varName(), element);
            try {
                executeStatements(stmt.body(), scope);
            } catch (ExitSignal e) {
                if (e.label == null || e.label.equalsIgnoreCase(stmt.label())) break;
                throw e;
            } catch (ContinueSignal c) {
                if (c.label == null || c.label.equalsIgnoreCase(stmt.label())) continue;
                throw c;
            }
        }
        scope.set("found", anyIteration);
    }

    /**
     * Slices a nested list structure at the given depth.
     * For depth 1, a 2D list [[1,2],[3,4]] returns [[1,2],[3,4]] (each top-level element is a slice).
     * For depth 2, a 3D list [[[1,2],[3,4]],[[5,6],[7,8]]] returns [[[1,2],[3,4]],[[5,6],[7,8]]].
     * Essentially, we iterate at (ndim - sliceDepth) levels deep and collect sub-arrays of sliceDepth dimensions.
     */
    @SuppressWarnings("unchecked")
    private List<?> sliceArray(List<?> list, int sliceDepth) {
        // Determine nesting depth of the array
        int ndim = arrayDepth(list);
        if (sliceDepth >= ndim) {
            // SLICE depth equals or exceeds array dimensions: return the whole array as a single slice
            return Cols.listOf(list);
        }
        // We need to descend (ndim - sliceDepth - 1) levels, then collect elements at that level.
        // For SLICE 1 on a 2D array (ndim=2): descend 0 levels, collect top-level elements (each is a 1D sub-array).
        int levelsToDescend = ndim - sliceDepth - 1;
        List<Object> result = new ArrayList<>();
        collectSlices(list, levelsToDescend, result);
        return result;
    }

    private void collectSlices(List<?> list, int levelsToDescend, List<Object> result) {
        if (levelsToDescend <= 0) {
            // Each element at this level is a slice
            for (Object element : list) {
                result.add(element);
            }
        } else {
            for (Object element : list) {
                if (element instanceof List) {
                    collectSlices((List<?>) element, levelsToDescend - 1, result);
                } else {
                    result.add(element);
                }
            }
        }
    }

    private int arrayDepth(Object obj) {
        if (obj instanceof List && !((List<?>) obj).isEmpty()) {
            return 1 + arrayDepth(((List<?>) obj).get(0));
        }
        return 0;
    }

    private void executeExit(PlpgsqlStatement.ExitStmt stmt, Scope scope) {
        if (stmt.whenCondition() != null && !isTruthy(evalExpr(stmt.whenCondition(), scope))) return;
        throw new ExitSignal(stmt.label());
    }

    private void executeContinue(PlpgsqlStatement.ContinueStmt stmt, Scope scope) {
        if (stmt.whenCondition() != null && !isTruthy(evalExpr(stmt.whenCondition(), scope))) return;
        throw new ContinueSignal(stmt.label());
    }

    // ---- RETURN ----

    private void executeReturn(PlpgsqlStatement.ReturnStmt stmt, Scope scope) {
        if (stmt.valueExpr() == null) throw new ReturnSignal(null);
        // Special cases: RETURN NEW / RETURN OLD
        String trimmed = stmt.valueExpr().trim();
        if (trimmed.equalsIgnoreCase("NEW")) throw new ReturnSignal(scope.get("new"));
        if (trimmed.equalsIgnoreCase("OLD")) throw new ReturnSignal(scope.get("old"));
        if (trimmed.equalsIgnoreCase("NULL")) throw new ReturnSignal(null);
        throw new ReturnSignal(evalExpr(stmt.valueExpr(), scope));
    }

    @SuppressWarnings("unchecked")
    private void executeReturnNext(PlpgsqlStatement.ReturnNextStmt stmt, Scope scope) {
        String expr = stmt.valueExpr();
        Object value;
        if (expr == null || expr.trim().isEmpty()) {
            // RETURN NEXT with no expression: collect OUT parameter values as a row
            List<String> outNames = (List<String>) scope.get("__return_next_out_params__");
            if (outNames != null && !outNames.isEmpty()) {
                Object[] row = new Object[outNames.size()];
                for (int i = 0; i < outNames.size(); i++) {
                    row[i] = scope.get(outNames.get(i));
                }
                value = row;
            } else {
                value = null;
            }
        } else {
            value = evalExpr(expr, scope);
        }
        List<Object> results = (List<Object>) scope.get("__return_next_results__");
        if (results != null) {
            // Deep-copy Maps (composite records) to avoid mutation by subsequent assignments
            if (value instanceof Map) {
                value = new java.util.LinkedHashMap<>((Map<?, ?>) value);
            } else if (value instanceof Object[]) {
                value = ((Object[]) value).clone();
            }
            results.add(value);
        }
    }

    @SuppressWarnings("unchecked")
    private void executeReturnQuery(PlpgsqlStatement.ReturnQueryStmt stmt, Scope scope) {
        String sql = substituteVariables(stmt.sql(), scope);
        QueryResult result = astExecutor.execute(sql);
        // PG sets FOUND after RETURN QUERY based on whether the query produced rows
        scope.set("found", !result.getRows().isEmpty());
        List<Object> results = (List<Object>) scope.get("__return_next_results__");
        if (results != null) {
            for (Object[] row : result.getRows()) {
                results.add(row.length == 1 ? row[0] : row);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void executeReturnQueryExecute(PlpgsqlStatement.ReturnQueryExecuteStmt stmt, Scope scope) {
        Object sqlVal = evalExpr(stmt.sqlExpr(), scope);
        if (sqlVal == null) {
            throw new MemgresException("query string argument of EXECUTE is null", "22004");
        }
        String sql = String.valueOf(sqlVal);
        // Splice USING parameters ($1, $2, ...) into the SQL string
        sql = substituteUsingParams(sql, stmt.usingExprs(), scope);
        QueryResult result = astExecutor.execute(sql);
        // PG sets FOUND after RETURN QUERY EXECUTE based on whether the query produced rows
        scope.set("found", !result.getRows().isEmpty());
        List<Object> results = (List<Object>) scope.get("__return_next_results__");
        if (results != null) {
            for (Object[] row : result.getRows()) {
                results.add(row.length == 1 ? row[0] : row);
            }
        }
    }

    // ---- ASSERT ----

    private void executeAssert(PlpgsqlStatement.AssertStmt stmt, Scope scope) {
        // Check plpgsql.check_asserts GUC
        if (session != null) {
            String checkAsserts = session.getGucSettings().get("plpgsql.check_asserts");
            if (checkAsserts != null && (checkAsserts.equalsIgnoreCase("off") || checkAsserts.equalsIgnoreCase("false"))) {
                return; // assertions disabled
            }
        }
        Object condVal = evalExpr(stmt.condition(), scope);
        boolean passed = false;
        if (condVal instanceof Boolean) {
            passed = (Boolean) condVal;
        } else if (condVal != null) {
            passed = true; // non-null, non-false is truthy
        }
        if (!passed) {
            String message = "assertion failed";
            if (stmt.message() != null) {
                Object msgVal = evalExpr(stmt.message(), scope);
                message = msgVal != null ? msgVal.toString() : "assertion failed";
            }
            throw new MemgresException(message, "P0004");
        }
    }

    // ---- RAISE ----

    private void executeRaise(PlpgsqlStatement.RaiseStmt stmt, Scope scope) {
        // Bare RAISE: re-raise the current exception
        if (stmt.format() == null && stmt.messageExpr() == null && stmt.argExprs().isEmpty()
                && stmt.errcode() == null && stmt.level().equals("EXCEPTION")) {
            String sqlState = scope.has("sqlstate") ? String.valueOf(scope.get("sqlstate")) : "P0001";
            String msg = scope.has("sqlerrm") ? String.valueOf(scope.get("sqlerrm")) : "PL/pgSQL exception";
            MemgresException ex = new MemgresException(msg, sqlState);
            if (scope.has("__pg_detail")) ex.setDetail(String.valueOf(scope.get("__pg_detail")));
            if (scope.has("__pg_hint")) ex.setHint(String.valueOf(scope.get("__pg_hint")));
            if (scope.has("__pg_column")) ex.setColumn(String.valueOf(scope.get("__pg_column")));
            if (scope.has("__pg_constraint")) ex.setConstraint(String.valueOf(scope.get("__pg_constraint")));
            if (scope.has("__pg_datatype")) ex.setDatatype(String.valueOf(scope.get("__pg_datatype")));
            if (scope.has("__pg_table")) ex.setTable(String.valueOf(scope.get("__pg_table")));
            if (scope.has("__pg_schema")) ex.setSchema(String.valueOf(scope.get("__pg_schema")));
            throw ex;
        }

        // Every USING option value is an expression, so it is evaluated here rather than being
        // taken from the parse as a literal.
        String message = stmt.messageExpr() != null
                ? asText(evalExpr(stmt.messageExpr(), scope))
                : formatRaiseMessage(stmt.format(), stmt.argExprs(), scope);
        String hint = asText(evalExpr(stmt.hint(), scope));
        String detail = asText(evalExpr(stmt.detail(), scope));
        String errcode = asText(evalExpr(stmt.errcode(), scope));
        String column = asText(evalExpr(stmt.column(), scope));
        String constraint = asText(evalExpr(stmt.constraint(), scope));
        String datatype = asText(evalExpr(stmt.datatype(), scope));
        String table = asText(evalExpr(stmt.table(), scope));
        String schema = asText(evalExpr(stmt.schema(), scope));
        switch (stmt.level()) {
            case "NOTICE":
            case "WARNING":
            case "INFO":
            case "LOG":
                LOG.info("PL/pgSQL {}: {}", stmt.level(), message);
                if (session != null) {
                    // PG default: WARNING→01000, NOTICE/INFO/LOG→00000
                    String sqlState = errcode != null ? conditionToSqlState(errcode)
                            : ("WARNING".equals(stmt.level()) ? "01000" : "00000");
                    session.addNotice(stmt.level(), sqlState, message, hint);
                }
                break;
            case "DEBUG":
                LOG.debug("PL/pgSQL {}: {}", stmt.level(), message);
                if (session != null) {
                    String sqlState = errcode != null ? conditionToSqlState(errcode) : "00000";
                    session.addNotice(stmt.level(), sqlState, message, hint);
                }
                break;
            case "EXCEPTION": {
                String sqlState = "P0001";
                if (errcode != null) sqlState = conditionToSqlState(errcode);
                MemgresException ex = new MemgresException(message != null ? message : "PL/pgSQL exception", sqlState);
                if (detail != null) ex.setDetail(detail);
                if (hint != null) ex.setHint(hint);
                if (column != null) ex.setColumn(column);
                if (constraint != null) ex.setConstraint(constraint);
                if (datatype != null) ex.setDatatype(datatype);
                if (table != null) ex.setTable(table);
                if (schema != null) ex.setSchema(schema);
                // Record context at throw time for PG_EXCEPTION_CONTEXT
                if (currentFunctionName != null) {
                    ex.setPgContext("PL/pgSQL function " + currentFunctionName + "() line 1 at RAISE");
                }
                throw ex;
            }
            default: {
                // Named condition: RAISE division_by_zero, RAISE unique_violation, etc.
                String condState = conditionToSqlState(stmt.level().toLowerCase());
                if (!condState.equals("P0001") || stmt.level().contains("_")) {
                    String msg = message != null ? message : stmt.level().replace("_", " ");
                    if (errcode != null) condState = conditionToSqlState(errcode);
                    MemgresException ex = new MemgresException(msg, condState);
                    if (detail != null) ex.setDetail(detail);
                    if (hint != null) ex.setHint(hint);
                    throw ex;
                }
                break;
            }
        }
    }

    private String asText(Object val) {
        return val == null ? null : String.valueOf(val);
    }

    private String formatRaiseMessage(String format, List<String> argExprs, Scope scope) {
        if (format == null) return null;
        // Count placeholders in the format string (% except %%)
        int placeholderCount = 0;
        for (int i = 0; i < format.length(); i++) {
            if (format.charAt(i) == '%') {
                if (i + 1 < format.length() && format.charAt(i + 1) == '%') {
                    i++; // skip escaped %%
                } else {
                    placeholderCount++;
                }
            }
        }
        if (argExprs.size() > placeholderCount) {
            throw new MemgresException("too many parameters specified for RAISE", "42601");
        }
        StringBuilder sb = new StringBuilder();
        int argIdx = 0;
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == '%') {
                if (i + 1 < format.length() && format.charAt(i + 1) == '%') {
                    sb.append('%');
                    i++; // skip second %
                } else if (argIdx < argExprs.size()) {
                    Object val = evalExpr(argExprs.get(argIdx), scope);
                    sb.append(val != null ? val.toString() : "NULL");
                    argIdx++;
                } else {
                    sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ---- PERFORM ----

    private void executePerform(PlpgsqlStatement.PerformStmt stmt, Scope scope) {
        String sql = substituteVariables(stmt.sql(), scope);
        QueryResult result = astExecutor.execute(sql);
        scope.set("found", !result.getRows().isEmpty());
    }

    // ---- EXECUTE dynamic SQL ----

    private void executeExecute(PlpgsqlStatement.ExecuteStmt stmt, Scope scope) {
        Object sqlVal = evalExpr(stmt.sqlExpr(), scope);
        if (sqlVal == null) {
            throw new MemgresException("query string argument of EXECUTE is null", "22004");
        }
        String sql = String.valueOf(sqlVal);
        // Splice USING parameters ($1, $2, ...) into the SQL string
        sql = substituteUsingParams(sql, stmt.usingExprs(), scope);
        boolean prev = astExecutor.isStrictColumnRefs();
        astExecutor.setStrictColumnRefs(true);
        try {
            QueryResult result = astExecutor.execute(sql);
            scope.lastRowCount = result.getAffectedRows();

            // Note: PG's EXECUTE never changes FOUND (it does update GET DIAGNOSTICS ROW_COUNT)
            if (stmt.intoVars() != null) {
                if (!result.getRows().isEmpty()) {
                    setFromRow(scope, stmt.intoVars(), result);
                } else {
                    // Non-STRICT EXECUTE ... INTO with no rows sets all targets to NULL (PG semantics)
                    for (String varName : stmt.intoVars()) {
                        scope.set(varName, null);
                    }
                }
            }
        } finally {
            astExecutor.setStrictColumnRefs(prev);
        }
    }

    /**
     * Evaluate USING parameter expressions and splice them into a dynamic SQL string in place
     * of $1..$n placeholders. Shared by EXECUTE, FOR ... IN EXECUTE and RETURN QUERY EXECUTE.
     *
     * <p>Values are rendered as SQL literals (quoted/escaped/typed via {@link #appendValue}).
     * Substitution happens in a single pass over the placeholders so that $1 can never corrupt
     * $10 and spliced values are never re-scanned for further placeholders.
     */
    private String substituteUsingParams(String sql, List<String> usingExprs, Scope scope) {
        if (usingExprs == null || usingExprs.isEmpty()) return sql;
        // Evaluate all parameter expressions first, in order (PG evaluates USING exprs once)
        List<String> literals = new ArrayList<>();
        for (String expr : usingExprs) {
            Object paramVal = evalExpr(expr, scope);
            StringBuilder sb = new StringBuilder();
            appendValue(sb, paramVal);
            literals.add(sb.toString().trim());
        }
        // Replace $N placeholders but skip those inside string literals and dollar-quoted strings.
        // We lex through the SQL character by character, tracking quoting state.
        StringBuilder out = new StringBuilder(sql.length());
        int len = sql.length();
        for (int i = 0; i < len; ) {
            char c = sql.charAt(i);
            // Single-quoted string literal
            if (c == '\'') {
                int end = i + 1;
                while (end < len) {
                    if (sql.charAt(end) == '\'') {
                        if (end + 1 < len && sql.charAt(end + 1) == '\'') {
                            end += 2; // escaped quote
                        } else {
                            end++;
                            break;
                        }
                    } else {
                        end++;
                    }
                }
                out.append(sql, i, end);
                i = end;
                continue;
            }
            // Dollar-quoted string
            if (c == '$') {
                // Check for dollar-quote tag: $tag$ or $$
                int tagStart = i;
                int tagEnd = i + 1;
                while (tagEnd < len && (Character.isLetterOrDigit(sql.charAt(tagEnd)) || sql.charAt(tagEnd) == '_')) {
                    tagEnd++;
                }
                if (tagEnd < len && sql.charAt(tagEnd) == '$') {
                    String tag = sql.substring(tagStart, tagEnd + 1);
                    int bodyStart = tagEnd + 1;
                    int closeIdx = sql.indexOf(tag, bodyStart);
                    if (closeIdx >= 0) {
                        out.append(sql, i, closeIdx + tag.length());
                        i = closeIdx + tag.length();
                        continue;
                    }
                }
                // Not a dollar-quote; check for $N parameter reference
                if (i + 1 < len && Character.isDigit(sql.charAt(i + 1))) {
                    int numStart = i + 1;
                    int numEnd = numStart;
                    while (numEnd < len && Character.isDigit(sql.charAt(numEnd))) numEnd++;
                    int idx = Integer.parseInt(sql.substring(numStart, numEnd));
                    if (idx >= 1 && idx <= literals.size()) {
                        out.append(literals.get(idx - 1));
                    } else {
                        out.append(sql, i, numEnd);
                    }
                    i = numEnd;
                    continue;
                }
                out.append(c);
                i++;
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    // ---- SQL statement execution ----

    private void executeSql(PlpgsqlStatement.SqlStmt stmt, Scope scope) {
        String originalSql = stmt.sql().trim();
        String sql = substituteVariables(originalSql, scope);

        // For CALL statements, detect OUT params and bind results back to PL/pgSQL variables
        if (originalSql.toUpperCase().startsWith("CALL ")) {
            executeCallInPlpgsql(originalSql, sql, scope);
            return;
        }

        QueryResult result = astExecutor.execute(sql);
        scope.lastRowCount = result.getAffectedRows();

        // PG sets FOUND after INSERT/UPDATE/DELETE/MERGE based on whether at least one row
        // was affected (rows skipped by a BEFORE trigger returning NULL do not count)
        QueryResult.Type resultType = result.getType();
        if (resultType == QueryResult.Type.INSERT || resultType == QueryResult.Type.UPDATE
                || resultType == QueryResult.Type.DELETE || resultType == QueryResult.Type.MERGE) {
            scope.set("found", result.getAffectedRows() > 0);
        }

        if (stmt.intoVars() != null) {
            int rowCount = result.getRows().size();
            if (stmt.strict()) {
                if (rowCount == 0) {
                    throw new MemgresException("query returned no rows", "P0002");
                }
                if (rowCount > 1) {
                    throw new MemgresException("query returned more than one row", "P0003");
                }
            }
            if (!result.getRows().isEmpty()) {
                setFromRow(scope, stmt.intoVars(), result);
                scope.set("found", true);
            } else {
                // Non-STRICT SELECT INTO with no rows sets all targets to NULL (PG semantics)
                for (String varName : stmt.intoVars()) {
                    scope.set(varName, null);
                }
                scope.set("found", false);
            }
        }
    }

    /**
     * Execute CALL within PL/pgSQL, binding OUT param values back to local variables.
     * In PG, CALL proc(in_val, out_var) binds the OUT result back to out_var.
     */
    private void executeCallInPlpgsql(String originalSql, String substitutedSql, Scope scope) {
        QueryResult result = astExecutor.execute(substitutedSql);
        scope.lastRowCount = result.getAffectedRows();

        // If the CALL returned a result set (OUT/INOUT params), bind them back to variables
        if (result.getType() == QueryResult.Type.SELECT && !result.getRows().isEmpty()
                && !result.getColumns().isEmpty()) {
            Object[] row = result.getRows().get(0);

            // Extract the original argument list from the CALL to find variable names
            // Parse: CALL proc_name(arg1, arg2, ...)
            int parenStart = originalSql.indexOf('(');
            int parenEnd = originalSql.lastIndexOf(')');
            if (parenStart > 0 && parenEnd > parenStart) {
                String argStr = originalSql.substring(parenStart + 1, parenEnd);
                // Split args by comma (respecting parentheses)
                List<String> argNames = splitArguments(argStr);

                // Look up the procedure to find which params are OUT/INOUT
                String procName = originalSql.substring(5, parenStart).trim().toLowerCase();
                PgFunction proc = database.getFunction(procName);
                if (proc != null) {
                    int outIdx = 0;
                    for (int i = 0; i < proc.getParams().size(); i++) {
                        PgFunction.Param p = proc.getParams().get(i);
                        String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
                        if ("OUT".equals(mode) || "INOUT".equals(mode)) {
                            if (i < argNames.size() && outIdx < row.length) {
                                String varName = argNames.get(i).trim();
                                if (scope.has(varName)) {
                                    scope.set(varName, row[outIdx]);
                                }
                            }
                            outIdx++;
                        }
                    }
                }
            }
        }
    }

    /** Split a comma-separated argument list respecting parentheses. */
    private List<String> splitArguments(String argStr) {
        List<String> args = new ArrayList<>();
        int depth = 0;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < argStr.length(); i++) {
            char c = argStr.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                args.add(current.toString().trim());
                current = new StringBuilder();
                continue;
            }
            current.append(c);
        }
        if (current.length() > 0) args.add(current.toString().trim());
        return args;
    }

    private void setFromRow(Scope scope, List<String> varNames, QueryResult result) {
        Object[] row = result.getRows().get(0);
        if (varNames.size() > 1) {
            // Multiple INTO variables: assign each column to its corresponding variable
            for (int i = 0; i < varNames.size() && i < row.length; i++) {
                scope.set(varNames.get(i), row[i]);
            }
        } else if (varNames.size() == 1) {
            String varName = varNames.get(0);
            if (row.length == 1) {
                scope.set(varName, row[0]);
            } else {
                Map<String, Object> record = new LinkedHashMap<>();
                for (int i = 0; i < result.getColumns().size(); i++) {
                    record.put(result.getColumns().get(i).getName().toLowerCase(), row[i]);
                }
                scope.set(varName, record);
            }
        }
    }

    // ---- Assignment ----

    private void executeAssignment(PlpgsqlStatement.Assignment stmt, Scope scope) {
        Object value = evalExpr(stmt.valueExpr(), scope);
        String target = stmt.target();

        int dotIdx = target.indexOf('.');
        if (dotIdx > 0) {
            String qualifier = target.substring(0, dotIdx);
            String field = target.substring(dotIdx + 1);
            if (scope.has(qualifier)) {
                Object qualObj = scope.get(qualifier);
                if (qualObj == null) {
                    // Initialize composite variable as a map on first field assignment
                    Map<String, Object> map = new java.util.LinkedHashMap<>();
                    scope.set(qualifier, map);
                    qualObj = map;
                }
                if (qualObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) qualObj;
                    assignNestedField(map, field, value);
                    return;
                }
            } else if (currentFunctionName != null && qualifier.equalsIgnoreCase(currentFunctionName)
                    && currentFunctionParams != null && currentFunctionParams.contains(field.toLowerCase())
                    && scope.has(field)) {
                // Function-name-qualified assignment reaches parameters only (PG: the block
                // labeled with the function name contains just the parameters)
                scope.set(field, value);
                return;
            }
        }
        // Check that the variable is declared; PG gives 42601 for undeclared variables
        if (!scope.has(target)) {
            throw new MemgresException("\"" + target + "\" is not a known variable", "42601");
        }
        rejectCompositeIntoScalar(scope.declaredType(target), value);
        // A row stored in a composite variable is kept by field name, so r.f reads it back
        scope.set(target, coerceParamValue(value, scope.declaredType(target)));
    }

    /**
     * Assign through subscripts, as in {@code a[2] := 99} or {@code a[1].x := 9}. PG rebuilds the
     * whole array value, extending it with NULLs when the subscript is past the end, so the
     * container is copied and written back rather than mutated in place.
     */
    private void executeSubscriptAssignment(PlpgsqlStatement.SubscriptAssignment stmt, Scope scope) {
        String base = stmt.baseName();
        if (!scope.has(base)) {
            throw new MemgresException("\"" + base + "\" is not a known variable", "42601");
        }
        List<PlpgsqlStatement.TargetStep> steps = stmt.steps();
        PlpgsqlStatement.TargetStep last = steps.get(steps.size() - 1);
        String declared = scope.declaredType(base);
        String elementType = declared != null && declared.endsWith("[]")
                ? declared.substring(0, declared.length() - 2) : null;

        Object value;
        if (!last.isField() && elementType != null) {
            // A slice takes a whole array; a single subscript takes one element. Casting here is
            // what turns '{8,9}' into an array rather than leaving it as its text.
            String castTo = last.isSlice() ? declared : elementType;
            value = evalExpr("CAST(" + stmt.valueExpr() + " AS " + castTo + ")", scope);
        } else {
            value = evalExpr(stmt.valueExpr(), scope);
        }

        Object current = scope.get(base);
        if (elementType != null && current != null && !(current instanceof List)) {
            // The variable may still hold the text of an array, which has no element to write to
            current = evalExpr("CAST(" + base + " AS " + declared + ")", scope);
        }
        scope.set(base, applyTargetSteps(current, declared, steps, 0, value, scope));
    }

    private Object applyTargetSteps(Object container, String containerType,
                                    List<PlpgsqlStatement.TargetStep> steps,
                                    int idx, Object value, Scope scope) {
        PlpgsqlStatement.TargetStep step = steps.get(idx);
        boolean last = idx == steps.size() - 1;

        if (step.isField()) {
            Map<String, Object> map = toMutableRecord(container, containerType);
            String field = step.field.toLowerCase();
            Object child = last ? value
                    : applyTargetSteps(map.get(field), fieldType(containerType, field),
                            steps, idx + 1, value, scope);
            map.put(field, child);
            return map;
        }

        String elementType = containerType != null && containerType.endsWith("[]")
                ? containerType.substring(0, containerType.length() - 2) : null;
        List<Object> list = toMutableArray(container);
        if (step.isSlice()) {
            int lower = toInt(evalExpr(step.index, scope));
            int upper = toInt(evalExpr(step.upper, scope));
            List<Object> replacement = toMutableArray(value);
            for (int i = lower; i <= upper; i++) {
                setArrayElement(list, i, i - lower < replacement.size() ? replacement.get(i - lower) : null);
            }
            return list;
        }

        int index = toInt(evalExpr(step.index, scope));
        Object existing = (index >= 1 && index <= list.size()) ? list.get(index - 1) : null;
        // A multi-dimensional array subscripts into itself, so the element keeps the array type
        String childType = last ? null
                : (steps.get(idx + 1).isField() ? elementType : containerType);
        Object child = last ? value
                : applyTargetSteps(existing, childType, steps, idx + 1, value, scope);
        setArrayElement(list, index, child);
        return list;
    }

    private String fieldType(String rowType, String field) {
        if (rowType == null) return null;
        List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields =
                database.getRowType(rowType);
        if (fields == null) return null;
        for (com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField f : fields) {
            if (f.name().equalsIgnoreCase(field)) return f.typeName();
        }
        return null;
    }

    /** Store at a 1-based subscript, padding with NULLs the way PG extends an array. */
    private void setArrayElement(List<Object> list, int index, Object value) {
        if (index < 1) {
            throw new MemgresException("array subscript out of range", "2202E");
        }
        while (list.size() < index) list.add(null);
        list.set(index - 1, value);
    }

    private List<Object> toMutableArray(Object val) {
        if (val instanceof List) return new ArrayList<Object>((List<?>) val);
        if (val == null) return new ArrayList<Object>();
        List<Object> single = new ArrayList<Object>();
        single.add(val);
        return single;
    }

    /**
     * Read a composite value as a field map so one field can be replaced. The row type supplies
     * the names, without which a stored row would lose the fields the assignment does not touch.
     */
    private Map<String, Object> toMutableRecord(Object val, String rowType) {
        if (val instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) val;
            return new LinkedHashMap<String, Object>(map);
        }
        List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields =
                rowType != null ? database.getRowType(rowType) : null;
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        List<Object> values = null;
        if (val instanceof AstExecutor.PgRow) {
            values = new ArrayList<Object>(((AstExecutor.PgRow) val).values());
        } else if (val instanceof String && fields != null) {
            AstExecutor.PgRow parsed = astExecutor.parseCompositeToRow((String) val, rowType);
            if (parsed != null) values = new ArrayList<Object>(parsed.values());
        }
        if (values == null) values = new ArrayList<Object>();
        for (int i = 0; i < Math.max(values.size(), fields != null ? fields.size() : 0); i++) {
            String name = fields != null && i < fields.size()
                    ? fields.get(i).name().toLowerCase() : ("f" + (i + 1));
            map.put(name, i < values.size() ? values.get(i) : null);
        }
        return map;
    }

    /**
     * A composite assigned to a scalar variable goes through the scalar type's input function in
     * PG, which rejects the row's text form. Silently storing the row would leave the variable
     * holding something its declared type cannot represent.
     */
    private void rejectCompositeIntoScalar(String declaredType, Object value) {
        if (declaredType == null) return;
        if (!(value instanceof Map) && !(value instanceof AstExecutor.PgRow)) return;
        if (!SCALAR_TARGET_TYPES.containsKey(declaredType)) return;
        AstExecutor.PgRow row = value instanceof AstExecutor.PgRow
                ? (AstExecutor.PgRow) value
                : AstExecutor.PgRow.fromFieldMap((Map<?, ?>) value);
        String canonical = SCALAR_TARGET_TYPES.get(declaredType);
        throw new MemgresException("invalid input syntax for type " + canonical
                + ": \"" + row.toPgText() + "\"", "22P02");
    }

    /**
     * Declared types that hold a single value and so cannot accept a whole row, mapped to the
     * spelling PostgreSQL reports them by.
     */
    private static final Map<String, String> SCALAR_TARGET_TYPES = new LinkedHashMap<>();
    static {
        SCALAR_TARGET_TYPES.put("int", "integer");
        SCALAR_TARGET_TYPES.put("integer", "integer");
        SCALAR_TARGET_TYPES.put("int4", "integer");
        SCALAR_TARGET_TYPES.put("int2", "smallint");
        SCALAR_TARGET_TYPES.put("smallint", "smallint");
        SCALAR_TARGET_TYPES.put("int8", "bigint");
        SCALAR_TARGET_TYPES.put("bigint", "bigint");
        SCALAR_TARGET_TYPES.put("numeric", "numeric");
        SCALAR_TARGET_TYPES.put("decimal", "numeric");
        SCALAR_TARGET_TYPES.put("real", "real");
        SCALAR_TARGET_TYPES.put("float4", "real");
        SCALAR_TARGET_TYPES.put("float8", "double precision");
        SCALAR_TARGET_TYPES.put("double precision", "double precision");
        SCALAR_TARGET_TYPES.put("boolean", "boolean");
        SCALAR_TARGET_TYPES.put("bool", "boolean");
        SCALAR_TARGET_TYPES.put("date", "date");
        SCALAR_TARGET_TYPES.put("time", "time");
        SCALAR_TARGET_TYPES.put("timestamp", "timestamp");
        SCALAR_TARGET_TYPES.put("timestamptz", "timestamp with time zone");
        SCALAR_TARGET_TYPES.put("uuid", "uuid");
    }

    /**
     * Assign through a dotted field path, creating intermediate composites as needed, so that
     * {@code v.y.a := 2} nests inside {@code v.y} rather than storing a field literally named
     * "y.a".
     */
    private void assignNestedField(Map<String, Object> map, String fieldPath, Object value) {
        Map<String, Object> current = map;
        String remaining = fieldPath;
        int dot;
        while ((dot = remaining.indexOf('.')) > 0) {
            String head = remaining.substring(0, dot).toLowerCase();
            remaining = remaining.substring(dot + 1);
            Object child = current.get(head);
            if (!(child instanceof Map)) {
                Map<String, Object> created = new java.util.LinkedHashMap<>();
                current.put(head, created);
                child = created;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> next = (Map<String, Object>) child;
            current = next;
        }
        current.put(remaining.toLowerCase(), value);
    }

    // ---- GET DIAGNOSTICS ----

    private void executeGetDiagnostics(PlpgsqlStatement.GetDiagnosticsStmt stmt, Scope scope) {
        for (PlpgsqlStatement.DiagItem item : stmt.items()) {
            Object value;
            String itemName = item.itemName().toUpperCase();
            if (stmt.stacked()) {
                // GET STACKED DIAGNOSTICS — retrieve exception info from handler scope
                switch (itemName) {
                    case "RETURNED_SQLSTATE":
                        value = scope.has("sqlstate") ? scope.get("sqlstate") : null;
                        break;
                    case "MESSAGE_TEXT":
                        value = scope.has("sqlerrm") ? scope.get("sqlerrm") : null;
                        break;
                    case "PG_EXCEPTION_DETAIL":
                        value = scope.has("__pg_detail") ? scope.get("__pg_detail") : "";
                        break;
                    case "PG_EXCEPTION_HINT":
                        value = scope.has("__pg_hint") ? scope.get("__pg_hint") : "";
                        break;
                    case "COLUMN_NAME":
                        value = scope.has("__pg_column") ? scope.get("__pg_column") : "";
                        break;
                    case "CONSTRAINT_NAME":
                        value = scope.has("__pg_constraint") ? scope.get("__pg_constraint") : "";
                        break;
                    case "PG_DATATYPE_NAME":
                        value = scope.has("__pg_datatype") ? scope.get("__pg_datatype") : "";
                        break;
                    case "TABLE_NAME":
                        value = scope.has("__pg_table") ? scope.get("__pg_table") : "";
                        break;
                    case "SCHEMA_NAME":
                        value = scope.has("__pg_schema") ? scope.get("__pg_schema") : "";
                        break;
                    case "PG_EXCEPTION_CONTEXT":
                    case "PG_CONTEXT":
                        value = scope.has("__pg_context") ? scope.get("__pg_context") : "PL/pgSQL function";
                        break;
                    default:
                        value = null;
                        break;
                }
            } else {
                switch (itemName) {
                    case "ROW_COUNT":
                        value = scope.lastRowCount;
                        break;
                    case "FOUND":
                        value = scope.get("found");
                        break;
                    case "RESULT_OID":
                        throw new com.memgres.engine.MemgresException(
                                "unrecognized GET DIAGNOSTICS item at or near \"RESULT_OID\"", "42601");
                    case "PG_CONTEXT":
                    case "PG_EXCEPTION_CONTEXT": {
                        // Non-stacked PG_CONTEXT: return current call stack
                        StringBuilder ctx = new StringBuilder();
                        ctx.append("PL/pgSQL function");
                        if (currentFunctionName != null) {
                            ctx.append(" ").append(currentFunctionName).append("()");
                        }
                        ctx.append(" line 1 during statement block local variable initialization");
                        value = ctx.toString();
                        break;
                    }
                    default:
                        value = null;
                        break;
                }
            }
            scope.set(item.varName(), value);
        }
    }

    // ---- Cursors ----

    private void executeOpenCursor(PlpgsqlStatement.OpenCursorStmt stmt, Scope scope) {
        String sql = stmt.sql();
        Object bound = scope.get(stmt.cursorName());
        Scope argScope = scope;
        if (sql == null) {
            if (bound instanceof BoundCursor) {
                BoundCursor cursor = (BoundCursor) bound;
                sql = cursor.sql;
                argScope = bindCursorArgs(cursor, stmt, scope);
            } else if (bound instanceof String) {
                sql = (String) bound;
            }
        } else if (!stmt.argExprs().isEmpty()) {
            throw new MemgresException(
                    "cursor \"" + stmt.cursorName() + "\" has no arguments", "42601");
        }
        if (sql != null) {
            // A refcursor variable holds its portal name; PG generates one when it has none
            String portal = bound instanceof String && !((String) bound).trim().isEmpty()
                    ? ((String) bound).trim() : stmt.cursorName();
            sql = substituteVariables(sql, argScope);
            QueryResult result = astExecutor.execute(sql);
            scope.set(stmt.cursorName(), new CursorState(result, portal));
            // Register the portal with the session so the caller can FETCH from it after the
            // function returns -- that is the whole point of returning a refcursor
            if (session != null) {
                session.addCursor(portal, new Session.CursorState(portal,
                        new java.util.ArrayList<>(result.getColumns()),
                        new java.util.ArrayList<>(result.getRows()),
                        sql, false, false, false, false));
            }
        }
    }

    /**
     * Bind the arguments of {@code OPEN c(...)} onto a scope of its own, so that the cursor's
     * parameters resolve while its query runs without leaking into the calling block.
     */
    private Scope bindCursorArgs(BoundCursor cursor, PlpgsqlStatement.OpenCursorStmt stmt, Scope scope) {
        List<String> params = cursor.params;
        if (params.isEmpty() && stmt.argExprs().isEmpty()) return scope;
        // The argument list was already checked against the declaration when the body was parsed
        Scope argScope = new Scope(scope);
        Object[] bound = new Object[params.size()];
        for (int i = 0; i < stmt.argExprs().size(); i++) {
            String name = stmt.argNames().get(i);
            int slot = i;
            if (name != null) {
                for (int p = 0; p < params.size(); p++) {
                    if (params.get(p).equalsIgnoreCase(name)) { slot = p; break; }
                }
            }
            if (slot < params.size()) {
                bound[slot] = evalExpr(stmt.argExprs().get(i), scope);
            }
        }
        for (int p = 0; p < params.size(); p++) {
            argScope.declare(params.get(p), bound[p]);
        }
        return argScope;
    }

    private void executeFetch(PlpgsqlStatement.FetchStmt stmt, Scope scope) {
        Object cursorObj = scope.get(stmt.cursorName());
        if (!(cursorObj instanceof CursorState)) {
            // An unopened cursor variable still holds no portal, which is what PG reports
            throw new MemgresException(
                    "cursor variable \"" + stmt.cursorName() + "\" is null", "22004");
        }
        CursorState cursor = (CursorState) cursorObj;
        int count = 1;
        if (stmt.countExpr() != null) {
            Object countVal = evalExpr(stmt.countExpr(), scope);
            count = countVal == null ? 1 : toInt(countVal);
        }
        Object[] row = cursor.move(stmt.direction(), count);
        if (row != null) {
            if (stmt.intoVars() != null) {
                if (stmt.intoVars().size() > 1) {
                    // Multiple INTO variables: assign each column to its corresponding variable
                    for (int i = 0; i < stmt.intoVars().size() && i < row.length; i++) {
                        scope.set(stmt.intoVars().get(i), row[i]);
                    }
                } else if (row.length == 1) {
                    scope.set(stmt.intoVars().get(0), row[0]);
                } else {
                    Map<String, Object> record = new LinkedHashMap<>();
                    for (int i = 0; i < cursor.result.getColumns().size(); i++) {
                        record.put(cursor.result.getColumns().get(i).getName().toLowerCase(), row[i]);
                    }
                    scope.set(stmt.intoVars().get(0), record);
                }
            }
            scope.set("found", true);
        } else {
            scope.set("found", false);
        }
    }

    private void executeCloseCursor(PlpgsqlStatement.CloseCursorStmt stmt, Scope scope) {
        scope.set(stmt.cursorName(), null);
    }

    /** A cursor declared with a query, before it is opened. */
    private static class BoundCursor {
        final String name;
        final String sql;
        final List<String> params;
        BoundCursor(String name, String sql, List<String> params) {
            this.name = name;
            this.sql = sql;
            this.params = params;
        }
        @Override public String toString() { return name; }
    }

    private static class CursorState {
        final QueryResult result;
        /** The portal name this cursor is registered under, which is also its text value. */
        final String portal;
        /**
         * PG's cursor position: 0 before the first row, n+1 past the last, otherwise the 1-based
         * index of the current row. Backward and absolute movement need the "before"/"after"
         * states, which a plain next-row index cannot express.
         */
        int position = 0;
        CursorState(QueryResult result, String portal) {
            this.result = result;
            this.portal = portal;
        }

        /** Reposition as the direction says and return the row landed on, or null for none. */
        Object[] move(String direction, int count) {
            int n = result.getRows().size();
            int target;
            if ("PRIOR".equals(direction)) target = position - 1;
            else if ("FIRST".equals(direction)) target = 1;
            else if ("LAST".equals(direction)) target = n;
            else if ("ABSOLUTE".equals(direction)) target = count >= 0 ? count : n + 1 + count;
            else if ("RELATIVE".equals(direction)) target = position + count;
            else if ("BACKWARD".equals(direction)) target = position - count;
            else if ("FORWARD".equals(direction)) target = position + count;
            else target = position + 1; // NEXT
            if (target < 0) target = 0;
            if (target > n) target = n + 1;
            position = target;
            return (target >= 1 && target <= n) ? result.getRows().get(target - 1) : null;
        }

        /** A refcursor renders as its portal name, so RETURN c hands back a usable name. */
        @Override public String toString() { return portal; }
    }

    // ---- Expression evaluation ----

    /**
     * Evaluate an expression string by substituting variables and using SELECT.
     */
    Object evalExpr(String exprText, Scope scope) {
        if (exprText == null || exprText.trim().isEmpty()) return null;

        String trimmed = exprText.trim();

        // Quick check for simple variable reference
        if (isSimpleIdentifier(trimmed) && scope.has(trimmed)) {
            return scope.get(trimmed);
        }

        // Check for qualified variable reference like NEW.col or rec.field
        if (trimmed.contains(".")) {
            int dot = trimmed.indexOf('.');
            String qualifier = trimmed.substring(0, dot).trim();
            String field = trimmed.substring(dot + 1).trim();
            if (isSimpleIdentifier(qualifier) && isSimpleIdentifier(field) && scope.has(qualifier)) {
                Object qualObj = scope.get(qualifier);
                if (qualObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) qualObj;
                    String lowerField = field.toLowerCase();
                    if (!map.containsKey(lowerField)) {
                        String upperQ = qualifier.toUpperCase();
                        if (upperQ.equals("NEW") || upperQ.equals("OLD")) {
                            // In PG, OLD is NULL during INSERT and NEW is NULL during DELETE.
                            // An empty map means the record is effectively NULL — return null.
                            if (map.isEmpty()) return null;
                            throw new MemgresException(
                                    "record \"" + qualifier.toLowerCase() + "\" has no field \"" + lowerField + "\"",
                                    "42703");
                        }
                    }
                    return map.get(lowerField);
                }
            }
        }

        // Substitute variables and evaluate as SQL
        String substituted = substituteVariables(trimmed, scope);
        String sql = "SELECT " + substituted;
        QueryResult result = astExecutor.execute(sql);
        if (!result.getRows().isEmpty() && result.getRows().get(0).length > 0) {
            return result.getRows().get(0)[0];
        }
        return null;
    }

    private boolean isSimpleIdentifier(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') return false;
        }
        return true;
    }

    // ---- Multi-statement SQL body splitting ----

    /** Split a SQL body (from a SQL-language function/procedure) into individual statements. */
    private List<String> splitSqlBody(String body) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c == '\'') {
                // String literal: skip to closing quote
                current.append(c);
                i++;
                while (i < body.length()) {
                    current.append(body.charAt(i));
                    if (body.charAt(i) == '\'' && (i + 1 >= body.length() || body.charAt(i + 1) != '\'')) {
                        i++;
                        break;
                    }
                    if (body.charAt(i) == '\'' && i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                        current.append(body.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    i++;
                }
            } else if (c == ';') {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) result.add(stmt);
                current.setLength(0);
                i++;
            } else {
                current.append(c);
                i++;
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) result.add(last);
        return result.isEmpty() ? Cols.listOf(body.trim()) : result;
    }

    // ---- Variable substitution in SQL ----

    String substituteVariables(String sql, Scope scope) {
        return substituteVariables(sql, scope, false);
    }

    /**
     * Substitute PL/pgSQL variables into a SQL statement.
     *
     * @param columnWins conflict-resolution mode for identifiers that match both a variable
     *        and a column of a table referenced by the statement. SQL-language functions pass
     *        {@code true}: the column silently wins. PL/pgSQL passes {@code false}: such a
     *        reference raises 42702 "column reference ... is ambiguous", which is PostgreSQL's
     *        default {@code plpgsql.variable_conflict = error} behavior.
     */
    String substituteVariables(String sql, Scope scope, boolean columnWins) {
        if (sql == null) return null;
        try {
            List<Token> tokens = new Lexer(sql).tokenize();
            StringBuilder sb = new StringBuilder();
            StatementScan scan = scanStatement(tokens);

            // Track INSERT column list context to avoid substituting column names
            // INSERT INTO tablename(col1, col2, ...) - identifiers inside are column names, not variables
            boolean inInsertColList = false;
            int insertColListDepth = 0;
            // Pre-scan to find INSERT INTO tablename( positions
            java.util.Set<Integer> insertColListRanges = new java.util.HashSet<>();
            for (int k = 0; k < tokens.size() - 3; k++) {
                Token tk = tokens.get(k);
                if (tk.type() == TokenType.KEYWORD && "INTO".equalsIgnoreCase(tk.value())
                        && k > 0 && "INSERT".equalsIgnoreCase(tokens.get(k - 1).value())) {
                    int tblIdx = k + 1;
                    if (tblIdx < tokens.size() && (tokens.get(tblIdx).type() == TokenType.IDENTIFIER
                            || tokens.get(tblIdx).type() == TokenType.KEYWORD)) {
                        int parenIdx = tblIdx + 1;
                        if (parenIdx < tokens.size() && tokens.get(parenIdx).type() == TokenType.LEFT_PAREN) {
                            // Mark all tokens inside the parens as column list
                            int depth = 1;
                            for (int j = parenIdx + 1; j < tokens.size() && depth > 0; j++) {
                                if (tokens.get(j).type() == TokenType.LEFT_PAREN) depth++;
                                else if (tokens.get(j).type() == TokenType.RIGHT_PAREN) depth--;
                                if (depth > 0) insertColListRanges.add(j);
                            }
                        }
                    }
                }
            }

            int substDepth = 0; // track paren depth during substitution
            for (int i = 0; i < tokens.size(); i++) {
                Token t = tokens.get(i);
                if (t.type() == TokenType.EOF) break;
                if (t.type() == TokenType.LEFT_PAREN) substDepth++;
                else if (t.type() == TokenType.RIGHT_PAREN) substDepth--;

                // A block label qualifies its own variables, which is the only way to reach one
                // that an inner block shadows.
                if ((t.type() == TokenType.IDENTIFIER || t.type() == TokenType.KEYWORD)
                        && i + 2 < tokens.size()
                        && tokens.get(i + 1).type() == TokenType.DOT
                        && (tokens.get(i + 2).type() == TokenType.IDENTIFIER
                            || tokens.get(i + 2).type() == TokenType.KEYWORD)
                        && !scan.tableRefNames.contains(t.value().toLowerCase())) {
                    Scope labeled = scope.labeled(t.value());
                    if (labeled != null && labeled.has(tokens.get(i + 2).value())) {
                        appendValue(sb, labeled.get(tokens.get(i + 2).value()));
                        i += 2;
                        continue;
                    }
                }

                // Handle function-name-qualified reference: funcname.x. In PG, the hidden outer
                // block labeled with the function name contains ONLY the parameters, so this
                // resolves parameters but never body-DECLAREd variables — those fall through to
                // table resolution, which fails with 42P01 (unless funcname names a real
                // table/alias in this statement).
                if ((t.type() == TokenType.IDENTIFIER || t.type() == TokenType.KEYWORD)
                        && currentFunctionName != null
                        && t.value().equalsIgnoreCase(currentFunctionName)
                        && !scope.has(t.value())
                        && i + 2 < tokens.size()
                        && tokens.get(i + 1).type() == TokenType.DOT
                        && (tokens.get(i + 2).type() == TokenType.IDENTIFIER
                            || tokens.get(i + 2).type() == TokenType.KEYWORD)
                        && !scan.tableRefNames.contains(t.value().toLowerCase())) {
                    String qualField = tokens.get(i + 2).value().toLowerCase();
                    if (currentFunctionParams != null && currentFunctionParams.contains(qualField)
                            && scope.has(qualField)) {
                        appendValue(sb, scope.get(qualField));
                        i += 2;
                        continue;
                    }
                    throw new MemgresException(
                            "missing FROM-clause entry for table \"" + t.value().toLowerCase() + "\"",
                            "42P01");
                }

                // Handle NEW.col / OLD.col / record.field
                if ((t.type() == TokenType.IDENTIFIER || t.type() == TokenType.KEYWORD)
                        && scope.has(t.value())
                        && i + 2 < tokens.size()
                        && tokens.get(i + 1).type() == TokenType.DOT) {
                    Object qualObj = scope.get(t.value().toLowerCase());
                    if (qualObj instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) qualObj;
                        String field = tokens.get(i + 2).value().toLowerCase();
                        if (!map.containsKey(field)) {
                            String qualifier = t.value().toUpperCase();
                            if (qualifier.equals("NEW") || qualifier.equals("OLD")) {
                                if (map.isEmpty()) {
                                    appendValue(sb, null);
                                    i += 2;
                                    continue;
                                }
                                throw new MemgresException(
                                        "record \"" + t.value().toLowerCase() + "\" has no field \"" + field + "\"",
                                        "42703");
                            }
                        }
                        Object val = map.get(field);
                        int consumed = 2;
                        // A nested composite is read as (v.y).a — the parentheses are required,
                        // because v.y.a alone would be taken for a schema-qualified name.
                        while (val instanceof Map
                                && i + consumed + 3 < tokens.size()
                                && tokens.get(i + consumed + 1).type() == TokenType.RIGHT_PAREN
                                && tokens.get(i + consumed + 2).type() == TokenType.DOT) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> nested = (Map<String, Object>) val;
                            String nestedField = tokens.get(i + consumed + 3).value().toLowerCase();
                            if (!nested.containsKey(nestedField)) break;
                            // Drop the opening paren already written for this group.
                            int lastParen = sb.length() - 1;
                            while (lastParen >= 0 && sb.charAt(lastParen) == ' ') lastParen--;
                            if (lastParen < 0 || sb.charAt(lastParen) != '(') break;
                            sb.setLength(lastParen);
                            val = nested.get(nestedField);
                            consumed += 3;
                        }
                        appendValue(sb, val);
                        i += consumed;
                        continue;
                    }
                }

                // Handle positional parameter: $1, $2, etc.
                if (t.type() == TokenType.PARAM && scope.has(t.value())) {
                    Object val = scope.get(t.value());
                    // Check for composite field access pattern: ($param).field
                    if (val instanceof Map && i + 2 < tokens.size()
                            && tokens.get(i + 1).type() == TokenType.RIGHT_PAREN
                            && tokens.get(i + 2).type() == TokenType.DOT
                            && i + 3 < tokens.size()) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) val;
                        String field = tokens.get(i + 3).value().toLowerCase();
                        // Remove the opening '(' we already appended to sb
                        int lastParen = sb.length() - 1;
                        while (lastParen >= 0 && sb.charAt(lastParen) == ' ') lastParen--;
                        if (lastParen >= 0 && sb.charAt(lastParen) == '(') {
                            sb.setLength(lastParen);
                        }
                        appendValue(sb, map.get(field));
                        i += 3; // skip ), ., field
                        continue;
                    }
                    appendValue(sb, val);
                    continue;
                }

                // Handle plain variable
                if ((t.type() == TokenType.IDENTIFIER || t.type() == TokenType.KEYWORD)
                        && scope.has(t.value())) {
                    boolean isPrecededByDot = i > 0 && tokens.get(i - 1).type() == TokenType.DOT;
                    boolean isInInsertColList = insertColListRanges.contains(i);
                    boolean isOutputOnly = scope.isOutputOnly(t.value());
                    // Don't substitute if preceded by dot (it's a field access like table.column)
                    // Don't substitute if inside INSERT column list (these are column names, not variables)
                    // Don't substitute table names/aliases or UPDATE SET target columns
                    // Don't substitute output-only variables (OUT params of RETURNS TABLE functions)
                    // Also don't substitute common SQL keywords that happen to match variable names
                    if (!isPrecededByDot && !isInInsertColList && !isOutputOnly
                            && !scan.protectedTokens.contains(i)
                            && isSubstitutableVariable(t.value(), scope)) {
                        // An identifier that matches both a variable and a column of a table
                        // referenced by this statement is ambiguous. PL/pgSQL (with the PG
                        // default plpgsql.variable_conflict = error) raises 42702; SQL-language
                        // functions resolve it in favor of the column.
                        String lowerName = t.value().toLowerCase();
                        // A variable conflicts with a column only when the identifier appears at the
                        // same paren depth as (or deeper than) the FROM/JOIN that introduced the column.
                        // This avoids false positives when a subquery references a table whose column
                        // name matches an outer variable used outside the subquery.
                        boolean conflictsWithColumn = false;
                        if (scan.scopeColumns.contains(lowerName)) {
                            Integer colDepth = scan.scopeColumnDepths.get(lowerName);
                            conflictsWithColumn = colDepth == null || substDepth >= colDepth;
                        }
                        if (!conflictsWithColumn && scan.returningIdx >= 0 && i > scan.returningIdx
                                    && scan.insertTargetColumns.contains(lowerName)) {
                            conflictsWithColumn = true;
                        }
                        if (conflictsWithColumn) {
                            // #variable_conflict names the winner; without it PG reports the
                            // ambiguity, and a SQL-language body always lets the column win.
                            if (!columnWins && "error".equals(variableConflict)) {
                                throw new MemgresException(
                                        "column reference \"" + lowerName + "\" is ambiguous", "42702");
                            }
                            if (columnWins || "use_column".equals(variableConflict)) {
                                appendTokenToSb(sb, t);
                                continue;
                            }
                        }
                        Object val = scope.get(t.value());
                        // Handle String[] with direct subscript: TG_ARGV[0]
                        if (val instanceof String[] && i + 3 < tokens.size()
                                && tokens.get(i + 1).type() == TokenType.LEFT_BRACKET) {
                            String[] arr = (String[]) val;
                            String idxStr = tokens.get(i + 2).value();
                            try {
                                int idx = Integer.parseInt(idxStr);
                                String elem = (idx >= 0 && idx < arr.length) ? arr[idx] : null;
                                appendValue(sb, elem);
                                i += 3; // skip [, idx, ]
                                continue;
                            } catch (NumberFormatException ignored) {}
                        }
                        if (val instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> mapVal = (Map<String, Object>) val;
                            boolean followedByDot = i + 1 < tokens.size() && tokens.get(i + 1).type() == TokenType.DOT;
                            if (followedByDot) {
                                appendTokenToSb(sb, t);
                            } else if (mapVal.size() == 1) {
                                appendValue(sb, mapVal.values().iterator().next());
                            } else {
                                // Used as a whole value. Emitting the bare name would leave an
                                // identifier that resolves to nothing, so build a row literal.
                                appendValue(sb, val);
                            }
                        } else {
                            appendValue(sb, val);
                        }
                        continue;
                    }
                }

                appendTokenToSb(sb, t);
            }
            return sb.toString().trim();
        } catch (MemgresException e) {
            throw e;
        } catch (Exception e) {
            return sql;
        }
    }

    /**
     * Result of a lightweight scan over the tokenized SQL statement, used to keep variable
     * substitution from corrupting column references.
     */
    private static class StatementScan {
        /** Token indexes that must never be substituted: table names, aliases, SET targets. */
        final java.util.Set<Integer> protectedTokens = new java.util.HashSet<>();
        /** Lower-cased column names of tables whose columns are in scope for expressions. */
        final java.util.Set<String> scopeColumns = new java.util.HashSet<>();
        /** Minimum paren depth at which each scope column was found. */
        final java.util.Map<String, Integer> scopeColumnDepths = new java.util.HashMap<>();
        /** Lower-cased column names of an INSERT's target table (in scope only after RETURNING). */
        final java.util.Set<String> insertTargetColumns = new java.util.HashSet<>();
        /** Lower-cased table names, schema names and aliases referenced by the statement. */
        final java.util.Set<String> tableRefNames = new java.util.HashSet<>();
        /** Index of the first top-level RETURNING keyword, or -1. */
        int returningIdx = -1;
    }

    /**
     * Scan a token stream to find (a) referenced tables and their columns (for detecting
     * variable/column ambiguity), (b) UPDATE SET target columns and table names/aliases
     * (which must never be substituted).
     */
    private StatementScan scanStatement(List<Token> tokens) {
        StatementScan scan = new StatementScan();

        // First meaningful word determines the statement kind
        String firstWord = null;
        for (Token t : tokens) {
            if (t.type() == TokenType.KEYWORD || t.type() == TokenType.IDENTIFIER) {
                firstWord = t.value().toUpperCase();
                break;
            }
            if (t.type() != TokenType.LEFT_PAREN) break;
        }
        boolean isUpdateStmt = "UPDATE".equals(firstWord);

        // Track, per paren level, whether the parenthesis introduces a query context
        // (subquery) or a function-call argument list. FROM inside EXTRACT(x FROM y),
        // SUBSTRING(s FROM n), TRIM(... FROM ...) must not be treated as a table clause.
        java.util.ArrayDeque<Boolean> queryCtx = new java.util.ArrayDeque<>();
        int depth = 0;
        boolean inSetList = false;
        int setDepth = 0;
        String prevWord = null;

        for (int i = 0; i < tokens.size(); i++) {
            Token t = tokens.get(i);
            TokenType tt = t.type();
            if (tt == TokenType.EOF) break;
            if (tt == TokenType.LEFT_PAREN) {
                depth++;
                // Peek at the next meaningful token to classify the paren
                boolean isQuery = false;
                for (int k = i + 1; k < tokens.size(); k++) {
                    TokenType kt = tokens.get(k).type();
                    if (kt == TokenType.LEFT_PAREN) continue;
                    if (kt == TokenType.KEYWORD || kt == TokenType.IDENTIFIER) {
                        String kw = tokens.get(k).value().toUpperCase();
                        isQuery = kw.equals("SELECT") || kw.equals("WITH") || kw.equals("VALUES")
                                || kw.equals("INSERT") || kw.equals("UPDATE") || kw.equals("DELETE");
                    }
                    break;
                }
                queryCtx.push(isQuery);
                continue;
            }
            if (tt == TokenType.RIGHT_PAREN) {
                depth--;
                if (!queryCtx.isEmpty()) queryCtx.pop();
                if (inSetList && depth < setDepth) inSetList = false;
                continue;
            }
            boolean isWord = tt == TokenType.KEYWORD || tt == TokenType.IDENTIFIER;
            if (!isWord) continue;
            String w = t.value().toUpperCase();
            boolean inQueryContext = queryCtx.isEmpty() || Boolean.TRUE.equals(queryCtx.peek());

            if (inSetList && depth == setDepth
                    && (w.equals("WHERE") || w.equals("FROM") || w.equals("RETURNING"))) {
                inSetList = false;
            }
            // UPDATE SET target: identifier directly followed by '=' inside the SET list.
            // These are always column names; never substitute a variable for them.
            if (inSetList && depth == setDepth && i + 1 < tokens.size()
                    && tokens.get(i + 1).type() == TokenType.EQUALS) {
                scan.protectedTokens.add(i);
            }

            if (w.equals("SET") && !inSetList
                    && ("UPDATE".equals(prevWord) || (isUpdateStmt && depth == 0))) {
                inSetList = true;
                setDepth = depth;
            } else if (w.equals("RETURNING") && depth == 0 && scan.returningIdx < 0) {
                scan.returningIdx = i;
            } else if (w.equals("UPDATE") && prevWord == null) {
                // Top-level UPDATE target table: its columns are in scope for SET/WHERE/RETURNING
                i = collectTableList(tokens, i + 1, scan, scan.scopeColumns, false, true);
            } else if ((w.equals("FROM") || w.equals("JOIN")) && inQueryContext) {
                // Record the depth at which FROM/JOIN tables are found, so we only flag
                // ambiguity when a variable reference is at the same paren depth.
                int sizeBefore = scan.scopeColumns.size();
                i = collectTableList(tokens, i + 1, scan, scan.scopeColumns, w.equals("FROM"), false);
                // Track depth for newly added columns
                for (String col : scan.scopeColumns) {
                    if (!scan.scopeColumnDepths.containsKey(col)) {
                        scan.scopeColumnDepths.put(col, depth);
                    }
                }
            } else if (w.equals("USING") && inQueryContext
                    && i + 1 < tokens.size() && tokens.get(i + 1).type() != TokenType.LEFT_PAREN) {
                // DELETE ... USING / MERGE ... USING table list (JOIN ... USING (cols) is excluded
                // by the paren check above)
                i = collectTableList(tokens, i + 1, scan, scan.scopeColumns, true, false);
            } else if (w.equals("INTO") && "INSERT".equals(prevWord)) {
                // INSERT target columns are only in scope for the RETURNING clause
                i = collectTableList(tokens, i + 1, scan, scan.insertTargetColumns, false, true);
            } else if (w.equals("INTO") && "MERGE".equals(prevWord)) {
                i = collectTableList(tokens, i + 1, scan, scan.scopeColumns, false, true);
            }
            prevWord = w;
        }
        return scan;
    }

    /**
     * Collect one table reference (or a comma-separated list when {@code allowList}) starting
     * at token index {@code start}. Adds the referenced tables' column names (lower-cased) to
     * {@code cols} and marks table-name/alias tokens as protected. Returns the index of the
     * last consumed token.
     */
    private int collectTableList(List<Token> tokens, int start, StatementScan scan,
                                 java.util.Set<String> cols, boolean allowList, boolean isDmlTarget) {
        int i = start;
        while (true) {
            // Skip noise words that may precede a table name
            while (i < tokens.size()
                    && (tokens.get(i).type() == TokenType.KEYWORD || tokens.get(i).type() == TokenType.IDENTIFIER)
                    && ("ONLY".equalsIgnoreCase(tokens.get(i).value())
                        || "LATERAL".equalsIgnoreCase(tokens.get(i).value()))) {
                i++;
            }
            if (i >= tokens.size()) return i - 1;
            Token t = tokens.get(i);
            if (t.type() != TokenType.IDENTIFIER && t.type() != TokenType.QUOTED_IDENTIFIER) {
                return start - 1; // subquery, VALUES, literal, etc. — not a plain table reference
            }
            // Function call like generate_series(...) — not a table. DML targets are exempt:
            // in INSERT INTO t (col, ...) the paren is the column list, not a call.
            if (!isDmlTarget && i + 1 < tokens.size() && tokens.get(i + 1).type() == TokenType.LEFT_PAREN) {
                return start - 1;
            }
            String schemaName = null;
            String tableName = t.value();
            int end = i;
            if (i + 2 < tokens.size() && tokens.get(i + 1).type() == TokenType.DOT
                    && (tokens.get(i + 2).type() == TokenType.IDENTIFIER
                        || tokens.get(i + 2).type() == TokenType.QUOTED_IDENTIFIER)) {
                schemaName = tableName;
                tableName = tokens.get(i + 2).value();
                end = i + 2;
                if (!isDmlTarget && end + 1 < tokens.size() && tokens.get(end + 1).type() == TokenType.LEFT_PAREN) {
                    return start - 1; // schema-qualified function call
                }
            }
            for (int k = i; k <= end; k++) {
                scan.protectedTokens.add(k);
            }
            if (schemaName != null) scan.tableRefNames.add(schemaName.toLowerCase());
            scan.tableRefNames.add(tableName.toLowerCase());
            Table tbl = resolveTableForScan(schemaName, tableName);
            if (tbl != null) {
                for (Column c : tbl.getColumns()) {
                    cols.add(c.getName().toLowerCase());
                }
            }
            i = end + 1;
            // Optional alias: [AS] identifier
            if (i < tokens.size() && tokens.get(i).type() == TokenType.KEYWORD
                    && "AS".equalsIgnoreCase(tokens.get(i).value())) {
                i++;
                if (i < tokens.size() && (tokens.get(i).type() == TokenType.IDENTIFIER
                        || tokens.get(i).type() == TokenType.QUOTED_IDENTIFIER)) {
                    scan.protectedTokens.add(i);
                    scan.tableRefNames.add(tokens.get(i).value().toLowerCase());
                    i++;
                }
            } else if (i < tokens.size() && tokens.get(i).type() == TokenType.IDENTIFIER) {
                scan.protectedTokens.add(i);
                scan.tableRefNames.add(tokens.get(i).value().toLowerCase());
                i++;
            }
            if (allowList && i < tokens.size() && tokens.get(i).type() == TokenType.COMMA) {
                i++;
                continue;
            }
            return i - 1;
        }
    }

    private Table resolveTableForScan(String schemaName, String tableName) {
        try {
            String name = tableName.toLowerCase();
            if (schemaName != null) {
                Schema schema = database.getSchema(schemaName.toLowerCase());
                return schema != null ? schema.getTable(name) : null;
            }
            return database.getTable(name);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private boolean isSubstitutableVariable(String name, Scope scope) {
        // Don't substitute certain SQL keywords even if they're in scope
        String upper = name.toUpperCase();
        if (upper.equals("NEW") || upper.equals("OLD")) return false;
        if (upper.equals("FOUND")) return true;
        return scope.has(name);
    }

    private void appendValue(StringBuilder sb, Object val) {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '(' && sb.charAt(sb.length() - 1) != ' ') {
            sb.append(" ");
        }
        if (val == null) {
            sb.append("NULL");
        } else if (val instanceof RawSql) {
            sb.append(((RawSql) val).sql);
        } else if (val instanceof Number || val instanceof Boolean) {
            sb.append(val);
        } else if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            // Format as bytea hex literal: '\x...'::bytea
            sb.append("'\\x");
            for (byte b : bytes) {
                sb.append(String.format("%02x", b & 0xff));
            }
            sb.append("'::bytea");
        } else if (val instanceof String[]) {
            String[] arr = (String[]) val;
            sb.append("(ARRAY[");
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(",");
                if (arr[i] == null) sb.append("NULL");
                else sb.append("'").append(arr[i].replace("'", "''")).append("'");
            }
            sb.append("])");
        } else if (val instanceof java.util.List<?>) {
            java.util.List<?> list = (java.util.List<?>) val;
            // Format as parenthesized PG array: (ARRAY['a','b','c'])
            // Parens are required so that subscript like (ARRAY[...])[1] is valid PG syntax
            // (bare ARRAY[...][1] is a syntax error in PG)
            sb.append("(ARRAY[");
            appendListElements(sb, list);
            sb.append("])");
        } else if (val instanceof PgInterval) {
            sb.append("'").append(val.toString().replace("'", "''")).append("'::interval");
        } else if (val instanceof java.time.OffsetDateTime) {
            sb.append("'").append(val.toString().replace("'", "''")).append("'::timestamptz");
        } else if (val instanceof java.time.LocalDateTime) {
            sb.append("'").append(val.toString().replace("'", "''")).append("'::timestamp");
        } else if (val instanceof java.time.LocalDate) {
            sb.append("'").append(val.toString().replace("'", "''")).append("'::date");
        } else if (val instanceof java.time.LocalTime) {
            sb.append("'").append(val.toString().replace("'", "''")).append("'::time");
        } else if (val instanceof com.memgres.engine.HstoreValue) {
            sb.append("'").append(val.toString().replace("'", "''")).append("'::hstore");
        } else if (val instanceof Map) {
            // A composite variable used as a whole value becomes a row constructor, so that
            // nested composites keep their structure instead of flattening into text.
            @SuppressWarnings("unchecked")
            Map<String, Object> mapVal = (Map<String, Object>) val;
            sb.append("ROW(");
            boolean firstField = true;
            for (Object field : mapVal.values()) {
                if (!firstField) sb.append(",");
                firstField = false;
                StringBuilder fieldSb = new StringBuilder();
                appendValue(fieldSb, field);
                sb.append(fieldSb.toString().trim());
            }
            sb.append(")");
        } else {
            sb.append("'").append(val.toString().replace("'", "''")).append("'");
        }
    }

    private void appendListElements(StringBuilder sb, java.util.List<?> list) {
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            Object elem = list.get(i);
            if (elem == null) sb.append("NULL");
            else if (elem instanceof java.util.List<?>) {
                sb.append("ARRAY[");
                appendListElements(sb, (java.util.List<?>) elem);
                sb.append("]");
            }
            else if (elem instanceof Number || elem instanceof Boolean) sb.append(elem);
            else if (elem instanceof Map) {
                // A composite element must stay a row rather than become its Java toString
                StringBuilder elemSb = new StringBuilder();
                appendValue(elemSb, elem);
                sb.append(elemSb.toString().trim());
            }
            else sb.append("'").append(elem.toString().replace("'", "''")).append("'");
        }
    }

    private void appendTokenToSb(StringBuilder sb, Token t) {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '('
                && sb.charAt(sb.length() - 1) != '.'
                && t.type() != TokenType.DOT
                && t.type() != TokenType.COMMA
                && t.type() != TokenType.RIGHT_PAREN
                && t.type() != TokenType.SEMICOLON
                && t.type() != TokenType.LEFT_BRACKET
                && t.type() != TokenType.RIGHT_BRACKET) {
            sb.append(" ");
        }
        if (t.type() == TokenType.STRING_LITERAL) {
            sb.append("'").append(t.value().replace("'", "''")).append("'");
        } else if (t.type() == TokenType.BIT_STRING_LITERAL) {
            sb.append("B'").append(t.value()).append("'");
        } else {
            sb.append(t.value());
        }
    }

    // ---- Helpers ----

    private boolean isTruthy(Object val) {
        if (val == null) return false;
        if (val instanceof Boolean) return ((Boolean) val);
        if (val instanceof Number) return ((Number) val).intValue() != 0;
        if (val instanceof String) return ((String) val).equalsIgnoreCase("true") || ((String) val).equals("t");
        return true;
    }

    private Object coerceParamValue(Object val, String typeName) {
        if (val == null || typeName == null) return val;
        String type = typeName.toLowerCase().trim();
        // Convert PgRow to a Map when the parameter type is a composite type
        if (val instanceof AstExecutor.PgRow) {
            List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields = database.getRowType(type);
            if (fields != null) {
                AstExecutor.PgRow row = (AstExecutor.PgRow) val;
                Map<String, Object> record = new LinkedHashMap<>();
                for (int i = 0; i < fields.size(); i++) {
                    record.put(fields.get(i).name().toLowerCase(),
                            i < row.values().size() ? row.values().get(i) : null);
                }
                return record;
            }
        }
        if (val instanceof String) {
            String s = (String) val;
            switch (type) {
                case "int":
                case "integer":
                case "int4": {
                    try { return Integer.parseInt(s); } catch (Exception e) { return val; } 
                }
                case "bigint":
                case "int8": {
                    try { return Long.parseLong(s); } catch (Exception e) { return val; } 
                }
                case "real":
                case "float4": {
                    try { return Float.parseFloat(s); } catch (Exception e) { return val; } 
                }
                case "double precision":
                case "float8": {
                    try { return Double.parseDouble(s); } catch (Exception e) { return val; } 
                }
                case "boolean":
                case "bool":
                    return Boolean.parseBoolean(s);
                case "numeric":
                case "decimal": {
                    try { return new java.math.BigDecimal(s); } catch (Exception e) { return val; } 
                }
                default:
                    return val;
            }
        }
        return val;
    }

    private boolean matchesCondition(List<String> conditions, String sqlState) {
        for (String cond : conditions) {
            String condLower = cond.toLowerCase().trim();
            if (condLower.equals("others")) return !"P0004".equals(sqlState);
            if (condLower.startsWith("sqlstate ")) {
                if (sqlState.equalsIgnoreCase(condLower.substring(9).trim().replace("'", ""))) return true;
                continue;
            }
            String condSqlState = conditionToSqlState(condLower);
            if (condSqlState.equals(sqlState)) return true;
            // Class-level matching: if condition SQLSTATE ends with "000", match by first 2 chars
            if (condSqlState.length() == 5 && condSqlState.substring(2).equals("000")
                    && sqlState.length() == 5 && sqlState.substring(0, 2).equals(condSqlState.substring(0, 2))) {
                return true;
            }
        }
        return false;
    }

    private String conditionToSqlState(String condition) {
        switch (condition.toLowerCase().replace("'", "").trim()) {
            case "division_by_zero":
                return "22012";
            case "unique_violation":
                return "23505";
            case "not_null_violation":
                return "23502";
            case "foreign_key_violation":
                return "23503";
            case "check_violation":
                return "23514";
            case "no_data_found":
                return "P0002";
            case "too_many_rows":
                return "P0003";
            case "raise_exception":
                return "P0001";
            case "data_exception":
                return "22000";
            case "integrity_constraint_violation":
                return "23000";
            case "undefined_table":
                return "42P01";
            case "undefined_column":
                return "42703";
            case "duplicate_table":
                return "42P07";
            case "duplicate_object":
                return "42710";
            case "duplicate_schema":
                return "42P06";
            case "invalid_text_representation":
                return "22P02";
            case "string_data_right_truncation":
                return "22001";
            case "numeric_value_out_of_range":
                return "22003";
            case "syntax_error":
                return "42601";
            case "invalid_column_reference":
                return "42P10";
            case "undefined_function":
                return "42883";
            case "wrong_object_type":
                return "42809";
            case "dependent_objects_still_exist":
                return "2BP01";
            case "serialization_failure":
                return "40001";
            case "read_only_sql_transaction":
                return "25006";
            case "assert_failure":
                return "P0004";
            case "datatype_mismatch":
                return "42804";
            case "feature_not_supported":
                return "0A000";
            case "invalid_parameter_value":
                return "22023";
            case "object_not_in_prerequisite_state":
                return "55000";
            case "insufficient_privilege":
                return "42501";
            case "deadlock_detected":
                return "40P01";
            case "lock_not_available":
                return "55P03";
            case "invalid_cursor_state":
                return "24000";
            case "invalid_transaction_state":
                return "25000";
            case "null_value_not_allowed":
                return "22004";
            case "invalid_regular_expression":
                return "2201B";
            case "datetime_field_overflow":
                return "22008";
            case "object_in_use":
                return "55006";
            case "program_limit_exceeded":
                return "54000";
            case "case_not_found":
                return "20000";
            case "with_check_option_violation":
                return "44000";
            case "triggered_action_exception":
                return "09000";
            case "plpgsql_error":
                return "P0000";
            case "invalid_escape_sequence":
                return "22025";
            case "name_too_long":
                return "42622";
            case "external_routine_exception":
                return "38000";
            case "syntax_error_or_access_rule_violation":
                return "42000";
            case "exclusion_violation":
                return "23P01";
            case "restrict_violation":
                return "23001";
            case "cardinality_violation":
                return "21000";
            case "transaction_rollback":
                return "40000";
            case "operator_intervention":
                return "57000";
            case "query_canceled":
                return "57014";
            case "connection_exception":
                return "08000";
            case "config_file_error":
                return "F0000";
            case "invalid_sql_statement_name":
                return "26000";
            default:
                return condition.length() == 5 ? condition : "P0001";
        }
    }

    private void populateDiagnosticScope(Scope scope, MemgresException e) {
        if (e.getDetail() != null) scope.declare("__pg_detail", e.getDetail());
        if (e.getHint() != null) scope.declare("__pg_hint", e.getHint());
        String column = e.getColumn();
        String constraint = e.getConstraint();
        String table = e.getTable();
        // Extract constraint/table from error message if not explicitly set
        String msg = e.getMessage();
        if (msg != null) {
            if (constraint == null) {
                java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                        "violates (?:unique |check |foreign key )?constraint \"([^\"]+)\"").matcher(msg);
                if (m.find()) constraint = m.group(1);
            }
            if (table == null) {
                java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                        "relation \"([^\"]+)\"").matcher(msg);
                if (m.find()) table = m.group(1);
            }
        }
        if (column != null) scope.declare("__pg_column", column);
        if (constraint != null) scope.declare("__pg_constraint", constraint);
        if (e.getDatatype() != null) scope.declare("__pg_datatype", e.getDatatype());
        if (table != null) scope.declare("__pg_table", table);
        if (e.getSchema() != null) scope.declare("__pg_schema", e.getSchema());
        // Build PG_EXCEPTION_CONTEXT with function name and line number
        String ctx = buildExceptionContext(e);
        if (ctx != null) scope.declare("__pg_context", ctx);
    }

    private String buildExceptionContext(MemgresException e) {
        // If the exception itself carries context from the throw site, use it
        if (e.getPgContext() != null) {
            return e.getPgContext();
        }
        StringBuilder sb = new StringBuilder();
        if (currentFunctionName != null) {
            sb.append("PL/pgSQL function ").append(currentFunctionName).append("()");
            sb.append(" line 1 at RAISE");
        } else {
            sb.append("PL/pgSQL function line 1 at RAISE");
        }
        return sb.toString();
    }

    private String mapJavaExceptionToSqlState(RuntimeException e) {
        if (e instanceof ArithmeticException) return "22012"; // division_by_zero
        if (e instanceof NumberFormatException) return "22000"; // data_exception
        return "P0001";
    }
}
