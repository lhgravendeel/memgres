package com.memgres.engine;

import com.memgres.engine.plpgsql.PlpgsqlStatement;
import com.memgres.engine.util.Cols;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The checks PostgreSQL applies to a PL/pgSQL body while compiling it, before anything runs: a
 * declaration's type has to resolve, a name may be declared once per block, a NOT NULL variable
 * needs a default, nothing may be written to a CONSTANT, EXIT and CONTINUE must name a label that
 * encloses them, an exception handler must name a condition that exists, GET DIAGNOSTICS must ask
 * for an item its form offers, and RETURN must suit the routine it sits in.
 *
 * <p>Running them at compile time is the point. A misspelled {@code %TYPE} is exactly what a
 * schema change produces, and a misspelled condition name is a handler that never fires; both
 * have to fail where the routine is created rather than surface later as a wrong value.
 */
public final class PlpgsqlBodyValidator {

    /** What the body is being compiled for, which decides which RETURN forms are legal. */
    public static final class Routine {
        private final boolean procedure;
        private final String returnType;
        private final boolean hasOutParams;

        public Routine(boolean procedure, String returnType, boolean hasOutParams) {
            this.procedure = procedure;
            this.returnType = returnType == null ? null : returnType.trim();
            this.hasOutParams = hasOutParams;
        }

        boolean setReturning() {
            return returnType != null
                    && (returnType.toUpperCase().startsWith("SETOF")
                        || returnType.equalsIgnoreCase("TABLE"));
        }

        boolean voidReturning() {
            return returnType == null || returnType.isEmpty() || returnType.equalsIgnoreCase("void");
        }
    }

    /** What a declaration said about a name, for the checks that depend on it. */
    private static final class VarInfo {
        final boolean constant;
        final boolean cursor;
        final boolean boundCursor;

        VarInfo(boolean constant, boolean cursor, boolean boundCursor) {
            this.constant = constant;
            this.cursor = cursor;
            this.boundCursor = boundCursor;
        }
    }

    /** A label EXIT or CONTINUE may name, and whether it belongs to a loop. */
    private static final class Label {
        final String name;
        final boolean loop;

        Label(String name, boolean loop) {
            this.name = name;
            this.loop = loop;
        }
    }

    private final AstExecutor executor;
    private final Routine routine;
    /** One frame per block, innermost first. */
    private final Deque<Map<String, VarInfo>> scopes = new ArrayDeque<Map<String, VarInfo>>();
    private final Deque<Label> labels = new ArrayDeque<Label>();
    private int loopDepth;
    private int handlerDepth;

    private PlpgsqlBodyValidator(AstExecutor executor, Routine routine) {
        this.executor = executor;
        this.routine = routine;
    }

    /**
     * @param paramNames names already in scope from the routine's signature; null for a DO block
     * @param routine    what the body belongs to, or null for a DO block
     */
    public static void validate(AstExecutor executor, PlpgsqlStatement.Block block,
                                Collection<String> paramNames, Routine routine) {
        if (executor == null || block == null) return;
        PlpgsqlBodyValidator validator = new PlpgsqlBodyValidator(executor, routine);
        Map<String, VarInfo> params = new LinkedHashMap<String, VarInfo>();
        if (paramNames != null) {
            for (String name : paramNames) {
                if (name != null) params.put(name.toLowerCase(), new VarInfo(false, false, false));
            }
        }
        validator.scopes.push(params);
        validator.validateBlock(block);
    }

    public static void validate(AstExecutor executor, PlpgsqlStatement.Block block,
                                Collection<String> paramNames) {
        validate(executor, block, paramNames, null);
    }

    // ---- Declarations ----

    private void validateBlock(PlpgsqlStatement.Block block) {
        Map<String, VarInfo> frame = new LinkedHashMap<String, VarInfo>();
        scopes.push(frame);
        if (block.label() != null) labels.push(new Label(block.label(), false));
        try {
            for (PlpgsqlStatement.VarDeclaration decl : block.declarations()) {
                String key = decl.name().toLowerCase();
                if (frame.containsKey(key)) {
                    throw new MemgresException(
                            "duplicate declaration at or near \"" + decl.name() + "\"", "42601");
                }
                boolean bound = decl.isCursor()
                        && decl.cursorQuery() != null && !decl.cursorQuery().isEmpty();
                if (!decl.isCursor()) {
                    validateDeclaredType(decl.typeName());
                    if (decl.notNull() && decl.defaultExpr() == null) {
                        throw new MemgresException("variable \"" + decl.name()
                                + "\" must have a default value, since it's declared NOT NULL", "22004");
                    }
                }
                boolean refcursor = decl.isCursor()
                        || (decl.typeName() != null && decl.typeName().equalsIgnoreCase("refcursor"));
                frame.put(key, new VarInfo(decl.constant(), refcursor, bound));
            }
            validateStatements(block.body());
            handlerDepth++;
            try {
                for (PlpgsqlStatement.ExceptionHandler handler : block.exceptionHandlers()) {
                    for (String condition : handler.conditionNames()) validateCondition(condition);
                    validateStatements(handler.body());
                }
            } finally {
                handlerDepth--;
            }
        } finally {
            if (block.label() != null) labels.pop();
            scopes.pop();
        }
    }

    private void validateDeclaredType(String typeName) {
        if (typeName == null) return;
        String type = typeName.trim();
        if (type.isEmpty()) return;
        String upper = type.toUpperCase();
        if (upper.endsWith("%ROWTYPE")) {
            requireRelation(type.substring(0, type.length() - "%ROWTYPE".length()));
            return;
        }
        if (upper.endsWith("%TYPE")) {
            validateColumnReference(type.substring(0, type.length() - "%TYPE".length()));
            return;
        }
        DdlObjectExecutor.validateTypeExists(executor, type);
    }

    /** {@code x%TYPE} names a variable; anything qualified names a column of a relation. */
    private void validateColumnReference(String ref) {
        String[] parts = ref.split("\\.");
        if (parts.length == 1) {
            if (!inScope(parts[0])) {
                throw new MemgresException(
                        "variable \"" + parts[0] + "\" does not exist", "42704");
            }
            return;
        }
        String column = parts[parts.length - 1];
        String relation = parts[parts.length - 2];
        // A record variable's field is written the same way and is only known at run time
        if (parts.length == 2 && inScope(relation)) return;
        String qualified = ref.substring(0, ref.length() - column.length() - 1);
        Table table = requireRelation(qualified);
        if (table != null && table.getColumnIndex(column) < 0) {
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + relation + "\" does not exist", "42703");
        }
    }

    /**
     * @return the relation, or null when the name is a composite type — which %ROWTYPE accepts
     *         but which has no column list to check against here
     */
    private Table requireRelation(String ref) {
        String[] parts = ref.split("\\.");
        String relation = parts[parts.length - 1];
        String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
        if (schema != null && executor.database.getSchema(schema) == null) {
            throw new MemgresException("schema \"" + schema + "\" does not exist", "3F000");
        }
        try {
            return executor.resolveTable(schema != null ? schema : executor.defaultSchema(), relation);
        } catch (MemgresException e) {
            // Anything but "no such relation" means the name resolved to something — a view, say,
            // which %ROWTYPE reads from even though resolveTable refuses to write to it
            if (!"42P01".equals(e.getSqlState())) return null;
            if (executor.database.isCompositeType(relation)) return null;
            throw new MemgresException("relation \"" + ref + "\" does not exist", "42P01");
        }
    }

    private boolean inScope(String name) {
        return lookup(name) != null;
    }

    private VarInfo lookup(String name) {
        String key = name.toLowerCase();
        for (Map<String, VarInfo> frame : scopes) {
            VarInfo info = frame.get(key);
            if (info != null) return info;
        }
        return null;
    }

    // ---- Writes to a CONSTANT ----

    /**
     * A write names the variable itself or reaches into it, as {@code r.f := 1} does; either way
     * the value the declaration froze would change, which is what CONSTANT forbids.
     */
    private void checkWritable(String target) {
        if (target == null) return;
        String name = target;
        int dot = name.indexOf('.');
        if (dot > 0) name = name.substring(0, dot);
        int bracket = name.indexOf('[');
        if (bracket > 0) name = name.substring(0, bracket);
        name = name.trim();
        VarInfo info = lookup(name);
        if (info != null && info.constant) {
            throw new MemgresException("variable \"" + name + "\" is declared CONSTANT", "22005");
        }
    }

    private void checkWritable(List<String> targets) {
        if (targets == null) return;
        for (String target : targets) checkWritable(target);
    }

    // ---- Exception conditions ----

    private void validateCondition(String condition) {
        String name = condition.trim();
        if (name.equalsIgnoreCase("others")) return;
        if (name.toLowerCase().startsWith("sqlstate ")) {
            String code = name.substring("sqlstate ".length()).trim();
            if (code.startsWith("'") && code.endsWith("'") && code.length() >= 2) {
                code = code.substring(1, code.length() - 1);
            }
            if (!isSqlStateCode(code)) {
                throw new MemgresException(
                        "invalid SQLSTATE code at or near \"'" + code + "'\"", "42601");
            }
            return;
        }
        if (!PlpgsqlConditionNames.isKnown(name)) {
            throw new MemgresException(
                    "unrecognized exception condition \"" + name.toLowerCase() + "\"", "42704");
        }
    }

    private static boolean isSqlStateCode(String code) {
        if (code.length() != 5) return false;
        for (int i = 0; i < 5; i++) {
            char c = code.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z'))) return false;
        }
        return true;
    }

    // ---- GET DIAGNOSTICS ----

    private static final Set<String> STACKED_ITEMS = Cols.setOf(
            "RETURNED_SQLSTATE", "COLUMN_NAME", "CONSTRAINT_NAME", "PG_DATATYPE_NAME",
            "MESSAGE_TEXT", "TABLE_NAME", "SCHEMA_NAME", "PG_EXCEPTION_DETAIL",
            "PG_EXCEPTION_HINT", "PG_EXCEPTION_CONTEXT");

    private static final Set<String> CURRENT_ITEMS = Cols.setOf(
            "ROW_COUNT", "PG_CONTEXT", "PG_ROUTINE_OID");

    private void validateDiagnostics(PlpgsqlStatement.GetDiagnosticsStmt stmt) {
        if (stmt.stacked() && handlerDepth == 0) {
            throw new MemgresException(
                    "GET STACKED DIAGNOSTICS cannot be used outside an exception handler", "0Z002");
        }
        for (PlpgsqlStatement.DiagItem item : stmt.items()) {
            checkWritable(item.varName());
            String name = item.itemName().trim();
            String upper = name.toUpperCase();
            boolean stackedItem = STACKED_ITEMS.contains(upper);
            boolean currentItem = CURRENT_ITEMS.contains(upper);
            if (!stackedItem && !currentItem) {
                throw new MemgresException("unrecognized GET DIAGNOSTICS item at or near \""
                        + name.toLowerCase() + "\"", "42601");
            }
            if (stmt.stacked() && !stackedItem) {
                throw new MemgresException("diagnostics item " + upper
                        + " is not allowed in GET STACKED DIAGNOSTICS", "42601");
            }
            if (!stmt.stacked() && !currentItem) {
                throw new MemgresException("diagnostics item " + upper
                        + " is not allowed in GET CURRENT DIAGNOSTICS", "42601");
            }
        }
    }

    // ---- EXIT / CONTINUE ----

    private void validateExit(PlpgsqlStatement.ExitStmt stmt) {
        if (stmt.label() == null) {
            if (loopDepth == 0) {
                throw new MemgresException(
                        "EXIT cannot be used outside a loop, unless it has a label", "42601");
            }
            return;
        }
        if (findLabel(stmt.label()) == null) throw noSuchLabel(stmt.label());
    }

    private void validateContinue(PlpgsqlStatement.ContinueStmt stmt) {
        if (stmt.label() == null) {
            if (loopDepth == 0) {
                throw new MemgresException("CONTINUE cannot be used outside a loop", "42601");
            }
            return;
        }
        Label label = findLabel(stmt.label());
        if (label == null) throw noSuchLabel(stmt.label());
        if (!label.loop) {
            // There is no iteration to continue, so naming a block is meaningless rather than a
            // shorthand for leaving it
            throw new MemgresException("block label \"" + label.name
                    + "\" cannot be used in CONTINUE", "42601");
        }
    }

    private Label findLabel(String name) {
        for (Label label : labels) {
            if (label.name.equalsIgnoreCase(name)) return label;
        }
        return null;
    }

    private static MemgresException noSuchLabel(String name) {
        return new MemgresException("there is no label \"" + name
                + "\" attached to any block or loop enclosing this statement", "42601");
    }

    // ---- RETURN ----

    private void validateReturn(PlpgsqlStatement.ReturnStmt stmt) {
        if (routine == null || stmt.valueExpr() == null) return;
        if (routine.procedure) {
            throw new MemgresException("RETURN cannot have a parameter in a procedure", "42601");
        }
        if (routine.setReturning()) {
            throw new MemgresException(
                    "RETURN cannot have a parameter in function returning set", "42804");
        }
        if (routine.hasOutParams) {
            throw new MemgresException(
                    "RETURN cannot have a parameter in function with OUT parameters", "42804");
        }
        if (routine.voidReturning()) {
            throw new MemgresException(
                    "RETURN cannot have a parameter in function returning void", "42804");
        }
    }

    private void validateReturnSet(String form) {
        if (routine == null || routine.setReturning()) return;
        throw new MemgresException(
                "cannot use RETURN " + form + " in a non-SETOF function", "42804");
    }

    // ---- Statement walk ----

    private void validateStatements(List<PlpgsqlStatement> stmts) {
        if (stmts == null) return;
        for (PlpgsqlStatement stmt : stmts) validateStatement(stmt);
    }

    private void validateLoop(String label, List<PlpgsqlStatement> body) {
        if (label != null) labels.push(new Label(label, true));
        loopDepth++;
        try {
            validateStatements(body);
        } finally {
            loopDepth--;
            if (label != null) labels.pop();
        }
    }

    private void validateStatement(PlpgsqlStatement stmt) {
        if (stmt instanceof PlpgsqlStatement.Block) {
            validateBlock((PlpgsqlStatement.Block) stmt);
        } else if (stmt instanceof PlpgsqlStatement.Assignment) {
            checkWritable(((PlpgsqlStatement.Assignment) stmt).target());
        } else if (stmt instanceof PlpgsqlStatement.SubscriptAssignment) {
            checkWritable(((PlpgsqlStatement.SubscriptAssignment) stmt).baseName());
        } else if (stmt instanceof PlpgsqlStatement.IfStmt) {
            PlpgsqlStatement.IfStmt s = (PlpgsqlStatement.IfStmt) stmt;
            validateStatements(s.thenBody());
            if (s.elsifClauses() != null) {
                for (PlpgsqlStatement.ElsifClause c : s.elsifClauses()) validateStatements(c.body());
            }
            validateStatements(s.elseBody());
        } else if (stmt instanceof PlpgsqlStatement.CaseStmt) {
            PlpgsqlStatement.CaseStmt s = (PlpgsqlStatement.CaseStmt) stmt;
            if (s.whenClauses() != null) {
                for (PlpgsqlStatement.CaseWhenClause c : s.whenClauses()) validateStatements(c.body());
            }
            validateStatements(s.elseBody());
        } else if (stmt instanceof PlpgsqlStatement.LoopStmt) {
            PlpgsqlStatement.LoopStmt s = (PlpgsqlStatement.LoopStmt) stmt;
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.WhileStmt) {
            PlpgsqlStatement.WhileStmt s = (PlpgsqlStatement.WhileStmt) stmt;
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForStmt) {
            // An integer FOR loop declares its own variable, so it never writes an outer one
            PlpgsqlStatement.ForStmt s = (PlpgsqlStatement.ForStmt) stmt;
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForQueryStmt) {
            PlpgsqlStatement.ForQueryStmt s = (PlpgsqlStatement.ForQueryStmt) stmt;
            validateCursorLoop(s.sql());
            checkWritable(s.varNames());
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForExecuteStmt) {
            PlpgsqlStatement.ForExecuteStmt s = (PlpgsqlStatement.ForExecuteStmt) stmt;
            checkWritable(s.varNames());
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForeachStmt) {
            PlpgsqlStatement.ForeachStmt s = (PlpgsqlStatement.ForeachStmt) stmt;
            checkWritable(s.varName());
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.ExitStmt) {
            validateExit((PlpgsqlStatement.ExitStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.ContinueStmt) {
            validateContinue((PlpgsqlStatement.ContinueStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.ReturnStmt) {
            validateReturn((PlpgsqlStatement.ReturnStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.ReturnNextStmt) {
            validateReturnSet("NEXT");
        } else if (stmt instanceof PlpgsqlStatement.ReturnQueryStmt
                || stmt instanceof PlpgsqlStatement.ReturnQueryExecuteStmt) {
            validateReturnSet("QUERY");
        } else if (stmt instanceof PlpgsqlStatement.SqlStmt) {
            checkWritable(((PlpgsqlStatement.SqlStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.ExecuteStmt) {
            checkWritable(((PlpgsqlStatement.ExecuteStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.FetchStmt) {
            checkWritable(((PlpgsqlStatement.FetchStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.GetDiagnosticsStmt) {
            validateDiagnostics((PlpgsqlStatement.GetDiagnosticsStmt) stmt);
        }
    }

    /**
     * {@code FOR r IN c LOOP} reads a cursor rather than a query, and only a cursor declared with
     * its query has one to read — an unbound refcursor names a portal that does not exist yet.
     */
    private void validateCursorLoop(String sql) {
        if (sql == null) return;
        String name = sql.trim();
        if (!name.matches("[A-Za-z_][A-Za-z0-9_$]*")) return;
        VarInfo info = lookup(name);
        if (info != null && info.cursor && !info.boundCursor) {
            throw new MemgresException("cursor FOR loop must use a bound cursor variable", "42601");
        }
    }
}
