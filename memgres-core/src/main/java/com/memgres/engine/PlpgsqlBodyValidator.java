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
                    && (returnType.toUpperCase(java.util.Locale.ROOT).startsWith("SETOF")
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
                if (name != null) params.put(name.toLowerCase(java.util.Locale.ROOT), new VarInfo(false, false, false));
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
                String key = decl.name().toLowerCase(java.util.Locale.ROOT);
                if (frame.containsKey(key)) {
                    throw new MemgresException(
                            "duplicate declaration at or near \"" + decl.name() + "\"", "42601");
                }
                if (decl.aliasFor() != null) {
                    // An alias names something that has to be there already; it declares nothing
                    VarInfo target = lookup(decl.aliasFor());
                    if (target == null) {
                        throw new MemgresException(
                                "variable \"" + decl.aliasFor() + "\" does not exist", "42704");
                    }
                    frame.put(key, target);
                    continue;
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
            for (PlpgsqlStatement.ExceptionHandler handler : block.exceptionHandlers()) {
                for (String condition : handler.conditionNames()) validateCondition(condition);
                validateStatements(handler.body());
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
        String upper = type.toUpperCase(java.util.Locale.ROOT);
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
        String key = name.toLowerCase(java.util.Locale.ROOT);
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

    // ---- FOR … IN query ----

    /**
     * The target of a FOR over rows names variables the block already declared — unlike the
     * integer FOR, which defines its own. PostgreSQL says so while compiling the body: a single
     * name it cannot find is not a record variable, and in a list every name has to be one it
     * knows. The two loop forms report the same complaint under different SQLSTATEs, which is
     * PostgreSQL's own inconsistency rather than a choice worth smoothing over.
     */
    private void checkLoopTargetDeclared(List<String> varNames, String sqlState) {
        if (varNames == null || varNames.isEmpty()) return;
        if (varNames.size() == 1) {
            if (lookup(varNames.get(0)) == null) {
                throw new MemgresException("loop variable of loop over rows must be a record"
                        + " variable or list of scalar variables", sqlState);
            }
            return;
        }
        for (String name : varNames) {
            if (lookup(name) == null) {
                throw new MemgresException("\"" + name + "\" is not a known variable", "42601");
            }
        }
    }

    // ---- FETCH … INTO ----

    /**
     * A FETCH with an INTO has one row to put somewhere, so PostgreSQL refuses the directions
     * that are defined to return more than one — FORWARD and BACKWARD with a count, and ALL. It
     * is the direction and not the count's value that decides: {@code FETCH FORWARD 1 … INTO} is
     * refused as surely as {@code FETCH FORWARD ALL … INTO}. MOVE takes no INTO, and the
     * single-row directions (NEXT, PRIOR, FIRST, LAST, ABSOLUTE n, RELATIVE n, bare FORWARD or
     * BACKWARD) are all fine. The check belongs here because PostgreSQL makes it while reading
     * the body: a branch that never runs still refuses to compile.
     */
    private void checkFetchIntoIsSingleRow(PlpgsqlStatement.FetchStmt stmt) {
        if (stmt.move() || stmt.intoVars() == null || stmt.intoVars().isEmpty()) return;
        String direction = stmt.direction() == null ? "NEXT" : stmt.direction().toUpperCase(java.util.Locale.ROOT);
        boolean counted = ("FORWARD".equals(direction) || "BACKWARD".equals(direction))
                && stmt.countExpr() != null;
        if (counted) {
            throw new MemgresException("FETCH statement cannot return multiple rows", "0A000");
        }
    }

    // ---- Exception conditions ----

    private void validateCondition(String condition) {
        String name = condition.trim();
        if (name.equalsIgnoreCase("others")) return;
        if (name.toLowerCase(java.util.Locale.ROOT).startsWith("sqlstate ")) {
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
                    "unrecognized exception condition \"" + name.toLowerCase(java.util.Locale.ROOT) + "\"", "42704");
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

    // PG_CONTEXT is where the handler itself is running, which a handler may ask for as readily
    // as anything else may: refused here, a body PostgreSQL compiles would not compile.
    private static final Set<String> STACKED_ITEMS = Cols.setOf(
            "RETURNED_SQLSTATE", "COLUMN_NAME", "CONSTRAINT_NAME", "PG_DATATYPE_NAME",
            "MESSAGE_TEXT", "TABLE_NAME", "SCHEMA_NAME", "PG_EXCEPTION_DETAIL",
            "PG_EXCEPTION_HINT", "PG_EXCEPTION_CONTEXT", "PG_CONTEXT");

    private static final Set<String> CURRENT_ITEMS = Cols.setOf(
            "ROW_COUNT", "PG_CONTEXT", "PG_ROUTINE_OID");

    /**
     * PostgreSQL reads the whole item list before it judges any item against the form it was
     * written in, so an unknown item name is reported ahead of a known one the form does not
     * offer. Whether a handler is running is not a question the compiler can answer at all; that
     * check belongs to the run and lives in the executor.
     */
    private void validateDiagnostics(PlpgsqlStatement.GetDiagnosticsStmt stmt) {
        for (PlpgsqlStatement.DiagItem item : stmt.items()) {
            // Each item is read target first, so an undeclared target outranks a bad item name
            if (!inScope(item.varName())) {
                throw new MemgresException(
                        "\"" + item.varName() + "\" is not a known variable", "42601");
            }
            checkWritable(item.varName());
            String name = item.itemName().trim();
            if (!STACKED_ITEMS.contains(name.toUpperCase(java.util.Locale.ROOT))
                    && !CURRENT_ITEMS.contains(name.toUpperCase(java.util.Locale.ROOT))) {
                throw new MemgresException("unrecognized GET DIAGNOSTICS item at or near \""
                        + name + "\"", "42601");
            }
        }
        for (PlpgsqlStatement.DiagItem item : stmt.items()) {
            String upper = item.itemName().trim().toUpperCase(java.util.Locale.ROOT);
            if (stmt.stacked() && !STACKED_ITEMS.contains(upper)) {
                throw new MemgresException("diagnostics item " + upper
                        + " is not allowed in GET STACKED DIAGNOSTICS", "42601");
            }
            if (!stmt.stacked() && !CURRENT_ITEMS.contains(upper)) {
                throw new MemgresException("diagnostics item " + upper
                        + " is not allowed in GET CURRENT DIAGNOSTICS", "42601");
            }
        }
    }

    // ---- RAISE ----

    /**
     * The format string and its arguments have to agree, and PostgreSQL settles that while it
     * compiles: both counts are written out in the body, so neither depends on anything the run
     * would discover. A {@code %} with nothing to fill it would otherwise print as itself and the
     * value it stood for would simply be missing from the line.
     */
    private void validateRaise(PlpgsqlStatement.RaiseStmt stmt) {
        String format = stmt.format();
        if (format == null) return;
        int placeholders = 0;
        for (int i = 0; i < format.length(); i++) {
            if (format.charAt(i) != '%') continue;
            if (i + 1 < format.length() && format.charAt(i + 1) == '%') i++;
            else placeholders++;
        }
        int args = stmt.argExprs() == null ? 0 : stmt.argExprs().size();
        if (args < placeholders) {
            throw new MemgresException("too few parameters specified for RAISE", "42601");
        }
        if (args > placeholders) {
            throw new MemgresException("too many parameters specified for RAISE", "42601");
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
                    "RETURN cannot have a parameter in function returning set"
                            + "\n  Hint: Use RETURN NEXT or RETURN QUERY.", "42804");
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

    /**
     * A statement in a body that answers with rows has to say where they go. PostgreSQL has
     * PERFORM for running a query for its effect and INTO for keeping its row; a bare SELECT is
     * neither, and running it anyway threw the rows away silently.
     */
    public static void requireDestination(PlpgsqlStatement.SqlStmt stmt) {
        if (stmt.intoVars() != null && !stmt.intoVars().isEmpty()) return;
        String sql = stmt.sql();
        if (sql == null) return;
        com.memgres.engine.parser.ast.Statement parsed;
        try {
            parsed = com.memgres.engine.parser.Parser.parse(sql);
        } catch (RuntimeException e) {
            return;   // a body this cannot read is left for execution to report
        }
        if (returnsRows(parsed)) {
            throw new MemgresException("query has no destination for result data"
                    + "\n  Hint: If you want to discard the results of a SELECT,"
                    + " use PERFORM instead.", "42601");
        }
    }

    /** Whether this statement hands rows back to whoever ran it. */
    private static boolean returnsRows(com.memgres.engine.parser.ast.Statement parsed) {
        if (parsed instanceof com.memgres.engine.parser.ast.SelectStmt) return true;
        if (parsed instanceof com.memgres.engine.parser.ast.SetOpStmt) return true;
        if (parsed instanceof com.memgres.engine.parser.ast.InsertStmt) {
            java.util.List<?> r = ((com.memgres.engine.parser.ast.InsertStmt) parsed).returning;
            return r != null && !r.isEmpty();
        }
        if (parsed instanceof com.memgres.engine.parser.ast.UpdateStmt) {
            java.util.List<?> r = ((com.memgres.engine.parser.ast.UpdateStmt) parsed).returning;
            return r != null && !r.isEmpty();
        }
        if (parsed instanceof com.memgres.engine.parser.ast.DeleteStmt) {
            java.util.List<?> r = ((com.memgres.engine.parser.ast.DeleteStmt) parsed).returning;
            return r != null && !r.isEmpty();
        }
        return false;
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
            // A cursor FOR loop declares its own record for the cursor's rows, so the target
            // there is nothing the block had to declare
            if (!namesACursor(s.sql())) checkLoopTargetDeclared(s.varNames(), "42601");
            checkWritable(s.varNames());
            validateLoop(s.label(), s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForExecuteStmt) {
            PlpgsqlStatement.ForExecuteStmt s = (PlpgsqlStatement.ForExecuteStmt) stmt;
            checkLoopTargetDeclared(s.varNames(), "42804");
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
            PlpgsqlStatement.SqlStmt sql = (PlpgsqlStatement.SqlStmt) stmt;
            checkWritable(sql.intoVars());
        } else if (stmt instanceof PlpgsqlStatement.ExecuteStmt) {
            checkWritable(((PlpgsqlStatement.ExecuteStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.FetchStmt) {
            checkWritable(((PlpgsqlStatement.FetchStmt) stmt).intoVars());
            checkFetchIntoIsSingleRow((PlpgsqlStatement.FetchStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.GetDiagnosticsStmt) {
            validateDiagnostics((PlpgsqlStatement.GetDiagnosticsStmt) stmt);
        } else if (stmt instanceof PlpgsqlStatement.RaiseStmt) {
            PlpgsqlStatement.RaiseStmt raise = (PlpgsqlStatement.RaiseStmt) stmt;
            validateRaise(raise);
            if (raise.condition() != null) validateCondition(raise.condition());
        }
    }

    /**
     * {@code FOR r IN c LOOP} reads a cursor rather than a query, and only a cursor declared with
     * its query has one to read — an unbound refcursor names a portal that does not exist yet.
     */
    /** True when what the loop reads is a cursor variable rather than a query of its own. */
    private boolean namesACursor(String sql) {
        if (sql == null) return false;
        String name = sql.trim();
        if (!name.matches("[A-Za-z_][A-Za-z0-9_$]*")) return false;
        VarInfo info = lookup(name);
        return info != null && info.cursor;
    }

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
