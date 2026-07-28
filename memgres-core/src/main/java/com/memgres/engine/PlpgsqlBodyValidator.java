package com.memgres.engine;

import com.memgres.engine.plpgsql.PlpgsqlStatement;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The checks PostgreSQL applies to a PL/pgSQL body while compiling it, before anything runs: a
 * declaration's type has to resolve, a name may be declared once per block, a NOT NULL variable
 * needs a default, and nothing may be written to a CONSTANT.
 *
 * <p>Running them at compile time is the point. A misspelled {@code %TYPE} is exactly what a
 * schema change produces, and it has to fail where the function is created rather than resolve to
 * something unspecified and surface as a wrong value later.
 */
public final class PlpgsqlBodyValidator {

    private final AstExecutor executor;
    /** One frame per block, mapping a declared name to whether it was declared CONSTANT. */
    private final Deque<Map<String, Boolean>> scopes = new ArrayDeque<>();

    private PlpgsqlBodyValidator(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * @param paramNames names already in scope from the routine's signature; null for a DO block
     */
    public static void validate(AstExecutor executor, PlpgsqlStatement.Block block,
                                Collection<String> paramNames) {
        if (executor == null || block == null) return;
        PlpgsqlBodyValidator validator = new PlpgsqlBodyValidator(executor);
        Map<String, Boolean> params = new LinkedHashMap<String, Boolean>();
        if (paramNames != null) {
            for (String name : paramNames) {
                if (name != null) params.put(name.toLowerCase(), Boolean.FALSE);
            }
        }
        validator.scopes.push(params);
        validator.validateBlock(block);
    }

    // ---- Declarations ----

    private void validateBlock(PlpgsqlStatement.Block block) {
        Map<String, Boolean> frame = new LinkedHashMap<String, Boolean>();
        scopes.push(frame);
        try {
            for (PlpgsqlStatement.VarDeclaration decl : block.declarations()) {
                String key = decl.name().toLowerCase();
                if (frame.containsKey(key)) {
                    throw new MemgresException(
                            "duplicate declaration at or near \"" + decl.name() + "\"", "42601");
                }
                if (!decl.isCursor()) {
                    validateDeclaredType(decl.typeName());
                    if (decl.notNull() && decl.defaultExpr() == null) {
                        throw new MemgresException("variable \"" + decl.name()
                                + "\" must have a default value, since it's declared NOT NULL", "22004");
                    }
                }
                frame.put(key, decl.constant());
            }
            validateStatements(block.body());
            for (PlpgsqlStatement.ExceptionHandler handler : block.exceptionHandlers()) {
                validateStatements(handler.body());
            }
        } finally {
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
        String key = name.toLowerCase();
        for (Map<String, Boolean> frame : scopes) {
            if (frame.containsKey(key)) return true;
        }
        return false;
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
        String key = name.trim().toLowerCase();
        for (Map<String, Boolean> frame : scopes) {
            Boolean constant = frame.get(key);
            if (constant == null) continue;
            if (constant.booleanValue()) {
                throw new MemgresException(
                        "variable \"" + name.trim() + "\" is declared CONSTANT", "22005");
            }
            return;
        }
    }

    private void checkWritable(List<String> targets) {
        if (targets == null) return;
        for (String target : targets) checkWritable(target);
    }

    private void validateStatements(List<PlpgsqlStatement> stmts) {
        if (stmts == null) return;
        for (PlpgsqlStatement stmt : stmts) validateStatement(stmt);
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
            validateStatements(((PlpgsqlStatement.LoopStmt) stmt).body());
        } else if (stmt instanceof PlpgsqlStatement.WhileStmt) {
            validateStatements(((PlpgsqlStatement.WhileStmt) stmt).body());
        } else if (stmt instanceof PlpgsqlStatement.ForStmt) {
            // An integer FOR loop declares its own variable, so it never writes an outer one
            validateStatements(((PlpgsqlStatement.ForStmt) stmt).body());
        } else if (stmt instanceof PlpgsqlStatement.ForQueryStmt) {
            PlpgsqlStatement.ForQueryStmt s = (PlpgsqlStatement.ForQueryStmt) stmt;
            checkWritable(s.varNames());
            validateStatements(s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForExecuteStmt) {
            PlpgsqlStatement.ForExecuteStmt s = (PlpgsqlStatement.ForExecuteStmt) stmt;
            checkWritable(s.varNames());
            validateStatements(s.body());
        } else if (stmt instanceof PlpgsqlStatement.ForeachStmt) {
            PlpgsqlStatement.ForeachStmt s = (PlpgsqlStatement.ForeachStmt) stmt;
            checkWritable(s.varName());
            validateStatements(s.body());
        } else if (stmt instanceof PlpgsqlStatement.SqlStmt) {
            checkWritable(((PlpgsqlStatement.SqlStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.ExecuteStmt) {
            checkWritable(((PlpgsqlStatement.ExecuteStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.FetchStmt) {
            checkWritable(((PlpgsqlStatement.FetchStmt) stmt).intoVars());
        } else if (stmt instanceof PlpgsqlStatement.GetDiagnosticsStmt) {
            for (PlpgsqlStatement.DiagItem item : ((PlpgsqlStatement.GetDiagnosticsStmt) stmt).items()) {
                checkWritable(item.varName());
            }
        }
    }
}
