package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles CREATE/ALTER VIEW, REFRESH MATERIALIZED VIEW.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlViewExecutor {
    private final DdlExecutor ddl;
    private final AstExecutor executor;

    DdlViewExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- CREATE VIEW ----

    QueryResult executeCreateView(CreateViewStmt stmt) {
        ddl.checkPgCatalogWriteProtection();
        if (!stmt.orReplace() && executor.database.hasView(stmt.name())) {
            throw new MemgresException("relation \"" + stmt.name() + "\" already exists", "42P07");
        }
        Database.ViewDef oldView = executor.database.getView(stmt.name());

        if (stmt.orReplace() && oldView != null && !stmt.materialized()) {
            try {
                QueryResult oldResult = executor.executeStatement(oldView.query());
                QueryResult newResult = executor.executeStatement(stmt.query());
                if (newResult.getColumns().size() < oldResult.getColumns().size()) {
                    throw new MemgresException("cannot drop columns from view", "42P16");
                }
            } catch (MemgresException e) {
                throw e;
            } catch (Exception e) {
                // If we can't execute either query, let it fail later
            }
        }

        String viewSchema = executor.defaultSchema();
        int rowCount = 0;
        if (stmt.materialized()) {
            if (stmt.withData()) {
                QueryResult result = executor.executeStatement(stmt.query());
                List<Column> cols = new ArrayList<>(result.getColumns());
                List<Object[]> rows = new ArrayList<>(result.getRows());
                rowCount = rows.size();
                executor.database.addView(new Database.ViewDef(stmt.name(), viewSchema, stmt.query(), stmt.orReplace(),
                        true, cols, rows));
            } else {
                executor.database.addView(new Database.ViewDef(stmt.name(), viewSchema, stmt.query(), stmt.orReplace(),
                        true, Cols.listOf(), Cols.listOf()));
            }
        } else {
            try {
                executor.executeStatement(stmt.query());
            } catch (MemgresException e) {
                if ("42P01".equals(e.getSqlState()) && e.getMessage() != null
                        && e.getMessage().contains("does not exist") && !e.getMessage().contains("missing FROM-clause")) {
                    throw e;
                }
                if ("42703".equals(e.getSqlState())) {
                    throw e;
                }
            } catch (Exception e) {
                // Silently ignore execution errors during view validation
            }
            executor.database.addView(new Database.ViewDef(stmt.name(), viewSchema, stmt.query(), stmt.orReplace(),
                    false, null, null, null, stmt.checkOption(), stmt.withOptions()));
        }

        executor.database.registerSchemaObject(viewSchema, "view", stmt.name());
        if (oldView != null) {
            executor.recordUndo(new Session.DropViewUndo(stmt.name(), oldView));
        }
        executor.recordUndo(new Session.CreateViewUndo(stmt.name()));
        executor.database.setObjectOwner("view:" + viewSchema + "." + stmt.name(), executor.sessionUser());
        if (stmt.materialized()) {
            return QueryResult.command(QueryResult.Type.SELECT_INTO, rowCount);
        }
        return QueryResult.message(QueryResult.Type.SET, "CREATE VIEW");
    }

    // ---- ALTER VIEW ----

    QueryResult executeAlterView(AlterViewStmt stmt) {
        if (stmt.action() == AlterViewStmt.Action.RENAME_TO) {
            Database.ViewDef existing = executor.database.getView(stmt.name());
            if (existing == null) {
                if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
                throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
            }
            if (executor.database.hasView(stmt.newName())) {
                throw new MemgresException("relation \"" + stmt.newName() + "\" already exists", "42P07");
            }
            executor.database.removeView(stmt.name());
            executor.database.addView(new Database.ViewDef(stmt.newName(), existing.schemaName(), existing.query(),
                    existing.orReplace(), existing.materialized(),
                    existing.cachedColumns(), existing.cachedRows(), existing.sourceSQL()));
        }
        if (stmt.action() == AlterViewStmt.Action.OWNER_TO) {
            String newOwner = ddl.resolveOwnerName(stmt.newName());
            if (!executor.database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            Database.ViewDef vd = executor.database.getView(stmt.name());
            String vSchema = (vd != null && vd.schemaName() != null) ? vd.schemaName() : executor.defaultSchema();
            executor.database.setObjectOwner("view:" + vSchema + "." + stmt.name(), newOwner);
        }
        if (stmt.action() == AlterViewStmt.Action.SET_OPTIONS) {
            Database.ViewDef existing = executor.database.getView(stmt.name());
            if (existing == null) {
                if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
                throw new MemgresException("view \"" + stmt.name() + "\" does not exist", "42P01");
            }
            // Merge new options into existing reloptions
            Map<String, String> merged = new LinkedHashMap<>();
            if (existing.reloptions() != null) merged.putAll(existing.reloptions());
            if (stmt.setOptions() != null) merged.putAll(stmt.setOptions());
            executor.database.removeView(stmt.name());
            executor.database.addView(new Database.ViewDef(existing.name(), existing.schemaName(), existing.query(),
                    existing.orReplace(), existing.materialized(),
                    existing.cachedColumns(), existing.cachedRows(), existing.sourceSQL(),
                    existing.checkOption(), merged));
        }
        if (stmt.action() == AlterViewStmt.Action.NO_OP) {
            Database.ViewDef existing = executor.database.getView(stmt.name());
            if (existing == null) {
                if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
                throw new MemgresException("view \"" + stmt.name() + "\" does not exist", "42P01");
            }
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    // ---- REFRESH MATERIALIZED VIEW ----

    QueryResult executeRefreshMaterializedView(RefreshMaterializedViewStmt stmt) {
        Database.ViewDef view = executor.database.getView(stmt.name());
        if (view == null) {
            throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
        }
        if (!view.materialized()) {
            throw new MemgresException("\"" + stmt.name() + "\" is not a materialized view");
        }
        QueryResult result = executor.executeStatement(view.query());
        List<Column> cols = new ArrayList<>(result.getColumns());
        List<Object[]> rows = new ArrayList<>(result.getRows());
        executor.database.addView(new Database.ViewDef(view.name(), view.schemaName(), view.query(), view.orReplace(),
                true, cols, rows));
        return QueryResult.message(QueryResult.Type.SET, "REFRESH MATERIALIZED VIEW");
    }
}
