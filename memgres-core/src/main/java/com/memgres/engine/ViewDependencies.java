package com.memgres.engine;

import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which stored views really read a relation.
 *
 * <p>A view depends on a table because its query reads it, not because the name occurs somewhere
 * in the text: {@code SELECT 'orders' AS why FROM audit} mentions {@code orders} and reads nothing
 * of the sort, {@code orders_2024} is a different relation that merely starts the same way, and a
 * {@code WITH orders AS (...)} shadows the table for the whole query. Matching on text refuses
 * drops PostgreSQL allows in all three cases. This class answers the question from the parsed
 * query instead: the relations named in FROM, at any depth, minus the names bound by a WITH.
 */
final class ViewDependencies {

    private ViewDependencies() {}

    /** True when {@code query} reads the relation {@code schemaName.relName}. */
    static boolean reads(Statement query, String viewSchema, String schemaName, String relName) {
        if (query == null || relName == null) return false;
        final String wanted = relName.toLowerCase();
        final Set<String> cteNames = new HashSet<String>();
        AstWalk.forEach(query, node -> {
            if (node instanceof SelectStmt.CommonTableExpr) {
                String n = ((SelectStmt.CommonTableExpr) node).name();
                if (n != null) cteNames.add(n.toLowerCase());
            }
        });
        final boolean[] found = new boolean[1];
        final String schema = schemaName == null ? "public" : schemaName.toLowerCase();
        final String home = viewSchema == null ? "public" : viewSchema.toLowerCase();
        AstWalk.forEach(query, node -> {
            if (found[0] || !(node instanceof SelectStmt.TableRef)) return;
            SelectStmt.TableRef ref = (SelectStmt.TableRef) node;
            if (ref.table() == null || !ref.table().toLowerCase().equals(wanted)) return;
            if (ref.schema() != null) {
                if (ref.schema().equalsIgnoreCase(schema)) found[0] = true;
                return;
            }
            // Unqualified: a WITH of the same name shadows the relation entirely, and the name
            // resolves through the search path, which reaches public and the view's own schema.
            if (cteNames.contains(wanted)) return;
            if (schema.equals("public") || schema.equals(home)) found[0] = true;
        });
        return found[0];
    }

    /** The names of the views that read {@code schemaName.relName} directly. */
    static List<String> directDependents(Database db, String schemaName, String relName) {
        List<String> out = new ArrayList<String>();
        for (Map.Entry<String, Database.ViewDef> e : db.getViews().entrySet()) {
            Database.ViewDef v = e.getValue();
            // A view that reads itself is not something else that depends on it. PostgreSQL
            // records no such dependency, and CREATE OR REPLACE can produce one -- the view is
            // then unreadable, but it still drops on its own.
            if (v.name().equalsIgnoreCase(relName)) continue;
            if (reads(v.query(), v.schemaName(), schemaName, relName)) out.add(v.name());
        }
        return out;
    }

    /**
     * The views that must go when {@code schemaName.relName} does: the ones reading it, the ones
     * reading those, and so on. A view over a view is dropped by a CASCADE on the base table.
     */
    static List<String> cascadeDependents(Database db, String schemaName, String relName) {
        List<String> out = new ArrayList<String>();
        List<String[]> frontier = new ArrayList<String[]>();
        frontier.add(new String[]{schemaName, relName});
        Set<String> seen = new HashSet<String>();
        while (!frontier.isEmpty()) {
            String[] cur = frontier.remove(frontier.size() - 1);
            for (String viewName : directDependents(db, cur[0], cur[1])) {
                if (!seen.add(viewName.toLowerCase())) continue;
                out.add(viewName);
                Database.ViewDef v = db.getView(viewName);
                String vs = v != null && v.schemaName() != null ? v.schemaName() : "public";
                frontier.add(new String[]{vs, viewName});
            }
        }
        return out;
    }
}
