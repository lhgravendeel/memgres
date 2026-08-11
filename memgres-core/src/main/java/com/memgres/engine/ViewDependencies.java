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

    /**
     * The names of the views that read {@code schemaName.relName} directly, in the order
     * PostgreSQL names them.
     *
     * <p>PostgreSQL walks the dependency catalogue, whose entries are keyed by the dependent's
     * OID, so the order it reports is the order the dependents were created. Walking the stored
     * view map handed them out in that map's hash order instead, which is neither creation order
     * nor name order, so the same three views were named differently from one run to the next.
     * The OID register follows creation order, so asking it is what puts them back.
     */
    static List<String> directDependents(Database db, OidSupplier oids,
                                         String schemaName, String relName) {
        List<Object[]> found = new ArrayList<Object[]>();
        for (Map.Entry<String, Database.ViewDef> e : db.getViews().entrySet()) {
            Database.ViewDef v = e.getValue();
            // A view that reads itself is not something else that depends on it. PostgreSQL
            // records no such dependency, and CREATE OR REPLACE can produce one -- the view is
            // then unreadable, but it still drops on its own.
            if (v.name().equalsIgnoreCase(relName)) continue;
            if (!reads(v.query(), v.schemaName(), schemaName, relName)) continue;
            String vs = v.schemaName() != null ? v.schemaName() : "public";
            int oid = oids == null ? 0 : oids.oid("rel:" + vs + "." + v.name());
            found.add(new Object[]{Integer.valueOf(oid), v.name()});
        }
        java.util.Collections.sort(found, new java.util.Comparator<Object[]>() {
            @Override
            public int compare(Object[] a, Object[] b) {
                return Integer.compare((Integer) a[0], (Integer) b[0]);
            }
        });
        List<String> out = new ArrayList<String>();
        for (Object[] entry : found) out.add((String) entry[1]);
        return out;
    }

    /**
     * The views that must go when {@code schemaName.relName} does: the ones reading it, the ones
     * reading those, and so on. A view over a view is dropped by a CASCADE on the base table.
     *
     * <p>Reported in the order PostgreSQL reports them, which is a depth-first walk: each
     * dependent is followed by whatever depends on it before the next one of its own rank.
     */
    static List<String> cascadeDependents(Database db, OidSupplier oids,
                                          String schemaName, String relName) {
        List<String> out = new ArrayList<String>();
        walkCascade(db, oids, schemaName, relName, new HashSet<String>(), out);
        return out;
    }

    private static void walkCascade(Database db, OidSupplier oids, String schemaName,
                                    String relName, Set<String> seen, List<String> out) {
        for (String viewName : directDependents(db, oids, schemaName, relName)) {
            if (!seen.add(viewName.toLowerCase())) continue;
            out.add(viewName);
            Database.ViewDef v = db.getView(viewName);
            String vs = v != null && v.schemaName() != null ? v.schemaName() : "public";
            walkCascade(db, oids, vs, viewName, seen, out);
        }
    }

    /**
     * The lines a blocked drop puts under DETAIL: every view that reads the relation, then every
     * view that reads one of those, each named beside the relation it actually reads and by the
     * kind that relation really is. A view over a view is as much a dependent as one over the
     * table, and naming the whole chain is what shows why the last of them is in the way.
     *
     * <p>The order is PostgreSQL's: the direct dependents in creation order, and each of them
     * followed at once by whatever depends on it in turn.
     */
    static List<String> dependencyLines(Database db, OidSupplier oids, String schemaName,
                                        String relName, String relKind, List<String> searchPath) {
        return dependencyLines(db, oids, schemaName, relName, relKind, searchPath,
                java.util.Collections.<String>emptySet());
    }

    /**
     * The same, less the relations the one DROP names beside this one. PostgreSQL settles
     * everything a statement will delete before it looks for what would be left pointing at any
     * of it, so a view the same DROP takes down is no reason to refuse, and what depends on that
     * view in turn is reported when its own name comes up, which is where PostgreSQL reports it.
     *
     * @param together bare relation names, lower case, that the same statement drops
     */
    static List<String> dependencyLines(Database db, OidSupplier oids, String schemaName,
                                        String relName, String relKind, List<String> searchPath,
                                        Set<String> together) {
        List<String> out = new ArrayList<String>();
        walkLines(db, oids, schemaName, relName, relKind, searchPath,
                new HashSet<String>(together), out);
        return out;
    }

    private static void walkLines(Database db, OidSupplier oids, String schemaName, String relName,
                                  String relKind, List<String> searchPath, Set<String> seen,
                                  List<String> out) {
        for (String viewName : directDependents(db, oids, schemaName, relName)) {
            if (!seen.add(viewName.toLowerCase())) continue;
            Database.ViewDef v = db.getView(viewName);
            String vs = v != null && v.schemaName() != null ? v.schemaName() : "public";
            String kind = v != null && v.materialized() ? "materialized view" : "view";
            out.add(kind + " " + RelationNamespace.shownName(searchPath, vs, viewName)
                    + " depends on " + relKind + " "
                    + RelationNamespace.shownName(searchPath, schemaName, relName));
            walkLines(db, oids, vs, viewName, kind, searchPath, seen, out);
        }
    }
}
