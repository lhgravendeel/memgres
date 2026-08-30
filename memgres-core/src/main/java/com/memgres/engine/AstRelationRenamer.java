package com.memgres.engine;

import com.memgres.engine.parser.ast.SelectStmt;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Retargets every reference to a renamed or moved relation inside a stored statement tree.
 *
 * <p>PostgreSQL stores a view's dependencies as OIDs, so renaming a table leaves the view
 * working and its deparsed definition showing the new name. Memgres stores parsed ASTs that
 * name relations as strings, so a rename has to walk those ASTs and retarget the references.
 * The walk is reflective over the AST package rather than a hand-written visitor: a node type
 * added later is then covered automatically instead of silently dropping dependents.
 *
 * <p>A name written in a query is not always a reference to a relation. A WITH clause binds the
 * name for the query under it, and inside that query the name is the WITH item's, whatever
 * relation happens to share it. Walked without regard to what is bound where, renaming a table
 * rewrote a CTE reference of the same name inside an unrelated view and changed what that view
 * returned. So the walk carries the names bound above it, and looks past them.
 */
final class AstRelationRenamer {

    private AstRelationRenamer() {}

    /**
     * Rewrite every {@code TableRef} in {@code root} that names {@code oldSchema.oldName} so it
     * names {@code newSchema.newName}. A reference with no explicit schema matches when it was
     * resolvable in the old schema, which the caller establishes by only calling this for the
     * views that actually depend on the relation.
     *
     * @return true when at least one reference was retargeted
     */
    static boolean retarget(Object root, String oldSchema, String oldName,
                            String newSchema, String newName) {
        Walk walk = new Walk(oldSchema, oldName, newSchema, newName);
        walk.visit(root, new HashSet<String>());
        return walk.changed;
    }

    /** True when the statement tree names this relation anywhere a WITH item has not bound. */
    static boolean referencesRelation(Object root, String schema, String name) {
        Walk walk = new Walk(schema, name, null, null);
        walk.visit(root, new HashSet<String>());
        return walk.found;
    }

    /**
     * One pass over a statement tree, carrying the names bound above the node being looked at.
     * The walk is depth-first rather than breadth-first because a binding holds over a subtree
     * and nothing else, which a queue has no way to say.
     */
    private static final class Walk {
        private final String schema;
        private final String name;
        private final String newSchema;
        private final String newName;
        private final Set<Object> seen =
                Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        boolean changed;
        boolean found;

        Walk(String schema, String name, String newSchema, String newName) {
            this.schema = schema;
            this.name = name;
            this.newSchema = newSchema;
            this.newName = newName;
        }

        void visit(Object node, Set<String> bound) {
            if (node == null || !seen.add(node)) return;

            if (node instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) node;
                // A bare name a WITH item bound is that item's, not the relation's.
                if (ref.schema() == null && bound.contains(ref.table().toLowerCase())) return;
                if (!matches(ref, schema, name)) return;
                found = true;
                if (newName == null) return;
                // A move needs an explicit qualification (search_path would miss the new
                // schema); a plain rename keeps whatever qualification was written.
                String targetSchema = ref.schema();
                if (newSchema != null && !newSchema.equalsIgnoreCase(schema)) targetSchema = newSchema;
                ref.retarget(targetSchema, newName);
                changed = true;
                return;
            }
            if (node instanceof SelectStmt) {
                SelectStmt select = (SelectStmt) node;
                Set<String> inner = bound;
                if (select.withClauses() != null && !select.withClauses().isEmpty()) {
                    inner = new HashSet<>(bound);
                    for (SelectStmt.CommonTableExpr cte : select.withClauses()) {
                        if (cte != null && cte.name() != null) inner.add(cte.name().toLowerCase());
                    }
                }
                visitFields(select, inner);
                return;
            }
            if (node instanceof Iterable) {
                for (Object child : (Iterable<?>) node) visit(child, bound);
                return;
            }
            if (node instanceof Map) {
                for (Object child : ((Map<?, ?>) node).values()) visit(child, bound);
                return;
            }
            if (node.getClass().isArray()) {
                if (node.getClass().getComponentType().isPrimitive()) return;
                int len = Array.getLength(node);
                for (int i = 0; i < len; i++) visit(Array.get(node, i), bound);
                return;
            }
            if (!isAstNode(node)) return;
            visitFields(node, bound);
        }

        private void visitFields(Object node, Set<String> bound) {
            for (Field f : node.getClass().getFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                if (f.getType().isPrimitive() || f.getType() == String.class) continue;
                try {
                    visit(f.get(node), bound);
                } catch (IllegalAccessException ignored) {
                    // A non-public field cannot hold AST children in this package
                }
            }
        }
    }

    /**
     * Whether a reference names this relation. A reference written with a schema names the
     * relation in that schema and no other: {@code public.t} is not a reference to a temporary
     * {@code t}, and treating it as one filed a view over a permanent table in the temporary
     * namespace, where it vanished at the end of the session.
     */
    private static boolean matches(SelectStmt.TableRef ref, String schema, String name) {
        if (!name.equalsIgnoreCase(ref.table())) return false;
        if (ref.schema() == null || schema == null) return true;
        return schema.equalsIgnoreCase(ref.schema());
    }

    private static boolean isAstNode(Object node) {
        Class<?> c = node.getClass();
        while (c != null && c.getEnclosingClass() != null) c = c.getEnclosingClass();
        String pkg = c == null || c.getPackage() == null ? "" : c.getPackage().getName();
        return pkg.startsWith("com.memgres.engine.parser.ast");
    }
}
