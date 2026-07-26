package com.memgres.engine;

import com.memgres.engine.parser.ast.SelectStmt;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
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
        if (root == null) return false;
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        Deque<Object> queue = new ArrayDeque<>();
        queue.add(root);
        boolean changed = false;
        while (!queue.isEmpty()) {
            Object node = queue.poll();
            if (!seen.add(node)) continue;

            if (node instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) node;
                if (matches(ref, oldSchema, oldName)) {
                    // A move needs an explicit qualification (search_path would miss the new
                    // schema); a plain rename keeps whatever qualification was written.
                    String targetSchema = ref.schema();
                    if (newSchema != null && !newSchema.equalsIgnoreCase(oldSchema)) targetSchema = newSchema;
                    ref.retarget(targetSchema, newName);
                    changed = true;
                }
                continue; // a TableRef has no AST children
            }
            if (node instanceof Iterable) {
                for (Object child : (Iterable<?>) node) if (child != null) queue.add(child);
                continue;
            }
            if (node instanceof Map) {
                for (Object child : ((Map<?, ?>) node).values()) if (child != null) queue.add(child);
                continue;
            }
            if (node.getClass().isArray()) {
                if (node.getClass().getComponentType().isPrimitive()) continue;
                int len = Array.getLength(node);
                for (int i = 0; i < len; i++) {
                    Object child = Array.get(node, i);
                    if (child != null) queue.add(child);
                }
                continue;
            }
            if (!isAstNode(node)) continue;
            for (Field f : node.getClass().getFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                if (f.getType().isPrimitive() || f.getType() == String.class) continue;
                try {
                    Object child = f.get(node);
                    if (child != null) queue.add(child);
                } catch (IllegalAccessException ignored) {
                    // A non-public field cannot hold AST children in this package
                }
            }
        }
        return changed;
    }

    /** True when the statement tree names this relation anywhere. */
    static boolean referencesRelation(Object root, String schema, String name) {
        if (root == null) return false;
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        Deque<Object> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Object node = queue.poll();
            if (!seen.add(node)) continue;
            if (node instanceof SelectStmt.TableRef) {
                if (matches((SelectStmt.TableRef) node, schema, name)) return true;
                continue;
            }
            if (node instanceof Iterable) {
                for (Object child : (Iterable<?>) node) if (child != null) queue.add(child);
                continue;
            }
            if (node instanceof Map) {
                for (Object child : ((Map<?, ?>) node).values()) if (child != null) queue.add(child);
                continue;
            }
            if (node.getClass().isArray()) {
                if (node.getClass().getComponentType().isPrimitive()) continue;
                int len = Array.getLength(node);
                for (int i = 0; i < len; i++) {
                    Object child = Array.get(node, i);
                    if (child != null) queue.add(child);
                }
                continue;
            }
            if (!isAstNode(node)) continue;
            for (Field f : node.getClass().getFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                if (f.getType().isPrimitive() || f.getType() == String.class) continue;
                try {
                    Object child = f.get(node);
                    if (child != null) queue.add(child);
                } catch (IllegalAccessException ignored) {
                    // see above
                }
            }
        }
        return false;
    }

    private static boolean matches(SelectStmt.TableRef ref, String schema, String name) {
        if (!name.equalsIgnoreCase(ref.table())) return false;
        return ref.schema() == null || schema == null || schema.equalsIgnoreCase(ref.schema());
    }

    private static boolean isAstNode(Object node) {
        Class<?> c = node.getClass();
        while (c != null && c.getEnclosingClass() != null) c = c.getEnclosingClass();
        String pkg = c == null || c.getPackage() == null ? "" : c.getPackage().getName();
        return pkg.startsWith("com.memgres.engine.parser.ast");
    }
}
