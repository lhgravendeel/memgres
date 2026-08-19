package com.memgres.engine;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Generic traversal over a parsed statement or expression tree.
 *
 * <p>The walk is reflective over the AST package rather than a hand-written visitor per node
 * type: checks like "does this expression contain a set-returning call anywhere" have to be
 * exhaustive to be correct, and a hand-written visitor silently stops covering node types
 * added later.
 */
final class AstWalk {

    private AstWalk() {}

    /** True when any node in the tree satisfies {@code pred}. */
    static boolean anyMatch(Object root, Predicate<Object> pred) {
        return findFirst(root, pred) != null;
    }

    /** The first node in the tree satisfying {@code pred}, or null. */
    static Object findFirst(Object root, Predicate<Object> pred) {
        if (root == null) return null;
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        Deque<Object> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Object node = queue.poll();
            if (!seen.add(node)) continue;
            if (pred.test(node)) return node;
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
            for (Field f : childFields(node.getClass())) {
                try {
                    Object child = f.get(node);
                    if (child != null) queue.add(child);
                } catch (IllegalAccessException ignored) {
                    // A non-public field cannot hold AST children in this package
                }
            }
        }
        return null;
    }

    /** Apply {@code action} to every node in the tree, once each. */
    static void forEach(Object root, java.util.function.Consumer<Object> action) {
        findFirst(root, node -> { action.accept(node); return false; });
    }

    /**
     * The same, stopping at every node {@code apart} holds: such a node is itself handed to
     * {@code action}, and what it stands over is not walked at all. A null predicate stops
     * nowhere, which is {@link #forEach}.
     */
    static void forEachOutside(Object root, Predicate<Object> apart,
                               java.util.function.Consumer<Object> action) {
        if (root == null) return;
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        Deque<Object> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Object node = queue.poll();
            if (!seen.add(node)) continue;
            action.accept(node);
            if (apart != null && apart.test(node)) continue;
            forEachChild(node, child -> queue.add(child));
        }
    }

    /** Apply {@code action} to each direct AST child of a node (one level, no recursion). */
    static void forEachChild(Object node, java.util.function.Consumer<Object> action) {
        if (node == null) return;
        if (node instanceof Iterable) {
            for (Object child : (Iterable<?>) node) if (child != null) action.accept(child);
            return;
        }
        if (node instanceof Map) {
            for (Object child : ((Map<?, ?>) node).values()) if (child != null) action.accept(child);
            return;
        }
        if (node.getClass().isArray()) {
            if (node.getClass().getComponentType().isPrimitive()) return;
            int len = Array.getLength(node);
            for (int i = 0; i < len; i++) {
                Object child = Array.get(node, i);
                if (child != null) action.accept(child);
            }
            return;
        }
        for (Field f : childFields(node.getClass())) {
            try {
                Object child = f.get(node);
                if (child != null) action.accept(child);
            } catch (IllegalAccessException ignored) {
                // see above
            }
        }
    }

    private static boolean isAstNode(Class<?> nodeClass) {
        Class<?> c = nodeClass;
        while (c != null && c.getEnclosingClass() != null) c = c.getEnclosingClass();
        String pkg = c == null || c.getPackage() == null ? "" : c.getPackage().getName();
        return pkg.startsWith("com.memgres.engine.parser.ast");
    }

    /** Nothing to walk into, shared rather than allocated per node of a type that has no children. */
    private static final Field[] NONE = new Field[0];

    /** The child fields of each node type, worked out once. */
    private static final java.util.Map<Class<?>, Field[]> CHILD_FIELDS =
            new java.util.concurrent.ConcurrentHashMap<Class<?>, Field[]>();

    /**
     * The fields of a node type that can hold children.
     *
     * <p>Which fields those are is a property of the type, so it is settled once per type rather
     * than once per node. {@link Class#getDeclaredFields()} hands back a fresh array on every call
     * and {@link Class#getEnclosingClass()} is not free either; between them they were the largest
     * cost of walking a statement, and a statement is walked several times over before it runs.
     *
     * <p>Declared rather than public: most nodes in the package hold their children in public
     * fields, but a few hold them privately behind accessors, and a walk that reads only what is
     * public simply does not enter those. That is not a distinction any caller means to draw --
     * a subscript reference and a custom operator hold operands like every other node -- and it
     * made the answers depend on how a node happened to be written.
     */
    private static Field[] childFields(Class<?> nodeClass) {
        Field[] cached = CHILD_FIELDS.get(nodeClass);
        if (cached != null) return cached;
        Field[] fields;
        if (!isAstNode(nodeClass)) {
            fields = NONE;
        } else {
            java.util.List<Field> kept = new java.util.ArrayList<Field>();
            for (Class<?> c = nodeClass; c != null && isAstNode(c); c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    if (Modifier.isStatic(f.getModifiers())) continue;
                    if (f.getType().isPrimitive() || f.getType() == String.class) continue;
                    if (!f.isAccessible()) f.setAccessible(true);
                    kept.add(f);
                }
            }
            fields = kept.isEmpty() ? NONE : kept.toArray(new Field[0]);
        }
        CHILD_FIELDS.put(nodeClass, fields);
        return fields;
    }
}
