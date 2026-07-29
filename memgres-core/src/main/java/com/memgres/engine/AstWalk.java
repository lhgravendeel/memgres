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
        return null;
    }

    /** Apply {@code action} to every node in the tree, once each. */
    static void forEach(Object root, java.util.function.Consumer<Object> action) {
        findFirst(root, node -> { action.accept(node); return false; });
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
        if (!isAstNode(node)) return;
        for (Field f : node.getClass().getFields()) {
            if (Modifier.isStatic(f.getModifiers())) continue;
            if (f.getType().isPrimitive() || f.getType() == String.class) continue;
            try {
                Object child = f.get(node);
                if (child != null) action.accept(child);
            } catch (IllegalAccessException ignored) {
                // see above
            }
        }
    }

    private static boolean isAstNode(Object node) {
        Class<?> c = node.getClass();
        while (c != null && c.getEnclosingClass() != null) c = c.getEnclosingClass();
        String pkg = c == null || c.getPackage() == null ? "" : c.getPackage().getName();
        return pkg.startsWith("com.memgres.engine.parser.ast");
    }
}
