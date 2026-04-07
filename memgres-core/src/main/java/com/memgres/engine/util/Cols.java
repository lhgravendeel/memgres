package com.memgres.engine.util;

import java.util.*;

/**
 * Java 8-compatible replacements for List.of(), Set.of(), Map.of(), etc.
 */
public final class Cols {
    private Cols() {}

    // ---- List ----

    public static <T> List<T> listOf() {
        return Collections.emptyList();
    }

    @SafeVarargs
    public static <T> List<T> listOf(T... elements) {
        return Collections.unmodifiableList(Arrays.asList(elements.clone()));
    }

    public static <T> List<T> listCopyOf(Collection<? extends T> c) {
        return Collections.unmodifiableList(new ArrayList<>(c));
    }

    // ---- Set ----

    public static <T> Set<T> setOf() {
        return Collections.emptySet();
    }

    @SafeVarargs
    public static <T> Set<T> setOf(T... elements) {
        Set<T> set = new HashSet<>(Arrays.asList(elements));
        if (set.size() != elements.length) {
            throw new IllegalArgumentException("duplicate element");
        }
        return Collections.unmodifiableSet(set);
    }

    // ---- Map ----

    public static <K, V> Map<K, V> mapOf() {
        return Collections.emptyMap();
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1) {
        return Collections.singletonMap(k1, v1);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2) {
        Map<K, V> m = new LinkedHashMap<>();
        m.put(k1, v1); m.put(k2, v2);
        return Collections.unmodifiableMap(m);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
        Map<K, V> m = new LinkedHashMap<>();
        m.put(k1, v1); m.put(k2, v2); m.put(k3, v3);
        return Collections.unmodifiableMap(m);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        Map<K, V> m = new LinkedHashMap<>();
        m.put(k1, v1); m.put(k2, v2); m.put(k3, v3); m.put(k4, v4);
        return Collections.unmodifiableMap(m);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        Map<K, V> m = new LinkedHashMap<>();
        m.put(k1, v1); m.put(k2, v2); m.put(k3, v3); m.put(k4, v4); m.put(k5, v5);
        return Collections.unmodifiableMap(m);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        Map<K, V> m = new LinkedHashMap<>();
        m.put(k1, v1); m.put(k2, v2); m.put(k3, v3); m.put(k4, v4); m.put(k5, v5); m.put(k6, v6);
        return Collections.unmodifiableMap(m);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        Map<K, V> m = new LinkedHashMap<>();
        m.put(k1, v1); m.put(k2, v2); m.put(k3, v3); m.put(k4, v4); m.put(k5, v5); m.put(k6, v6); m.put(k7, v7);
        return Collections.unmodifiableMap(m);
    }

    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    @SafeVarargs
    public static <K, V> Map<K, V> mapOfEntries(Map.Entry<K, V>... entries) {
        Map<K, V> m = new LinkedHashMap<>();
        for (Map.Entry<K, V> e : entries) {
            m.put(e.getKey(), e.getValue());
        }
        return Collections.unmodifiableMap(m);
    }
}
