package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL's polymorphic pseudo-types.
 *
 * A routine may declare {@code anyelement}, {@code anyarray}, {@code anynonarray},
 * {@code anycompatible} and friends instead of concrete types; the concrete types are worked
 * out from the actual arguments at each call, and the declared result type follows from them.
 * Two independent families exist: the "any" family demands that every slot resolve to exactly
 * the same type, while the "anycompatible" family only demands a common type they can all be
 * promoted to. They never resolve from one another, which is why a signature that mixes them
 * has nothing to determine its result from.
 */
public final class PolymorphicTypes {

    private PolymorphicTypes() {
    }

    /** Pseudo-type OIDs, as PG assigns them. */
    private static final Map<String, Integer> OIDS;

    static {
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("anyelement", 2283);
        m.put("anyarray", 2277);
        m.put("anynonarray", 2776);
        m.put("anyenum", 3500);
        m.put("anyrange", 3831);
        m.put("anymultirange", 4537);
        m.put("anycompatible", 5077);
        m.put("anycompatiblearray", 5078);
        m.put("anycompatiblenonarray", 5079);
        m.put("anycompatiblerange", 5080);
        m.put("anycompatiblemultirange", 4538);
        OIDS = java.util.Collections.unmodifiableMap(m);
    }

    /** The "any" family: every slot must end up at the identical concrete type. */
    private static final Set<String> ANY_FAMILY = Cols.setOf(
            "anyelement", "anyarray", "anynonarray", "anyenum", "anyrange", "anymultirange");

    /** The "anycompatible" family: slots only need a common type. */
    private static final Set<String> COMPATIBLE_FAMILY = Cols.setOf(
            "anycompatible", "anycompatiblearray", "anycompatiblenonarray",
            "anycompatiblerange", "anycompatiblemultirange");

    private static final Set<String> NUMERIC_FAMILY = Cols.setOf(
            "smallint", "integer", "bigint", "numeric", "real", "double precision");

    /** Widening order within the numeric family, used to pick a common anycompatible type. */
    private static final List<String> NUMERIC_RANK = Cols.listOf(
            "smallint", "integer", "bigint", "numeric", "real", "double precision");

    public static boolean isPolymorphic(String typeName) {
        return typeName != null && OIDS.containsKey(baseName(typeName));
    }

    /** The pseudo-type's OID, or 0 when the name is not polymorphic. */
    public static int oid(String typeName) {
        Integer o = typeName == null ? null : OIDS.get(baseName(typeName));
        return o != null ? o : 0;
    }

    public static Set<String> names() {
        return OIDS.keySet();
    }

    private static String baseName(String typeName) {
        return typeName.trim().toLowerCase();
    }

    /** True when this polymorphic name stands for an array rather than a single element. */
    private static boolean isArrayShaped(String typeName) {
        String t = baseName(typeName);
        return t.equals("anyarray") || t.equals("anycompatiblearray");
    }

    /**
     * PG rejects a routine whose result type is polymorphic unless some argument of the same
     * family can determine it — SQLSTATE 42P13.
     */
    public static void validateSignature(String returnType, List<String> paramTypes) {
        if (returnType == null) return;
        String ret = baseName(returnType.replaceAll("(?i)^setof\\s+", ""));
        if (!isPolymorphic(ret)) return;
        boolean anyFamilyReturn = ANY_FAMILY.contains(ret);
        if (paramTypes != null) {
            for (String p : paramTypes) {
                if (p == null) continue;
                String pt = baseName(p);
                if (anyFamilyReturn ? ANY_FAMILY.contains(pt) : COMPATIBLE_FAMILY.contains(pt)) return;
            }
        }
        throw new MemgresException("cannot determine result data type"
                + "\n  Detail: A result of type " + ret + " requires at least one input of type "
                + (anyFamilyReturn
                        ? "anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange."
                        : "anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange."),
                "42P13");
    }

    /**
     * Binds the polymorphic slots of a signature against the actual argument types.
     * Returns null when the arguments cannot satisfy the signature, in which case the caller
     * reports the call as an unknown function the way PG does.
     */
    public static Binding bind(List<String> paramTypes, List<String> actualTypes) {
        Binding b = new Binding();
        for (int i = 0; i < paramTypes.size() && i < actualTypes.size(); i++) {
            String declared = paramTypes.get(i);
            if (declared == null || !isPolymorphic(declared)) continue;
            String actual = actualTypes.get(i);
            // An untyped NULL leaves the slot to the other arguments, as PG's unknown does.
            if (actual == null) continue;
            String slot = baseName(declared);
            boolean wantsArray = isArrayShaped(slot);
            String elem = actual;
            if (wantsArray) {
                if (!actual.endsWith("[]")) return null;
                elem = actual.substring(0, actual.length() - 2);
            } else if (slot.equals("anynonarray") || slot.equals("anycompatiblenonarray")) {
                if (actual.endsWith("[]")) return null;
            }
            if (ANY_FAMILY.contains(slot)) {
                if (b.anyType == null) {
                    b.anyType = elem;
                } else if (!b.anyType.equalsIgnoreCase(elem)) {
                    return null;
                }
            } else {
                String merged = commonType(b.compatibleType, elem);
                if (merged == null) return null;
                b.compatibleType = merged;
            }
        }
        return b;
    }

    /** The concrete type a declared (possibly polymorphic) type resolves to, or null. */
    public static String concreteType(String declaredType, Binding binding) {
        if (declaredType == null || binding == null || !isPolymorphic(declaredType)) return declaredType;
        String slot = baseName(declaredType);
        String base = ANY_FAMILY.contains(slot) ? binding.anyType : binding.compatibleType;
        if (base == null) return null;
        return isArrayShaped(slot) ? base + "[]" : base;
    }

    /**
     * The type both arguments can be promoted to, or null when there is none. Only the numeric
     * and text families widen; anything else has to match outright.
     */
    private static String commonType(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a.equalsIgnoreCase(b)) return a;
        if (NUMERIC_FAMILY.contains(a) && NUMERIC_FAMILY.contains(b)) {
            return NUMERIC_RANK.indexOf(a) >= NUMERIC_RANK.indexOf(b) ? a : b;
        }
        if (isTextual(a) && isTextual(b)) return "text";
        return null;
    }

    private static boolean isTextual(String t) {
        return "text".equals(t) || "character varying".equals(t) || "character".equals(t)
                || "name".equals(t);
    }

    /**
     * The PG type name of a runtime value, as the polymorphic binder needs to see it.
     * Returns null for NULL, which the binder treats as an undetermined argument.
     */
    public static String actualTypeName(Object v) {
        if (v == null) return null;
        if (v instanceof List<?>) {
            List<?> list = (List<?>) v;
            String elem = null;
            for (Object o : list) {
                String t = actualTypeName(o);
                if (t == null) continue;
                elem = elem == null ? t : commonType(elem, t);
                if (elem == null) return null;
            }
            return (elem != null ? elem : "text") + "[]";
        }
        if (v instanceof Integer) return "integer";
        if (v instanceof Short) return "smallint";
        if (v instanceof Long) return "bigint";
        if (v instanceof BigDecimal) return "numeric";
        if (v instanceof Float) return "real";
        if (v instanceof Double) return "double precision";
        if (v instanceof Boolean) return "boolean";
        if (v instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) v).typeName();
        DataType dt = TypeCoercion.inferType(v);
        return dt != null ? dt.getPgName() : "text";
    }

    /** The PG type name a DataType stands for, spelled the way the binder compares names. */
    public static String typeName(DataType dt) {
        if (dt == null) return null;
        switch (dt) {
            case INTEGER: return "integer";
            case SMALLINT: return "smallint";
            case BIGINT: return "bigint";
            case NUMERIC: return "numeric";
            case REAL: return "real";
            case DOUBLE_PRECISION: return "double precision";
            case BOOLEAN: return "boolean";
            case TEXT: return "text";
            case VARCHAR: return "character varying";
            case CHAR: return "character";
            case INT4_ARRAY: return "integer[]";
            case TEXT_ARRAY: return "text[]";
            default: return dt.getPgName();
        }
    }

    /** The concrete types bound to each polymorphic family for one call. */
    public static final class Binding {
        String anyType;
        String compatibleType;

        public String anyType() { return anyType; }
        public String compatibleType() { return compatibleType; }
    }
}
