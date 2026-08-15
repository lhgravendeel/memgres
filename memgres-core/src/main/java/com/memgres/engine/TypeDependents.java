package com.memgres.engine;

import com.memgres.engine.parser.ast.CreateTypeStmt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * What is written in terms of a type, which is what a drop of that type is refused for and what
 * CASCADE takes away with it.
 *
 * <p>A column declared as a type is not the only thing that depends on one. A routine is declared
 * in terms of the types it takes and the type it answers with, and PostgreSQL records a dependency
 * on each of them, so the type cannot go while a routine is still there to be called with it. Every
 * relation carries a composite type of its own name as well, which is why DROP TABLE is refused for
 * a routine whose body never names the table at all — the refusal says the dependency is on the
 * type rather than on the relation.
 *
 * <p>A type can be written in terms of another type too. A composite type's attribute is declared
 * as a type exactly as a table's column is, and a domain is a base type with conditions on it, so
 * both are recorded against the type they were written over. What CASCADE does with them differs:
 * the composite type outlives the drop and loses the attribute, while the domain goes whole — and
 * with it goes everything written in terms of the domain, which is why a refusal can name an object
 * the statement's own type never reached.
 *
 * <p>What the refusal names and what CASCADE removes have to be one list, and the same list
 * whichever statement asked for it: DROP TYPE by name, DROP TABLE for the relation's row type, or
 * DROP SCHEMA for a type the schema holds. Two answers to that question is how a drop came to name
 * a dependent it then left standing.
 */
final class TypeDependents {

    private TypeDependents() {
    }

    /** Whether a type name written in a declaration is the type being dropped. */
    interface Names {
        boolean matches(String writtenTypeName);
    }

    /** One object written in terms of the type, and which of the type's two names it used. */
    static final class Dependent {
        /** The routine, or null when this is an aggregate, which memgres keeps its own register of. */
        final PgFunction routine;
        final PgAggregate aggregate;
        /** The composite type an attribute of which is declared as the type, or null. */
        final String compositeKey;
        /** That attribute's name, or null when this dependent is not a composite type's. */
        final String attributeName;
        /** The domain written over the type, or null when this dependent is not a domain. */
        final String domainKey;
        /** True when the declaration named the type's array type rather than the type itself. */
        final boolean throughArray;
        private final String described;
        private final int age;
        /** The attribute's number, so one relation's attributes come out from the last back. */
        private final int attnum;

        private Dependent(PgFunction routine, PgAggregate aggregate, String compositeKey,
                          String attributeName, String domainKey, boolean throughArray,
                          String described, int age, int attnum) {
            this.routine = routine;
            this.aggregate = aggregate;
            this.compositeKey = compositeKey;
            this.attributeName = attributeName;
            this.domainKey = domainKey;
            this.throughArray = throughArray;
            this.described = described;
            this.age = age;
            this.attnum = attnum;
        }

        /**
         * The object as PostgreSQL writes it where it names a dependency. An aggregate is a
         * routine in the catalogue like any other, so it is called a function here too.
         */
        String described() {
            return described;
        }

        /** The type this dependency is on, which for an array argument is the array type. */
        String typeShown(String display) {
            return throughArray ? display + "[]" : display;
        }

        /** When the dependent was created, which is the order PostgreSQL reports dependents in. */
        int age() {
            return age;
        }

        /** The dependent attribute's number, or 0 when the dependent is not one. */
        int attnum() {
            return attnum;
        }

        /** The schema the dependent type lives in, for a composite type or a domain. */
        String typeSchema() {
            String key = compositeKey != null ? compositeKey : domainKey;
            return key == null ? null : TypeNamespace.schemaOfKey(key);
        }
    }

    /**
     * The order PostgreSQL reports dependents in. It walks the dependency records outwards from the
     * type, and an array type is recorded as depending on its element type, so what the array
     * reaches comes out ahead of what the type itself reaches; the rest follow in the order they
     * were created, and one relation's own attributes from the last back to the first.
     */
    private static final Comparator<Dependent> IN_RECORD_ORDER = new Comparator<Dependent>() {
        @Override
        public int compare(Dependent a, Dependent b) {
            if (a.throughArray != b.throughArray) return a.throughArray ? -1 : 1;
            int byAge = Integer.compare(a.age, b.age);
            if (byAge != 0) return byAge;
            int byAttribute = Integer.compare(b.attnum, a.attnum);
            return byAttribute != 0 ? byAttribute : a.described.compareTo(b.described);
        }
    };

    /**
     * Every routine whose declared result or parameter list is written in terms of this type.
     *
     * <p>An OUT parameter counts as much as an IN one — it is part of what the routine answers
     * with, and the routine could not be called at all once the type it is declared in terms of has
     * gone.
     */
    static List<Dependent> writtenIn(Database database, OidSupplier oids, List<String> visible,
                                     Names names) {
        List<Dependent> found = new ArrayList<Dependent>();
        for (PgFunction fn : database.getAllFunctionOverloads()) {
            Boolean array = mentions(fn, names);
            if (array == null) continue;
            found.add(new Dependent(fn, null, null, null, null, array.booleanValue(),
                    "function " + routineSignature(visible, fn),
                    oids.oid("proc:" + fn.getName()), 0));
        }
        for (PgAggregate agg : database.getUserAggregates().values()) {
            Boolean array = mentions(agg, names);
            if (array == null) continue;
            found.add(new Dependent(null, agg, null, null, null, array.booleanValue(),
                    "function " + aggregateSignature(visible, agg),
                    oids.oid("proc:" + agg.getName()), 0));
        }
        Collections.sort(found, IN_RECORD_ORDER);
        return found;
    }

    /**
     * Every type written in terms of this one: a composite type with an attribute declared as it,
     * and a domain whose base type is it.
     *
     * <p>An attribute a drop already took away is not a dependent. Its row stays behind to hold its
     * number and keeps the type it was declared with written on it, and reading that as a
     * declaration would refuse a drop for an attribute nothing can name.
     */
    static List<Dependent> typesWrittenIn(Database database, OidSupplier oids, List<String> visible,
                                          Names names) {
        List<Dependent> found = new ArrayList<Dependent>();
        for (Map.Entry<String, List<CreateTypeStmt.CompositeField>> entry
                : database.getCompositeTypes().entrySet()) {
            List<CreateTypeStmt.CompositeField> fields = entry.getValue();
            if (fields == null) continue;
            String shown = TypeNamespace.displayName(database, visible, entry.getKey());
            // A composite type owns a relation of its own name, and the attribute's dependency is
            // recorded against that relation, so it takes its place in creation order by the
            // relation's number rather than by the type's.
            int age = oids.oid("rel:" + TypeNamespace.schemaOfKey(entry.getKey()) + "."
                    + TypeNamespace.nameOfKey(entry.getKey()));
            for (int i = 0; i < fields.size(); i++) {
                CreateTypeStmt.CompositeField field = fields.get(i);
                if (Database.isDroppedAttribute(field)) continue;
                Boolean array = declares(field.typeName(), names);
                if (array == null) continue;
                found.add(new Dependent(null, null, entry.getKey(), field.name(), null,
                        array.booleanValue(),
                        "column " + field.name() + " of composite type " + shown, age, i + 1));
            }
        }
        for (Map.Entry<String, DomainType> entry : database.getDomains().entrySet()) {
            DomainType domain = entry.getValue();
            // A domain over an array of the type keeps only the element's name, so what it was
            // written over is the array type whenever the domain records that it holds arrays.
            Boolean array = declares(domain.getBaseTypeName(), names);
            if (array == null) continue;
            found.add(new Dependent(null, null, null, null, entry.getKey(),
                    array.booleanValue() || domain.isArray(),
                    "type " + TypeNamespace.displayName(database, visible, entry.getKey()),
                    oids.oid("type:" + entry.getKey()), 0));
        }
        Collections.sort(found, IN_RECORD_ORDER);
        return found;
    }

    /**
     * Take the routine away, recording what it takes so a transaction that rolls back gets it
     * back. A routine is identified by the types it is called with, so only the overload written
     * in terms of the type goes and the others of its name stay where they are.
     */
    static void remove(Database database, Dependent dependent, List<Session.UndoEntry> undo) {
        if (dependent.routine != null) {
            undo.add(new Session.DropFunctionUndo(Database.schemaOf(dependent.routine),
                    dependent.routine.getName(),
                    Collections.singletonList(dependent.routine)));
            database.removeFunctionOverload(dependent.routine);
            return;
        }
        database.removeAggregate(dependent.aggregate.getName());
    }

    /**
     * Whether the declaration is written in terms of the type, and whether through its array type;
     * null when it is not written in terms of it at all. A declaration naming both the type and an
     * array of it is one dependent object, and PostgreSQL reaches it first through the array.
     */
    private static Boolean mentions(PgFunction fn, Names names) {
        boolean direct = false;
        boolean array = false;
        String result = fn.getReturnType();
        if (result != null) {
            String bare = result.trim();
            if (bare.length() > 6 && bare.substring(0, 6).equalsIgnoreCase("SETOF ")) {
                bare = bare.substring(6).trim();
            }
            if (isArrayOf(bare, names)) array = true;
            else if (names.matches(bare)) direct = true;
        }
        if (fn.getParams() != null) {
            for (PgFunction.Param p : fn.getParams()) {
                String written = p.typeName();
                if (written == null) continue;
                if (isArrayOf(written.trim(), names)) array = true;
                else if (names.matches(written.trim())) direct = true;
            }
        }
        return array ? Boolean.TRUE : (direct ? Boolean.FALSE : null);
    }

    private static Boolean mentions(PgAggregate agg, Names names) {
        boolean direct = false;
        boolean array = false;
        String[] argTypes = agg.getArgTypes();
        if (argTypes != null) {
            for (String written : argTypes) {
                if (written == null) continue;
                if (isArrayOf(written.trim(), names)) array = true;
                else if (names.matches(written.trim())) direct = true;
            }
        }
        return array ? Boolean.TRUE : (direct ? Boolean.FALSE : null);
    }

    /** The same question of a single written type name, as an attribute or a base type writes it. */
    private static Boolean declares(String written, Names names) {
        if (written == null) return null;
        String bare = written.trim();
        if (isArrayOf(bare, names)) return Boolean.TRUE;
        return names.matches(bare) ? Boolean.FALSE : null;
    }

    private static boolean isArrayOf(String written, Names names) {
        return written.endsWith("[]")
                && names.matches(written.substring(0, written.length() - 2).trim());
    }

    /**
     * The routine written the way PostgreSQL writes it where a dependency names one: its name and
     * the types it is called with, spelled as the type's own name rather than as the declaration
     * wrote it, with no space after the comma. An OUT parameter is no part of how a routine is
     * called and is left out. The name carries its schema when the search path does not reach it,
     * so the reader can tell which of two same-named routines is in the way.
     */
    static String routineSignature(List<String> visible, PgFunction fn) {
        StringBuilder sb = new StringBuilder(RelationNamespace.shownName(visible,
                Database.schemaOf(fn), RelationNamespace.bareName(fn.getName())));
        sb.append('(');
        boolean first = true;
        if (fn.getParams() != null) {
            for (PgFunction.Param p : fn.getParams()) {
                if ("OUT".equalsIgnoreCase(p.mode())) continue;
                if (!first) sb.append(',');
                first = false;
                sb.append(CatalogMetadataFunctions.normalizePgTypeName(p.typeName()));
            }
        }
        return sb.append(')').toString();
    }

    private static String aggregateSignature(List<String> visible, PgAggregate agg) {
        String schema = agg.getSchemaName() != null ? agg.getSchemaName() : "public";
        StringBuilder sb = new StringBuilder(RelationNamespace.shownName(visible, schema,
                RelationNamespace.bareName(agg.getName())));
        sb.append('(');
        String[] argTypes = agg.getArgTypes();
        if (argTypes != null) {
            for (int i = 0; i < argTypes.length; i++) {
                if (i > 0) sb.append(',');
                sb.append(CatalogMetadataFunctions.normalizePgTypeName(argTypes[i]));
            }
        }
        return sb.append(')').toString();
    }
}
