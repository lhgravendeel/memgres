package com.memgres.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The routines written in terms of a type, which is what a drop of that type is refused for and
 * what CASCADE takes away with it.
 *
 * <p>A column declared as a type is not the only thing that depends on one. A routine is declared
 * in terms of the types it takes and the type it answers with, and PostgreSQL records a dependency
 * on each of them, so the type cannot go while a routine is still there to be called with it. Every
 * relation carries a composite type of its own name as well, which is why DROP TABLE is refused for
 * a routine whose body never names the table at all — the refusal says the dependency is on the
 * type rather than on the relation.
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

    /** One routine written in terms of the type, and which of the type's two names it used. */
    static final class Dependent {
        /** The routine, or null when this is an aggregate, which memgres keeps its own register of. */
        final PgFunction routine;
        final PgAggregate aggregate;
        /** True when the declaration named the type's array type rather than the type itself. */
        final boolean throughArray;
        private final String signature;
        private final int age;

        private Dependent(PgFunction routine, PgAggregate aggregate, boolean throughArray,
                          String signature, int age) {
            this.routine = routine;
            this.aggregate = aggregate;
            this.throughArray = throughArray;
            this.signature = signature;
            this.age = age;
        }

        /**
         * The routine as PostgreSQL writes it where it names a dependency. An aggregate is a
         * routine in the catalogue like any other, so it is called a function here too.
         */
        String described() {
            return "function " + signature;
        }

        /** The type this dependency is on, which for an array argument is the array type. */
        String typeShown(String display) {
            return throughArray ? display + "[]" : display;
        }
    }

    /**
     * Every routine whose declared result or parameter list is written in terms of this type.
     *
     * <p>PostgreSQL walks the dependency records outwards from the type, and an array type is
     * recorded as depending on its element type, so what the array reaches comes out ahead of what
     * the type itself reaches; the rest follow in the order they were created. An OUT parameter
     * counts as much as an IN one — it is part of what the routine answers with, and the routine
     * could not be called at all once the type it is declared in terms of has gone.
     */
    static List<Dependent> writtenIn(Database database, OidSupplier oids, List<String> visible,
                                     Names names) {
        List<Dependent> found = new ArrayList<Dependent>();
        for (PgFunction fn : database.getAllFunctionOverloads()) {
            Boolean array = mentions(fn, names);
            if (array == null) continue;
            found.add(new Dependent(fn, null, array.booleanValue(),
                    routineSignature(visible, fn), oids.oid("proc:" + fn.getName())));
        }
        for (PgAggregate agg : database.getUserAggregates().values()) {
            Boolean array = mentions(agg, names);
            if (array == null) continue;
            found.add(new Dependent(null, agg, array.booleanValue(),
                    aggregateSignature(visible, agg), oids.oid("proc:" + agg.getName())));
        }
        Collections.sort(found, new Comparator<Dependent>() {
            @Override
            public int compare(Dependent a, Dependent b) {
                if (a.throughArray != b.throughArray) return a.throughArray ? -1 : 1;
                int byAge = Integer.compare(a.age, b.age);
                return byAge != 0 ? byAge : a.signature.compareTo(b.signature);
            }
        });
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
