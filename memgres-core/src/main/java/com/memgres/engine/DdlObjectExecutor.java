package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles CREATE/ALTER/DROP for types, functions, triggers, sequences, domains, and generic DROP dispatch.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlObjectExecutor {
    private final DdlExecutor ddl;
    private final AstExecutor executor;

    DdlObjectExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- CREATE TYPE ----

    QueryResult executeCreateType(CreateTypeStmt stmt) {
        ddl.checkPgCatalogWriteProtection();
        String name = stmt.name();
        // A type lands in the schema written before its name, or in the one a CREATE lands in.
        // Only that schema's own namespace decides whether the name is free, so a.e and b.e are
        // two types rather than one refusal.
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schemaName());
        String schema = stmt.schemaName() != null
                ? stmt.schemaName().toLowerCase() : executor.creationSchema();
        boolean wasShell = executor.database.getShellTypes().contains(TypeNamespace.key(schema, name));
        if (wasShell) {
            // CREATE TYPE x; twice reserves a name already reserved; any other form fills it in.
            if (stmt.shell()) throw PgErrors.duplicateObject("type", name);
        } else {
            // A composite type owns a pg_class row as well as a pg_type row, so it also has to
            // find the relation name free; an enum, a range, a domain and a shell do not.
            TypeNamespace.requireCreatableType(executor.database, schema, name,
                    stmt.compositeFields() != null);
        }
        if (stmt.shell()) {
            executor.database.addShellType(schema, name);
            executor.database.registerSchemaObject(schema, "shell", name);
            return QueryResult.command(QueryResult.Type.CREATE_TYPE, 0);
        }
        if (stmt.enumLabels() != null) {
            Set<String> seen = new HashSet<>();
            for (String label : stmt.enumLabels()) {
                if (!seen.add(label)) {
                    // PostgreSQL surfaces this as the pg_enum index it violates, not as a DDL error
                    MemgresException dup = new MemgresException("duplicate key value violates "
                            + "unique constraint \"pg_enum_typid_label_index\"", "23505");
                    dup.setConstraint("pg_enum_typid_label_index");
                    throw dup;
                }
            }
            // A type created under a name a dropped type used to answer to is a new type, and
            // takes a new OID -- PostgreSQL never reuses one.
            executor.identity().typeCreated(TypeNamespace.key(schema, name));
            CustomEnum created = new CustomEnum(schema, name, stmt.enumLabels());
            executor.database.addCustomEnum(created);
            executor.database.markUncommittedObject(created, executor.session);
            executor.database.registerSchemaObject(schema, "enum", name);
            // CREATE TYPE is undone by ROLLBACK like any other DDL; without this the type
            // outlives the transaction that never committed it.
            executor.recordUndo(new Session.CreateEnumTypeUndo(schema, name));
        } else if (stmt.rangeSubtype() != null) {
            DdlExecutor.ResolvedType subtype = ddl.resolveColumnType(stmt.rangeSubtype(), null);
            // A canonical function takes the range being defined and answers one, so it can only be
            // written once the name already stands for something — which is what the shell type is
            // for, and why a definition that names one without having reserved the name first is
            // refused rather than left to fail when the function is looked up.
            if (stmt.rangeCanonical() && !wasShell) {
                MemgresException e = PgErrors.invalidObjectState(
                        "cannot specify a canonical function without a pre-created shell type");
                // Three statements in order are what it takes, and PostgreSQL spells them out
                // because the order is the whole of the difficulty.
                e.setHint("Create the type as a shell type, then create its canonicalization "
                        + "function, then do a full CREATE TYPE.");
                throw e;
            }
            if (stmt.rangeSubtypeOpclass() == null) {
                DdlDefinitionChecks.requireOrderableRangeSubtype(subtype.dataType());
            } else {
                requireBtreeOpclass(stmt.rangeSubtypeOpclass());
            }
            // A type created under a name a dropped type used to answer to is a new type, and
            // takes a new OID -- PostgreSQL never reuses one.
            executor.identity().typeCreated(TypeNamespace.key(schema, name));
            executor.database.addRangeType(schema, name, stmt.rangeSubtype());
            executor.database.registerSchemaObject(schema, "range", name);
            // CREATE TYPE is undone by ROLLBACK like any other DDL; without this the type
            // outlives the transaction that never committed it, and the name it left behind
            // refuses the next statement that tries to create it for real.
            executor.recordUndo(new Session.CreateRangeTypeUndo(schema, name));
        } else if (stmt.compositeFields() != null) {
            // Attribute names are checked before their types, as PostgreSQL does: a duplicate name
            // is reported even when the second attribute also names a type that does not exist.
            // Compared as written: the parser has already folded unquoted names to lower case, so
            // ("A" int, a text) is two attributes while (a int, A text) is one name twice.
            Set<String> seen = new HashSet<>();
            for (CreateTypeStmt.CompositeField f : stmt.compositeFields()) {
                if (!seen.add(f.name())) {
                    throw PgErrors.duplicateColumn(f.name());
                }
            }
            for (CreateTypeStmt.CompositeField f : stmt.compositeFields()) {
                ddl.resolveColumnType(f.typeName(), null);
            }
            executor.identity().typeCreated(TypeNamespace.key(schema, name));
            executor.database.addCompositeType(schema, name, stmt.compositeFields());
            executor.database.registerSchemaObject(schema, "composite", name);
            executor.recordUndo(new Session.CreateCompositeTypeUndo(schema, name));
        }
        if (wasShell) executor.database.getShellTypes().remove(TypeNamespace.key(schema, name));
        return QueryResult.command(QueryResult.Type.CREATE_TYPE, 0);
    }

    /**
     * Refuse a SUBTYPE_OPCLASS that names no btree class, whether one PostgreSQL ships or one this
     * database was told about. The complaint names the access method as well as the class, because
     * a class belongs to one method and it is btree's list that was searched.
     */
    private void requireBtreeOpclass(String written) {
        String bare = TypeNamespace.bare(written).toLowerCase(Locale.ROOT);
        if (CatalogTypeSystemBuilder.shipsBtreeOpclass(bare)
                || executor.database.hasOperatorClass(bare + ":btree")) {
            return;
        }
        throw new MemgresException("operator class \"" + bare
                + "\" does not exist for access method \"btree\"", "42704");
    }

    // ---- ALTER TYPE ----

    QueryResult executeAlterType(AlterTypeStmt stmt) {
        boolean onAttribute = stmt.action() == AlterTypeStmt.Action.ADD_ATTRIBUTE
                || stmt.action() == AlterTypeStmt.Action.DROP_ATTRIBUTE
                || stmt.action() == AlterTypeStmt.Action.ALTER_ATTRIBUTE_TYPE
                || stmt.action() == AlterTypeStmt.Action.RENAME_ATTRIBUTE;
        // A qualifier names the schema the type has to be in, and resolution honours it: ALTER
        // TYPE a.e reaches a's e or nothing, so a type somewhere else is left alone.
        // Check if this is a composite type operation first
        if (onAttribute) {
            return executeAlterCompositeType(stmt);
        }
        // RENAME TO and SET SCHEMA apply to any type; only an enum is looked up below, so a
        // composite has to be handled here or it would be reported as a type that does not exist.
        String named = TypeNamespace.resolve(executor.database, executor.session, stmt.typeName());
        if ((stmt.action() == AlterTypeStmt.Action.RENAME_TO
                || stmt.action() == AlterTypeStmt.Action.SET_SCHEMA)
                && named != null
                && executor.database.getCompositeTypes().containsKey(named)) {
            return executeAlterCompositeType(stmt);
        }

        // Which e this is is settled once, by the schema written or by the search path, and the
        // rest of the statement works on that one.
        String typeKey = TypeNamespace.resolve(executor.database, executor.session, stmt.typeName());
        CustomEnum existing = typeKey == null ? null : executor.database.getCustomEnums().get(typeKey);
        if (existing == null) {
            // A label belongs to an enum and to nothing else, so a type of another kind — or a
            // relation, whose row type is a type of that name too — is the wrong kind of object
            // here rather than a missing one.
            boolean onLabel = stmt.action() == AlterTypeStmt.Action.ADD_VALUE
                    || stmt.action() == AlterTypeStmt.Action.RENAME_VALUE;
            String lookIn = TypeNamespace.writtenSchema(stmt.typeName()) != null
                    ? TypeNamespace.writtenSchema(stmt.typeName()) : executor.defaultSchema();
            String lookFor = TypeNamespace.bare(stmt.typeName());
            if (onLabel && (TypeNamespace.kindOf(executor.database, lookIn, lookFor) != null
                    || TypeNamespace.rowTypeOwner(executor.database, lookIn, lookFor) != null)) {
                throw PgErrors.wrongObjectType(stmt.typeName() + " is not an enum");
            }
            throw new MemgresException("type \"" + stmt.typeName() + "\" does not exist", "42704");
        }

        switch (stmt.action()) {
            case ADD_VALUE: {
                if (stmt.ifNotExists() && existing.isValidLabel(stmt.value())) break;
                if (!stmt.ifNotExists() && existing.isValidLabel(stmt.value())) {
                    throw new MemgresException("enum label \"" + stmt.value() + "\" already exists", "42710");
                }
                // L11: use addLabel methods to preserve fractional enumsortorder
                if ("BEFORE".equals(stmt.position())) {
                    existing.addLabelBefore(stmt.value(), stmt.neighbor());
                } else if ("AFTER".equals(stmt.position())) {
                    existing.addLabelAfter(stmt.value(), stmt.neighbor());
                } else {
                    existing.addLabel(stmt.value());
                }
                break;
            }
            case RENAME_VALUE: {
                int idx = existing.getLabels().indexOf(stmt.value());
                if (idx < 0) {
                    throw new MemgresException("\"" + stmt.value() + "\" is not an existing enum label", "22023");
                }
                if (existing.getLabels().indexOf(stmt.newValue()) >= 0) {
                    // Two labels with the same text would orphan every stored value of the old one
                    throw new MemgresException("enum label \"" + stmt.newValue() + "\" already exists", "42710");
                }
                existing.getLabels().set(idx, stmt.newValue());
                break;
            }
            case RENAME_TO: {
                // The type stays where it is; only its name changes, and it keeps its identity —
                // its labels, and whatever was said about it in a comment.
                String schema = TypeNamespace.schemaOfKey(typeKey);
                requireTypeNameFree(typeRef(schema, stmt.value()));
                inStoredTypes(() -> executor.database.getCustomEnums().remove(typeKey));
                CustomEnum renamed = new CustomEnum(schema, stmt.value(), existing.getLabels());
                executor.database.addCustomEnum(renamed);
                executor.database.unregisterSchemaObject(schema, "enum", TypeNamespace.nameOfKey(typeKey));
                executor.database.registerSchemaObject(schema, "enum", stmt.value());
                executor.database.moveComment("type", typeKey, TypeNamespace.key(schema, stmt.value()));
                retargetTypeColumns(typeKey, TypeNamespace.key(schema, stmt.value()));
                // A column declared with the old word is not rewritten, so the old word goes on
                // answering with this type's OID.
                executor.identity().typeRenamed("e", typeKey, TypeNamespace.key(schema, stmt.value()));
                break;
            }
            case SET_SCHEMA: {
                requireSchemaExists(stmt.value());
                String from = TypeNamespace.schemaOfKey(typeKey);
                String bare = TypeNamespace.nameOfKey(typeKey);
                TypeNamespace.requireFree(executor.database, stmt.value(), bare);
                inStoredTypes(() -> executor.database.getCustomEnums().remove(typeKey));
                existing.setSchemaName(stmt.value());
                executor.database.addCustomEnum(existing);
                executor.database.unregisterSchemaObject(from, "enum", bare);
                executor.database.registerSchemaObject(stmt.value(), "enum", bare);
                executor.database.moveComment("type", typeKey, TypeNamespace.key(stmt.value(), bare));
                retargetTypeColumns(typeKey, TypeNamespace.key(stmt.value(), bare));
                break;
            }
            case OWNER_TO: {
                requireOwnerExists(stmt.value());
                break;
            }
        }
        return QueryResult.command(QueryResult.Type.ALTER_TYPE, 0);
    }

    private QueryResult executeAlterCompositeType(AlterTypeStmt stmt) {
        String typeKey = TypeNamespace.resolve(executor.database, executor.session, stmt.typeName());
        List<CreateTypeStmt.CompositeField> fields =
                typeKey == null ? null : executor.database.getCompositeTypes().get(typeKey);
        if (fields == null) {
            // An attribute lives on the relation a composite type owns, so this is a relation
            // lookup: PostgreSQL reports a name that owns no relation as a missing relation, and a
            // name that owns one of the wrong kind as not a composite type. Only RENAME TO and SET
            // SCHEMA, which any type takes, still report a missing type.
            if (stmt.action() == AlterTypeStmt.Action.RENAME_TO
                    || stmt.action() == AlterTypeStmt.Action.SET_SCHEMA) {
                throw new MemgresException(
                        "type \"" + stmt.typeName() + "\" does not exist", "42704");
            }
            String lookIn = TypeNamespace.writtenSchema(stmt.typeName()) != null
                    ? TypeNamespace.writtenSchema(stmt.typeName()) : executor.defaultSchema();
            String lookFor = TypeNamespace.bare(stmt.typeName());
            if (RelationNamespace.kindOf(executor.database, lookIn, lookFor) != null) {
                throw new MemgresException(
                        "\"" + lookFor + "\" is not a composite type", "42809");
            }
            throw new MemgresException(
                    "relation \"" + stmt.typeName() + "\" does not exist", "42P01");
        }
        // An attribute error names the relation the composite owns, which is the type's own bare
        // name however the statement wrote it.
        String typeName = TypeNamespace.nameOfKey(typeKey);

        // What a type is made of is shared state: every value of it and every column declared with
        // it reads the attribute list, so an attribute added, dropped, renamed or retyped inside a
        // transaction that rolls back has to leave the list it found -- the same reason ALTER
        // DOMAIN records what it displaced. RENAME TO and SET SCHEMA move the list whole and have
        // nothing here to put back.
        if (stmt.action() == AlterTypeStmt.Action.ADD_ATTRIBUTE
                || stmt.action() == AlterTypeStmt.Action.DROP_ATTRIBUTE
                || stmt.action() == AlterTypeStmt.Action.ALTER_ATTRIBUTE_TYPE
                || stmt.action() == AlterTypeStmt.Action.RENAME_ATTRIBUTE) {
            executor.recordUndo(new Session.AlterCompositeTypeUndo(typeKey, fields));
        }

        switch (stmt.action()) {
            case ADD_ATTRIBUTE: {
                if (hasAttribute(fields, stmt.value())) {
                    throw new MemgresException("column \"" + stmt.value() + "\" of relation \""
                            + typeName + "\" already exists", "42701");
                }
                ddl.resolveColumnType(stmt.newValue(), null);
                List<CreateTypeStmt.CompositeField> newFields = new ArrayList<>(fields);
                // After whatever a drop left behind, not in the gap it made: an attribute number
                // PostgreSQL has handed out once is never handed out again.
                newFields.add(new CreateTypeStmt.CompositeField(stmt.value(), stmt.newValue()));
                executor.database.replaceCompositeFields(typeKey, newFields);
                break;
            }
            case DROP_ATTRIBUTE: {
                if (!hasAttribute(fields, stmt.value())) {
                    if (stmt.ifExists()) {
                        if (executor.session != null) {
                            executor.session.addNotice("NOTICE", "00000", "column \"" + stmt.value()
                                    + "\" of relation \"" + typeName
                                    + "\" does not exist, skipping", null);
                        }
                        break;
                    }
                    throw new MemgresException("column \"" + stmt.value() + "\" of relation \""
                            + typeName + "\" does not exist", "42703");
                }
                List<CreateTypeStmt.CompositeField> newFields = new ArrayList<>();
                for (int i = 0; i < fields.size(); i++) {
                    CreateTypeStmt.CompositeField f = fields.get(i);
                    if (f.name().equalsIgnoreCase(stmt.value())) {
                        // The attribute is not taken out of the list: PostgreSQL leaves its row
                        // under a name nobody could have written and marks it dropped, so the
                        // attributes after it keep their numbers and the next one added takes a
                        // number of its own. The type it was declared with stays on the row too,
                        // which is what the modifier goes on being reported from.
                        newFields.add(new CreateTypeStmt.CompositeField(
                                Database.droppedAttributeName(i + 1), f.typeName()));
                    } else {
                        newFields.add(f);
                    }
                }
                executor.database.replaceCompositeFields(typeKey, newFields);
                break;
            }
            case ALTER_ATTRIBUTE_TYPE: {
                if (!hasAttribute(fields, stmt.value())) {
                    throw new MemgresException("column \"" + stmt.value() + "\" of relation \""
                            + typeName + "\" does not exist", "42703");
                }
                refuseAttributeTypeChangeInUse(typeKey, stmt.cascade());
                List<CreateTypeStmt.CompositeField> newFields = new ArrayList<>();
                for (CreateTypeStmt.CompositeField f : fields) {
                    if (f.name().equalsIgnoreCase(stmt.value())) {
                        newFields.add(new CreateTypeStmt.CompositeField(f.name(), stmt.newValue()));
                    } else {
                        newFields.add(f);
                    }
                }
                executor.database.replaceCompositeFields(typeKey, newFields);
                break;
            }
            case RENAME_ATTRIBUTE: {
                if (!hasAttribute(fields, stmt.value())) {
                    throw new MemgresException("column \"" + stmt.value() + "\" does not exist", "42703");
                }
                if (hasAttribute(fields, stmt.newValue())) {
                    throw new MemgresException("column \"" + stmt.newValue() + "\" of relation \""
                            + typeName + "\" already exists", "42701");
                }
                List<CreateTypeStmt.CompositeField> newFields = new ArrayList<>();
                for (CreateTypeStmt.CompositeField f : fields) {
                    if (f.name().equalsIgnoreCase(stmt.value())) {
                        newFields.add(new CreateTypeStmt.CompositeField(stmt.newValue(), f.typeName()));
                    } else {
                        newFields.add(f);
                    }
                }
                executor.database.replaceCompositeFields(typeKey, newFields);
                break;
            }
            case RENAME_TO: {
                String schema = TypeNamespace.schemaOfKey(typeKey);
                requireCompositeRenameTargetFree(typeRef(schema, stmt.value()));
                inStoredTypes(() -> executor.database.getCompositeTypes().remove(typeKey));
                executor.database.addCompositeType(schema, stmt.value(), fields);
                executor.database.unregisterSchemaObject(
                        schema, "composite", TypeNamespace.nameOfKey(typeKey));
                executor.database.registerSchemaObject(schema, "composite", stmt.value());
                executor.database.moveComment("type", typeKey, TypeNamespace.key(schema, stmt.value()));
                retargetTypeColumns(typeKey, TypeNamespace.key(schema, stmt.value()));
                executor.identity().typeRenamed("c", typeKey, TypeNamespace.key(schema, stmt.value()));
                // A composite type owns a pg_class row of its own, which is the same relation
                // under the new name.
                executor.identity().relationRenamed("c", schema, TypeNamespace.nameOfKey(typeKey),
                        schema, stmt.value());
                break;
            }
            case SET_SCHEMA: {
                requireSchemaExists(stmt.value());
                String from = TypeNamespace.schemaOfKey(typeKey);
                String bare = TypeNamespace.nameOfKey(typeKey);
                TypeNamespace.requireFree(executor.database, stmt.value(), bare);
                inStoredTypes(() -> executor.database.getCompositeTypes().remove(typeKey));
                executor.database.addCompositeType(stmt.value(), bare, fields);
                executor.database.unregisterSchemaObject(from, "composite", bare);
                executor.database.registerSchemaObject(stmt.value(), "composite", bare);
                executor.database.moveComment("type", typeKey, TypeNamespace.key(stmt.value(), bare));
                retargetTypeColumns(typeKey, TypeNamespace.key(stmt.value(), bare));
                // Same object, new schema: the pg_class row it owns goes with it.
                executor.identity().relationRenamed("c", from, bare, stmt.value(), bare);
                break;
            }
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.ALTER_TYPE, 0);
    }

    /**
     * Refuse an attribute type change while something is holding values of the type.
     *
     * <p>A stored value of a composite is laid out by the attribute types the type had when it was
     * written, so PostgreSQL will not change one under a relation that holds such values: it names
     * the first column that does and refuses, and CASCADE does not excuse it. A table declared OF
     * the type is the one case CASCADE is for -- its whole shape is the type's, so the change can
     * reshape it -- and PostgreSQL says so in the hint. The column check comes first: with both in
     * the way, it is the column PostgreSQL names.
     */
    private void refuseAttributeTypeChangeInUse(String typeKey, boolean cascade) {
        String shown = TypeNamespace.display(executor.database, executor.session, typeKey);
        Set<String> holding = typeKeysBuiltOn(typeKey);
        Table typedTable = null;
        // Relations in the order they were created, and within one relation its columns from the
        // first: that is the order PostgreSQL walks the dependencies in, and it names the first it
        // finds. The relation is named bare, whatever schema it lives in.
        List<Object[]> found = new ArrayList<>();
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table t : schema.getTables().values()) {
                if (sameType(typeKey, t.getOfTypeName())) {
                    if (typedTable == null) typedTable = t;
                    continue;
                }
                int relOid = executor.systemCatalog.getOid(
                        "rel:" + schema.getName() + "." + t.getName());
                List<Column> cols = t.getColumns();
                for (int i = 0; i < cols.size(); i++) {
                    Column c = cols.get(i);
                    if (columnHoldsOneOf(holding, c)) {
                        found.add(new Object[]{Integer.valueOf(relOid), Integer.valueOf(i),
                                t.getName() + "." + c.getName()});
                    }
                }
            }
        }
        if (!found.isEmpty()) {
            java.util.Collections.sort(found, new java.util.Comparator<Object[]>() {
                @Override
                public int compare(Object[] a, Object[] b) {
                    int byRelation = Integer.compare((Integer) a[0], (Integer) b[0]);
                    return byRelation != 0 ? byRelation
                            : Integer.compare((Integer) a[1], (Integer) b[1]);
                }
            });
            throw new MemgresException("cannot alter type \"" + shown + "\" because column \""
                    + found.get(0)[2] + "\" uses it", "0A000");
        }
        if (typedTable != null && !cascade) {
            MemgresException e = new MemgresException("cannot alter type \"" + shown
                    + "\" because it is the type of a typed table", "2BP01");
            e.setHint("Use ALTER ... CASCADE to alter the typed tables too.");
            throw e;
        }
    }

    /**
     * Every type whose values contain one of this type: the type itself, any composite with an
     * attribute of one of them, and any domain built over one of them, followed round until
     * nothing new turns up. A column of any of them holds a value laid out by this type's
     * attributes, which is why PostgreSQL refuses the change for all of them alike.
     */
    private Set<String> typeKeysBuiltOn(String typeKey) {
        Set<String> keys = new LinkedHashSet<>();
        keys.add(typeKey);
        boolean grew = true;
        while (grew) {
            grew = false;
            for (Map.Entry<String, List<CreateTypeStmt.CompositeField>> e
                    : executor.database.getCompositeTypes().entrySet()) {
                if (keys.contains(e.getKey()) || e.getValue() == null) continue;
                for (CreateTypeStmt.CompositeField f : e.getValue()) {
                    if (keys.contains(TypeNamespace.find(executor.database.typeKeys(),
                            bareTypeName(f.typeName())))) {
                        keys.add(e.getKey());
                        grew = true;
                        break;
                    }
                }
            }
            for (Map.Entry<String, DomainType> e : executor.database.getDomains().entrySet()) {
                if (keys.contains(e.getKey())) continue;
                String base = e.getValue().getBaseTypeName();
                if (base != null && keys.contains(TypeNamespace.find(
                        executor.database.typeKeys(), bareTypeName(base)))) {
                    keys.add(e.getKey());
                    grew = true;
                }
            }
        }
        return keys;
    }

    /** Whether this column was declared with one of those types, as an array of one or not. */
    private boolean columnHoldsOneOf(Set<String> keys, Column c) {
        String declared = c.getCompositeTypeName() != null
                ? c.getCompositeTypeName() : c.getDomainTypeName();
        if (declared == null) return false;
        return keys.contains(TypeNamespace.find(executor.database.typeKeys(),
                bareTypeName(declared)));
    }

    /** A written type name with its modifier and its array brackets taken off. */
    private static String bareTypeName(String written) {
        if (written == null) return null;
        String t = written.trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        while (t.endsWith("[]")) t = t.substring(0, t.length() - 2).trim();
        return t;
    }

    /**
     * A type name is one name across enums, composites and domains, so a rename onto any of them
     * would leave two types answering to it and every stored value ambiguous.
     */
    private void requireTypeNameFree(String name) {
        if (name == null) return;
        String schemaName = TypeNamespace.writtenSchema(name) != null
                ? TypeNamespace.writtenSchema(name) : executor.defaultSchema();
        String bare = TypeNamespace.bare(name);
        Schema schema = executor.database.getSchema(schemaName);
        // A table carries a composite type of its own name, so it takes the name for types too.
        if (schema != null && schema.getTable(bare) != null) {
            throw new MemgresException("type \"" + bare + "\" already exists", "42710");
        }
        TypeNamespace.requireFree(executor.database, schemaName, bare);
    }

    /**
     * A composite type owns a pg_class row of its own, so renaming one is a relation rename: the
     * collision PostgreSQL reports first is with whatever else owns a relation of that name — a
     * table or another composite — and it reports it as a relation rather than as a type. Names
     * taken by an enum or a domain, which own no relation, still come back as a type collision.
     */
    private void requireCompositeRenameTargetFree(String name) {
        if (name == null) return;
        String schemaName = TypeNamespace.writtenSchema(name) != null
                ? TypeNamespace.writtenSchema(name) : executor.defaultSchema();
        String bare = TypeNamespace.bare(name);
        if (executor.database.getCompositeTypes().containsKey(TypeNamespace.key(schemaName, bare))
                || RelationNamespace.kindOf(executor.database, schemaName, bare) != null) {
            throw new MemgresException("relation \"" + bare + "\" already exists", "42P07");
        }
        requireTypeNameFree(name);
    }

    /** A destination schema that does not exist has nowhere to put the object. */
    void requireSchemaExists(String schemaName) {
        if (schemaName == null) return;
        if (executor.database.getSchema(schemaName) == null) {
            throw new MemgresException("schema \"" + schemaName + "\" does not exist", "3F000");
        }
    }

    /** A role that does not exist cannot be given anything to own. */
    void requireOwnerExists(String roleName) {
        if (roleName == null) return;
        String resolved = ddl.resolveOwnerName(roleName);
        if (!executor.database.hasRole(resolved)) {
            throw new MemgresException("role \"" + resolved + "\" does not exist", "42704");
        }
    }

    private static boolean hasAttribute(List<CreateTypeStmt.CompositeField> fields, String name) {
        if (name == null) return false;
        for (CreateTypeStmt.CompositeField f : fields) {
            if (f.name().equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    // ---- CREATE FUNCTION ----

    QueryResult executeCreateAggregate(CreateAggregateStmt stmt) {
        // An aggregate with no state function or no state type cannot compute anything
        if (stmt.sfunc() == null) {
            throw new MemgresException("aggregate sfunc must be specified", "42P13");
        }
        if (stmt.stype() == null) {
            throw new MemgresException("aggregate stype must be specified", "42P13");
        }

        // The transition function takes the running state plus the aggregated arguments; an
        // ordered-set aggregate's direct arguments go to the final function instead.
        List<String> transArgs = new ArrayList<>();
        transArgs.add(stmt.stype());
        transArgs.addAll(aggregatedArgTypes(stmt));
        requireFunctionSignature(stmt.sfunc(), transArgs);
        if (stmt.finalfunc() != null) {
            requireFunctionSignature(stmt.finalfunc(), Cols.listOf(stmt.stype()));
        }

        PgAggregate existing = executor.database.getAggregate(stmt.name());
        if (existing != null && aggregateArgsMatch(existing, stmt.argTypes())) {
            throw new MemgresException("function \"" + stmt.name()
                    + "\" already exists with same argument types", "42723");
        }

        PgAggregate agg = new PgAggregate(
                stmt.name(),
                stmt.sfunc(),
                stmt.stype(),
                stmt.initcond(),
                stmt.finalfunc(),
                stmt.combinefunc(),
                stmt.sortop(),
                stmt.argTypes() != null ? stmt.argTypes().toArray(new String[0]) : new String[0]
        );
        agg.setSchemaName(executor.defaultSchema());
        executor.database.addAggregate(agg);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    /** The argument types the transition function receives, i.e. everything after ORDER BY. */
    private static List<String> aggregatedArgTypes(CreateAggregateStmt stmt) {
        List<String> out = new ArrayList<>();
        if (stmt.argTypes() == null) return out;
        int from = stmt.directArgCount() >= 0 ? stmt.directArgCount() : 0;
        for (int i = from; i < stmt.argTypes().size(); i++) {
            String t = stmt.argTypes().get(i);
            if (!"*".equals(t)) out.add(t);
        }
        return out;
    }

    /**
     * Require a function of this name accepting exactly these argument types. Names that look
     * like PG's internal C functions are accepted unseen — memgres has no catalog of them.
     */
    private void requireFunctionSignature(String funcName, List<String> argTypes) {
        String bare = funcName.contains(".")
                ? funcName.substring(funcName.lastIndexOf('.') + 1) : funcName;
        List<PgFunction> overloads = executor.database.getFunctionOverloads(bare);
        if (overloads.isEmpty()) {
            if (isKnownBuiltinFunction(funcName)) return;
        } else {
            for (PgFunction f : overloads) {
                if (paramTypesMatch(f, argTypes)) return;
            }
        }
        throw new MemgresException(
                "function " + funcName + "(" + canonicalTypeList(argTypes) + ") does not exist",
                "42883").withoutHint();
    }

    /** True when the function's declared input parameters are exactly these types. */
    private static boolean paramTypesMatch(PgFunction f, List<String> argTypes) {
        List<String> declared = new ArrayList<>();
        for (PgFunction.Param p : f.getParams()) {
            if (!"OUT".equalsIgnoreCase(p.mode())) declared.add(DataType.canonicalName(p.typeName()));
        }
        if (declared.size() != argTypes.size()) return false;
        for (int i = 0; i < declared.size(); i++) {
            if (!declared.get(i).equals(DataType.canonicalName(argTypes.get(i)))) return false;
        }
        return true;
    }

    static String canonicalTypeList(List<String> types) {
        if (types == null || types.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String t : types) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(DataType.canonicalName(t));
        }
        return sb.toString();
    }

    /** True when the aggregate was declared with exactly these argument types. */
    static boolean aggregateArgsMatch(PgAggregate agg, List<String> argTypes) {
        String[] declared = agg.getArgTypes() != null ? agg.getArgTypes() : new String[0];
        List<String> wanted = argTypes != null ? argTypes : Cols.<String>listOf();
        if (declared.length != wanted.size()) return false;
        for (int i = 0; i < declared.length; i++) {
            if (!DataType.canonicalName(declared[i]).equals(DataType.canonicalName(wanted.get(i)))) return false;
        }
        return true;
    }

    // ---- CREATE/ALTER/DROP OPERATOR ----

    QueryResult executeCreateOperator(CreateOperatorStmt stmt) {
        // PG rule: multi-character operators ending with + or - must contain at least
        // one character from ~!@#%^&|`?\ (e.g., +++ is invalid)
        String opName = stmt.name();
        if (opName != null && opName.length() > 1) {
            char last = opName.charAt(opName.length() - 1);
            if (last == '+' || last == '-') {
                boolean hasSpecial = false;
                for (int i = 0; i < opName.length(); i++) {
                    if ("~!@#%^&|`?\\".indexOf(opName.charAt(i)) >= 0) { hasSpecial = true; break; }
                }
                if (!hasSpecial) {
                    throw new MemgresException(
                        "operator name \"" + opName + "\" is not valid: "
                        + "a symbol name ending in \"+\" or \"-\" must contain at least one "
                        + "character from ~!@#%^&|`?", "42601");
                }
            }
        }
        // Validate that at least one of LEFTARG/RIGHTARG is specified
        if (stmt.leftArg() == null && stmt.rightArg() == null) {
            throw new MemgresException(
                "operator argument types must be specified", "42P13");
        }
        // An operator with no function has nothing to evaluate
        if (stmt.function() == null) {
            throw new MemgresException("operator function must be specified", "42P13");
        }

        // Validate that the backing function exists (skip for well-known built-in PG functions)
        List<String> opArgs = new ArrayList<>();
        if (stmt.leftArg() != null) opArgs.add(stmt.leftArg());
        if (stmt.rightArg() != null) opArgs.add(stmt.rightArg());
        requireFunctionSignature(stmt.function(), opArgs);

        PgOperator op = new PgOperator(stmt.name(), stmt.leftArg(), stmt.rightArg(), stmt.function());
        op.setCommutator(stmt.commutator());
        op.setNegator(stmt.negator());
        op.setRestrict(stmt.restrict());
        op.setJoin(stmt.join());
        op.setHashes(stmt.hashes());
        op.setMerges(stmt.merges());
        if (stmt.schema() != null) {
            op.setSchemaName(stmt.schema());
        } else {
            // Use current default schema from search_path (PG creates in first writable schema)
            op.setSchemaName(executor.defaultSchema());
        }
        // Set owner to current user
        if (executor.session != null) {
            String role = executor.session.getGucSettings().get("role");
            op.setOwner(role != null ? role : "memgres");
        }

        // Check for duplicate
        if (executor.database.hasOperator(op.getKey())) {
            throw new MemgresException("operator " + stmt.name() + " already exists", "42710");
        }

        executor.database.addOperator(op);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeCreateOperatorFamily(CreateOperatorFamilyStmt stmt) {
        PgOperatorFamily fam = new PgOperatorFamily(stmt.name(), stmt.method());
        if (stmt.schema() != null) fam.setSchemaName(stmt.schema());
        if (executor.session != null) {
            String role = executor.session.getGucSettings().get("role");
            fam.setOwner(role != null ? role : "memgres");
        }

        // Check for duplicate
        if (executor.database.hasOperatorFamily(fam.getKey())) {
            throw new MemgresException("operator family \"" + stmt.name() + "\" for access method \""
                    + stmt.method() + "\" already exists", "42710");
        }

        executor.database.addOperatorFamily(fam);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeCreateOperatorClass(CreateOperatorClassStmt stmt) {
        PgOperatorClass cls = new PgOperatorClass(stmt.name(), stmt.forType(), stmt.method(), stmt.isDefault());
        if (stmt.schema() != null) cls.setSchemaName(stmt.schema());
        cls.setFamilyName(stmt.familyName());
        if (executor.session != null) {
            String role = executor.session.getGucSettings().get("role");
            cls.setOwner(role != null ? role : "memgres");
        }

        // Check for duplicate
        if (executor.database.hasOperatorClass(cls.getKey())) {
            throw new MemgresException("operator class \"" + stmt.name() + "\" for access method \""
                    + stmt.method() + "\" already exists", "42710");
        }

        executor.database.addOperatorClass(cls);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeAlterOperator(AlterOperatorStmt stmt) {
        switch (stmt.objectKind()) {
            case OPERATOR:
                return executeAlterOperatorObj(stmt);
            case OPERATOR_FAMILY:
                return executeAlterOperatorFamilyObj(stmt);
            case OPERATOR_CLASS:
                return executeAlterOperatorClassObj(stmt);
            default:
                return QueryResult.command(QueryResult.Type.SET, 0);
        }
    }

    private QueryResult executeAlterOperatorObj(AlterOperatorStmt stmt) {
        // Build key from schema + name + arg types
        String l = stmt.leftArg() != null ? stmt.leftArg().toLowerCase() : "NONE";
        String r = stmt.rightArg() != null ? stmt.rightArg().toLowerCase() : "NONE";
        String schema = "public";
        String opName = stmt.name();
        int dotIdx = opName.indexOf('.');
        if (dotIdx > 0) { schema = opName.substring(0, dotIdx); opName = opName.substring(dotIdx + 1); }
        String key = schema.toLowerCase() + "." + opName + "(" + l + "," + r + ")";

        PgOperator op = executor.database.getOperator(key);
        if (op == null) {
            // PostgreSQL names the operator the way a caller would have to write it — operand
            // types and all — because the same symbol over other types may well exist.
            StringBuilder sig = new StringBuilder();
            // A prefix operator is written NONE on the left and has no left operand to name.
            if (stmt.leftArg() != null && !"NONE".equalsIgnoreCase(stmt.leftArg().trim())) {
                sig.append(DataType.canonicalName(stmt.leftArg())).append(' ');
            }
            sig.append(stmt.name());
            if (stmt.rightArg() != null && !"NONE".equalsIgnoreCase(stmt.rightArg().trim())) {
                sig.append(' ').append(DataType.canonicalName(stmt.rightArg()));
            }
            throw new MemgresException("operator does not exist: " + sig, "42883").withoutHint();
        }

        switch (stmt.action()) {
            case OWNER_TO:
                requireOwnerExists(stmt.value());
                op.setOwner(stmt.value());
                break;
            case SET_SCHEMA:
                requireSchemaExists(stmt.value());
                op.setSchemaName(stmt.value());
                break;
            case SET_PROPERTIES:
                // Properties already consumed by parser; no further action needed
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    private QueryResult executeAlterOperatorFamilyObj(AlterOperatorStmt stmt) {
        String key = stmt.name().toLowerCase() + ":" + stmt.method().toLowerCase();
        PgOperatorFamily fam = executor.database.getOperatorFamily(key);
        if (fam == null && stmt.action() != AlterOperatorStmt.AlterAction.ADD_MEMBER
                && stmt.action() != AlterOperatorStmt.AlterAction.DROP_MEMBER) {
            throw new MemgresException("operator family \"" + stmt.name()
                    + "\" does not exist for access method \"" + stmt.method() + "\"", "42704");
        }
        if (fam == null) {
            // ADD/DROP MEMBER on non-existent family — just accept
            return QueryResult.command(QueryResult.Type.SET, 0);
        }

        switch (stmt.action()) {
            case OWNER_TO:
                fam.setOwner(stmt.value());
                break;
            case RENAME_TO:
                executor.database.removeOperatorFamily(key);
                fam.setName(stmt.value());
                executor.database.addOperatorFamily(fam);
                break;
            case SET_SCHEMA:
                fam.setSchemaName(stmt.value());
                break;
            case ADD_MEMBER:
            case DROP_MEMBER:
                // Members are tracked conceptually but we don't maintain a member list
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    private QueryResult executeAlterOperatorClassObj(AlterOperatorStmt stmt) {
        String key = stmt.name().toLowerCase() + ":" + stmt.method().toLowerCase();
        PgOperatorClass cls = executor.database.getOperatorClass(key);
        if (cls == null) {
            throw new MemgresException("operator class \"" + stmt.name()
                    + "\" does not exist for access method \"" + stmt.method() + "\"", "42704");
        }

        switch (stmt.action()) {
            case OWNER_TO:
                cls.setOwner(stmt.value());
                break;
            case RENAME_TO:
                executor.database.removeOperatorClass(key);
                cls.setName(stmt.value());
                executor.database.addOperatorClass(cls);
                break;
            case SET_SCHEMA:
                cls.setSchemaName(stmt.value());
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeCreateFunction(CreateFunctionStmt stmt) {
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        rejectOutParameterAfterDefault(stmt);
        if ("pg_catalog".equalsIgnoreCase(stmt.schema())) {
            throw new MemgresException("permission denied to create function in schema pg_catalog", "42501");
        }
        if (stmt.schema() == null) {
            String targetSchema = executor.defaultSchema();
            if ("pg_catalog".equals(targetSchema)) {
                throw new MemgresException("permission denied to create function in schema pg_catalog", "42501");
            }
            if ("information_schema".equals(targetSchema)) {
                throw new MemgresException("no schema has been selected to create in", "3F000");
            }
            if (executor.database.getSchema(targetSchema) == null) {
                throw new MemgresException("no schema has been selected to create in", "3F000");
            }
        }

        // A body written for a language nothing can run is stored but never callable
        if (stmt.language() != null && !INSTALLED_LANGUAGES.contains(stmt.language().toLowerCase())) {
            throw new MemgresException(
                    "language \"" + stmt.language().toLowerCase() + "\" does not exist", "42704");
        }

        // Only now: there is no point asking what a language is to run before knowing it is one,
        // which is why the missing body is reported here rather than where the statement was read.
        if (!stmt.bodyGiven) {
            throw new MemgresException("no function body specified", "42P13");
        }

        // Validate return type exists (PG validates at CREATE time)
        if (stmt.returnType() != null && !stmt.returnType().isEmpty()) {
            String retType = stmt.returnType();
            String baseRetType = retType;
            if (baseRetType.toUpperCase().startsWith("SETOF ")) {
                baseRetType = baseRetType.substring(6).trim();
            }
            validateTypeExists(baseRetType);
        }

        // SUPPORT clause: validate the support function exists (PG validates at CREATE time)
        if (stmt.supportFunction != null) {
            String supportFn = stmt.supportFunction;
            if (executor.database.getFunction(supportFn) == null
                    && executor.database.getFunction(supportFn.toLowerCase()) == null) {
                throw new MemgresException("function " + supportFn + " does not exist", "42883");
            }
        }

        List<PgFunction.Param> params = new ArrayList<>();
        if (stmt.parsedParams() != null) {
            for (CreateFunctionStmt.FuncParam fp : stmt.parsedParams()) {
                // Validate parameter types exist (PG validates at CREATE time)
                if (fp.typeName() != null) {
                    validateTypeExists(fp.typeName());
                }
                // Validate default expression function references
                if (fp.defaultExpr() != null) {
                    validateDefaultExpr(fp.defaultExpr());
                }
                // PL/pgSQL: reject sqlstate and sqlerrm as parameter names (they are implicit CONSTANT variables)
                if ("plpgsql".equalsIgnoreCase(stmt.language()) && fp.name() != null) {
                    String lowerName = fp.name().toLowerCase();
                    if ("sqlstate".equals(lowerName) || "sqlerrm".equals(lowerName)) {
                        throw new MemgresException(
                                "variable \"" + fp.name() + "\" is declared CONSTANT", "42601");
                    }
                }
                params.add(new PgFunction.Param(fp.name(), fp.typeName(), fp.mode(), fp.defaultExpr()));
            }
        }
        validateSignature(params);

        // A polymorphic result has to be determinable from a polymorphic argument of the same family.
        PolymorphicTypes.validateSignature(stmt.returnType(), params.stream()
                .map(PgFunction.Param::typeName)
                .collect(Collectors.toList()));

        // Validate PL/pgSQL declared variable types (PG validates at CREATE time when check_function_bodies=on)
        boolean checkBodies = executor.session == null || !"off".equalsIgnoreCase(
                executor.session.getGucSettings().get("check_function_bodies"));
        // Check for unsupported transaction commands (SAVEPOINT, ROLLBACK TO) that prevent procedure registration
        boolean hasUnsupportedTxnCmd = false;
        if (checkBodies && "plpgsql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            hasUnsupportedTxnCmd = containsUnsupportedTransactionCommand(stmt.body());
        }
        if (checkBodies && "plpgsql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            // PG compiles the body at CREATE time, so a body it cannot parse never becomes a
            // function that only fails when someone calls it.
            com.memgres.engine.plpgsql.PlpgsqlStatement.Block parsedBody =
                    com.memgres.engine.plpgsql.PlpgsqlParser.parse(stmt.body());
            List<String> paramNames = new ArrayList<>();
            boolean hasOutParams = false;
            for (int i = 0; i < params.size(); i++) {
                PgFunction.Param p = params.get(i);
                if (p.name() != null) paramNames.add(p.name());
                // Every parameter also answers to its position, which is the only name an
                // unnamed one has and what ALIAS FOR usually points at
                paramNames.add("$" + (i + 1));
                String mode = p.mode() == null ? "IN" : p.mode().toUpperCase();
                if ("OUT".equals(mode) || "INOUT".equals(mode)) hasOutParams = true;
            }
            PlpgsqlBodyValidator.Routine routine = new PlpgsqlBodyValidator.Routine(
                    stmt.isProcedure(), stmt.returnType(), hasOutParams);
            PlpgsqlBodyValidator.validate(executor, parsedBody, paramNames, routine);
        }

        // Validate SQL language function bodies (only when check_function_bodies=on)
        if (checkBodies && "sql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            validateSqlFunctionBody(stmt, params);
        }

        // Validate plpgsql function bodies (only when check_function_bodies=on)
        if (checkBodies && "plpgsql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            String retType = stmt.returnType();
            boolean needsReturnValue = retType != null && !retType.isEmpty()
                    && !"void".equalsIgnoreCase(retType) && !"trigger".equalsIgnoreCase(retType)
                    && !retType.toUpperCase().startsWith("SETOF") && !"TABLE".equalsIgnoreCase(retType);
            if (needsReturnValue) {
                java.util.regex.Matcher rm = java.util.regex.Pattern.compile("\\breturn\\s*;", java.util.regex.Pattern.CASE_INSENSITIVE).matcher(stmt.body());
                if (rm.find()) {
                    throw new MemgresException("RETURN must have a return value for function returning " + retType, "42601");
                }
            }
            // Try to parse SQL expressions inside the PL/pgSQL body (PG validates at creation time).
            // Specifically parse RETURN <expr> statements to catch syntax errors.
            // Skip RETURN QUERY ..., RETURN NEXT ..., and bare RETURN; statements.
            try {
                java.util.regex.Matcher retMatcher = java.util.regex.Pattern.compile(
                    "\\bRETURN\\s+(.+?)\\s*;", java.util.regex.Pattern.CASE_INSENSITIVE
                ).matcher(stmt.body());
                while (retMatcher.find()) {
                    String retExpr = retMatcher.group(1).trim();
                    if (!retExpr.isEmpty() && !retExpr.equalsIgnoreCase("NEXT")
                            && !retExpr.equalsIgnoreCase("QUERY")
                            && !retExpr.toUpperCase().startsWith("QUERY ")
                            && !retExpr.toUpperCase().startsWith("NEXT ")) {
                        // Try parsing as a SELECT expression
                        com.memgres.engine.parser.Parser.parse("SELECT " + retExpr);
                    }
                }
            } catch (MemgresException e) {
                if ("42601".equals(e.getSqlState())) throw e;
                // Ignore non-syntax errors
            }
        }

        String funcSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();

        // Check for duplicate function. Functions are a per-schema namespace, so a same-named
        // function with the same argument types in another schema is a different function.
        List<String> newParamTypes = params.stream()
                .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                .map(PgFunction.Param::typeName)
                .collect(Collectors.toList());
        // What CREATE OR REPLACE displaces has to be kept: rolling the statement back restores the
        // definition that was there, and dropping it instead left no routine at all.
        PgFunction replaced = null;
        List<PgFunction> existingOverloads = executor.database.getFunctionOverloads(funcSchema, stmt.name());
        for (PgFunction existing : existingOverloads) {
            List<String> existingTypes = existing.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(Collectors.toList());
            if (existingTypes.size() == newParamTypes.size()) {
                // What identifies a function is the argument types themselves, not the words they
                // were written with: int, int4 and integer are one type, and a length or a
                // precision is not part of the type at all. Comparing the spellings would let
                // f(int) and f(int4) both exist, and then no call could choose between them.
                boolean sameTypes = true;
                for (int i = 0; i < existingTypes.size(); i++) {
                    if (!sameTypeName(existingTypes.get(i), newParamTypes.get(i))) {
                        sameTypes = false;
                        break;
                    }
                }
                if (sameTypes) {
                    if (stmt.orReplace()) {
                        checkReplaceKeepsSignature(existing, stmt, params);
                        replaced = existing;
                        executor.database.removeFunction(funcSchema, stmt.name(), existingTypes);
                        break;
                    }
                    throw new MemgresException("function \"" + stmt.name() + "\" already exists with same argument types", "42723");
                }
            }
        }

        // If the body contains unsupported transaction commands (SAVEPOINT, ROLLBACK TO SAVEPOINT),
        // PG rejects the function at creation time. We silently skip registration so CALL gets 42883.
        if (hasUnsupportedTxnCmd) {
            return QueryResult.command(QueryResult.Type.CREATE_FUNCTION, 0);
        }

        PgFunction pgFunc = new PgFunction(stmt.name(), stmt.returnType(), stmt.body(),
                stmt.language(), params, stmt.isProcedure());
        pgFunc.setSchemaName(funcSchema);
        pgFunc.setSecurityDefiner(stmt.securityDefiner());
        pgFunc.setStrict(stmt.strict());
        pgFunc.setLeakproof(stmt.leakproof());
        pgFunc.setWindowFunction(stmt.windowFunction);
        pgFunc.setVolatility(stmt.volatility());
        pgFunc.setSetClauses(stmt.setClauses());
        pgFunc.setOwner(executor.sessionUser());
        pgFunc.setAtomicBody(stmt.atomicBody);
        if (stmt.parallel() != null) pgFunc.setParallel(stmt.parallel());
        // A cost nobody wrote is the language's own: 1 for the languages whose calls are compiled
        // in, 100 for an interpreted one. Taking 100 for all of them made every SQL function claim
        // to be a hundred times the work it is, which is a number a planner reads.
        if (stmt.cost() >= 0) pgFunc.setCost(stmt.cost());
        else pgFunc.setCost(defaultCostForLanguage(stmt.language()));
        if (stmt.rows() >= 0) pgFunc.setRows(stmt.rows());
        executor.database.addFunction(pgFunc);
        executor.database.registerSchemaObject(funcSchema, "function", stmt.name());
        executor.database.setObjectOwner("function:" + stmt.name(), executor.sessionUser());
        // A routine is identified by its schema and its argument types, so that is what the undo
        // holds: rolling back one added overload must not take the other overloads of that name,
        // nor the same name in another schema, with it.
        executor.recordUndo(new Session.CreateFunctionUndo(funcSchema, stmt.name(),
                newParamTypes, replaced));
        return QueryResult.command(QueryResult.Type.CREATE_FUNCTION, 0);
    }

    /**
     * What a routine costs when its definition did not say. PostgreSQL charges 1 for the languages
     * whose calls are compiled in and 100 for every other, SQL included.
     */
    static double defaultCostForLanguage(String language) {
        return "internal".equalsIgnoreCase(language) || "c".equalsIgnoreCase(language) ? 1 : 100;
    }

    /** The languages a stock PostgreSQL has; memgres runs sql and plpgsql bodies itself. */
    private static final Set<String> INSTALLED_LANGUAGES =
            Cols.setOf("sql", "plpgsql", "c", "internal");

    /**
     * CREATE OR REPLACE keeps the identity of the existing routine, so callers compiled against
     * its kind, its result type, its parameter names and the arguments they were allowed to omit
     * stay valid; changing any of those is a different routine, and PostgreSQL makes you drop the
     * old one first. The checks run in PostgreSQL's own order, because a definition wrong in two
     * ways is reported by the first thing wrong with it.
     */
    private void checkReplaceKeepsSignature(PgFunction existing, CreateFunctionStmt stmt,
                                            List<PgFunction.Param> params) {
        try {
            checkReplaceKeepsIdentity(existing, stmt, params);
        } catch (MemgresException e) {
            // Every one of these refusals has the same way out, and PostgreSQL writes it out
            // rather than leaving the reader to work out the signature a DROP has to name.
            if ("42P13".equals(e.getSqlState())) {
                e.setHint("Use DROP " + (existing.isProcedure() ? "PROCEDURE" : "FUNCTION") + " "
                        + existing.getName() + "(" + identityArguments(existing.getParams())
                        + ") first.");
            }
            throw e;
        }
    }

    /** The arguments a DROP of this routine has to name: its input parameters' types, in order. */
    private static String identityArguments(List<PgFunction.Param> params) {
        StringBuilder sb = new StringBuilder();
        for (PgFunction.Param p : inParams(params)) {
            if (sb.length() > 0) sb.append(',');
            sb.append(DataType.canonicalName(p.typeName()));
        }
        return sb.toString();
    }

    private void checkReplaceKeepsIdentity(PgFunction existing, CreateFunctionStmt stmt,
                                           List<PgFunction.Param> params) {
        // A function is SELECTed and a procedure is CALLed, so one never silently becomes the
        // other. PostgreSQL calls this the wrong kind of object rather than a bad definition.
        if (existing.isProcedure() != stmt.isProcedure()) {
            MemgresException e = new MemgresException("cannot change routine kind", "42809");
            e.setDetail("\"" + stmt.name() + "\" is a "
                    + (existing.isProcedure() ? "procedure" : "function") + ".");
            throw e;
        }

        List<PgFunction.Param> oldOut = outParams(existing.getParams());
        List<PgFunction.Param> newOut = outParams(params);
        if (existing.isProcedure()) {
            // A procedure yields no value, so all a caller sees is whether it hands anything back
            // through parameters — and if it does, what that row looks like, one column or many.
            if (oldOut.isEmpty() != newOut.isEmpty()) {
                throw new MemgresException(
                        "cannot change whether a procedure has output parameters", "42P13");
            }
            if (!oldOut.isEmpty()) rejectChangedOutputRow(oldOut, newOut);
        } else {
            // What a call yields is not always what RETURNS says: a lone output parameter carries
            // the result itself, so IN a int RETURNS int and INOUT a int are the same function
            // seen from outside, while several output parameters make a row type whose column
            // names are part of it.
            String oldRet = effectiveReturnType(existing.getParams(), existing.getReturnType());
            String newRet = effectiveReturnType(params, stmt.returnType());
            if (!oldRet.equals(newRet)) {
                throw new MemgresException("cannot change return type of existing function", "42P13");
            }
            if (oldOut.size() > 1 || newOut.size() > 1) rejectChangedOutputRow(oldOut, newOut);
        }

        List<PgFunction.Param> oldIn = inParams(existing.getParams());
        List<PgFunction.Param> newIn = inParams(params);
        for (int i = 0; i < oldIn.size() && i < newIn.size(); i++) {
            String oldName = oldIn.get(i).name();
            if (oldName == null || oldName.isEmpty()) continue;
            if (!sameParamName(oldName, newIn.get(i).name())) {
                throw new MemgresException(
                        "cannot change name of input parameter \"" + oldName + "\"", "42P13");
            }
        }

        // A default is part of the call signature: a caller that omitted the argument would stop
        // resolving if the replacement took the default away. Adding defaults is safe and allowed,
        // so it is only a shrinking count that PostgreSQL refuses.
        if (countDefaults(params) < countDefaults(existing.getParams())) {
            throw new MemgresException(
                    "cannot remove parameter defaults from existing function", "42P13");
        }
    }

    /**
     * The row a set of output parameters defines is its columns in order, names included, so a
     * renamed or retyped output column is a different result type even where the count agrees.
     */
    private static void rejectChangedOutputRow(List<PgFunction.Param> oldOut,
                                               List<PgFunction.Param> newOut) {
        boolean changed = oldOut.size() != newOut.size();
        for (int i = 0; !changed && i < oldOut.size(); i++) {
            changed = !sameParamName(oldOut.get(i).name(), newOut.get(i).name())
                    || !sameTypeName(oldOut.get(i).typeName(), newOut.get(i).typeName());
        }
        if (changed) {
            MemgresException e = new MemgresException(
                    "cannot change return type of existing function", "42P13");
            e.setDetail("Row type defined by OUT parameters is different.");
            throw e;
        }
    }

    /**
     * The type a call of this routine yields, as a caller sees it. Output parameters, which the
     * RETURNS TABLE columns also are, override the declared return type: exactly one of them is
     * the result, several of them make an anonymous row.
     */
    private static String effectiveReturnType(List<PgFunction.Param> params, String declared) {
        String decl = declared == null ? "" : declared.trim();
        boolean table = decl.equalsIgnoreCase("TABLE");
        boolean setOf = table || decl.regionMatches(true, 0, "SETOF ", 0, 6);
        List<PgFunction.Param> outs = outParams(params);
        String base;
        if (outs.size() == 1) {
            base = DataType.canonicalName(outs.get(0).typeName());
        } else if (outs.size() > 1) {
            base = "record";
        } else {
            String bare = table ? "" : (setOf ? decl.substring(6).trim() : decl);
            base = bare.isEmpty() ? "void" : DataType.canonicalName(bare);
        }
        return (setOf ? "setof " : "") + base;
    }

    /** How many parameters may be left out of a call, which is how many carry a default. */
    /**
     * How many of these parameters carry a default.
     *
     * <p>The parser records no default as null and a written one as its text, so a default whose
     * text is empty -- {@code DEFAULT $q$$q$}, {@code DEFAULT B''}, {@code DEFAULT X''} -- is still
     * a default. Reading an empty text as an absent default made replacing a function with one of
     * those look like taking a default away.
     */
    private static int countDefaults(List<PgFunction.Param> params) {
        int n = 0;
        for (PgFunction.Param p : params) {
            if (p.defaultExpr() != null) n++;
        }
        return n;
    }

    private static boolean sameTypeName(String a, String b) {
        String ca = a == null ? "" : DataType.canonicalName(withoutModifier(a));
        String cb = b == null ? "" : DataType.canonicalName(withoutModifier(b));
        return ca == null ? cb == null : ca.equals(cb);
    }

    /**
     * A type name with the length or precision taken off it.
     *
     * <p>PostgreSQL records a parameter's type and not its modifier, so {@code timestamp(6) with
     * time zone} and {@code timestamptz} are one type and a function declared over both is declared
     * twice. Canonicalising the written name whole did not see that: the modifier sits in the
     * middle of the multi-word spellings, so everything after it was dropped and
     * {@code timestamp(6) with time zone} was read as a bare timestamp.
     *
     * <p>{@code float(n)} is the exception PostgreSQL's grammar makes: it is not a modifier at all
     * but a choice of type, real up to 24 and double precision beyond, so it is resolved rather
     * than removed.
     */
    private static String withoutModifier(String typeName) {
        String lower = typeName.trim().toLowerCase(java.util.Locale.ROOT);
        java.util.regex.Matcher f =
                java.util.regex.Pattern.compile("^float\\s*\\(\\s*(\\d+)\\s*\\)(.*)$").matcher(lower);
        if (f.matches()) {
            int bits = Integer.parseInt(f.group(1));
            return (bits <= 24 ? "real" : "double precision") + f.group(2);
        }
        return lower.replaceAll("\\(\\s*[^()]*\\)", "").replaceAll("\\s+", " ").trim();
    }

    private static boolean sameParamName(String a, String b) {
        if (a == null || a.isEmpty()) return b == null || b.isEmpty();
        return a.equalsIgnoreCase(b);
    }

    private static List<PgFunction.Param> outParams(List<PgFunction.Param> params) {
        List<PgFunction.Param> out = new ArrayList<>();
        for (PgFunction.Param p : params) {
            if ("OUT".equalsIgnoreCase(p.mode()) || "INOUT".equalsIgnoreCase(p.mode())) out.add(p);
        }
        return out;
    }

    private static List<PgFunction.Param> inParams(List<PgFunction.Param> params) {
        List<PgFunction.Param> out = new ArrayList<>();
        for (PgFunction.Param p : params) {
            if (!"OUT".equalsIgnoreCase(p.mode())) out.add(p);
        }
        return out;
    }

    private void validateSqlFunctionBody(CreateFunctionStmt stmt, List<PgFunction.Param> params) {
        try {
            // Validate type casts, function calls, and sequences in SQL body text
            validateSqlBodyReferences(stmt.body());
            // Validate collation references in SQL body (PG validates eagerly at CREATE time)
            validateSqlBodyCollations(stmt.body());
            List<String> bodyStmts = splitSqlStatements(stmt.body());
            for (String bodyStr : bodyStmts) {
                Statement parsed = com.memgres.engine.parser.Parser.parse(bodyStr);
                validateSqlFunctionStatement(parsed, stmt, params);
            }
        } catch (MemgresException e) {
            if ("42601".equals(e.getSqlState()) && stmt.body() != null) {
                String bodyTrimmed = stmt.body().trim().replaceAll(";\\s*$", "").trim();
                if (bodyTrimmed.equalsIgnoreCase("SELECT")) {
                    throw new MemgresException(
                            "return type mismatch in function declared to return " + (stmt.returnType() != null ? stmt.returnType() : "unknown")
                            + "\n  Detail: Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
                            "42P13");
                }
            }
            throw e;
        } catch (Exception e) {
            String msg = e.getMessage();
            throw new MemgresException(msg != null ? msg : "syntax error in function body", "42601");
        }
    }

    /**
     * Validate collation references in SQL function body.
     * PG eagerly validates COLLATE names at CREATE FUNCTION time for SQL-language functions.
     */
    private void validateSqlBodyCollations(String body) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\\bCOLLATE\\s+\"?([\\w.]+)\"?", java.util.regex.Pattern.CASE_INSENSITIVE).matcher(body);
        while (m.find()) {
            String collation = m.group(1).toLowerCase();
            if (collation.equals("c") || collation.equals("posix") || collation.equals("default")
                    || collation.equals("ucs_basic") || collation.equals("unicode")
                    || collation.equals("icu_root") || collation.equals("pg_c_utf8")
                    || collation.startsWith("c.") || collation.startsWith("pg_catalog.")) {
                continue;
            }
            if (executor.database.getCollation(collation) != null) continue;
            throw new MemgresException("collation \"" + m.group(1) + "\" for encoding \"UTF8\" does not exist", "42704");
        }
    }

    /**
     * Validate references in SQL function body text: type casts, function calls, sequences.
     * SQL-language functions in PG eagerly validate all object references at CREATE time.
     */
    private void validateSqlBodyReferences(String body) {
        // Check type casts: ::type_name
        java.util.regex.Matcher castMatcher = java.util.regex.Pattern.compile(
                "::\\s*([a-zA-Z_][a-zA-Z0-9_]*)").matcher(body);
        while (castMatcher.find()) {
            String typeName = castMatcher.group(1);
            if (!isKnownType(typeName)) {
                throw new MemgresException("type \"" + typeName + "\" does not exist", "42704");
            }
        }
        // Check function calls: name(...) that aren't table refs like INSERT INTO table(col)
        java.util.regex.Matcher fnMatcher = java.util.regex.Pattern.compile(
                "(?:^|\\s)([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(").matcher(body);
        while (fnMatcher.find()) {
            String fnName = fnMatcher.group(1).toLowerCase();
            if (isKnownSqlKeyword(fnName)) continue;
            if (isBuiltinFunction(fnName)) continue;
            // Skip if preceded by INTO, FROM, TABLE, UPDATE, JOIN (table reference, not function call)
            // Also skip if preceded by ')' which indicates a table alias like ') t(col)'
            int start = fnMatcher.start(1);
            String before = body.substring(0, start).trim().toLowerCase();
            if (before.endsWith("into") || before.endsWith("from") || before.endsWith("table")
                    || before.endsWith("update") || before.endsWith("join")
                    || before.endsWith("on") || before.endsWith(")")) continue;
            if (executor.database.getFunction(fnName) == null) {
                throw new MemgresException("function " + fnName + "() does not exist", "42883");
            }
        }
        // Check sequence references: nextval('seq_name'), currval('seq_name'), setval('seq_name', ...)
        java.util.regex.Matcher seqMatcher = java.util.regex.Pattern.compile(
                "(?:nextval|currval|setval)\\s*\\(\\s*'([^']+)'").matcher(body);
        while (seqMatcher.find()) {
            String seqName = seqMatcher.group(1);
            if (executor.database.getSequence(seqName) == null) {
                throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
            }
        }
    }

    private boolean isKnownType(String typeName) {
        if (BUILTIN_TYPES.contains(typeName.toLowerCase())) return true;
        if (DataType.fromPgName(typeName) != null) return true;
        if (executor.database.isCustomEnum(typeName)) return true;
        if (executor.database.isDomain(typeName)) return true;
        if (executor.database.isCompositeType(typeName)) return true;
        return false;
    }

    private static boolean isKnownSqlKeyword(String name) {
        return Cols.setOf("select", "from", "where", "insert", "update", "delete",
                "values", "into", "set", "create", "alter", "drop", "table",
                "index", "view", "function", "procedure", "trigger",
                "if", "case", "when", "then", "else", "end", "and", "or", "not",
                "in", "exists", "between", "like", "is", "as", "on", "join",
                "left", "right", "inner", "outer", "cross", "full",
                "group", "order", "having", "limit", "offset", "union",
                "except", "intersect", "with", "returning", "row",
                "over", "partition", "rows", "range", "groups", "filter",
                "within", "window", "lateral", "distinct", "all", "any",
                "some", "array", "default", "null", "true", "false",
                // The words that stand before a parenthesis without naming a function
                "grouping", "sets", "cube", "rollup", "tablesample", "ordinality",
                "asc", "desc", "nulls", "first", "last", "fetch", "next",
                "coalesce", "greatest", "least", "cast",
                "count", "sum", "avg", "min", "max",
                "begin", "do", "call", "perform", "return").contains(name);
    }

    private void validateSqlFunctionStatement(Statement parsed, CreateFunctionStmt stmt,
                                               List<PgFunction.Param> params) {
        String retType = stmt.returnType();

        if (parsed instanceof SelectStmt
                && (((SelectStmt) parsed).targets() == null || ((SelectStmt) parsed).targets().isEmpty())
                && (((SelectStmt) parsed).from() == null || ((SelectStmt) parsed).from().isEmpty())) {
            SelectStmt sel = (SelectStmt) parsed;
            throw new MemgresException(
                    "return type mismatch in function declared to return " + (retType != null ? retType : "integer")
                    + "\n  Detail: Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
                    "42P13");
        }

        if (parsed instanceof SelectStmt
                && ((SelectStmt) parsed).targets() != null && !((SelectStmt) parsed).targets().isEmpty()
                && retType != null && !retType.isEmpty()
                && !"void".equalsIgnoreCase(retType)
                && !retType.toUpperCase().startsWith("SETOF")) {
            SelectStmt sel = (SelectStmt) parsed;
            Expression firstExpr = sel.targets().get(0).expr();
            if (firstExpr instanceof Literal
                    && ((Literal) firstExpr).literalType() == Literal.LiteralType.STRING) {
                Literal lit = (Literal) firstExpr;
                if (isNumericType(retType)) {
                    throw new MemgresException(
                            "return type mismatch in function declared to return " + retType
                                    + "\n  Detail: Actual return type is text.", "42P13");
                }
            }
            if (firstExpr instanceof CastExpr) {
                CastExpr castExpr = (CastExpr) firstExpr;
                checkCastReturnTypeMismatch(castExpr.typeName(), retType);
            }
        }

        // Recurse into UNION/INTERSECT/EXCEPT sub-statements
        if (parsed instanceof com.memgres.engine.parser.ast.SetOpStmt) {
            com.memgres.engine.parser.ast.SetOpStmt setOp = (com.memgres.engine.parser.ast.SetOpStmt) parsed;
            validateSqlFunctionStatement(setOp.left(), stmt, params);
            validateSqlFunctionStatement(setOp.right(), stmt, params);
            return;
        }

        validateTableRefsInStatement(parsed);

        // A SQL function's body is analysed when the function is written, not when it is called,
        // so a clause that cannot hold an aggregate or a window call is judged here — the same
        // judgement a view body gets, and for the same reason: storing it only defers the error.
        // Clause by clause in the order PostgreSQL reads them, so a body wrong in two places is
        // refused for the same one.
        if (parsed instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) parsed;
            PlacementCheck placement = executor.selectExecutor.placementCheck;
            placement.rejectWindowCallWithoutOverInTargets(sel);
            placement.reject(sel.where(), "WHERE");
            placement.rejectWindowCallWithoutOverAfterWhere(sel);
            // And what the query groups by, which needs the relations the FROM names.
            executor.selectExecutor.validateStoredQueryGrouping(sel);
        }

        if (parsed instanceof SelectStmt && ((SelectStmt) parsed).from() != null) {
            SelectStmt sel = (SelectStmt) parsed;
            Set<String> bodyCtes = new HashSet<>();
            if (sel.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                    bodyCtes.add(cte.name.toLowerCase(java.util.Locale.ROOT));
                }
            }
            for (SelectStmt.FromItem fromItem : sel.from()) {
                if (fromItem instanceof SelectStmt.TableRef) {
                    SelectStmt.TableRef tr = (SelectStmt.TableRef) fromItem;
                    // A CTE the body defines and a catalog relation are both relations this
                    // query may read; neither is a table the schema answers for.
                    if (tr.schema() == null && tr.table() != null
                            && bodyCtes.contains(tr.table().toLowerCase(java.util.Locale.ROOT))) {
                        continue;
                    }
                    if (SystemCatalog.isSystemCatalog(tr.schema(), tr.table())) continue;
                    try {
                        String trSchema = tr.schema() != null ? tr.schema() : "public";
                        Table t = executor.resolveTable(trSchema, tr.table());
                        for (SelectStmt.SelectTarget target : sel.targets()) {
                            if (target.expr() instanceof ColumnRef
                                    && ((ColumnRef) target.expr()).table() == null && !"*".equals(((ColumnRef) target.expr()).column())) {
                                ColumnRef cr = (ColumnRef) target.expr();
                                boolean isParam = params.stream().anyMatch(p -> p.name() != null && p.name().equalsIgnoreCase(cr.column()));
                                if (!isParam && t.getColumnIndex(cr.column()) < 0) {
                                    throw new MemgresException("column \"" + cr.column() + "\" does not exist", "42703");
                                }
                            }
                        }
                    } catch (MemgresException me) {
                        if ("42703".equals(me.getSqlState()) || "42P01".equals(me.getSqlState())) throw me;
                    }
                }
            }
        }
    }

    private void validateTableRefsInStatement(Statement parsed) {
        if (parsed instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) parsed;
            resolveTableIfPresent(ins.schema(), ins.table());
            // Validate subquery in INSERT ... SELECT
            if (ins.selectStmt() != null) {
                validateTableRefsInStatement(ins.selectStmt());
            }
        } else if (parsed instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) parsed;
            resolveTableIfPresent(upd.schema(), upd.table());
            if (upd.from() != null) {
                for (SelectStmt.FromItem fi : upd.from()) {
                    validateFromItem(fi);
                }
            }
        } else if (parsed instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) parsed;
            resolveTableIfPresent(del.schema(), del.table());
            if (del.using() != null) {
                for (SelectStmt.FromItem fi : del.using()) {
                    validateFromItem(fi);
                }
            }
        } else if (parsed instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) parsed;
            // A name the body's own WITH clause defines is a relation of this query, not one the
            // schema has to answer for. What the CTE itself reads still is: a body that defines
            // one over a table that does not exist is refused for that table.
            Set<String> cteNames = collectCteNames(sel);
            if (sel.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                    validateCteBody(cte.query, cteNames);
                }
            }
            if (sel.from() != null) {
                for (SelectStmt.FromItem fi : sel.from()) {
                    validateFromItem(fi, cteNames);
                }
            }
        }
    }

    private static Set<String> collectCteNames(SelectStmt sel) {
        Set<String> names = new HashSet<>();
        if (sel.withClauses() != null) {
            for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                if (cte.name != null) names.add(cte.name.toLowerCase(java.util.Locale.ROOT));
            }
        }
        return names;
    }

    /** A CTE's own body, with the WITH clause's names in scope so a self-reference is not a miss. */
    private void validateCteBody(Statement body, Set<String> inScope) {
        if (!(body instanceof SelectStmt)) {
            validateTableRefsInStatement(body);
            return;
        }
        SelectStmt sel = (SelectStmt) body;
        Set<String> names = new HashSet<>(inScope);
        names.addAll(collectCteNames(sel));
        if (sel.withClauses() != null) {
            for (SelectStmt.CommonTableExpr nested : sel.withClauses()) {
                validateCteBody(nested.query, names);
            }
        }
        if (sel.from() != null) {
            for (SelectStmt.FromItem fi : sel.from()) validateFromItem(fi, names);
        }
    }

    private void resolveTableIfPresent(String schema, String tableName) {
        if (tableName != null) {
            String s = schema != null ? schema : executor.defaultSchema();
            // A catalog relation is answered by the catalog rather than by a schema's table map,
            // so asking the schema for one would refuse a body that reads pg_class.
            if (SystemCatalog.isSystemCatalog(schema, tableName)) return;
            try {
                executor.resolveTable(s, tableName);
            } catch (MemgresException e) {
                if ("42P01".equals(e.getSqlState())) throw e;
            }
        }
    }

    private void validateFromItem(SelectStmt.FromItem fi) {
        validateFromItem(fi, java.util.Collections.<String>emptySet());
    }

    private void validateFromItem(SelectStmt.FromItem fi, Set<String> cteNames) {
        if (fi instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
            if (tr.schema() == null && tr.table() != null
                    && cteNames.contains(tr.table().toLowerCase(java.util.Locale.ROOT))) {
                return;
            }
            resolveTableIfPresent(tr.schema(), tr.table());
        } else if (fi instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) fi;
            validateFromItem(join.left(), cteNames);
            validateFromItem(join.right(), cteNames);
        } else if (fi instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) fi;
            validateTableRefsInStatement(sub.subquery());
        }
    }

    private static final Set<String> BUILTIN_TYPES = Cols.setOf(
            "void", "trigger", "record", "anyelement", "anyarray", "anynonarray", "anyenum",
            "anyrange", "anymultirange", "anycompatible", "anycompatiblearray",
            "anycompatiblenonarray", "anycompatiblerange", "cstring", "internal",
            "opaque", "refcursor", "unknown", "event_trigger",
            "int", "int2", "int4", "int8", "integer", "bigint", "smallint",
            "serial", "bigserial", "smallserial",
            "numeric", "decimal", "real", "float", "float4", "float8",
            "double precision", "money",
            "text", "varchar", "character varying", "char", "character", "bpchar", "name",
            "boolean", "bool",
            "bytea", "uuid", "json", "jsonb", "xml",
            "date", "time", "timestamp", "timestamptz",
            "timestamp with time zone", "timestamp without time zone",
            "time with time zone", "time without time zone",
            "interval",
            "inet", "cidr", "macaddr", "macaddr8",
            "bit", "varbit", "bit varying",
            "tsvector", "tsquery",
            "point", "line", "lseg", "box", "path", "polygon", "circle",
            "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
            "int4multirange", "int8multirange", "nummultirange", "datemultirange",
            "tsmultirange", "tstzmultirange",
            "oid", "regproc", "regtype", "regclass", "regoper", "regprocedure",
            "regoperator", "regconfig", "regdictionary", "regnamespace", "regrole",
            "pg_lsn", "pg_snapshot", "txid_snapshot", "xid", "xid8", "cid", "tid",
            "aclitem");

    /**
     * Validate that a type name exists (built-in, enum, domain, composite, or table-as-type).
     * Used for validating return types and parameter types at CREATE FUNCTION time.
     */
    private void validateTypeExists(String typeName) {
        validateTypeExists(executor, typeName);
    }

    /** Shared with the PL/pgSQL body validator, which checks declared variable types the same way. */
    static void validateTypeExists(AstExecutor executor, String typeName) {
        if (typeName == null || typeName.isEmpty()) return;
        String base = typeName.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        if (base.isEmpty()) return;
        // TABLE return type with column list is validated separately
        if (base.equalsIgnoreCase("TABLE")) return;
        if (BUILTIN_TYPES.contains(base.toLowerCase())) return;
        if (DataType.fromPgName(base) != null) return;
        if (executor.database.isCustomEnum(base)) return;
        if (executor.database.isDomain(base)) return;
        if (executor.database.isCompositeType(base)) return;
        // Check if it's a table used as a composite type
        String schema = executor.defaultSchema();
        if (schema != null) {
            Schema s = executor.database.getSchema(schema);
            if (s != null && s.getTable(base) != null) return;
        }
        // Also check public schema
        Schema pub = executor.database.getSchema("public");
        if (pub != null && pub.getTable(base) != null) return;
        throw new MemgresException("type \"" + base + "\" does not exist", "42704");
    }

    /**
     * Validate default expression for function parameters.
     * Checks that function calls in defaults reference existing functions.
     */
    private void validateDefaultExpr(String defaultExpr) {
        if (defaultExpr == null) return;
        // Check for function call pattern: name()
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(").matcher(defaultExpr);
        while (m.find()) {
            String fnName = m.group(1).toLowerCase();
            // Skip built-in functions
            if (isBuiltinFunction(fnName)) continue;
            if (executor.database.getFunction(fnName) == null) {
                throw new MemgresException("function " + fnName + "() does not exist", "42883");
            }
        }
    }

    /** Check if function name looks like a known PG internal C function (used in CREATE OPERATOR/AGGREGATE). */
    private static boolean isKnownBuiltinFunction(String rawName) {
        String name = rawName.contains(".") ? rawName.substring(rawName.lastIndexOf('.') + 1) : rawName;
        // PG internal C functions often follow patterns: int4pl, int4eq, float8lt, texteq, etc.
        if (name.matches("(int[248]|float[48]|numeric|text|bool|date|timestamp|interval|oid|name|char|varchar|bytea|uuid|json|jsonb|box|point|circle|path|line|lseg|polygon|inet|macaddr|bit|varbit|cash|bpchar)\\w+")) return true;
        // Common aggregate transition/combine/final functions
        if (name.matches("(hash|btree|gin|gist|brin|spg|pg_)\\w+")) return true;
        if (name.matches("\\w+(recv|send|in|out|typmod|analyze|cmp|hash|eq|ne|lt|le|gt|ge|_agg|_accum|_combine|_final|_serialize|_deserialize|_transition)")) return true;
        return false;
    }

    private static boolean isBuiltinFunction(String name) {
        // Common built-in functions that don't need validation
        return Cols.setOf("now", "current_timestamp", "current_date", "current_time",
                "current_user", "session_user", "localtime", "localtimestamp",
                "clock_timestamp", "statement_timestamp", "transaction_timestamp",
                "gen_random_uuid", "random", "nextval", "currval", "setval",
                "coalesce", "nullif", "greatest", "least", "cast",
                "array_agg", "string_agg", "count", "sum", "avg", "min", "max",
                "row_number", "rank", "dense_rank", "lag", "lead",
                "upper", "lower", "trim", "btrim", "ltrim", "rtrim",
                "length", "char_length", "octet_length", "bit_length",
                "substring", "position", "overlay", "replace", "translate",
                "concat", "concat_ws", "format", "quote_ident", "quote_literal",
                "quote_nullable", "regexp_match", "regexp_matches", "regexp_replace",
                "to_char", "to_number", "to_date", "to_timestamp",
                "abs", "ceil", "ceiling", "floor", "round", "trunc", "sign", "sqrt",
                "power", "exp", "ln", "log", "mod", "div",
                "array_length", "array_upper", "array_lower", "unnest",
                "array_append", "array_prepend", "array_cat", "array_remove",
                "array_to_string", "string_to_array", "array_position", "array_positions",
                "array_ndims", "array_dims", "array_fill", "array_replace",
                "current_setting", "set_config",
                "split_part", "left", "right", "repeat", "reverse", "lpad", "rpad",
                "starts_with", "encode", "decode", "md5",
                "date_part", "date_trunc", "extract", "age", "make_interval",
                "row_to_json", "json_build_object", "json_build_array",
                "jsonb_build_object", "jsonb_build_array",
                "generate_series", "pg_typeof", "pg_sleep", "pg_sleep_for", "pg_sleep_until",
                "hstore", "exist", "defined", "isexists", "isdefined",
                "akeys", "avals", "skeys", "svals", "each",
                "delete", "slice", "hstore_to_json", "hstore_to_jsonb",
                "hstore_to_json_loose", "hstore_to_jsonb_loose",
                "hstore_to_array", "hstore_to_matrix", "populate_record").contains(name);
    }

    /**
     * The rules a parameter list has to obey to be callable at all: VARIADIC collects the trailing
     * arguments into an array, so it must be an array and must come last, and a parameter without
     * a default can never be reached once an earlier one has taken its default's place.
     */
    private void validateSignature(List<PgFunction.Param> params) {
        boolean sawDefault = false;
        for (int i = 0; i < params.size(); i++) {
            PgFunction.Param p = params.get(i);
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase();
            if ("OUT".equals(mode)) continue;
            if ("VARIADIC".equals(mode)) {
                String type = p.typeName() == null ? "" : p.typeName().trim().toLowerCase();
                type = type.replace("\"", "");
                // VARIADIC "any" is the untyped form, which takes the arguments as they come
                if (!type.isEmpty() && !type.endsWith("[]") && !type.equals("anyarray")
                        && !type.equals("any") && !type.equals("anycompatiblearray")) {
                    throw new MemgresException("VARIADIC parameter must be an array", "42P13");
                }
                for (int j = i + 1; j < params.size(); j++) {
                    String laterMode = params.get(j).mode() == null
                            ? "IN" : params.get(j).mode().toUpperCase();
                    if (!"OUT".equals(laterMode)) {
                        throw new MemgresException(
                                "VARIADIC parameter must be the last input parameter", "42P13");
                    }
                }
                continue;
            }
            if (p.defaultExpr() != null) {
                sawDefault = true;
            } else if (sawDefault) {
                throw new MemgresException(
                        "input parameters after one with a default value must also have defaults",
                        "42P13");
            }
        }
    }

    /**
     * Checks if a PL/pgSQL body contains unsupported transaction commands
     * (SAVEPOINT, ROLLBACK TO SAVEPOINT) that PG rejects at creation time.
     */
    private boolean containsUnsupportedTransactionCommand(String body) {
        try {
            com.memgres.engine.plpgsql.PlpgsqlStatement.Block block =
                    com.memgres.engine.plpgsql.PlpgsqlParser.parse(body);
            return containsAbortStmt(block.body());
        } catch (Exception e) {
            return false;
        }
    }

    private boolean containsAbortStmt(java.util.List<com.memgres.engine.plpgsql.PlpgsqlStatement> stmts) {
        for (com.memgres.engine.plpgsql.PlpgsqlStatement stmt : stmts) {
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.SavepointStmt) {
                return true;
            }
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.Block) {
                com.memgres.engine.plpgsql.PlpgsqlStatement.Block b =
                        (com.memgres.engine.plpgsql.PlpgsqlStatement.Block) stmt;
                if (containsAbortStmt(b.body())) return true;
                for (com.memgres.engine.plpgsql.PlpgsqlStatement.ExceptionHandler h : b.exceptionHandlers()) {
                    if (containsAbortStmt(h.body())) return true;
                }
            }
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.IfStmt) {
                com.memgres.engine.plpgsql.PlpgsqlStatement.IfStmt ifStmt =
                        (com.memgres.engine.plpgsql.PlpgsqlStatement.IfStmt) stmt;
                if (containsAbortStmt(ifStmt.thenBody())) return true;
                if (containsAbortStmt(ifStmt.elseBody())) return true;
                for (com.memgres.engine.plpgsql.PlpgsqlStatement.ElsifClause c : ifStmt.elsifClauses()) {
                    if (containsAbortStmt(c.body())) return true;
                }
            }
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.LoopStmt) {
                if (containsAbortStmt(((com.memgres.engine.plpgsql.PlpgsqlStatement.LoopStmt) stmt).body())) return true;
            }
        }
        return false;
    }

    private static final Set<String> NUMERIC_TYPES = Cols.setOf(
            "int", "integer", "bigint", "smallint", "int2", "int4", "int8",
            "numeric", "decimal", "real", "float", "float4", "float8",
            "double precision", "money");
    private static final Set<String> STRING_TYPES = Cols.setOf(
            "text", "varchar", "character varying", "char", "character", "name");

    static boolean isNumericType(String type) {
        return NUMERIC_TYPES.contains(type.toLowerCase().trim());
    }

    static void checkCastReturnTypeMismatch(String castTo, String retType) {
        String ct = castTo.toLowerCase().trim();
        String rt = retType.toLowerCase().trim();
        boolean castIsString = STRING_TYPES.contains(ct) || ct.startsWith("character varying") || ct.startsWith("character(");
        boolean retIsNumeric = NUMERIC_TYPES.contains(rt);
        boolean retIsString = STRING_TYPES.contains(rt) || rt.startsWith("character varying");
        boolean castIsNumeric = NUMERIC_TYPES.contains(ct);
        if (castIsString && retIsNumeric) {
            throw new MemgresException(
                    "return type mismatch in function declared to return " + retType
                            + "\n  Detail: Actual return type is text.", "42P13");
        }
        if (castIsNumeric && retIsString) {
            throw new MemgresException(
                    "return type mismatch in function declared to return " + retType
                            + "\n  Detail: Actual return type is integer.", "42P13");
        }
    }

    /** Split SQL body into individual statements separated by semicolons. */
    private List<String> splitSqlStatements(String body) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c == '\'') {
                current.append(c);
                i++;
                while (i < body.length()) {
                    current.append(body.charAt(i));
                    if (body.charAt(i) == '\'' && (i + 1 >= body.length() || body.charAt(i + 1) != '\'')) {
                        i++;
                        break;
                    }
                    if (body.charAt(i) == '\'' && i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                        current.append(body.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    i++;
                }
            } else if (c == ';') {
                String stmt2 = current.toString().trim();
                if (!stmt2.isEmpty()) result.add(stmt2);
                current.setLength(0);
                i++;
            } else {
                current.append(c);
                i++;
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) result.add(last);
        return result.isEmpty() ? Cols.listOf(body.trim()) : result;
    }

    // ---- CALL ----

    /**
     * A procedure's OUT parameters come before any parameter with a default.
     *
     * <p>A CALL supplies its arguments in order and an OUT parameter takes a place in that order,
     * so one written after a defaulted parameter can never be reached: the call would have to
     * leave out the default to get to it. PostgreSQL refuses the declaration rather than the call.
     */
    private void rejectOutParameterAfterDefault(CreateFunctionStmt stmt) {
        if (stmt.parsedParams == null || !stmt.isProcedure()) return;
        boolean seenDefault = false;
        for (CreateFunctionStmt.FuncParam p : stmt.parsedParams) {
            String mode = p.mode == null ? "IN" : p.mode.toUpperCase();
            if ("OUT".equals(mode)) {
                if (seenDefault) {
                    throw new MemgresException(
                            "procedure OUT parameters cannot appear after one with a default value",
                            "42P13");
                }
                continue;
            }
            if (p.defaultExpr != null && !p.defaultExpr.isEmpty()) seenDefault = true;
        }
    }

    /** A CALL argument is a value; a query that produces one is not one. */
    private void rejectSubqueryArgument(Expression expr) {
        if (expr == null) return;
        if (expr instanceof SubqueryExpr) {
            throw new MemgresException("cannot use subquery in CALL argument", "0A000");
        }
        for (Expression child : argumentChildren(expr)) rejectSubqueryArgument(child);
    }

    /** Nor is an aggregate, which needs rows to aggregate and a CALL has none. */
    private void rejectAggregateArgument(Expression expr) {
        if (expr == null) return;
        if (expr instanceof FunctionCallExpr
                && CALL_AGGREGATES.contains(((FunctionCallExpr) expr).name().toLowerCase())) {
            throw new MemgresException(
                    "aggregate functions are not allowed in CALL arguments", "42803");
        }
        for (Expression child : argumentChildren(expr)) rejectAggregateArgument(child);
    }

    private static final Set<String> CALL_AGGREGATES = new HashSet<>(Arrays.asList(
            "count", "sum", "avg", "min", "max", "array_agg", "string_agg", "bool_and", "bool_or",
            "every", "json_agg", "jsonb_agg", "xmlagg", "stddev", "variance"));

    /** The sub-expressions of a CALL argument, for the two things an argument may not contain. */
    private static List<Expression> argumentChildren(Expression expr) {
        List<Expression> kids = new ArrayList<>();
        if (expr instanceof BinaryExpr) {
            kids.add(((BinaryExpr) expr).left());
            kids.add(((BinaryExpr) expr).right());
        } else if (expr instanceof UnaryExpr) {
            kids.add(((UnaryExpr) expr).operand());
        } else if (expr instanceof FunctionCallExpr) {
            List<Expression> args = ((FunctionCallExpr) expr).args();
            if (args != null) kids.addAll(args);
        } else if (expr instanceof CastExpr) {
            kids.add(((CastExpr) expr).expr());
        }
        return kids;
    }

    /** The types a call's arguments have, named the way a message names them. */
    private List<String> callArgumentTypes(List<Expression> args) {
        List<String> names = new ArrayList<>();
        for (Expression arg : args) {
            names.add(callArgumentType(
                    arg instanceof NamedArgExpr ? ((NamedArgExpr) arg).value() : arg));
        }
        return names;
    }

    private String callArgumentType(Expression arg) {
        if (arg instanceof CastExpr) return ((CastExpr) arg).typeName();
        Object value;
        try {
            value = executor.evalExpr(arg, null);
        } catch (RuntimeException e) {
            return "unknown";
        }
        if (value instanceof Integer) return "integer";
        if (value instanceof Short) return "smallint";
        if (value instanceof Long) return "bigint";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof java.math.BigDecimal) return "numeric";
        if (value instanceof Float) return "real";
        if (value instanceof Double) return "double precision";
        if (value instanceof String) return "text";
        return "unknown";
    }

    /**
     * The procedure a call names: the one of that name whose parameters the call can supply.
     */
    private PgFunction resolveProcedure(String callName, int argCount, List<String> argTypes) {
        List<PgFunction> candidates;
        if (callName.contains(".")) {
            String[] parts = callName.split("\\.", 2);
            candidates = executor.database.getFunctionOverloads(parts[0], parts[1]);
        } else {
            candidates = executor.database.getFunctionOverloads(callName);
        }
        if (candidates == null || candidates.isEmpty()) return null;
        // Prefer a signature the arguments actually fit; fall back to one that merely takes that
        // many, so a call with an argument of no knowable type still reaches a procedure.
        for (PgFunction candidate : candidates) {
            if (acceptsArgumentCount(candidate, argCount)
                    && acceptsArgumentTypes(candidate, argTypes)) {
                return candidate;
            }
        }
        for (PgFunction candidate : candidates) {
            if (acceptsArgumentCount(candidate, argCount) && argTypes.contains("unknown")) {
                return candidate;
            }
        }
        // A name that exists but takes other arguments is still "does not exist" with the types
        // written; a name that is not a procedure at all is reported by the caller.
        for (PgFunction candidate : candidates) {
            if (!candidate.isProcedure()) return candidate;
        }
        return null;
    }

    /**
     * Whether the arguments written can be passed to this routine's parameters.
     *
     * <p>A value is passed when its type is the parameter's, or when it is a kind of number the
     * parameter's type holds without losing anything: an integer fits a bigint or a numeric, and
     * a numeric does not fit an integer. PostgreSQL will not silently round an argument down to
     * reach a procedure, and neither did the call the reader wrote.
     */
    private static boolean acceptsArgumentTypes(PgFunction fn, List<String> argTypes) {
        for (PgFunction.Param p : fn.getParams()) {
            if ("VARIADIC".equalsIgnoreCase(p.mode())) return true;
        }
        int at = 0;
        for (PgFunction.Param p : fn.getParams()) {
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase();
            if ("OUT".equals(mode)) continue;
            if (at >= argTypes.size()) break;
            String given = argTypes.get(at++);
            if ("unknown".equals(given)) continue;
            if (!typeFits(given, p.typeName())) return false;
        }
        return true;
    }

    /** Whether a value of {@code given} reaches a parameter declared {@code declared}. */
    private static boolean typeFits(String given, String declared) {
        if (declared == null) return true;
        String want = declared.trim().toLowerCase();
        int paren = want.indexOf('(');
        if (paren > 0) want = want.substring(0, paren).trim();
        String have = given.toLowerCase();
        if (have.equals(want)) return true;
        int haveRank = numericRank(have);
        int wantRank = numericRank(want);
        if (haveRank > 0 && wantRank > 0) return haveRank <= wantRank;
        // Names for one type, written more than one way.
        if (want.equals("int") || want.equals("int4") || want.equals("integer")) {
            return have.equals("integer") || have.equals("smallint");
        }
        if (want.equals("varchar") || want.equals("character varying")) return have.equals("text");
        if (want.equals("text")) return have.equals("text") || have.equals("character varying");
        return false;
    }

    /** How wide a number is, so a narrower one may be passed where a wider is declared. */
    private static int numericRank(String type) {
        switch (type) {
            case "smallint": case "int2": return 1;
            case "integer": case "int": case "int4": return 2;
            case "bigint": case "int8": return 3;
            case "numeric": case "decimal": return 4;
            case "real": case "float4": return 5;
            case "double precision": case "float8": return 6;
            default: return 0;
        }
    }

    /** Whether an OUT parameter comes before any parameter that takes a value. */
    private static boolean hasLeadingOut(PgFunction fn) {
        for (PgFunction.Param p : fn.getParams()) {
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase();
            if ("OUT".equals(mode)) return true;
            return false;
        }
        return false;
    }

    /** A routine call written with VARIADIC passes one array where the tail would go. */
    private static boolean writtenVariadic(CallStmt stmt) {
        for (Expression arg : stmt.args()) {
            if (arg instanceof NamedArgExpr
                    && "__variadic__".equals(((NamedArgExpr) arg).name())) {
                return true;
            }
        }
        return false;
    }

    /** Whether this routine can be called with that many arguments. */
    private static boolean acceptsArgumentCount(PgFunction fn, int argCount) {
        int required = 0;
        int accepted = 0;
        int total = fn.getParams().size();
        for (PgFunction.Param p : fn.getParams()) {
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase();
            if ("OUT".equals(mode)) continue;
            accepted++;
            if (p.defaultExpr() == null) required++;
        }
        boolean variadic = false;
        for (PgFunction.Param p : fn.getParams()) {
            if ("VARIADIC".equalsIgnoreCase(p.mode())) variadic = true;
        }
        if (variadic) return argCount >= required - 1;
        return argCount == total || (argCount >= required && argCount <= accepted);
    }

    QueryResult executeCall(CallStmt stmt) {
        // What a CALL may be given: values, not queries and not aggregates. Both are refused
        // before anything is looked up, the way PostgreSQL refuses them.
        for (Expression arg : stmt.args()) {
            rejectSubqueryArgument(arg);
            rejectAggregateArgument(arg);
        }
        String callName = stmt.name();
        List<String> argTypes = callArgumentTypes(stmt.args());
        PgFunction function = resolveProcedure(callName, stmt.args().size(), argTypes);
        if (function == null) {
            // A procedure is its name and the types it takes, so a call that matches no
            // signature names the types it was written with. Looking the name up alone found
            // whichever overload was stored first and ran it on the wrong arguments.
            throw new MemgresException("procedure " + stmt.name() + "("
                    + String.join(", ", argTypes) + ") does not exist", "42883");
        }
        if (!function.isProcedure()) {
            String declared = function.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(Collectors.joining(", "));
            throw new MemgresException(stmt.name() + "(" + declared + ") is not a procedure", "42809");
        }
        // Count required IN params (minimum) and total params (maximum, including OUT/INOUT)
        int requiredInParams = 0, inParamCount = 0, totalParams = function.getParams().size();
        @SuppressWarnings("unused") boolean typesNamed = true;
        List<PgFunction.Param> outParams = new ArrayList<>();
        for (PgFunction.Param p : function.getParams()) {
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode)) {
                outParams.add(p);
            } else if ("INOUT".equals(mode)) {
                inParamCount++;
                if (p.defaultExpr() == null) requiredInParams++;
                outParams.add(p);
            } else {
                inParamCount++;
                if (p.defaultExpr() == null) requiredInParams++;
            }
        }
        // PG allows CALL with either just IN args or all args (including OUT placeholders)
        int argCount = stmt.args().size();
        // A VARIADIC parameter takes the whole tail, so a call may pass more arguments than the
        // procedure declares parameters and still be the procedure that was declared.
        boolean variadicTail = false;
        for (PgFunction.Param p : function.getParams()) {
            if ("VARIADIC".equalsIgnoreCase(p.mode())) variadicTail = true;
        }
        // Every parameter takes a place in a CALL's argument list, an OUT one included: it is
        // written as a placeholder and the value it is given is thrown away. Accepting a call
        // that named only the IN parameters made CALL p(3) reach a p(int, OUT int) that no such
        // call in PostgreSQL ever reaches.
        int requiredParams = 0;
        for (PgFunction.Param p : function.getParams()) {
            if (p.defaultExpr() == null || p.defaultExpr().isEmpty()) requiredParams++;
        }
        if (!variadicTail && (argCount < requiredParams || argCount > totalParams)) {
            throw new MemgresException("procedure " + stmt.name() + "("
                    + String.join(", ", argTypes) + ") does not exist", "42883");
        }
        // Build args list: only pass IN/INOUT values to executeFunction
        // A call's arguments line up with the parameters in order. An OUT parameter takes a place
        // in that order and gives nothing to the body; an IN or INOUT parameter the call did not
        // reach takes the value its declaration gives it. Counting only the IN parameters put the
        // arguments against the wrong ones whenever an OUT came first, and dropping the ones the
        // call left off had the body read an unset variable as null.
        List<Object> args = new ArrayList<>();
        int argIdx = 0;
        boolean placeholdersGiven = argCount == totalParams || hasLeadingOut(function);
        // An argument written with a name goes to the parameter of that name wherever it sits,
        // and one written after VARIADIC is the whole tail as an array.
        Map<String, Expression> byName = new LinkedHashMap<>();
        Expression variadic = null;
        for (Expression arg : stmt.args()) {
            if (!(arg instanceof NamedArgExpr)) continue;
            NamedArgExpr named = (NamedArgExpr) arg;
            if ("__variadic__".equals(named.name())) variadic = named.value();
            else byName.put(named.name().toLowerCase(), named.value());
        }
        for (PgFunction.Param p : function.getParams()) {
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode)) {
                if (placeholdersGiven && argIdx < stmt.args().size()
                        && !(stmt.args().get(argIdx) instanceof NamedArgExpr)) {
                    argIdx++;
                }
                continue;
            }
            Expression named = p.name() == null ? null : byName.get(p.name().toLowerCase());
            if (named != null) {
                args.add(executor.evalExpr(named, null));
                continue;
            }
            if ("VARIADIC".equals(mode) && variadic != null) {
                args.add(executor.evalExpr(variadic, null));
                continue;
            }
            if ("VARIADIC".equals(mode)) {
                // Written without the keyword, the arguments left over are the array the
                // parameter takes: CALL p(1, 2, 3) reaches VARIADIC a int[] as {1,2,3}. They are
                // handed over one by one, because gathering them is what binding the parameter
                // already does — collected here as well the array held one array inside it.
                while (argIdx < stmt.args().size()) {
                    Expression tailArg = stmt.args().get(argIdx++);
                    if (tailArg instanceof NamedArgExpr) continue;
                    args.add(executor.evalExpr(tailArg, null));
                }
                continue;
            }
            // Skip the positions the named arguments already filled.
            while (argIdx < stmt.args().size() && stmt.args().get(argIdx) instanceof NamedArgExpr) {
                argIdx++;
            }
            if (argIdx < stmt.args().size()) {
                args.add(executor.evalExpr(stmt.args().get(argIdx++), null));
            } else if (p.defaultExpr() != null && !p.defaultExpr().isEmpty()) {
                args.add(executor.evalExpr(
                        com.memgres.engine.parser.Parser.parseExpression(p.defaultExpr()), null));
            }
        }
        // Start implicit transaction for procedure (PG behavior: CALL in autocommit starts a txn)
        boolean implicitTxn = false;
        if (executor.session != null && executor.session.getStatus() == Session.TransactionStatus.IDLE) {
            executor.session.begin();
            implicitTxn = true;
        }
        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
        Object returnVal;
        boolean procedureFailed = false;
        try {
            returnVal = plExec.executeFunction(function, args);
        } catch (RuntimeException e) {
            procedureFailed = true;
            // Rollback the transaction on procedure error
            if (executor.session != null) {
                Session.TransactionStatus st = executor.session.getStatus();
                if (st == Session.TransactionStatus.IN_TRANSACTION || st == Session.TransactionStatus.FAILED) {
                    executor.session.rollback();
                }
            }
            throw e;
        } finally {
            // After procedure returns successfully, commit any trailing implicit transaction
            if (!procedureFailed && executor.session != null) {
                Session.TransactionStatus st = executor.session.getStatus();
                if (st == Session.TransactionStatus.IN_TRANSACTION) {
                    executor.session.commit();
                } else if (st == Session.TransactionStatus.FAILED) {
                    executor.session.rollback();
                }
            }
        }
        // If there are OUT/INOUT params, return a result set
        if (!outParams.isEmpty()) {
            List<Column> columns = new ArrayList<>();
            for (PgFunction.Param p : outParams) {
                String colName = p.name() != null ? p.name() : "column" + (columns.size() + 1);
                columns.add(new Column(colName, DataType.TEXT, true, false, null));
            }
            Object[] row;
            if (returnVal instanceof Object[]) {
                row = (Object[]) returnVal;
            } else {
                row = new Object[] { returnVal };
            }
            List<Object[]> rows = new ArrayList<>();
            rows.add(row);
            return QueryResult.select(columns, rows);
        }
        return QueryResult.command(QueryResult.Type.CALL, 0);
    }

    // ---- CREATE TRIGGER ----

    QueryResult executeCreateTrigger(CreateTriggerStmt stmt) {
        // A constraint trigger's grammar has no REFERENCING clause and no OR REPLACE. Neither is a
        // definition PostgreSQL inspects and rejects: they are sentences the grammar never had,
        // which is why the first comes back as a syntax error before the relation is opened at all.
        if (stmt.constraintTrigger()) {
            if (stmt.newTransitionTable() != null || stmt.oldTransitionTable() != null) {
                throw PgErrors.syntax("syntax error at or near \"REFERENCING\"");
            }
            if (stmt.orReplace()) {
                throw PgErrors.notImplemented(
                        "CREATE OR REPLACE CONSTRAINT TRIGGER is not supported");
            }
        }
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        String triggerTableSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        PgTrigger.Timing timing;
        switch (stmt.timing()) {
            case "BEFORE":
                timing = PgTrigger.Timing.BEFORE;
                break;
            case "AFTER":
                timing = PgTrigger.Timing.AFTER;
                break;
            case "INSTEAD OF":
                timing = PgTrigger.Timing.INSTEAD_OF;
                break;
            default:
                timing = PgTrigger.Timing.BEFORE;
                break;
        }
        boolean isView = stmt.table() != null && executor.database.hasView(stmt.table());
        // Which kind of relation was named settles before anything about the trigger does. A
        // sequence and a materialized view are relations a trigger cannot be attached to at all,
        // and PostgreSQL refuses each for what it is rather than letting a sequence through as a
        // table or answering for a materialized view with the rule about views.
        String relationKind = stmt.table() == null ? null
                : RelationNamespace.kindOf(executor.database, triggerTableSchema, stmt.table());
        if (RelationNamespace.SEQUENCE.equals(relationKind)
                || RelationNamespace.MATVIEW.equals(relationKind)) {
            MemgresException wrongKind = PgErrors.wrongObjectType(
                    "relation \"" + stmt.table() + "\" cannot have triggers");
            wrongKind.setDetail("This operation is not supported for "
                    + (RelationNamespace.SEQUENCE.equals(relationKind)
                            ? "sequences." : "materialized views."));
            throw wrongKind;
        }
        // PostgreSQL opens the relation before it judges what kind it is, so a trigger named on
        // a relation that is not there is a missing relation — not a table that cannot take an
        // INSTEAD OF trigger, which is what "not a view" was read as.
        if (stmt.table() != null && !isView) {
            executor.resolveTable(triggerTableSchema, stmt.table());
        }
        if (timing == PgTrigger.Timing.INSTEAD_OF && !isView) {
            throw PgErrors.wrongObjectType("\"" + stmt.table() + "\" is a table"
                    + "\n  Detail: Tables cannot have INSTEAD OF triggers.");
        }
        // A view has no rows of its own to hand a BEFORE or AFTER trigger one at a time, so those
        // must be INSTEAD OF — but a statement-level trigger is handed no row at all, and
        // PostgreSQL accepts it on a view whether or not the view could be written through.
        if ((timing == PgTrigger.Timing.BEFORE || timing == PgTrigger.Timing.AFTER)
                && isView && !stmt.forEachStatement()) {
            throw new MemgresException("\"" + stmt.table() + "\" is a view"
                    + "\n  Detail: Views cannot have row-level BEFORE or AFTER triggers.", "42809");
        }
        // A trigger runs code of the definer's choosing every time the relation is written, so
        // PostgreSQL asks for the TRIGGER privilege on it — and answers "permission denied" rather
        // than "must be owner", because a grant is enough. The relation is opened first, so a name
        // that is not there is still reported as a missing relation.
        if (stmt.table() != null) {
            executor.checkTablePrivilege("TRIGGER", triggerTableSchema, stmt.table());
        }
        List<PgTrigger.Event> trigEvents = new ArrayList<>();
        for (String event : stmt.events()) {
            try {
                trigEvents.add(PgTrigger.Event.valueOf(event));
            } catch (IllegalArgumentException e) {
                throw new MemgresException("syntax error at or near \"" + event.toLowerCase() + "\"", "42601");
            }
        }
        checkTriggerShape(stmt, timing, trigEvents, triggerTableSchema, isView);
        // The WHEN condition is analysed against the relation before the function the trigger will
        // call is looked for, so a condition that cannot stand is what PostgreSQL reports even when
        // the function is missing too.
        checkTriggerWhen(stmt, trigEvents, triggerTableSchema, isView);
        if (stmt.functionName() != null) {
            PgFunction trigFunc = executor.database.getFunction(stmt.functionName());
            if (trigFunc == null) {
                throw new MemgresException("function " + stmt.functionName() + "() does not exist",
                        "42883").withoutHint();
            }
            String trigRetType = trigFunc.getReturnType();
            if (trigRetType != null && !trigRetType.isEmpty()
                    && !"trigger".equalsIgnoreCase(trigRetType) && !"void".equalsIgnoreCase(trigRetType)) {
                throw new MemgresException("function " + stmt.functionName() + " must return type trigger", "42P17");
            }
        }
        if (stmt.orReplace()) {
            executor.database.removeTrigger(stmt.name(), stmt.table());
        }
        for (PgTrigger.Event trigEvent : trigEvents) {
            PgTrigger trigger = new PgTrigger(
                    stmt.name(), timing, trigEvent, stmt.table(), stmt.functionName(),
                    trigEvent == PgTrigger.Event.UPDATE ? stmt.updateOfColumns() : null,
                    stmt.newTransitionTable(), stmt.oldTransitionTable(), stmt.forEachStatement(),
                    stmt.whenClause(), stmt.deferrable, stmt.initiallyDeferred, stmt.functionArgs());
            trigger.setSchemaName(triggerTableSchema);
            // What the statement said it was: a constraint trigger is a constraint too, and the
            // catalogue has to be able to say so.
            trigger.setConstraintTrigger(stmt.constraintTrigger());
            trigger.setConstraintRelation(stmt.constraintRelation());
            executor.database.addTrigger(trigger);
        }
        return QueryResult.command(QueryResult.Type.CREATE_TRIGGER, 0);
    }

    /**
     * Reject trigger definitions that describe something the engine could never fire correctly:
     * a row-level view trigger with no row to substitute for, a transition table built from a
     * statement that has not run yet, a column list naming a column that is not there.
     */
    private void checkTriggerShape(CreateTriggerStmt stmt, PgTrigger.Timing timing,
                                   List<PgTrigger.Event> events, String schema, boolean isView) {
        // A view has no rows of its own to truncate, so TRUNCATE is not an event it can carry at
        // all. PostgreSQL settles that about the relation before it looks at how the trigger fires,
        // and checking the level first reported a rule the statement never broke.
        if (isView && events.contains(PgTrigger.Event.TRUNCATE)) {
            MemgresException viewTruncate =
                    PgErrors.wrongObjectType("\"" + stmt.table() + "\" is a view");
            viewTruncate.setDetail("Views cannot have TRUNCATE triggers.");
            throw viewTruncate;
        }
        // A transition table holds the rows a statement wrote, and a view stores none of its own,
        // so there is nothing for one to be built from.
        if (isView && (stmt.newTransitionTable() != null || stmt.oldTransitionTable() != null)) {
            MemgresException viewTransition =
                    PgErrors.wrongObjectType("\"" + stmt.table() + "\" is a view");
            viewTransition.setDetail("Triggers on views cannot have transition tables.");
            throw viewTransition;
        }
        // A row trigger with a transition table would have to be shown the rows of every partition
        // at once, which PostgreSQL does not build: neither a partitioned table nor one of its
        // partitions may carry one. The relation's kind is judged before how the trigger fires, so
        // a partitioned table is answered for being one.
        if (!isView && stmt.table() != null && !stmt.forEachStatement()
                && (stmt.newTransitionTable() != null || stmt.oldTransitionTable() != null)
                && executor.resolveTable(schema, stmt.table()).getPartitionStrategy() != null) {
            MemgresException partitioned = PgErrors.notImplemented(
                    "\"" + stmt.table() + "\" is a partitioned table");
            partitioned.setDetail(
                    "ROW triggers with transition tables are not supported on partitioned tables.");
            throw partitioned;
        }
        if (timing == PgTrigger.Timing.INSTEAD_OF) {
            if (stmt.forEachStatement()) {
                throw PgErrors.notImplemented("INSTEAD OF triggers must be FOR EACH ROW");
            }
            if (stmt.whenClause() != null) {
                throw PgErrors.notImplemented("INSTEAD OF triggers cannot have WHEN conditions");
            }
            // An INSTEAD OF trigger replaces the whole statement on the view, so there is no
            // "was this column written" to narrow it down to.
            if (stmt.updateOfColumns() != null && !stmt.updateOfColumns().isEmpty()) {
                throw PgErrors.notImplemented("INSTEAD OF triggers cannot have column lists");
            }
        }
        if (!stmt.forEachStatement() && events.contains(PgTrigger.Event.TRUNCATE)) {
            throw PgErrors.notImplemented("TRUNCATE FOR EACH ROW triggers are not supported");
        }
        if (stmt.updateOfColumns() != null && !stmt.updateOfColumns().isEmpty() && !isView) {
            Table target = executor.resolveTable(schema, stmt.table());
            for (String col : stmt.updateOfColumns()) {
                if (target.getColumnIndex(col) < 0) {
                    throw new MemgresException("column \"" + col + "\" of relation \""
                            + stmt.table() + "\" does not exist", "42703");
                }
            }
        }
        if (stmt.newTransitionTable() != null || stmt.oldTransitionTable() != null) {
            if (timing != PgTrigger.Timing.AFTER) {
                throw PgErrors.invalidObjectState(
                        "transition table name can only be specified for an AFTER trigger");
            }
            // A row of a partition is one row of the partitioned table's statement, and the
            // transition table belongs to that statement rather than to any one partition.
            if (!isView && stmt.table() != null && !stmt.forEachStatement()
                    && executor.resolveTable(schema, stmt.table()).getPartitionParent() != null) {
                throw PgErrors.notImplemented(
                        "ROW triggers with transition tables are not supported on partitions");
            }
            if (stmt.oldTransitionTable() != null
                    && !events.contains(PgTrigger.Event.DELETE) && !events.contains(PgTrigger.Event.UPDATE)) {
                throw PgErrors.invalidObjectState(
                        "OLD TABLE can only be specified for a DELETE or UPDATE trigger");
            }
            if (stmt.newTransitionTable() != null
                    && !events.contains(PgTrigger.Event.INSERT) && !events.contains(PgTrigger.Event.UPDATE)) {
                throw PgErrors.invalidObjectState(
                        "NEW TABLE can only be specified for an INSERT or UPDATE trigger");
            }
        }
        if (!stmt.orReplace()) {
            for (PgTrigger existing : executor.database.getTriggersForTable(schema, stmt.table())) {
                if (existing.getName().equalsIgnoreCase(stmt.name())) {
                    throw new MemgresException("trigger \"" + stmt.name() + "\" for relation \""
                            + stmt.table() + "\" already exists", "42710");
                }
            }
            // The trigger is about to be cloned onto every partition, so a partition already
            // carrying one of that name is a collision, and PostgreSQL reports it against the
            // partition rather than against the relation the statement named.
            if (!isView && stmt.table() != null && !stmt.forEachStatement()) {
                rejectTriggerNameInPartitions(executor.resolveTable(schema, stmt.table()),
                        stmt.name());
            }
        }
    }

    /** A trigger cloned onto the partitions may not collide with one written on a partition. */
    private void rejectTriggerNameInPartitions(Table on, String name) {
        for (Table partition : on.getPartitions()) {
            for (PgTrigger existing : executor.database.getTriggersForTable(partition.getName())) {
                if (existing.getName().equalsIgnoreCase(name)) {
                    throw new MemgresException("trigger \"" + name + "\" for relation \""
                            + partition.getName() + "\" already exists", "42710");
                }
            }
            rejectTriggerNameInPartitions(partition, name);
        }
    }

    /**
     * A WHEN condition is resolved against the trigger's own OLD/NEW rows, so which of those two
     * exists depends on the events the trigger fires for, and a statement-level trigger has
     * neither.
     */
    private void checkTriggerWhen(CreateTriggerStmt stmt, List<PgTrigger.Event> events,
                                  String schema, boolean isView) {
        if (stmt.whenClause() == null || stmt.table() == null || isView) return;
        Table target = executor.resolveTable(schema, stmt.table());
        Expression whenExpr;
        try {
            whenExpr = com.memgres.engine.parser.Parser.parseExpression(stmt.whenClause());
        } catch (RuntimeException e) {
            return; // an unparsable WHEN clause is reported when the trigger fires, as before
        }
        boolean usesOld = referencesRow(whenExpr, "old");
        boolean usesNew = referencesRow(whenExpr, "new");
        if (stmt.forEachStatement()) {
            if (usesOld || usesNew || AstWalk.anyMatch(whenExpr, n -> n instanceof ColumnRef)) {
                throw PgErrors.invalidObjectState(
                        "statement trigger's WHEN condition cannot reference column values");
            }
            return;
        }
        if (usesOld && events.contains(PgTrigger.Event.INSERT)) {
            throw PgErrors.invalidObjectState(
                    "INSERT trigger's WHEN condition cannot reference OLD values");
        }
        if (usesNew && events.contains(PgTrigger.Event.DELETE)) {
            throw PgErrors.invalidObjectState(
                    "DELETE trigger's WHEN condition cannot reference NEW values");
        }
        StoredExprCheck.forTriggerWhen(target).check(whenExpr, executor.selectExecutor);
    }

    private static boolean referencesRow(Expression expr, String alias) {
        return AstWalk.anyMatch(expr, n -> n instanceof ColumnRef
                && ((ColumnRef) n).table() != null
                && ((ColumnRef) n).table().equalsIgnoreCase(alias));
    }

    // ---- CREATE EVENT TRIGGER ----

    QueryResult executeCreateEventTrigger(CreateEventTriggerStmt stmt) {
        // A trigger whose function returns anything else cannot be called by the event machinery
        PgFunction func = executor.database.getFunction(stmt.functionName());
        if (func == null) {
            throw new MemgresException("function " + stmt.functionName() + "() does not exist", "42883");
        }
        if (!"event_trigger".equalsIgnoreCase(String.valueOf(func.getReturnType()).trim())) {
            throw new MemgresException(
                    "function " + stmt.functionName() + " must return type event_trigger", "42P17");
        }
        if (stmt.tags() != null) {
            for (String tag : stmt.tags()) {
                String normalized = tag.trim().toUpperCase();
                if (EVENT_TRIGGER_TAGS.contains(normalized)) continue;
                if (NON_DDL_COMMAND_TAGS.contains(normalized)) {
                    throw new MemgresException(
                            "event triggers are not supported for " + normalized, "0A000");
                }
                throw new MemgresException(
                        "filter value \"" + tag + "\" not recognized for filter variable \"tag\"", "42601");
            }
        }
        if (executor.database.getEventTrigger(stmt.name()) != null) {
            throw new MemgresException("event trigger \"" + stmt.name() + "\" already exists", "42710");
        }
        PgEventTrigger et = new PgEventTrigger(stmt.name(), stmt.event(), stmt.functionName(), stmt.tags());
        executor.database.addEventTrigger(et);
        return QueryResult.command(QueryResult.Type.CREATE_TRIGGER, 0);
    }

    /** Command tags an event trigger may filter on (PG's "Event Trigger Firing Matrix"). */
    private static final Set<String> EVENT_TRIGGER_TAGS = Cols.setOf(
            "ALTER AGGREGATE", "ALTER COLLATION", "ALTER CONVERSION", "ALTER DOMAIN",
            "ALTER DEFAULT PRIVILEGES", "ALTER EXTENSION", "ALTER FOREIGN DATA WRAPPER",
            "ALTER FOREIGN TABLE", "ALTER FUNCTION", "ALTER LANGUAGE", "ALTER LARGE OBJECT",
            "ALTER MATERIALIZED VIEW", "ALTER OPERATOR", "ALTER OPERATOR CLASS",
            "ALTER OPERATOR FAMILY", "ALTER POLICY", "ALTER PROCEDURE", "ALTER PUBLICATION",
            "ALTER ROUTINE", "ALTER RULE", "ALTER SCHEMA", "ALTER SEQUENCE", "ALTER SERVER",
            "ALTER STATISTICS", "ALTER SUBSCRIPTION", "ALTER TABLE", "ALTER TEXT SEARCH CONFIGURATION",
            "ALTER TEXT SEARCH DICTIONARY", "ALTER TEXT SEARCH PARSER", "ALTER TEXT SEARCH TEMPLATE",
            "ALTER TRIGGER", "ALTER TYPE", "ALTER USER MAPPING", "ALTER VIEW",
            "COMMENT", "CREATE ACCESS METHOD", "CREATE AGGREGATE", "CREATE CAST",
            "CREATE COLLATION", "CREATE CONVERSION", "CREATE DOMAIN", "CREATE EXTENSION",
            "CREATE FOREIGN DATA WRAPPER", "CREATE FOREIGN TABLE", "CREATE FUNCTION",
            "CREATE INDEX", "CREATE LANGUAGE", "CREATE MATERIALIZED VIEW", "CREATE OPERATOR",
            "CREATE OPERATOR CLASS", "CREATE OPERATOR FAMILY", "CREATE POLICY", "CREATE PROCEDURE",
            "CREATE PUBLICATION", "CREATE RULE", "CREATE SCHEMA", "CREATE SEQUENCE",
            "CREATE SERVER", "CREATE STATISTICS", "CREATE SUBSCRIPTION", "CREATE TABLE",
            "CREATE TABLE AS", "CREATE TEXT SEARCH CONFIGURATION", "CREATE TEXT SEARCH DICTIONARY",
            "CREATE TEXT SEARCH PARSER", "CREATE TEXT SEARCH TEMPLATE", "CREATE TRANSFORM",
            "CREATE TRIGGER", "CREATE TYPE", "CREATE USER MAPPING", "CREATE VIEW",
            "DROP ACCESS METHOD", "DROP AGGREGATE", "DROP CAST", "DROP COLLATION",
            "DROP CONVERSION", "DROP DOMAIN", "DROP EXTENSION", "DROP FOREIGN DATA WRAPPER",
            "DROP FOREIGN TABLE", "DROP FUNCTION", "DROP INDEX", "DROP LANGUAGE",
            "DROP MATERIALIZED VIEW", "DROP OPERATOR", "DROP OPERATOR CLASS",
            "DROP OPERATOR FAMILY", "DROP OWNED", "DROP POLICY", "DROP PROCEDURE",
            "DROP PUBLICATION", "DROP ROUTINE", "DROP RULE", "DROP SCHEMA", "DROP SEQUENCE",
            "DROP SERVER", "DROP STATISTICS", "DROP SUBSCRIPTION", "DROP TABLE",
            "DROP TEXT SEARCH CONFIGURATION", "DROP TEXT SEARCH DICTIONARY",
            "DROP TEXT SEARCH PARSER", "DROP TEXT SEARCH TEMPLATE", "DROP TRANSFORM",
            "DROP TRIGGER", "DROP TYPE", "DROP USER MAPPING", "DROP VIEW",
            "GRANT", "IMPORT FOREIGN SCHEMA", "REFRESH MATERIALIZED VIEW", "REVOKE",
            "SECURITY LABEL", "SELECT INTO");

    /** Command tags PostgreSQL knows but refuses to fire event triggers for. */
    private static final Set<String> NON_DDL_COMMAND_TAGS = Cols.setOf(
            "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "COPY", "CALL", "DO",
            "VACUUM", "ANALYZE", "CLUSTER", "REINDEX", "CHECKPOINT", "EXPLAIN", "PREPARE",
            "EXECUTE", "DEALLOCATE", "DECLARE CURSOR", "FETCH", "MOVE", "CLOSE CURSOR",
            "LISTEN", "NOTIFY", "UNLISTEN", "LOCK TABLE", "SET", "RESET", "SHOW",
            "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "START TRANSACTION",
            "CREATE DATABASE", "DROP DATABASE", "ALTER DATABASE", "CREATE ROLE", "DROP ROLE",
            "ALTER ROLE", "CREATE TABLESPACE", "DROP TABLESPACE", "ALTER TABLESPACE",
            "CREATE EVENT TRIGGER", "ALTER EVENT TRIGGER", "DROP EVENT TRIGGER",
            "REASSIGN OWNED", "LOAD", "DISCARD", "ALTER SYSTEM", "REFRESH COLLATION VERSION");

    // ---- ALTER EVENT TRIGGER ----

    QueryResult executeAlterEventTrigger(AlterEventTriggerStmt stmt) {
        PgEventTrigger et = executor.database.getEventTrigger(stmt.name());
        if (et == null) {
            throw new MemgresException(
                    "event trigger \"" + stmt.name() + "\" does not exist", "42704");
        }
        switch (stmt.action()) {
            case DISABLE:
                et.setEnabled('D');
                break;
            case ENABLE:
                et.setEnabled('O');
                break;
            case ENABLE_REPLICA:
                et.setEnabled('R');
                break;
            case ENABLE_ALWAYS:
                et.setEnabled('A');
                break;
            case RENAME:
                executor.database.removeEventTrigger(stmt.name());
                et.setName(stmt.newName());
                executor.database.addEventTrigger(et);
                break;
            case OWNER:
                // no-op for now (owner tracking not implemented)
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    // ---- DROP EVENT TRIGGER ----

    QueryResult executeDropEventTrigger(DropEventTriggerStmt stmt) {
        PgEventTrigger et = executor.database.getEventTrigger(stmt.name());
        if (et == null) {
            if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.SET, 0);
            throw new MemgresException(
                    "event trigger \"" + stmt.name() + "\" does not exist", "42704");
        }
        executor.database.removeEventTrigger(stmt.name());
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    // ---- DROP (generic) ----

    /**
     * PostgreSQL says what CASCADE took with the object that was named: it names a single
     * dependent and counts more than one. Without it a script that meant to drop one table has no
     * way of learning it dropped four.
     */
    static void noticeDropCascades(AstExecutor executor, List<String> objects) {
        if (objects == null || objects.isEmpty() || executor.session == null) return;
        if (objects.size() == 1) {
            executor.session.addNotice("NOTICE", "00000",
                    "drop cascades to " + objects.get(0), null);
            return;
        }
        // Past one dependent the message is only a count, so PostgreSQL puts the names under
        // DETAIL, one "drop cascades to <object>" line each. Without them a script was told four
        // things had gone and never which four.
        List<String> lines = new ArrayList<>();
        for (String object : objects) lines.add("drop cascades to " + object);
        executor.session.addNotice("NOTICE", "00000",
                "drop cascades to " + objects.size() + " other objects", null,
                dependencyDetail(lines));
    }

    /** How many dependents PostgreSQL names to the client before it stops listing them. */
    private static final int MAX_REPORTED_DEPENDENTS = 100;

    /**
     * The DETAIL a drop reports, one dependent to a line.
     *
     * <p>PostgreSQL names a hundred of them and no more: past that the client is told how many
     * were left out and the whole list goes to the server log instead, so that dropping a
     * relation thousands of objects hang from does not send all of them down the wire.
     */
    static String dependencyDetail(List<String> lines) {
        int shown = Math.min(lines.size(), MAX_REPORTED_DEPENDENTS);
        StringBuilder detail = new StringBuilder();
        for (int i = 0; i < shown; i++) {
            if (i > 0) detail.append('\n');
            detail.append(lines.get(i));
        }
        int hidden = lines.size() - shown;
        if (hidden > 0) {
            detail.append("\nand ").append(hidden)
                    .append(hidden == 1 ? " other object" : " other objects")
                    .append(" (see server log for list)");
        }
        return detail.toString();
    }

    /**
     * How PostgreSQL words a drop it will not make. A statement naming one object names it; one
     * naming several says only that it cannot have what it asked for, because it judged the whole
     * set at once and what is in the way may be in the way of any of them.
     */
    static String cannotDropMessage(boolean severalNamed, String what) {
        return severalNamed
                ? "cannot drop desired object(s) because other objects depend on them"
                : "cannot drop " + what + " because other objects depend on it";
    }

    /** IF EXISTS says what to skip, and PostgreSQL says which one it skipped. */
    private void noticeSkipped(String what) {
        if (executor.session != null) {
            executor.session.addNotice("NOTICE", "00000", what + " does not exist, skipping", null);
        }
    }

    QueryResult executeDropStmt(DropStmt stmt) {
        // Every name the statement lists goes, and each of them knows what the others are:
        // PostgreSQL works out the whole set before it looks for what would be left pointing at
        // one of them, so an object the same DROP takes down is no reason to refuse.
        Set<String> together = new HashSet<>();
        if (!stmt.more().isEmpty()) {
            together.add(RelationNamespace.bareName(stmt.name()).toLowerCase());
            for (DropStmt other : stmt.more()) {
                together.add(RelationNamespace.bareName(other.name()).toLowerCase());
            }
        }
        // A name the list gives twice stands for one object, and PostgreSQL drops it once rather
        // than reporting the second as missing.
        Set<String> named = new HashSet<>();
        named.add(dropTargetIdentity(stmt));
        QueryResult result = executeDropOne(stmt, together);
        for (DropStmt other : stmt.more()) {
            if (!named.add(dropTargetIdentity(other))) continue;
            result = executeDropOne(other, together);
        }
        return result;
    }

    /**
     * What one name in a DROP list stands for: the schema it reaches and the name itself, so that
     * a relation written bare and the same relation written with the schema that holds it are the
     * one object. A signature is part of the identity where the kind has one.
     */
    private String dropTargetIdentity(DropStmt stmt) {
        String written = stmt.schema() != null ? stmt.schema()
                : SchemaQualifier.qualifierOf(stmt.name());
        String schema = written != null ? written : executor.defaultSchema();
        return stmt.objectType() + ":" + schema.toLowerCase() + "."
                + RelationNamespace.bareName(stmt.name()).toLowerCase()
                + (stmt.paramTypes() == null ? "" : stmt.paramTypes().toString().toLowerCase());
    }

    private QueryResult executeDropOne(DropStmt stmt, Set<String> together) {
        // A DROP that names a schema of its own is looking in that schema, so a schema which is
        // not there is what is missing — PostgreSQL reports 3F000 rather than naming an object
        // it never went looking for. SCHEMA and EXTENSION name no schema of their own.
        if (stmt.objectType() != DropStmt.ObjectType.SCHEMA
                && stmt.objectType() != DropStmt.ObjectType.EXTENSION
                && ddl.tableExecutor.checkDropSchemaExists(stmt.schema(), stmt.ifExists())) {
            return QueryResult.command(QueryResult.Type.DROP_TABLE, 0);
        }
        switch (stmt.objectType()) {
            case VIEW:
            case MATERIALIZED_VIEW:
                dropView(stmt, together);
                break;
            case SEQUENCE:
                dropSequence(stmt);
                break;
            case INDEX:
                dropIndex(stmt);
                break;
            case FUNCTION:
            case PROCEDURE:
                dropFunction(stmt);
                break;
            case TRIGGER:
                dropTrigger(stmt);
                break;
            case TYPE:
                dropType(stmt);
                break;
            case SCHEMA:
                dropSchema(stmt);
                break;
            case DOMAIN: {
                // DROP DOMAIN b.d drops b's and leaves a's; a bare name is the search path's.
                String written = typeRef(stmt.schema(), stmt.name());
                String key = TypeNamespace.resolveParts(executor.database, executor.session,
                        stmt.schema(), stmt.name());
                if (key == null || !executor.database.getDomains().containsKey(key)) {
                    // A type of this name that is not a domain is the wrong kind of object for
                    // DROP DOMAIN, not a missing one, and IF EXISTS does not make it right.
                    if (key != null) {
                        throw PgErrors.wrongObjectType("\"" + written + "\" is not a domain");
                    }
                    if (!stmt.ifExists()) {
                        throw new MemgresException(
                                "type \"" + written + "\" does not exist", "42704");
                    }
                    noticeSkipped("type \"" + written + "\"");
                    break;
                }
                // A column declared as the domain depends on it exactly as one declared as an
                // enum depends on that, and blocks the drop the same way.
                refuseOrCascadeTypeDependents(stmt, key);
                inStoredTypes(() -> executor.database.getDomains().remove(key));
                executor.database.unregisterSchemaObject(TypeNamespace.schemaOfKey(key),
                        "domain", TypeNamespace.nameOfKey(key));
                executor.database.addComment("type", key, null);
                executor.identity().typeDropped("d", key);
                break;
            }
            case POLICY:
                dropPolicy(stmt);
                break;
            case RULE:
                dropRule(stmt);
                break;
            case AGGREGATE: {
                PgAggregate agg = executor.database.getAggregate(stmt.name());
                // Dropping by name alone would take down an aggregate of a different signature
                if (agg == null
                        || (stmt.paramTypes() != null && !aggregateArgsMatch(agg, stmt.paramTypes()))) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("aggregate " + stmt.name() + "("
                                + canonicalTypeList(stmt.paramTypes()) + ") does not exist", "42883");
                    }
                    break;
                }
                executor.database.removeAggregate(stmt.name());
                break;
            }
            case EXTENSION: {
                if (!objectExists("extension", stmt.name())) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException(
                                "extension \"" + stmt.name() + "\" does not exist", "42704");
                    }
                    noticeSkipped("extension \"" + stmt.name() + "\"");
                }
                executor.database.removeExtension(stmt.name());
                break;
            }
            case COLLATION: {
                // Which locale-derived collations a machine has is a property of the machine, so
                // an unknown name is not something to refuse — but IF EXISTS still says what it
                // skipped when memgres has no collation of that name.
                if (executor.database.getCollation(stmt.name()) == null) {
                    if (stmt.ifExists()) noticeSkipped("collation \"" + stmt.name() + "\"");
                    break;
                }
                // A column that sorts by this collation is written in terms of it, and
                // PostgreSQL will not take away something another object depends on. Dropping
                // nothing at all left the collation answering for a name that had been removed.
                if (!stmt.cascade() && columnCollatedBy(stmt.name()) != null) {
                    throw new MemgresException("cannot drop collation " + stmt.name()
                            + " because other objects depend on it", "2BP01");
                }
                executor.database.removeCollation(stmt.name());
                break;
            }
            case CONVERSION: {
                break; // no-op
            }
            case CAST: {
                // name is encoded as "sourceType->targetType"
                String castName = stmt.name();
                if (castName != null && castName.contains("->")) {
                    String[] parts = castName.split("->");
                    int srcOid = resolveTypeOid(parts[0].trim());
                    int tgtOid = resolveTypeOid(parts[1].trim());
                    boolean exists = executor.database.getUserDefinedCasts().stream()
                            .anyMatch(c -> (int) c[0] == srcOid && (int) c[1] == tgtOid);
                    if (!exists && !stmt.ifExists()) {
                        throw new MemgresException(
                                "cast from type " + DataType.canonicalName(parts[0])
                                + " to type " + DataType.canonicalName(parts[1]) + " does not exist",
                                "42704");
                    }
                    if (exists) {
                        executor.database.removeUserCast(srcOid, tgtOid);
                    }
                }
                break;
            }
            case OPERATOR: {
                // name is encoded as "schema.opname(leftarg,rightarg)" by the parser, which
                // assumes public for an unqualified name; the operator lives wherever the
                // search path put it.
                String opKey = stmt.name();
                if (!executor.database.hasOperator(opKey) && opKey.startsWith("public.")) {
                    String searchPathKey = executor.defaultSchema().toLowerCase()
                            + opKey.substring("public".length());
                    if (executor.database.hasOperator(searchPathKey)) opKey = searchPathKey;
                }
                if (!executor.database.hasOperator(opKey)) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("operator does not exist: " + stmt.name(), "42704");
                    }
                }
                executor.database.removeOperator(opKey);
                break;
            }
            case OPERATOR_FAMILY: {
                String famMethod = stmt.onTable() != null ? stmt.onTable() : "btree";
                String famKey = stmt.name().toLowerCase() + ":" + famMethod.toLowerCase();
                if (!executor.database.hasOperatorFamily(famKey)) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("operator family \"" + stmt.name()
                                + "\" does not exist for access method \"" + famMethod + "\"", "42704");
                    }
                } else {
                    // CASCADE: also drop operator classes in this family
                    if (stmt.cascade()) {
                        executor.database.removeOperatorClassesByFamily(stmt.name());
                    }
                    executor.database.removeOperatorFamily(famKey);
                }
                break;
            }
            case OPERATOR_CLASS: {
                String clsMethod = stmt.onTable() != null ? stmt.onTable() : "btree";
                String clsKey = stmt.name().toLowerCase() + ":" + clsMethod.toLowerCase();
                if (!executor.database.hasOperatorClass(clsKey)) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("operator class \"" + stmt.name()
                                + "\" does not exist for access method \"" + clsMethod + "\"", "42704");
                    }
                }
                executor.database.removeOperatorClass(clsKey);
                break;
            }
        }
        return QueryResult.command(QueryResult.Type.DROP_TABLE, 0);
    }

    /**
     * The schema a DROP VIEW complaint is about: the one holding the view when there is one, the
     * one the statement wrote, or the first on the search path that holds a relation of the name.
     */
    private String dropTargetSchema(DropStmt stmt, Database.ViewDef existing) {
        if (existing != null && existing.schemaName() != null) return existing.schemaName();
        if (stmt.schema() != null) return stmt.schema();
        String written = SchemaQualifier.qualifierOf(stmt.name());
        if (written != null) return written;
        String holder = RelationNamespace.schemaHolding(executor.database,
                executor.relationSearchPath(), stmt.name());
        return holder != null ? holder : executor.defaultSchema();
    }

    private void dropView(DropStmt stmt, Set<String> together) {
        // A materialized view is a different kind of object from a view, and dropping one by the
        // wrong name would destroy stored data on what is usually a typo.
        Database.ViewDef existing = executor.database.getView(stmt.name());
        boolean wantMaterialized = stmt.objectType() == DropStmt.ObjectType.MATERIALIZED_VIEW;
        // A sequence or an index of this name is the wrong kind of relation too, and IF EXISTS
        // does not turn the wrong kind into "nothing to do". The complaint has to be made about
        // the schema the name really reaches, or a qualified DROP is told nothing is amiss.
        RelationNamespace.requireKindForDrop(executor.database, dropTargetSchema(stmt, existing),
                RelationNamespace.bareName(stmt.name()),
                wantMaterialized ? RelationNamespace.MATVIEW : RelationNamespace.VIEW);
        if (existing != null && existing.materialized() != wantMaterialized) {
            throw new MemgresException("\"" + stmt.name() + "\" is not a "
                    + (wantMaterialized ? "materialized view" : "view"), "42809");
        }
        // A sequence, an index or a composite type of this name is the wrong kind of relation
        // too, and IF EXISTS does not turn the wrong kind into "nothing to do". A written
        // qualifier names the one schema to look in.
        RelationNamespace.requireKind(executor.database, dropLookupSchema(stmt), stmt.name(),
                wantMaterialized ? RelationNamespace.MATVIEW : RelationNamespace.VIEW);
        if (existing == null) {
            if (!stmt.ifExists()) {
                if (ddl.resolveTableOrNull(stmt.name()) != null) {
                    throw new MemgresException("\"" + stmt.name() + "\" is not a "
                            + (wantMaterialized ? "materialized view" : "view"), "42809");
                }
                throw new MemgresException((wantMaterialized ? "materialized view \"" : "view \"")
                        + stmt.name() + "\" does not exist", "42P01");
            }
            noticeSkipped((wantMaterialized ? "materialized view \"" : "view \"")
                    + stmt.name() + "\"");
        }
        Database.ViewDef oldView = executor.database.getView(stmt.name());
        String dropViewSchema = (oldView != null && oldView.schemaName() != null)
                ? oldView.schemaName() : executor.defaultSchema();
        if (oldView != null) {
            executor.recordUndo(new Session.DropViewUndo(stmt.name(), oldView,
                    executor.database.getTriggersForTable(dropViewSchema,
                            RelationNamespace.bareName(stmt.name())),
                    executor.database.snapshotRulesOn(dropViewSchema,
                            RelationNamespace.bareName(stmt.name()))));
        }
        // A view is a relation like any other: what reads it depends on it, so dropping it
        // blocks on those readers and CASCADE takes them with it.
        String bareViewName = RelationNamespace.bareName(stmt.name());
        List<String> viewDependents = oldView == null || stmt.cascade() ? Cols.listOf()
                : ViewDependencies.dependencyLines(executor.database, executor.systemCatalog,
                        dropViewSchema, bareViewName,
                        wantMaterialized ? "materialized view" : "view",
                        executor.searchPathSchemas(), together);
        if (!viewDependents.isEmpty()) {
            MemgresException e = new MemgresException(cannotDropMessage(together.size() > 1,
                    (wantMaterialized ? "materialized view " : "view ") + bareViewName), "2BP01");
            e.setDetail(dependencyDetail(viewDependents));
            e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
            throw e;
        }
        if (oldView != null && stmt.cascade()) {
            List<String> cascaded = new ArrayList<>();
            for (String dependent : ViewDependencies.cascadeDependents(
                    executor.database, executor.systemCatalog, dropViewSchema, bareViewName)) {
                cascaded.add("view " + RelationNamespace.bareName(dependent));
                executor.database.removeView(dependent);
            }
            noticeDropCascades(executor, cascaded);
        }
        executor.database.removeObjectOwner("view:" + dropViewSchema + "." + stmt.name());
        // A trigger belongs to the relation it watches, so an INSTEAD OF trigger goes with the
        // view. Leaving it registered kept the dependency it records on its function alive, and
        // the function could then never be dropped -- for a trigger on a relation that was gone.
        executor.database.removeTriggersForTable(dropViewSchema, bareViewName);
        // A rule belongs to the relation it is written on, and a view is a relation like any
        // other. Leaving one registered kept pg_rules describing a rule on a relation that was no
        // longer there, and a table created under the name afterwards inherited it.
        executor.database.dropRulesOn(dropViewSchema, bareViewName);
        executor.database.removeView(stmt.name());
    }

    private void dropSequence(DropStmt stmt) {
        String seqName = stmt.name();
        // A written qualifier names the one schema to look in. Dropping the sequence some other
        // schema happens to hold under that name destroys an object the statement never named.
        // The qualifier reaches here either parsed off into stmt.schema() or still on the name.
        int dot = seqName.lastIndexOf('.');
        String writtenSchema = stmt.schema() != null ? stmt.schema()
                : (dot > 0 ? seqName.substring(0, dot) : null);
        String bareSeqName = dot > 0 ? seqName.substring(dot + 1) : seqName;
        SchemaQualifier.requireSchema(executor.database, executor.session, writtenSchema);
        // pg_temp is the alias this session's temporary schema answers to, not a schema name.
        writtenSchema = SchemaQualifier.resolveAlias(executor.session, writtenSchema);
        Sequence found = executor.database.resolveSequence(executor.relationSearchPath(),
                writtenSchema != null ? writtenSchema + "." + bareSeqName : bareSeqName);
        String seqSchema = found != null ? found.getSchemaName()
                : (writtenSchema != null ? writtenSchema : executor.defaultSchema());
        RelationNamespace.requireKindForDrop(executor.database, seqSchema, bareSeqName,
                RelationNamespace.SEQUENCE);
        if (found == null) {
            if (!stmt.ifExists()) {
                if (ddl.resolveTableOrNull(bareSeqName) != null || executor.database.hasView(bareSeqName)) {
                    throw new MemgresException("\"" + bareSeqName + "\" is not a sequence", "42809");
                }
                throw new MemgresException("sequence \"" + bareSeqName + "\" does not exist", "42P01");
            }
            noticeSkipped("sequence \"" + bareSeqName + "\"");
            return;
        }
        // Check for dependent columns
        List<SequenceDependent> dependents = findSequenceDependents(found);
        List<String> visibleSchemas = executor.searchPathSchemas();
        if (!dependents.isEmpty() && !stmt.cascade()) {
            // PostgreSQL names an object the search path does not reach by its schema too, so the
            // reader can tell which of two same-named sequences the complaint is about.
            boolean visible = visibleSchemas.contains(seqSchema.toLowerCase());
            String shown = visible ? bareSeqName : seqSchema + "." + bareSeqName;
            MemgresException e = new MemgresException("cannot drop sequence " + shown
                    + " because other objects depend on it", "2BP01");
            List<String> lines = new ArrayList<>();
            for (SequenceDependent dep : dependents) {
                lines.add("default value for column " + dep.columnName() + " of table "
                        + dep.tableRef(visibleSchemas) + " depends on sequence " + shown);
            }
            e.setDetail(dependencyDetail(lines));
            e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
            throw e;
        }
        // CASCADE: remove the default from dependent columns
        if (stmt.cascade()) {
            List<String> cascaded = new ArrayList<>();
            for (SequenceDependent dep : dependents) {
                cascaded.add("default value for column " + dep.columnName() + " of table "
                        + dep.tableRef(visibleSchemas));
            }
            noticeDropCascades(executor, cascaded);
            for (SequenceDependent dep : dependents) {
                dep.column.setDefaultValue(null);
            }
        }
        executor.recordUndo(new Session.DropSequenceUndo(found.qualifiedName(), found));
        executor.database.removeSequence(seqSchema, bareSeqName);
        executor.database.removeObjectOwner("sequence:" + bareSeqName);
    }

    /** A column whose default draws from a particular sequence, and the table that holds it. */
    private static final class SequenceDependent {
        final String schemaName;
        final Table table;
        final Column column;
        SequenceDependent(String schemaName, Table table, Column column) {
            this.schemaName = schemaName;
            this.table = table;
            this.column = column;
        }
        String columnName() { return column.getName(); }

        /**
         * The table named the way PostgreSQL names it in a dependency message: by its bare name
         * when the search path reaches it, and schema-qualified when it does not, so the reader
         * can tell which of two same-named tables the message is about.
         */
        String tableRef(java.util.List<String> searchPath) {
            return searchPath.contains(schemaName.toLowerCase())
                    ? table.getName() : schemaName + "." + table.getName();
        }
    }

    /**
     * The columns whose default draws from this sequence, in whatever schema they live.
     *
     * <p>A default written without a qualifier resolved through the search path when the column
     * was created, so a table in one schema can perfectly well depend on a sequence in another —
     * which is exactly the case PostgreSQL refuses to drop out from under. Looking only in the
     * sequence's own schema let the drop succeed and left the default pointing at nothing.
     */
    private List<SequenceDependent> findSequenceDependents(Sequence seq) {
        // PostgreSQL reports these in the order it recorded them: the tables in the order they
        // were created, and within one table its columns from the first to the last. Walking the
        // schema maps reported them in whatever order those maps happened to hold them, which for
        // three tables drawing on one sequence was neither. Each column is kept beside its
        // relation's OID -- which follows creation order -- and its position, and the list is put
        // in that order once it is complete.
        List<Object[]> found = new ArrayList<>();
        for (java.util.Map.Entry<String, Schema> se : executor.database.getSchemas().entrySet()) {
            for (Table tbl : se.getValue().getTables().values()) {
                int relOid = executor.systemCatalog.getOid(
                        "rel:" + se.getKey() + "." + tbl.getName());
                List<Column> cols = tbl.getColumns();
                for (int i = 0; i < cols.size(); i++) {
                    Column col = cols.get(i);
                    String written = Sequence.nameInDefault(col.getDefaultValue());
                    if (written == null) continue;
                    if (executor.database.getSequenceFor(se.getKey(), written) == seq) {
                        found.add(new Object[]{relOid, i,
                                new SequenceDependent(se.getKey(), tbl, col)});
                    }
                }
            }
        }
        java.util.Collections.sort(found, new java.util.Comparator<Object[]>() {
            @Override
            public int compare(Object[] a, Object[] b) {
                int byRelation = Integer.compare((Integer) a[0], (Integer) b[0]);
                return byRelation != 0 ? byRelation
                        : Integer.compare((Integer) a[1], (Integer) b[1]);
            }
        });
        List<SequenceDependent> result = new ArrayList<>();
        for (Object[] entry : found) result.add((SequenceDependent) entry[2]);
        return result;
    }

    /**
     * Point every column that draws from this sequence at the name it now answers to.
     *
     * <p>A default holds the sequence's name as text, so a sequence that is renamed or moved to
     * another schema leaves every default that named it pointing at nothing, and the next INSERT
     * fails on the default's own text. PostgreSQL's defaults reference the sequence itself and
     * follow it wherever it goes; rewriting the text is how the same thing is said here.
     */
    private void retargetSequenceDefaults(List<SequenceDependent> dependents, Sequence seq) {
        for (SequenceDependent dep : dependents) {
            String written = dep.schemaName.equalsIgnoreCase(seq.getSchemaName())
                    ? seq.getName() : seq.qualifiedName();
            String def = dep.column.getDefaultValue();
            String old = Sequence.nameInDefault(def);
            if (old == null) continue;
            dep.column.setDefaultValue(def.contains(":seq:")
                    ? def.substring(0, def.indexOf(":seq:") + 5) + written
                    : "nextval('" + written + "'::regclass)");
        }
    }

    private void dropIndex(DropStmt stmt) {
        String bareIndexName = RelationNamespace.bareName(stmt.name());
        String written = stmt.schema() != null ? stmt.schema() + "." + bareIndexName : stmt.name();
        // The index this statement names, in the schema it wrote or the first on the search path
        // that holds one. Everything below works on that one index and no other of the name.
        String indexKey = executor.database.resolveIndexName(executor.relationSearchPath(), written);
        String indexSchema = indexKey != null ? Database.idxSchema(indexKey)
                : (stmt.schema() != null ? stmt.schema()
                    : stmt.name().contains(".")
                        ? stmt.name().substring(0, stmt.name().lastIndexOf('.'))
                        : executor.defaultSchema());
        RelationNamespace.requireKindForDrop(executor.database, indexSchema,
                bareIndexName, RelationNamespace.INDEX);
        // The index behind a PRIMARY KEY or UNIQUE constraint belongs to the constraint, which
        // needs it: it goes when the constraint does, and not before. Only the index the
        // constraint made for itself is protected — an index somebody wrote a CREATE INDEX for
        // is theirs to drop, even when a constraint of the same name was recorded beside it.
        Schema indexHome = executor.database.getSchema(indexSchema);
        if (indexHome != null && indexKey == null) {
            for (Table owner : indexHome.getTables().values()) {
                for (StoredConstraint sc : owner.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                            && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
                    if (sc.getName() == null || !sc.getName().equalsIgnoreCase(bareIndexName)) continue;
                    MemgresException e = new MemgresException("cannot drop index " + bareIndexName
                            + " because constraint " + sc.getName() + " on table "
                            + owner.getName() + " requires it", "2BP01");
                    // The index belongs to the constraint, so the constraint is the thing there
                    // is to drop, and PostgreSQL names the statement that would work.
                    e.setHint("You can drop constraint " + sc.getName() + " on table "
                            + owner.getName() + " instead.");
                    throw e;
                }
            }
        }
        // An unqualified name is looked for through the search path, so an index in a schema
        // that is not on it is not found — dropping it needs the schema written out.
        boolean visible = indexKey != null;
        if (!visible) {
            if (!stmt.ifExists()) {
                throw new MemgresException("index \"" + bareIndexName + "\" does not exist", "42704");
            }
            noticeSkipped("index \"" + bareIndexName + "\"");
            return;
        }
        String storedTable = executor.database.getIndexTable(indexKey);
        if (storedTable != null) {
            try {
                int dotIdx = storedTable.indexOf('.');
                String schema = dotIdx >= 0 ? storedTable.substring(0, dotIdx) : "public";
                String tableName = dotIdx >= 0 ? storedTable.substring(dotIdx + 1) : storedTable;
                Table t = executor.resolveTable(schema, tableName);
                t.getConstraints().removeIf(sc -> sc.getName().equalsIgnoreCase(bareIndexName));
            } catch (MemgresException ignored) {}
        }
        executor.database.removeIndex(indexSchema, bareIndexName);
    }

    private void dropFunction(DropStmt stmt) {
        // An unqualified DROP only reaches the schemas the search_path makes visible.
        String schema = stmt.schema() != null ? stmt.schema() : visibleSchemaOfFunction(stmt.name());
        List<PgFunction> candidates = schema != null
                ? executor.database.getFunctionOverloads(schema, stmt.name())
                : Cols.<PgFunction>listOf();
        if (candidates.isEmpty()) {
            if (!stmt.ifExists()) {
                throw new MemgresException("function " + stmt.name() + "() does not exist", "42883");
            }
            noticeSkipped((stmt.objectType() == DropStmt.ObjectType.PROCEDURE
                    ? "procedure " : "function ") + stmt.name()
                    + "(" + pgArgumentList(stmt.paramTypes()) + ")");
            return;
        }
        // A trigger runs its function every time the relation it sits on is written, so the
        // function is not the definer's alone to drop: PostgreSQL refuses while a trigger depends
        // on it, names the trigger, and takes the trigger along when CASCADE is written.
        refuseOrCascadeTriggerDependents(stmt);
        if (stmt.paramTypes() != null) {
            executor.database.removeFunction(schema, stmt.name(), stmt.paramTypes());
        } else {
            executor.database.removeFunction(schema, stmt.name());
        }
        // DDL is transactional, so a DROP whose transaction rolls back never happened. Only the
        // overloads that actually went are recorded: a DROP by signature leaves the rest standing,
        // and putting those back would resurrect routines nobody dropped.
        List<PgFunction> survivors = executor.database.getFunctionOverloads(schema, stmt.name());
        List<PgFunction> removed = new ArrayList<>();
        for (PgFunction candidate : candidates) {
            boolean survives = false;
            for (PgFunction s : survivors) {
                if (s == candidate) { survives = true; break; }
            }
            if (!survives) removed.add(candidate);
        }
        if (!removed.isEmpty()) {
            executor.recordUndo(new Session.DropFunctionUndo(schema, stmt.name(), removed));
        }
        if (executor.database.getFunctionOverloads(stmt.name()).isEmpty()) {
            executor.database.removeObjectOwner("function:" + stmt.name());
        }
    }

    /**
     * The triggers that execute a function being dropped. PostgreSQL records the dependency when
     * the trigger is created, so the function cannot go while a trigger is still there to call it
     * -- the trigger would fire into nothing and the write it was watching would go through in
     * silence -- and CASCADE drops the trigger with it.
     */
    private void refuseOrCascadeTriggerDependents(DropStmt stmt) {
        // A trigger's function takes no arguments, so a DROP that names any is naming a different
        // overload and no trigger depends on that one.
        if (stmt.paramTypes() != null && !stmt.paramTypes().isEmpty()) return;
        List<PgTrigger> dependents = new ArrayList<>();
        for (List<PgTrigger> onOneRelation : executor.database.getAllTriggers().values()) {
            for (PgTrigger t : onOneRelation) {
                // A partition's copy of a trigger goes with the trigger it was cloned from, so it
                // is not a dependent of its own.
                if (t.getClonedFromTable() != null || t.getFunctionName() == null) continue;
                if (RelationNamespace.bareName(t.getFunctionName()).equalsIgnoreCase(stmt.name())) {
                    dependents.add(t);
                }
            }
        }
        if (dependents.isEmpty()) return;
        String written = stmt.name() + "(" + pgArgumentList(stmt.paramTypes()) + ")";
        if (!stmt.cascade()) {
            List<String> lines = new ArrayList<>();
            for (PgTrigger t : dependents) {
                lines.add(triggerOnRelation(t) + " depends on function " + written);
            }
            MemgresException e = new MemgresException("cannot drop function " + written
                    + " because other objects depend on it", "2BP01");
            e.setDetail(dependencyDetail(lines));
            e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
            throw e;
        }
        List<String> cascaded = new ArrayList<>();
        for (PgTrigger t : dependents) {
            cascaded.add(triggerOnRelation(t));
            executor.database.removeTrigger(t.getName(), t.getTableName());
        }
        noticeDropCascades(executor, cascaded);
    }

    /**
     * A trigger named the way PostgreSQL names it when it reports a dependency, which says what
     * kind of relation the trigger watches. An INSTEAD OF trigger sits on a view, and calling that
     * a table named an object of a kind the database does not hold under that name.
     */
    private String triggerOnRelation(PgTrigger t) {
        String relation = RelationNamespace.bareName(t.getTableName());
        Database.ViewDef view = executor.database.getView(relation);
        String kind = view == null ? "table" : (view.materialized() ? "materialized view" : "view");
        return "trigger " + t.getName() + " on " + kind + " " + relation;
    }

    /**
     * The argument list as PostgreSQL prints it when it names a routine that is not there. It
     * echoes the type as it was written, except that the names the grammar keeps as aliases for a
     * built-in type are printed as that type's own name in pg_catalog.
     */
    private static String pgArgumentList(List<String> paramTypes) {
        if (paramTypes == null || paramTypes.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String t : paramTypes) {
            if (sb.length() > 0) sb.append(",");
            String written = t == null ? "" : t.trim().toLowerCase();
            String internal = GRAMMAR_TYPE_ALIASES.get(written);
            sb.append(internal != null ? "pg_catalog." + internal : written);
        }
        return sb.toString();
    }

    /** Type names PostgreSQL's grammar reads as an alias for a catalog type. */
    private static final Map<String, String> GRAMMAR_TYPE_ALIASES = grammarTypeAliases();

    private static Map<String, String> grammarTypeAliases() {
        Map<String, String> m = new HashMap<>();
        m.put("int", "int4");
        m.put("integer", "int4");
        m.put("smallint", "int2");
        m.put("bigint", "int8");
        m.put("real", "float4");
        m.put("float", "float8");
        m.put("double precision", "float8");
        m.put("boolean", "bool");
        m.put("decimal", "numeric");
        m.put("dec", "numeric");
        m.put("numeric", "numeric");
        m.put("varchar", "varchar");
        m.put("character varying", "varchar");
        m.put("char", "bpchar");
        m.put("character", "bpchar");
        m.put("timestamp", "timestamp");
        m.put("time", "time");
        return m;
    }

    /** First schema on the search_path that holds a function of this name, or null if none does. */
    private String visibleSchemaOfFunction(String name) {
        List<PgFunction> all = executor.database.getFunctionOverloads(name);
        if (all.isEmpty()) return null;
        for (String schema : executor.searchPathSchemas()) {
            for (PgFunction f : all) {
                if (Database.schemaOf(f).equalsIgnoreCase(schema)) return schema;
            }
        }
        return null;
    }

    private void dropTrigger(DropStmt stmt) {
        if (stmt.onTable() != null) {
            // The relation may be written with the schema that holds it, and the trigger registry
            // is keyed by the relation's bare name. PostgreSQL names the relation as it was
            // written when it is not there, and without its schema when the trigger is not.
            String written = stmt.onTable();
            String onSchema = executor.relationSchemaOf(null, written);
            String onTable = RelationNamespace.bareName(written);
            List<PgTrigger> tableTriggers = executor.database.getTriggersForTable(onSchema, onTable);
            PgTrigger named = null;
            for (PgTrigger t : tableTriggers) {
                if (t.getName().equalsIgnoreCase(stmt.name())) { named = t; break; }
            }
            // A partition's copy of its parent's trigger is the parent's to drop: dropping the
            // copy alone would leave the partition out of a trigger the partitioned table still
            // declares, so PostgreSQL refuses it and names the drop that takes both. CASCADE does
            // not soften it, because the copy is not an object that depends on the original but
            // part of it.
            if (named != null && named.getClonedFromTable() != null) {
                MemgresException required = new MemgresException("cannot drop trigger " + stmt.name()
                        + " on table " + onTable + " because trigger " + stmt.name()
                        + " on table " + named.getClonedFromTable() + " requires it", "2BP01");
                required.setHint("You can drop trigger " + stmt.name() + " on table "
                        + named.getClonedFromTable() + " instead.");
                throw required;
            }
            boolean found = named != null;
            if (!found) {
                // A trigger is named by its relation, so a relation that is not there is what is
                // missing — PostgreSQL names that rather than the trigger it never looked for.
                boolean relationThere =
                        RelationNamespace.kindOf(executor.database, onSchema, onTable) != null;
                if (!stmt.ifExists()) {
                    if (!relationThere) {
                        throw new MemgresException(
                                "relation \"" + written + "\" does not exist", "42P01");
                    }
                    throw new MemgresException("trigger \"" + stmt.name() + "\" for table \"" + onTable + "\" does not exist", "42704");
                }
                noticeSkipped(relationThere
                        ? "trigger \"" + stmt.name() + "\" for relation \"" + onTable + "\""
                        : "relation \"" + written + "\"");
            }
            executor.database.removeTrigger(stmt.name(), onTable);
        }
    }

    /**
     * Dropping a type a column is declared as would leave that column pointing at nothing, so
     * without CASCADE the columns block the drop and with it they are taken along.
     */
    private void refuseOrCascadeTypeDependents(DropStmt stmt, String key) {
        List<String> dependents = columnsDeclaredAsType(key);
        if (dependents.isEmpty()) return;
        String display = TypeNamespace.display(executor.database, executor.session, key);
        if (!stmt.cascade()) {
            // One dependent per line of a single detail. Written as repeated labelled sections
            // only the first of them reached the client and the rest stayed inside the message.
            List<String> lines = new ArrayList<>();
            for (String d : dependents) {
                lines.add(d + " depends on type " + display);
            }
            MemgresException e = new MemgresException("cannot drop type " + display
                    + " because other objects depend on it", "2BP01");
            e.setDetail(dependencyDetail(lines));
            e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
            throw e;
        }
        noticeDropCascades(executor, dependents);
    }

    /** The schema a DROP looks in: the one it named, or the session's own. */
    private String dropLookupSchema(DropStmt stmt) {
        return stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
    }

    /**
     * Change the stored type maps rather than what this session may see of them.
     *
     * <p>The enums, composite types and domains a session reads are filtered to the ones visible
     * to it, and while another session holds uncommitted DDL that reading is a copy of the stored
     * map rather than the map itself. Removing a key from the copy threw the copy away: the drop
     * reported success and the type stayed where it was, still usable under a name PostgreSQL had
     * already taken away from every session. Taking a type away, or moving it to another name or
     * schema, is not a question of what this session can see, so it is done with no viewer bound
     * and reaches the stored map whatever anyone else is in the middle of.
     */
    private void inStoredTypes(Runnable change) {
        Session viewer = Database.bindViewer(null);
        try {
            change.run();
        } finally {
            Database.bindViewer(viewer);
        }
    }

    private void dropType(DropStmt stmt) {
        // Which schema's type this is settles first: DROP TYPE b.e drops b's and leaves a's,
        // and a bare name is the search path's to answer.
        String written = typeRef(stmt.schema(), stmt.name());
        // The schema and the name stay apart: an unqualified name is the search path's to answer,
        // and reaching past it would drop a type this session cannot even see.
        String key = TypeNamespace.resolveParts(executor.database, executor.session,
                stmt.schema(), stmt.name());
        boolean isEnum = key != null && executor.database.getCustomEnums().containsKey(key);
        boolean isComposite = key != null && executor.database.getCompositeTypes().containsKey(key);
        boolean isRange = key != null && executor.database.getRangeTypes().containsKey(key);
        boolean isShell = key != null && executor.database.getShellTypes().contains(key);
        // DROP TYPE takes a domain too: PostgreSQL has one type namespace and DROP TYPE names it.
        boolean isDomain = key != null && executor.database.getDomains().containsKey(key);
        if (!isEnum && !isComposite && !isRange && !isShell && !isDomain) {
            // A table, a view and a materialized view each own a row type of their own name, and
            // that row type is not something DROP TYPE may take away on its own. IF EXISTS does
            // not soften it: the type is there, it is just not one this statement may drop.
            String lookIn = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
            String owner = TypeNamespace.rowTypeOwner(executor.database, lookIn, stmt.name());
            if (owner != null) {
                MemgresException e = new MemgresException("cannot drop type " + written
                        + " because " + owner + " " + written + " requires it", "2BP01");
                // A row type goes when its relation does, so PostgreSQL names the drop that
                // would take both rather than leaving the reader with no way forward.
                e.setHint("You can drop " + owner + " " + written + " instead.");
                throw e;
            }
            if (!stmt.ifExists()) {
                throw new MemgresException("type \"" + written + "\" does not exist", "42704");
            }
            noticeSkipped("type \"" + written + "\"");
            return;
        }
        refuseOrCascadeTypeDependents(stmt, key);
        String schema = TypeNamespace.schemaOfKey(key);
        String bare = TypeNamespace.nameOfKey(key);
        // The OIDs go with the type: PostgreSQL never hands a dropped one to a type created later
        // under the same name.
        if (isEnum) {
            inStoredTypes(() -> executor.database.getCustomEnums().remove(key));
            executor.identity().typeDropped("e", key);
        }
        if (isComposite) {
            inStoredTypes(() -> executor.database.getCompositeTypes().remove(key));
            executor.identity().typeDropped("c", key);
        }
        if (isRange) executor.database.getRangeTypes().remove(key);
        if (isShell) executor.database.getShellTypes().remove(key);
        if (isDomain) {
            inStoredTypes(() -> executor.database.getDomains().remove(key));
            executor.identity().typeDropped("d", key);
        }
        executor.database.unregisterSchemaObject(schema,
                isEnum ? "enum" : isComposite ? "composite" : isRange ? "range"
                        : isDomain ? "domain" : "shell", bare);
        executor.database.addComment("type", key, null);
    }

    /** A written type name: the schema the statement gave, when it gave one, and the name. */
    static String typeRef(String schema, String name) {
        if (name == null) return null;
        if (schema == null || TypeNamespace.writtenSchema(name) != null) return name;
        return schema.toLowerCase() + "." + name;
    }

    /**
     * Every table column whose declared type is the type stored under {@code key}, described the
     * way PostgreSQL describes a column that stands in the way of a drop — {@code column c of
     * table t}, with the table schema-qualified where the search path does not reach it. A column
     * records the type it was declared with under the same key, so a column of a.e is not found
     * by dropping b.e.
     */
    private List<String> columnsDeclaredAsType(String key) {
        // PostgreSQL reports dependencies in the order it recorded them: relations in the order
        // they were created, and within one relation its columns from the last back to the first.
        // Walking the schema maps reported them in whatever order those maps happened to hold
        // them, which for two tables built on the same type was neither order. Each entry is kept
        // beside the relation's OID -- which follows creation order -- and its attnum, and the
        // list is put in that order once it is complete.
        List<Object[]> found = new ArrayList<>();
        List<String> visible = executor.searchPathSchemas();
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table t : schema.getTables().values()) {
                int relOid = executor.systemCatalog.getOid(
                        "rel:" + schema.getName() + "." + t.getName());
                // A typed table depends on the whole type, not on one column of it: its shape is
                // the type's, so dropping the type would leave the table with no definition at
                // all. PostgreSQL names the table itself rather than any of its columns.
                if (sameType(key, t.getOfTypeName())) {
                    found.add(new Object[]{relOid, 0, "table "
                            + RelationNamespace.shownName(visible, schema.getName(), t.getName())});
                }
                List<Column> cols = t.getColumns();
                for (int i = 0; i < cols.size(); i++) {
                    Column c = cols.get(i);
                    // A domain is a type like any other, and a column declared as one depends on
                    // it exactly as a column declared as an enum depends on that.
                    if (sameType(key, c.getEnumTypeName()) || sameType(key, c.getCompositeTypeName())
                            || sameType(key, c.getDomainTypeName())) {
                        found.add(new Object[]{relOid, i + 1, "column " + c.getName() + " of table "
                                + RelationNamespace.shownName(visible, schema.getName(), t.getName())});
                    }
                }
            }
        }
        java.util.Collections.sort(found, new java.util.Comparator<Object[]>() {
            @Override
            public int compare(Object[] a, Object[] b) {
                int byRelation = Integer.compare((Integer) a[0], (Integer) b[0]);
                return byRelation != 0 ? byRelation
                        : Integer.compare((Integer) b[1], (Integer) a[1]);
            }
        });
        List<String> named = new ArrayList<>();
        for (Object[] entry : found) named.add((String) entry[2]);
        return named;
    }

    /** Whether a column's recorded type name denotes the type stored under {@code key}. */
    private boolean sameType(String key, String declared) {
        if (declared == null) return false;
        String resolved = TypeNamespace.find(executor.database.typeKeys(), declared);
        return key.equals(resolved);
    }

    private void dropSchema(DropStmt stmt) {
        Schema schema = executor.database.getSchema(stmt.name());
        if (schema == null && !stmt.ifExists()) {
            throw new MemgresException("schema \"" + stmt.name() + "\" does not exist", "3F000");
        }
        if (schema == null && executor.session != null) {
            // IF EXISTS says what to skip, and PostgreSQL says which one it skipped
            executor.session.addNotice("NOTICE", "00000",
                    "schema \"" + stmt.name() + "\" does not exist, skipping", null);
        }
        if (schema != null) {
            if (stmt.cascade()) {
                List<String> tableNames = new ArrayList<>(schema.getTables().keySet());
                for (String tName : tableNames) {
                    executor.database.getAllTriggers().remove(tName.toLowerCase());
                }
                for (String tName : tableNames) {
                    executor.database.removePrivilegesOnObject("TABLE",
                            AstExecutor.privilegeKey(stmt.name(), tName));
                    executor.database.removeObjectOwner("table:" + stmt.name() + "." + tName);
                }
                executor.database.removePrivilegesOnObject("SCHEMA", stmt.name());
                // CASCADE: remove FK constraints from tables in other schemas referencing dropped tables
                String droppedSchemaName = stmt.name();
                for (Schema otherSchema : executor.database.getSchemas().values()) {
                    if (otherSchema == schema) continue;
                    for (Table otherTable : otherSchema.getTables().values()) {
                        List<String> fksToRemove = new java.util.ArrayList<>();
                        for (StoredConstraint sc : otherTable.getConstraints()) {
                            if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                            String refTable = sc.getReferencesTable();
                            boolean matchesDroppedTable = false;
                            for (String tName : tableNames) {
                                if (tName.equalsIgnoreCase(refTable)) {
                                    // Check if FK explicitly references the dropped schema, or if it's unqualified
                                    if (sc.getReferencesSchema() == null
                                            || sc.getReferencesSchema().equalsIgnoreCase(droppedSchemaName)) {
                                        matchesDroppedTable = true;
                                        break;
                                    }
                                }
                            }
                            if (matchesDroppedTable) fksToRemove.add(sc.getName());
                        }
                        for (String fkName : fksToRemove) otherTable.removeConstraint(fkName);
                    }
                }
                tableNames.forEach(schema::removeTable);
                // A rule goes with the relation it is written on, and these relations have just
                // gone.
                for (String tName : tableNames) {
                    executor.database.dropRulesOn(droppedSchemaName, tName);
                }

                String schemaName = stmt.name().toLowerCase();
                Set<String> registeredObjects = new HashSet<>(executor.database.getSchemaObjects(schemaName));
                for (String entry : registeredObjects) {
                    int colonIdx = entry.indexOf(':');
                    if (colonIdx < 0) continue;
                    String objType = entry.substring(0, colonIdx);
                    String objName = entry.substring(colonIdx + 1);
                    switch (objType) {
                        // Only this schema's type goes: the same name may be another schema's.
                        case "enum":
                            executor.database.getCustomEnums()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "composite":
                            executor.database.getCompositeTypes()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "range":
                            executor.database.getRangeTypes()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "shell":
                            executor.database.getShellTypes()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "sequence":
                            // This schema's sequence of that name, not another schema's.
                            executor.database.removeSequence(schemaName, objName);
                            break;
                        case "domain":
                            executor.database.getDomains()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "index":
                            executor.database.removeIndex(schemaName, objName);
                            break;
                        case "function":
                            // Only this schema's copy goes; the same name may exist elsewhere.
                            executor.database.removeFunction(schemaName, objName);
                            break;
                        case "view":
                            executor.database.removeView(schemaName, objName);
                            break;
                    }
                }
                executor.database.removeSchemaObjects(schemaName);
            } else if (!schema.getTables().isEmpty()) {
                throw new MemgresException("cannot drop schema " + stmt.name() + " because other objects depend on it");
            }
            executor.database.removeSchema(stmt.name());
            executor.database.removeObjectOwner("schema:" + stmt.name());
        }
    }

    private void dropPolicy(DropStmt stmt) {
        if (stmt.onTable() != null) {
            // A policy is named by its relation, so IF EXISTS skips on a relation that is not
            // there just as it does on a policy that is not.
            if (stmt.ifExists() && ddl.resolveTableOrNull(stmt.onTable()) == null) {
                noticeSkipped("relation \"" + stmt.onTable() + "\"");
                return;
            }
            Table table = executor.resolveTable("public", stmt.onTable());
            // Row security is what protects the relation from the role reading it, so taking a
            // policy away is the owner's to do. PostgreSQL says relation here, not table.
            executor.requireRelationOwner(executor.defaultSchema(), stmt.onTable());
            boolean found = false;
            for (RlsPolicy p : table.getRlsPolicies()) {
                if (p.getName().equalsIgnoreCase(stmt.name())) { found = true; break; }
            }
            if (!found) {
                if (!stmt.ifExists()) {
                    throw new MemgresException("policy \"" + stmt.name() + "\" for table \"" + stmt.onTable() + "\" does not exist", "42704");
                }
                noticeSkipped("policy \"" + stmt.name() + "\" for table \"" + stmt.onTable() + "\"");
            }
            table.getRlsPolicies().removeIf(p -> p.getName().equalsIgnoreCase(stmt.name()));
        } else if (!stmt.ifExists()) {
            throw new MemgresException("must specify table for DROP POLICY");
        }
    }

    private void dropRule(DropStmt stmt) {
        // A written qualifier says which schema holds the relation the rule is on; a rule on
        // another schema's relation of that name is a different rule. PostgreSQL names the
        // relation without its schema when it reports the rule missing.
        String written = stmt.onTable() != null ? stmt.onTable() : "";
        String onSchema = executor.relationSchemaOf(null, written);
        String onTable = RelationNamespace.bareName(written);
        if (stmt.onTable() != null
                && RelationNamespace.kindOf(executor.database, onSchema, onTable) == null) {
            if (!stmt.ifExists()) {
                throw new MemgresException(
                        "relation \"" + written + "\" does not exist", "42P01");
            }
            noticeSkipped("relation \"" + written + "\"");
            return;
        }
        if (executor.database.hasRule(onSchema, stmt.name(), onTable)) {
            executor.database.removeRule(onSchema, stmt.name(), onTable);
        } else if (!stmt.ifExists()) {
            throw new MemgresException("rule \"" + stmt.name() + "\" for relation \"" + onTable + "\" does not exist", "42704");
        } else {
            noticeSkipped("rule \"" + stmt.name() + "\" for relation \"" + onTable + "\"");
        }
    }

    // ---- CREATE SEQUENCE ----

    QueryResult executeCreateSequence(CreateSequenceStmt stmt) {
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        String seqName = stmt.name();
        // A qualified name puts the sequence in the schema it names, and it is that schema's
        // relations the name has to be free of. A temporary sequence lands in this session's
        // temporary schema, which is a schema like any other.
        String seqSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        if (stmt.temporary() && executor.session != null) {
            seqSchema = executor.session.getTempSchemaName();
            // The temporary schema has to be a schema the engine really holds, or a later
            // statement that writes pg_temp is told there is no such schema to write.
            executor.database.getOrCreateSchema(seqSchema);
        }
        boolean nameOwned = executor.database.hasSequence(seqSchema, seqName)
                || RelationNamespace.kindOf(executor.database, seqSchema, seqName) != null;
        if (nameOwned) {
            if (stmt.ifNotExists()) return QueryResult.message(QueryResult.Type.SET, "CREATE SEQUENCE");
            throw new MemgresException("relation \"" + RelationNamespace.bareName(stmt.name())
                    + "\" already exists", "42P07");
        }
        requireSequenceTypeExists(stmt.getAsType());
        DdlSequenceValidator.Params p = DdlSequenceValidator.forCreate(stmt.getAsType(),
                stmt.incrementBy(), stmt.minValue(), stmt.maxValue(), stmt.startWith(), stmt.getCache());
        Sequence seq = new Sequence(seqName, p.startWith, p.incrementBy, p.minValue, p.maxValue);
        seq.setSchemaName(seqSchema);
        seq.setUnlogged(stmt.unlogged());
        DdlSequenceValidator.apply(seq, p);
        if (stmt.cycle() != null) seq.setCycle(stmt.cycle());
        if (stmt.ownedByTable() != null) applySequenceOwnedBy(seq, stmt.ownedByTable(), stmt.ownedByColumn());
        executor.database.addSequence(seq);
        executor.database.markUncommittedObject(seq, executor.session);
        executor.database.registerSchemaObject(seqSchema, "sequence", seqName);
        executor.recordUndo(new Session.CreateSequenceUndo(seq.qualifiedName()));
        executor.database.setObjectOwner("sequence:" + seqName, executor.sessionUser());
        return QueryResult.message(QueryResult.Type.SET, "CREATE SEQUENCE");
    }

    /**
     * PostgreSQL asks whether an {@code AS} name is a type at all before it asks whether it is one
     * a sequence can be built on, so an unknown name is the missing type it is (42704) and a real
     * type that is not one of the three integer types is left to the sequence's own check (22023).
     */
    private void requireSequenceTypeExists(String written) {
        if (written == null) return;
        String bare = written.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        int dot = bare.lastIndexOf('.');
        String lookup = dot >= 0 ? bare.substring(dot + 1) : bare;
        if (DataType.fromPgName(lookup) != null
                || executor.database.isCustomEnum(lookup)
                || executor.database.isDomain(lookup)
                || executor.database.isCompositeType(lookup)
                || executor.database.isRangeType(lookup)) {
            return;
        }
        throw PgErrors.undefinedObject("type", lookup);
    }

    // ---- ALTER SEQUENCE ----

    QueryResult executeAlterSequence(AlterSequenceStmt stmt) {
        int dot = stmt.name().lastIndexOf('.');
        String writtenSchema = dot > 0 ? stmt.name().substring(0, dot) : null;
        String bareName = dot > 0 ? stmt.name().substring(dot + 1) : stmt.name();
        SchemaQualifier.requireSchema(executor.database, executor.session, writtenSchema);
        // pg_temp is the alias this session's temporary schema answers to, not a schema name.
        writtenSchema = SchemaQualifier.resolveAlias(executor.session, writtenSchema);
        // A qualifier names one schema's sequence and no other's; without that, an ALTER aimed at
        // a schema that holds nothing of the name silently altered — and relocated — someone else's.
        Sequence seq = executor.database.resolveSequence(executor.relationSearchPath(),
                writtenSchema != null ? writtenSchema + "." + bareName : bareName);
        if (seq == null) {
            if (stmt.ifExists()) return QueryResult.message(QueryResult.Type.SET, "ALTER SEQUENCE");
            // A relation of that name that is not a sequence is a different complaint: the name
            // resolves, the kind is wrong, and reporting it as missing sends the reader looking
            // for an object that is right there.
            RelationNamespace.requireKind(executor.database,
                    writtenSchema != null ? writtenSchema : executor.defaultSchema(),
                    bareName, RelationNamespace.SEQUENCE);
            throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
        }
        String seqSchema = seq.getSchemaName();
        if (stmt.setSchema() != null) {
            requireSchemaExists(stmt.setSchema());
            if (!seqSchema.equalsIgnoreCase(stmt.setSchema())) {
                if (RelationNamespace.kindOf(executor.database, stmt.setSchema(), seq.getName()) != null) {
                    throw new MemgresException("relation \"" + seq.getName()
                            + "\" already exists in schema \"" + stmt.setSchema() + "\"", "42P07");
                }
                // The columns that draw from it have to be told where it went, before it goes.
                List<SequenceDependent> moved = findSequenceDependents(seq);
                executor.database.removeSequence(seqSchema, seq.getName());
                executor.database.unregisterSchemaObject(seqSchema, "sequence", seq.getName());
                seq.setSchemaName(stmt.setSchema());
                executor.database.addSequence(seq);
                executor.identity().relationRenamed("S", seqSchema, seq.getName(),
                        stmt.setSchema(), seq.getName());
                retargetSequenceDefaults(moved, seq);
            }
            executor.database.registerSchemaObject(stmt.setSchema(), "sequence", seq.getName());
            return QueryResult.message(QueryResult.Type.SET, "ALTER SEQUENCE");
        }
        if (stmt.renameTo() != null) {
            // The new name is the old one's schema plus a new bare name: a rename never moves an
            // object between schemas.
            RelationNamespace.requireFree(executor.database, seqSchema, stmt.renameTo(), null);
            if (executor.database.hasSequence(seqSchema, stmt.renameTo())) {
                throw new MemgresException("relation \"" + stmt.renameTo() + "\" already exists", "42P07");
            }
            List<SequenceDependent> renamed = findSequenceDependents(seq);
            String wasCalled = seq.getName();
            executor.database.removeSequence(seqSchema, seq.getName());
            executor.database.unregisterSchemaObject(seqSchema, "sequence", seq.getName());
            seq.setName(stmt.renameTo());
            executor.database.addSequence(seq);
            executor.database.registerSchemaObject(seqSchema, "sequence", stmt.renameTo());
            // The same sequence under a new name keeps its OID, its comment and its owner.
            executor.identity().relationRenamed("S", seqSchema, wasCalled,
                    seqSchema, stmt.renameTo());
            retargetSequenceDefaults(renamed, seq);
            return QueryResult.message(QueryResult.Type.SET, "ALTER SEQUENCE");
        }
        requireSequenceTypeExists(stmt.getAsType());
        DdlSequenceValidator.Params p = DdlSequenceValidator.forAlter(seq, stmt.getAsType(),
                stmt.incrementBy(), stmt.minValue(), stmt.maxValue(), stmt.startWith(),
                stmt.restart(), stmt.restartWith(), stmt.getCache());
        DdlSequenceValidator.apply(seq, p);
        if (stmt.cycle() != null) seq.setCycle(stmt.cycle());
        if (stmt.ownerTo() != null) {
            String newOwner = ddl.resolveOwnerName(stmt.ownerTo());
            if (!executor.database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            executor.database.setObjectOwner("sequence:" + seq.getName(), newOwner);
        }
        // M20: OWNED BY table.column
        if (stmt.ownedByTable() != null) {
            applySequenceOwnedBy(seq, stmt.ownedByTable(), stmt.ownedByColumn());
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER SEQUENCE");
    }

    /** Attach a sequence to a table column, or detach it for OWNED BY NONE. */
    private void applySequenceOwnedBy(Sequence seq, String tblName, String colName) {
        if ("NONE".equalsIgnoreCase(tblName)) {
            seq.setOwnedByTable(null);
            seq.setOwnedByColumn(null);
            return;
        }
        Table tbl = null;
        for (Schema s : executor.database.getSchemas().values()) {
            tbl = s.getTable(tblName);
            if (tbl != null) break;
        }
        if (tbl == null) throw new MemgresException("relation \"" + tblName + "\" does not exist", "42P01");
        if (colName != null && tbl.getColumnIndex(colName) < 0) {
            throw new MemgresException(
                    "column \"" + colName + "\" of relation \"" + tblName + "\" does not exist", "42703");
        }
        seq.setOwnedByTable(tblName);
        seq.setOwnedByColumn(colName);
    }

    // ---- CREATE DOMAIN ----

    QueryResult executeCreateDomain(CreateDomainStmt stmt) {
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schemaName());
        String domainSchema = stmt.schemaName() != null
                ? stmt.schemaName().toLowerCase() : executor.creationSchema();
        // A domain shares one namespace per schema with every other kind of type, and only that
        // schema's: a.d and b.d are two domains. A relation's row type is in that namespace too,
        // so a table's name is taken for a domain as much as an enum's is.
        TypeNamespace.requireCreatableType(executor.database, domainSchema, stmt.name(), false);
        // The base type's modifier is resolved after the name collision and before anything is
        // written, so a domain over a width the type could never carry is never created.
        TypeCoercion.checkDeclaredTypeLimits(stmt.baseType());
        boolean baseIsArray = stmt.baseType().replaceAll("\\(.*\\)", "").trim().endsWith("[]");
        String baseTypeName = TypeNamespace.qualify(executor.database, executor.session,
                stmt.baseType().replaceAll("\\(.*\\)", "").trim().replace("[]", "").trim());
        DataType baseType = DataType.fromPgName(baseTypeName);
        if (baseType == null) {
            DomainType parent = executor.database.getDomain(baseTypeName);
            if (parent != null) baseType = parent.getBaseType();
            else if (executor.database.isCustomEnum(baseTypeName)) baseType = DataType.ENUM;
            // A composite is a type like any other, and PostgreSQL lets a domain stand over one:
            // the domain's values are rows of that composite, so they are carried as records, and
            // the base type's own name is what says which composite the rows belong to.
            else if (executor.database.isCompositeType(baseTypeName)) baseType = DataType.RECORD;
            else if (TypeNamespace.writtenSchema(baseTypeName) != null
                    && DataType.fromPgName(TypeNamespace.bare(baseTypeName)) != null) {
                baseType = DataType.fromPgName(TypeNamespace.bare(baseTypeName));
            }
            else throw PgErrors.undefinedObject("type", baseTypeName);
        }
        // A domain over an array is a domain over the array type, not over its element: the
        // catalogs describe it as integer[] and a column of it holds an array.
        DataType elementType = null;
        if (baseIsArray) {
            DataType arrayType = DataType.arrayOf(baseType);
            if (arrayType != null) {
                elementType = baseType;
                baseType = arrayType;
            }
        }
        // Which collations exist is settled before the base type is asked whether it carries one:
        // PostgreSQL resolves the name first and reports a missing collation whatever the type.
        DdlDefinitionChecks.requireCollationExists(executor.database, stmt.collation());
        if (stmt.collation() != null && !isCollatable(baseType)) {
            throw PgErrors.datatypeMismatch(
                    "collations are not supported by type " + CatalogHelper.pgTypeName(baseType));
        }
        // Every CHECK the definition wrote is a constraint of its own. A statement parsed before
        // the list existed still arrives with the single pair, so it is read as a list of one.
        java.util.List<CreateDomainStmt.DomainCheck> declared = stmt.checks();
        if (declared == null) declared = new java.util.ArrayList<CreateDomainStmt.DomainCheck>();
        if (declared.isEmpty() && stmt.checkExpr() != null) {
            declared.add(new CreateDomainStmt.DomainCheck(stmt.constraintName(), stmt.checkExpr()));
        }
        for (CreateDomainStmt.DomainCheck check : declared) {
            checkDomainConstraintExpr(check.expr());
        }
        // The first unnamed CHECK is the domain's inline one, which is the constraint PostgreSQL
        // names <domain>_check; every other one is stored under the name it was written with, or
        // under the next generated name after the ones already taken.
        CreateDomainStmt.DomainCheck inline =
                !declared.isEmpty() && declared.get(0).name() == null ? declared.get(0) : null;
        DomainType domain = new DomainType(
                stmt.name(), baseType, baseTypeName, stmt.notNull(),
                inline != null ? inline.expr().toString() : null,
                inline != null ? inline.expr() : null,
                stmt.defaultExpr() != null ? DdlExecutor.exprToDefaultString(stmt.defaultExpr()) : null
        );
        for (CreateDomainStmt.DomainCheck check : declared) {
            if (check == inline) continue;
            String checkName = check.name();
            // A name written twice for one domain is a name pg_constraint could hold only once, and
            // PostgreSQL refuses the whole definition rather than storing one of the two.
            if (checkName != null && domainHasConstraint(domain, checkName)) {
                throw new MemgresException("constraint \"" + checkName + "\" for domain \""
                        + stmt.name() + "\" already exists", "42710");
            }
            if (checkName == null) checkName = generatedCheckName(domain);
            domain.addConstraint(checkName, check.expr().toString(), check.expr());
        }
        // Keep the base type's modifier: information_schema.domains describes a domain the way
        // it describes a column, so varchar(12) has to know it is twelve characters wide.
        int[] typmod = parseTypmod(stmt.baseType());
        domain.setTypmod(typmod[0] < 0 ? null : Integer.valueOf(typmod[0]),
                typmod[1] < 0 ? null : Integer.valueOf(typmod[1]));
        domain.setBaseTypeFacts(DataType.intervalQualifier(stmt.baseType()), elementType);
        // The default is read with the base type's input function here, not at the first insert.
        domain.setDefaultValue(requireDefaultReadableAsDomain(domain, domain.getDefaultValue()));
        domain.setSchemaName(domainSchema);
        executor.identity().typeCreated(TypeNamespace.key(domainSchema, stmt.name()));
        executor.database.addDomain(domain);
        executor.database.registerSchemaObject(domainSchema, "domain", stmt.name());
        executor.recordUndo(new Session.CreateDomainUndo(domainSchema, stmt.name()));
        return QueryResult.message(QueryResult.Type.SET, "CREATE DOMAIN");
    }

    /** The {@code (p)} or {@code (p,s)} written after a type name, as {p, s} with -1 for absent. */
    private static int[] parseTypmod(String declaredType) {
        int[] out = {-1, -1};
        if (declaredType == null) return out;
        int open = declaredType.indexOf('(');
        int close = declaredType.indexOf(')', open + 1);
        if (open < 0 || close < 0) return out;
        String[] parts = declaredType.substring(open + 1, close).split(",");
        try {
            out[0] = Integer.parseInt(parts[0].trim());
            if (parts.length > 1) out[1] = Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException ignored) {
            return new int[]{-1, -1};
        }
        return out;
    }

    /**
     * A domain constraint has one thing in scope, the pseudo-column VALUE. Any other name is a
     * column reference that cannot resolve, and PostgreSQL rejects it rather than letting the
     * domain be created with a constraint that can never be evaluated.
     */
    /** Only the string types carry a collation. */
    private static boolean isCollatable(DataType type) {
        return type == DataType.TEXT || type == DataType.VARCHAR
                || type == DataType.CHAR || type == DataType.NAME;
    }

    private void checkDomainConstraintExpr(Expression checkExpr) {
        if (checkExpr == null) return;
        Object bad = AstWalk.findFirst(checkExpr, n -> n instanceof ColumnRef
                && !"value".equalsIgnoreCase(((ColumnRef) n).column()));
        if (bad != null) {
            throw new MemgresException(
                    "column \"" + ((ColumnRef) bad).column() + "\" does not exist", "42703");
        }
        // A domain constraint is tested against one value with no row and no query around it, so
        // it is judged like every other stored expression.
        executor.selectExecutor.placementCheck.rejectStoredDefinition(
                checkExpr, "check constraints", "check constraint");
    }

    // ---- ALTER DOMAIN ----

    /**
     * The name PostgreSQL gives a CHECK added without one: the domain name and {@code _check},
     * then {@code _check1}, {@code _check2} and so on once that is taken.
     */
    private static String generatedCheckName(DomainType domain) {
        String base = domain.getName() + "_check";
        for (int suffix = 0; ; suffix++) {
            String candidate = suffix == 0 ? base : base + suffix;
            if (!domainHasConstraint(domain, candidate)) return candidate;
        }
    }

    /** Drop a constraint by name, including the unnamed CHECK that CREATE DOMAIN wrote. */
    private static void dropDomainConstraint(DomainType domain, String name) {
        if (name == null) return;
        if (domain.getParsedCheck() != null && name.equalsIgnoreCase(domain.getName() + "_check")) {
            domain.clearInlineCheck();
            return;
        }
        domain.removeConstraint(name);
    }

    /** True when the domain already carries a constraint of this name, inline one included. */
    private static boolean domainHasConstraint(DomainType domain, String name) {
        if (name == null) return false;
        if (domain.getParsedCheck() != null && name.equalsIgnoreCase(domain.getName() + "_check")) {
            return true;
        }
        for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
            if (name.equalsIgnoreCase(nc.name())) return true;
        }
        return false;
    }

    /**
     * A domain default written as a bare quoted literal is read with the base type's input
     * function when the domain is defined, so one that is not a value of the type is refused there
     * rather than at the first insert. Only that shape is checked: a literal of a known type, an
     * expression or a number is coerced at use, and PostgreSQL takes those without complaint even
     * when they will overflow later.
     */
    private String requireDefaultReadableAsDomain(DomainType domain, String rawDefault) {
        String literal = bareStringLiteral(rawDefault);
        if (literal == null || domain.getBaseType() == null) return rawDefault;
        String typeName = domain.getBaseTypeName() != null
                ? domain.getBaseTypeName() : domain.getBaseType().getPgName();
        // A domain over an array records its ELEMENT type here, so reading '{1,2}' with that
        // type's input function refuses a default PostgreSQL takes. Neither shape is checked.
        if (typeName.indexOf('[') >= 0 || literal.startsWith("{")) return rawDefault;
        Object value;
        try {
            value = executor.castEvaluator.applyCast(literal, typeName, true);
        } catch (MemgresException e) {
            // The input function is the authority on what it can read, but only where it is the
            // right one. Anything it cannot make sense of for a reason this method did not
            // anticipate is left to fail at first use, as it did before the check existed.
            if ("22P02".equals(e.getSqlState()) || "22003".equals(e.getSqlState())
                    || "22007".equals(e.getSqlState()) || "22008".equals(e.getSqlState())) {
                throw e;
            }
            return rawDefault;
        } catch (RuntimeException e) {
            return rawDefault;
        }
        // What the catalogs then report is the value the input function read, not the text it was
        // written as — but only for the types whose values PostgreSQL prints unquoted.
        if (value != null && UNQUOTED_DEFAULT_TYPES.contains(domain.getBaseType())) {
            return String.valueOf(value);
        }
        return rawDefault;
    }

    /** Base types whose default PostgreSQL prints as a bare value rather than a quoted literal. */
    private static final Set<DataType> UNQUOTED_DEFAULT_TYPES = Cols.setOf(
            DataType.SMALLINT, DataType.INTEGER, DataType.BIGINT, DataType.REAL,
            DataType.DOUBLE_PRECISION, DataType.NUMERIC, DataType.BOOLEAN);

    /** The text of {@code 'abc'} when that is the whole expression, else null. */
    private static String bareStringLiteral(String raw) {
        if (raw == null) return null;
        String text = raw.trim();
        if (text.length() < 2 || text.charAt(0) != '\'' || text.charAt(text.length() - 1) != '\'') {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        int i = 1;
        while (i < text.length() - 1) {
            char c = text.charAt(i);
            if (c == '\'') {
                if (i + 2 < text.length() && text.charAt(i + 1) == '\'') { sb.append('\''); i += 2; continue; }
                return null; // the literal ended before the expression did
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    QueryResult executeAlterDomain(AlterDomainStmt stmt) {
        // Which domain a bare name means is the search path's answer, and one held by a schema the
        // path does not reach is not a domain this statement can name at all -- the same rule DROP
        // DOMAIN and COMMENT ON DOMAIN already follow.
        String domainKey =
                TypeNamespace.resolve(executor.database, executor.session, stmt.domainName());
        DomainType domain = domainKey == null ? null : executor.database.getDomain(domainKey);
        if (domain == null) {
            throw new MemgresException("type \"" + stmt.domainName() + "\" does not exist", "42704");
        }
        // The domain is shared state that every column declared with it reads, so what it looked
        // like before this statement has to be recoverable if the transaction rolls back.
        executor.recordUndo(new Session.AlterDomainUndo(domain));
        switch (stmt.action()) {
            case "SET_DEFAULT": {
                String newDefault = requireDefaultReadableAsDomain(domain, stmt.defaultValue());
                // SET DEFAULT NULL leaves the domain with no default at all, which is what
                // information_schema then reports.
                if (newDefault != null && newDefault.trim().equalsIgnoreCase("NULL")) newDefault = null;
                domain.setDefaultValue(newDefault);
                break;
            }
            case "DROP_DEFAULT":
                domain.setDefaultValue(null);
                break;
            case "SET_NOT_NULL": {
                // The columns of this domain already hold what they hold; declaring the domain
                // NOT NULL while one of them holds a null leaves a catalog that disagrees with
                // the stored rows, and nothing would ever put it right.
                forEachDomainValue(stmt.domainName(), (tableName, columnName, value) -> {
                    if (value == null) {
                        throw PgErrors.columnContainsNulls(columnName, "table", tableName);
                    }
                });
                domain.setNotNull(true);
                break;
            }
            case "DROP_NOT_NULL":
                domain.setNotNull(false);
                break;
            case "ADD_CONSTRAINT": {
                String constraintName = stmt.constraintName() != null
                        ? stmt.constraintName() : generatedCheckName(domain);
                checkDomainConstraintExpr(stmt.checkExpr());
                if (stmt.checkExpr() != null) {
                    try {
                        Table valueTable = new Table("_domain_check",
                                Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
                        RowContext checkCtx = new RowContext(valueTable, null, new Object[]{null});
                        executor.evalExpr(stmt.checkExpr(), checkCtx);
                    } catch (MemgresException e) {
                        if ("42883".equals(e.getSqlState())) throw e;
                    }
                }
                if (!stmt.notValid()) {
                    checkDomainConstraintAgainstStoredRows(stmt.domainName(), domain, stmt.checkExpr());
                }
                domain.addConstraint(constraintName, stmt.rawCheckExpr(), stmt.checkExpr(), !stmt.notValid());
                break;
            }
            case "DROP_CONSTRAINT":
                if (!domainHasConstraint(domain, stmt.constraintName())) {
                    throw new MemgresException("constraint \"" + stmt.constraintName()
                            + "\" of domain \"" + stmt.domainName() + "\" does not exist", "42704");
                }
                dropDomainConstraint(domain, stmt.constraintName());
                break;
            case "DROP_CONSTRAINT_IF_EXISTS":
                dropDomainConstraint(domain, stmt.constraintName());
                break;
            case "VALIDATE": {
                boolean found = false;
                for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
                    if (nc.name().equalsIgnoreCase(stmt.constraintName())) {
                        // Marking a NOT VALID constraint valid is a claim about the rows that
                        // were let through while it was not being enforced, so those rows are
                        // what has to be looked at.
                        checkDomainConstraintAgainstStoredRows(stmt.domainName(), domain,
                                nc.parsedCheck());
                        nc.setValidated(true);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new MemgresException(
                            "constraint \"" + stmt.constraintName() + "\" of domain \"" + stmt.domainName() + "\" does not exist", "42704");
                }
                break;
            }
            case "RENAME_CONSTRAINT": {
                boolean found = false;
                for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
                    if (nc.name().equalsIgnoreCase(stmt.constraintName())) {
                        nc.setName(stmt.newConstraintName());
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new MemgresException(
                            "constraint \"" + stmt.constraintName() + "\" of domain \"" + stmt.domainName() + "\" does not exist", "42704");
                }
                break;
            }
            case "RENAME_TO": {
                String newName = stmt.newConstraintName();
                // The domain stays in its own schema and keeps its identity: every column
                // declared with it, and whatever a comment said about it.
                String domSchema = domain.getSchemaName();
                String oldKey = TypeNamespace.key(domSchema, domain.getName());
                requireTypeNameFree(typeRef(domSchema, newName));
                DomainType renamed = new DomainType(newName, domain.getBaseType(),
                        domain.getBaseTypeName(), domain.isNotNull(), domain.getCheckExpression(),
                        domain.getParsedCheck(), domain.getDefaultValue());
                renamed.setTypmod(domain.getPrecision(), domain.getScale());
                renamed.setBaseTypeFacts(domain.getIntervalQualifier(), domain.getArrayElementType());
                renamed.setSchemaName(domain.getSchemaName());
                for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
                    renamed.addConstraint(nc.name(), nc.rawCheckExpr(), nc.parsedCheck(),
                            nc.isValidated());
                }
                String newKey = TypeNamespace.key(domSchema, newName);
                inStoredTypes(() -> executor.database.getDomains().remove(oldKey));
                executor.database.addDomain(renamed);
                executor.database.unregisterSchemaObject(domSchema, "domain", domain.getName());
                executor.database.registerSchemaObject(domSchema, "domain", newName);
                executor.database.moveComment("type", oldKey, newKey);
                retargetDomainColumns(oldKey, newKey);
                // The same domain under a new name: a column declared with the old word goes on
                // reading this domain, so the OID stays with it.
                executor.identity().typeRenamed("d", oldKey, newKey);
                break;
            }
            case "SET_SCHEMA": {
                requireSchemaExists(stmt.newConstraintName());
                String from = domain.getSchemaName();
                String oldKey = TypeNamespace.key(from, domain.getName());
                String to = stmt.newConstraintName().toLowerCase();
                TypeNamespace.requireFree(executor.database, to, domain.getName());
                inStoredTypes(() -> executor.database.getDomains().remove(oldKey));
                domain.setSchemaName(to);
                executor.database.addDomain(domain);
                executor.database.unregisterSchemaObject(from, "domain", domain.getName());
                executor.database.registerSchemaObject(to, "domain", domain.getName());
                String newKey = TypeNamespace.key(to, domain.getName());
                executor.database.moveComment("type", oldKey, newKey);
                retargetDomainColumns(oldKey, newKey);
                executor.identity().typeRenamed("d", oldKey, newKey);
                break;
            }
            case "OWNER_TO": {
                requireOwnerExists(stmt.newConstraintName());
                break;
            }
            case "NO_OP": {
                break;
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER DOMAIN");
    }

    /**
     * Follow a renamed or moved domain from every column declared with it. A column records the
     * type it was declared with by name; PostgreSQL records an OID, which a rename does not
     * change, so the recorded name has to be rewritten here for the column to keep its type.
     */
    private void retargetDomainColumns(String oldKey, String newKey) {
        for (Schema s : executor.database.getSchemas().values()) {
            for (Table t : s.getTables().values()) {
                for (Column c : t.getColumns()) {
                    if (oldKey.equalsIgnoreCase(c.getDomainTypeName())) {
                        c.setDomainTypeName(newKey);
                    }
                }
            }
        }
    }

    /**
     * Follow a renamed or moved enum or composite from every column declared with it. A column of
     * a type keeps reading that type through the rename, which is what PostgreSQL's OIDs give it
     * for nothing and memgres, which records names, has to do here.
     */
    private void retargetTypeColumns(String oldKey, String newKey) {
        for (Schema s : executor.database.getSchemas().values()) {
            for (Table t : s.getTables().values()) {
                for (Column c : t.getColumns()) {
                    if (oldKey.equalsIgnoreCase(c.getEnumTypeName())) c.setEnumTypeName(newKey);
                    if (oldKey.equalsIgnoreCase(c.getCompositeTypeName())) {
                        c.setCompositeTypeName(newKey);
                    }
                }
            }
        }
    }

    /** What to do with one stored value of a domain: the table and column it sits in, and it. */
    private interface DomainValueVisitor {
        void accept(String tableName, String columnName, Object value);
    }

    /** Visit every stored value in every column declared with this domain. */
    private void forEachDomainValue(String domainName, DomainValueVisitor visitor) {
        // A column records the schema its domain lives in, so the written name is resolved to the
        // one domain it names before the columns are compared against it.
        String key = TypeNamespace.resolve(executor.database, executor.session, domainName);
        if (key == null) return;
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table table : schema.getTables().values()) {
                for (int ci = 0; ci < table.getColumns().size(); ci++) {
                    Column col = table.getColumns().get(ci);
                    if (col.getDomainTypeName() == null) continue;
                    if (!key.equals(TypeNamespace.find(executor.database.typeKeys(),
                            col.getDomainTypeName()))) continue;
                    for (Object[] row : table.getRows()) {
                        visitor.accept(table.getName(), col.getName(), row[ci]);
                    }
                }
            }
        }
    }

    /**
     * Refuse a domain constraint the rows already stored under that domain do not satisfy.
     * PostgreSQL names the column and table that hold the offending value, because that is where
     * the work of fixing it has to happen.
     */
    private void checkDomainConstraintAgainstStoredRows(String domainName, DomainType domain,
                                                        Expression checkExpr) {
        if (checkExpr == null) return;
        final Table valTable = new Table("_domain_check",
                Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
        forEachDomainValue(domainName, (tableName, columnName, value) -> {
            if (value == null) return;
            RowContext valCtx = new RowContext(valTable, null, new Object[]{value});
            Object result = executor.evalExpr(checkExpr, valCtx);
            if (!executor.isTruthy(result)) {
                MemgresException ex = new MemgresException("column \"" + columnName + "\" of table \""
                        + tableName + "\" contains values that violate the new constraint", "23514");
                ex.setColumn(columnName);
                throw ex;
            }
        });
    }

    // ---- CREATE INDEX ----

    /**
     * The relation an index is being built on, or null when it cannot be resolved here (a schema
     * this executor cannot reach, or a materialized view, which is a valid target held elsewhere).
     * Relations that can never carry an index are rejected before the access method is looked at.
     */
    private Table resolveIndexTarget(CreateIndexStmt s) {
        if (s.table() == null) return null;
        Database.ViewDef view = executor.database.getView(s.table());
        if (view != null && !view.materialized()) {
            MemgresException e = PgErrors.wrongObjectType(
                    "cannot create index on relation \"" + s.table() + "\"");
            e.setDetail("This operation is not supported for views.");
            throw e;
        }
        if (executor.database.hasIndex(s.table())) {
            throw PgErrors.wrongObjectType("cannot open relation \"" + s.table() + "\"");
        }
        if (executor.database.hasSequence(s.table())) {
            throw PgErrors.wrongObjectType("cannot open relation \"" + s.table() + "\"");
        }
        try {
            // A qualified reference that found nothing is reported the way it was written.
            return executor.resolveTable(
                    s.schema() != null ? s.schema() : executor.defaultSchema(), s.table(),
                    s.schema() != null);
        } catch (MemgresException e) {
            if ("42P01".equals(e.getSqlState()) && view == null) throw e;
            return null;
        }
    }

    /**
     * Parse-analysis rules for an index definition: an index key is evaluated one row at a time,
     * so it can be neither an aggregate nor a query, and the same goes for a partial index's
     * predicate. An accepted non-immutable predicate is worse than a rejection — the index then
     * omits rows whose predicate changed since they were written, and a query answered from it
     * silently returns fewer rows than the same query answered from the heap.
     */
    private void checkIndexExpressionsAndPredicate(CreateIndexStmt s) {
        if (s.columns() != null) {
            for (String col : s.columns()) {
                // An index key is kept as the text it was written as, so its type names are read
                // here rather than when the statement itself was parsed. What tells a key that is
                // an expression from one that names a column is what it parses to: the parentheses
                // an expression key is written in are stripped before it is stored, so
                // "((id::int4))" arrives here with none left to recognise it by.
                List<String> typeSchemas = new ArrayList<>();
                Expression expr;
                try {
                    expr = com.memgres.engine.parser.Parser.parseExpression(col, typeSchemas);
                } catch (Exception ignored) {
                    continue;
                }
                if (expr instanceof ColumnRef) continue;
                SchemaQualifier.rejectMissingTypeSchemas(
                        executor.database, executor.session, executor.getSystemCatalog(), typeSchemas);
                executor.selectExecutor.placementCheck.rejectStoredDefinition(
                        expr, "index expressions", "index expression");
            }
        }
        if (s.whereClause() != null) {
            List<String> typeSchemas = new ArrayList<>();
            Expression pred;
            try {
                pred = com.memgres.engine.parser.Parser.parseExpression(s.whereClause(), typeSchemas);
            } catch (Exception ignored) {
                return;
            }
            SchemaQualifier.rejectMissingTypeSchemas(
                    executor.database, executor.session, executor.getSystemCatalog(), typeSchemas);
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    pred, "index predicates", "index predicate");
        }
    }

    /**
     * A partial index's predicate says which rows the index holds, so it is a condition and
     * PostgreSQL coerces it to boolean while it analyses the statement — naming WHERE, the clause
     * it was written in.
     */
    private void checkPredicateIsBoolean(Table table, String whereClause) {
        Expression pred;
        try {
            pred = com.memgres.engine.parser.Parser.parseExpression(whereClause);
        } catch (RuntimeException ignored) {
            return; // a predicate this cannot read is reported by the checks that follow
        }
        BooleanContext.check(pred, "WHERE", BooleanContext.Types.of(table));
    }

    /**
     * Evaluate a partial index's predicate once against a row of placeholder values, so that an
     * operator or function that does not exist for the column types is reported now.
     */
    private void checkPredicateTypes(Table table, String whereClause) {
        if (table == null) return;
        try {
            Expression pred = com.memgres.engine.parser.Parser.parseExpression(whereClause);
            Object[] dummyRow = new Object[table.getColumns().size()];
            for (int i = 0; i < dummyRow.length; i++) {
                dummyRow[i] = placeholderValue(table.getColumns().get(i).getType());
            }
            executor.evalExpr(pred, new RowContext(table, table.getName(), dummyRow));
        } catch (MemgresException e) {
            if ("42883".equals(e.getSqlState()) || "42804".equals(e.getSqlState())
                    || "42P18".equals(e.getSqlState())) throw e;
        } catch (Exception ignored) {
            // Anything else here is not a definition error; the predicate is stored as written
        }
    }

    /** A non-null value of the right shape for evaluating an expression at definition time. */
    private static Object placeholderValue(DataType type) {
        switch (type) {
            case INTEGER:
            case BIGINT:
            case SMALLINT:
                return 0L;
            case NUMERIC:
            case DOUBLE_PRECISION:
            case REAL:
                return 0.0;
            case BOOLEAN:
                return false;
            case JSON:
            case JSONB:
                return "{}";
            default:
                return "dummy";
        }
    }

    QueryResult executeCreateIndex(CreateIndexStmt s) {
        // Building an index concurrently takes more than one transaction of its own, so it cannot
        // be one step inside somebody else's. Running it anyway made a statement PostgreSQL
        // refuses look like it had worked.
        if (s.concurrently && executor.session != null && executor.session.isInTransaction()) {
            throw new MemgresException(
                    "CREATE INDEX CONCURRENTLY cannot run inside a transaction block", "25001");
        }
        // The relation is opened before the statement is analysed, and opening it starts by
        // finding the schema it was written under.
        SchemaQualifier.requireSchema(executor.database, executor.session, s.schema());
        String indexSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
        // An index is part of the relation's own definition, so adding one is the owner's to do.
        // A role holding nothing but SELECT could build an index over the whole table.
        if (s.table() != null) executor.requireTableOwner(indexSchema, s.table());
        // Only this schema's relations can take the name: two schemas may each hold an index
        // called i, and refusing the second one turned valid SQL away.
        boolean nameTaken = s.name() != null
                && (executor.database.hasIndex(indexSchema, s.name())
                    || RelationNamespace.kindOf(executor.database, indexSchema, s.name()) != null);
        if (nameTaken && s.ifNotExists()) return QueryResult.message(QueryResult.Type.SET, "CREATE INDEX");
        // PostgreSQL analyses the statement before it opens anything, so an aggregate or a
        // subquery is reported even when the access method or the index name is also wrong.
        checkIndexExpressionsAndPredicate(s);
        Table indexTarget = resolveIndexTarget(s);
        // An index nobody named still gets one: PostgreSQL derives it from the relation and from
        // what each indexed column is worth as a name, and numbers it when a relation of that name
        // already lives in the schema. Leaving the name null registered nothing at all and still
        // reported success, so the index the writer asked for simply was not there afterwards.
        if (s.name() == null && s.table() != null && s.columns() != null) {
            s = new CreateIndexStmt(
                    IndexNameChooser.choose(executor.database, indexSchema, s.table(), indexTarget,
                            s.columns(), s.includeColumns()),
                    s.schema(), s.table(), s.columns(), s.unique(), s.ifNotExists(),
                    s.concurrently(), s.method(), s.includeColumns(), s.whereClause(),
                    s.columnOptions(), s.nullsNotDistinct(), s.withOptions());
        }
        if (indexTarget != null) {
            DdlIndexValidator.validate(executor.database, indexTarget, s.method(), s.unique(),
                    s.columns(), s.columnOptions(), s.includeColumns(), s.withOptions());
        }
        if (s.whereClause() != null) {
            // Type resolution happens while the statement is analysed, so a predicate that does
            // not type-check is reported as that rather than as a mutable-function predicate.
            checkPredicateIsBoolean(indexTarget, s.whereClause());
            checkPredicateTypes(indexTarget, s.whereClause());
            DdlExecutor.checkBuiltinVolatileInExpression(s.whereClause(), executor.database,
                    "functions in index predicate must be marked IMMUTABLE");
        }
        // Validate index columns exist on the target table
        if (s.table() != null && s.columns() != null) {
            try {
                String idxSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table idxTable = executor.resolveTable(idxSchema, s.table());
                for (String col : s.columns()) {
                    // CURRENT_DATE and the other value functions SQL writes without an argument
                    // list are keys of the same kind as a call, not names of columns: PostgreSQL
                    // reads them through the grammar that admits a call and then refuses them for
                    // being no more than stable. A relation that does hold a column of that name
                    // can still be indexed on it, because only a quoted name reaches it.
                    if (DdlIndexValidator.isSqlValueFunction(col) && idxTable.getColumnIndex(col) < 0) {
                        throw new MemgresException(
                                "functions in index expression must be marked IMMUTABLE", "42P17");
                    }
                    // Skip expression-based index columns (contain parens, operators, or spaces)
                    if (col.contains("(") || col.contains(")") || col.contains(" ")
                            || col.contains("+") || col.contains("*") || col.contains("/") || col.contains("||")) {
                        // Expression-based index column; try to evaluate against a dummy row to catch type errors
                        String exprStr = col.trim();
                        // Reject built-in volatile functions (random, now, etc.) in index expressions.
                        // User-defined function volatility is NOT checked — PG allows it.
                        DdlExecutor.checkBuiltinVolatileInExpression(exprStr, executor.database,
                                "functions in index expression must be marked IMMUTABLE");
                        // Try to evaluate the expression against a dummy row to catch type errors
                        try {
                            // Strip outer wrapper parens like ((a + b)) → a + b, but NOT function call parens
                            String exprToParse = exprStr;
                            while (exprToParse.startsWith("(") && exprToParse.endsWith(")")) {
                                // Check that the outer parens are matched (not part of a function call)
                                int depth = 0;
                                boolean outerMatched = false;
                                for (int ci = 0; ci < exprToParse.length(); ci++) {
                                    if (exprToParse.charAt(ci) == '(') depth++;
                                    else if (exprToParse.charAt(ci) == ')') depth--;
                                    if (depth == 0 && ci == exprToParse.length() - 1) outerMatched = true;
                                    if (depth == 0 && ci < exprToParse.length() - 1) break;
                                }
                                if (outerMatched) {
                                    exprToParse = exprToParse.substring(1, exprToParse.length() - 1).trim();
                                } else {
                                    break;
                                }
                            }
                            Expression idxExpr =
                                com.memgres.engine.parser.Parser.parseExpression(exprToParse);
                            // Create a dummy row context with default non-null values
                            Object[] dummyRow = new Object[idxTable.getColumns().size()];
                            for (int di = 0; di < idxTable.getColumns().size(); di++) {
                                Column dc = idxTable.getColumns().get(di);
                                switch (dc.getType()) {
                                    case INTEGER:
                                    case BIGINT:
                                    case SMALLINT:
                                        dummyRow[di] = 0L;
                                        break;
                                    case NUMERIC:
                                    case DOUBLE_PRECISION:
                                    case REAL:
                                        dummyRow[di] = 0.0;
                                        break;
                                    case BOOLEAN:
                                        dummyRow[di] = false;
                                        break;
                                    case JSON:
                                    case JSONB:
                                        dummyRow[di] = "{}";
                                        break;
                                    default:
                                        dummyRow[di] = "dummy";
                                        break;
                                }
                            }
                            // Compute virtual columns on the dummy row so expression
                            // indexes referencing virtual columns evaluate correctly
                            if (executor.dmlExecutor.hasVirtualColumns(idxTable)) {
                                dummyRow = executor.dmlExecutor.computeVirtualColumns(idxTable, dummyRow);
                            }
                            RowContext dummyCtx = new RowContext(idxTable, idxTable.getName(), dummyRow);
                            executor.evalExpr(idxExpr, dummyCtx);
                        } catch (MemgresException me) {
                            if ("42883".equals(me.getSqlState()) || "42804".equals(me.getSqlState())
                                    || "42702".equals(me.getSqlState()) || "42P18".equals(me.getSqlState())
                                    || me.getMessage() != null && me.getMessage().contains("operator does not exist")) {
                                throw me;
                            }
                            // Other eval errors (e.g., null arithmetic), try string extraction fallback
                            String stripped = exprStr;
                            while (stripped.startsWith("(")) stripped = stripped.substring(1).trim();
                            int parenIdx = stripped.indexOf('(');
                            if (parenIdx > 0) {
                                String funcName = stripped.substring(0, parenIdx).trim().toLowerCase();
                                if (!funcName.isEmpty()) {
                                    // SQL/JSON special forms are not regular functions — skip validation
                                    if (isJsonSpecialForm(funcName)) continue;
                                    if (executor.database.getFunction(funcName) != null) continue;
                                    // Whether the name is a function at all is what this asks, and
                                    // the register answers it. It used to call the name with one
                                    // NULL argument and read a 42883 as proof the name was
                                    // unknown, which confused "no function of that name" with "no
                                    // signature taking one argument" — so an index on
                                    // exist(hstore, text) was refused as a missing function once
                                    // the arity rule could say exist(unknown) resolves to nothing.
                                    if (BuiltinFunctionNames.isCallable(funcName)) continue;
                                    throw new MemgresException(
                                            "function " + funcName + "(text) does not exist", "42883");
                                }
                            }
                        } catch (Exception ignored) {}
                        continue;
                    }
                    int colIdx = idxTable.getColumnIndex(col);
                    if (colIdx < 0) {
                        throw new MemgresException("column \"" + col + "\" does not exist", "42703");
                    }
                    // PG 18: indexes on virtual generated columns are not supported
                    if (idxTable.getColumns().get(colIdx).isVirtual()) {
                        throw new MemgresException(
                                "indexes on virtual generated columns are not supported", "0A000");
                    }
                }
                // PG 18: partial index WHERE clause referencing virtual generated columns is not supported
                if (s.whereClause() != null) {
                    checkWhereClauseVirtualColumns(s.whereClause(), idxTable);
                }
                // Validate WHERE predicate (partial index condition) references existing columns
                if (s.whereClause() != null) {
                    try {
                        Expression predExpr =
                            com.memgres.engine.parser.Parser.parseExpression(s.whereClause());
                        // Walk the expression to find column references
                        ddl.validateExprColumnRefs(predExpr, idxTable, null);
                    } catch (MemgresException me) {
                        if ("42703".equals(me.getSqlState())) throw me;
                        // Other errors ignored
                    } catch (Exception ignored) {}
                }
                DdlIndexValidator.validateIncludeColumns(idxTable, s.includeColumns());
            } catch (MemgresException e) {
                if ("42703".equals(e.getSqlState()) || "42883".equals(e.getSqlState())
                        || "0A000".equals(e.getSqlState()) || "42804".equals(e.getSqlState())
                        || "42P17".equals(e.getSqlState())) throw e;
                // Re-throw table-not-found only if it's also not a view (materialized views are valid index targets)
                if ("42P01".equals(e.getSqlState()) && !executor.database.hasView(s.table())) throw e;
                // Other errors (e.g., schema issues, table is a view); skip column validation
            }
        }
        // For UNIQUE/PK indexes on partitioned tables, the index columns must include the partition key
        if (s.unique() && s.table() != null && s.columns() != null) {
            try {
                String partSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table partTable = executor.resolveTable(partSchema, s.table());
                if (partTable.getPartitionStrategy() != null && partTable.getPartitionColumn() != null) {
                    String partCol = partTable.getPartitionColumn().toLowerCase();
                    if (partCol.startsWith("(")) partCol = partCol.substring(1);
                    if (partCol.endsWith(")")) partCol = partCol.substring(0, partCol.length() - 1);
                    partCol = partCol.trim();
                    boolean partColFound = false;
                    for (String idxCol : s.columns()) {
                        if (idxCol.toLowerCase().equals(partCol)) { partColFound = true; break; }
                    }
                    if (!partColFound) {
                        throw new MemgresException("unique constraint on partitioned table must include all partitioning columns\n"
                                + "  Detail: UNIQUE constraint on table \"" + s.table()
                                + "\" lacks column \"" + partCol + "\" which is part of the partition key.",
                                "0A000");
                    }
                }
            } catch (MemgresException e) {
                if ("0A000".equals(e.getSqlState())) throw e;
            }
        }
        // For UNIQUE indexes, validate existing data for uniqueness before creating the index
        if (s.unique() && s.table() != null && s.columns() != null) {
            try {
                String valSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table valTable = executor.resolveTable(valSchema, s.table());
                List<Object[]> existingRows = valTable.getRows();
                if (existingRows != null && existingRows.size() > 1) {
                    // Parse WHERE predicate if present (partial unique index)
                    Expression wherePred = null;
                    if (s.whereClause() != null) {
                        try {
                            wherePred = com.memgres.engine.parser.Parser.parseExpression(s.whereClause());
                        } catch (Exception ignored) {}
                    }
                    // Parse expression columns if any. A key that names a column of the relation
                    // is that column whatever characters its name happens to hold: "A b" is one
                    // quoted name, not two words, and reading it as an expression left the index
                    // enforcing something the relation has no column for.
                    boolean hasExprCols = s.columns().stream().anyMatch(c ->
                            valTable.getColumnIndex(c) < 0
                            && (c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                            || c.contains("*") || c.contains("/") || c.contains("||")));
                    List<Expression> parsedExprs = null;
                    if (hasExprCols) {
                        parsedExprs = new ArrayList<>();
                        for (String col : s.columns()) {
                            try {
                                parsedExprs.add(com.memgres.engine.parser.Parser.parseExpression(col));
                            } catch (Exception e) {
                                parsedExprs = null;
                                break;
                            }
                        }
                    }
                    // Collect key values for rows that pass the WHERE predicate
                    Set<String> seenKeys = new HashSet<>();
                    boolean idxHasVirtual = executor.dmlExecutor.hasVirtualColumns(valTable);
                    for (Object[] row : existingRows) {
                        Object[] evalRow = idxHasVirtual ? executor.dmlExecutor.computeVirtualColumns(valTable, row) : row;
                        RowContext rowCtx = new RowContext(valTable, valTable.getName(), evalRow);
                        // Check WHERE predicate and skip rows that don't match
                        if (wherePred != null) {
                            try {
                                Object predResult = executor.evalExpr(wherePred, rowCtx);
                                if (!Boolean.TRUE.equals(predResult)) continue;
                            } catch (Exception e) {
                                continue;
                            }
                        }
                        // Compute key values
                        StringBuilder keyBuilder = new StringBuilder();
                        List<Object> keyValues = new ArrayList<>();
                        if (parsedExprs != null) {
                            for (Expression expr : parsedExprs) {
                                try {
                                    Object val = executor.evalExpr(expr, rowCtx);
                                    keyValues.add(val);
                                    keyBuilder.append(val == null ? "\0NULL\0" : val.toString()).append('\1');
                                } catch (Exception e) {
                                    keyValues.add(null);
                                    keyBuilder.append("\0ERR\0").append('\1');
                                }
                            }
                        } else {
                            for (String col : s.columns()) {
                                int ci = valTable.getColumnIndex(col);
                                if (ci >= 0) {
                                    Object val = evalRow[ci];
                                    keyValues.add(val);
                                    keyBuilder.append(val == null ? "\0NULL\0" : val.toString()).append('\1');
                                }
                            }
                        }
                        String key = keyBuilder.toString();
                        // A null in the key makes its row unlike every other, so it cannot stop the
                        // index being built -- unless the index was declared NULLS NOT DISTINCT,
                        // which is what that clause is for.
                        boolean comparable = s.nullsNotDistinct() || !key.contains("\0NULL\0");
                        if (comparable && !seenKeys.add(key)) {
                            // Nothing was written here, so the complaint is about what the table
                            // was found holding, not about a key that already exists: PostgreSQL
                            // ends this one "is duplicated." and names the index as the constraint.
                            MemgresException dup = new MemgresException(
                                    "could not create unique index \"" + s.name() + "\"", "23505");
                            dup.setConstraint(s.name());
                            dup.setDetail(IndexKeyDescription.duplicated(
                                    valTable, s.columns(), keyValues.toArray()));
                            throw dup;
                        }
                    }
                }
            } catch (MemgresException e) {
                if ("23505".equals(e.getSqlState())) throw e;
                // Other errors (table not found, etc.); skip validation
            }
        }
        // The name clash is reported last: PostgreSQL only picks the index name once the
        // definition itself is known to be sound.
        if (nameTaken) {
            throw new MemgresException("relation \"" + s.name() + "\" already exists", "42P07");
        }
        if (s.name() != null && s.columns() != null) {
            // An index lives in the schema of the table it indexes, and it is that schema's
            // relations its name has to be free of -- not every schema's at once.
            String idxSchemaForMeta = s.schema() != null ? s.schema() : executor.defaultSchema();
            executor.database.addIndex(idxSchemaForMeta, s.name(), s.columns());
            // Store index metadata (table name, uniqueness, method, WHERE clause)
            executor.database.addIndexMeta(idxSchemaForMeta, s.name(),
                    idxSchemaForMeta + "." + s.table(), s.unique(), s.method(), s.whereClause());
            executor.database.setIndexColumnOptions(idxSchemaForMeta, s.name(), s.columnOptions());
            executor.database.setIndexIncludeColumns(idxSchemaForMeta, s.name(), s.includeColumns());
            executor.database.setIndexNullsNotDistinct(idxSchemaForMeta, s.name(), s.nullsNotDistinct());
            // The storage parameters an index was created WITH are part of what it is: pg_class
            // lists them and pg_get_indexdef writes them back. Only ALTER INDEX SET recorded
            // them, so an index created with a fillfactor reported none.
            if (s.withOptions() != null && !s.withOptions().isEmpty()) {
                executor.database.setIndexReloptions(
                        Database.idxKey(idxSchemaForMeta, s.name()), s.withOptions());
            }
            executor.database.registerSchemaObject(idxSchemaForMeta, "index", s.name());
            executor.recordUndo(new Session.CreateIndexUndo(
                    Database.idxKey(idxSchemaForMeta, s.name())));
            // Build a real TableIndex for simple column indexes (non-expression, non-partial)
            // so they can be used for index scans in SELECT queries
            if (s.table() != null && s.whereClause() == null) {
                boolean hasExprCols = s.columns().stream().anyMatch(c ->
                        c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                        || c.contains("*") || c.contains("/") || c.contains("||"));
                if (!hasExprCols) {
                    try {
                        Table idxTable2 = executor.resolveTable(idxSchemaForMeta, s.table());
                        int[] colIndices = new int[s.columns().size()];
                        boolean allFound = true;
                        for (int ci = 0; ci < s.columns().size(); ci++) {
                            int idx = idxTable2.getColumnIndex(s.columns().get(ci));
                            if (idx < 0) { allFound = false; break; }
                            colIndices[ci] = idx;
                        }
                        // Skip building index on virtual columns (computed on read, not stored)
                        boolean hasVirtualCol = false;
                        if (allFound) {
                            for (int ci : colIndices) {
                                if (idxTable2.getColumns().get(ci).isVirtual()) {
                                    hasVirtualCol = true;
                                    break;
                                }
                            }
                        }
                        if (allFound && !hasVirtualCol && idxTable2.getIndex(s.name()) == null) {
                            TableIndex tableIdx = new TableIndex(s.name(), colIndices, s.unique());
                            idxTable2.buildIndex(tableIdx);
                        }
                    } catch (MemgresException ignored) {}
                }
            }
        }
        // For UNIQUE indexes, also add a UNIQUE constraint to enforce uniqueness
        if (s.unique() && s.table() != null && s.columns() != null) {
            try {
                String uIdxSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table idxTable = executor.resolveTable(uIdxSchema, s.table());
                String constraintName = s.name() != null ? s.name() : s.table() + "_unique";
                StoredConstraint sc = StoredConstraint.unique(constraintName, s.columns());
                // For partial unique indexes, parse and store the WHERE predicate
                if (s.whereClause() != null) {
                    try {
                        Expression predExpr = com.memgres.engine.parser.Parser.parseExpression(s.whereClause());
                        sc.setWhereExpr(predExpr);
                    } catch (Exception ignored) {}
                }
                // For expression-based indexes (e.g., lower(email), (a + b)), parse and store the expressions
                // Detect expressions: contains parens, operators, or spaces (not a simple column name)
                boolean hasExprCols = s.columns().stream().anyMatch(c ->
                        idxTable.getColumnIndex(c) < 0
                        && (c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                        || c.contains("*") || c.contains("/") || c.contains("||")));
                if (hasExprCols) {
                    List<Expression> exprCols = new ArrayList<>();
                    for (String col : s.columns()) {
                        try {
                            exprCols.add(com.memgres.engine.parser.Parser.parseExpression(col));
                        } catch (Exception e) {
                            exprCols = null;
                            break;
                        }
                    }
                    if (exprCols != null) {
                        sc.setExpressionColumns(exprCols);
                    }
                }
                sc.setFromIndex(true);
                if (s.nullsNotDistinct()) sc.setNullsNotDistinct(true);
                idxTable.addConstraint(sc);
            } catch (MemgresException ignored) {
                // Table might not exist yet (e.g., on materialized views)
            }
        }
        // An index on a partitioned table is a rule about the whole hierarchy, so every partition
        // gets its own copy of it -- named after the partition and the columns it reads, which is
        // the name PostgreSQL reports when a duplicate key violates a unique one. The copies are
        // made here, last, because a unique index leaves behind the constraint they are taken
        // from, and a copy made before it existed enforced nothing.
        if (s.name() != null && s.columns() != null && s.table() != null) {
            try {
                String partitionedSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table partitionedParent = executor.resolveTable(partitionedSchema, s.table());
                if (partitionedParent.getPartitionStrategy() != null) {
                    for (Table childPartition : partitionedParent.getPartitions()) {
                        ddl.tableExecutor.copyParentIndex(partitionedParent, childPartition,
                                partitionedSchema, Database.idxKey(partitionedSchema, s.name()));
                    }
                }
            } catch (MemgresException ignored) {}
        }
        return QueryResult.message(QueryResult.Type.SET, "CREATE INDEX");
    }

    /** SQL/JSON special forms that are parsed as keywords, not regular functions. */
    private static boolean isJsonSpecialForm(String name) {
        switch (name) {
            case "json_value":
            case "json_query":
            case "json_exists":
            case "json_serialize":
            case "json_scalar":
            case "json_table":
            case "json_array":
            case "json_object":
            case "json_arrayagg":
            case "json_objectagg":
                return true;
            default:
                return false;
        }
    }

    /** PG 18: reject partial indexes whose WHERE clause references virtual generated columns. */
    private void checkWhereClauseVirtualColumns(String whereClause, Table table) {
        try {
            Expression predExpr = com.memgres.engine.parser.Parser.parseExpression(whereClause);
            checkExprVirtualColumnRefs(predExpr, table);
        } catch (MemgresException me) {
            throw me;
        } catch (Exception ignored) {}
    }

    private void checkExprVirtualColumnRefs(Expression expr, Table table) {
        if (expr == null) return;
        if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            int idx = table.getColumnIndex(cr.column());
            if (idx >= 0 && table.getColumns().get(idx).isVirtual()) {
                throw new MemgresException(
                        "indexes on virtual generated columns are not supported", "0A000");
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            checkExprVirtualColumnRefs(bin.left(), table);
            checkExprVirtualColumnRefs(bin.right(), table);
        } else if (expr instanceof UnaryExpr) {
            checkExprVirtualColumnRefs(((UnaryExpr) expr).operand(), table);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            if (fn.args() != null) {
                for (Expression arg : fn.args()) checkExprVirtualColumnRefs(arg, table);
            }
        }
    }

    /** The first column that sorts by this collation, or null when none does. */
    private String columnCollatedBy(String collation) {
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table table : schema.getTables().values()) {
                for (Column column : table.getColumns()) {
                    if (collation.equalsIgnoreCase(column.getCollation())) {
                        return table.getName() + "." + column.getName();
                    }
                }
            }
        }
        return null;
    }

    // ---- CREATE COLLATION ----

    QueryResult executeCreateCollation(CreateCollationStmt stmt) {
        String name = stmt.name;
        if (ddl.executor.database.getCollation(name) != null) {
            if (stmt.ifNotExists) {
                return QueryResult.message(QueryResult.Type.SET, "CREATE COLLATION");
            }
            // PostgreSQL names the encoding a collation is for, because two collations of
            // one name may exist for two encodings.
            throw new MemgresException(
                    "collation \"" + name + "\" for encoding \"UTF8\" already exists", "42710");
        }

        String provider = "c";
        String locale = null;
        String lcCollate = null;
        String lcCtype = null;
        boolean deterministic = true;

        if (stmt.fromCollation != null) {
            // CREATE COLLATION name FROM existing_collation
            // Just register it with default libc provider
            ddl.executor.database.addCollation(new Database.CollationDef(
                    name, "c", null, null, null, true, stmt.fromCollation));
            return QueryResult.message(QueryResult.Type.SET, "CREATE COLLATION");
        }

        for (java.util.Map.Entry<String, String> entry : stmt.options.entrySet()) {
            switch (entry.getKey()) {
                case "provider":
                    String pv = entry.getValue().toLowerCase();
                    if (pv.equals("icu")) provider = "i";
                    else if (pv.equals("libc")) provider = "c";
                    else provider = pv;
                    break;
                case "locale":
                    locale = entry.getValue();
                    break;
                case "lc_collate":
                    lcCollate = entry.getValue();
                    break;
                case "lc_ctype":
                    lcCtype = entry.getValue();
                    break;
                case "deterministic":
                    deterministic = !"false".equalsIgnoreCase(entry.getValue());
                    break;
            }
        }

        ddl.executor.database.addCollation(new Database.CollationDef(
                name, provider, locale, lcCollate, lcCtype, deterministic, null));
        return QueryResult.message(QueryResult.Type.SET, "CREATE COLLATION");
    }

    // ---- CREATE CAST ----

    QueryResult executeCreateCast(CreateCastStmt stmt) {
        validateTypeExists(stmt.sourceType);
        validateTypeExists(stmt.targetType);
        // Resolve source and target type OIDs using DataType enum for built-in types
        int sourceOid = resolveTypeOid(stmt.sourceType);
        int targetOid = resolveTypeOid(stmt.targetType);
        String castMethod = stmt.functionName != null ? "f" : (stmt.withInout ? "i" : "b");
        int castFunc = 0; // 0 for binary coercible / without function
        if (castMethod.equals("f")) {
            validateCastFunction(stmt);
        } else if (castMethod.equals("b")) {
            validateBinaryCoercible(stmt);
        }
        // A cast from a type to itself converts nothing, so there is no cast to create. The
        // function is judged first, because a cast written with one that does not fit is wrong
        // about the function before it is wrong about the types.
        if (sourceOid == targetOid) {
            throw new MemgresException(
                    "source data type and target data type are the same", "42P17");
        }
        if (executor.database.getUserDefinedCasts().stream()
                .anyMatch(c -> (int) c[0] == sourceOid && (int) c[1] == targetOid)) {
            throw new MemgresException("cast from type " + DataType.canonicalName(stmt.sourceType)
                    + " to type " + DataType.canonicalName(stmt.targetType) + " already exists", "42710");
        }
        // Store in database for inclusion in pg_cast virtual table
        executor.database.addUserCast(sourceOid, targetOid, castFunc, stmt.castContext, castMethod,
                castMethod.equals("f") ? stmt.functionName : null);
        return QueryResult.command(QueryResult.Type.CREATE_TYPE, 0);
    }

    /**
     * A cast function has to take the source type and produce the target type; registering one
     * that does neither leaves a cast that reinterprets values of a type it never handled.
     */
    private void validateCastFunction(CreateCastStmt stmt) {
        String bare = stmt.functionName.contains(".")
                ? stmt.functionName.substring(stmt.functionName.lastIndexOf('.') + 1) : stmt.functionName;
        List<PgFunction> overloads = executor.database.getFunctionOverloads(bare);
        PgFunction func = null;
        if (stmt.funcArgTypes != null) {
            for (PgFunction f : overloads) {
                if (paramTypesMatch(f, stmt.funcArgTypes)) { func = f; break; }
            }
        } else if (!overloads.isEmpty()) {
            func = overloads.get(0);
        }
        if (func == null) {
            if (isKnownBuiltinFunction(stmt.functionName)) return;
            throw new MemgresException("function " + stmt.functionName + "("
                    + canonicalTypeList(stmt.funcArgTypes) + ") does not exist",
                    "42883").withoutHint();
        }
        List<PgFunction.Param> params = func.getParams();
        String firstParam = params.isEmpty() ? null : params.get(0).typeName();
        if (firstParam == null
                || !DataType.canonicalName(firstParam).equals(DataType.canonicalName(stmt.sourceType))) {
            throw new MemgresException(
                    "argument of cast function must match or be binary-coercible from source data type",
                    "42P17");
        }
        if (func.getReturnType() == null
                || !DataType.canonicalName(func.getReturnType()).equals(DataType.canonicalName(stmt.targetType))) {
            throw new MemgresException(
                    "return data type of cast function must match or be binary-coercible to target data type",
                    "42P17");
        }
    }

    /**
     * WITHOUT FUNCTION claims the two types are the same bytes. Accepting that between types
     * stored differently would register a cast that reinterprets one type's memory as another.
     */
    private void validateBinaryCoercible(CreateCastStmt stmt) {
        String src = stmt.sourceType.toLowerCase();
        String tgt = stmt.targetType.toLowerCase();
        if (DataType.canonicalName(src).equals(DataType.canonicalName(tgt))) {
            throw new MemgresException("source data type and target data type are the same", "42P17");
        }
        if (!storageClass(src).equals(storageClass(tgt))) {
            throw new MemgresException("source and target data types are not physically compatible", "42P17");
        }
        if (executor.database.isCompositeType(src) || executor.database.isCompositeType(tgt)) {
            throw new MemgresException("composite data types are not binary-compatible", "42P17");
        }
        if (executor.database.isCustomEnum(src) || executor.database.isCustomEnum(tgt)) {
            throw new MemgresException("enum data types are not binary-compatible", "42P17");
        }
        if (executor.database.getDomain(src) != null || executor.database.getDomain(tgt) != null) {
            throw new MemgresException("domain data types must not be marked binary-compatible", "42P17");
        }
    }

    /**
     * PG calls two types physically compatible when typlen, typbyval and typalign all agree;
     * this reports that triple as one key.
     */
    private String storageClass(String typeName) {
        String base = typeName.toLowerCase().replaceAll("\\(.*\\)", "").trim();
        if (base.endsWith("[]")) return "-1/f/d";           // every array is a varlena
        if (executor.database.isCompositeType(base)) return "-1/f/d";
        if (executor.database.isCustomEnum(base)) return "4/t/i";
        DomainType dom = executor.database.getDomain(base);
        if (dom != null) return storageClass(dom.getBaseTypeName());
        DataType dt = DataType.fromPgName(base);
        if (dt == null) return "unknown:" + base;
        switch (dt) {
            case SMALLINT: return "2/t/s";
            case INTEGER: case SERIAL: case REAL: case DATE: case OID: return "4/t/i";
            case BIGINT: case BIGSERIAL: case DOUBLE_PRECISION: case TIMESTAMP: case TIMESTAMPTZ:
            case TIME: case MONEY: return "8/t/d";
            case BOOLEAN: return "1/t/c";
            case UUID: return "16/f/c";
            case INTERVAL: return "16/f/d";
            case TIMETZ: return "12/f/d";
            case CHAR: case VARCHAR: case TEXT: return "-1/f/i";
            case NAME: return "64/f/c";
            case POINT: return "16/f/d";
            case LSEG: case BOX: return "32/f/d";
            case LINE: return "24/f/d";
            case CIRCLE: return "24/f/d";
            default: return "-1/f/i";
        }
    }

    // ---- ALTER on an object kind memgres records but does not implement ----

    /** Separator inside the encoded payload of an ALTER stub; SQL text cannot contain it. */
    private static final String OBJ_SEP = "\u0001";

    /** Text search objects PostgreSQL ships with, which exist without ever being created. */
    private static final Set<String> BUILTIN_TS_CONFIGS = Cols.setOf("simple", "english");
    private static final Set<String> BUILTIN_TS_DICTS = Cols.setOf("simple", "english_stem");
    private static final Set<String> BUILTIN_TS_PARSERS = Cols.setOf("default");
    private static final Set<String> BUILTIN_TS_TEMPLATES =
            Cols.setOf("simple", "snowball", "synonym");

    /** Extensions a PostgreSQL database has without installing them, and their versions. */
    private static final Map<String, String> BUILTIN_EXTENSIONS = builtinExtensions();

    private static Map<String, String> builtinExtensions() {
        Map<String, String> m = new HashMap<>();
        m.put("plpgsql", "1.0");
        return m;
    }

    /**
     * {@code ALTER <kind> name ...} for the kinds memgres keeps in a registry of its own. None of
     * these alterations change anything memgres stores, but reporting success for one on a name
     * that was never created is what lets a script go on believing the object is there.
     */
    QueryResult executeAlterObject(String payload) {
        String[] parts = (payload == null ? "" : payload).split(OBJ_SEP, -1);
        String kind = parts.length > 0 ? parts[0] : "";
        String name = parts.length > 1 ? parts[1] : "";
        String newName = parts.length > 2 ? parts[2] : "";
        String extra = parts.length > 3 ? parts[3] : "";
        String intoSchema = parts.length > 4 ? parts[4] : "";
        String newOwner = parts.length > 5 ? parts[5] : "";
        requireObjectExists(kind, name);
        // The object is there; the schema or role the statement points at has to be there too.
        // PostgreSQL checks it whether or not anything is then recorded, and a script told the
        // move happened will believe it did.
        if (!intoSchema.isEmpty()) requireSchemaExists(intoSchema);
        if (!newOwner.isEmpty()) requireOwnerExists(newOwner);
        if (!newName.isEmpty()) renameRegisteredObject(kind, name, newName);
        // ALTER EXTENSION ... UPDATE with nothing to update to says so. Only an extension whose
        // version memgres actually knows can be named honestly here; the ones it accepts without
        // implementing carry no version PostgreSQL would agree with.
        if (kind.equals("extension") && extra.equals("update") && executor.session != null) {
            String version = BUILTIN_EXTENSIONS.get(name.toLowerCase());
            if (version != null) {
                executor.session.addNotice("NOTICE", "00000", "version \"" + version
                        + "\" of extension \"" + name + "\" is already installed", null);
            }
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    /**
     * {@code DROP <kind> [IF EXISTS] name} for those same kinds. A DROP that reports success on a
     * name that was never created reads as if the object had been there.
     */
    QueryResult executeDropObject(String payload) {
        String[] parts = (payload == null ? "" : payload).split(OBJ_SEP, -1);
        String kind = parts.length > 0 ? parts[0] : "";
        String name = parts.length > 1 ? parts[1] : "";
        boolean ifExists = parts.length > 2 && parts[2].equals("1");
        Database db = executor.database;
        if (!objectExists(kind, name)) {
            if (!ifExists) {
                throw new MemgresException(kind + " \"" + name + "\" does not exist", "42704");
            }
            noticeSkipped(kind + " \"" + name + "\"");
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (kind.equals("text search configuration")) db.removeTsConfig(name);
        else if (kind.equals("text search dictionary")) db.removeTsDict(name);
        else if (kind.equals("foreign-data wrapper")) db.removeForeignDataWrapper(name);
        else if (kind.equals("server")) db.removeForeignServer(name);
        else if (kind.equals("publication")) db.removePublication(name);
        else if (kind.equals("subscription")) db.removeSubscription(name);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    /** Refuse a name of this kind that was never created, in PostgreSQL's words for the kind. */
    void requireObjectExists(String kind, String name) {
        if (name == null || name.isEmpty()) return;
        String lower = name.toLowerCase();
        Database db = executor.database;
        boolean exists;
        if (kind.equals("publication")) {
            exists = db.getPublication(name) != null;
        } else if (kind.equals("text search configuration")) {
            exists = BUILTIN_TS_CONFIGS.contains(lower) || db.getTsConfigs().containsKey(lower);
        } else if (kind.equals("text search dictionary")) {
            exists = BUILTIN_TS_DICTS.contains(lower) || db.getTsDicts().containsKey(lower);
        } else if (kind.equals("text search parser")) {
            exists = BUILTIN_TS_PARSERS.contains(lower);
        } else if (kind.equals("text search template")) {
            exists = BUILTIN_TS_TEMPLATES.contains(lower);
        } else if (kind.equals("foreign-data wrapper")) {
            exists = db.getForeignDataWrappers().containsKey(lower);
        } else if (kind.equals("server")) {
            exists = db.getForeignServer(name) != null;
        } else if (kind.equals("subscription")) {
            exists = db.getSubscriptions().containsKey(lower);
        } else if (kind.equals("extension")) {
            exists = db.hasExtension(name) || BUILTIN_EXTENSIONS.containsKey(lower);
        } else if (kind.equals("large object")) {
            exists = db.getLargeObjectStore().exists(parseLargeObjectId(name));
            if (!exists) {
                throw new MemgresException("large object " + name + " does not exist", "42704");
            }
            return;
        } else if (kind.equals("foreign table")) {
            // A foreign table lives in the relation namespace, so PostgreSQL reports the name as
            // a relation rather than naming the kind.
            exists = db.getForeignTables().containsKey(lower);
            if (!exists) {
                throw new MemgresException("relation \"" + name + "\" does not exist", "42P01");
            }
            return;
        } else {
            return;
        }
        if (!exists) {
            throw new MemgresException(kind + " \"" + name + "\" does not exist", "42704");
        }
    }

    /** True when a name of this kind is one memgres has recorded or PostgreSQL ships with. */
    private boolean objectExists(String kind, String name) {
        try {
            requireObjectExists(kind, name);
            return true;
        } catch (MemgresException e) {
            return false;
        }
    }

    private static long parseLargeObjectId(String text) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    /** A rename that reports success has to move the name the object answers to. */
    private void renameRegisteredObject(String kind, String name, String newName) {
        Database db = executor.database;
        String lower = name.toLowerCase();
        if (kind.equals("text search configuration")) {
            Database.TsConfigDef cfg = db.getTsConfigs().get(lower);
            if (cfg == null) return;
            if (!lower.equals(newName.toLowerCase())
                    && (BUILTIN_TS_CONFIGS.contains(newName.toLowerCase())
                        || db.getTsConfigs().containsKey(newName.toLowerCase()))) {
                throw new MemgresException("text search configuration \"" + newName
                        + "\" already exists in schema \"" + executor.defaultSchema() + "\"", "42710");
            }
            db.removeTsConfig(name);
            db.addTsConfig(new Database.TsConfigDef(newName, cfg.parserName, cfg.copyFrom));
        } else if (kind.equals("text search dictionary")) {
            Database.TsDictDef dict = db.getTsDicts().get(lower);
            if (dict == null) return;
            if (!lower.equals(newName.toLowerCase())
                    && (BUILTIN_TS_DICTS.contains(newName.toLowerCase())
                        || db.getTsDicts().containsKey(newName.toLowerCase()))) {
                throw new MemgresException("text search dictionary \"" + newName
                        + "\" already exists in schema \"" + executor.defaultSchema() + "\"", "42710");
            }
            db.removeTsDict(name);
            db.addTsDict(new Database.TsDictDef(newName, dict.template, dict.options));
        } else if (kind.equals("server")) {
            Database.FdwServer srv = db.getForeignServer(name);
            if (srv == null) return;
            if (db.getForeignServer(newName) != null && !lower.equals(newName.toLowerCase())) {
                throw new MemgresException("server \"" + newName + "\" already exists", "42710");
            }
            db.removeForeignServer(name);
            db.addForeignServer(new Database.FdwServer(newName, srv.fdwName, srv.options));
        } else if (kind.equals("foreign-data wrapper")) {
            Database.FdwWrapper fdw = db.getForeignDataWrappers().get(lower);
            if (fdw == null) return;
            if (db.getForeignDataWrappers().containsKey(newName.toLowerCase())
                    && !lower.equals(newName.toLowerCase())) {
                throw new MemgresException(
                        "foreign-data wrapper \"" + newName + "\" already exists", "42710");
            }
            db.removeForeignDataWrapper(name);
            db.addForeignDataWrapper(new Database.FdwWrapper(newName, fdw.options));
        } else if (kind.equals("subscription")) {
            Database.SubDef sub = db.getSubscriptions().get(lower);
            if (sub == null) return;
            if (db.getSubscriptions().containsKey(newName.toLowerCase())
                    && !lower.equals(newName.toLowerCase())) {
                throw new MemgresException(
                        "subscription \"" + newName + "\" already exists", "42710");
            }
            db.removeSubscription(name);
            db.addSubscription(new Database.SubDef(newName, sub.conninfo, sub.publication));
        }
    }

    /** Resolve a SQL type name to its OID, checking built-in types first, then domains/enums. */
    private int resolveTypeOid(String typeName) {
        DataType dt = DataType.fromPgName(typeName);
        if (dt != null) return dt.getOid();
        String key = TypeNamespace.oidKeyFor(executor.database, typeName);
        return executor.systemCatalog.getOid(
                key != null ? key : TypeNamespace.oidKey(null, typeName));
    }
}
