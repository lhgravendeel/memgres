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
                ? stmt.schemaName().toLowerCase(java.util.Locale.ROOT) : executor.creationSchema();
        boolean wasShell = executor.database.getShellTypes().contains(TypeNamespace.key(schema, name));
        // A base type is defined in two steps: the shell reserves the name, and this statement
        // fills it in. Without the shell there is nothing to fill in, and PostgreSQL says so.
        if (stmt.baseTypeDefinition() && !wasShell) {
            throw new MemgresException("type \"" + name + "\" does not exist", "42710");
        }
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
                // A label is a name, so it is bounded the way every name is: PostgreSQL refuses
                // one longer than 63 bytes rather than storing a label nothing can write back.
                if (label != null
                        && label.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 63) {
                    throw new MemgresException(
                            "invalid enum label \"" + label + "\"", "42602");
                }
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
            executor.identity().typeCreated("e", TypeNamespace.key(schema, name));
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
            executor.identity().typeCreated("r", TypeNamespace.key(schema, name));
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
            executor.identity().typeCreated("c", TypeNamespace.key(schema, name));
            executor.database.addCompositeType(schema, name, stmt.compositeFields());
            executor.database.registerSchemaObject(schema, "composite", name);
            executor.recordUndo(new Session.CreateCompositeTypeUndo(schema, name));
        }
        if (wasShell) executor.database.getShellTypes().remove(TypeNamespace.key(schema, name));
        // A type belongs to whoever created it, and both its ownership and the grantor written
        // into its ACL are read from that. Recorded nowhere, every type belonged to the role
        // the server runs as rather than to the session that made it.
        executor.database.setObjectOwner("type:" + TypeNamespace.key(schema, name),
                executor.currentRole());
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
        // Every type has an owner, so OWNER TO names one whatever kind of type it is. Only the
        // enums were looked up, so giving away a composite or a domain said no such type.
        if (stmt.action() == AlterTypeStmt.Action.OWNER_TO && named != null
                && !executor.database.getCustomEnums().containsKey(named)) {
            requireOwnerExists(stmt.value());
            executor.database.setObjectOwner("type:" + named,
                    executor.ddlExecutor.resolveOwnerName(stmt.value()));
            return QueryResult.command(QueryResult.Type.ALTER_TYPE, 0);
        }
        // A range is a type like the others and answers to the statements that name a type. Only
        // the enums were looked up below, so a range could not be renamed or moved at all.
        if (stmt.action() == AlterTypeStmt.Action.RENAME_TO
                && named != null && executor.database.isRangeType(named)) {
            String schema = TypeNamespace.schemaOfKey(named);
            String bare = TypeNamespace.nameOfKey(named);
            requireTypeNameFree(typeRef(schema, stmt.value()));
            String subtype = executor.database.getRangeSubtype(named);
            // The multirange keeps the name it was created with: renaming a range renames the
            // range and nothing else.
            String multirange = executor.database.getMultirangeName(named);
            executor.database.removeRangeType(named);
            executor.database.addRangeType(schema, stmt.value(), subtype, multirange);
            executor.database.unregisterSchemaObject(schema, "range", bare);
            executor.database.registerSchemaObject(schema, "range", stmt.value());
            executor.database.moveComment("type", named, TypeNamespace.key(schema, stmt.value()));
            retargetTypeColumns(named, TypeNamespace.key(schema, stmt.value()));
            executor.identity().typeRenamed("r", named, TypeNamespace.key(schema, stmt.value()));
            return QueryResult.command(QueryResult.Type.ALTER_TYPE, 0);
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

        // The state type is what the whole definition is written in terms of, so a name that is
        // no type is reported before anything is looked up in terms of it.
        validateTypeExists(stmt.stype());
        // FINALFUNC_MODIFY says how the final function may treat the state, and it has three
        // answers; a word that is none of them is a fault in the statement.
        if (stmt.finalfuncModify() != null
                && !FINALFUNC_MODIFY_VALUES.contains(
                        stmt.finalfuncModify().toUpperCase(java.util.Locale.ROOT))) {
            throw new MemgresException("parameter \"finalfunc_modify\" must be READ_ONLY,"
                    + " SHAREABLE, or READ_WRITE", "42601");
        }
        // The moving-aggregate functions belong to the moving state, so naming one without
        // saying what that state is describes nothing.
        if (stmt.minvfunc() != null && stmt.mstype() == null) {
            throw new MemgresException(
                    "aggregate minvfunc must not be specified without mstype", "42P13");
        }
        // The transition function takes the running state plus the aggregated arguments; an
        // ordered-set aggregate's direct arguments go to the final function instead.
        List<String> transArgs = new ArrayList<>();
        transArgs.add(stmt.stype());
        transArgs.addAll(aggregatedArgTypes(stmt));
        requireFunctionSignature(stmt.sfunc(), transArgs);
        if (stmt.finalfunc() != null) {
            // FINALFUNC_EXTRA hands the aggregate's own arguments to the final function as well,
            // so what the function has to take is a longer list than the state alone.
            List<String> finalArgs = new ArrayList<>();
            finalArgs.add(stmt.stype());
            if (stmt.finalfuncExtra()) finalArgs.addAll(aggregatedArgTypes(stmt));
            requireFunctionSignature(stmt.finalfunc(), finalArgs);
        }
        // The initial state is a value of the state type, read by that type's own reader.
        if (stmt.initcond() != null) {
            executor.castValue(stmt.initcond(), stmt.stype());
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

    /** What FINALFUNC_MODIFY may say about how the final function treats the state. */
    private static final Set<String> FINALFUNC_MODIFY_VALUES =
            Cols.setOf("READ_ONLY", "SHAREABLE", "READ_WRITE");

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
    /**
     * An operator key written back as the operator: {@code public.###@(int,int)} reads
     * {@code integer ###@ integer}, and a prefix operator leaves its left side out.
     */
    private static String writtenOperator(String key) {
        int paren = key.indexOf('(');
        if (paren < 0 || !key.endsWith(")")) return key;
        String name = key.substring(0, paren);
        int dot = name.lastIndexOf('.');
        if (dot >= 0) name = name.substring(dot + 1);
        String[] operands = key.substring(paren + 1, key.length() - 1).split(",");
        String left = operands.length > 0 ? operands[0].trim() : "none";
        String right = operands.length > 1 ? operands[1].trim() : "none";
        StringBuilder out = new StringBuilder();
        if (!"none".equalsIgnoreCase(left)) {
            out.append(DataType.canonicalName(left)).append(' ');
        }
        out.append(name);
        if (!"none".equalsIgnoreCase(right)) {
            out.append(' ').append(DataType.canonicalName(right));
        }
        return out.toString();
    }

    /** What a restriction estimator is declared over, and what a join estimator is. */
    private static final List<String> RESTRICT_ESTIMATOR_ARGS =
            java.util.Arrays.asList("internal", "oid", "internal", "integer");
    private static final List<String> JOIN_ESTIMATOR_ARGS =
            java.util.Arrays.asList("internal", "oid", "internal", "smallint", "internal");

    private void requireEstimator(String named, List<String> argTypes) {
        if (named == null || named.isEmpty()) return;
        requireFunctionSignature(named, argTypes);
    }

    private void requireFunctionSignature(String funcName, List<String> argTypes) {
        String bare = funcName.contains(".")
                ? funcName.substring(funcName.lastIndexOf('.') + 1) : funcName;
        List<PgFunction> overloads = executor.database.getFunctionOverloads(bare);
        if (overloads.isEmpty()) {
            // A built-in whose signatures are recorded is held to them: PostgreSQL has int4, and
            // does not have int4(character varying). One whose signatures are not recorded is
            // accepted on its name, which is as much as is known about it.
            if (builtinSignatureMatches(bare, argTypes)) return;
            if (!BuiltinCallTypes.records(bare) && isKnownBuiltinFunction(funcName)) return;
        } else {
            for (PgFunction f : overloads) {
                if (paramTypesMatch(f, argTypes)) return;
            }
        }
        throw new MemgresException(
                "function " + funcName + "(" + canonicalTypeList(argTypes) + ") does not exist",
                "42883").withoutHint();
    }

    /** Whether a recorded built-in signature of this name takes exactly these argument types. */
    private static boolean builtinSignatureMatches(String bare, List<String> argTypes) {
        List<String> wanted = new ArrayList<>();
        for (String written : argTypes) wanted.add(DataType.canonicalName(written));
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(bare)) continue;
            String declared = signature[2].trim();
            List<String> theirs = new ArrayList<>();
            if (!declared.isEmpty()) {
                for (String oid : declared.split("\\s+")) {
                    DataType type = DataType.fromOid(Integer.parseInt(oid));
                    theirs.add(type == null ? oid : DataType.canonicalName(type.getPgName()));
                }
            }
            if (theirs.equals(wanted)) return true;
        }
        return false;
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

        // The operand types are read before the function is looked for: an operator over a type
        // nobody declared names no type, and there is no signature to look for until both sides
        // name one. Looked for first, the complaint was about a routine whose name the statement
        // never got wrong.
        validateTypeExists(stmt.leftArg());
        validateTypeExists(stmt.rightArg());
        // Validate that the backing function exists (skip for well-known built-in PG functions)
        List<String> opArgs = new ArrayList<>();
        if (stmt.leftArg() != null) opArgs.add(stmt.leftArg());
        if (stmt.rightArg() != null) opArgs.add(stmt.rightArg());
        requireFunctionSignature(stmt.function(), opArgs);
        // The two estimators are routines of their own, with signatures of their own; an
        // operator naming one the server has not got points the planner at nothing.
        requireEstimator(stmt.restrict(), RESTRICT_ESTIMATOR_ARGS);
        requireEstimator(stmt.join(), JOIN_ESTIMATOR_ARGS);

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
            String role = executor.currentRole();
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
        // A family belongs to an access method, so there has to be one.
        AccessMethods.require(stmt.method());
        PgOperatorFamily fam = new PgOperatorFamily(stmt.name(), stmt.method());
        if (stmt.schema() != null) fam.setSchemaName(stmt.schema());
        if (executor.session != null) {
            String role = executor.currentRole();
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
        checkOperatorClassDefinition(stmt);
        PgOperatorClass cls = new PgOperatorClass(stmt.name(), stmt.forType(), stmt.method(), stmt.isDefault());
        if (stmt.schema() != null) cls.setSchemaName(stmt.schema());
        cls.setFamilyName(stmt.familyName());
        if (executor.session != null) {
            String role = executor.currentRole();
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

    /**
     * Everything an operator class names has to be there before it is recorded.
     *
     * <p>The access method, the type it is for, the family it joins, the strategy and support
     * numbers it uses, and the operators and functions it points at. None of it was checked, so
     * a class over a type nobody declared, using a method nobody implements, naming a strategy
     * the method has not got, was written down and reported as created.
     */
    private void checkOperatorClassDefinition(CreateOperatorClassStmt stmt) {
        AccessMethods.require(stmt.method());
        requireOperatorClassType(stmt.forType());
        if (stmt.familyName() != null) {
            String key = stmt.familyName().toLowerCase(java.util.Locale.ROOT) + ":"
                    + stmt.method().toLowerCase(java.util.Locale.ROOT);
            if (!executor.database.hasOperatorFamily(key)) {
                throw new MemgresException("operator family \"" + stmt.familyName()
                        + "\" does not exist for access method \"" + stmt.method() + "\"", "42704");
            }
        }
        for (OperatorClassItem item : stmt.items()) {
            boolean isOperator = item.kind() == OperatorClassItem.Kind.OPERATOR;
            AccessMethods.requireNumberInRange(stmt.method(), isOperator, item.number());
            if (isOperator) {
                requireStrategyOperator(item, stmt.forType());
            } else if (item.name() != null && !item.name().isEmpty()) {
                requireFunctionSignature(item.name(), item.argTypes());
            }
        }
        // A type has one default operator class per access method, and the built-in types all
        // already have theirs.
        if (stmt.isDefault() && defaultOperatorClassExists(stmt.forType(), stmt.method())) {
            throw new MemgresException("could not make operator class \"" + stmt.name()
                    + "\" be default for type " + stmt.forType(), "42710");
        }
    }

    /** The type an operator class is over has to be a type. */
    private void requireOperatorClassType(String name) {
        if (name == null) return;
        String bare = name.toLowerCase(java.util.Locale.ROOT).trim();
        while (bare.endsWith("[]")) bare = bare.substring(0, bare.length() - 2).trim();
        if (bare.isEmpty() || DataType.fromPgName(bare) != null) return;
        if (executor.database.isCompositeType(bare) || executor.database.getDomain(bare) != null
                || executor.database.getCustomEnum(bare) != null
                || executor.database.isRangeType(bare)) {
            return;
        }
        throw new MemgresException("type \"" + name + "\" does not exist", "42704");
    }

    /** The operator a strategy points at has to exist for the types the class is over. */
    private void requireStrategyOperator(OperatorClassItem item, String forType) {
        String symbol = item.name();
        if (symbol == null || symbol.isEmpty()) return;
        String left = item.argTypes().isEmpty() ? forType : item.argTypes().get(0);
        String right = item.argTypes().size() > 1 ? item.argTypes().get(1) : left;
        // The operators the server itself provides are not in the user operator table.
        if (PgOperatorTable.isKnownOperatorName(symbol)) return;
        String l = left == null ? "NONE" : left.toLowerCase(java.util.Locale.ROOT);
        String r = right == null ? "NONE" : right.toLowerCase(java.util.Locale.ROOT);
        // An operator may be written with the schema that holds it, and then that is the only
        // schema it is looked for in.
        int dot = symbol.lastIndexOf('.');
        if (dot > 0) {
            String schema = symbol.substring(0, dot).toLowerCase(java.util.Locale.ROOT);
            String bare = symbol.substring(dot + 1);
            if (executor.database.hasOperator(schema + "." + bare + "(" + l + "," + r + ")")) return;
            if (PgOperatorTable.isKnownOperatorName(bare)) return;
        } else {
            for (String schema : executor.relationSearchPath()) {
                if (executor.database.hasOperator(schema + "." + symbol + "(" + l + "," + r + ")")) {
                    return;
                }
            }
        }
        throw new MemgresException("operator does not exist: "
                + CatalogSystemFunctions.readableTypeName(left) + " " + symbol + " "
                + CatalogSystemFunctions.readableTypeName(right), "42883");
    }

    /** Whether this type already has a default operator class for the method. */
    private boolean defaultOperatorClassExists(String forType, String method) {
        for (PgOperatorClass c : executor.database.getUserOperatorClasses().values()) {
            if (c.isDefault() && c.getMethod().equalsIgnoreCase(method)
                    && c.getForType().equalsIgnoreCase(forType)) {
                return true;
            }
        }
        // Every built-in type ships with a default class for the two methods that index by
        // ordering and by hashing. The others have none until somebody writes one.
        String m = method == null ? "" : method.toLowerCase(java.util.Locale.ROOT);
        if (!"btree".equals(m) && !"hash".equals(m)) return false;
        return DataType.fromPgName(forType == null ? "" : forType.toLowerCase(java.util.Locale.ROOT))
                != null;
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
        String l = stmt.leftArg() != null ? stmt.leftArg().toLowerCase(java.util.Locale.ROOT) : "NONE";
        String r = stmt.rightArg() != null ? stmt.rightArg().toLowerCase(java.util.Locale.ROOT) : "NONE";
        String schema = "public";
        String opName = stmt.name();
        int dotIdx = opName.indexOf('.');
        if (dotIdx > 0) { schema = opName.substring(0, dotIdx); opName = opName.substring(dotIdx + 1); }
        String key = schema.toLowerCase(java.util.Locale.ROOT) + "." + opName + "(" + l + "," + r + ")";

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
        // Who the new owner is to be is settled before anything else, and before the family is
        // even looked for: a role that is not there is what PostgreSQL reports for
        // ALTER OPERATOR FAMILY nosuchfamily USING btree OWNER TO nosuchrole.
        requireNewOwnerExists(stmt);
        // A family belongs to an access method, so a method nothing implements is reported before
        // the family is looked for inside it.
        AccessMethods.require(stmt.method());
        String key = stmt.name().toLowerCase(java.util.Locale.ROOT) + ":" + stmt.method().toLowerCase(java.util.Locale.ROOT);
        PgOperatorFamily fam = executor.database.getOperatorFamily(key);
        if (fam == null) {
            throw new MemgresException("operator family \"" + stmt.name()
                    + "\" does not exist for access method \"" + stmt.method() + "\"", "42704");
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
                SchemaQualifier.requireSchema(executor.database, executor.session, stmt.value());
                fam.setSchemaName(stmt.value());
                break;
            case ADD_MEMBER:
            case DROP_MEMBER:
                alterFamilyMembers(stmt, fam);
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    /**
     * Fill or empty the places an ADD or DROP list names.
     *
     * <p>A place is a number and a pair of operand types, and the access method decides which
     * numbers it has at all. What fills the place has to exist to be put there; and a place that
     * was never filled cannot be emptied, which is what PostgreSQL says by naming the number and
     * the types back to the writer.
     */
    private void alterFamilyMembers(AlterOperatorStmt stmt, PgOperatorFamily fam) {
        boolean adding = stmt.action() == AlterOperatorStmt.AlterAction.ADD_MEMBER;
        for (AlterOperatorStmt.Member member : stmt.members()) {
            AccessMethods.requireNumberInRange(stmt.method(), !member.function, member.number);
            // A member covers a pair of operands, and one type named where two belong leaves the
            // clause unfinished rather than describing some other member.
            if (member.argTypes.size() < 2) {
                throw new MemgresException("missing argument", "42601");
            }
            List<String> canonical = new ArrayList<>();
            for (String written : member.argTypes) {
                validateTypeExists(written);
                canonical.add(DataType.canonicalName(written));
            }
            String place = (member.function ? "f" : "o") + member.written(canonical);
            if (adding) {
                if (!member.function && member.named != null && !member.named.isEmpty()
                        && canonical.size() == 2
                        && !executor.database.hasOperator("public." + member.named
                                + "(" + canonical.get(0) + "," + canonical.get(1) + ")")
                        && !PgOperatorTable.exists(member.named, canonical.get(0),
                                canonical.get(1))) {
                    throw new MemgresException("operator does not exist: " + canonical.get(0)
                            + " " + member.named + " " + canonical.get(1), "42883");
                }
                fam.addMember(place, new PgOperatorFamily.Member(member.function, member.number,
                        canonical.get(0), canonical.get(1), member.named));
            } else if (!fam.removeMember(place)) {
                throw new MemgresException((member.function ? "function " : "operator ")
                        + member.written(canonical) + " does not exist in operator family \""
                        + stmt.name() + "\"", "42704");
            }
        }
    }

    /** The role an OWNER TO names has to be one the server has, whatever is being altered. */
    private void requireNewOwnerExists(AlterOperatorStmt stmt) {
        if (stmt.action() != AlterOperatorStmt.AlterAction.OWNER_TO || stmt.value() == null) return;
        String owner = executor.ddlExecutor.resolveOwnerName(stmt.value());
        if (!executor.database.hasRole(owner)) {
            throw new MemgresException("role \"" + stmt.value() + "\" does not exist", "42704");
        }
    }

    private QueryResult executeAlterOperatorClassObj(AlterOperatorStmt stmt) {
        requireNewOwnerExists(stmt);
        String key = stmt.name().toLowerCase(java.util.Locale.ROOT) + ":" + stmt.method().toLowerCase(java.util.Locale.ROOT);
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
                SchemaQualifier.requireSchema(executor.database, executor.session, stmt.value());
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
        if (stmt.language() != null && !INSTALLED_LANGUAGES.contains(stmt.language().toLowerCase(java.util.Locale.ROOT))) {
            throw new MemgresException(
                    "language \"" + stmt.language().toLowerCase(java.util.Locale.ROOT) + "\" does not exist", "42704");
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
            if (baseRetType.toUpperCase(java.util.Locale.ROOT).startsWith("SETOF ")) {
                baseRetType = baseRetType.substring(6).trim();
            }
            validateTypeExists(baseRetType);
        }

        // SUPPORT clause: validate the support function exists (PG validates at CREATE time)
        if (stmt.supportFunction != null) {
            String supportFn = stmt.supportFunction;
            if (executor.database.getFunction(supportFn) == null
                    && executor.database.getFunction(supportFn.toLowerCase(java.util.Locale.ROOT)) == null) {
                throw new MemgresException("function " + supportFn + " does not exist", "42883");
            }
        }

        List<PgFunction.Param> params = new ArrayList<>();
        if (stmt.parsedParams() != null) {
            for (CreateFunctionStmt.FuncParam fp : stmt.parsedParams()) {
                // Validate parameter types exist (PG validates at CREATE time)
                if (fp.typeName() != null) {
                    validateTypeExists(executor, fp.typeName(), false);
                }
                // Validate default expression function references
                if (fp.defaultExpr() != null) {
                    validateDefaultExpr(fp.defaultExpr());
                }
                // PL/pgSQL: reject sqlstate and sqlerrm as parameter names (they are implicit CONSTANT variables)
                if ("plpgsql".equalsIgnoreCase(stmt.language()) && fp.name() != null) {
                    String lowerName = fp.name().toLowerCase(java.util.Locale.ROOT);
                    if ("sqlstate".equals(lowerName) || "sqlerrm".equals(lowerName)) {
                        throw new MemgresException(
                                "variable \"" + fp.name() + "\" is declared CONSTANT", "42601");
                    }
                }
                params.add(new PgFunction.Param(fp.name(), fp.typeName(), fp.mode(), fp.defaultExpr()));
            }
        }
        validateSignature(params);
        requireDistinctParameterNames(params);
        // A trigger function is called by the trigger machinery, which passes its arguments
        // through TG_ARGV rather than as parameters; one declared with parameters could never
        // be called at all.
        if ("trigger".equalsIgnoreCase(stmt.returnType()) && !params.isEmpty()) {
            throw new MemgresException("trigger functions cannot have declared arguments", "42P13");
        }
        requireRowsOnlyForASetReturner(stmt);

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
                    com.memgres.engine.plpgsql.PlpgsqlParser.parse(stmt.body(), stmt.name());
            List<String> paramNames = new ArrayList<>();
            boolean hasOutParams = false;
            for (int i = 0; i < params.size(); i++) {
                PgFunction.Param p = params.get(i);
                if (p.name() != null) paramNames.add(p.name());
                // Every parameter also answers to its position, which is the only name an
                // unnamed one has and what ALIAS FOR usually points at
                paramNames.add("$" + (i + 1));
                String mode = p.mode() == null ? "IN" : p.mode().toUpperCase(java.util.Locale.ROOT);
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
                    && !retType.toUpperCase(java.util.Locale.ROOT).startsWith("SETOF") && !"TABLE".equalsIgnoreCase(retType);
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
                            && !retExpr.toUpperCase(java.util.Locale.ROOT).startsWith("QUERY ")
                            && !retExpr.toUpperCase(java.util.Locale.ROOT).startsWith("NEXT ")) {
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
        pgFunc.setSetClauses(resolvedSetClauses(stmt.setClauses()));
        pgFunc.setOwner(executor.sessionUser());
        pgFunc.setAtomicBody(stmt.atomicBody);
        pgFunc.setSqlStandardBody(stmt.sqlStandardBody);
        if (stmt.parallel() != null) pgFunc.setParallel(stmt.parallel());
        // A cost nobody wrote is the language's own: 1 for the languages whose calls are compiled
        // in, 100 for an interpreted one. Taking 100 for all of them made every SQL function claim
        // to be a hundred times the work it is, which is a number a planner reads.
        if (stmt.cost() >= 0) pgFunc.setCost(stmt.cost());
        else pgFunc.setCost(defaultCostForLanguage(stmt.language()));
        if (stmt.rows() >= 0) pgFunc.setRows(stmt.rows());
        executor.database.addFunction(pgFunc);
        // PostgreSQL numbers a pg_proc row as CREATE FUNCTION writes it, so a list ordered by OID
        // -- what depends on a schema, what stands in the way of a drop -- reports routines in the
        // order they were made. Handing the number out only when something first asked about the
        // name put every routine after every relation, in whatever order the question happened to
        // walk the catalogue. CREATE OR REPLACE writes over the row that was already there and
        // keeps its number, which is why a replacement mints nothing.
        if (replaced == null) {
            executor.identity().routineCreated(
                    CatalogCoreBuilder.routineOidKey(executor.database, pgFunc));
        }
        executor.database.registerSchemaObject(funcSchema, "function", stmt.name());
        executor.database.setObjectOwner("function:" + stmt.name(), executor.sessionUser());
        // A routine gets the privileges ALTER DEFAULT PRIVILEGES set aside for the routines to
        // come, as a relation does: applied to relations alone, a grantee promised EXECUTE in
        // advance was promised nothing.
        DdlTableExecutor.applyDefaultPrivileges(executor, funcSchema,
                stmt.name().toLowerCase(java.util.Locale.ROOT), executor.currentRole(),
                "FUNCTIONS", "FUNCTION");
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
     * The SET clauses a routine was written with, as the catalogue keeps them.
     *
     * <p>SET ... FROM CURRENT takes the value the session holds when the routine is defined, so
     * the routine carries a value and not an instruction: the clause is resolved once, here.
     * Kept as the words "FROM CURRENT", what the catalogue reported was not a setting at all.
     */
    private java.util.Map<String, String> resolvedSetClauses(java.util.Map<String, String> written) {
        if (written == null || written.isEmpty()) return written;
        java.util.Map<String, String> resolved = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<String, String> e : written.entrySet()) {
            String value = e.getValue();
            if ("FROM CURRENT".equals(value)) {
                value = executor.session == null ? null
                        : executor.session.getGucSettings().getForDisplay(e.getKey());
                if (value == null) continue;
            }
            resolved.put(e.getKey(), value);
        }
        return resolved.isEmpty() ? null : resolved;
    }

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
        requireBodyThatCanReturn(stmt);
        try {
            // Validate type casts, function calls, and sequences in SQL body text
            validateSqlBodyReferences(stmt.body(), stmt.name());
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
                            "return type mismatch in function declared to return "
                            + spelledOutTypeName(stmt.returnType() != null ? stmt.returnType() : "unknown")
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
                "\\bCOLLATE\\s+\"?([\\w.]+)\"?", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(withoutStringContents(body));
        while (m.find()) {
            String collation = m.group(1).toLowerCase(java.util.Locale.ROOT);
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
    /**
     * @param beingCreated the function this body belongs to, which may call itself: PostgreSQL
     *                     records the routine before it reads the body, so a recursive SQL
     *                     function resolves its own name.
     */
    /**
     * The body with the contents of every string literal blanked out.
     *
     * <p>These checks read the body as text rather than as a parse tree, and a word inside a
     * string is not a word of the statement: {@code SELECT 'COLLATE nosuch'} names no collation,
     * and {@code SELECT 'x::nosuchtype'} casts to nothing. The quotes are left where they are so
     * every other offset in the text is unchanged.
     */
    private static String withoutStringContents(String body) {
        if (body == null) return "";
        StringBuilder out = new StringBuilder(body.length());
        boolean inString = false;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\'') {
                inString = !inString;
                out.append(c);
                continue;
            }
            out.append(inString && c != '\n' ? ' ' : c);
        }
        return out.toString();
    }

    private void validateSqlBodyReferences(String body, String beingCreated) {
        // Check type casts: ::type_name
        // A type name may say which schema to look in, and the qualifier is part of the name
        // rather than a name of its own: reading only as far as the dot made pg_catalog.text a
        // cast to a type called pg_catalog.
        String scannable = withoutStringContents(body);
        java.util.regex.Matcher castMatcher = java.util.regex.Pattern.compile(
                "::\\s*([a-zA-Z_][a-zA-Z0-9_]*)(?:\\s*\\.\\s*([a-zA-Z_][a-zA-Z0-9_]*))?")
                .matcher(scannable);
        while (castMatcher.find()) {
            String typeName = castMatcher.group(2) == null ? castMatcher.group(1)
                    : castMatcher.group(1) + "." + castMatcher.group(2);
            String bare = castMatcher.group(2) == null ? typeName : castMatcher.group(2);
            // A built-in answers to pg_catalog's name as well as to none.
            if ("pg_catalog".equalsIgnoreCase(castMatcher.group(1)) && castMatcher.group(2) != null
                    && isKnownType(bare)) {
                continue;
            }
            if (!isKnownType(typeName)) {
                throw new MemgresException("type \"" + typeName + "\" does not exist", "42704");
            }
        }
        // Check function calls: name(...) that aren't table refs like INSERT INTO table(col)
        java.util.regex.Matcher fnMatcher = java.util.regex.Pattern.compile(
                "(?:^|\\s)([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(").matcher(body);
        while (fnMatcher.find()) {
            String fnName = fnMatcher.group(1).toLowerCase(java.util.Locale.ROOT);
            if (isKnownSqlKeyword(fnName)) continue;
            if (isBuiltinFunction(fnName)) continue;
            // Skip if preceded by INTO, FROM, TABLE, UPDATE, JOIN (table reference, not function call)
            // Also skip if preceded by ')' which indicates a table alias like ') t(col)'
            int start = fnMatcher.start(1);
            String before = body.substring(0, start).trim().toLowerCase(java.util.Locale.ROOT);
            if (before.endsWith("into") || before.endsWith("from") || before.endsWith("table")
                    || before.endsWith("update") || before.endsWith("join")
                    || before.endsWith("on") || before.endsWith(")")) continue;
            if (beingCreated != null && fnName.equals(unqualified(beingCreated))) continue;
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

    /** A name with any schema qualification taken off, lower cased. */
    private static String unqualified(String name) {
        int dot = name.lastIndexOf('.');
        return (dot < 0 ? name : name.substring(dot + 1)).toLowerCase(java.util.Locale.ROOT);
    }

    private boolean isKnownType(String typeName) {
        if (BUILTIN_TYPES.contains(typeName.toLowerCase(java.util.Locale.ROOT))) return true;
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

    /**
     * A SQL function returns the result of its final statement, so a body holding no statement at
     * all can only return a value when there is no value to return. PostgreSQL reports the
     * mismatch against the declared type; {@code void} and a procedure are content with nothing.
     */
    /**
     * Two parameters of one routine cannot share a name: the body would have no way to say which
     * of them it meant.
     */
    private void requireDistinctParameterNames(List<PgFunction.Param> params) {
        java.util.Set<String> seen = new java.util.HashSet<String>();
        for (PgFunction.Param p : params) {
            if (p.name() == null || p.name().isEmpty()) continue;
            if (!seen.add(p.name().toLowerCase(java.util.Locale.ROOT))) {
                throw new MemgresException(
                        "parameter name \"" + p.name() + "\" used more than once", "42P13");
            }
        }
    }

    /**
     * ROWS estimates how many rows a call gives back, so it says nothing about a routine that
     * gives back one value.
     */
    private void requireRowsOnlyForASetReturner(CreateFunctionStmt stmt) {
        // A negative value is the mark for "no ROWS was written".
        if (stmt.rows() < 0) return;
        String returns = stmt.returnType();
        boolean setReturning = returns != null
                && (returns.toUpperCase(java.util.Locale.ROOT).startsWith("SETOF")
                    || returns.toUpperCase(java.util.Locale.ROOT).startsWith("TABLE"));
        if (!setReturning) {
            throw new MemgresException(
                    "ROWS is not applicable when function does not return a set", "22023");
        }
    }

    private void requireBodyThatCanReturn(CreateFunctionStmt stmt) {
        if (stmt.isProcedure()) return;
        String retType = stmt.returnType();
        if (retType == null || retType.isEmpty() || "void".equalsIgnoreCase(retType)
                || "trigger".equalsIgnoreCase(retType) || "event_trigger".equalsIgnoreCase(retType)) {
            return;
        }
        for (String piece : splitSqlStatements(stmt.body())) {
            if (!strippedOfComments(piece).isEmpty()) return;
        }
        // SETOF integer still reports the element type, which is what the function returns.
        String named = retType.trim();
        if (named.toUpperCase(java.util.Locale.ROOT).startsWith("SETOF ")) {
            named = named.substring("SETOF ".length()).trim();
        }
        throw new MemgresException(
                "return type mismatch in function declared to return " + spelledOutTypeName(named)
                + "\n  Detail: Function's final statement must be SELECT or"
                + " INSERT/UPDATE/DELETE RETURNING.", "42P13");
    }

    /** What is left of a body once comments and whitespace are taken away. */
    private static String strippedOfComments(String text) {
        if (text == null) return "";
        StringBuilder kept = new StringBuilder();
        for (String line : text.split("\n", -1)) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--")) continue;
            kept.append(trimmed);
        }
        return kept.toString().replace("/*", "").replace("*/", "").trim();
    }

    /** The name PostgreSQL prints for a declared type, so {@code int} reads as {@code integer}. */
    private static String spelledOutTypeName(String declared) {
        try {
            String canonical = DataType.canonicalName(declared);
            if (canonical != null && !canonical.isEmpty()) return canonical;
        } catch (RuntimeException ignored) {
            // A domain or a composite keeps the name it was declared with.
        }
        return declared;
    }

    private void validateSqlFunctionStatement(Statement parsed, CreateFunctionStmt stmt,
                                               List<PgFunction.Param> params) {
        String retType = stmt.returnType();

        if (parsed instanceof SelectStmt
                && (((SelectStmt) parsed).targets() == null || ((SelectStmt) parsed).targets().isEmpty())
                && (((SelectStmt) parsed).from() == null || ((SelectStmt) parsed).from().isEmpty())) {
            SelectStmt sel = (SelectStmt) parsed;
            throw new MemgresException(
                    "return type mismatch in function declared to return "
                    + spelledOutTypeName(retType != null ? retType : "integer")
                    + "\n  Detail: Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
                    "42P13");
        }

        if (parsed instanceof SelectStmt
                && ((SelectStmt) parsed).targets() != null && !((SelectStmt) parsed).targets().isEmpty()
                && retType != null && !retType.isEmpty()
                && !"void".equalsIgnoreCase(retType)
                && !retType.toUpperCase(java.util.Locale.ROOT).startsWith("SETOF")) {
            SelectStmt sel = (SelectStmt) parsed;
            Expression firstExpr = sel.targets().get(0).expr();
            if (firstExpr instanceof Literal
                    && ((Literal) firstExpr).literalType() == Literal.LiteralType.STRING) {
                Literal lit = (Literal) firstExpr;
                if (isNumericType(retType)) {
                    throw new MemgresException(
                            "return type mismatch in function declared to return "
                                    + spelledOutTypeName(retType)
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
        validateTypeExists(executor, typeName, true);
    }

    /**
     * @param quoted whether the name is wrapped in quotes when it is reported missing. Everywhere
     *     PostgreSQL looks a type name up it quotes it, except in a routine's parameter list,
     *     where the list is written back as it was read.
     */
    static void validateTypeExists(AstExecutor executor, String typeName, boolean quoted) {
        if (typeName == null || typeName.isEmpty()) return;
        String base = typeName.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        if (base.isEmpty()) return;
        // TABLE return type with column list is validated separately
        if (base.equalsIgnoreCase("TABLE")) return;
        if (BUILTIN_TYPES.contains(base.toLowerCase(java.util.Locale.ROOT))) return;
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
        throw new MemgresException(quoted
                ? "type \"" + base + "\" does not exist"
                : "type " + base + " does not exist", "42704");
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
            String fnName = m.group(1).toLowerCase(java.util.Locale.ROOT);
            // Skip built-in functions
            if (isBuiltinFunction(fnName)) continue;
            if (executor.database.getFunction(fnName) == null) {
                throw new MemgresException("function " + fnName + "() does not exist", "42883");
            }
        }
    }

    /** Check if function name looks like a known PG internal C function (used in CREATE OPERATOR/AGGREGATE). */
    /**
     * Whether the server itself provides a routine of this name.
     *
     * <p>Read off the registers the catalogue is built from — the callable built-ins, the
     * routines behind the built-in operators and their selectivity estimators, the built-in
     * aggregates and the routines that read and write each type. Guessed from the shape of the
     * name instead, every name ending in {@code eq} or {@code in} was taken for one, so an
     * operator could be created over a function nobody had written.
     */
    private static boolean isKnownBuiltinFunction(String rawName) {
        String name = rawName.contains(".") ? rawName.substring(rawName.lastIndexOf('.') + 1) : rawName;
        return BuiltinRoutineNames.contains(name)
                || BUILTIN_ROUTINE_NAMES.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    private static final Set<String> BUILTIN_ROUTINE_NAMES = builtinRoutineNames();

    private static Set<String> builtinRoutineNames() {
        Set<String> names = new java.util.HashSet<String>();
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            names.add(signature[0].toLowerCase(java.util.Locale.ROOT));
        }
        for (String[] window : BuiltinFunctionSignatures.WINDOW_FUNCTIONS) {
            names.add(window[0].toLowerCase(java.util.Locale.ROOT));
        }
        for (String[] aggregate : BuiltinAggregateSignatures.AGGREGATES) {
            names.add(aggregate[0].toLowerCase(java.util.Locale.ROOT));
        }
        for (Object[] operator : PgOperatorTable.OPERATORS) {
            names.add(((String) operator[5]).toLowerCase(java.util.Locale.ROOT));
            names.add(((String) operator[11]).toLowerCase(java.util.Locale.ROOT));
            names.add(((String) operator[12]).toLowerCase(java.util.Locale.ROOT));
        }
        for (Object[] cast : PgCastTable.CASTS) {
            names.add(((String) cast[2]).toLowerCase(java.util.Locale.ROOT));
        }
        // The routines each type is read and written by, named the way the catalogue names them.
        for (DataType type : DataType.values()) {
            String bare = type.getPgName().replace("[]", "").replace("\"", "");
            for (String suffix : new String[]{"in", "out", "recv", "send", "typmodin",
                    "typmodout", "_typanalyze"}) {
                names.add((bare + suffix).toLowerCase(java.util.Locale.ROOT));
            }
        }
        names.remove("");
        names.remove("-");
        return names;
    }

    private static boolean isBuiltinFunction(String name) {
        // The register the engine dispatches calls from, so a body may name anything the engine
        // can actually call. The list below it is the older hand-kept subset, which said no to
        // built-ins nobody had thought to add — ascii and cardinality among them.
        if (BuiltinFunctionNames.isCallable(name)) return true;
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
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase(java.util.Locale.ROOT);
            if ("OUT".equals(mode)) continue;
            if ("VARIADIC".equals(mode)) {
                String type = p.typeName() == null ? "" : p.typeName().trim().toLowerCase(java.util.Locale.ROOT);
                type = type.replace("\"", "");
                // VARIADIC "any" is the untyped form, which takes the arguments as they come
                if (!type.isEmpty() && !type.endsWith("[]") && !type.equals("anyarray")
                        && !type.equals("any") && !type.equals("anycompatiblearray")) {
                    throw new MemgresException("VARIADIC parameter must be an array", "42P13");
                }
                for (int j = i + 1; j < params.size(); j++) {
                    String laterMode = params.get(j).mode() == null
                            ? "IN" : params.get(j).mode().toUpperCase(java.util.Locale.ROOT);
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
        return NUMERIC_TYPES.contains(type.toLowerCase(java.util.Locale.ROOT).trim());
    }

    static void checkCastReturnTypeMismatch(String castTo, String retType) {
        String ct = castTo.toLowerCase(java.util.Locale.ROOT).trim();
        String rt = retType.toLowerCase(java.util.Locale.ROOT).trim();
        boolean castIsString = STRING_TYPES.contains(ct) || ct.startsWith("character varying") || ct.startsWith("character(");
        boolean retIsNumeric = NUMERIC_TYPES.contains(rt);
        boolean retIsString = STRING_TYPES.contains(rt) || rt.startsWith("character varying");
        boolean castIsNumeric = NUMERIC_TYPES.contains(ct);
        if (castIsString && retIsNumeric) {
            throw new MemgresException(
                    "return type mismatch in function declared to return " + spelledOutTypeName(retType)
                            + "\n  Detail: Actual return type is text.", "42P13");
        }
        if (castIsNumeric && retIsString) {
            throw new MemgresException(
                    "return type mismatch in function declared to return " + spelledOutTypeName(retType)
                            + "\n  Detail: Actual return type is integer.", "42P13");
        }
    }

    /** Split SQL body into individual statements separated by semicolons. */
    /** The statements a routine's body is written as. */
    private List<String> splitSqlStatements(String body) {
        List<String> pieces = SqlPieces.statementsIn(body);
        return pieces.isEmpty() ? Cols.listOf(body.trim()) : pieces;
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
            String mode = p.mode == null ? "IN" : p.mode.toUpperCase(java.util.Locale.ROOT);
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
                && CALL_AGGREGATES.contains(((FunctionCallExpr) expr).name().toLowerCase(java.util.Locale.ROOT))) {
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
        // A bare quoted literal has no type yet -- PostgreSQL calls it unknown and lets it reach
        // a parameter of any type, which is then what reads it. Calling it text made
        // CALL p('1.5') miss a p(int) that PostgreSQL reaches and then complains about the value.
        if (arg instanceof Literal
                && (((Literal) arg).literalType() == Literal.LiteralType.STRING
                    || ((Literal) arg).literalType() == Literal.LiteralType.NULL)) {
            return "unknown";
        }
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

    /** A value read as the type the parameter it is being passed to was declared with. */
    private Object argumentAsDeclared(Object value, String typeName) {
        if (value == null || typeName == null || typeName.trim().isEmpty()) return value;
        try {
            return executor.castValue(value, typeName);
        } catch (MemgresException e) {
            // A type this engine does not read here is not one to refuse the call over.
            if ("42704".equals(e.getSqlState())) return value;
            throw e;
        }
    }

    /**
     * Whether the name belongs to a routine this server has that is not a procedure.
     *
     * <p>CALL is for procedures, and a name that is a function is not one it can reach — so what
     * PostgreSQL reports is the routine it found and what kind it is, rather than saying nothing
     * of the name exists.
     */
    private boolean namesARoutineThatIsNotAProcedure(String callName) {
        String bare = callName.substring(callName.indexOf('.') + 1)
                .toLowerCase(java.util.Locale.ROOT);
        List<PgFunction> declared = executor.database.getFunctionOverloads(bare);
        if (declared != null) {
            for (PgFunction f : declared) {
                if (!f.isProcedure()) return true;
            }
        }
        return BuiltinFunctionNames.contains(bare) || BuiltinRoutineNames.contains(bare);
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
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase(java.util.Locale.ROOT);
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
        String want = declared.trim().toLowerCase(java.util.Locale.ROOT);
        int paren = want.indexOf('(');
        if (paren > 0) want = want.substring(0, paren).trim();
        String have = given.toLowerCase(java.util.Locale.ROOT);
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
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase(java.util.Locale.ROOT);
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
            String mode = p.mode() == null ? "IN" : p.mode().toUpperCase(java.util.Locale.ROOT);
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
        // A written schema is opened before the procedure inside it is looked for, so naming one
        // that is not there is reported as the missing schema rather than as a missing procedure.
        int callDot = callName.indexOf('.');
        if (callDot > 0) {
            SchemaQualifier.requireSchema(executor.database, executor.session,
                    callName.substring(0, callDot));
        }
        List<String> argTypes = callArgumentTypes(stmt.args());
        PgFunction function = resolveProcedure(callName, stmt.args().size(), argTypes);
        if (function == null) {
            // A routine of that name that is not a procedure is a different complaint: CALL is
            // for procedures, and PostgreSQL names the routine it found rather than saying there
            // is none.
            if (namesARoutineThatIsNotAProcedure(callName)) {
                throw new MemgresException(stmt.name() + "(" + String.join(", ", argTypes)
                        + ") is not a procedure", "42809");
            }
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
            String mode = p.mode() != null ? p.mode().toUpperCase(java.util.Locale.ROOT) : "IN";
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
            else byName.put(named.name().toLowerCase(java.util.Locale.ROOT), named.value());
        }
        for (PgFunction.Param p : function.getParams()) {
            String mode = p.mode() != null ? p.mode().toUpperCase(java.util.Locale.ROOT) : "IN";
            if ("OUT".equals(mode)) {
                if (placeholdersGiven && argIdx < stmt.args().size()
                        && !(stmt.args().get(argIdx) instanceof NamedArgExpr)) {
                    argIdx++;
                }
                continue;
            }
            Expression named = p.name() == null ? null : byName.get(p.name().toLowerCase(java.util.Locale.ROOT));
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
                // A value reaching a parameter is read as that parameter's type, which is where
                // an unknown literal is finally settled: CALL p('1.5') on a p(int) is the
                // integer's own reader refusing 1.5, not a procedure nobody has.
                args.add(argumentAsDeclared(executor.evalExpr(stmt.args().get(argIdx++), null),
                        p.typeName()));
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
                // An output parameter comes back as the type it was declared with, which is what
                // a client decodes it by; described as text, an integer arrived as a string.
                DataType declaredOut = p.typeName() == null ? null
                        : DataType.fromPgName(p.typeName().replaceAll("\\(.*\\)", "").trim());
                columns.add(new Column(colName,
                        declaredOut == null ? DataType.TEXT : declaredOut, true, false, null));
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
                throw new MemgresException("syntax error at or near \"" + event.toLowerCase(java.util.Locale.ROOT) + "\"", "42601");
            }
        }
        checkTriggerShape(stmt, timing, trigEvents, triggerTableSchema, isView);
        // The WHEN condition is analysed against the relation before the function the trigger will
        // call is looked for, so a condition that cannot stand is what PostgreSQL reports even when
        // the function is missing too.
        checkTriggerWhen(stmt, trigEvents, triggerTableSchema, isView);
        if (stmt.functionName() != null) {
            PgFunction trigFunc = executor.database.getFunction(stmt.functionName());
            // The server provides trigger functions of its own, and one of them is as much a
            // function to call as anything a reader wrote.
            if (trigFunc == null && !DmlTriggerHelper.isBuiltinTriggerFunction(stmt.functionName())) {
                throw new MemgresException("function " + stmt.functionName() + "() does not exist",
                        "42883").withoutHint();
            }
            String trigRetType = trigFunc == null ? null : trigFunc.getReturnType();
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
                String normalized = tag.trim().toUpperCase(java.util.Locale.ROOT);
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
            together.add(RelationNamespace.bareName(stmt.name()).toLowerCase(java.util.Locale.ROOT));
            for (DropStmt other : stmt.more()) {
                together.add(RelationNamespace.bareName(other.name()).toLowerCase(java.util.Locale.ROOT));
            }
        }
        // Which of the names reach an object is settled here, before any of them goes: a name
        // whose object another name in the same list takes with it — a view over a view the list
        // also names, a type another named type was declared from — is gone by the time its own
        // turn comes. PostgreSQL settles the whole set of names before it deletes any of it and
        // deletes each object once, so the second name is neither an error nor something to
        // report as skipped.
        Set<String> wasThere = new HashSet<>();
        for (DropStmt one : DropStmt.allOf(stmt)) {
            if (dropTargetPresent(one)) wasThere.add(dropTargetIdentity(one));
        }
        // A name the list gives twice stands for one object, and PostgreSQL drops it once rather
        // than reporting the second as missing.
        Set<String> named = new HashSet<>();
        named.add(dropTargetIdentity(stmt));
        QueryResult result = executeDropOne(stmt, together);
        for (DropStmt other : stmt.more()) {
            if (!named.add(dropTargetIdentity(other))) continue;
            if (wasThere.contains(dropTargetIdentity(other)) && !dropTargetPresent(other)) continue;
            result = executeDropOne(other, together);
        }
        return result;
    }

    /**
     * Whether the object one name in a DROP list stands for is still there, asked the way that
     * kind's own drop asks it. Only the kinds a drop in the same statement can reach are answered
     * for; anything else is taken to be where it was, so its own turn judges it as it always did.
     */
    private boolean dropTargetPresent(DropStmt stmt) {
        switch (stmt.objectType()) {
            case VIEW:
            case MATERIALIZED_VIEW:
                return executor.database.getView(stmt.name()) != null;
            case TYPE:
            case DOMAIN:
                return TypeNamespace.resolveParts(executor.database, executor.session,
                        stmt.schema(), stmt.name()) != null;
            default:
                return true;
        }
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
        return stmt.objectType() + ":" + schema.toLowerCase(java.util.Locale.ROOT) + "."
                + RelationNamespace.bareName(stmt.name()).toLowerCase(java.util.Locale.ROOT)
                + (stmt.paramTypes() == null ? "" : stmt.paramTypes().toString().toLowerCase(java.util.Locale.ROOT));
    }

    private QueryResult executeDropOne(DropStmt stmt, Set<String> together) {
        // A DROP that names a schema of its own is looking in that schema, so a schema which is
        // not there is what is missing — PostgreSQL reports 3F000 rather than naming an object
        // it never went looking for. SCHEMA and EXTENSION name no schema of their own.
        if (stmt.objectType() != DropStmt.ObjectType.SCHEMA
                && stmt.objectType() != DropStmt.ObjectType.EXTENSION
                && ddl.tableExecutor.checkDropSchemaExists(stmt.schema(), stmt.ifExists())) {
            return QueryResult.command(QueryResult.Type.DROP_TABLE, 0)
                    .withCommandTag(dropTagOf(stmt.objectType()));
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
            case ROUTINE:
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
                // DDL is transactional, so a drop whose statement or transaction rolls back never
                // happened. Without a record of what went, a rolled-back DROP DOMAIN left the
                // domain gone for good, and a DROP DOMAIN whose second name reached nothing had
                // already taken the first.
                executor.recordUndo(new Session.DropDomainUndo(TypeNamespace.schemaOfKey(key),
                        TypeNamespace.nameOfKey(key), executor.database.getDomains().get(key)));
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
                // A collation this server does not have is one it cannot drop, and what it has is
                // what pg_collation lists: a name that is not there is refused the way a missing
                // table is, and IF EXISTS says what it skipped instead.
                if (executor.database.getCollation(stmt.name()) == null) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("collation \"" + stmt.name()
                                + "\" for encoding \"UTF8\" does not exist", "42704");
                    }
                    noticeSkipped("collation \"" + stmt.name() + "\"");
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
                // A conversion has no behaviour here, but its name is remembered when it is
                // created: dropping one that was never created is refused rather than reported
                // as done, which is what left the next statement to fail somewhere unrelated.
                String bare = RelationNamespace.bareName(stmt.name());
                if (!executor.database.hasStubObject("conversion", bare)) {
                    if (stmt.ifExists()) break;
                    throw new MemgresException(
                            "conversion \"" + stmt.name() + "\" does not exist", "42704");
                }
                executor.database.removeStubObject("conversion", bare);
                break;
            }
            case CAST: {
                // name is encoded as "sourceType->targetType"
                String castName = stmt.name();
                if (castName != null && castName.contains("->")) {
                    String[] parts = castName.split("->");
                    // A cast is between two types, and a type that is not there is what
                    // PostgreSQL reports first: there is no cast to look for until both are.
                    // IF EXISTS excuses that too — a cast over a type nobody declared is a cast
                    // that is not there, which is exactly what the clause is for.
                    if (!stmt.ifExists()) {
                        validateTypeExists(parts[0].trim());
                        validateTypeExists(parts[1].trim());
                    } else if (!isKnownType(parts[0].trim()) || !isKnownType(parts[1].trim())) {
                        break;
                    }
                    int srcOid = resolveTypeOid(parts[0].trim());
                    int tgtOid = resolveTypeOid(parts[1].trim());
                    // A cast the server itself provides belongs to the server: dropping one
                    // would take away a conversion the rest of the catalogue depends on.
                    for (Object[] shipped : PgCastTable.CASTS) {
                        if ((Integer) shipped[0] == srcOid && (Integer) shipped[1] == tgtOid) {
                            throw new MemgresException("cannot drop cast from "
                                    + DataType.canonicalName(parts[0]) + " to "
                                    + DataType.canonicalName(parts[1])
                                    + " because it is required by the database system", "2BP01");
                        }
                    }
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
                    String searchPathKey = executor.defaultSchema().toLowerCase(java.util.Locale.ROOT)
                            + opKey.substring("public".length());
                    if (executor.database.hasOperator(searchPathKey)) opKey = searchPathKey;
                }
                if (!executor.database.hasOperator(opKey)) {
                    if (!stmt.ifExists()) {
                        // Named the way an operator is written: its operands around its symbol,
                        // with the side it has none on left out.
                        throw new MemgresException(
                                "operator does not exist: " + writtenOperator(stmt.name()),
                                "42883").withoutHint();
                    }
                }
                executor.database.removeOperator(opKey);
                break;
            }
            case OPERATOR_FAMILY: {
                String famMethod = stmt.onTable() != null ? stmt.onTable() : "btree";
                String famKey = stmt.name().toLowerCase(java.util.Locale.ROOT) + ":" + famMethod.toLowerCase(java.util.Locale.ROOT);
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
                String clsKey = stmt.name().toLowerCase(java.util.Locale.ROOT) + ":" + clsMethod.toLowerCase(java.util.Locale.ROOT);
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
        // The tag names the kind of object that was dropped, which the statement said and the
        // result type does not: every one of them reported itself as DROP TABLE.
        return QueryResult.command(QueryResult.Type.DROP_TABLE, 0)
                .withCommandTag(dropTagOf(stmt.objectType()));
    }

    /** The tag PostgreSQL reports for a DROP of this kind of object. */
    private static String dropTagOf(DropStmt.ObjectType kind) {
        if (kind == null) return "DROP TABLE";
        switch (kind) {
            case MATERIALIZED_VIEW: return "DROP MATERIALIZED VIEW";
            case OPERATOR_CLASS: return "DROP OPERATOR CLASS";
            case OPERATOR_FAMILY: return "DROP OPERATOR FAMILY";
            default: return "DROP " + kind.name().replace('_', ' ');
        }
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
                // A relation the same DROP names is not something the cascade reached:
                // PostgreSQL settles the whole set of names first and reports only what it had to
                // take besides. A materialized view is named by the kind it really is, because
                // that is the kind PostgreSQL recorded it under.
                Database.ViewDef going = executor.database.getView(dependent);
                if (!together.contains(RelationNamespace.bareName(dependent).toLowerCase(java.util.Locale.ROOT))) {
                    cascaded.add((going != null && going.materialized()
                            ? "materialized view " : "view ")
                            + RelationNamespace.bareName(dependent));
                }
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
        // A view is a relation, and a grant on it names it: both go together.
        executor.database.removePrivilegesOnObject("TABLE",
                AstExecutor.privilegeKey(dropViewSchema, stmt.name()));
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
        requireNoIdentityColumnNeedsIt(dependents, bareSeqName, seqSchema, visibleSchemas);
        if (!dependents.isEmpty() && !stmt.cascade()) {
            // PostgreSQL names an object the search path does not reach by its schema too, so the
            // reader can tell which of two same-named sequences the complaint is about.
            boolean visible = visibleSchemas.contains(seqSchema.toLowerCase(java.util.Locale.ROOT));
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
            clearSequenceDefaults(found);
        }
        executor.recordUndo(new Session.DropSequenceUndo(found.qualifiedName(), found));
        // A sequence is a relation, and a grant on it names it: both go together.
        executor.database.removePrivilegesOnObject("SEQUENCE",
                AstExecutor.privilegeKey(seqSchema, bareSeqName));
        executor.database.removeObjectOwner("sequence:" + bareSeqName);
        executor.database.removeSequence(seqSchema, bareSeqName);
        executor.database.removeObjectOwner("sequence:" + bareSeqName);
    }

    /** A column whose default draws from a particular sequence, and the table that holds it. */
    /**
     * Refuse to drop a sequence an identity column is made of.
     *
     * <p>An identity column does not merely default to a sequence: the sequence is part of what
     * the column is, so it goes when the column goes and not before -- CASCADE included, which
     * offers to drop what depends on the sequence and cannot offer to drop half a column.
     */
    private void requireNoIdentityColumnNeedsIt(List<SequenceDependent> dependents,
                                                String bareSeqName, String seqSchema,
                                                List<String> visibleSchemas) {
        for (SequenceDependent dep : dependents) {
            String written = dep.column.getDefaultValue();
            if (written == null || !written.contains("__identity__")) continue;
            boolean visible = visibleSchemas.contains(seqSchema.toLowerCase(java.util.Locale.ROOT));
            String shown = visible ? bareSeqName : seqSchema + "." + bareSeqName;
            MemgresException e = new MemgresException("cannot drop sequence " + shown
                    + " because column " + dep.columnName() + " of table "
                    + dep.tableRef(visibleSchemas) + " requires it", "2BP01");
            e.setHint("You can drop column " + dep.columnName() + " of table "
                    + dep.tableRef(visibleSchemas) + " instead.");
            throw e;
        }
    }

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
            return searchPath.contains(schemaName.toLowerCase(java.util.Locale.ROOT))
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
        // What the index enforced on its table goes with it, and comes back with it: a unique
        // index is recorded twice over, once as an index and once as the rule its table is
        // checked against, and putting back only the first left a unique index that let a
        // duplicate through.
        Table indexOwner = null;
        List<StoredConstraint> enforced = new ArrayList<>();
        if (storedTable != null) {
            try {
                int dotIdx = storedTable.indexOf('.');
                String schema = dotIdx >= 0 ? storedTable.substring(0, dotIdx) : "public";
                String tableName = dotIdx >= 0 ? storedTable.substring(dotIdx + 1) : storedTable;
                Table t = executor.resolveTable(schema, tableName);
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getName() != null && sc.getName().equalsIgnoreCase(bareIndexName)) {
                        enforced.add(sc);
                    }
                }
                indexOwner = t;
                t.getConstraints().removeIf(sc -> sc.getName().equalsIgnoreCase(bareIndexName));
            } catch (MemgresException ignored) {}
        }
        // DDL is transactional, so a drop whose statement or transaction rolls back never
        // happened. Without a record of what went, a rolled-back DROP INDEX left the index gone
        // for good, and a DROP INDEX whose second name reached nothing had already taken the
        // first.
        executor.recordUndo(new DroppedIndexUndo(executor.database, indexKey, indexOwner, enforced));
        executor.database.removeIndex(indexSchema, bareIndexName);
    }

    /**
     * Undo a DROP INDEX. PostgreSQL rolls a catalogue change back whole, so an index a
     * rolled-back statement dropped is back on its table, still unique, still partial and still
     * built the way it was written; everything the drop cleared is read off the database as it
     * stands and put back under the same key.
     */
    private static final class DroppedIndexUndo implements Session.UndoEntry {
        private final String schema;
        private final String name;
        private final List<String> columns;
        private final String table;
        private final boolean unique;
        private final String method;
        private final String whereClause;
        private final Map<String, String> reloptions;
        private final List<String> columnOptions;
        private final List<String> includeColumns;
        private final boolean nullsNotDistinct;
        private final Table owner;
        private final List<StoredConstraint> enforced;

        DroppedIndexUndo(Database db, String indexKey, Table owner,
                         List<StoredConstraint> enforced) {
            this.owner = owner;
            this.enforced = enforced == null ? null : new ArrayList<>(enforced);
            this.schema = Database.idxSchema(indexKey);
            this.name = Database.idxName(indexKey);
            List<String> cols = db.getIndexColumns(indexKey);
            this.columns = cols == null ? null : new ArrayList<>(cols);
            this.table = db.getIndexTable(indexKey);
            this.unique = db.isUniqueIndex(indexKey);
            this.method = db.getIndexMethod(indexKey);
            this.whereClause = db.getIndexWhereClause(indexKey);
            Map<String, String> opts = db.getIndexReloptions(indexKey);
            this.reloptions = opts == null ? null : new LinkedHashMap<>(opts);
            List<String> colOpts = db.getIndexColumnOptions(indexKey);
            this.columnOptions = colOpts == null ? null : new ArrayList<>(colOpts);
            List<String> incl = db.getIndexIncludeColumns(indexKey);
            this.includeColumns = incl == null ? null : new ArrayList<>(incl);
            this.nullsNotDistinct = db.isIndexNullsNotDistinct(indexKey);
        }

        @Override
        public void undo(Database db) {
            if (columns == null) return;
            db.addIndex(schema, name, columns);
            db.addIndexMeta(schema, name, table, unique, method, whereClause);
            if (reloptions != null) db.setIndexReloptions(schema + "." + name, reloptions);
            db.setIndexColumnOptions(schema, name, columnOptions);
            db.setIndexIncludeColumns(schema, name, includeColumns);
            db.setIndexNullsNotDistinct(schema, name, nullsNotDistinct);
            if (owner != null && enforced != null) {
                for (StoredConstraint sc : enforced) {
                    boolean back = false;
                    for (StoredConstraint held : owner.getConstraints()) {
                        if (held == sc) { back = true; break; }
                    }
                    if (!back) owner.addConstraint(sc);
                }
            }
        }

        @Override
        public String toString() {
            return "DroppedIndexUndo[name=" + schema + "." + name + "]";
        }
    }

    /**
     * Refuse a drop that names a routine of the other kind.
     *
     * <p>DROP FUNCTION names a function and DROP PROCEDURE a procedure; DROP ROUTINE names either.
     * A routine of the name that is not of the kind written is the wrong kind of object rather
     * than a missing one, and IF EXISTS does not excuse it: it excuses a name that reaches
     * nothing, and this name reaches something.
     */
    private void requireRoutineKind(DropStmt stmt, List<PgFunction> candidates) {
        if (stmt.objectType() == DropStmt.ObjectType.ROUTINE) return;
        boolean wantProcedure = stmt.objectType() == DropStmt.ObjectType.PROCEDURE;
        for (PgFunction candidate : candidates) {
            if (candidate.isProcedure() == wantProcedure) return;
        }
        throw new MemgresException(stmt.name() + "(" + pgArgumentList(stmt.paramTypes()) + ")"
                + " is not a " + (wantProcedure ? "procedure" : "function"), "42809");
    }

    private void dropFunction(DropStmt stmt) {
        // An unqualified DROP only reaches the schemas the search_path makes visible.
        String schema = stmt.schema() != null ? stmt.schema() : visibleSchemaOfFunction(stmt.name());
        List<PgFunction> candidates = schema != null
                ? executor.database.getFunctionOverloads(schema, stmt.name())
                : Cols.<PgFunction>listOf();
        if (candidates.isEmpty()) {
            if (!stmt.ifExists()) {
                boolean procedure = stmt.objectType() == DropStmt.ObjectType.PROCEDURE;
                // A DROP with no argument list looked the name up on its own, so there is no
                // signature to report and nothing a cast could change — which is why PostgreSQL
                // says only that it found nothing by that name, and offers no advice.
                if (stmt.paramTypes() == null) {
                    throw new MemgresException("could not find a " + (procedure ? "procedure" : "function")
                            + " named \"" + stmt.name() + "\"", "42883");
                }
                MemgresException missing = new MemgresException(
                        (procedure ? "procedure " : "function ") + stmt.name()
                                + "(" + canonicalTypeList(stmt.paramTypes()) + ") does not exist",
                        "42883");
                // The routine was named by its signature, not resolved from a call's arguments.
                missing.setHint(null);
                throw missing;
            }
            noticeSkipped((stmt.objectType() == DropStmt.ObjectType.PROCEDURE
                    ? "procedure " : "function ") + stmt.name()
                    + "(" + pgArgumentList(stmt.paramTypes()) + ")");
            return;
        }
        requireRoutineKind(stmt, candidates);
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
            // A grant belongs to the routine, so it goes with it. Left behind, the role it was
            // written to could not be dropped: the database still held a grant on a routine
            // nobody could name.
            executor.database.removePrivilegesOnObject("FUNCTION", stmt.name());
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
            String written = t == null ? "" : t.trim().toLowerCase(java.util.Locale.ROOT);
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
     * Dropping a type something is declared as — a column, a composite type's attribute, a domain
     * written over it, a routine written in terms of it — would leave that thing naming nothing,
     * so without CASCADE they block the drop and with it they are taken along.
     *
     * <p>What the refusal names and what CASCADE removes are worked out from one list and taken
     * away in one place, because a drop that names a dependent and then leaves it standing is a
     * worse answer than either refusing or removing it.
     */
    private void refuseOrCascadeTypeDependents(DropStmt stmt, String key) {
        List<String> dependents = new ArrayList<>();
        List<String> lines = new ArrayList<>();
        collectTypeDependents(key, new HashSet<>(), dependents, lines);
        if (dependents.isEmpty()) return;
        if (!stmt.cascade()) {
            // One dependent per line of a single detail. Written as repeated labelled sections
            // only the first of them reached the client and the rest stayed inside the message.
            MemgresException e = new MemgresException("cannot drop type "
                    + TypeNamespace.display(executor.database, executor.session, key)
                    + " because other objects depend on it", "2BP01");
            e.setDetail(dependencyDetail(lines));
            e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
            throw e;
        }
        noticeDropCascades(executor, dependents);
        dropTypeDependents(key, null);
    }

    /**
     * Everything a drop of this type would leave naming nothing, in the order PostgreSQL reports
     * it. A domain written over the type is a type in its own right, so what was written over the
     * domain is in the way of this drop as well, and PostgreSQL names it straight after the domain
     * it hangs from rather than at the end of the list. Each line says which of the two types the
     * dependency is really on, so a reader can tell a direct dependent from one reached through a
     * domain.
     *
     * @param dependents how the cascade notice names each object
     * @param lines the same objects, each said to depend on the type its declaration reached
     */
    private void collectTypeDependents(String key, Set<String> seen, List<String> dependents,
                                       List<String> lines) {
        if (!seen.add(key)) return;
        String display = TypeNamespace.display(executor.database, executor.session, key);
        for (TypeUser user : declaredAsType(key)) {
            dependents.add(user.described);
            lines.add(user.described + " depends on type " + user.typeShown(display));
            if (user.domainKey() != null) {
                collectTypeDependents(user.domainKey(), seen, dependents, lines);
            }
        }
        for (TypeDependents.Dependent d : TypeDependents.writtenIn(executor.database,
                executor.systemCatalog, executor.searchPathSchemas(), typeNamed(key))) {
            dependents.add(d.described());
            lines.add(d.described() + " depends on type " + d.typeShown(display));
        }
    }

    /**
     * Take away everything written in terms of a type, which is what CASCADE means wherever a type
     * is dropped -- named by DROP TYPE, or carried off by the schema that holds it. Both statements
     * come here so that the same dependent meets the same end whichever of them asked.
     *
     * @param exceptSchema a schema whose own objects are going anyway, so that a schema drop does
     *                     not take a column off a relation it is about to remove entirely
     */
    private void dropTypeDependents(String key, String exceptSchema) {
        List<Session.UndoEntry> undo = new ArrayList<>();
        dropTypeDependents(key, exceptSchema, new HashSet<>(), undo);
        for (Session.UndoEntry entry : undo) executor.recordUndo(entry);
    }

    /**
     * The same, following a domain written over the type into what was written over the domain.
     * The domain cannot outlive the type it stands on, so nothing written over the domain can
     * outlive it either, and each of those goes before the domain that carried it off.
     */
    private void dropTypeDependents(String key, String exceptSchema, Set<String> done,
                                    List<Session.UndoEntry> undo) {
        if (!done.add(key)) return;
        for (TypeUser user : declaredAsType(key)) {
            if (exceptSchema != null && exceptSchema.equalsIgnoreCase(user.schemaName)) continue;
            if (user.domainKey() != null) {
                dropTypeDependents(user.domainKey(), exceptSchema, done, undo);
                takeDomainAway(user.domainKey(), undo);
                continue;
            }
            if (user.compositeKey() != null) {
                dropCompositeAttribute(user, undo);
                continue;
            }
            if (user.column == null) {
                // A typed table has no shape of its own once the type has gone -- its columns are
                // the type's -- so the whole relation goes rather than any column of it.
                ddl.tableExecutor.dropSingleTable(user.schemaName, user.table.getName(), true, true);
                continue;
            }
            int idx = user.table.getColumnIndex(user.column.getName());
            if (idx < 0) continue;
            List<Object> held = new ArrayList<>();
            for (Object[] row : user.table.getRows()) held.add(row[idx]);
            // The constraints written over the column go with it and have to come back with it,
            // after it: a key constraint is rebuilt over the column's position, so the column has
            // to be there first. Undo runs in reverse, so this is recorded ahead of the column's
            // own entry. Without it a rolled-back CASCADE gave the column back unconstrained, and
            // the unique index over it stopped refusing a duplicate.
            List<StoredConstraint> doomed = new ArrayList<>();
            for (StoredConstraint sc : user.table.getConstraints()) {
                if (sc.dependsOnColumn(user.column.getName())) doomed.add(sc);
            }
            if (!doomed.isEmpty()) {
                undo.add(new Session.DropColumnConstraintsUndo(user.schemaName,
                        user.table.getName(), doomed));
            }
            undo.add(new Session.DropColumnUndo(user.schemaName, user.table.getName(),
                    user.column, idx, held));
            user.table.removeColumn(user.column.getName());
            dropIndexesOverColumn(user.schemaName, user.table.getName(), user.column.getName());
        }
        for (TypeDependents.Dependent d : TypeDependents.writtenIn(executor.database,
                executor.systemCatalog, executor.searchPathSchemas(), typeNamed(key))) {
            if (exceptSchema != null && exceptSchema.equalsIgnoreCase(routineSchemaOf(d))) continue;
            TypeDependents.remove(executor.database, d, undo);
        }
    }

    /**
     * Take the attribute off the composite type, the way ALTER TYPE ... DROP ATTRIBUTE does. The
     * composite type outlives the drop of the type its attribute was declared as, so the attribute
     * is not taken out of the list: it keeps its number under a name nobody could have written,
     * and the attributes after it keep theirs.
     */
    private void dropCompositeAttribute(TypeUser user, List<Session.UndoEntry> undo) {
        List<CreateTypeStmt.CompositeField> fields =
                executor.database.getCompositeTypes().get(user.compositeKey());
        if (fields == null) return;
        // What a type is made of is shared state, so a transaction that rolls back has to find the
        // attribute list it started with — the same record ALTER TYPE keeps for itself.
        undo.add(new Session.AlterCompositeTypeUndo(user.compositeKey(), fields));
        List<CreateTypeStmt.CompositeField> kept = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            CreateTypeStmt.CompositeField f = fields.get(i);
            kept.add(f.name().equalsIgnoreCase(user.attributeName())
                    ? new CreateTypeStmt.CompositeField(Database.droppedAttributeName(i + 1),
                            f.typeName())
                    : f);
        }
        executor.database.replaceCompositeFields(user.compositeKey(), kept);
    }

    /**
     * Take a domain away because the type it was written over is going. A domain carried off this
     * way has to leave exactly what DROP DOMAIN leaves: a record to put it back if the transaction
     * rolls back, no entry in the schema's register, nothing said about it, and an OID that is
     * never handed to a domain created later under the same name.
     */
    private void takeDomainAway(final String key, List<Session.UndoEntry> undo) {
        DomainType domain = executor.database.getDomains().get(key);
        if (domain == null) return;
        String schema = TypeNamespace.schemaOfKey(key);
        String bare = TypeNamespace.nameOfKey(key);
        undo.add(new Session.DropDomainUndo(schema, bare, domain));
        inStoredTypes(() -> executor.database.getDomains().remove(key));
        executor.database.unregisterSchemaObject(schema, "domain", bare);
        executor.database.addComment("type", key, null);
        executor.identity().typeDropped("d", key);
    }

    /** The schema a dependent routine or aggregate lives in. */
    private static String routineSchemaOf(TypeDependents.Dependent dependent) {
        if (dependent.routine != null) return Database.schemaOf(dependent.routine);
        return dependent.aggregate.getSchemaName() != null
                ? dependent.aggregate.getSchemaName() : "public";
    }

    /**
     * An index over a column that has just gone is built on a column that is no longer there.
     * PostgreSQL drops it with the column; leaving it registered kept the index's name taken and
     * left pg_indexes describing an index over nothing.
     */
    private void dropIndexesOverColumn(String schemaName, String tableName, String columnName) {
        String qualified = schemaName + "." + tableName;
        java.util.regex.Pattern word = java.util.regex.Pattern.compile(
                "(?i)\\b" + java.util.regex.Pattern.quote(columnName) + "\\b");
        List<String> going = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry
                : executor.database.getIndexColumns().entrySet()) {
            String owner = executor.database.getIndexTable(entry.getKey());
            if (owner == null || !owner.equalsIgnoreCase(qualified)) continue;
            boolean uses = false;
            if (entry.getValue() != null) {
                for (String c : entry.getValue()) {
                    if (c.equalsIgnoreCase(columnName) || word.matcher(c).find()) {
                        uses = true;
                        break;
                    }
                }
            }
            String where = executor.database.getIndexWhereClause(entry.getKey());
            if (!uses && where != null && word.matcher(where).find()) uses = true;
            if (uses) going.add(entry.getKey());
        }
        for (String index : going) {
            // The index goes with the column, so a rolled-back drop has to bring both back.
            executor.recordUndo(new Session.DropIndexUndo(executor.database.snapshotIndex(index)));
            executor.database.removeIndex(index);
        }
    }

    /**
     * Whether a type name written in a declaration reaches the type stored under {@code key}. The
     * word a column or a parameter carries resolves through the search path exactly as it did when
     * it was written, so what is compared is the type each name reaches and not the text of it.
     */
    private TypeDependents.Names typeNamed(final String key) {
        return written -> key.equals(TypeNamespace.find(executor.database.typeKeys(), written));
    }

    /**
     * Clear the default of every column that draws from this sequence, recording what each one
     * held. The columns sit on relations the statement never named, so they are in no relation's
     * snapshot: without a record of its own a rolled-back drop left them with no default and the
     * next INSERT stopped filling them in.
     */
    private void clearSequenceDefaults(Sequence seq) {
        for (SequenceDependent dep : findSequenceDependents(seq)) {
            executor.recordUndo(new Session.ColumnDefaultUndo(dep.schemaName,
                    dep.table.getName(), dep.column.getName(), dep.column.getDefaultValue()));
            dep.column.setDefaultValue(null);
        }
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
        // DDL is transactional, so a drop whose statement or transaction rolls back never
        // happened. Without a record of what went, a rolled-back DROP TYPE left the type gone for
        // good, and a DROP TYPE whose second name reached nothing had already taken the first.
        if (isEnum) {
            executor.recordUndo(new Session.DropEnumTypeUndo(schema, bare,
                    executor.database.getCustomEnums().get(key)));
        }
        if (isComposite) {
            executor.recordUndo(new Session.DropCompositeTypeUndo(schema, bare,
                    executor.database.getCompositeTypes().get(key)));
        }
        if (isRange) {
            executor.recordUndo(new Session.DropRangeTypeUndo(schema, bare,
                    executor.database.getRangeTypes().get(key)));
        }
        if (isDomain) {
            executor.recordUndo(new Session.DropDomainUndo(schema, bare,
                    executor.database.getDomains().get(key)));
        }
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
        // The type is gone, so nobody owns it: a role that owned one it no longer has is a role
        // nothing depends on, and could not be dropped while the ownership was still recorded.
        executor.database.removeObjectOwner("type:" + key);
        executor.database.addComment("type", key, null);
    }

    /** A written type name: the schema the statement gave, when it gave one, and the name. */
    static String typeRef(String schema, String name) {
        if (name == null) return null;
        if (schema == null || TypeNamespace.writtenSchema(name) != null) return name;
        return schema.toLowerCase(java.util.Locale.ROOT) + "." + name;
    }

    /**
     * One thing declared as a type: a relation, a column of one, an attribute of a composite type,
     * or a domain written over it — with how PostgreSQL names it where a drop is refused.
     */
    private static final class TypeUser {
        final String schemaName;
        final Table table;
        /** The column declared as the type, or null when the whole relation is declared OF it. */
        final Column column;
        /** A composite type's attribute or a domain written over the type, or null. */
        final TypeDependents.Dependent type;
        final String described;

        TypeUser(String schemaName, Table table, Column column, String described) {
            this(schemaName, table, column, null, described);
        }

        TypeUser(String schemaName, Table table, Column column, TypeDependents.Dependent type,
                 String described) {
            this.schemaName = schemaName;
            this.table = table;
            this.column = column;
            this.type = type;
            this.described = described;
        }

        /** The composite type this attribute belongs to, or null when this is not one. */
        String compositeKey() {
            return type == null ? null : type.compositeKey;
        }

        /** That attribute's name, or null. */
        String attributeName() {
            return type == null ? null : type.attributeName;
        }

        /** The domain written over the type, or null when this dependent is not a domain. */
        String domainKey() {
            return type == null ? null : type.domainKey;
        }

        /** The type this one was declared as, which for an array declaration is the array type. */
        String typeShown(String display) {
            return type == null ? display : type.typeShown(display);
        }
    }

    /**
     * Everything declared as the type stored under {@code key} — a table column, a table declared
     * OF it, a composite type's attribute, a domain written over it — described the way PostgreSQL
     * describes what stands in the way of a drop: {@code column c of table t}, {@code column x of
     * composite type c}, {@code type d}, each qualified where the search path does not reach it. A
     * declaration records the type it was written with under the same key, so a column of a.e is
     * not found by dropping b.e. Each one is kept beside the thing itself, so that CASCADE takes
     * away what the refusal names rather than working the list out a second time.
     */
    private List<TypeUser> declaredAsType(String key) {
        // PostgreSQL reports dependencies in the order it recorded them: an array declaration
        // ahead of everything, because an array type is recorded as depending on its element type;
        // then relations in the order they were created, and within one relation its columns from
        // the last back to the first. Walking the schema maps reported them in whatever order
        // those maps happened to hold them, which for two tables built on the same type was
        // neither order. Each entry is kept beside the OID of what carries it -- which follows
        // creation order -- and its attnum, and the list is put in that order once it is complete.
        List<Object[]> found = new ArrayList<>();
        List<String> visible = executor.searchPathSchemas();
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table t : schema.getTables().values()) {
                int relOid = executor.systemCatalog.getOid(
                        "rel:" + schema.getName() + "." + t.getName());
                String shown = RelationNamespace.shownName(visible, schema.getName(), t.getName());
                // A typed table depends on the whole type, not on one column of it: its shape is
                // the type's, so dropping the type would leave the table with no definition at
                // all. PostgreSQL names the table itself rather than any of its columns.
                if (sameType(key, t.getOfTypeName())) {
                    found.add(new Object[]{1, relOid, 0,
                            new TypeUser(schema.getName(), t, null, "table " + shown)});
                }
                List<Column> cols = t.getColumns();
                for (int i = 0; i < cols.size(); i++) {
                    Column c = cols.get(i);
                    // A domain and a range are types like any other, and a column declared as one
                    // depends on it exactly as a column declared as an enum depends on that.
                    if (sameType(key, c.getEnumTypeName()) || sameType(key, c.getCompositeTypeName())
                            || sameType(key, c.getDomainTypeName())
                            || sameType(key, c.getRangeTypeName())) {
                        found.add(new Object[]{1, relOid, i + 1, new TypeUser(schema.getName(), t, c,
                                "column " + c.getName() + " of table " + shown)});
                    }
                }
            }
        }
        // A composite type's attribute and a domain are declared as the type exactly as a column
        // is, and stand in the way of the drop the same way, so they take their places in this one
        // list rather than in a second one of their own. Which declarations name a type is settled
        // where the routines written in terms of it are settled.
        for (TypeDependents.Dependent d : TypeDependents.typesWrittenIn(executor.database,
                executor.systemCatalog, visible, typeNamed(key))) {
            found.add(new Object[]{d.throughArray ? 0 : 1, d.age(), d.attnum(),
                    new TypeUser(d.typeSchema(), null, null, d, d.described())});
        }
        java.util.Collections.sort(found, new java.util.Comparator<Object[]>() {
            @Override
            public int compare(Object[] a, Object[] b) {
                int byArray = Integer.compare((Integer) a[0], (Integer) b[0]);
                if (byArray != 0) return byArray;
                int byRelation = Integer.compare((Integer) a[1], (Integer) b[1]);
                return byRelation != 0 ? byRelation
                        : Integer.compare((Integer) b[2], (Integer) a[2]);
            }
        });
        List<TypeUser> users = new ArrayList<>();
        for (Object[] entry : found) users.add((TypeUser) entry[3]);
        return users;
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
            // What hangs from the schema is worked out before anything is taken away, because it
            // is both what RESTRICT refuses for and what CASCADE reports having removed.
            List<SchemaDependent> hanging = schemaDependents(schema, stmt.name());
            if (!stmt.cascade() && !hanging.isEmpty()) {
                List<String> lines = new ArrayList<>();
                for (SchemaDependent d : hanging) lines.add(d.because);
                MemgresException refusal = new MemgresException("cannot drop schema " + stmt.name()
                        + " because other objects depend on it", "2BP01");
                refusal.setDetail(dependencyDetail(lines));
                refusal.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
                throw refusal;
            }
            List<Session.UndoEntry> restore = new ArrayList<>();
            Set<String> registryBack = new HashSet<>(
                    executor.database.getSchemaObjects(stmt.name().toLowerCase(java.util.Locale.ROOT)));
            if (stmt.cascade()) {
                dropOutsideSchemaDependents(stmt.name(), hanging, restore);
                // A type and a sequence the schema holds are dropped with it, and CASCADE means
                // the same for them here as it does where they are named on their own: what was
                // written in terms of the type, and every default drawing on the sequence, goes
                // too. Naming them in the notice and leaving them standing left a column declared
                // as a type nothing held.
                dropSchemaTypeAndSequenceDependents(stmt.name());
                List<String> tableNames = new ArrayList<>(schema.getTables().keySet());
                for (String tName : tableNames) {
                    Table dropped = schema.getTable(tName);
                    if (dropped != null) {
                        restore.add(new Session.DropTableUndo(stmt.name(), tName, dropped,
                                executor.database.getTriggersForTable(stmt.name(), tName),
                                executor.database.snapshotRulesGoingWith(stmt.name(), tName)));
                    }
                    executor.database.getAllTriggers().remove(tName.toLowerCase(java.util.Locale.ROOT));
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
                // gone -- and so does a rule written on a relation elsewhere whose actions name
                // one of them, which is what CASCADE was asked for.
                for (String tName : tableNames) {
                    executor.database.dropRulesGoingWith(droppedSchemaName, tName);
                }

                String schemaName = stmt.name().toLowerCase(java.util.Locale.ROOT);
                Set<String> registeredObjects = new HashSet<>(executor.database.getSchemaObjects(schemaName));
                for (String entry : registeredObjects) {
                    int colonIdx = entry.indexOf(':');
                    if (colonIdx < 0) continue;
                    String objType = entry.substring(0, colonIdx);
                    String objName = entry.substring(colonIdx + 1);
                    switch (objType) {
                        // Only this schema's type goes: the same name may be another schema's.
                        case "enum":
                            restore.add(new Session.DropEnumTypeUndo(schemaName, objName,
                                    executor.database.getCustomEnums()
                                            .get(TypeNamespace.key(schemaName, objName))));
                            executor.database.getCustomEnums()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "composite":
                            restore.add(new Session.DropCompositeTypeUndo(schemaName, objName,
                                    executor.database.getCompositeTypes()
                                            .get(TypeNamespace.key(schemaName, objName))));
                            executor.database.getCompositeTypes()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "range":
                            restore.add(new Session.DropRangeTypeUndo(schemaName, objName,
                                    executor.database.getRangeTypes()
                                            .get(TypeNamespace.key(schemaName, objName))));
                            executor.database.getRangeTypes()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "shell":
                            executor.database.getShellTypes()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "sequence": {
                            // This schema's sequence of that name, not another schema's.
                            Sequence going = executor.database.getSequence(schemaName, objName);
                            if (going != null) {
                                restore.add(new Session.DropSequenceUndo(going.qualifiedName(), going));
                            }
                            executor.database.removeSequence(schemaName, objName);
                            break;
                        }
                        case "domain":
                            restore.add(new Session.DropDomainUndo(schemaName, objName,
                                    executor.database.getDomains()
                                            .get(TypeNamespace.key(schemaName, objName))));
                            executor.database.getDomains()
                                    .remove(TypeNamespace.key(schemaName, objName));
                            break;
                        case "index":
                            executor.database.removeIndex(schemaName, objName);
                            break;
                        case "function": {
                            // Only this schema's copy goes; the same name may exist elsewhere.
                            List<PgFunction> going = new ArrayList<>(
                                    executor.database.getFunctionOverloads(schemaName, objName));
                            if (!going.isEmpty()) {
                                restore.add(new Session.DropFunctionUndo(schemaName, objName, going));
                            }
                            executor.database.removeFunction(schemaName, objName);
                            // A grant on the routine names it, and the routine is going: left
                            // behind, the grant named a role nothing could drop.
                            executor.database.removePrivilegesOnObject("FUNCTION", objName);
                            break;
                        }
                        case "view": {
                            // A view is a relation like any other: it carries rules of its own,
                            // and a rule elsewhere that writes to it or reads it cannot outlive it.
                            Database.ViewDef going = executor.database.getView(schemaName, objName);
                            if (going != null) {
                                restore.add(new Session.DropViewUndo(objName, going,
                                        executor.database.getTriggersForTable(schemaName, objName),
                                        executor.database.snapshotRulesGoingWith(schemaName, objName)));
                            }
                            executor.database.dropRulesGoingWith(schemaName, objName);
                            executor.database.removeView(schemaName, objName);
                            break;
                        }
                    }
                }
                executor.database.removeSchemaObjects(schemaName);
                // PostgreSQL says what CASCADE took with the schema, as it does for every other
                // drop -- a script that meant to remove one schema has no other way of learning
                // it removed a view in another.
                List<String> cascaded = new ArrayList<>();
                for (SchemaDependent d : hanging) cascaded.add(d.described);
                noticeDropCascades(executor, cascaded);
            }
            // A grant on the schema names it, and so does a grant on anything the schema held:
            // both go when the schema does, or the role that held them could not be dropped.
            executor.database.removePrivilegesOnObject("SCHEMA", stmt.name());
            executor.database.removePrivilegesInSchema(stmt.name());
            // A list of privileges set aside for the objects a schema will hold goes with the
            // schema: kept behind, it named a role nothing could drop and a schema nobody has.
            executor.database.removeDefaultAclsInSchema(stmt.name());
            executor.database.removeObjectOwner("schema:" + stmt.name());
            executor.database.removeSchema(stmt.name());
            executor.database.removeObjectOwner("schema:" + stmt.name());
            // A rolled-back DROP SCHEMA never happened, so the schema comes back holding
            // everything it held: PostgreSQL rolls a catalogue change back whole, and without this
            // the relations, their rows and the rules written on them stayed gone for good.
            executor.recordUndo(new Session.DropSchemaUndo(schema, registryBack, restore));
        }
    }

    /**
     * Take away what hangs from the types and the sequences a schema holds, before the schema and
     * everything in it goes. Each of them is reached through the same call the statement that
     * names it on its own makes, so a dependent meets the same end either way.
     */
    private void dropSchemaTypeAndSequenceDependents(String schemaName) {
        for (String entry : new ArrayList<>(
                executor.database.getSchemaObjects(schemaName.toLowerCase(java.util.Locale.ROOT)))) {
            int colon = entry.indexOf(':');
            if (colon < 0) continue;
            String kind = entry.substring(0, colon);
            String name = entry.substring(colon + 1);
            if ("sequence".equals(kind)) {
                Sequence seq = executor.database.getSequence(schemaName, name);
                if (seq != null) clearSequenceDefaults(seq);
                continue;
            }
            String typeKey = TypeNamespace.key(schemaName, name);
            if (isTypeKind(kind, typeKey)) dropTypeDependents(typeKey, schemaName);
        }
    }

    /** One object that goes when a schema does, as PostgreSQL names it and as it explains it. */
    private static final class SchemaDependent {
        /** How the cascade report names it: {@code table s.t}. */
        final String described;
        /** Why it is in the way: {@code table s.t depends on schema s}. */
        final String because;
        /** The schema of a view outside the dropped one, which CASCADE has to remove itself. */
        final String outsideSchema;
        /** The bare name of that view, or null when this line stands for something else. */
        final String outsideView;
        /** The relation carrying a policy outside the dropped schema, or null. */
        final Table policyTable;
        final RlsPolicy policy;

        SchemaDependent(String described, String because) {
            this(described, because, null, null, null, null);
        }

        SchemaDependent(String described, String because, String outsideSchema, String outsideView,
                        Table policyTable, RlsPolicy policy) {
            this.described = described;
            this.because = because;
            this.outsideSchema = outsideSchema;
            this.outsideView = outsideView;
            this.policyTable = policyTable;
            this.policy = policy;
        }
    }

    /** One object a schema holds, with the OID that says when it was created. */
    private static final class SchemaMember {
        final int oid;
        final String kind;
        final String name;
        /** The name as PostgreSQL writes it in a dependency line, schema-qualified where needed. */
        final String shown;

        SchemaMember(int oid, String kind, String name, String shown) {
            this.oid = oid;
            this.kind = kind;
            this.name = name;
            this.shown = shown;
        }

        String described() { return kind + " " + shown; }
    }

    /**
     * Everything that goes when a schema does, in the order PostgreSQL reports it.
     *
     * <p>A schema is what its objects hang from, so every relation, type and routine in it depends
     * on it: that is what RESTRICT refuses for and what CASCADE takes away. An index is not among
     * them, because an index hangs from the relation it is on rather than from the schema. Each of
     * them is followed at once by whatever outside the schema hangs from that one, which is the
     * order PostgreSQL walks its dependency catalogue in.
     */
    private List<SchemaDependent> schemaDependents(Schema schema, String schemaName) {
        List<String> visible = executor.searchPathSchemas();
        List<SchemaMember> members = schemaMembers(schema, schemaName, visible);
        List<SchemaDependent> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (SchemaMember m : members) {
            out.add(new SchemaDependent(m.described(),
                    m.described() + " depends on schema " + schemaName));
            appendDependentsOf(out, schemaName, m.kind, schemaName, m.name, m.shown, visible, seen);
        }
        return out;
    }

    /**
     * The objects a schema holds, in creation order. The relations are the ones the schema itself
     * carries; everything else is read out of the register that records which schema an object was
     * created in, which keeps an entry for an object that has since been dropped -- so what the
     * database still holds is what is asked, not what the register remembers.
     */
    private List<SchemaMember> schemaMembers(Schema schema, String schemaName,
                                             List<String> visible) {
        List<SchemaMember> found = new ArrayList<>();
        for (Table t : schema.getTables().values()) {
            found.add(relationMember(visible, schemaName, "table", t.getName()));
        }
        List<String> entries = new ArrayList<>(
                executor.database.getSchemaObjects(schemaName.toLowerCase(java.util.Locale.ROOT)));
        Collections.sort(entries);
        for (String entry : entries) {
            int colon = entry.indexOf(':');
            if (colon < 0) continue;
            String kind = entry.substring(0, colon);
            String name = entry.substring(colon + 1);
            String typeKey = TypeNamespace.key(schemaName, name);
            if ("view".equals(kind)) {
                Database.ViewDef v = executor.database.getView(schemaName, name);
                if (v == null) continue;
                found.add(relationMember(visible, schemaName,
                        v.materialized() ? "materialized view" : "view", v.name()));
            } else if ("sequence".equals(kind)) {
                Sequence seq = executor.database.getSequence(schemaName, name);
                if (seq == null) continue;
                found.add(relationMember(visible, schemaName, "sequence", seq.getName()));
            } else if (ForeignTables.OBJECT_TYPE.equals(kind)) {
                if (!ForeignTables.existsIn(executor.database, schemaName, name)) continue;
                found.add(relationMember(visible, schemaName, "foreign table", name));
            } else if ("function".equals(kind)) {
                // A routine is identified by its argument types, so every overload of the name is
                // an object of its own and PostgreSQL names each with the arguments it takes.
                for (PgFunction f : executor.database.getFunctionOverloads(schemaName, name)) {
                    // Each overload owns a pg_proc row of its own. Reading the bare name gave every
                    // overload of it the first one's number, so they all sorted to one place.
                    found.add(new SchemaMember(executor.systemCatalog.getOid(
                            CatalogCoreBuilder.routineOidKey(executor.database, f)),
                            f.isProcedure() ? "procedure" : "function", name,
                            RelationNamespace.shownName(visible, schemaName, name)
                                    + "(" + routineArgumentTypes(f) + ")"));
                }
            } else if (isTypeKind(kind, typeKey)) {
                // Enums, composites, ranges, shells and domains are all types, and PostgreSQL
                // calls each of them a type when it says what depends on what.
                found.add(new SchemaMember(executor.systemCatalog.getOid("type:" + typeKey),
                        "type", name, RelationNamespace.shownName(visible, schemaName, name)));
            }
        }
        Collections.sort(found, new java.util.Comparator<SchemaMember>() {
            @Override
            public int compare(SchemaMember a, SchemaMember b) {
                return Integer.compare(a.oid, b.oid);
            }
        });
        return found;
    }

    /** Whether the register's entry still names a type this database holds. */
    private boolean isTypeKind(String kind, String typeKey) {
        if ("enum".equals(kind)) return executor.database.getCustomEnums().containsKey(typeKey);
        if ("composite".equals(kind)) return executor.database.getCompositeTypes().containsKey(typeKey);
        if ("range".equals(kind)) return executor.database.getRangeTypes().containsKey(typeKey);
        if ("shell".equals(kind)) return executor.database.getShellTypes().contains(typeKey);
        if ("domain".equals(kind)) return executor.database.getDomains().containsKey(typeKey);
        return false;
    }

    private SchemaMember relationMember(List<String> visible, String schemaName, String kind,
                                        String name) {
        return new SchemaMember(executor.systemCatalog.getOid("rel:" + schemaName + "." + name),
                kind, name, RelationNamespace.shownName(visible, schemaName, name));
    }

    /**
     * A routine's argument types as PostgreSQL writes them when it names the routine. An output
     * parameter is not part of what identifies the routine, so it is not written here either.
     */
    private static String routineArgumentTypes(PgFunction f) {
        StringBuilder sb = new StringBuilder();
        if (f.getParams() == null) return "";
        for (PgFunction.Param p : f.getParams()) {
            if (p.mode() != null && ("OUT".equalsIgnoreCase(p.mode())
                    || "TABLE".equalsIgnoreCase(p.mode()))) {
                continue;
            }
            if (sb.length() > 0) sb.append(", ");
            sb.append(DataType.canonicalName(p.typeName()));
        }
        return sb.toString();
    }

    /**
     * What outside a schema hangs from one object inside it: a view that reads the relation or
     * calls the routine, whatever reads that view in turn, a row security policy whose expressions
     * name either, a column declared as the type, and a column default drawing on the sequence.
     *
     * <p>A view or a policy inside the schema is left out: it hangs from the schema itself and is
     * already reported there, which is where PostgreSQL reports it.
     */
    private void appendDependentsOf(List<SchemaDependent> out, String schemaName, String ownerKind,
                                    String ownerSchema, String ownerName, String ownerShown,
                                    List<String> visible, Set<String> seen) {
        String on = " depends on " + ownerKind + " " + ownerShown;
        // A view and a policy hanging from one object are two rows of the same dependency
        // catalogue, and PostgreSQL walks it in OID order: which of them is reported first is
        // which of them was created first, not which kind of thing it is. Every view followed by
        // every policy agreed with PostgreSQL only where the views happened to be the older.
        List<Object[]> direct = new ArrayList<>();
        if (!"type".equals(ownerKind) && !"function".equals(ownerKind)
                && !"procedure".equals(ownerKind)) {
            for (Database.ViewDef v : ViewDependencies.directDependentViews(
                    executor.database, executor.systemCatalog, ownerSchema, ownerName)) {
                String vs = v.schemaName() != null ? v.schemaName() : "public";
                if (vs.equalsIgnoreCase(schemaName)) continue;
                direct.add(new Object[]{vs, v});
            }
        }
        if ("function".equals(ownerKind) || "procedure".equals(ownerKind)) {
            direct.addAll(viewsCalling(schemaName, ownerName));
        }
        direct.addAll(policiesDependingOn(schemaName, ownerKind, ownerSchema, ownerName));
        Collections.sort(direct, new java.util.Comparator<Object[]>() {
            @Override
            public int compare(Object[] a, Object[] b) {
                return Integer.compare(oidOfDependent(a), oidOfDependent(b));
            }
        });
        for (Object[] entry : direct) {
            if (entry.length == 2) {
                String vs = (String) entry[0];
                Database.ViewDef v = (Database.ViewDef) entry[1];
                if (!seen.add("view:" + vs.toLowerCase(java.util.Locale.ROOT) + "." + v.name().toLowerCase(java.util.Locale.ROOT))) continue;
                String kind = v.materialized() ? "materialized view" : "view";
                String shown = RelationNamespace.shownName(visible, vs, v.name());
                out.add(new SchemaDependent(kind + " " + shown, kind + " " + shown + on,
                        vs, v.name(), null, null));
                appendDependentsOf(out, schemaName, kind, vs, v.name(), shown, visible, seen);
                continue;
            }
            String ts = (String) entry[0];
            Table t = (Table) entry[1];
            RlsPolicy p = (RlsPolicy) entry[2];
            if (!seen.add("policy:" + ts.toLowerCase(java.util.Locale.ROOT) + "." + t.getName().toLowerCase(java.util.Locale.ROOT)
                    + ":" + p.getName().toLowerCase(java.util.Locale.ROOT))) {
                continue;
            }
            String shown = "policy " + p.getName() + " on table "
                    + RelationNamespace.shownName(visible, ts, t.getName());
            out.add(new SchemaDependent(shown, shown + on, null, null, t, p));
        }
        if ("type".equals(ownerKind)) {
            String typeKey = TypeNamespace.key(ownerSchema, ownerName);
            for (TypeUser user : declaredAsType(typeKey)) {
                // A column of a relation the schema is taking with it is covered by the relation's
                // own line, which is the one PostgreSQL reports: a whole relation on the way out
                // stands for every column of it.
                if (schemaName.equalsIgnoreCase(user.schemaName)) continue;
                if (!seen.add("column:" + user.described.toLowerCase(java.util.Locale.ROOT))) continue;
                out.add(new SchemaDependent(user.described, user.described + on));
            }
            // A routine written in terms of the type cannot outlive it either. One inside the
            // schema hangs from the schema itself and is already reported there, which is where
            // PostgreSQL reports it.
            for (TypeDependents.Dependent d : TypeDependents.writtenIn(executor.database,
                    executor.systemCatalog, visible, typeNamed(typeKey))) {
                if (routineSchemaOf(d).equalsIgnoreCase(schemaName)) continue;
                if (!seen.add("routine:" + d.described().toLowerCase(java.util.Locale.ROOT))) continue;
                out.add(new SchemaDependent(d.described(), d.described() + " depends on "
                        + ownerKind + " " + d.typeShown(ownerShown)));
            }
        }
        if ("sequence".equals(ownerKind)) {
            Sequence seq = executor.database.getSequence(ownerSchema, ownerName);
            if (seq == null) return;
            for (SequenceDependent dep : findSequenceDependents(seq)) {
                String shown = "default value for column " + dep.columnName() + " of table "
                        + dep.tableRef(visible);
                if (!seen.add("default:" + shown.toLowerCase(java.util.Locale.ROOT))) continue;
                out.add(new SchemaDependent(shown, shown + on));
            }
        }
    }

    /**
     * The stored views whose query calls a routine of this name in this schema, each beside the
     * schema it lives in. PostgreSQL records a view's dependency on every routine its query calls,
     * so a view outside the schema cannot outlive a routine inside it.
     */
    private List<Object[]> viewsCalling(String schemaName, String routineName) {
        List<Object[]> found = new ArrayList<>();
        for (Database.ViewDef v : executor.database.getViews().values()) {
            String vs = v.schemaName() != null ? v.schemaName() : "public";
            if (vs.equalsIgnoreCase(schemaName)) continue;
            if (!calls(v.query(), vs, schemaName, routineName)) continue;
            found.add(new Object[]{vs, v});
        }
        Collections.sort(found, new java.util.Comparator<Object[]>() {
            @Override
            public int compare(Object[] a, Object[] b) {
                return Integer.compare(oidOfView(a), oidOfView(b));
            }
        });
        return found;
    }

    private int oidOfView(Object[] entry) {
        return executor.systemCatalog.getOid("rel:" + entry[0] + "."
                + ((Database.ViewDef) entry[1]).name());
    }

    /**
     * The number that says when a dependent was created: a view's own relation, or the policy's
     * row. Both are drawn from the one sequence PostgreSQL numbers every catalogue row from, so
     * the two kinds can be put in one order.
     */
    private int oidOfDependent(Object[] entry) {
        if (entry.length == 2) return oidOfView(entry);
        return executor.systemCatalog.getOid(ObjectIdentity.policyKey((String) entry[0],
                ((Table) entry[1]).getName(), ((RlsPolicy) entry[2]).getName()));
    }

    /** Whether a parsed tree calls the routine {@code schemaName.routineName}. */
    private static boolean calls(Object tree, String home, String schemaName, String routineName) {
        if (tree == null || routineName == null) return false;
        final String wanted = routineName.toLowerCase(java.util.Locale.ROOT);
        final String schema = schemaName == null ? "public" : schemaName.toLowerCase(java.util.Locale.ROOT);
        final String where = home == null ? "public" : home.toLowerCase(java.util.Locale.ROOT);
        final boolean[] found = new boolean[1];
        AstWalk.forEach(tree, node -> {
            if (found[0] || !(node instanceof FunctionCallExpr)) return;
            String written = ((FunctionCallExpr) node).name();
            if (written == null) return;
            String bare = RelationNamespace.bareName(written).toLowerCase(java.util.Locale.ROOT);
            if (!bare.equals(wanted)) return;
            int dot = written.lastIndexOf('.');
            if (dot > 0) {
                if (written.substring(0, dot).equalsIgnoreCase(schema)) found[0] = true;
                return;
            }
            // Written bare, the name resolved through the search path when the definition was
            // stored, which reaches public and the schema the definition itself lives in.
            if (schema.equals("public") || schema.equals(where)) found[0] = true;
        });
        return found[0];
    }

    /**
     * The row security policies outside the schema whose USING or WITH CHECK expression names this
     * object. PostgreSQL records what a policy's expressions read, so a policy on a relation
     * elsewhere goes when the relation or the routine it reads does.
     */
    private List<Object[]> policiesDependingOn(String schemaName, String ownerKind,
                                               String ownerSchema, String ownerName) {
        List<Object[]> found = new ArrayList<>();
        boolean routine = "function".equals(ownerKind) || "procedure".equals(ownerKind);
        if (!routine && "type".equals(ownerKind)) return found;
        for (Map.Entry<String, Schema> se : executor.database.getSchemas().entrySet()) {
            if (se.getKey().equalsIgnoreCase(schemaName)) continue;
            for (Table t : se.getValue().getTables().values()) {
                for (RlsPolicy p : t.getRlsPolicies()) {
                    boolean hit = routine
                            ? calls(p.getUsingExpr(), se.getKey(), ownerSchema, ownerName)
                                    || calls(p.getWithCheckExpr(), se.getKey(), ownerSchema, ownerName)
                            : ViewDependencies.reads(p.getUsingExpr(), se.getKey(), ownerSchema, ownerName)
                                    || ViewDependencies.reads(p.getWithCheckExpr(), se.getKey(),
                                            ownerSchema, ownerName);
                    if (hit) found.add(new Object[]{se.getKey(), t, p});
                }
            }
        }
        return found;
    }

    /**
     * Take away the views and policies outside the schema that hang from something inside it, so
     * that CASCADE means what PostgreSQL means by it. A view left behind read a relation that was
     * no longer there and a policy left behind silenced every row of the relation it was on.
     */
    private void dropOutsideSchemaDependents(String schemaName, List<SchemaDependent> hanging,
                                             List<Session.UndoEntry> restore) {
        for (SchemaDependent d : hanging) {
            if (d.policy != null && d.policyTable != null) {
                restore.add(new Session.DropPolicyUndo(d.policyTable, d.policy));
                d.policyTable.getRlsPolicies().remove(d.policy);
            }
            if (d.outsideView == null) continue;
            Database.ViewDef going = executor.database.getView(d.outsideSchema, d.outsideView);
            if (going == null) continue;
            restore.add(new Session.DropViewUndo(d.outsideView, going,
                    executor.database.getTriggersForTable(d.outsideSchema, d.outsideView),
                    executor.database.snapshotRulesGoingWith(d.outsideSchema, d.outsideView)));
            executor.database.dropRulesGoingWith(d.outsideSchema, d.outsideView);
            executor.database.removeView(d.outsideSchema, d.outsideView);
            executor.database.removeObjectOwner("view:" + d.outsideSchema + "." + d.outsideView);
        }
    }

    private void dropPolicy(DropStmt stmt) {
        if (stmt.onTable() != null) {
            // A policy is named by its relation, so IF EXISTS skips on a relation that is not
            // there just as it does on a policy that is not.
            // A written qualifier says which schema holds the relation the policy is on; a policy
            // on another schema's relation of that name is a different policy. PostgreSQL names
            // the relation without its schema when it reports the policy missing.
            String written = stmt.onTable();
            boolean qualified = written.indexOf('.') >= 0;
            String onSchema = qualified ? written.substring(0, written.indexOf('.')) : "public";
            String onTable = RelationNamespace.bareName(written);
            if (stmt.ifExists()
                    && (qualified
                            ? RelationNamespace.kindOf(executor.database, onSchema, onTable) == null
                            : ddl.resolveTableOrNull(onTable) == null)) {
                noticeSkipped("relation \"" + written + "\"");
                return;
            }
            // A qualifier naming no schema at all is a missing schema rather than a missing
            // relation, which is what PostgreSQL reports for it.
            if (qualified) {
                SchemaQualifier.requireSchema(executor.database, executor.session, onSchema);
            }
            Table table = executor.resolveTable(onSchema, onTable, qualified);
            // Row security is what protects the relation from the role reading it, so taking a
            // policy away is the owner's to do. PostgreSQL says relation here, not table.
            executor.requireRelationOwner(
                    qualified ? onSchema : executor.defaultSchema(), onTable);
            boolean found = false;
            for (RlsPolicy p : table.getRlsPolicies()) {
                if (p.getName().equalsIgnoreCase(stmt.name())) { found = true; break; }
            }
            if (!found) {
                if (!stmt.ifExists()) {
                    throw new MemgresException("policy \"" + stmt.name() + "\" for table \"" + onTable + "\" does not exist", "42704");
                }
                noticeSkipped("policy \"" + stmt.name() + "\" for table \"" + onTable + "\"");
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
        DdlTableExecutor.applyDefaultPrivileges(executor, seqSchema, seqName,
                executor.currentRole(), "SEQUENCES", "SEQUENCE");
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
        String bare = RelationNamespace.bareName(tblName);
        int dot = tblName.lastIndexOf('.');
        String written = dot > 0 ? tblName.substring(0, dot) : null;
        String tableSchema = executor.relationSchemaOf(written, bare);
        Schema held = tableSchema == null ? null : executor.database.getSchema(tableSchema);
        Table tbl = held == null ? null : held.getTable(bare);
        if (tbl == null) throw new MemgresException("relation \"" + tblName + "\" does not exist", "42P01");
        // A sequence a column owns is dropped with the column, so it has to be somewhere the
        // column's own schema can take it with: PostgreSQL refuses to link one across schemas.
        // Linked anyway, a sequence in public was answered for by pg_get_serial_sequence for a
        // column of a table in another schema, and outlived the table it belonged to.
        if (!tableSchema.equalsIgnoreCase(seq.getSchemaName())) {
            throw new MemgresException(
                    "sequence must be in same schema as table it is linked to", "55000");
        }
        if (colName != null && tbl.getColumnIndex(colName) < 0) {
            throw new MemgresException(
                    "column \"" + colName + "\" of relation \"" + tblName + "\" does not exist", "42703");
        }
        seq.setOwnedByTable(bare);
        seq.setOwnedByColumn(colName);
    }

    // ---- CREATE DOMAIN ----

    QueryResult executeCreateDomain(CreateDomainStmt stmt) {
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schemaName());
        String domainSchema = stmt.schemaName() != null
                ? stmt.schemaName().toLowerCase(java.util.Locale.ROOT) : executor.creationSchema();
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
        // A domain's default is evaluated with nothing in scope, exactly as a column's is, so the
        // same expressions are refused in both.
        DdlDefinitionChecks.validateDefaultExpression(stmt.defaultExpr());
        // Keep the base type's modifier: information_schema.domains describes a domain the way
        // it describes a column, so varchar(12) has to know it is twelve characters wide.
        int[] typmod = parseTypmod(stmt.baseType());
        domain.setTypmod(typmod[0] < 0 ? null : Integer.valueOf(typmod[0]),
                typmod[1] < 0 ? null : Integer.valueOf(typmod[1]));
        domain.setBaseTypeFacts(DataType.intervalQualifier(stmt.baseType()), elementType);
        // The default is read with the base type's input function here, not at the first insert.
        domain.setDefaultValue(requireDefaultReadableAsDomain(domain, domain.getDefaultValue()));
        domain.setSchemaName(domainSchema);
        executor.identity().typeCreated("d", TypeNamespace.key(domainSchema, stmt.name()));
        executor.database.addDomain(domain);
        executor.database.registerSchemaObject(domainSchema, "domain", stmt.name());
        executor.recordUndo(new Session.CreateDomainUndo(domainSchema, stmt.name()));
        executor.database.setObjectOwner("type:" + TypeNamespace.key(domainSchema, stmt.name()),
                executor.currentRole());
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
                String to = stmt.newConstraintName().toLowerCase(java.util.Locale.ROOT);
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
     * Follow a renamed or moved enum, composite or range from every column declared with it. A
     * column of a type keeps reading that type through the rename, which is what PostgreSQL's OIDs
     * give it for nothing and memgres, which records names, has to do here.
     */
    private void retargetTypeColumns(String oldKey, String newKey) {
        for (Schema s : executor.database.getSchemas().values()) {
            for (Table t : s.getTables().values()) {
                for (Column c : t.getColumns()) {
                    if (oldKey.equalsIgnoreCase(c.getEnumTypeName())) c.setEnumTypeName(newKey);
                    if (oldKey.equalsIgnoreCase(c.getCompositeTypeName())) {
                        c.setCompositeTypeName(newKey);
                    }
                    if (oldKey.equalsIgnoreCase(c.getRangeTypeName())) {
                        c.setRangeTypeName(newKey);
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
        rejectIndexOverSystemColumns(s);
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
     * An index reads what a row is, and a system column says instead where the row is and who
     * wrote it -- values the index itself would move. PostgreSQL builds none over them, wherever
     * in the statement one is written: a key, an INCLUDE payload, an expression or a predicate.
     *
     * <p>A key written as a bare column name is refused by {@code rejectSystemKeyColumn}, which
     * has PostgreSQL's other wording for the two of them no index could be built over anyway.
     */
    private void rejectIndexOverSystemColumns(CreateIndexStmt s) {
        if (s.columns() != null) {
            for (String col : s.columns()) {
                if (col == null) continue;
                DdlDefinitionChecks.rejectSystemKeyColumn(col.trim());
                if (namesASystemColumn(col)) throw DdlDefinitionChecks.indexOnSystemColumn();
            }
        }
        if (s.includeColumns() != null) {
            for (String col : s.includeColumns()) {
                if (col != null) DdlDefinitionChecks.rejectSystemKeyColumn(col.trim());
            }
        }
        if (s.whereClause() != null && namesASystemColumn(s.whereClause())) {
            throw DdlDefinitionChecks.indexOnSystemColumn();
        }
    }

    /** Whether a written index key or predicate reads a system column anywhere inside it. */
    private static boolean namesASystemColumn(String written) {
        Expression expr;
        try {
            expr = com.memgres.engine.parser.Parser.parseExpression(written);
        } catch (RuntimeException ignored) {
            return false;
        }
        return AstWalk.findFirst(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof ColumnRef
                        && DdlDefinitionChecks.isSystemColumnName(((ColumnRef) n).column());
            }
        }) != null;
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
     * Resolve the names and the calls a partial index's predicate is written over, against the
     * relation the index is being built on. PostgreSQL settles both while it reads the statement,
     * so a qualifier naming another relation and a call naming no function are reported as
     * themselves rather than as whatever the trial row below happens to make of them -- which is
     * what named an unknown function by an argument type the column never had.
     */
    private void readPredicateNames(Table table, String whereClause) {
        if (table == null) return;
        Expression pred;
        try {
            pred = com.memgres.engine.parser.Parser.parseExpression(whereClause);
        } catch (RuntimeException ignored) {
            return; // a predicate this cannot read is reported by the checks that follow
        }
        StoredExprNames.read(ddl, pred, table, null, false, true);
    }

    /**
     * The same for an index key that is an expression rather than a bare column name.
     *
     * <p>A key that reads back as one plain name is left alone, and has to be: a key is recorded
     * as the text it was written as with the quotes taken off, so a column called {@code "A b"}
     * arrives here looking like an expression and reads as the column {@code A} alone.
     */
    private void readKeyNames(Table table, String written) {
        if (table == null) return;
        Expression key;
        try {
            key = com.memgres.engine.parser.Parser.parseExpression(written);
        } catch (RuntimeException ignored) {
            return; // a key this cannot read is reported by the checks that follow
        }
        if (key instanceof ColumnRef) return;
        StoredExprNames.read(ddl, key, table, null, false, true);
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
        // The tablespace is judged once the relation is open, which is the order PostgreSQL
        // judges them in. memgres keeps everything in memory and so has only the two tablespaces
        // PostgreSQL always ships; a name outside them names nowhere to put the index.
        if (s.tablespace() != null && !"pg_default".equalsIgnoreCase(s.tablespace())
                && !"pg_global".equalsIgnoreCase(s.tablespace())) {
            throw new MemgresException(
                    "tablespace \"" + s.tablespace() + "\" does not exist", "42704");
        }
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
                    s.columnOptions(), s.nullsNotDistinct(), s.withOptions())
                    .withTablespace(s.tablespace());
        }
        if (indexTarget != null) {
            DdlIndexValidator.validate(executor.database, indexTarget, s.method(), s.unique(),
                    s.columns(), s.columnOptions(), s.includeColumns(), s.withOptions());
        }
        if (s.whereClause() != null) {
            // Every name and every call in the predicate is settled where the index is written,
            // and settled before the predicate is asked to be a condition: a call naming no
            // function at all is reported as that, not by whatever a trial row makes of it.
            readPredicateNames(indexTarget, s.whereClause());
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
                        // A key is stored against this relation and no other, so a qualifier on a
                        // name is a relation the statement never mentioned and a call in it names a
                        // function that has to exist -- both settled where the index is written
                        // rather than by whatever the trial row below happens to make of them,
                        // which named an unknown function by an argument type it never had.
                        readKeyNames(idxTable, exprStr);
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
                                String funcName = stripped.substring(0, parenIdx).trim().toLowerCase(java.util.Locale.ROOT);
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
                        // A predicate is stored against this relation and no other, so a qualifier
                        // on a name is a relation the statement never mentioned and a call in it
                        // names a function that has to exist -- both settled where the index is
                        // written, as they are wherever else PostgreSQL stores an expression.
                        StoredExprNames.read(ddl, predExpr, idxTable, null, false, true);
                        // Walk the expression to find column references
                        ddl.validateExprColumnRefs(predExpr, idxTable, null);
                    } catch (MemgresException me) {
                        if ("42703".equals(me.getSqlState()) || "42P01".equals(me.getSqlState())
                                || "42883".equals(me.getSqlState())) throw me;
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
                    String partCol = partTable.getPartitionColumn().toLowerCase(java.util.Locale.ROOT);
                    if (partCol.startsWith("(")) partCol = partCol.substring(1);
                    if (partCol.endsWith(")")) partCol = partCol.substring(0, partCol.length() - 1);
                    partCol = partCol.trim();
                    boolean partColFound = false;
                    for (String idxCol : s.columns()) {
                        if (idxCol.toLowerCase(java.util.Locale.ROOT).equals(partCol)) { partColFound = true; break; }
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
                    Map<String, Object[]> keysFirstSeen = new HashMap<>();
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
                                    appendKeyPart(keyBuilder,
                                            val == null ? "\0NULL\0" : val.toString());
                                } catch (Exception e) {
                                    keyValues.add(null);
                                    appendKeyPart(keyBuilder, "\0ERR\0");
                                }
                            }
                        } else {
                            for (String col : s.columns()) {
                                int ci = valTable.getColumnIndex(col);
                                if (ci >= 0) {
                                    Object val = evalRow[ci];
                                    keyValues.add(val);
                                    // The key is the value, and two values are the same key when
                                    // the type says they are equal: writing them out and
                                    // comparing the text made numeric 1.0 and 1.00 two keys, so
                                    // a unique index was built over rows that violate it.
                                    appendKeyPart(keyBuilder, val == null ? "\0NULL\0"
                                            : TypeCoercion.keyText(val));
                                }
                            }
                        }
                        String key = keyBuilder.toString();
                        // The complaint names the key as the row that first held it wrote it,
                        // which is the row a reader will find when they go looking.
                        Object[] firstSeen = keysFirstSeen.get(key);
                        if (firstSeen == null) keysFirstSeen.put(key, keyValues.toArray());
                        else keyValues = new java.util.ArrayList<Object>(
                                java.util.Arrays.asList(firstSeen));
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
                            // The relation the index would be over is part of the report, so a
                            // client reading the fields learns where to look.
                            dup.setSchema(executor.defaultSchema());
                            dup.setTable(valTable.getName());
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
                            TableIndex tableIdx = new TableIndex(s.name(), colIndices, s.unique(),
                                    idxTable2.getColumns());
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
                        // The copy belongs to the relation it indexes, so it is registered in that
                        // relation's own schema -- which is not the partitioned table's when the
                        // partition was created somewhere else.
                        ddl.tableExecutor.copyParentIndex(partitionedParent, childPartition,
                                childPartition.getSchemaName(),
                                Database.idxKey(partitionedSchema, s.name()));
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
            // The collation copied from has to be one, or there is nothing to copy.
            DdlDefinitionChecks.requireCollationExists(ddl.executor.database, stmt.fromCollation);
            ddl.executor.database.addCollation(new Database.CollationDef(
                    name, "c", null, null, null, true, stmt.fromCollation,
                    collationSchema(stmt)));
            return QueryResult.message(QueryResult.Type.SET, "CREATE COLLATION");
        }
        // Everything the definition names has to be something, and the two locale halves come
        // as a pair. None of it was checked, so a collation over a locale nobody has, from a
        // provider nobody implements, written with an attribute that does not exist, was
        // recorded and reported as created.
        checkCollationOptions(stmt.options);

        for (java.util.Map.Entry<String, String> entry : stmt.options.entrySet()) {
            switch (entry.getKey()) {
                case "provider":
                    String pv = entry.getValue().toLowerCase(java.util.Locale.ROOT);
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

        // LOCALE sets both halves, which is what pg_collation records — they were left null, so
        // a reader of collcollate and collctype saw nothing for a collation that has both.
        if (lcCollate == null) lcCollate = locale;
        if (lcCtype == null) lcCtype = locale;
        ddl.executor.database.addCollation(new Database.CollationDef(
                name, provider, locale, lcCollate, lcCtype, deterministic, null,
                collationSchema(stmt)));
        return QueryResult.message(QueryResult.Type.SET, "CREATE COLLATION");
    }

    /** The schema a collation is made in: the one it named, else where the session creates. */
    private String collationSchema(CreateCollationStmt stmt) {
        if (stmt.schemaName == null) return ddl.executor.creationSchema();
        ddl.executor.ddlExecutor.requireSchemaExists(stmt.schemaName);
        return stmt.schemaName;
    }

    /** The locales this server can collate under. Anything else has no locale to load. */
    /** The locale names that stand for no locale at all, and so always exist. */
    private static final java.util.Set<String> LOCALE_INDEPENDENT_NAMES =
            new java.util.HashSet<String>(java.util.Arrays.asList(
                    "c", "posix", "und", "c.utf-8", "c.utf8", "ucs_basic", "default"));

    /**
     * Whether the host could have this locale, judged by the language and country it names.
     *
     * <p>Which locales are actually installed is a property of the machine, so an allow-list of
     * names would say different things on different hosts, and memgres runs wherever the JVM
     * does. What can be decided anywhere is whether the name denotes a language and a country at
     * all: {@code fr_FR.utf8} does and {@code zz_ZZ.nosuchlocale} does not, because {@code zz} is
     * no language.
     */
    private static boolean namesAPossibleLocale(String value) {
        String name = value.trim().toLowerCase(java.util.Locale.ROOT);
        if (name.isEmpty()) return false;
        if (LOCALE_INDEPENDENT_NAMES.contains(name)) return true;
        int at = name.indexOf('@');
        if (at >= 0) name = name.substring(0, at);
        int dot = name.indexOf('.');
        if (dot >= 0) name = name.substring(0, dot);
        String[] parts = name.split("[_-]");
        if (!java.util.Arrays.asList(java.util.Locale.getISOLanguages()).contains(parts[0])) {
            return false;
        }
        if (parts.length > 1 && !parts[1].isEmpty()
                && !java.util.Arrays.asList(java.util.Locale.getISOCountries())
                        .contains(parts[1].toUpperCase(java.util.Locale.ROOT))) {
            return false;
        }
        return parts.length <= 2;
    }

    /**
     * Refuse a collation definition that names something the server has not got.
     *
     * <p>PostgreSQL checks these in a fixed order: an attribute it does not know first, then the
     * provider, then that the two locale halves are both settled, then the locale itself, and
     * last whether the provider can do a nondeterministic collation.
     */
    private void checkCollationOptions(java.util.Map<String, String> options) {
        for (String written : options.keySet()) {
            if (!java.util.Arrays.asList("provider", "locale", "lc_collate", "lc_ctype",
                    "deterministic", "rules", "version").contains(written)) {
                throw new MemgresException(
                        "collation attribute \"" + written + "\" not recognized", "42601");
            }
        }
        String provider = options.get("provider");
        if (provider != null) {
            String p = provider.toLowerCase(java.util.Locale.ROOT);
            if (!p.equals("libc") && !p.equals("icu") && !p.equals("builtin")) {
                throw new MemgresException(
                        "unrecognized collation provider: " + provider, "42P17");
            }
        }
        // LOCALE settles both halves; otherwise each has to be given.
        if (options.get("locale") == null) {
            if (options.get("lc_collate") == null) {
                throw new MemgresException("parameter \"lc_collate\" must be specified", "42P17");
            }
            if (options.get("lc_ctype") == null) {
                throw new MemgresException("parameter \"lc_ctype\" must be specified", "42P17");
            }
        }
        // ICU takes any tag and falls back to its root collation, so it rejects nothing.
        boolean icu = "icu".equalsIgnoreCase(provider == null ? "" : provider);
        for (String key : new String[]{"locale", "lc_collate", "lc_ctype"}) {
            String value = options.get(key);
            if (value == null || icu) continue;
            if (!namesAPossibleLocale(value)) {
                throw new MemgresException("could not create locale \"" + value
                        + "\": No such file or directory", "22023");
            }
        }
        // Only ICU can compare two different strings as equal, and memgres provides libc.
        if ("false".equalsIgnoreCase(options.get("deterministic"))
                && !"icu".equalsIgnoreCase(provider == null ? "" : provider)) {
            throw new MemgresException(
                    "nondeterministic collations not supported with this provider", "0A000");
        }
    }

    // ---- CREATE CAST ----

    /**
     * Add one column's contribution to a key built as text.
     *
     * <p>Each part carries its own length, so where one part ends and the next begins never
     * depends on what is in them: joined by a separator alone, two rows whose values differ only
     * in where that character falls made the same key, and a unique index over them could not be
     * built.
     */
    private static void appendKeyPart(StringBuilder key, String part) {
        key.append(part.length()).append(':').append(part);
    }

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
        // A cast the server already provides is one that already exists, whether somebody wrote
        // it or PostgreSQL ships it.
        boolean alreadyThere = executor.database.getUserDefinedCasts().stream()
                .anyMatch(c -> (int) c[0] == sourceOid && (int) c[1] == targetOid);
        if (!alreadyThere) {
            for (Object[] shipped : PgCastTable.CASTS) {
                if ((Integer) shipped[0] == sourceOid && (Integer) shipped[1] == targetOid) {
                    alreadyThere = true;
                    break;
                }
            }
        }
        if (alreadyThere) {
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
            // A built-in whose signatures are recorded is held to them; one whose signatures are
            // not is accepted on its name, which is as much as is known about it.
            List<String> written = stmt.funcArgTypes == null
                    ? java.util.Collections.<String>emptyList() : stmt.funcArgTypes;
            if (builtinSignatureMatches(bare, written)) return;
            if (!BuiltinCallTypes.records(bare) && isKnownBuiltinFunction(stmt.functionName)) return;
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
        String src = stmt.sourceType.toLowerCase(java.util.Locale.ROOT);
        String tgt = stmt.targetType.toLowerCase(java.util.Locale.ROOT);
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
        String base = typeName.toLowerCase(java.util.Locale.ROOT).replaceAll("\\(.*\\)", "").trim();
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
            String version = BUILTIN_EXTENSIONS.get(name.toLowerCase(java.util.Locale.ROOT));
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
        String lower = name.toLowerCase(java.util.Locale.ROOT);
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
        } else if (kind.equals("table") || kind.equals("view") || kind.equals("materialized view")
                || kind.equals("index") || kind.equals("sequence")) {
            // A relation is reported missing the way every other statement reports one, so
            // ALTER EXTENSION ... ADD TABLE names a relation that has to be there.
            try {
                executor.resolveTable(null, name);
                return;
            } catch (MemgresException notARelation) {
                if (executor.database.getSequence(name) != null) return;
                if (executor.database.getView(name) != null) return;
                if (executor.database.hasIndex(name.toLowerCase(java.util.Locale.ROOT))) return;
                throw new MemgresException(
                        "relation \"" + name + "\" does not exist", "42P01");
            }
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
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if (kind.equals("text search configuration")) {
            Database.TsConfigDef cfg = db.getTsConfigs().get(lower);
            if (cfg == null) return;
            if (!lower.equals(newName.toLowerCase(java.util.Locale.ROOT))
                    && (BUILTIN_TS_CONFIGS.contains(newName.toLowerCase(java.util.Locale.ROOT))
                        || db.getTsConfigs().containsKey(newName.toLowerCase(java.util.Locale.ROOT)))) {
                throw new MemgresException("text search configuration \"" + newName
                        + "\" already exists in schema \"" + executor.defaultSchema() + "\"", "42710");
            }
            db.removeTsConfig(name);
            db.addTsConfig(new Database.TsConfigDef(newName, cfg.parserName, cfg.copyFrom));
        } else if (kind.equals("text search dictionary")) {
            Database.TsDictDef dict = db.getTsDicts().get(lower);
            if (dict == null) return;
            if (!lower.equals(newName.toLowerCase(java.util.Locale.ROOT))
                    && (BUILTIN_TS_DICTS.contains(newName.toLowerCase(java.util.Locale.ROOT))
                        || db.getTsDicts().containsKey(newName.toLowerCase(java.util.Locale.ROOT)))) {
                throw new MemgresException("text search dictionary \"" + newName
                        + "\" already exists in schema \"" + executor.defaultSchema() + "\"", "42710");
            }
            db.removeTsDict(name);
            db.addTsDict(new Database.TsDictDef(newName, dict.template, dict.options));
        } else if (kind.equals("server")) {
            Database.FdwServer srv = db.getForeignServer(name);
            if (srv == null) return;
            if (db.getForeignServer(newName) != null && !lower.equals(newName.toLowerCase(java.util.Locale.ROOT))) {
                throw new MemgresException("server \"" + newName + "\" already exists", "42710");
            }
            db.removeForeignServer(name);
            db.addForeignServer(new Database.FdwServer(newName, srv.fdwName, srv.options));
        } else if (kind.equals("foreign-data wrapper")) {
            Database.FdwWrapper fdw = db.getForeignDataWrappers().get(lower);
            if (fdw == null) return;
            if (db.getForeignDataWrappers().containsKey(newName.toLowerCase(java.util.Locale.ROOT))
                    && !lower.equals(newName.toLowerCase(java.util.Locale.ROOT))) {
                throw new MemgresException(
                        "foreign-data wrapper \"" + newName + "\" already exists", "42710");
            }
            db.removeForeignDataWrapper(name);
            db.addForeignDataWrapper(new Database.FdwWrapper(newName, fdw.options));
        } else if (kind.equals("subscription")) {
            Database.SubDef sub = db.getSubscriptions().get(lower);
            if (sub == null) return;
            if (db.getSubscriptions().containsKey(newName.toLowerCase(java.util.Locale.ROOT))
                    && !lower.equals(newName.toLowerCase(java.util.Locale.ROOT))) {
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
