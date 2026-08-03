package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles CREATE TABLE, DROP TABLE, TRUNCATE, CREATE TABLE AS.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlTableExecutor {
    /** What INCLUDING ALL stands for, so that a later EXCLUDING can take one of them back. */
    private static final Set<String> LIKE_OPTION_NAMES = Cols.setOf(
            "COMMENTS", "COMPRESSION", "CONSTRAINTS", "DEFAULTS", "GENERATED",
            "IDENTITY", "INDEXES", "STATISTICS", "STORAGE");

    final DdlExecutor ddl;
    final AstExecutor executor;

    DdlTableExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- CREATE TABLE ----

    QueryResult executeCreateTable(CreateTableStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.creationSchema();
        if (stmt.temporary()) {
            schemaName = executor.session != null ? executor.session.getTempSchemaName() : "pg_temp";
        }
        if (stmt.schema() != null && executor.database.getSchema(stmt.schema()) == null) {
            throw new MemgresException("schema \"" + stmt.schema() + "\" does not exist", "3F000");
        }
        if ("pg_catalog".equalsIgnoreCase(schemaName) || "information_schema".equalsIgnoreCase(schemaName)) {
            throw new MemgresException("permission denied for schema " + schemaName, "42501");
        }
        // ON COMMIT describes what happens to the rows at the end of the transaction that made
        // them, which only means anything for a table that lives no longer than the session.
        if (stmt.onCommitAction() != null && !stmt.temporary()) {
            throw new MemgresException("ON COMMIT can only be used on temporary tables", "42P16");
        }
        Schema schema = executor.database.getOrCreateSchema(schemaName);

        // A schema holds one relation of a given name whatever its kind, so a view, sequence or
        // index of this name blocks the table just as another table would.
        if (RelationNamespace.kindOf(executor.database, schemaName, stmt.name()) != null) {
            if (stmt.ifNotExists()) {
                if (executor.session != null) {
                    executor.session.addNotice("NOTICE", "42P07",
                            "relation \"" + stmt.name() + "\" already exists, skipping", null);
                }
                return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
            }
            throw new MemgresException("relation \"" + stmt.name() + "\" already exists", "42P07");
        }

        // Handle PARTITION OF
        if (stmt.partitionOfParent() != null) {
            return createPartitionOfTable(stmt, schema, schemaName);
        }

        // Build inherited columns first
        List<Column> inheritedColumns = new ArrayList<>();
        List<Table> parentTables = new ArrayList<>();
        if (stmt.inherits() != null) {
            Set<String> childDeclared = new HashSet<>();
            for (ColumnDef def : stmt.columns()) childDeclared.add(def.name().toLowerCase());
            Set<String> seenParents = new HashSet<>();
            for (String parentName : stmt.inherits()) {
                Table parent = executor.resolveTable(schemaName, parentName);
                if (!seenParents.add(parent.getName().toLowerCase())) {
                    throw new MemgresException("relation \"" + parentName
                            + "\" would be inherited from more than once", "42P07");
                }
                parentTables.add(parent);
                for (Column col : parent.getColumns()) {
                    Column existing = null;
                    for (Column c : inheritedColumns) {
                        if (c.getName().equalsIgnoreCase(col.getName())) { existing = c; break; }
                    }
                    if (existing == null) { inheritedColumns.add(col); continue; }
                    // Two parents contributing one column have to agree about what it holds:
                    // the child gets one column, so a second parent's wider type has nowhere
                    // to go and its rows would silently change shape on the way in.
                    if (existing.getType() != col.getType()) {
                        throw new MemgresException("inherited column \"" + col.getName()
                                + "\" has a type conflict", "42804");
                    }
                    // A default the child does not override would have to be picked from two
                    // parents, and there is no rule for choosing.
                    if (!childDeclared.contains(col.getName().toLowerCase())
                            && existing.getDefaultValue() != null && col.getDefaultValue() != null
                            && !existing.getDefaultValue().equals(col.getDefaultValue())) {
                        throw new MemgresException("column \"" + col.getName()
                                + "\" inherits conflicting default values", "42611");
                    }
                }
            }
        }

        // Handle LIKE tables
        List<StoredConstraint> likeConstraints = new ArrayList<>();
        // Track indexes to clone from LIKE ... INCLUDING INDEXES
        List<String[]> likeIndexesToClone = new ArrayList<>(); // each: {srcIndexName, newTableName}
        // The name each copied NOT NULL constraint keeps, and the comments INCLUDING COMMENTS
        // brings along, both applied once the table itself exists.
        Map<String, String> likeNotNullNames = new LinkedHashMap<>();
        Map<String, String> likeColumnComments = new LinkedHashMap<>();
        if (stmt.likeTables() != null) {
            for (String likeEntry : stmt.likeTables()) {
                // Parse "tablename:OPT1,OPT2" format
                String likeTableName;
                Set<String> likeOptions = new HashSet<>();
                int colonIdx = likeEntry.indexOf(':');
                if (colonIdx >= 0) {
                    likeTableName = likeEntry.substring(0, colonIdx);
                    // The options are applied in the order they were written, so a later
                    // EXCLUDING takes back what an earlier INCLUDING ALL brought in.
                    for (String opt : likeEntry.substring(colonIdx + 1).split(",")) {
                        String what = opt.trim().toUpperCase();
                        boolean excluding = what.startsWith("-");
                        if (excluding) what = what.substring(1);
                        Collection<String> affected = "ALL".equals(what) ? LIKE_OPTION_NAMES
                                : Cols.setOf(what);
                        if (excluding) likeOptions.removeAll(affected);
                        else likeOptions.addAll(affected);
                    }
                } else {
                    likeTableName = likeEntry;
                }
                Table likeTable = executor.resolveTable(schemaName, likeTableName);
                // LIKE copies the shape of a column and nothing more unless the option that
                // carries the rest is written out. Adding the source column object itself gave
                // the new table the source's defaults, its identity and its generation
                // expression, none of which PostgreSQL copies unasked.
                for (Column col : likeTable.getColumns()) {
                    boolean exists = inheritedColumns.stream()
                            .anyMatch(c -> c.getName().equalsIgnoreCase(col.getName()));
                    if (!exists) {
                        inheritedColumns.add(likeColumnCopy(col, likeOptions));
                        // A NOT NULL constraint travels with the column, under the name it has
                        // on the source table: PostgreSQL copies the constraint, not the rule.
                        if (!col.isNullable()) {
                            likeNotNullNames.put(col.getName().toLowerCase(),
                                    likeTable.notNullConstraintName(col.getName()));
                        }
                        if (wants(likeOptions, "COMMENTS")) {
                            String srcComment = executor.database.getComment("column",
                                    likeTable.getName() + "." + col.getName());
                            if (srcComment != null) {
                                likeColumnComments.put(col.getName().toLowerCase(), srcComment);
                            }
                        }
                    }
                }
                // LIKE copies the columns and their NOT NULL, and nothing else unless asked.
                // The keys travel with INCLUDING INDEXES, because it is the index they need;
                // INCLUDING CONSTRAINTS brings the CHECK constraints only. Copying them all
                // unasked gave the new table a primary key PostgreSQL never put there — under
                // the source table's constraint name, so a duplicate was reported against a
                // constraint on another relation.
                boolean wantKeys = likeOptions.contains("INDEXES") || likeOptions.contains("ALL");
                boolean wantChecks = likeOptions.contains("CONSTRAINTS") || likeOptions.contains("ALL");
                for (StoredConstraint sc : likeTable.getConstraints()) {
                    boolean isKey = sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                            || sc.getType() == StoredConstraint.Type.UNIQUE
                            || sc.getType() == StoredConstraint.Type.EXCLUDE;
                    if (isKey ? !wantKeys : (!wantChecks
                            || sc.getType() != StoredConstraint.Type.CHECK)) {
                        continue;
                    }
                    // The copy belongs to the new table and is named after it.
                    likeConstraints.add(sc.copyForPartition(stmt.name()));
                }
                // Collect indexes to clone if INCLUDING INDEXES or INCLUDING ALL
                if (likeOptions.contains("INDEXES") || likeOptions.contains("ALL")) {
                    for (Map.Entry<String, List<String>> idxEntry : executor.database.getIndexColumns().entrySet()) {
                        String srcIdx = idxEntry.getKey();
                        String idxTable = executor.database.getIndexTable(srcIdx);
                        if (idxTable != null && (idxTable.equalsIgnoreCase(schemaName + "." + likeTableName)
                                || idxTable.equalsIgnoreCase(likeTableName))) {
                            likeIndexesToClone.add(new String[]{srcIdx, stmt.name()});
                            // A standalone CREATE UNIQUE INDEX also registers a UNIQUE constraint
                            // here, which PostgreSQL does not do — pg_constraint has no row for
                            // one. Cloning both would give the new table two indexes over the same
                            // columns where PostgreSQL gives it one, so the index stands and the
                            // constraint it implies does not travel with it.
                            dropKeyCoveredByIndex(likeConstraints, idxEntry.getValue());
                        }
                    }
                }
            }
        }

        List<Column> columns = new ArrayList<>(inheritedColumns);
        Set<String> definedColumnNames = new HashSet<>();
        for (ColumnDef def : stmt.columns()) {
            if (!definedColumnNames.add(def.name().toLowerCase())) {
                throw new MemgresException("column \"" + def.name() + "\" specified more than once", "42701");
            }
            DdlDefinitionChecks.rejectSystemColumnName(def.name());
            DdlDefinitionChecks.validateDefaultExpression(def.defaultExpr());

            DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(def.typeName(), def.precision());
            DataType dataType = resolved.dataType();
            String enumTypeName = resolved.enumTypeName();
            String domainTypeName = resolved.domainTypeName();
            String compositeTypeName = resolved.compositeTypeName();
            DataType arrayElementType = resolved.arrayElementType();
            boolean notNull = def.notNull();
            if (resolved.domainNotNull()) notNull = true;

            String defaultVal = null;

            // GENERATED AS IDENTITY
            if (def.identity() != null) {
                // Identity is fed by a sequence, so both the type and the absence of a competing
                // DEFAULT have to hold before the sequence is created.
                DdlDefinitionChecks.requireIdentityType(dataType);
                if (def.defaultExpr() != null
                        || dataType == DataType.SERIAL || dataType == DataType.BIGSERIAL
                        || dataType == DataType.SMALLSERIAL) {
                    throw PgErrors.syntax("both default and identity specified for column \""
                            + def.name() + "\" of table \"" + stmt.name() + "\"");
                }
                notNull = true;
                String seqName = stmt.name() + "_" + def.name() + "_seq";
                Sequence seq = new Sequence(seqName, def.identityStart(), def.identityIncrement(), null, null);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                if (def.identityStart() != null || def.identityIncrement() != null) {
                    if (dataType != DataType.BIGINT && dataType != DataType.INTEGER && dataType != DataType.SMALLINT) {
                        dataType = DataType.INTEGER;
                    }
                } else {
                    if (dataType == DataType.INTEGER) dataType = DataType.SERIAL;
                    else if (dataType == DataType.BIGINT) dataType = DataType.BIGSERIAL;
                    else if (dataType == DataType.SMALLINT) dataType = DataType.SMALLSERIAL;
                    else dataType = DataType.SERIAL;
                }
                if ("ALWAYS".equalsIgnoreCase(def.identity())) {
                    defaultVal = "__identity__:always:seq:" + seqName;
                } else {
                    defaultVal = "__identity__:bydefault:seq:" + seqName;
                }
            }

            // SERIAL/BIGSERIAL/SMALLSERIAL — create a real sequence (PG-compatible)
            if (def.identity() == null && (dataType == DataType.SERIAL || dataType == DataType.BIGSERIAL || dataType == DataType.SMALLSERIAL)) {
                String seqName = stmt.name() + "_" + def.name() + "_seq";
                // M14: serial sequence bounds match column type (PG: int4 max=2147483647, int2 max=32767)
                Long seqMax = dataType == DataType.SMALLSERIAL ? Long.valueOf(32767L)
                        : dataType == DataType.SERIAL ? Long.valueOf(2147483647L) : Long.valueOf(9223372036854775807L);
                Sequence seq = new Sequence(seqName, null, null, null, seqMax);
                // M14: set sequence data type to match column type
                if (dataType == DataType.SERIAL) seq.setDataType("integer");
                else if (dataType == DataType.SMALLSERIAL) seq.setDataType("smallint");
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                defaultVal = "nextval('" + seqName + "'::regclass)";
                notNull = true;
            }
            if (def.defaultExpr() != null) {
                defaultVal = DdlExecutor.exprToDefaultString(def.defaultExpr());
                if (dataType != null && TypeCoercion.categoryOf(dataType) == TypeCoercion.TypeCategory.NUMERIC) {
                    String defNorm = defaultVal.toLowerCase().replaceAll("\\s+", "");
                    if (defNorm.contains("now(") || defNorm.contains("current_timestamp")
                            || defNorm.contains("clock_timestamp(") || defNorm.contains("localtimestamp")) {
                        throw new MemgresException("column \"" + def.name() + "\" is of type " + dataType.getPgName()
                                + " but default expression is of type timestamp with time zone", "42804");
                    }
                    if (def.defaultExpr() instanceof Literal
                            && ((Literal) def.defaultExpr()).value() != null) {
                        // DEFAULT NULL names no value at all, so there is nothing to read as a
                        // number: reading one threw a NullPointerException out of the wire handler.
                        Literal lit = (Literal) def.defaultExpr();
                        String strVal = lit.value();
                        try {
                            new java.math.BigDecimal(strVal);
                        } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type "
                                    + dataType.toRegtypeDisplay() + ": \"" + strVal + "\"", "22P02");
                        }
                    }
                }
            }

            // Override inherited column if exists
            int existingIdx = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equalsIgnoreCase(def.name())) {
                    existingIdx = i;
                    break;
                }
            }

            // A DEFAULT and a generation expression are both evaluated for one row at a time, with
            // no other row in sight; PostgreSQL names each context in its own words.
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    def.defaultExpr(), "DEFAULT expressions", null);
            if (def.generatedExpr() != null) {
                // A generation expression is kept as the text it was written as, so its type names
                // are read here rather than when the statement itself was parsed.
                List<String> generatedTypeSchemas = new ArrayList<>();
                Expression generated = null;
                try {
                    generated = com.memgres.engine.parser.Parser.parseExpression(
                            def.generatedExpr(), generatedTypeSchemas);
                } catch (RuntimeException ignored) {
                    // An expression that will not parse is reported by whatever reads it next
                }
                SchemaQualifier.rejectMissingTypeSchemas(
                        executor.database, executor.session, generatedTypeSchemas);
                executor.selectExecutor.placementCheck.rejectStoredDefinition(
                        generated, "column generation expressions", null);
            }

            // Validate generated column expression
            if (def.generatedExpr() != null) {
                // PG rejects DEFAULT + GENERATED ALWAYS AS on same column
                if (def.defaultExpr() != null) {
                    throw new MemgresException("both default and generation expression specified for column \"" + def.name() + "\"", "42601");
                }
                if (def.generatedVirtual()) {
                    // PG 18: check UDF restriction first (0A000), then immutability (42P17)
                    DdlExecutor.checkVirtualColumnUdf(def.generatedExpr(), executor.database);
                    DdlExecutor.checkExpressionImmutability(def.generatedExpr(), executor.database,
                            "generation expression is not immutable");
                } else {
                    DdlExecutor.checkExpressionImmutability(def.generatedExpr(), executor.database,
                            "generation expression is not immutable");
                }
                if (def.generatedExpr().toLowerCase().replaceAll("\\s+", "").contains("select")) {
                    throw new MemgresException("cannot use subquery in column generation expression", "0A000");
                }
            }

            Integer colPrecision = def.precision() != null ? def.precision() : resolved.domainPrecision();
            Integer colScale = def.scale() != null ? def.scale() : resolved.domainScale();
            Column col = new Column(def.name(), dataType, !notNull, def.primaryKey(), defaultVal,
                    enumTypeName, colPrecision, colScale, def.generatedExpr(), def.generatedVirtual(),
                    domainTypeName, compositeTypeName, arrayElementType);
            String qualifier = DataType.intervalQualifier(def.typeName());
            col.setIntervalQualifier(qualifier != null ? qualifier : resolved.domainIntervalQualifier());
            if (def.defaultExpr() != null) {
                col.setParsedDefaultExpr(def.defaultExpr());
            }
            if (existingIdx >= 0) {
                // Redeclaring an inherited column only restates it: the child cannot give the
                // column a type the parent's rows are not already stored in.
                Column inheritedCol = columns.get(existingIdx);
                boolean fromParent = inheritedColumns.stream()
                        .anyMatch(c -> c.getName().equalsIgnoreCase(def.name()));
                if (fromParent && stmt.inherits() != null && inheritedCol.getType() != col.getType()) {
                    throw new MemgresException("column \"" + def.name() + "\" has a type conflict",
                            "42804");
                }
                columns.set(existingIdx, col);
            } else {
                columns.add(col);
            }
        }

        // Validate generated column expressions
        validateGeneratedColumns(stmt.columns(), columns);
        rejectKeysOnVirtualColumns(stmt, columns);

        // A partitioned parent holds no rows of its own, so there is nothing for UNLOGGED to
        // mean; PostgreSQL refuses the combination outright rather than silently ignoring it.
        if (stmt.unlogged() && stmt.partitionBy() != null) {
            throw new MemgresException("partitioned tables cannot be unlogged", "0A000");
        }
        Table table = new Table(stmt.name(), columns);
        if (stmt.unlogged()) table.setUnlogged(true);
        if (stmt.withOptions() != null && !stmt.withOptions().isEmpty()) {
            // A storage parameter is read while the table is being defined: an unknown name or a
            // value outside the range stops the statement rather than being stored as written.
            DdlIndexValidator.checkRelOptions("heap", stmt.withOptions());
            table.setReloptions(stmt.withOptions());
        }

        // Set up inheritance links
        for (Table parent : parentTables) {
            table.setParentTable(parent);
            parent.addChild(table);
        }

        // Set up partitioning
        if (stmt.partitionBy() != null) {
            table.setPartitionStrategy(stmt.partitionBy());
            String partCol = stmt.partitionColumn();
            if (partCol != null) {
                for (String col : splitTopLevel(partCol)) {
                    String trimmed = col.trim();
                    // An expression key decides which partition a row lives in, so it has to
                    // give the same answer every time it is asked. A volatile or stable one
                    // may route a row to one partition on INSERT and look for it in another.
                    if (trimmed.contains("(")) {
                        checkPartitionKeyExpression(trimmed, columns, table);
                        continue;
                    }
                    if (table.getColumnIndex(trimmed) < 0) {
                        throw new MemgresException("column \"" + trimmed + "\" named in partition key does not exist", "42703");
                    }
                }
            }
            table.setPartitionColumn(partCol);
        }

        schema.addTable(table);
        // A relation created under a name that once carried rules starts clean: the pg_class row
        // that remembered them went with the relation that was dropped.
        executor.database.clearRuleHistory(stmt.name());
        executor.database.markUncommittedObject(table, executor.session);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));

        try {
        // ON COMMIT actions for temp tables
        if ("DROP".equals(stmt.onCommitAction()) && executor.session != null) {
            if (executor.session.isInTransaction()) {
                executor.session.registerOnCommitDrop(schemaName, stmt.name());
            } else if (executor.session.isInNestedExecution()) {
                // A function body is part of one outer statement, so the table has to outlive the
                // CREATE and die when that statement does — not the moment it is created.
                executor.session.registerStatementEndDrop(schemaName, stmt.name());
            } else {
                schema.removeTable(stmt.name());
            }
        }
        if ("DELETE ROWS".equals(stmt.onCommitAction()) && executor.session != null) {
            executor.session.registerOnCommitDeleteRows(schemaName, stmt.name());
        }

        // Store column-level constraints
        for (ColumnDef def : stmt.columns()) {
            if (def.primaryKey()) {
                StoredConstraint pk = StoredConstraint.primaryKey(
                        def.primaryKeyName() != null ? def.primaryKeyName() : stmt.name() + "_pkey",
                        Cols.listOf(def.name()));
                // A column-level key carries its own DEFERRABLE, exactly as a table-level one does.
                pk.setDeferrable(def.deferrable());
                pk.setInitiallyDeferred(def.initiallyDeferred());
                table.addConstraint(pk);
            }
            if (def.unique()) {
                StoredConstraint uq = StoredConstraint.unique(
                        def.uniqueName() != null ? def.uniqueName()
                                : stmt.name() + "_" + def.name() + "_key",
                        Cols.listOf(def.name()));
                uq.setDeferrable(def.deferrable());
                uq.setInitiallyDeferred(def.initiallyDeferred());
                table.addConstraint(uq);
            }
            if (def.referencesTable() != null) {
                addColumnForeignKey(table, def, schemaName, stmt.name());
            }
        }

        // Store table-level constraints
        if (stmt.constraints() != null) {
            for (TableConstraint tc : stmt.constraints()) {
                if (tc.type() == TableConstraint.ConstraintType.NOT_NULL) {
                    for (String colName : tc.columns()) {
                        table.alterColumnNullable(colName, false);
                    }
                    continue;
                }
                if (tc.type() == TableConstraint.ConstraintType.CHECK) {
                    // A CHECK is tested against the row being written, on its own; it can see no
                    // other row, so nothing in it may need a group or a finished result — and no
                    // call in it may carry a clause only an aggregate has a use for.
                    executor.selectExecutor.placementCheck.rejectStoredDefinition(
                            tc.checkExpr(), "check constraints", "check constraint");
                    DdlDefinitionChecks.requireBooleanPredicate(tc.checkExpr(), table, "CHECK");
                }
                StoredConstraint sc = ddl.convertTableConstraint(stmt.name(), tc, table);
                if (sc != null) {
                    // For FK constraints without explicit schema, set the schema from the table's schema
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                            && sc.getReferencesSchema() == null && sc.getReferencesTable() != null) {
                        Table refTable = ddl.resolveTableOrNull(sc.getReferencesTable());
                        if (refTable != null) checkTempPermanentReference(schemaName, refTable);
                        sc.setReferencesSchema(schemaName);
                    }
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                        ddl.validateForeignKeyDefinition(sc, table, schemaName);
                    }
                    table.addConstraint(sc);
                    ddl.registerExcludeIndex(schemaName, stmt.name(), sc);
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                        for (String colName : sc.getColumns()) {
                            table.alterColumnNullable(colName, false);
                        }
                    }
                }
            }
        }

        // A table has at most one primary key, so a second declaration is a fault in the
        // definition rather than a second constraint to store.
        DdlDefinitionChecks.rejectSecondPrimaryKey(table, stmt.name());

        // Validate that PK/UNIQUE constraints on partitioned tables include the partition column
        for (StoredConstraint sc : table.getConstraints()) {
            validatePartitionKeyCoverage(table, sc);
        }
        } catch (MemgresException e) {
            // Roll back: remove the table from schema so it doesn't persist after a failed CREATE TABLE.
            // This matches PG's atomic DDL behavior where a failed CREATE TABLE leaves no trace.
            schema.removeTable(stmt.name());
            throw e;
        }

        // Add constraints from LIKE tables
        for (StoredConstraint likeSc : likeConstraints) {
            table.addConstraint(likeSc);
        }

        // A NOT NULL constraint that came in over LIKE answers to the name it had on the source
        // table, and INCLUDING COMMENTS brings the source's column comments with it.
        for (Map.Entry<String, String> nn : likeNotNullNames.entrySet()) {
            if (nn.getValue() != null && !table.getColumns().isEmpty()) {
                table.setNotNullConstraintName(nn.getKey(), nn.getValue());
            }
        }
        for (Map.Entry<String, String> cc : likeColumnComments.entrySet()) {
            executor.database.addComment("column",
                    (stmt.name() + "." + cc.getKey()).toLowerCase(), cc.getValue());
        }

        // Clone indexes from LIKE ... INCLUDING INDEXES
        for (String[] idxInfo : likeIndexesToClone) {
            String srcIdx = idxInfo[0];
            String newTableName = idxInfo[1];
            List<String> srcCols = executor.database.getIndexColumns().get(srcIdx);
            if (srcCols != null) {
                // Generate a new index name: replace source table name prefix with new table name
                String newIdxName = newTableName + "_" + String.join("_", srcCols) + "_idx";
                boolean isUnique = executor.database.isUniqueIndex(srcIdx);
                String method = executor.database.getIndexMethod(srcIdx);
                executor.database.addIndex(newIdxName, new ArrayList<>(srcCols));
                executor.database.addIndexMeta(newIdxName, schemaName + "." + newTableName, isUnique, method, null);
            }
        }

        executor.database.setObjectOwner("table:" + schemaName + "." + stmt.name(), executor.currentRole());
        // M11: Apply ALTER DEFAULT PRIVILEGES to newly created table
        applyDefaultPrivileges(schemaName, stmt.name(), executor.currentRole());
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * A VIRTUAL generated column is recomputed on every read and never stored, so there is
     * nothing for a unique index to hold; PostgreSQL refuses to key on one. Checked before the
     * table exists, so a rejected definition leaves nothing behind.
     */
    /** Whether a LIKE option was asked for, either by name or through INCLUDING ALL. */
    private static boolean wants(Set<String> options, String option) {
        return options.contains(option) || options.contains("ALL");
    }

    /**
     * The column LIKE actually copies: name, type and NOT NULL always; the default, the identity,
     * the generation expression, the storage settings and the statistics target only when the
     * matching INCLUDING option asked for them. A serial or identity column whose default does
     * not travel is left as the plain integer type underneath it, because without the default
     * there is no sequence behind it to draw from.
     */
    private static Column likeColumnCopy(Column src, Set<String> options) {
        String defaultValue = src.getDefaultValue();
        boolean identity = defaultValue != null && defaultValue.startsWith("__identity__:");
        boolean keepDefault = identity ? wants(options, "IDENTITY") : wants(options, "DEFAULTS");
        if (!keepDefault) defaultValue = null;
        DataType type = src.getType();
        if (defaultValue == null) type = withoutSerial(type);
        String generated = wants(options, "GENERATED") ? src.getGeneratedExpr() : null;
        Column copy = new Column(src.getName(), type, src.isNullable(),
                src.isPrimaryKey() && wants(options, "INDEXES"), defaultValue,
                src.getEnumTypeName(), src.getPrecision(), src.getScale(), generated,
                generated != null && src.isVirtual(), src.getDomainTypeName(),
                src.getCompositeTypeName(), src.getArrayElementType());
        copy.setIntervalQualifier(src.getIntervalQualifier());
        if (wants(options, "STORAGE")) {
            copy.setAttStorageOverride(src.getAttStorageOverride());
            copy.setAttCompression(src.getAttCompression());
        }
        if (wants(options, "STATISTICS")) copy.setAttStattarget(src.getAttStattarget());
        return copy;
    }

    /** The integer type a serial column is stored in, for a copy that carries no sequence. */
    private static DataType withoutSerial(DataType type) {
        if (type == DataType.SERIAL) return DataType.INTEGER;
        if (type == DataType.BIGSERIAL) return DataType.BIGINT;
        if (type == DataType.SMALLSERIAL) return DataType.SMALLINT;
        return type;
    }

    /**
     * Drops the UNIQUE constraint a cloned index already stands for, matching on the columns
     * because that is what the two have in common. A PRIMARY KEY is left alone: PostgreSQL does
     * record one in pg_constraint and does clone it, and its index is named for the key rather
     * than copied under its own name.
     */
    private static void dropKeyCoveredByIndex(List<StoredConstraint> constraints,
                                              List<String> indexColumns) {
        if (indexColumns == null || indexColumns.isEmpty()) return;
        java.util.Iterator<StoredConstraint> it = constraints.iterator();
        while (it.hasNext()) {
            StoredConstraint sc = it.next();
            if (sc.getType() != StoredConstraint.Type.UNIQUE) continue;
            List<String> cols = sc.getColumns();
            if (cols == null || cols.size() != indexColumns.size()) continue;
            boolean same = true;
            for (int i = 0; i < cols.size(); i++) {
                if (!cols.get(i).equalsIgnoreCase(indexColumns.get(i))) { same = false; break; }
            }
            if (same) it.remove();
        }
    }

    private void rejectKeysOnVirtualColumns(CreateTableStmt stmt, List<Column> columns) {
        Set<String> virtual = new HashSet<>();
        for (Column c : columns) {
            if (c.getGeneratedExpr() != null && c.isVirtual()) {
                virtual.add(c.getName().toLowerCase());
            }
        }
        if (virtual.isEmpty()) return;
        for (ColumnDef def : stmt.columns()) {
            if (!virtual.contains(def.name().toLowerCase())) continue;
            if (def.primaryKey()) throw virtualKeyError(true);
            if (def.unique()) throw virtualKeyError(false);
        }
        if (stmt.constraints() == null) return;
        for (TableConstraint tc : stmt.constraints()) {
            boolean pk = tc.type() == TableConstraint.ConstraintType.PRIMARY_KEY;
            boolean uq = tc.type() == TableConstraint.ConstraintType.UNIQUE;
            if ((!pk && !uq) || tc.columns() == null) continue;
            for (String col : tc.columns()) {
                if (virtual.contains(col.toLowerCase())) throw virtualKeyError(pk);
            }
        }
    }

    private static MemgresException virtualKeyError(boolean primaryKey) {
        return PgErrors.notImplemented(primaryKey
                ? "primary keys on virtual generated columns are not supported"
                : "unique constraints on virtual generated columns are not supported");
    }

    /**
     * A temporary table disappears at session end, so a foreign key across that boundary either
     * outlives its target or holds a permanent table hostage to one session. PostgreSQL refuses
     * both directions.
     */
    void checkTempPermanentReference(String schemaName, Table refTable) {
        boolean sourceTemp = isTempSchema(schemaName);
        boolean targetTemp = isTempSchema(findSchemaNameOf(refTable, schemaName));
        if (sourceTemp == targetTemp) return;
        throw new MemgresException(sourceTemp
                ? "constraints on temporary tables may reference only temporary tables"
                : "constraints on permanent tables may reference only permanent tables", "42P16");
    }

    /** True for the per-session temp schema, whatever suffix this session's is named with. */
    static boolean isTempSchema(String schemaName) {
        return schemaName != null && schemaName.toLowerCase().startsWith("pg_temp");
    }

    /** M11: Apply default privileges from ALTER DEFAULT PRIVILEGES to a newly created table. */
    private void applyDefaultPrivileges(String schemaName, String tableName, String creator) {
        for (Database.DefaultAclEntry entry : executor.database.getDefaultAcls()) {
            if (!entry.isGrant) continue;
            // Must be for TABLES
            if (!"TABLES".equalsIgnoreCase(entry.objectType)) continue;
            // Grantor must match the creating role
            if (entry.grantor != null && !entry.grantor.equalsIgnoreCase(creator)) continue;
            // Schema must match (null = all schemas)
            if (entry.schema != null && !entry.schema.equalsIgnoreCase(schemaName)) continue;
            for (String grantee : entry.grantees) {
                for (String priv : entry.privileges) {
                    executor.database.addRolePrivilege(grantee, priv, "TABLE",
                            AstExecutor.privilegeKey(schemaName, tableName));
                }
            }
        }
    }

    private QueryResult createPartitionOfTable(CreateTableStmt stmt, Schema schema, String schemaName) {
        Table parent = executor.resolveTable(schemaName, stmt.partitionOfParent());
        // A table with no partition key has no slots to attach to, so a bound over it describes
        // nothing; accepting it produces a table that holds rows nothing ever routes to.
        if (parent.getPartitionStrategy() == null) {
            throw new MemgresException("\"" + stmt.partitionOfParent() + "\" is not partitioned",
                    "42P17");
        }
        Table partition = new Table(stmt.name(), new ArrayList<>(parent.getColumns()));
        partition.setPartitionParent(parent);

        // Partitions must enforce the parent's PK/UNIQUE constraints themselves: actual row
        // storage lives on the leaf partition, not the parent, so without a copy here the
        // partition has no TableIndex and neither per-partition duplicate-key checks nor
        // ON CONFLICT conflict detection can find rows that already live in that partition
        // (PostgreSQL requires unique constraints on a partitioned table to include the
        // partition key, so enforcing them independently per-partition is correct).
        // Each partition gets its own independent StoredConstraint copy (not the parent's
        // instance) so that later mutations - e.g. ALTER TABLE ... VALIDATE CONSTRAINT - applied
        // through one table can't silently leak into siblings sharing the same object.
        for (StoredConstraint sc : parent.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                partition.addConstraint(sc.copyForPartition(stmt.name()));
            }
        }

        if (stmt.partitionBounds() != null && !stmt.partitionBounds().isEmpty()) {
            applyPartitionBounds(partition, parent, stmt.partitionBounds(), stmt.name());
        }

        if (stmt.partitionBy() != null) {
            partition.setPartitionStrategy(stmt.partitionBy());
            partition.setPartitionColumn(stmt.partitionColumn());
        }

        // Attach to the parent only after bound validation succeeded, so a rejected
        // partition (e.g. overlapping bounds) doesn't linger in the parent's routing list
        parent.addPartition(partition);
        schema.addTable(partition);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * Validates that a PK/UNIQUE constraint on a partitioned table includes every column of the
     * table's partition key (PostgreSQL requires this so that unique enforcement can be done
     * per-partition). No-op for non-partitioned tables, other constraint types, and
     * expression-based partition keys (which this simple check doesn't attempt to parse).
     * Shared between CREATE TABLE (table-level and column-level constraints) and
     * ALTER TABLE ... ADD CONSTRAINT so both entry points reject the same invalid definitions.
     */
    static void validatePartitionKeyCoverage(Table table, StoredConstraint sc) {
        if (table.getPartitionStrategy() == null || table.getPartitionColumn() == null) return;
        if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY && sc.getType() != StoredConstraint.Type.UNIQUE) return;

        String rawPartCol = table.getPartitionColumn().toLowerCase().trim();
        // Strip surrounding parens from expression-based partition keys
        if (rawPartCol.startsWith("(")) rawPartCol = rawPartCol.substring(1);
        if (rawPartCol.endsWith(")")) rawPartCol = rawPartCol.substring(0, rawPartCol.length() - 1);
        rawPartCol = rawPartCol.trim();
        // Only validate simple column-name partition keys (skip expressions)
        if (rawPartCol.contains("(")) return;

        for (String partKeyCol : rawPartCol.split(",")) {
            String partKey = partKeyCol.trim();
            boolean found = false;
            for (String col : sc.getColumns()) {
                if (col.equalsIgnoreCase(partKey)) { found = true; break; }
            }
            if (!found) {
                String constraintKind = sc.getType() == StoredConstraint.Type.PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE";
                throw new MemgresException(
                        "unique constraint on partitioned table must include all partitioning columns\n"
                        + "  Detail: " + constraintKind + " constraint missing column \""
                        + partKey + "\" which is part of the partition key.",
                        "0A000");
            }
        }
    }

    /**
     * A bound written as an expression rather than a literal, reduced to the literal it is worth.
     *
     * <p>PostgreSQL settles a partition bound when the partition is created, so it may be any
     * expression that has a value then — {@code abs(-1)}, {@code 1 + 1} — and it may not be one
     * that depends on a row or on other rows. An aggregate or a window call there is refused by
     * the same walk and under the same clause name every other definition uses.
     */
    private List<String> evaluateBoundExpressions(List<String> bounds) {
        String marker = com.memgres.engine.parser.Parser.BOUND_EXPRESSION_MARKER;
        boolean any = false;
        for (String bound : bounds) {
            if (bound != null && bound.startsWith(marker)) any = true;
        }
        if (!any) return bounds;
        List<String> resolved = new ArrayList<>(bounds.size());
        for (String bound : bounds) {
            if (bound == null || !bound.startsWith(marker)) {
                resolved.add(bound);
                continue;
            }
            String text = bound.substring(marker.length());
            Expression expr = com.memgres.engine.parser.Parser.parseExpression(text);
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    expr, "partition bound", null);
            // A bound is settled once, with no row to read and no query to read one from, so a
            // column reference and a sub-select are both refused for what they are rather than
            // reported as a name nothing answers for.
            rejectColumnReferenceInBound(expr);
            executor.selectExecutor.placementCheck.rejectSubquery(expr, "partition bound");
            Object value = executor.evalExpr(expr, new RowContext(Cols.listOf()));
            if (value == null) {
                resolved.add("NULL");
            } else if (value instanceof Number || value instanceof Boolean) {
                resolved.add(value.toString());
            } else {
                resolved.add("'" + value.toString().replace("'", "''") + "'");
            }
        }
        return resolved;
    }

    /** The same refusal the bound parser makes for a bare name, for one written inside an expression. */
    private static void rejectColumnReferenceInBound(Expression expr) {
        if (AstWalk.anyMatch(expr, n -> n instanceof com.memgres.engine.parser.ast.ColumnRef)) {
            throw new MemgresException(
                    "cannot use column reference in partition bound expression", "0A000");
        }
    }

    /**
     * Apply partition bounds (FROM/IN/HASH/DEFAULT) to a partition, validating against siblings.
     * Shared between CREATE TABLE PARTITION OF and ALTER TABLE ATTACH PARTITION.
     */
    void applyPartitionBounds(Table partition, Table parent, List<String> bounds, String partitionName) {
        bounds = evaluateBoundExpressions(bounds);
        String boundType = bounds.get(0);
        String strategy = parent.getPartitionStrategy();
        if (boundType.equals("DEFAULT")) {
            if ("HASH".equalsIgnoreCase(strategy)) {
                throw new MemgresException(
                        "a hash-partitioned table may not have a default partition", "42P16");
            }
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                if (existingPart.isDefaultPartition()) {
                    throw new MemgresException("partition \"" + partitionName
                            + "\" conflicts with existing default partition \""
                            + existingPart.getName() + "\"", "42P17");
                }
            }
            partition.setDefaultPartition(true);
        } else if (boundType.equals("FROM") && bounds.size() >= 4) {
            requireStrategy(strategy, "RANGE");
            // bounds format: FROM, v1[, v2, ...], TO, v1[, v2, ...]
            int toIdx = bounds.indexOf("TO");
            if (toIdx < 0) toIdx = 2; // defensive; parser always emits TO
            List<Column> keyCols = partitionKeyColumns(parent);
            int keyCount = keyCols != null ? keyCols.size() : toIdx - 1;
            if (toIdx - 1 != keyCount) {
                throw new MemgresException(
                        "FROM must specify exactly one value per partitioning column", "42P16");
            }
            if (bounds.size() - toIdx - 1 != keyCount) {
                throw new MemgresException(
                        "TO must specify exactly one value per partitioning column", "42P16");
            }
            List<Object> lowerVals = new ArrayList<>();
            for (int i = 1; i < toIdx; i++) {
                lowerVals.add(coerceBoundToKeyType(DdlExecutor.parseBoundValue(bounds.get(i)), keyCols, i - 1));
            }
            List<Object> upperVals = new ArrayList<>();
            for (int i = toIdx + 1; i < bounds.size(); i++) {
                upperVals.add(coerceBoundToKeyType(DdlExecutor.parseBoundValue(bounds.get(i)), keyCols, i - toIdx - 1));
            }
            Object newLow = lowerVals.size() == 1 ? lowerVals.get(0) : lowerVals;
            Object newHigh = upperVals.size() == 1 ? upperVals.get(0) : upperVals;
            // A partition whose lower bound is not below its upper bound can never receive a row
            if (DdlExecutor.comparePartitionBound(newLow, newHigh) >= 0) {
                MemgresException ex = new MemgresException("empty range bound specified for partition \""
                        + partitionName + "\"", "42P17");
                ex.setDetail("Specified lower bound (" + formatBound(lowerVals)
                        + ") is greater than or equal to upper bound (" + formatBound(upperVals) + ").");
                throw ex;
            }
            // Check for overlap with existing RANGE partitions
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                if (existingPart.getPartitionLower() != null && existingPart.getPartitionUpper() != null) {
                    if (DdlExecutor.comparePartitionBound(newLow, existingPart.getPartitionUpper()) < 0
                            && DdlExecutor.comparePartitionBound(newHigh, existingPart.getPartitionLower()) > 0) {
                        throw new MemgresException("partition \"" + partitionName
                                + "\" would overlap partition \"" + existingPart.getName() + "\"", "42P17");
                    }
                }
            }
            // Check sub-partition bounds against parent bounds
            if (parent.getPartitionParent() != null) {
                Object parentLow = parent.getPartitionLower();
                Object parentHigh = parent.getPartitionUpper();
                if (parentLow != null && parentHigh != null) {
                    String parentCol = parent.getPartitionColumn();
                    String grandparentCol = parent.getPartitionParent().getPartitionColumn();
                    if (parentCol != null && parentCol.equalsIgnoreCase(grandparentCol)) {
                        if (DdlExecutor.comparePartitionBound(newLow, parentLow) < 0
                                || DdlExecutor.comparePartitionBound(newHigh, parentHigh) > 0) {
                            throw new MemgresException("partition \"" + partitionName
                                    + "\" is outside the bounds of its parent partition \"" + parent.getName() + "\"", "42P16");
                        }
                    }
                }
            }
            partition.setPartitionBounds(newLow, newHigh);
        } else if (boundType.equals("IN")) {
            requireStrategy(strategy, "LIST");
            List<Column> keyCols = partitionKeyColumns(parent);
            List<Object> values = new ArrayList<>();
            for (int i = 1; i < bounds.size(); i++) {
                values.add(coerceBoundToKeyType(DdlExecutor.parseBoundValue(bounds.get(i)), keyCols, 0));
            }
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                List<Object> existingVals = existingPart.getPartitionValues();
                if (existingVals != null) {
                    for (Object v : values) {
                        boolean overlaps;
                        if (v == null) {
                            overlaps = existingVals.stream().anyMatch(ev -> ev == null);
                        } else {
                            overlaps = existingVals.stream().anyMatch(ev -> ev != null
                                    && String.valueOf(ev).equals(String.valueOf(v)));
                        }
                        if (overlaps) {
                            throw new MemgresException("partition \"" + partitionName
                                    + "\" would overlap partition \"" + existingPart.getName() + "\"", "42P17");
                        }
                    }
                }
            }
            partition.setPartitionValues(values);
        } else if (boundType.equals("HASH") && bounds.size() >= 3) {
            requireStrategy(strategy, "HASH");
            int modulus = Integer.parseInt(bounds.get(1));
            int remainder = Integer.parseInt(bounds.get(2));
            if (modulus <= 0) {
                throw new MemgresException(
                        "modulus for hash partition must be an integer value greater than zero", "42P16");
            }
            if (remainder < 0 || remainder >= modulus) {
                throw new MemgresException("remainder for hash partition must be less than modulus", "42P16");
            }
            // A hash partition takes the rows whose hash leaves this remainder modulo this
            // modulus. Two moduli divide the same space consistently only when one is a multiple
            // of the other, and two such slots collide when their remainders agree modulo the
            // greatest common divisor — which is how MODULUS 4 REMAINDER 1 and MODULUS 2
            // REMAINDER 0 can live side by side while MODULUS 2 REMAINDER 1 cannot.
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                Integer otherModulus = existingPart.getPartitionModulus();
                if (otherModulus == null || otherModulus <= 0) continue;
                if (modulus % otherModulus != 0 && otherModulus % modulus != 0) {
                    throw new MemgresException(
                            "every hash partition modulus must be a factor of the next larger modulus",
                            "42P17");
                }
            }
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                Integer otherModulus = existingPart.getPartitionModulus();
                Integer otherRemainder = existingPart.getPartitionRemainder();
                if (otherModulus == null || otherRemainder == null || otherModulus <= 0) continue;
                int common = gcd(modulus, otherModulus);
                if (remainder % common == otherRemainder % common) {
                    throw new MemgresException("partition \"" + partitionName
                            + "\" would overlap partition \"" + existingPart.getName() + "\"", "42P17");
                }
            }
            partition.setPartitionHash(modulus, remainder);
        }
    }

    /**
     * How a relation is named in a message about it: bare when the schema it lives in is reached
     * through the search path, schema-qualified when it is not and the bare name would name
     * something else — or nothing.
     */
    private String visibleName(String schemaName, String name) {
        String schema = schemaName == null ? "public" : schemaName;
        if (schema.equalsIgnoreCase("public") || schema.equalsIgnoreCase(executor.defaultSchema())) {
            return name;
        }
        return schema + "." + name;
    }

    /** Greatest common divisor, for deciding whether two hash partition slots overlap. */
    private static int gcd(int a, int b) {
        while (b != 0) {
            int t = b;
            b = a % b;
            a = t;
        }
        return a;
    }

    /**
     * Resolve the parent's partition key to its Column definitions, in key order.
     * Returns null for expression-based keys or unresolvable column names.
     */
    private static List<Column> partitionKeyColumns(Table parent) {
        String partCol = parent.getPartitionColumn();
        if (partCol == null || partCol.contains("(")) return null;
        List<Column> cols = new ArrayList<>();
        for (String part : partCol.split(",")) {
            int idx = parent.getColumnIndex(part.trim());
            if (idx < 0) return null;
            cols.add(parent.getColumns().get(idx));
        }
        return cols;
    }

    /**
     * Coerce a parsed bound literal to the partition key column's type (e.g. '2026-01-01'
     * to LocalDate for a date key) so routing compares typed values instead of strings.
     * Sentinels (MINVALUE/MAXVALUE) and NULL pass through untouched.
     */
    private static Object coerceBoundToKeyType(Object value, List<Column> keyCols, int keyIndex) {
        if (value == null || value instanceof PartitionBound) return value;
        if (keyCols == null || keyIndex < 0 || keyIndex >= keyCols.size()) return value;
        try {
            return TypeCoercion.coerce(value, keyCols.get(keyIndex).getType());
        } catch (MemgresException e) {
            // Keeping a bound this engine could not read as the key's type would leave a
            // partition that routing can never match; PG rejects the definition instead.
            throw e;
        } catch (Exception e) {
            return value;
        }
    }

    /** The bound form must match the parent's partitioning strategy. */
    private static void requireStrategy(String actual, String required) {
        if (actual == null || actual.equalsIgnoreCase(required)) return;
        throw new MemgresException("invalid bound specification for a "
                + actual.toLowerCase() + " partition", "42P16");
    }

    /** Render a bound tuple the way PostgreSQL spells it in the empty-range detail line. */
    private static String formatBound(List<Object> values) {
        StringBuilder sb = new StringBuilder();
        for (Object v : values) {
            if (sb.length() > 0) sb.append(", ");
            if (v == PartitionBound.MINVALUE) sb.append("MINVALUE");
            else if (v == PartitionBound.MAXVALUE) sb.append("MAXVALUE");
            else if (v == null) sb.append("NULL");
            else if (v instanceof String) sb.append('\'').append(v).append('\'');
            else sb.append(v);
        }
        return sb.toString();
    }

    private void addColumnForeignKey(Table table, ColumnDef def, String schemaName, String tableName) {
        String refTableName = def.referencesTable();
        String refSchemaName = null;
        if (refTableName.contains(".")) {
            String[] parts = refTableName.split("\\.", 2);
            refSchemaName = parts[0];
            refTableName = parts[1]; // bare table name
        }
        List<String> refCols = def.referencesColumn() != null
                ? Cols.listOf(def.referencesColumn()) : Cols.listOf();
        StoredConstraint fk = StoredConstraint.foreignKey(
                def.foreignKeyName() != null ? def.foreignKeyName()
                        : tableName + "_" + def.name() + "_fkey",
                Cols.listOf(def.name()), refTableName, refCols,
                StoredConstraint.parseFkAction(def.refOnDelete()),
                StoredConstraint.parseFkAction(def.refOnUpdate()));
        if (refSchemaName != null) fk.setReferencesSchema(refSchemaName);
        if (def.deferrable()) {
            fk.setDeferrable(true);
            fk.setInitiallyDeferred(def.initiallyDeferred());
        }
        if (def.notEnforced()) fk.setNotEnforced(true);
        if (def.refMatchType() != null) fk.setMatchType(def.refMatchType());
        fk.setOnDeleteSetNullColumns(StoredConstraint.parseSetNullColumns(def.refOnDelete()));
        fk.setOnUpdateSetNullColumns(StoredConstraint.parseSetNullColumns(def.refOnUpdate()));
        ddl.validateForeignKeyDefinition(fk, table, schemaName);
        table.addConstraint(fk);
    }

    /** The message PostgreSQL uses for a partition key expression that may change its answer. */
    private static final String PARTITION_KEY_NOT_IMMUTABLE =
            "functions in partition key expression must be marked IMMUTABLE";

    /**
     * Refuse a partition key expression that cannot be relied on to give the same answer twice.
     * A volatile function is the obvious case; a cast off a {@code timestamptz} is the quiet one,
     * since it reads the session's time zone and so answers differently in another session.
     */
    private void checkPartitionKeyExpression(String keyText, List<Column> columns, Table table) {
        Expression keyExpr;
        try {
            keyExpr = com.memgres.engine.parser.Parser.parseExpression(keyText);
        } catch (RuntimeException notAnExpression) {
            return; // whatever reads it next reports it
        }
        rejectUnknownPartitionKeyColumn(keyExpr, table);
        DdlExecutor.checkExpressionImmutability(keyExpr, executor.database,
                PARTITION_KEY_NOT_IMMUTABLE);
        rejectTimeZoneDependentCast(keyExpr, columns);
    }

    /** A key expression reads the row it routes, so every name in it has to be a column. */
    private void rejectUnknownPartitionKeyColumn(Object node, Table table) {
        AstWalk.forEach(node, n -> {
            if (!(n instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) n;
            if (ref.table() != null) return;
            if (table.getColumnIndex(ref.column()) < 0) {
                throw new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
            }
        });
    }

    /**
     * Split a partition key list on the commas that separate its keys, leaving the ones inside
     * a key's own parentheses or string literals alone: {@code (date_trunc('month', ts))} is one
     * key, not two, and splitting it blindly reported the column {@code ts))} as missing.
     */
    static List<String> splitTopLevel(String text) {
        List<String> parts = new java.util.ArrayList<>();
        int depth = 0;
        boolean inString = false;
        boolean inQuotedName = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (inString) {
                if (ch == '\'') inString = false;
            } else if (inQuotedName) {
                if (ch == '"') inQuotedName = false;
            } else if (ch == '\'') {
                inString = true;
            } else if (ch == '"') {
                inQuotedName = true;
            } else if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                parts.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        parts.add(current.toString());
        return parts;
    }

    /** Walk for a cast whose result depends on the session time zone. */
    private void rejectTimeZoneDependentCast(Object node, List<Column> columns) {
        AstWalk.forEach(node, n -> {
            if (!(n instanceof CastExpr)) return;
            CastExpr cast = (CastExpr) n;
            String target = cast.typeName() == null ? "" : cast.typeName().toLowerCase();
            boolean toLocalTime = target.startsWith("date") || target.startsWith("time")
                    || target.startsWith("timestamp");
            if (!toLocalTime || !(cast.expr() instanceof ColumnRef)) return;
            String colName = ((ColumnRef) cast.expr()).column();
            for (Column c : columns) {
                if (!c.getName().equalsIgnoreCase(colName)) continue;
                if (c.getType() == DataType.TIMESTAMPTZ || c.getType() == DataType.TIMETZ) {
                    throw new MemgresException(PARTITION_KEY_NOT_IMMUTABLE, "42P17");
                }
            }
        });
    }

    private void validateGeneratedColumns(List<ColumnDef> columnDefs, List<Column> columns) {
        Set<String> generatedColNames = new HashSet<>();
        Set<String> allColNames = new HashSet<>();
        for (ColumnDef def : columnDefs) {
            if (def.generatedExpr() != null) generatedColNames.add(def.name().toLowerCase());
            allColNames.add(def.name().toLowerCase());
        }
        for (ColumnDef def : columnDefs) {
            if (def.generatedExpr() != null) {
                List<String> referencedIdents = DdlExecutor.extractIdentifiers(def.generatedExpr());
                for (String ident : referencedIdents) {
                    String identLower = ident.toLowerCase();
                    if (!allColNames.contains(identLower)) {
                        if (!DdlExecutor.isSqlKeywordOrFunction(identLower)) {
                            throw new MemgresException("column \"" + ident + "\" does not exist", "42703");
                        }
                    } else if (generatedColNames.contains(identLower)) {
                        throw new MemgresException(
                                "cannot use generated column \"" + ident + "\" in column generation expression", "42P17");
                    }
                }
            }
        }
    }

    // ---- DROP TABLE ----

    QueryResult executeDropTable(DropTableStmt stmt) {
        // One DROP naming several tables drops them together, so a table in the list is not a
        // dependency that blocks another one in the same list — PostgreSQL takes the whole set.
        Set<String> together = new HashSet<>();
        together.add(RelationNamespace.bareName(stmt.name()).toLowerCase());
        if (stmt.additionalTables() != null) {
            for (String tableName : stmt.additionalTables()) {
                together.add(RelationNamespace.bareName(tableName).toLowerCase());
            }
        }
        dropSingleTable(stmt.schema(), stmt.name(), stmt.ifExists(), stmt.cascade(), together);
        if (stmt.additionalTables() != null) {
            for (String tableName : stmt.additionalTables()) {
                dropSingleTable(null, tableName, stmt.ifExists(), stmt.cascade(), together);
            }
        }
        return QueryResult.command(QueryResult.Type.DROP_TABLE, 0);
    }

    void dropSingleTable(String schemaHint, String name, boolean ifExists, boolean cascade) {
        dropSingleTable(schemaHint, name, ifExists, cascade, java.util.Collections.<String>emptySet());
    }

    void dropSingleTable(String schemaHint, String name, boolean ifExists, boolean cascade,
                         Set<String> together) {
        if (checkDropSchemaExists(schemaHint, ifExists)) return;
        String schemaName = schemaHint != null ? schemaHint : executor.defaultSchema();
        String tempSchema = executor.session != null ? executor.session.getTempSchemaName() : "pg_temp";
        Schema pgTemp = executor.database.getSchema(tempSchema);
        if (pgTemp != null && pgTemp.getTable(name) != null) {
            schemaName = tempSchema;
        }
        Schema schema = executor.database.getSchema(schemaName);
        if (schema != null) {
            Table droppedTable = schema.getTable(name);
            if (droppedTable == null) {
                // A view, sequence or index of this name is the wrong kind of relation, and
                // IF EXISTS does not make DROP TABLE the right command for it.
                RelationNamespace.requireKind(executor.database, schemaName, name,
                        RelationNamespace.TABLE);
                if (!ifExists) {
                    throw new MemgresException("table \"" + name + "\" does not exist", "42P01");
                }
                if (executor.session != null) {
                    executor.session.addNotice("NOTICE", "00000",
                            "table \"" + name + "\" does not exist, skipping", null);
                }
                return;
            }
            if (droppedTable != null) {
                if (!cascade) {
                    // Check FK dependencies: any table in any schema referencing this table
                    for (Schema s : executor.database.getSchemas().values()) {
                        for (Table otherTable : s.getTables().values()) {
                            if (otherTable == droppedTable) continue;
                            for (StoredConstraint sc : otherTable.getConstraints()) {
                                if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                                if (!sc.getReferencesTable().equalsIgnoreCase(name)) continue;
                                if (sc.getReferencesSchema() != null
                                        && !sc.getReferencesSchema().equalsIgnoreCase(schemaName)) continue;
                                throw new MemgresException(
                                        "cannot drop table " + name + " because other objects depend on it\n"
                                        + "  Detail: constraint " + sc.getName() + " on table " + otherTable.getName() + " depends on table " + name,
                                        "2BP01");
                            }
                        }
                    }
                    // An inheritance child reads its parent's definition, so the parent cannot go
                    // while the child is still there — and a reader of either would otherwise find
                    // a child whose inherited columns come from a table that no longer exists.
                    List<String> dependents = new ArrayList<>();
                    for (Table child : droppedTable.getChildren()) {
                        if (together.contains(child.getName().toLowerCase())) continue;
                        dependents.add("table " + child.getName()
                                + " depends on table " + visibleName(schemaName, name));
                    }
                    for (String v : ViewDependencies.directDependents(
                            executor.database, schemaName, name)) {
                        dependents.add("view " + RelationNamespace.bareName(v)
                                + " depends on table " + visibleName(schemaName, name));
                    }
                    if (!dependents.isEmpty()) {
                        MemgresException e = new MemgresException("cannot drop table "
                                + visibleName(schemaName, name)
                                + " because other objects depend on it", "2BP01");
                        e.setDetail(String.join("\n", dependents));
                        e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
                        throw e;
                    }
                    // Check function dependencies (%ROWTYPE, %TYPE, RETURNS table_type, SETOF table_type)
                    for (PgFunction fn : executor.database.getFunctions().values()) {
                        String body = fn.getBody();
                        String retType = fn.getReturnType();
                        boolean depends = false;
                        if (retType != null) {
                            String rt = retType.toLowerCase().replace("setof ", "").trim();
                            if (rt.equals(name.toLowerCase())) depends = true;
                        }
                        if (!depends && body != null) {
                            String lBody = body.toLowerCase();
                            if (lBody.contains(name.toLowerCase() + "%rowtype")
                                    || lBody.contains(name.toLowerCase() + ".")) {
                                // Check for %ROWTYPE or %TYPE references in DECLARE
                                java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                                        "\\b" + java.util.regex.Pattern.quote(name.toLowerCase()) + "\\s*(%rowtype|\\.[a-z_][a-z0-9_]*\\s*%type)",
                                        java.util.regex.Pattern.CASE_INSENSITIVE).matcher(body);
                                if (m.find()) depends = true;
                            }
                        }
                        // A BEGIN ATOMIC body is parsed at definition time and records a real
                        // dependency, unlike a string body which is only resolved when called
                        if (!depends && fn.isAtomicBody() && sqlFunctionDependsOnTable(fn, name)) {
                            depends = true;
                        }
                        if (depends) {
                            throw new MemgresException(
                                    "cannot drop table " + name + " because other objects depend on it\n"
                                    + "  Detail: function " + fn.getName() + " depends on table " + name,
                                    "2BP01");
                        }
                    }
                } else {
                    // CASCADE names what it took with it, so a script that meant to drop one
                    // table learns it dropped four.
                    List<String> cascaded = new ArrayList<>();
                    // CASCADE: remove FK constraints from tables referencing this table
                    for (Schema s : executor.database.getSchemas().values()) {
                        for (Table otherTable : s.getTables().values()) {
                            if (otherTable == droppedTable) continue;
                            List<String> fksToRemove = new ArrayList<>();
                            for (StoredConstraint sc : otherTable.getConstraints()) {
                                if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                                if (!sc.getReferencesTable().equalsIgnoreCase(name)) continue;
                                if (sc.getReferencesSchema() != null
                                        && !sc.getReferencesSchema().equalsIgnoreCase(schemaName)) continue;
                                fksToRemove.add(sc.getName());
                            }
                            for (String fkName : fksToRemove) {
                                cascaded.add("constraint " + fkName + " on table " + otherTable.getName());
                                otherTable.removeConstraint(fkName);
                            }
                        }
                    }
                    // An inheritance child goes with its parent, and so does everything reading
                    // either — all of it named in one notice, because that is what PostgreSQL
                    // reports for one DROP.
                    List<Table> descendants = inheritanceDescendants(droppedTable);
                    List<String> viewsToDrop = new ArrayList<>();
                    for (String v : ViewDependencies.cascadeDependents(executor.database, schemaName, name)) {
                        if (!viewsToDrop.contains(v)) viewsToDrop.add(v);
                    }
                    for (Table child : descendants) {
                        String childSchema = findSchemaNameOf(child, schemaName);
                        for (String v : ViewDependencies.cascadeDependents(
                                executor.database, childSchema, child.getName())) {
                            if (!viewsToDrop.contains(v)) viewsToDrop.add(v);
                        }
                    }
                    for (String v : viewsToDrop) {
                        cascaded.add("view " + RelationNamespace.bareName(v));
                        executor.database.removeView(v);
                    }
                    for (Table child : descendants) {
                        cascaded.add("table " + child.getName());
                    }
                    // The whole tree is accounted for here, so each child is dropped with no
                    // children of its own left — one DROP reports one cascade, not one per level.
                    droppedTable.getChildren().clear();
                    for (Table child : descendants) child.getChildren().clear();
                    for (int d = descendants.size() - 1; d >= 0; d--) {
                        Table child = descendants.get(d);
                        dropSingleTable(findSchemaNameOf(child, schemaName), child.getName(), true, true);
                    }
                    // CASCADE: also drop dependent functions (e.g., BEGIN ATOMIC bodies referencing this table)
                    List<String> funcsToDrop = new ArrayList<>();
                    for (PgFunction fn : executor.database.getFunctions().values()) {
                        if (sqlFunctionDependsOnTable(fn, name)) {
                            funcsToDrop.add(fn.getName());
                        }
                    }
                    for (String f : funcsToDrop) {
                        cascaded.add("function " + f + "()");
                        executor.database.removeFunction(f);
                    }
                    DdlObjectExecutor.noticeDropCascades(executor, cascaded);
                }
                // PG drops all partitions together with a partitioned parent (no CASCADE needed)
                if (!droppedTable.getPartitions().isEmpty()) {
                    for (Table part : new ArrayList<>(droppedTable.getPartitions())) {
                        String partSchemaName = findSchemaNameOf(part, schemaName);
                        dropSingleTable(partSchemaName, part.getName(), true, cascade);
                    }
                }
                // Dropping a partition must remove it from the parent's routing list so
                // routed INSERTs can't land in (and reads can't see) a dropped table
                Table partitionParent = droppedTable.getPartitionParent();
                if (partitionParent != null) {
                    partitionParent.removePartition(droppedTable);
                }
                // Dropping an inheritance child must unlink it from its parent for the same
                // reason: a parent still listing a child that is gone is a dependency on nothing,
                // and the parent could then never be dropped.
                Table inheritanceParent = droppedTable.getParentTable();
                if (inheritanceParent != null) {
                    inheritanceParent.removeChild(droppedTable);
                }
                executor.recordUndo(new Session.DropTableUndo(schemaName, name, droppedTable,
                        executor.database.getTriggersForTable(schemaName, name)));
            }
            schema.removeTable(name);
            // An index belongs to its table and goes with it. Leaving it registered kept the
            // name taken, so re-creating the table and its index reported a name clash for an
            // index over a relation that no longer existed.
            for (String indexName : new ArrayList<>(executor.database.getIndexColumns().keySet())) {
                String owner = executor.database.getIndexTable(indexName);
                if (owner != null && (owner.equalsIgnoreCase(schemaName + "." + name)
                        || owner.equalsIgnoreCase(name))) {
                    executor.database.removeIndex(indexName);
                }
            }
            // Remove only this schema's triggers: a same-named table elsewhere keeps its own
            executor.database.removeTriggersForTable(schemaName, name);
            // Drop implicit sequences owned by SERIAL/IDENTITY columns
            // Only drop sequences that were auto-created (SERIAL types or __identity__ defaults),
            // NOT independently-created sequences referenced via DEFAULT nextval(...)
            if (droppedTable != null) {
                for (Column col : droppedTable.getColumns()) {
                    String seqName = null;
                    if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL) {
                        // SERIAL columns: sequence name follows tablename_colname_seq pattern
                        String candidateSeq = name + "_" + col.getName() + "_seq";
                        String def = col.getDefaultValue();
                        if (def != null && def.contains("nextval('" + candidateSeq + "'")) {
                            seqName = candidateSeq;
                        } else {
                            seqName = candidateSeq; // still try even without default
                        }
                    }
                    if (col.getDefaultValue() != null && col.getDefaultValue().contains(":seq:")) {
                        // __identity__:...:seq:seqname — always owned
                        seqName = col.getDefaultValue().substring(col.getDefaultValue().indexOf(":seq:") + 5);
                    }
                    if (seqName != null && executor.database.hasSequence(seqName)) {
                        executor.database.removeSequence(seqName);
                        executor.database.removeObjectOwner("sequence:" + seqName);
                    }
                }
            }
            executor.database.removeObjectOwner("table:" + schemaName + "." + name);
            executor.database.removePrivilegesOnObject("TABLE", AstExecutor.privilegeKey(schemaName, name));
        } else if (!ifExists) {
            if ("pg_catalog".equalsIgnoreCase(schemaName) || "information_schema".equalsIgnoreCase(schemaName)) {
                throw new MemgresException("table \"" + name + "\" does not exist", "42P01");
            }
            throw new MemgresException("table \"" + name + "\" does not exist", "42P01");
        } else {
            if (executor.session != null) {
                executor.session.addNotice("NOTICE", "00000",
                        "table \"" + name + "\" does not exist, skipping", null);
            }
        }
    }

    /**
     * A DROP that names a schema of its own says where to look, and a schema that is not there
     * is what is missing — PostgreSQL reports 3F000 for the schema rather than 42P01 for an
     * object it never went looking for, and IF EXISTS skips on the schema by name. Returns true
     * when the caller should stop because the schema is absent and IF EXISTS was given.
     */
    boolean checkDropSchemaExists(String schemaHint, boolean ifExists) {
        if (schemaHint == null || executor.database.getSchema(schemaHint) != null) return false;
        String tempSchema = executor.session != null ? executor.session.getTempSchemaName() : null;
        if (schemaHint.equalsIgnoreCase("pg_temp")
                || (tempSchema != null && schemaHint.equalsIgnoreCase(tempSchema))) {
            return false;
        }
        if (!ifExists) {
            throw new MemgresException("schema \"" + schemaHint + "\" does not exist", "3F000");
        }
        if (executor.session != null) {
            executor.session.addNotice("NOTICE", "00000",
                    "schema \"" + schemaHint + "\" does not exist, skipping", null);
        }
        return true;
    }

    /** Every table below this one in the inheritance tree, parents before their own children. */
    private static List<Table> inheritanceDescendants(Table parent) {
        List<Table> out = new ArrayList<>();
        List<Table> queue = new ArrayList<>(parent.getChildren());
        while (!queue.isEmpty()) {
            Table next = queue.remove(0);
            if (out.contains(next)) continue;
            out.add(next);
            queue.addAll(next.getChildren());
        }
        return out;
    }

    /** Find the schema name that holds this exact table instance, falling back to the given name. */
    private String findSchemaNameOf(Table table, String fallback) {
        for (Map.Entry<String, Schema> entry : executor.database.getSchemas().entrySet()) {
            if (entry.getValue().getTable(table.getName()) == table) {
                return entry.getKey();
            }
        }
        return fallback;
    }

    /**
     * Check if a SQL-language function body references the given table name.
     * Covers RETURNS type, SETOF type, and FROM/INTO/UPDATE/DELETE table references in the body.
     */
    private boolean sqlFunctionDependsOnTable(PgFunction fn, String tableName) {
        String lName = tableName.toLowerCase();
        String retType = fn.getReturnType();
        if (retType != null) {
            String rt = retType.toLowerCase().replace("setof ", "").trim();
            if (rt.equals(lName)) return true;
        }
        String body = fn.getBody();
        if (body != null) {
            String lBody = body.toLowerCase();
            // Check for table reference: FROM table, INTO table, UPDATE table, etc.
            if (java.util.regex.Pattern.compile("\\b" + java.util.regex.Pattern.quote(lName) + "\\b",
                    java.util.regex.Pattern.CASE_INSENSITIVE).matcher(body).find()) {
                return true;
            }
        }
        return false;
    }

    // ---- TRUNCATE ----

    /**
     * Truncate every table with a foreign key onto {@code parentName}, then repeat for those
     * tables' own dependents. Each cascaded truncation records undo, fires the child's
     * statement-level TRUNCATE triggers and syncs the session's RR snapshot, exactly as if
     * the child had been named in the TRUNCATE itself.
     */
    private void truncateCascade(String parentName, String parentSchema, Set<Table> done) {
        for (Map.Entry<String, Schema> se : executor.database.getSchemas().entrySet()) {
            for (Table child : new ArrayList<>(se.getValue().getTables().values())) {
                if (done.contains(child)) continue;
                boolean references = false;
                for (StoredConstraint sc : child.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (!sc.getReferencesTable().equalsIgnoreCase(parentName)) continue;
                    if (sc.getReferencesSchema() != null
                            && !sc.getReferencesSchema().equalsIgnoreCase(parentSchema)) continue;
                    references = true;
                    break;
                }
                if (!references) continue;
                done.add(child);
                String childSchema = se.getKey();
                executor.recordUndo(new Session.TruncateUndo(childSchema, child.getName(),
                        new ArrayList<>(child.getRows()), child.getSerialCounter()));
                List<PgTrigger> childTriggers = executor.database.getTriggersForTable(child.getName());
                fireTruncateStatementTriggers(childTriggers, PgTrigger.Timing.BEFORE, child);
                child.deleteAll();
                if (executor.session != null) {
                    executor.session.clearRRSnapshotForTable(childSchema + "." + child.getName());
                }
                fireTruncateStatementTriggers(childTriggers, PgTrigger.Timing.AFTER, child);
                truncateCascade(child.getName(), childSchema, done);
            }
        }
    }

    /** Run the statement-level TRUNCATE triggers of one timing for a table. */
    private void fireTruncateStatementTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing, Table table) {
        for (PgTrigger trig : triggers) {
            if (trig.isDisabled() || trig.getEvent() != PgTrigger.Event.TRUNCATE
                    || trig.getTiming() != timing || !trig.isForEachStatement()) continue;
            PgFunction fn = executor.database.getFunction(trig.getFunctionName());
            if (fn != null) {
                new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session)
                        .executeTriggerFunction(fn, null, null, table, trig);
            }
        }
    }

    /** Resolve one name of a TRUNCATE list the same way the main loop does, without throwing. */
    private Table resolveTruncateTarget(String tableName) {
        String bareName = tableName;
        List<String> searchSchemas;
        if (tableName.contains(".")) {
            int dot = tableName.indexOf('.');
            searchSchemas = Cols.listOf(tableName.substring(0, dot));
            bareName = tableName.substring(dot + 1);
        } else {
            String defSchema = executor.defaultSchema();
            searchSchemas = defSchema.equals("public") ? Cols.listOf("public") : Cols.listOf(defSchema, "public");
        }
        for (String schemaName : searchSchemas) {
            Schema schema = executor.database.getSchema(schemaName);
            if (schema == null) continue;
            Table table = schema.getTable(bareName);
            if (table != null) return table;
        }
        return null;
    }

    QueryResult executeTruncate(TruncateStmt stmt) {
        int totalCount = 0;
        // PG lets one TRUNCATE name every table in a reference graph, and then nothing is left
        // dangling. Resolve the whole list first so a referencing table listed alongside its
        // parent does not block the parent, which is how fixture teardown clears related tables.
        Set<Table> alsoTruncating = Collections.newSetFromMap(new IdentityHashMap<Table, Boolean>());
        for (String name : stmt.tables()) {
            Table t = resolveTruncateTarget(name);
            if (t == null) continue;
            List<Table> tree = new ArrayList<>();
            DmlPartitionHelper.collectAllPartitionTables(t, tree);
            alsoTruncating.addAll(tree);
        }
        for (int tableIdx = 0; tableIdx < stmt.tables().size(); tableIdx++) {
            String tableName = stmt.tables().get(tableIdx);
            boolean truncateOnly = stmt.only(tableIdx);
            boolean found = false;
            // Check if table name is schema-qualified
            String explicitSchema = null;
            String bareName = tableName;
            if (tableName.contains(".")) {
                int dot = tableName.indexOf('.');
                explicitSchema = tableName.substring(0, dot);
                bareName = tableName.substring(dot + 1);
            }
            List<String> searchSchemas;
            if (explicitSchema != null) {
                // A schema named here is where the table is looked for, so a schema that is not
                // there is what is missing rather than the table
                checkDropSchemaExists(explicitSchema, false);
                searchSchemas = Cols.listOf(explicitSchema);
            } else {
                // Use search_path from session, falling back to "public"
                String defSchema = executor.defaultSchema();
                searchSchemas = defSchema.equals("public") ? Cols.listOf("public") : Cols.listOf(defSchema, "public");
            }
            for (String schemaName : searchSchemas) {
                Schema schema = executor.database.getSchema(schemaName);
                if (schema != null) {
                    Table table = schema.getTable(bareName);
                    if (table != null) {
                        found = true;
                        // C6: Enforce TRUNCATE privilege
                        executor.checkTablePrivilege("TRUNCATE", schemaName, bareName);
                        // TRUNCATE takes ACCESS EXCLUSIVE, so it waits behind any open reader
                        if (executor.session != null) {
                            executor.database.acquireTableLock(schemaName + "." + bareName,
                                    "AccessExclusiveLock", executor.session, false);
                        }
                        // PG rejects TRUNCATE ONLY on a partitioned table: rows live in the
                        // partitions, so ONLY (which excludes them) can never be honored
                        if (truncateOnly && table.getPartitionStrategy() != null) {
                            throw new MemgresException(
                                    "cannot truncate only a partitioned table\n"
                                    + "  Hint: Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly.",
                                    "42809");
                        }
                        // Check FK dependencies: tables referencing this one
                        if (!stmt.cascade()) {
                            for (Schema s : executor.database.getSchemas().values()) {
                                for (Table otherTable : s.getTables().values()) {
                                    if (otherTable == table) continue;
                                    if (alsoTruncating.contains(otherTable)) continue;
                                    for (StoredConstraint sc : otherTable.getConstraints()) {
                                        if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                                        if (sc.isNotEnforced()) continue;
                                        if (!sc.getReferencesTable().equalsIgnoreCase(bareName)) continue;
                                        if (sc.getReferencesSchema() != null
                                                && !sc.getReferencesSchema().equalsIgnoreCase(schemaName)) continue;
                                        // The constraint's existence is what blocks the TRUNCATE,
                                        // not whether the referencing table currently holds rows:
                                        // PG refuses an empty child just the same, so a migration
                                        // that works on an empty database has to work here too.
                                        MemgresException fkBlocked = new MemgresException(
                                                "cannot truncate a table referenced in a foreign key constraint",
                                                "0A000");
                                        fkBlocked.setDetail("Table \"" + otherTable.getName()
                                                + "\" references \"" + bareName + "\".");
                                        fkBlocked.setHint("Truncate table \"" + otherTable.getName()
                                                + "\" at the same time, or use TRUNCATE ... CASCADE.");
                                        throw fkBlocked;
                                    }
                                }
                            }
                        }
                        // A partitioned parent holds no rows itself: truncate the whole
                        // partition tree (parent + all partitions, recursively) like PG does
                        List<Table> truncateTargets = new ArrayList<>();
                        DmlPartitionHelper.collectAllPartitionTables(table, truncateTargets);
                        for (Table target : truncateTargets) {
                            String targetSchema = target == table ? schemaName : findSchemaNameOf(target, schemaName);
                            executor.recordUndo(new Session.TruncateUndo(targetSchema, target.getName(),
                                    new ArrayList<>(target.getRows()), target.getSerialCounter()));
                        }
                        // Fire BEFORE TRUNCATE statement-level triggers
                        List<PgTrigger> triggers = executor.database.getTriggersForTable(bareName);
                        for (PgTrigger trig : triggers) {
                            if (!trig.isDisabled() && trig.getEvent() == PgTrigger.Event.TRUNCATE
                                    && trig.getTiming() == PgTrigger.Timing.BEFORE && trig.isForEachStatement()) {
                                PgFunction fn = executor.database.getFunction(trig.getFunctionName());
                                if (fn != null) {
                                    new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session)
                                            .executeTriggerFunction(fn, null, null, table, trig);
                                }
                            }
                        }
                        for (Table target : truncateTargets) {
                            totalCount += target.deleteAll();
                            // C9: Sync RR snapshot — own TRUNCATE must be visible to itself
                            if (executor.session != null) {
                                String targetSchema = target == table ? schemaName : findSchemaNameOf(target, schemaName);
                                executor.session.clearRRSnapshotForTable(targetSchema + "." + target.getName());
                            }
                        }
                        // CASCADE: truncate dependent tables, recursively — PG treats each
                        // cascaded child as a full TRUNCATE (undo, triggers, snapshot sync)
                        if (stmt.cascade()) {
                            Set<Table> cascaded = Collections.newSetFromMap(new IdentityHashMap<Table, Boolean>());
                            cascaded.add(table);
                            for (Table target : truncateTargets) cascaded.add(target);
                            truncateCascade(bareName, schemaName, cascaded);
                        }
                        if (stmt.restartIdentity()) {
                            for (Table target : truncateTargets) {
                                target.resetSerialCounter(1);
                            }
                            // Also restart real sequences for SERIAL/IDENTITY columns
                            for (Column col : table.getColumns()) {
                                String seqName = null;
                                String def = col.getDefaultValue();
                                if (def != null && def.contains("nextval('")) {
                                    int q1 = def.indexOf('\'');
                                    int q2 = def.indexOf('\'', q1 + 1);
                                    if (q1 >= 0 && q2 > q1) seqName = def.substring(q1 + 1, q2);
                                } else if (def != null && def.contains(":seq:")) {
                                    seqName = def.substring(def.indexOf(":seq:") + 5);
                                }
                                if (seqName != null) {
                                    Sequence seq = executor.database.getSequence(seqName);
                                    if (seq != null) {
                                        // C11: Record undo so ROLLBACK restores the sequence
                                        executor.recordUndo(new Session.SequenceRestartUndo(
                                                seqName, seq.currValRaw(), seq.isCalled()));
                                        seq.restart();
                                    }
                                }
                            }
                        }
                        // Fire AFTER TRUNCATE statement-level triggers
                        for (PgTrigger trig : triggers) {
                            if (!trig.isDisabled() && trig.getEvent() == PgTrigger.Event.TRUNCATE
                                    && trig.getTiming() == PgTrigger.Timing.AFTER && trig.isForEachStatement()) {
                                PgFunction fn = executor.database.getFunction(trig.getFunctionName());
                                if (fn != null) {
                                    new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session)
                                            .executeTriggerFunction(fn, null, null, table, trig);
                                }
                            }
                        }
                        break;
                    }
                }
            }
            if (!found) {
                // A name that resolves to something which is not a table is a different
                // complaint from a name that resolves to nothing, and PostgreSQL says so.
                if (executor.database.hasView(bareName)) {
                    throw new MemgresException("\"" + bareName + "\" is not a table", "42809");
                }
                throw new MemgresException(
                        "relation \"" + bareName + "\" does not exist", "42P01");
            }
        }
        return QueryResult.command(QueryResult.Type.DELETE, 0);
    }

    // ---- CREATE TABLE AS / SELECT INTO ----

    QueryResult executeCreateTableAs(CreateTableAsStmt stmt) {
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.creationSchema();
        Schema schema = executor.database.getOrCreateSchema(schemaName);

        if (schema.getTable(stmt.name()) != null) {
            if (stmt.ifNotExists()) return QueryResult.command(QueryResult.Type.SELECT_INTO, 0);
            throw new MemgresException("relation \"" + stmt.name() + "\" already exists", "42P07");
        }

        QueryResult result = executor.executeStatement(stmt.query());
        // CREATE TABLE t (a, b) AS query renames the query's columns left to right. Fewer names
        // than columns is allowed and leaves the rest as the query named them; more is not.
        List<String> given = stmt.columnNames();
        if (given != null && given.size() > result.getColumns().size()) {
            throw new MemgresException("too many column names were specified", "42601");
        }
        List<Column> columns = new ArrayList<>();
        int givenIdx = 0;
        // The query's output names become the table's column names, so they have to satisfy the
        // same rules a column list does — a repeated or system name is rejected here, not later.
        Set<String> seenNames = new HashSet<>();
        for (Column srcCol : result.getColumns()) {
            String colName = given != null && givenIdx < given.size()
                    ? given.get(givenIdx) : srcCol.getName();
            givenIdx++;
            DdlDefinitionChecks.rejectSystemColumnName(colName);
            if (!seenNames.add(colName.toLowerCase())) {
                throw PgErrors.duplicateColumn(colName);
            }
            columns.add(new Column(colName, srcCol.getType(), true, false, null,
                    srcCol.getEnumTypeName(), srcCol.getPrecision(), srcCol.getScale(), null));
        }

        Table table = new Table(stmt.name(), columns);
        schema.addTable(table);
        // A relation created under a name that once carried rules starts clean: the pg_class row
        // that remembered them went with the relation that was dropped.
        executor.database.clearRuleHistory(stmt.name());
        executor.database.markUncommittedObject(table, executor.session);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));

        int rowCount = 0;
        if (stmt.withData()) {
            for (Object[] row : result.getRows()) {
                Object[] copy = row.clone();
                table.insertRow(copy);
                executor.recordUndo(new Session.InsertUndo(schemaName, table.getName(), copy));
                rowCount++;
            }
        }

        return QueryResult.command(QueryResult.Type.SELECT_INTO, rowCount);
    }
}
