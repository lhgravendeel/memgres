package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles CREATE TABLE, DROP TABLE, TRUNCATE, CREATE TABLE AS.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlTableExecutor {
    final DdlExecutor ddl;
    final AstExecutor executor;

    DdlTableExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- CREATE TABLE ----

    QueryResult executeCreateTable(CreateTableStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        if (stmt.temporary()) {
            schemaName = executor.session != null ? executor.session.getTempSchemaName() : "pg_temp";
        }
        if (stmt.schema() != null && executor.database.getSchema(stmt.schema()) == null) {
            throw new MemgresException("schema \"" + stmt.schema() + "\" does not exist", "3F000");
        }
        if ("pg_catalog".equalsIgnoreCase(schemaName) || "information_schema".equalsIgnoreCase(schemaName)) {
            throw new MemgresException("permission denied for schema " + schemaName, "42501");
        }
        Schema schema = executor.database.getOrCreateSchema(schemaName);

        if (schema.getTable(stmt.name()) != null) {
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
            for (String parentName : stmt.inherits()) {
                Table parent = executor.resolveTable(schemaName, parentName);
                parentTables.add(parent);
                for (Column col : parent.getColumns()) {
                    boolean exists = inheritedColumns.stream()
                            .anyMatch(c -> c.getName().equalsIgnoreCase(col.getName()));
                    if (!exists) inheritedColumns.add(col);
                }
            }
        }

        // Handle LIKE tables
        List<StoredConstraint> likeConstraints = new ArrayList<>();
        if (stmt.likeTables() != null) {
            for (String likeTableName : stmt.likeTables()) {
                Table likeTable = executor.resolveTable(schemaName, likeTableName);
                for (Column col : likeTable.getColumns()) {
                    boolean exists = inheritedColumns.stream()
                            .anyMatch(c -> c.getName().equalsIgnoreCase(col.getName()));
                    if (!exists) inheritedColumns.add(col);
                }
                for (StoredConstraint sc : likeTable.getConstraints()) {
                    likeConstraints.add(sc);
                }
            }
        }

        List<Column> columns = new ArrayList<>(inheritedColumns);
        Set<String> definedColumnNames = new HashSet<>();
        for (ColumnDef def : stmt.columns()) {
            if (!definedColumnNames.add(def.name().toLowerCase())) {
                throw new MemgresException("column \"" + def.name() + "\" specified more than once", "42701");
            }

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
                notNull = true;
                if (def.identityStart() != null || def.identityIncrement() != null) {
                    String seqName = stmt.name() + "_" + def.name() + "_seq";
                    Sequence seq = new Sequence(seqName, def.identityStart(), def.identityIncrement(), null, null);
                    executor.database.addSequence(seq);
                    executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                    if (dataType != DataType.BIGINT && dataType != DataType.INTEGER && dataType != DataType.SMALLINT) {
                        dataType = DataType.INTEGER;
                    }
                    defaultVal = "nextval('" + seqName + "')";
                } else {
                    if (dataType == DataType.INTEGER) dataType = DataType.SERIAL;
                    else if (dataType == DataType.BIGINT) dataType = DataType.BIGSERIAL;
                    else if (dataType == DataType.SMALLINT) dataType = DataType.SMALLSERIAL;
                    else dataType = DataType.SERIAL;
                    if ("ALWAYS".equalsIgnoreCase(def.identity())) {
                        defaultVal = "__identity__:always";
                    }
                }
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
                    if (def.defaultExpr() instanceof Literal) {
                        Literal lit = (Literal) def.defaultExpr();
                        String strVal = lit.value();
                        try {
                            new java.math.BigDecimal(strVal);
                        } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type " + dataType.getPgName()
                                    + ": \"" + strVal + "\"", "22P02");
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

            // Validate generated column expression
            if (def.generatedExpr() != null) {
                // PG rejects DEFAULT + GENERATED ALWAYS AS on same column
                if (def.defaultExpr() != null) {
                    throw new MemgresException("a generated column is not allowed to have a default value", "42601");
                }
                String genNorm = def.generatedExpr().toLowerCase().replaceAll("\\s+", "");
                if (genNorm.contains("now(") || genNorm.contains("random(") || genNorm.contains("clock_timestamp(")
                        || genNorm.contains("current_timestamp") || genNorm.contains("timeofday(")
                        || genNorm.contains("current_time") || genNorm.contains("current_date")) {
                    throw new MemgresException("generation expression is not immutable", "42P17");
                }
                if (genNorm.contains("select")) {
                    throw new MemgresException("cannot use subquery in column generation expression", "0A000");
                }
            }

            Column col = new Column(def.name(), dataType, !notNull, def.primaryKey(), defaultVal,
                    enumTypeName, def.precision(), def.scale(), def.generatedExpr(), def.generatedVirtual(),
                    domainTypeName, compositeTypeName, arrayElementType);
            if (def.defaultExpr() != null) {
                col.setParsedDefaultExpr(def.defaultExpr());
            }
            if (existingIdx >= 0) {
                columns.set(existingIdx, col);
            } else {
                columns.add(col);
            }
        }

        // Validate generated column expressions
        validateGeneratedColumns(stmt.columns(), columns);

        Table table = new Table(stmt.name(), columns);

        // Set up inheritance links
        for (Table parent : parentTables) {
            table.setParentTable(parent);
            parent.addChild(table);
        }

        // Set up partitioning
        if (stmt.partitionBy() != null) {
            table.setPartitionStrategy(stmt.partitionBy());
            String partCol = stmt.partitionColumn();
            if (partCol != null && table.getColumnIndex(partCol) < 0) {
                throw new MemgresException("column \"" + partCol + "\" named in partition key does not exist", "42703");
            }
            table.setPartitionColumn(partCol);
        }

        schema.addTable(table);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));

        // ON COMMIT actions for temp tables
        if ("DROP".equals(stmt.onCommitAction()) && executor.session != null) {
            if (executor.session.isInTransaction()) {
                executor.session.registerOnCommitDrop(schemaName, stmt.name());
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
                table.addConstraint(StoredConstraint.primaryKey(
                        stmt.name() + "_pkey", Cols.listOf(def.name())));
            }
            if (def.unique()) {
                table.addConstraint(StoredConstraint.unique(
                        stmt.name() + "_" + def.name() + "_key", Cols.listOf(def.name())));
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
                StoredConstraint sc = ddl.convertTableConstraint(stmt.name(), tc);
                if (sc != null) {
                    table.addConstraint(sc);
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                        for (String colName : sc.getColumns()) {
                            table.alterColumnNullable(colName, false);
                        }
                    }
                }
            }
        }

        // Add constraints from LIKE tables
        for (StoredConstraint likeSc : likeConstraints) {
            table.addConstraint(likeSc);
        }

        executor.database.setObjectOwner("table:" + schemaName + "." + stmt.name(), executor.sessionUser());
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    private QueryResult createPartitionOfTable(CreateTableStmt stmt, Schema schema, String schemaName) {
        Table parent = executor.resolveTable(schemaName, stmt.partitionOfParent());
        Table partition = new Table(stmt.name(), new ArrayList<>(parent.getColumns()));
        partition.setPartitionParent(parent);
        parent.addPartition(partition);

        if (stmt.partitionBounds() != null && !stmt.partitionBounds().isEmpty()) {
            applyPartitionBounds(partition, parent, stmt.partitionBounds(), stmt.name());
        }

        if (stmt.partitionBy() != null) {
            partition.setPartitionStrategy(stmt.partitionBy());
            partition.setPartitionColumn(stmt.partitionColumn());
        }

        schema.addTable(partition);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * Apply partition bounds (FROM/IN/HASH/DEFAULT) to a partition, validating against siblings.
     * Shared between CREATE TABLE PARTITION OF and ALTER TABLE ATTACH PARTITION.
     */
    void applyPartitionBounds(Table partition, Table parent, List<String> bounds, String partitionName) {
        String boundType = bounds.get(0);
        if (boundType.equals("DEFAULT")) {
            partition.setDefaultPartition(true);
        } else if (boundType.equals("FROM") && bounds.size() >= 4) {
            Object newLow = DdlExecutor.parseBoundValue(bounds.get(1));
            Object newHigh = DdlExecutor.parseBoundValue(bounds.get(3));
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
            List<Object> values = new ArrayList<>();
            for (int i = 1; i < bounds.size(); i++) {
                values.add(DdlExecutor.parseBoundValue(bounds.get(i)));
            }
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                List<Object> existingVals = existingPart.getPartitionValues();
                if (existingVals != null) {
                    for (Object v : values) {
                        if (existingVals.stream().anyMatch(ev -> String.valueOf(ev).equals(String.valueOf(v)))) {
                            throw new MemgresException("partition \"" + partitionName
                                    + "\" would overlap partition \"" + existingPart.getName() + "\"", "42P17");
                        }
                    }
                }
            }
            partition.setPartitionValues(values);
        } else if (boundType.equals("HASH") && bounds.size() >= 3) {
            int modulus = Integer.parseInt(bounds.get(1));
            int remainder = Integer.parseInt(bounds.get(2));
            if (modulus <= 0) {
                throw new MemgresException("modulus for hash partition must be a positive integer", "22023");
            }
            if (remainder < 0 || remainder >= modulus) {
                throw new MemgresException("remainder for hash partition must be less than modulus", "42P16");
            }
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                if (existingPart.getPartitionModulus() != null
                        && existingPart.getPartitionModulus() != modulus) {
                    throw new MemgresException("every hash partition modulus must be a factor of the largest modulus", "42P16");
                }
            }
            for (Table existingPart : parent.getPartitions()) {
                if (existingPart == partition) continue;
                if (existingPart.getPartitionModulus() != null
                        && existingPart.getPartitionModulus() == modulus
                        && existingPart.getPartitionRemainder() != null
                        && existingPart.getPartitionRemainder() == remainder) {
                    throw new MemgresException("partition \"" + partitionName
                            + "\" would overlap partition \"" + existingPart.getName() + "\"", "42P16");
                }
            }
            partition.setPartitionHash(modulus, remainder);
        }
    }

    private void addColumnForeignKey(Table table, ColumnDef def, String schemaName, String tableName) {
        String refTableName = def.referencesTable();
        Table refTable = null;
        if (refTableName.contains(".")) {
            String[] parts = refTableName.split("\\.", 2);
            try { refTable = executor.resolveTable(parts[0], parts[1]); } catch (MemgresException ignored) {}
        }
        if (refTable == null) {
            try { refTable = executor.resolveTable(schemaName, refTableName); } catch (MemgresException ignored) {}
        }
        if (refTable == null) refTable = ddl.resolveTableOrNull(refTableName);
        if (refTable == null) {
            throw new MemgresException("relation \"" + refTableName + "\" does not exist", "42P01");
        }
        if (def.referencesColumn() != null && refTable.getColumnIndex(def.referencesColumn()) < 0) {
            throw new MemgresException("column \"" + def.referencesColumn() + "\" referenced in foreign key constraint does not exist", "42703");
        }
        List<String> refCols = def.referencesColumn() != null
                ? Cols.listOf(def.referencesColumn()) : Cols.listOf();
        StoredConstraint fk = StoredConstraint.foreignKey(
                tableName + "_" + def.name() + "_fkey",
                Cols.listOf(def.name()), def.referencesTable(), refCols,
                StoredConstraint.parseFkAction(def.refOnDelete()),
                StoredConstraint.parseFkAction(def.refOnUpdate()));
        if (def.deferrable()) {
            fk.setDeferrable(true);
            fk.setInitiallyDeferred(def.initiallyDeferred());
        }
        if (def.notEnforced()) fk.setNotEnforced(true);
        table.addConstraint(fk);
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
        dropSingleTable(stmt.schema(), stmt.name(), stmt.ifExists(), stmt.cascade());
        if (stmt.additionalTables() != null) {
            for (String tableName : stmt.additionalTables()) {
                dropSingleTable(null, tableName, stmt.ifExists(), stmt.cascade());
            }
        }
        return QueryResult.command(QueryResult.Type.DROP_TABLE, 0);
    }

    void dropSingleTable(String schemaHint, String name, boolean ifExists, boolean cascade) {
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
                if (!ifExists) {
                    if (executor.database.hasView(name)) {
                        throw new MemgresException("\"" + name + "\" is not a table", "42809");
                    }
                    if (executor.database.hasSequence(name)) {
                        throw new MemgresException("\"" + name + "\" is not a table", "42809");
                    }
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
                    for (Database.ViewDef view : executor.database.getViews().values()) {
                        String viewSql = view.query() != null ? view.query().toString().toLowerCase() : "";
                        if (viewSql.contains(name.toLowerCase())) {
                            throw new MemgresException("cannot drop table " + name + " because other objects depend on it", "2BP01");
                        }
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
                        if (depends) {
                            throw new MemgresException(
                                    "cannot drop table " + name + " because other objects depend on it\n"
                                    + "  Detail: function " + fn.getName() + " depends on table " + name,
                                    "2BP01");
                        }
                    }
                } else {
                    List<String> viewsToDrop = new ArrayList<>();
                    for (Map.Entry<String, Database.ViewDef> entry : executor.database.getViews().entrySet()) {
                        String viewSql = entry.getValue().query() != null ? entry.getValue().query().toString().toLowerCase() : "";
                        if (viewSql.contains(name.toLowerCase())) {
                            viewsToDrop.add(entry.getKey());
                        }
                    }
                    for (String v : viewsToDrop) executor.database.removeView(v);
                }
                executor.recordUndo(new Session.DropTableUndo(schemaName, name, droppedTable));
            }
            schema.removeTable(name);
            executor.database.removeObjectOwner("table:" + schemaName + "." + name);
            executor.database.removePrivilegesOnObject("TABLE", name);
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

    // ---- TRUNCATE ----

    QueryResult executeTruncate(TruncateStmt stmt) {
        int totalCount = 0;
        for (String tableName : stmt.tables()) {
            boolean found = false;
            for (String schemaName : Cols.listOf("public")) {
                Schema schema = executor.database.getSchema(schemaName);
                if (schema != null) {
                    Table table = schema.getTable(tableName);
                    if (table != null) {
                        found = true;
                        List<Object[]> oldRows = new ArrayList<>(table.getRows());
                        executor.recordUndo(new Session.TruncateUndo(schemaName, tableName, oldRows, table.getSerialCounter()));
                        totalCount += table.deleteAll();
                        if (stmt.restartIdentity()) {
                            table.resetSerialCounter(1);
                        }
                        break;
                    }
                }
            }
            if (!found) {
                throw new MemgresException("Table not found: " + tableName);
            }
        }
        return QueryResult.command(QueryResult.Type.DELETE, 0);
    }

    // ---- CREATE TABLE AS / SELECT INTO ----

    QueryResult executeCreateTableAs(CreateTableAsStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        Schema schema = executor.database.getOrCreateSchema(schemaName);

        if (schema.getTable(stmt.name()) != null) {
            if (stmt.ifNotExists()) return QueryResult.command(QueryResult.Type.SELECT_INTO, 0);
            throw new MemgresException("relation \"" + stmt.name() + "\" already exists", "42P07");
        }

        QueryResult result = executor.executeStatement(stmt.query());
        List<Column> columns = new ArrayList<>();
        for (Column srcCol : result.getColumns()) {
            columns.add(new Column(srcCol.getName(), srcCol.getType(), true, false, null,
                    srcCol.getEnumTypeName(), srcCol.getPrecision(), srcCol.getScale(), null));
        }

        Table table = new Table(stmt.name(), columns);
        schema.addTable(table);
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
