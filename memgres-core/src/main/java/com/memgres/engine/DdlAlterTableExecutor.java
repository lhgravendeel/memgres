package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles ALTER TABLE execution.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlAlterTableExecutor {
    private final DdlExecutor ddl;
    private final AstExecutor executor;

    DdlAlterTableExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    QueryResult executeAlterTable(AlterTableStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        Table table;
        try {
            table = executor.resolveTable(schemaName, stmt.table());
        } catch (MemgresException e) {
            if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
            throw e;
        }

        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            table = executeAction(action, table, stmt, schemaName);
        }

        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    private Table executeAction(AlterTableStmt.AlterAction action, Table table,
                                 AlterTableStmt stmt, String schemaName) {
        if (action instanceof AlterTableStmt.AddColumn) {
            AlterTableStmt.AddColumn addCol = (AlterTableStmt.AddColumn) action;
            executeAddColumn(addCol, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.DropColumn) {
            AlterTableStmt.DropColumn dropCol = (AlterTableStmt.DropColumn) action;
            executeDropColumn(dropCol, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.RenameColumn) {
            AlterTableStmt.RenameColumn rename = (AlterTableStmt.RenameColumn) action;
            if (table.getColumnIndex(rename.newName()) >= 0) {
                throw new MemgresException("column \"" + rename.newName() + "\" of relation \"" + stmt.table() + "\" already exists", "42701");
            }
            table.renameColumn(rename.oldName(), rename.newName());
            executor.recordUndo(new Session.RenameColumnUndo(schemaName, stmt.table(), rename.newName(), rename.oldName()));
        } else if (action instanceof AlterTableStmt.RenameTable) {
            AlterTableStmt.RenameTable rename = (AlterTableStmt.RenameTable) action;
            table = executeRenameTable(rename, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.OwnerTo) {
            AlterTableStmt.OwnerTo ownerTo = (AlterTableStmt.OwnerTo) action;
            String newOwner = ddl.resolveOwnerName(ownerTo.newOwner());
            if (!executor.database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            executor.database.setObjectOwner("table:" + schemaName + "." + stmt.table(), newOwner);
        } else if (action instanceof AlterTableStmt.AlterColumn) {
            AlterTableStmt.AlterColumn alterCol = (AlterTableStmt.AlterColumn) action;
            executeAlterColumn(alterCol, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.AddConstraint) {
            AlterTableStmt.AddConstraint addConstraint = (AlterTableStmt.AddConstraint) action;
            executeAddConstraint(addConstraint, table, stmt);
        } else if (action instanceof AlterTableStmt.ValidateConstraint) {
            AlterTableStmt.ValidateConstraint vc = (AlterTableStmt.ValidateConstraint) action;
            executeValidateConstraint(vc, table, stmt);
        } else if (action instanceof AlterTableStmt.DropConstraint) {
            AlterTableStmt.DropConstraint dropConstraint = (AlterTableStmt.DropConstraint) action;
            if (!dropConstraint.ifExists() && table.getConstraint(dropConstraint.name()) == null) {
                throw new MemgresException("constraint \"" + dropConstraint.name() + "\" of relation \"" + stmt.table() + "\" does not exist", "42704");
            }
            table.removeConstraint(dropConstraint.name());
        } else if (action instanceof AlterTableStmt.EnableRls) {
            table.setRlsEnabled(true);
        } else if (action instanceof AlterTableStmt.DisableRls) {
            table.setRlsEnabled(false);
        } else if (action instanceof AlterTableStmt.ForceRls) {
            table.setRlsForced(true);
        } else if (action instanceof AlterTableStmt.NoForceRls) {
            table.setRlsForced(false);
        } else if (action instanceof AlterTableStmt.AttachPartition) {
            AlterTableStmt.AttachPartition attach = (AlterTableStmt.AttachPartition) action;
            executeAttachPartition(attach, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.DetachPartition) {
            AlterTableStmt.DetachPartition detach = (AlterTableStmt.DetachPartition) action;
            String detachSchemaName = detach.partitionSchema() != null ? detach.partitionSchema() : schemaName;
            Table partition = executor.resolveTable(detachSchemaName, detach.partitionName());
            table.removePartition(partition);
            partition.setPartitionParent(null);
            partition.setPartitionValues(null);
            partition.setDefaultPartition(false);
        } else if (action instanceof AlterTableStmt.RenameConstraint) {
            AlterTableStmt.RenameConstraint renameConstraint = (AlterTableStmt.RenameConstraint) action;
            StoredConstraint oldConstraint = table.getConstraint(renameConstraint.oldName());
            if (oldConstraint == null) {
                throw new MemgresException("constraint \"" + renameConstraint.oldName() + "\" does not exist");
            }
            table.removeConstraint(renameConstraint.oldName());
            StoredConstraint newConstraint = new StoredConstraint(
                    renameConstraint.newName(), oldConstraint.getType(), oldConstraint.getColumns(),
                    oldConstraint.getCheckExpr(), oldConstraint.getReferencesTable(),
                    oldConstraint.getReferencesColumns(), oldConstraint.getOnDelete(), oldConstraint.getOnUpdate());
            table.addConstraint(newConstraint);
        } else if (action instanceof AlterTableStmt.AlterConstraintEnforced) {
            AlterTableStmt.AlterConstraintEnforced ace = (AlterTableStmt.AlterConstraintEnforced) action;
            StoredConstraint sc = table.getConstraint(ace.constraintName());
            if (sc == null) {
                throw new MemgresException("constraint \"" + ace.constraintName() + "\" of relation \"" + stmt.table() + "\" does not exist", "42704");
            }
            sc.setNotEnforced(ace.notEnforced());
        } else if (action instanceof AlterTableStmt.SetSchema) {
            AlterTableStmt.SetSchema setSchema = (AlterTableStmt.SetSchema) action;
            Schema oldSchema = executor.database.getSchema(schemaName);
            Schema newSchema = executor.database.getOrCreateSchema(setSchema.newSchema());
            oldSchema.removeTable(stmt.table());
            newSchema.addTable(table);
        } else if (action instanceof AlterTableStmt.Inherit) {
            AlterTableStmt.Inherit inherit = (AlterTableStmt.Inherit) action;
            Table parentTable = executor.resolveTable(schemaName, inherit.parentTable());
            table.setParentTable(parentTable);
            parentTable.addChild(table);
        } else if (action instanceof AlterTableStmt.NoInherit) {
            AlterTableStmt.NoInherit noInherit = (AlterTableStmt.NoInherit) action;
            Table parentTable = executor.resolveTable(schemaName, noInherit.parentTable());
            parentTable.removeChild(table);
            table.setParentTable(null);
        } else if (action instanceof AlterTableStmt.SetStorageParams) {
            // no-op
        }
        return table;
    }

    private void executeAddColumn(AlterTableStmt.AddColumn addCol, Table table,
                                   AlterTableStmt stmt, String schemaName) {
        ColumnDef def = addCol.column();
        if (table.getColumnIndex(def.name()) >= 0) {
            if (addCol.ifNotExists()) return;
            throw new MemgresException("column \"" + def.name() + "\" of relation \"" + stmt.table() + "\" already exists", "42701");
        }

        DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(def.typeName(), null);
        DataType dt = resolved.dataType();
        String enumTypeName = resolved.enumTypeName();
        String domainTypeName = resolved.domainTypeName();

        String defaultVal = def.defaultExpr() != null ? DdlExecutor.exprToDefaultString(def.defaultExpr()) : null;
        String genExpr = def.generatedExpr();

        // Validate generated column expression references valid columns
        if (genExpr != null) {
            try {
                Expression genParsed = com.memgres.engine.parser.Parser.parseExpression(genExpr);
                ddl.validateExprColumnRefs(genParsed, table, def.name());
            } catch (MemgresException me) {
                throw me;
            } catch (Exception ignored) {}
        }

        if (def.notNull() && defaultVal == null && genExpr == null && !table.getRows().isEmpty()) {
            throw new MemgresException("column \"" + def.name()
                    + "\" of relation \"" + stmt.table() + "\" contains null values", "23502");
        }

        Column col = new Column(def.name(), dt, !def.notNull(), def.primaryKey(), defaultVal,
                enumTypeName, def.precision(), def.scale(), genExpr, def.generatedVirtual(), domainTypeName, null, null);
        Object evaluatedDefault = defaultVal != null ? executor.evaluateDefault(defaultVal, dt) : null;

        // Validate default type compatibility
        if (defaultVal != null && evaluatedDefault != null) {
            switch (dt) {
                case INTEGER:
                case BIGINT:
                case SMALLINT:
                case REAL:
                case DOUBLE_PRECISION:
                case NUMERIC: {
                    if (evaluatedDefault instanceof String) {
                        String s = (String) evaluatedDefault;
                        try { new java.math.BigDecimal(s.trim()); }
                        catch (NumberFormatException nfe) {
                            throw new MemgresException("invalid input syntax for type "
                                    + dt.name().toLowerCase() + ": \"" + s + "\"", "22P02");
                        }
                    }
                    break;
                }
                default: {
                    break;
                }
            }
        }

        // Check default against CHECK constraint
        if (evaluatedDefault != null && def.checkConstraintExpr() != null) {
            try {
                Table tempTable = new Table(table.getName(), new ArrayList<>(table.getColumns()));
                tempTable.addColumn(new Column(def.name(), dt, !def.notNull(), def.primaryKey(), null), null);
                Object[] tempRow = new Object[tempTable.getColumns().size()];
                int tempIdx = tempTable.getColumnIndex(def.name());
                if (tempIdx >= 0) tempRow[tempIdx] = evaluatedDefault;
                RowContext tempCtx = new RowContext(tempTable, table.getName(), tempRow);
                Object checkResult = executor.evalExpr(def.checkConstraintExpr(), tempCtx);
                if (!executor.isTruthy(checkResult)) {
                    throw new MemgresException("check constraint \"" + def.name()
                            + "_check\" is violated by some row", "23514");
                }
            } catch (MemgresException me) {
                if ("23514".equals(me.getSqlState())) throw me;
            } catch (Exception ignored) {}
        }

        table.addColumn(col, evaluatedDefault);
        executor.recordUndo(new Session.AddColumnUndo(schemaName, stmt.table(), def.name()));

        // Compute STORED generated column values for existing rows (VIRTUAL columns are computed on read)
        if (genExpr != null && !def.generatedVirtual()) {
            int colIdx = table.getColumnIndex(def.name());
            if (colIdx >= 0) {
                for (Object[] row : table.getRows()) {
                    row[colIdx] = executor.dmlExecutor.evalGeneratedColumn(table, row, col);
                }
            }
        }
    }

    private void executeDropColumn(AlterTableStmt.DropColumn dropCol, Table table,
                                    AlterTableStmt stmt, String schemaName) {
        if (dropCol.ifExists() && table.getColumnIndex(dropCol.column()) < 0) {
            return;
        }
        int colIdx = table.getColumnIndex(dropCol.column());
        if (colIdx < 0) {
            throw new MemgresException("column \"" + dropCol.column()
                    + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
        }
        if (!dropCol.cascade()) {
            String colName = dropCol.column().toLowerCase();
            for (Map.Entry<String, Database.ViewDef> viewEntry : executor.database.getViews().entrySet()) {
                String viewSql = viewEntry.getValue().query() != null ? viewEntry.getValue().query().toString() : "";
                if (viewSql.toLowerCase().contains(stmt.table().toLowerCase())
                        && viewSql.toLowerCase().contains(colName)) {
                    throw new MemgresException("cannot drop column \"" + dropCol.column()
                            + "\" of table \"" + stmt.table()
                            + "\" because view \"" + viewEntry.getKey() + "\" depends on it", "42P16");
                }
            }
        }
        Column droppedCol = table.getColumns().get(colIdx);
        List<Object> colValues = new ArrayList<>();
        for (Object[] row : table.getRows()) {
            colValues.add(row[colIdx]);
        }
        executor.recordUndo(new Session.DropColumnUndo(schemaName, stmt.table(), droppedCol, colIdx, colValues));
        table.removeColumn(dropCol.column());
    }

    private Table executeRenameTable(AlterTableStmt.RenameTable rename, Table table,
                                      AlterTableStmt stmt, String schemaName) {
        if (rename.newName() == null) return table;
        Schema schema = executor.database.getSchema(schemaName);
        if (schema.getTable(rename.newName()) != null) {
            throw new MemgresException("relation \"" + rename.newName() + "\" already exists", "42P07");
        }
        schema.removeTable(stmt.table());
        Table renamed = new Table(rename.newName(), table.getColumns());
        for (Object[] row : table.getRows()) renamed.insertRow(row);
        for (StoredConstraint sc : table.getConstraints()) renamed.addConstraint(sc);
        if (table.getPartitionStrategy() != null) {
            renamed.setPartitionStrategy(table.getPartitionStrategy());
            renamed.setPartitionColumn(table.getPartitionColumn());
        }
        for (Table partition : table.getPartitions()) {
            renamed.addPartition(partition);
            partition.setPartitionParent(renamed);
        }
        if (table.getParentTable() != null) {
            renamed.setParentTable(table.getParentTable());
        }
        renamed.setRlsEnabled(table.isRlsEnabled());
        schema.addTable(renamed);
        return renamed;
    }

    private void executeAlterColumn(AlterTableStmt.AlterColumn alterCol, Table table,
                                     AlterTableStmt stmt, String schemaName) {
        if (alterCol.action() instanceof AlterTableStmt.SetType) {
            AlterTableStmt.SetType setType = (AlterTableStmt.SetType) alterCol.action();
            executeSetType(alterCol, setType, table, stmt, schemaName);
        } else if (alterCol.action() instanceof AlterTableStmt.SetDefault) {
            AlterTableStmt.SetDefault setDefault = (AlterTableStmt.SetDefault) alterCol.action();
            executeSetDefault(alterCol, setDefault, table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.DropDefault) {
            table.alterColumnDefault(alterCol.column(), null);
        } else if (alterCol.action() instanceof AlterTableStmt.SetNotNull) {
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx >= 0) {
                for (Object[] row : table.getRows()) {
                    if (row[colIdx] == null) {
                        throw new MemgresException("column \"" + alterCol.column() + "\" of relation \""
                                + stmt.table() + "\" contains null values", "23502");
                    }
                }
            }
            table.alterColumnNullable(alterCol.column(), false);
        } else if (alterCol.action() instanceof AlterTableStmt.DropNotNull) {
            table.alterColumnNullable(alterCol.column(), true);
        } else if (alterCol.action() instanceof AlterTableStmt.ColumnNoOp) {
            // no-op
        }
    }

    private void executeSetType(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetType setType,
                                 Table table, AlterTableStmt stmt, String schemaName) {
        String baseType = setType.typeName().replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        DataType dt = DataType.fromPgName(baseType);
        if (dt == null) {
            if (executor.database.isCustomEnum(baseType)) {
                dt = DataType.ENUM;
            } else {
                throw new MemgresException("type \"" + baseType + "\" does not exist", "42704");
            }
        }
        int colIdx = table.getColumnIndex(alterCol.column());
        if (colIdx < 0) {
            throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
        }
        DataType currentType = table.getColumns().get(colIdx).getType();
        if (setType.usingExpr() == null && currentType != null && dt != null && currentType != dt) {
            TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
            TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
            if (fromCat != toCat && fromCat != TypeCoercion.TypeCategory.STRING && toCat != TypeCoercion.TypeCategory.STRING) {
                throw new MemgresException("column \"" + alterCol.column() + "\" cannot be cast automatically to type " + dt.getPgName(), "42804");
            }
        }
        // Check for view dependencies
        for (Database.ViewDef view : executor.database.getViews().values()) {
            String viewSql = view.sourceSQL() != null ? view.sourceSQL().toLowerCase()
                    : (view.query() != null ? view.query().toString().toLowerCase() : "");
            String tblPattern = "\\b" + java.util.regex.Pattern.quote(stmt.table().toLowerCase()) + "\\b";
            if (java.util.regex.Pattern.compile(tblPattern).matcher(viewSql).find()) {
                boolean usesWildcard = viewSql.contains("*") || viewSql.contains("wildcard");
                String colPattern = "\\b" + java.util.regex.Pattern.quote(alterCol.column().toLowerCase()) + "\\b";
                if (usesWildcard || java.util.regex.Pattern.compile(colPattern).matcher(viewSql).find()) {
                    throw new MemgresException("cannot alter type of a column used by a view or rule", "0A000");
                }
            }
        }
        // Check for index dependencies
        if (currentType != dt && setType.usingExpr() == null) {
            String colNameLower = alterCol.column().toLowerCase();
            String checkTable = schemaName + "." + stmt.table();
            for (Map.Entry<String, java.util.List<String>> idxEntry : executor.database.getIndexColumns().entrySet()) {
                java.util.List<String> idxCols = idxEntry.getValue();
                String indexMetaTable = executor.database.getIndexTable(idxEntry.getKey());
                if (indexMetaTable != null && indexMetaTable.equalsIgnoreCase(checkTable)
                        && idxCols != null && idxCols.stream().anyMatch(c -> c.equalsIgnoreCase(colNameLower))) {
                    TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
                    TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
                    if (fromCat != toCat) {
                        throw new MemgresException(
                                "operator class for access method \"btree\" does not accept data type "
                                        + dt.getPgName(), "42804");
                    }
                }
            }
        }
        int convIdx = table.getColumnIndex(alterCol.column());
        if (setType.usingExpr() != null) {
            Object[] convertedValues = new Object[table.getRows().size()];
            for (int ri = 0; ri < table.getRows().size(); ri++) {
                Object[] row = table.getRows().get(ri);
                RowContext ctx = new RowContext(table, null, row);
                convertedValues[ri] = executor.evalExpr(setType.usingExpr(), ctx);
            }
            table.alterColumnType(alterCol.column(), dt);
            Column newCol = table.getColumns().get(convIdx);
            for (int ri = 0; ri < table.getRows().size(); ri++) {
                Object[] row = table.getRows().get(ri);
                row[convIdx] = convertedValues[ri] != null
                        ? TypeCoercion.coerceForStorage(convertedValues[ri], newCol)
                        : null;
            }
        } else {
            table.alterColumnType(alterCol.column(), dt);
            Column newCol = table.getColumns().get(convIdx);
            for (Object[] row : table.getRows()) {
                if (row[convIdx] != null) {
                    row[convIdx] = TypeCoercion.coerceForStorage(row[convIdx], newCol);
                }
            }
        }
    }

    private void executeSetDefault(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetDefault setDefault,
                                    Table table, AlterTableStmt stmt) {
        String defaultVal = DdlExecutor.exprToDefaultString(setDefault.expr());

        if (defaultVal.contains("__set_increment__")) {
            handleSetIncrement(alterCol.column(), defaultVal, table);
        } else if (defaultVal.contains("__restart__")) {
            handleRestart(alterCol.column(), defaultVal, table);
        } else if (defaultVal.contains("__identity__")) {
            handleIdentity(alterCol.column(), defaultVal, table, stmt);
        } else {
            table.alterColumnDefault(alterCol.column(), defaultVal);
        }
    }

    private void handleSetIncrement(String column, String defaultVal, Table table) {
        String marker = DdlExecutor.extractMarker(defaultVal);
        int colIdx = table.getColumnIndex(column);
        Column col = colIdx >= 0 ? table.getColumns().get(colIdx) : null;
        if (col != null && col.getDefaultValue() != null && col.getDefaultValue().contains("nextval")) {
            Sequence seq = findBackingSequence(col);
            if (seq != null) {
                long inc = Long.parseLong(marker.substring(marker.indexOf(":") + 1));
                seq.setIncrementBy(inc);
            }
        }
    }

    private void handleRestart(String column, String defaultVal, Table table) {
        String marker = DdlExecutor.extractMarker(defaultVal);
        int colIdx = table.getColumnIndex(column);
        Column col = colIdx >= 0 ? table.getColumns().get(colIdx) : null;
        boolean restarted = false;
        if (col != null && col.getDefaultValue() != null && col.getDefaultValue().contains("nextval")) {
            Sequence seq = findBackingSequence(col);
            if (seq != null) {
                if (marker.contains(":")) {
                    long val = Long.parseLong(marker.substring(marker.indexOf(":") + 1));
                    seq.restart(val);
                } else {
                    seq.restart();
                }
                restarted = true;
            }
        }
        if (!restarted && col != null && (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL)) {
            if (marker.contains(":")) {
                long val = Long.parseLong(marker.substring(marker.indexOf(":") + 1));
                table.resetSerialCounter(val);
            } else {
                table.resetSerialCounter(1);
            }
        }
    }

    private void handleIdentity(String column, String defaultVal, Table table, AlterTableStmt stmt) {
        String marker = DdlExecutor.extractMarker(defaultVal);
        boolean isSetGenerated = defaultVal.contains("__identity__:always") || defaultVal.contains("__identity__:bydefault");

        if (isSetGenerated) {
            int ci = table.getColumnIndex(column);
            Column cc = ci >= 0 ? table.getColumns().get(ci) : null;
            if (cc != null) {
                boolean isIdentity = false;
                if (cc.getDefaultValue() != null && (cc.getDefaultValue().contains("nextval") || cc.getDefaultValue().contains("__identity__"))) {
                    isIdentity = true;
                }
                DataType ct = cc.getType();
                if (ct == DataType.SERIAL || ct == DataType.BIGSERIAL || ct == DataType.SMALLSERIAL) {
                    isIdentity = true;
                }
                if (!isIdentity) {
                    throw new MemgresException("column \"" + column + "\" of relation \"" + stmt.table()
                            + "\" is not an identity column", "55000");
                }
            }
        }

        // Parse identity options
        Long startWith = null;
        Long incrementBy = null;
        String explicitSeqName = null;
        if (marker.contains(":start=")) {
            String s = marker.substring(marker.indexOf(":start=") + 7);
            if (s.contains(":")) s = s.substring(0, s.indexOf(":"));
            startWith = Long.parseLong(s);
        }
        if (marker.contains(":increment=")) {
            String s = marker.substring(marker.indexOf(":increment=") + 11);
            if (s.contains(":")) s = s.substring(0, s.indexOf(":"));
            incrementBy = Long.parseLong(s);
        }
        if (marker.contains(":seqname=")) {
            String s = marker.substring(marker.indexOf(":seqname=") + 9);
            if (s.contains(":")) s = s.substring(0, s.indexOf(":"));
            explicitSeqName = s;
        }

        String seqName = explicitSeqName != null ? explicitSeqName :
                stmt.table() + "_" + column + "_seq";
        if (!executor.database.hasSequence(seqName)) {
            executor.database.addSequence(new Sequence(seqName, startWith, incrementBy, null, null));
            executor.database.registerSchemaObject(executor.defaultSchema(), "sequence", seqName);
        } else if (startWith != null || incrementBy != null) {
            Sequence seq = executor.database.getSequence(seqName);
            if (startWith != null) seq.restart(startWith);
            if (incrementBy != null) seq.setIncrementBy(incrementBy);
        }

        String newDefault;
        if (marker.contains(":always")) {
            newDefault = "__identity__:always";
        } else {
            newDefault = "nextval('" + seqName + "')";
        }
        table.alterColumnDefault(column, newDefault);

        boolean isAddGenerated = marker.contains(":add:");
        if (isAddGenerated) {
            int ci = table.getColumnIndex(column);
            if (ci >= 0) {
                Column cc = table.getColumns().get(ci);
                DataType serialType;
                switch (cc.getType()) {
                    case BIGINT:
                        serialType = DataType.BIGSERIAL;
                        break;
                    case SMALLINT:
                        serialType = DataType.SMALLSERIAL;
                        break;
                    default:
                        serialType = DataType.SERIAL;
                        break;
                }
                table.alterColumnType(column, serialType);
                if (startWith != null) {
                    table.resetSerialCounter(startWith);
                }
            }
        }
    }

    private Sequence findBackingSequence(Column col) {
        String seqRef = col.getDefaultValue();
        int qi = seqRef.indexOf("'");
        int qi2 = seqRef.indexOf("'", qi + 1);
        if (qi >= 0 && qi2 >= 0) {
            String seqName = seqRef.substring(qi + 1, qi2);
            return executor.database.getSequence(seqName);
        }
        return null;
    }

    private void executeAddConstraint(AlterTableStmt.AddConstraint addConstraint, Table table,
                                       AlterTableStmt stmt) {
        if (addConstraint.constraint().type() == TableConstraint.ConstraintType.NOT_NULL) {
            for (String colName : addConstraint.constraint().columns()) {
                table.alterColumnNullable(colName, false);
            }
            return;
        }

        List<String> rawCols = addConstraint.constraint().columns();
        boolean isUsingIndex = rawCols != null && rawCols.size() == 1 && rawCols.get(0).startsWith("__using_index__:");
        if (isUsingIndex) {
            String idxName = rawCols.get(0).substring("__using_index__:".length());
            table.removeConstraint(idxName);
        }
        StoredConstraint sc = ddl.convertTableConstraint(stmt.table(), addConstraint.constraint());
        if (sc != null && isUsingIndex) {
            sc.setPromotedFromIndex(true);
        }
        if (sc != null) {
            if (sc.getName() != null && table.getConstraint(sc.getName()) != null) {
                throw new MemgresException("constraint \"" + sc.getName() + "\" for relation \"" + stmt.table() + "\" already exists", "42710");
            }
            if (addConstraint.notValid()) {
                sc.setConvalidated(false);
            }
            if (!sc.isNotEnforced() && !addConstraint.notValid()) {
                if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                    validateForeignKeyData(sc, table, stmt.table());
                }
                if (sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null) {
                    validateCheckConstraintData(sc, table);
                }
            }
            table.addConstraint(sc);
        }
    }

    private void validateForeignKeyData(StoredConstraint sc, Table table, String tableName) {
        Table refTable = executor.resolveTable("public", sc.getReferencesTable());
        for (String refCol : sc.getReferencesColumns()) {
            if (refTable.getColumnIndex(refCol) < 0) {
                throw new MemgresException("column \"" + refCol + "\" referenced in foreign key constraint does not exist", "42703");
            }
        }
        if (!sc.getColumns().isEmpty()) {
            int[] fkIndices = new int[sc.getColumns().size()];
            for (int ci = 0; ci < sc.getColumns().size(); ci++) {
                fkIndices[ci] = table.getColumnIndex(sc.getColumns().get(ci));
            }
            int[] refIndices = new int[sc.getReferencesColumns().size()];
            for (int ci = 0; ci < sc.getReferencesColumns().size(); ci++) {
                refIndices[ci] = refTable.getColumnIndex(sc.getReferencesColumns().get(ci));
            }
            for (Object[] row : table.getRows()) {
                boolean allNull = true;
                for (int fi : fkIndices) {
                    if (fi >= 0 && row[fi] != null) { allNull = false; break; }
                }
                if (allNull) continue;
                boolean found = false;
                for (Object[] refRow : refTable.getRows()) {
                    boolean match = true;
                    for (int ci = 0; ci < fkIndices.length; ci++) {
                        Object fkVal = fkIndices[ci] >= 0 ? row[fkIndices[ci]] : null;
                        Object refVal = refIndices[ci] >= 0 ? refRow[refIndices[ci]] : null;
                        if (!java.util.Objects.equals(String.valueOf(fkVal), String.valueOf(refVal))) { match = false; break; }
                    }
                    if (match) { found = true; break; }
                }
                if (!found) {
                    throw new MemgresException("insert or update on table \"" + tableName + "\" violates foreign key constraint \"" + sc.getName() + "\"", "23503");
                }
            }
        }
    }

    private void validateCheckConstraintData(StoredConstraint sc, Table table) {
        ddl.validateExprColumnRefs(sc.getCheckExpr(), table, null);
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        for (Object[] row : table.getRows()) {
            Object[] evalRow = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
            RowContext checkCtx = new RowContext(table, null, evalRow);
            try {
                Object result = executor.evalExpr(sc.getCheckExpr(), checkCtx);
                if (result instanceof Boolean && !((Boolean) result)) {
                    throw new MemgresException("check constraint \"" + sc.getName() + "\" of relation \"" + table.getName() + "\" is violated by some row", "23514");
                }
            } catch (MemgresException me) {
                if ("23514".equals(me.getSqlState())) throw me;
            }
        }
    }

    private void executeValidateConstraint(AlterTableStmt.ValidateConstraint vc, Table table,
                                            AlterTableStmt stmt) {
        StoredConstraint sc = table.getConstraint(vc.constraintName());
        if (sc == null) {
            throw new MemgresException("constraint \"" + vc.constraintName() + "\" of relation \"" + stmt.table() + "\" does not exist", "42704");
        }
        if (sc.isConvalidated()) {
            return; // already validated, no-op
        }
        // Validate existing data
        if (sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null) {
            validateCheckConstraintData(sc, table);
        }
        if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
            validateForeignKeyData(sc, table, stmt.table());
        }
        sc.setConvalidated(true);
    }

    private void executeAttachPartition(AlterTableStmt.AttachPartition attach, Table table,
                                         AlterTableStmt stmt, String schemaName) {
        String partSchemaName = attach.partitionSchema() != null ? attach.partitionSchema() : schemaName;
        Table partition = executor.resolveTable(partSchemaName, attach.partitionName());
        if (table.getPartitions().contains(partition)) {
            throw new MemgresException("table \"" + attach.partitionName()
                    + "\" is already a partition of \"" + stmt.table() + "\"", "42809");
        }
        partition.setPartitionParent(table);
        table.addPartition(partition);
        if (attach.bounds() != null && !attach.bounds().isEmpty()) {
            ddl.tableExecutor.applyPartitionBounds(partition, table, attach.bounds(), attach.partitionName());
        }
    }
}
