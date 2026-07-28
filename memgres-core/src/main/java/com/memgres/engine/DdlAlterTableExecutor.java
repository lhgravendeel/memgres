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

    /**
     * VOLATILE built-in functions whose DEFAULT must be evaluated once per existing row when
     * added via ALTER TABLE ADD COLUMN (PG rewrites the table, calling the default per row).
     * STABLE functions (now(), current_timestamp, statement_timestamp()) are intentionally
     * absent: one value per statement is correct for them.
     */
    private static final java.util.Set<String> PER_ROW_VOLATILE_FUNCTIONS = com.memgres.engine.util.Cols.setOf(
            "random", "random_normal", "setseed", "gen_random_uuid", "uuid_generate_v4",
            "uuid_generate_v1", "uuidv4", "uuidv7", "gen_random_bytes", "nextval",
            "clock_timestamp", "timeofday"
    );

    DdlAlterTableExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    /** True if the expression text calls a built-in VOLATILE function (see PER_ROW_VOLATILE_FUNCTIONS). */
    private static boolean hasVolatileFunction(String exprStr) {
        String norm = exprStr.toLowerCase().replaceAll("\\s+", "");
        for (String fn : PER_ROW_VOLATILE_FUNCTIONS) {
            if (norm.contains(fn + "(")) return true;
        }
        return false;
    }

    QueryResult executeAlterTable(AlterTableStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        rejectActionsOnOtherRelationKinds(stmt);
        QueryResult viewResult = alterViewRelation(stmt);
        if (viewResult != null) return viewResult;
        Table table;
        try {
            table = executor.resolveTable(schemaName, stmt.table());
        } catch (MemgresException e) {
            if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
            throw e;
        }

        // ALTER TABLE takes ACCESS EXCLUSIVE, so it waits behind any open reader
        if (executor.session != null) {
            executor.database.acquireTableLock(schemaName + "." + stmt.table(),
                    "AccessExclusiveLock", executor.session, false);
        }

        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            table = executeAction(action, table, stmt, schemaName);
        }

        // The session's own DDL is visible to it, so its snapshot of the old shape must go
        if (executor.session != null) {
            executor.session.discardRRSnapshotForTable(schemaName + "." + stmt.table());
        }

        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * Rename actions aimed at a view have to change the view definition, which is what the
     * catalog reads. Routing them through the table path renamed a shadow relation instead,
     * leaving the view registered under its old name and its columns untouched.
     *
     * @return the result when the statement targeted a view, or null to continue as a table
     */
    private QueryResult alterViewRelation(AlterTableStmt stmt) {
        String bare = stmt.table().contains(".")
                ? stmt.table().substring(stmt.table().lastIndexOf('.') + 1) : stmt.table();
        Database.ViewDef view = executor.database.getView(bare);
        if (view == null) return null;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            // Anything else the view accepts, such as a column default, is left to the ordinary
            // path rather than half-handled here.
            if (!(action instanceof AlterTableStmt.RenameTable
                    || action instanceof AlterTableStmt.RenameColumn
                    || action instanceof AlterTableStmt.SetSchema)) {
                return null;
            }
        }
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            if (action instanceof AlterTableStmt.RenameTable) {
                String newName = ((AlterTableStmt.RenameTable) action).newName();
                if (executor.database.getView(newName) != null
                        || executor.database.getSchema(view.schemaName() != null
                            ? view.schemaName() : executor.defaultSchema()) != null
                        && executor.database.getSchema(view.schemaName() != null
                            ? view.schemaName() : executor.defaultSchema()).getTable(newName) != null) {
                    throw new MemgresException("relation \"" + newName + "\" already exists", "42P07");
                }
                executor.database.removeView(bare);
                executor.database.addView(withViewName(view, newName, view.cachedColumns()));
                view = executor.database.getView(newName);
                bare = newName;
            } else if (action instanceof AlterTableStmt.RenameColumn) {
                AlterTableStmt.RenameColumn rc = (AlterTableStmt.RenameColumn) action;
                List<Column> renamed = renameViewColumn(view, rc.oldName(), rc.newName(), bare);
                executor.database.addView(withViewName(view, bare, renamed));
                view = executor.database.getView(bare);
            } else if (action instanceof AlterTableStmt.SetSchema) {
                String target = ((AlterTableStmt.SetSchema) action).newSchema();
                if (executor.database.getSchema(target) == null) {
                    throw new MemgresException("schema \"" + target + "\" does not exist", "3F000");
                }
                executor.database.removeView(bare);
                executor.database.addView(new Database.ViewDef(view.name(), target, view.query(),
                        view.orReplace(), view.materialized(), view.cachedColumns(),
                        view.cachedRows(), view.sourceSQL(), view.checkOption(),
                        view.reloptions(), view.populated()));
                view = executor.database.getView(bare);
            }
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    private List<Column> renameViewColumn(Database.ViewDef view, String oldName, String newName,
                                           String relation) {
        List<Column> cols = view.cachedColumns();
        if (cols == null) {
            throw new MemgresException("column \"" + oldName + "\" of relation \"" + relation
                    + "\" does not exist", "42703");
        }
        List<Column> out = new ArrayList<>();
        boolean found = false;
        for (Column c : cols) {
            if (c.getName().equalsIgnoreCase(newName)) {
                throw new MemgresException("column \"" + newName + "\" of relation \"" + relation
                        + "\" already exists", "42701");
            }
        }
        for (Column c : cols) {
            if (c.getName().equalsIgnoreCase(oldName)) {
                found = true;
                out.add(c.withName(newName));
            } else {
                out.add(c);
            }
        }
        if (!found) {
            throw new MemgresException("column \"" + oldName + "\" of relation \"" + relation
                    + "\" does not exist", "42703");
        }
        return out;
    }

    private Database.ViewDef withViewName(Database.ViewDef view, String name, List<Column> cols) {
        return new Database.ViewDef(name, view.schemaName(), view.query(), view.orReplace(),
                view.materialized(), cols, view.cachedRows(), view.sourceSQL(),
                view.checkOption(), view.reloptions(), view.populated());
    }

    /**
     * ALTER TABLE reaches views, materialized views and sequences too, but only for the actions
     * that are about the relation's name or ownership. Anything that would reshape stored rows
     * has no meaning on those, and PostgreSQL names the offending action when it refuses.
     */
    private void rejectActionsOnOtherRelationKinds(AlterTableStmt stmt) {
        String bare = stmt.table().contains(".")
                ? stmt.table().substring(stmt.table().lastIndexOf('.') + 1) : stmt.table();
        Database.ViewDef view = executor.database.getView(bare);
        boolean isSequence = view == null && executor.database.hasSequence(bare);
        if (view == null && !isSequence) return;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            if (action instanceof AlterTableStmt.RenameTable
                    || action instanceof AlterTableStmt.SetSchema
                    || action instanceof AlterTableStmt.OwnerTo) {
                continue;
            }
            // Renaming a column, and giving one a default that INSERT through the view can
            // use, are both meaningful for a view but not for a sequence.
            if (view != null && (action instanceof AlterTableStmt.RenameColumn
                    || isColumnDefaultAction(action))) {
                continue;
            }
            throw new MemgresException("ALTER action " + alterActionName(action)
                    + " cannot be performed on relation \"" + bare + "\"", "42809");
        }
    }

    /** True for ALTER COLUMN ... SET/DROP DEFAULT, which a view accepts. */
    private static boolean isColumnDefaultAction(AlterTableStmt.AlterAction action) {
        if (!(action instanceof AlterTableStmt.AlterColumn)) return false;
        AlterTableStmt.AlterColumnAction inner = ((AlterTableStmt.AlterColumn) action).action();
        return inner instanceof AlterTableStmt.SetDefault
                || inner instanceof AlterTableStmt.DropDefault;
    }

    /** The action's name as PostgreSQL spells it when reporting an unsupported ALTER. */
    private static String alterActionName(AlterTableStmt.AlterAction action) {
        if (action instanceof AlterTableStmt.AddColumn) return "ADD COLUMN";
        if (action instanceof AlterTableStmt.DropColumn) return "DROP COLUMN";
        if (action instanceof AlterTableStmt.AddConstraint) return "ADD CONSTRAINT";
        if (action instanceof AlterTableStmt.DropConstraint) return "DROP CONSTRAINT";
        if (action instanceof AlterTableStmt.ValidateConstraint) return "VALIDATE CONSTRAINT";
        if (action instanceof AlterTableStmt.RenameConstraint) return "RENAME CONSTRAINT";
        if (action instanceof AlterTableStmt.AttachPartition) return "ATTACH PARTITION";
        if (action instanceof AlterTableStmt.DetachPartition) return "DETACH PARTITION";
        if (action instanceof AlterTableStmt.Inherit) return "INHERIT";
        if (action instanceof AlterTableStmt.NoInherit) return "NO INHERIT";
        if (action instanceof AlterTableStmt.EnableRls) return "ENABLE ROW SECURITY";
        if (action instanceof AlterTableStmt.DisableRls) return "DISABLE ROW SECURITY";
        if (action instanceof AlterTableStmt.ForceRls) return "FORCE ROW SECURITY";
        if (action instanceof AlterTableStmt.NoForceRls) return "NO FORCE ROW SECURITY";
        if (action instanceof AlterTableStmt.SetLogged) return "SET LOGGED";
        if (action instanceof AlterTableStmt.SetStorageParams) return "SET";
        if (action instanceof AlterTableStmt.AlterColumn) {
            return "ALTER COLUMN ... " + alterColumnActionName(
                    ((AlterTableStmt.AlterColumn) action).action());
        }
        return "ALTER";
    }

    private static String alterColumnActionName(AlterTableStmt.AlterColumnAction action) {
        if (action instanceof AlterTableStmt.SetNotNull) return "SET NOT NULL";
        if (action instanceof AlterTableStmt.DropNotNull) return "DROP NOT NULL";
        if (action instanceof AlterTableStmt.SetDefault) return "SET DEFAULT";
        if (action instanceof AlterTableStmt.DropDefault) return "DROP DEFAULT";
        if (action instanceof AlterTableStmt.SetType) return "SET DATA TYPE";
        return "ALTER";
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
            rewriteIncomingForeignKeys(stmt.table(), schemaName, rename.oldName(), rename.newName());
            rewriteIndexMetadata(stmt.table(), schemaName, rename.oldName(), rename.newName());
            rewriteDependentViews(stmt.table(), rename.oldName(), rename.newName());
            executor.recordUndo(new Session.RenameColumnUndo(schemaName, stmt.table(), rename.newName(), rename.oldName()));
        } else if (action instanceof AlterTableStmt.SetReplicaIdentity) {
            table.setReplicaIdentity(((AlterTableStmt.SetReplicaIdentity) action).identity());
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
            executeAddConstraint(addConstraint, table, stmt, schemaName);
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
            partition.clearPartitionBounds();
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
            // PG 18: only FOREIGN KEY constraints support ALTER CONSTRAINT ... [NOT] ENFORCED.
            // CHECK, UNIQUE, PRIMARY KEY, and EXCLUDE constraints cannot be toggled.
            if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) {
                throw new MemgresException(
                        "cannot alter enforceability of constraint \"" + ace.constraintName()
                                + "\" of relation \"" + stmt.table() + "\"",
                        "42809");
            }
            sc.setNotEnforced(ace.notEnforced());
        } else if (action instanceof AlterTableStmt.SetSchema) {
            AlterTableStmt.SetSchema setSchema = (AlterTableStmt.SetSchema) action;
            Schema oldSchema = executor.database.getSchema(schemaName);
            // Creating the destination on demand would move the table somewhere nothing can
            // name, which loses it outright.
            Schema newSchema = executor.database.getSchema(setSchema.newSchema());
            if (newSchema == null) {
                throw new MemgresException("schema \"" + setSchema.newSchema()
                        + "\" does not exist", "3F000");
            }
            oldSchema.removeTable(stmt.table());
            newSchema.addTable(table);
            retargetDependents(schemaName, stmt.table(), setSchema.newSchema(), stmt.table());
        } else if (action instanceof AlterTableStmt.Inherit) {
            AlterTableStmt.Inherit inherit = (AlterTableStmt.Inherit) action;
            Table parentTable = executor.resolveTable(schemaName, inherit.parentTable());
            rejectInheritanceCycle(table, parentTable, stmt.table(), inherit.parentTable());
            table.setParentTable(parentTable);
            parentTable.addChild(table);
        } else if (action instanceof AlterTableStmt.NoInherit) {
            AlterTableStmt.NoInherit noInherit = (AlterTableStmt.NoInherit) action;
            Table parentTable = executor.resolveTable(schemaName, noInherit.parentTable());
            parentTable.removeChild(table);
            table.setParentTable(null);
        } else if (action instanceof AlterTableStmt.DisableTrigger) {
            AlterTableStmt.DisableTrigger dt = (AlterTableStmt.DisableTrigger) action;
            setTriggerEnabled(table, dt.triggerName(), true);
        } else if (action instanceof AlterTableStmt.EnableTrigger) {
            AlterTableStmt.EnableTrigger et = (AlterTableStmt.EnableTrigger) action;
            setTriggerEnabled(table, et.triggerName(), false);
        } else if (action instanceof AlterTableStmt.SetStorageParams) {
            // no-op
        } else if (action instanceof AlterTableStmt.SetLogged) {
            AlterTableStmt.SetLogged sl = (AlterTableStmt.SetLogged) action;
            table.setUnlogged(!sl.logged());
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
        String compositeTypeName = resolved.compositeTypeName();
        DataType arrayElementType = resolved.arrayElementType();

        String defaultVal = def.defaultExpr() != null ? DdlExecutor.exprToDefaultString(def.defaultExpr()) : null;
        String genExpr = def.generatedExpr();

        // SERIAL/BIGSERIAL/SMALLSERIAL: create a real sequence (same as CREATE TABLE)
        if (dt == DataType.SERIAL || dt == DataType.BIGSERIAL || dt == DataType.SMALLSERIAL) {
            if (defaultVal == null && def.identity() == null) {
                String seqName = stmt.table() + "_" + def.name() + "_seq";
                Sequence seq = new Sequence(seqName, null, null, null, null);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                defaultVal = "nextval('" + seqName + "'::regclass)";
            }
        }

        // GENERATED AS IDENTITY on ADD COLUMN
        if (def.identity() != null) {
            String seqName = stmt.table() + "_" + def.name() + "_seq";
            if (!executor.database.hasSequence(seqName)) {
                Sequence seq = new Sequence(seqName, def.identityStart(), def.identityIncrement(), null, null);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
            }
            if ("ALWAYS".equalsIgnoreCase(def.identity())) {
                defaultVal = "__identity__:always:seq:" + seqName;
            } else {
                defaultVal = "__identity__:bydefault:seq:" + seqName;
            }
        }

        // Validate generated column expression references valid columns and is immutable
        if (genExpr != null) {
            // Reject volatile/stable functions and operators in generated column expressions
            DdlExecutor.checkExpressionImmutability(genExpr, ddl.executor.database,
                    "generation expression is not immutable");
            if (genExpr.toLowerCase().replaceAll("\\s+", "").contains("select")) {
                throw new MemgresException("cannot use subquery in column generation expression", "0A000");
            }
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

        // SERIAL and GENERATED AS IDENTITY columns are implicitly NOT NULL (same as CREATE TABLE)
        boolean notNull = def.notNull()
                || dt == DataType.SERIAL || dt == DataType.BIGSERIAL || dt == DataType.SMALLSERIAL
                || def.identity() != null;

        // Carry the resolved compositeTypeName/arrayElementType through, same as CREATE TABLE
        // (DdlTableExecutor) — hardcoding them to null made an ALTER-added "enum_type[]" column
        // indistinguishable from a scalar enum column, so PgWireValueFormatter.columnTypeOid
        // advertised the enum element's OID instead of the array type's.
        Column col = new Column(def.name(), dt, !notNull, def.primaryKey(), defaultVal,
                enumTypeName, def.precision(), def.scale(), genExpr, def.generatedVirtual(), domainTypeName,
                compositeTypeName, arrayElementType);
        // Don't pre-evaluate serial/nextval/identity defaults — they are evaluated per-row below.
        // Likewise, other VOLATILE defaults (random(), gen_random_uuid(), ...) must produce a
        // distinct value per existing row, matching PG's table rewrite. STABLE functions such as
        // now()/current_timestamp correctly evaluate once per statement and take the single-value
        // path.
        boolean sequenceBacked = defaultVal != null
                && (defaultVal.contains("nextval(") || defaultVal.startsWith("__identity__"));
        boolean volatileDefault = !sequenceBacked && defaultVal != null && genExpr == null
                && hasVolatileFunction(defaultVal);
        Object evaluatedDefault;
        if (sequenceBacked || volatileDefault) {
            evaluatedDefault = null;
        } else {
            evaluatedDefault = defaultVal != null ? executor.evaluateDefault(defaultVal, dt) : null;
            // The default has to be storable in the column being created. Accepting one that is
            // not leaves the table holding a value its own declaration says is impossible.
            if (evaluatedDefault != null) {
                TypeCoercion.coerceForStorage(evaluatedDefault, col);
            }
        }

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

        // Mark column as having a missing value if it was added with a DEFAULT and the table has existing rows
        if (evaluatedDefault != null && !table.getRows().isEmpty()) {
            col.setAttHasMissing(true);
        }
        table.addColumn(col, evaluatedDefault);
        executor.recordUndo(new Session.AddColumnUndo(schemaName, stmt.table(), def.name()));

        // Backfill existing rows for sequence-backed (serial/identity/nextval) and volatile
        // defaults, evaluating once per row. PG never leaves existing rows NULL for a serial/
        // identity column, and gives each existing row its own random()/gen_random_uuid() value.
        if ((sequenceBacked || volatileDefault) && !table.getRows().isEmpty()) {
            int newColIdx = table.getColumnIndex(def.name());
            if (newColIdx >= 0) {
                String identitySeqName = null;
                if (defaultVal.startsWith("__identity__") && defaultVal.contains(":seq:")) {
                    identitySeqName = defaultVal.substring(defaultVal.indexOf(":seq:") + 5);
                }
                for (Object[] row : table.getRows()) {
                    Object v;
                    if (defaultVal.startsWith("__identity__")) {
                        Sequence seq = identitySeqName != null
                                ? executor.database.getSequence(identitySeqName) : null;
                        v = seq != null ? seq.nextVal() : table.nextSerial();
                    } else {
                        v = executor.evaluateDefault(defaultVal, dt);
                    }
                    if (v != null) {
                        try {
                            v = TypeCoercion.coerceForStorage(v, col);
                        } catch (Exception ignored) {
                            // keep the uncoerced value rather than losing it
                        }
                    }
                    row[newColIdx] = v;
                }
            }
        }

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
        // Check for dependent generated columns
        String colNameLower = dropCol.column().toLowerCase();
        List<String> dependentGenCols = new ArrayList<>();
        for (Column c : table.getColumns()) {
            if (c.getGeneratedExpr() != null && !c.getName().equalsIgnoreCase(dropCol.column())) {
                String genExpr = c.getGeneratedExpr().toLowerCase();
                if (genExpr.contains(colNameLower)) {
                    dependentGenCols.add(c.getName());
                }
            }
        }
        if (!dropCol.cascade()) {
            String colName = dropCol.column().toLowerCase();
            if (!dependentGenCols.isEmpty()) {
                throw new MemgresException("cannot drop column " + dropCol.column()
                        + " of relation " + stmt.table()
                        + " because other objects depend on it", "2BP01");
            }
            for (Map.Entry<String, Database.ViewDef> viewEntry : executor.database.getViews().entrySet()) {
                String viewSql = viewEntry.getValue().query() != null ? viewEntry.getValue().query().toString() : "";
                if (viewSql.toLowerCase().contains(stmt.table().toLowerCase())
                        && viewSql.toLowerCase().contains(colName)) {
                    throw new MemgresException("cannot drop column \"" + dropCol.column()
                            + "\" of table \"" + stmt.table()
                            + "\" because view \"" + viewEntry.getKey() + "\" depends on it", "42P16");
                }
            }
            // FOREIGN KEY constraints (on any table, including self-references) whose REFERENCED
            // columns include the dropped column depend on it via the unique index — PG requires
            // CASCADE to drop them.
            for (Schema sch : executor.database.getSchemas().values()) {
                for (Table t : sch.getTables().values()) {
                    for (StoredConstraint sc : t.getConstraints()) {
                        if (isFkReferencing(sc, stmt.table(), schemaName, dropCol.column())
                                && !StoredConstraint.containsIgnoreCase(sc.getColumns(), dropCol.column())) {
                            throw new MemgresException("cannot drop column " + dropCol.column()
                                    + " of table " + stmt.table()
                                    + " because other objects depend on it\n  Detail: constraint "
                                    + sc.getName() + " on table " + t.getName() + " depends on column "
                                    + dropCol.column(), "2BP01");
                        }
                    }
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
        // Drop incoming FOREIGN KEY constraints that referenced the dropped column (reached only
        // with CASCADE, or when the FK's own columns also contained the dropped column).
        for (Schema sch : executor.database.getSchemas().values()) {
            for (Table t : sch.getTables().values()) {
                List<String> fkToDrop = new ArrayList<>();
                for (StoredConstraint sc : t.getConstraints()) {
                    if (isFkReferencing(sc, stmt.table(), schemaName, dropCol.column())) {
                        fkToDrop.add(sc.getName());
                    }
                }
                for (String fkName : fkToDrop) {
                    if (fkName != null) t.removeConstraint(fkName);
                }
            }
        }
        // Drop database-level index metadata for indexes that used the dropped column, so they
        // don't linger in pg_indexes (PG drops such indexes automatically).
        String qualifiedTable = schemaName + "." + stmt.table();
        List<String> idxToDrop = new ArrayList<>();
        java.util.regex.Pattern colWord = java.util.regex.Pattern.compile(
                "(?i)\\b" + java.util.regex.Pattern.quote(dropCol.column()) + "\\b");
        for (Map.Entry<String, List<String>> idxEntry : executor.database.getIndexColumns().entrySet()) {
            String idxTable = executor.database.getIndexTable(idxEntry.getKey());
            if (idxTable == null || !idxTable.equalsIgnoreCase(qualifiedTable)) continue;
            boolean usesColumn = false;
            if (idxEntry.getValue() != null) {
                for (String c : idxEntry.getValue()) {
                    if (c.equalsIgnoreCase(dropCol.column()) || colWord.matcher(c).find()) {
                        usesColumn = true;
                        break;
                    }
                }
            }
            String whereClause = executor.database.getIndexWhereClause(idxEntry.getKey());
            if (!usesColumn && whereClause != null && colWord.matcher(whereClause).find()) {
                usesColumn = true;
            }
            if (usesColumn) idxToDrop.add(idxEntry.getKey());
        }
        for (String idxName : idxToDrop) {
            executor.database.removeIndex(idxName);
        }
        // CASCADE: also drop dependent generated columns
        if (dropCol.cascade() && !dependentGenCols.isEmpty()) {
            for (String depCol : dependentGenCols) {
                int depIdx = table.getColumnIndex(depCol);
                if (depIdx >= 0) {
                    Column depColumn = table.getColumns().get(depIdx);
                    List<Object> depValues = new ArrayList<>();
                    for (Object[] row : table.getRows()) {
                        depValues.add(row[depIdx]);
                    }
                    executor.recordUndo(new Session.DropColumnUndo(schemaName, stmt.table(), depColumn, depIdx, depValues));
                    table.removeColumn(depCol);
                }
            }
        }
    }

    /** True if sc is a FOREIGN KEY whose referenced table/column match the given table and column. */
    private static boolean isFkReferencing(StoredConstraint sc, String tableName, String schemaName, String column) {
        return sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                && sc.getReferencesTable() != null
                && sc.getReferencesTable().equalsIgnoreCase(tableName)
                && (sc.getReferencesSchema() == null || sc.getReferencesSchema().equalsIgnoreCase(schemaName))
                && StoredConstraint.containsIgnoreCase(sc.getReferencesColumns(), column);
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
        retargetDependents(schemaName, stmt.table(), schemaName, rename.newName());
        return renamed;
    }

    /**
     * Follow a renamed or moved relation from everything that names it. PG records these
     * dependencies as OIDs, so a rename leaves foreign keys enforcing and views reading the
     * same relation; memgres stores names, so they have to be rewritten here.
     */
    private void retargetDependents(String oldSchema, String oldName, String newSchema, String newName) {
        boolean moved = newSchema != null && !newSchema.equalsIgnoreCase(oldSchema);
        for (Map.Entry<String, Schema> se : executor.database.getSchemas().entrySet()) {
            for (Table t : new ArrayList<>(se.getValue().getTables().values())) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (!oldName.equalsIgnoreCase(sc.getReferencesTable())) continue;
                    String refSchema = sc.getReferencesSchema();
                    if (refSchema != null && !refSchema.equalsIgnoreCase(oldSchema)) continue;
                    sc.setReferencesTable(newName);
                    if (moved) sc.setReferencesSchema(newSchema);
                }
            }
        }
        for (Database.ViewDef view : new ArrayList<>(executor.database.getViews().values())) {
            if (AstRelationRenamer.retarget(view.query(), oldSchema, oldName, newSchema, newName)) {
                view.sourceSQL = rewriteSourceSql(view.sourceSQL(), oldSchema, oldName, newSchema, newName);
            }
        }
    }

    /** Rewrite the relation name inside a stored view definition so pg_get_viewdef reads true. */
    private static String rewriteSourceSql(String sql, String oldSchema, String oldName,
                                           String newSchema, String newName) {
        if (sql == null) return null;
        boolean moved = newSchema != null && !newSchema.equalsIgnoreCase(oldSchema);
        String replacement = moved ? newSchema + "." + newName : newName;
        String qualified = "(?i)\\b" + java.util.regex.Pattern.quote(oldSchema) + "\\."
                + java.util.regex.Pattern.quote(oldName) + "\\b";
        String bare = "(?i)\\b" + java.util.regex.Pattern.quote(oldName) + "\\b";
        return sql.replaceAll(qualified, java.util.regex.Matcher.quoteReplacement(replacement))
                  .replaceAll(bare, java.util.regex.Matcher.quoteReplacement(replacement));
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
        } else if (alterCol.action() instanceof AlterTableStmt.SetStatistics) {
            AlterTableStmt.SetStatistics ss = (AlterTableStmt.SetStatistics) alterCol.action();
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx < 0) throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
            table.getColumns().get(colIdx).setAttStattarget((short) ss.target());
        } else if (alterCol.action() instanceof AlterTableStmt.SetStorage) {
            AlterTableStmt.SetStorage ss = (AlterTableStmt.SetStorage) alterCol.action();
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx < 0) throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
            String storageCode;
            switch (ss.storageType().toUpperCase()) {
                case "PLAIN": storageCode = "p"; break;
                case "EXTERNAL": storageCode = "e"; break;
                case "EXTENDED": storageCode = "x"; break;
                case "MAIN": storageCode = "m"; break;
                default: storageCode = "p"; break;
            }
            table.getColumns().get(colIdx).setAttStorageOverride(storageCode);
        } else if (alterCol.action() instanceof AlterTableStmt.SetCompression) {
            AlterTableStmt.SetCompression sc = (AlterTableStmt.SetCompression) alterCol.action();
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx < 0) throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
            String compressionCode;
            switch (sc.method().toLowerCase()) {
                case "pglz": compressionCode = "p"; break;
                case "lz4": compressionCode = "l"; break;
                default: compressionCode = ""; break;
            }
            table.getColumns().get(colIdx).setAttCompression(compressionCode);
        } else if (alterCol.action() instanceof AlterTableStmt.ColumnNoOp) {
            // no-op
        }
    }

    private void executeSetType(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetType setType,
                                 Table table, AlterTableStmt stmt, String schemaName) {
        String baseType = setType.typeName().replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        // Extract the new type's typmod (precision/scale) so it replaces the old column's —
        // e.g. ALTER COLUMN capacity TYPE numeric(10, 2) must set precision 10 / scale 2, or
        // NUMERIC storage coercion (TypeCoercion.applyPrecision) never enforces the declared
        // scale and values round-trip at whatever incidental scale they arrived with.
        Integer newPrecision = null;
        Integer newScale = null;
        java.util.regex.Matcher typmod = java.util.regex.Pattern
                .compile("\\(\\s*(\\d+)\\s*(?:,\\s*(-?\\d+)\\s*)?\\)")
                .matcher(setType.typeName());
        if (typmod.find()) {
            try {
                newPrecision = Integer.valueOf(typmod.group(1));
                if (typmod.group(2) != null) newScale = Integer.valueOf(typmod.group(2));
            } catch (NumberFormatException ignored) {
                newPrecision = null;
                newScale = null;
            }
        }
        boolean isArrayType = setType.typeName().replaceAll("\\(.*\\)", "").trim().endsWith("[]");
        DataType dt = DataType.fromPgName(baseType);
        String newEnumTypeName = null;
        DataType newArrayElementType = null;
        if (dt == null) {
            if (executor.database.isCustomEnum(baseType)) {
                dt = DataType.ENUM;
                // Carry the enum identity (and array-ness) into the retyped column, same as
                // CREATE TABLE / ADD COLUMN via resolveColumnType — without these the retyped
                // column advertised the unresolvable ENUM placeholder OID 0 (enumTypeName null),
                // and "enum_type[]" was indistinguishable from a scalar enum column.
                newEnumTypeName = baseType;
                if (isArrayType) {
                    newArrayElementType = DataType.ENUM;
                }
            } else {
                throw new MemgresException("type \"" + baseType + "\" does not exist", "42704");
            }
        }
        int colIdx = table.getColumnIndex(alterCol.column());
        if (colIdx < 0) {
            throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
        }
        // Check for generated column dependencies
        String alterColLower = alterCol.column().toLowerCase();
        for (Column c : table.getColumns()) {
            if (c.getGeneratedExpr() != null && !c.getName().equalsIgnoreCase(alterCol.column())) {
                String genExpr = c.getGeneratedExpr().toLowerCase();
                if (genExpr.contains(alterColLower)) {
                    throw new MemgresException("cannot alter type of a column used by a generated column", "0A000");
                }
            }
        }
        DataType currentType = table.getColumns().get(colIdx).getType();
        if (setType.usingExpr() == null && currentType != null && dt != null && currentType != dt) {
            TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
            TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
            // Without USING, PG only applies assignment casts: conversions within the same type
            // category (varchar<->text, int->bigint, ...) and conversions TO a string-category
            // type (any type is I/O-coercible to text in assignment context). Conversions FROM a
            // string type to anything else (text->integer, text->date, varchar->enum, ...) are
            // explicit-only and must fail with 42804.
            if (fromCat != toCat && toCat != TypeCoercion.TypeCategory.STRING) {
                String targetName = newEnumTypeName != null ? newEnumTypeName : dt.toRegtypeDisplay();
                if (isArrayType) targetName += "[]";
                throw new MemgresException("column \"" + alterCol.column() + "\" cannot be cast automatically to type "
                        + targetName + "\n  Hint: You might need to specify \"USING "
                        + alterCol.column() + "::" + targetName + "\".", "42804");
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
            table.alterColumnType(alterCol.column(), dt, newPrecision, newScale, newEnumTypeName, newArrayElementType);
            Column newCol = table.getColumns().get(convIdx);
            for (int ri = 0; ri < table.getRows().size(); ri++) {
                Object[] row = table.getRows().get(ri);
                row[convIdx] = convertedValues[ri] != null
                        ? TypeCoercion.coerceForStorage(convertedValues[ri], newCol)
                        : null;
            }
        } else {
            table.alterColumnType(alterCol.column(), dt, newPrecision, newScale, newEnumTypeName, newArrayElementType);
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
        if (col != null && col.getDefaultValue() != null
                && (col.getDefaultValue().contains("nextval") || col.getDefaultValue().contains(":seq:"))) {
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
            newDefault = "__identity__:always:seq:" + seqName;
        } else {
            newDefault = "__identity__:bydefault:seq:" + seqName;
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
        // Check for nextval('seqname'::regclass) pattern
        int qi = seqRef.indexOf("'");
        int qi2 = seqRef.indexOf("'", qi + 1);
        if (qi >= 0 && qi2 >= 0) {
            String seqName = seqRef.substring(qi + 1, qi2);
            return executor.database.getSequence(seqName);
        }
        // Check for __identity__:...:seq:seqname pattern
        if (seqRef.contains(":seq:")) {
            String seqName = seqRef.substring(seqRef.indexOf(":seq:") + 5);
            return executor.database.getSequence(seqName);
        }
        return null;
    }

    private void executeAddConstraint(AlterTableStmt.AddConstraint addConstraint, Table table,
                                       AlterTableStmt stmt, String schemaName) {
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
            // For FK constraints without explicit schema, default to the table's schema
            if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                    && sc.getReferencesSchema() == null && sc.getReferencesTable() != null) {
                sc.setReferencesSchema(schemaName);
            }
            if (!sc.isNotEnforced() && !addConstraint.notValid()) {
                if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                    validateForeignKeyData(sc, table, stmt.table());
                }
                if (sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null) {
                    validateCheckConstraintData(sc, table);
                }
            }
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                // Same invariant CREATE TABLE enforces for partitioned tables: a PK/UNIQUE
                // constraint must include every partition key column. Validate before adding
                // to (or propagating onto) the table, matching creation-time ordering.
                DdlTableExecutor.validatePartitionKeyCoverage(table, sc);
                // PG validates existing data while building the unique index: duplicates abort
                // with 23505, and NULLs in a new PRIMARY KEY column abort with 23502.
                if (!sc.isNotEnforced()) {
                    table.validateNewUniqueConstraint(sc);
                }
            }
            table.addConstraint(sc);
            ddl.registerExcludeIndex(schemaName, stmt.table(), sc);
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                // PG: ADD PRIMARY KEY marks the key columns NOT NULL (attnotnull /
                // information_schema.columns.is_nullable = 'NO'), on the table and its partitions.
                setColumnsNotNull(table, sc.getColumns());
            }
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                propagateConstraintToPartitions(table, sc);
            }
        }
    }

    /**
     * Copies a newly-added PK/UNIQUE constraint onto every existing partition (recursively, for
     * multi-level partitioning) of a partitioned parent. Row storage for a partitioned table
     * lives entirely on the leaf partitions, so a constraint added to the parent after
     * partitions already exist must reach each partition's own TableIndex too - otherwise
     * per-partition duplicate-key checks and ON CONFLICT conflict detection would miss rows that
     * already live there, the same bug class fixed for creation-time constraints in
     * {@link DdlTableExecutor#createPartitionOfTable}. Each partition gets its own independent
     * {@code StoredConstraint} copy (see {@link StoredConstraint#copyForPartition}) rather than
     * sharing the parent's instance.
     */
    /** Marks the given columns NOT NULL on a table and, recursively, on all its partitions. */
    private void setColumnsNotNull(Table table, List<String> columns) {
        for (String col : columns) {
            if (table.getColumnIndex(col) >= 0) {
                table.alterColumnNullable(col, false);
            }
        }
        for (Table partition : table.getPartitions()) {
            setColumnsNotNull(partition, columns);
        }
    }

    private void propagateConstraintToPartitions(Table table, StoredConstraint sc) {
        for (Table partition : table.getPartitions()) {
            DdlTableExecutor.validatePartitionKeyCoverage(partition, sc);
            partition.addConstraint(sc.copyForPartition(partition.getName()));
            propagateConstraintToPartitions(partition, sc);
        }
    }

    private void validateForeignKeyData(StoredConstraint sc, Table table, String tableName) {
        Table refTable;
        if (sc.getReferencesSchema() != null) {
            refTable = executor.resolveTable(sc.getReferencesSchema(), sc.getReferencesTable());
        } else {
            refTable = executor.resolveTableAnySchema(sc.getReferencesTable());
        }
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
        rejectInheritanceCycle(partition, table, attach.partitionName(), stmt.table());
        if (table.getPartitions().contains(partition)) {
            throw new MemgresException("table \"" + attach.partitionName()
                    + "\" is already a partition of \"" + stmt.table() + "\"", "42809");
        }
        // C4a: Validate column compatibility (names and types must match parent)
        validatePartitionColumns(table, partition, attach.partitionName());
        // Validate bounds before attaching, so overlapping bounds (42P17) don't leave
        // the table half-attached to the parent's routing list
        if (attach.bounds() != null && !attach.bounds().isEmpty()) {
            ddl.tableExecutor.applyPartitionBounds(partition, table, attach.bounds(), attach.partitionName());
        }
        // C4b: Validate existing rows satisfy partition bounds
        validateExistingRowBounds(partition, table, attach.partitionName());
        partition.setPartitionParent(table);
        partition.setParentColumnRemap(buildParentColumnRemap(table, partition));
        table.addPartition(partition);
    }

    /**
     * Refuse to make {@code child} a child of {@code parent} when the parent already sits below
     * the child in the hierarchy — inheritance and partitioning share one graph, so either link
     * can close a loop. Storing the link instead would leave a cyclic catalog, and every later
     * walk of it (a SELECT on either table, DROP TABLE, a pg_inherits query) would run until the
     * stack was gone, leaving the tables unusable and undroppable.
     */
    private static void rejectInheritanceCycle(Table child, Table parent,
                                               String childName, String parentName) {
        Set<Table> seen = Collections.newSetFromMap(new IdentityHashMap<Table, Boolean>());
        if (isSelfOrDescendant(parent, child, seen)) {
            throw PgErrors.circularInheritance(childName, parentName);
        }
    }

    /** True when {@code candidate} is {@code root} itself or sits anywhere beneath it. */
    private static boolean isSelfOrDescendant(Table candidate, Table root, Set<Table> seen) {
        if (candidate == root) return true;
        if (!seen.add(root)) return false;
        for (Table c : root.getChildren()) {
            if (isSelfOrDescendant(candidate, c, seen)) return true;
        }
        for (Table p : root.getPartitions()) {
            if (isSelfOrDescendant(candidate, p, seen)) return true;
        }
        return false;
    }

    /**
     * Position map from the parent's columns to the partition's, matched by name.
     * Returns null when the orders already agree, which is the usual case.
     */
    private static int[] buildParentColumnRemap(Table parent, Table partition) {
        List<Column> parentCols = parent.getColumns();
        int[] remap = new int[parentCols.size()];
        boolean identical = parentCols.size() == partition.getColumns().size();
        for (int i = 0; i < parentCols.size(); i++) {
            remap[i] = partition.getColumnIndex(parentCols.get(i).getName());
            if (remap[i] != i) identical = false;
        }
        return identical ? null : remap;
    }

    /** C4a: Partition must have the same columns (by name and type) as the parent.
     * PG raises 42804 (ERRCODE_DATATYPE_MISMATCH) for every column mismatch — extra
     * column, missing column, or a same-named column whose type differs. */
    private void validatePartitionColumns(Table parent, Table partition, String partName) {
        List<Column> parentCols = parent.getColumns();
        List<Column> partCols = partition.getColumns();
        // Check partition doesn't have columns not in parent
        for (Column pc : partCols) {
            boolean found = false;
            for (Column pp : parentCols) {
                if (pp.getName().equalsIgnoreCase(pc.getName())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new MemgresException("table \"" + partName
                        + "\" contains column \"" + pc.getName()
                        + "\" not found in parent \"" + parent.getName() + "\"", "42804");
            }
        }
        // Check parent columns exist in partition AND have a matching type
        for (Column pp : parentCols) {
            Column match = null;
            for (Column pc : partCols) {
                if (pc.getName().equalsIgnoreCase(pp.getName())) {
                    match = pc;
                    break;
                }
            }
            if (match == null) {
                throw new MemgresException("child table is missing column \""
                        + pp.getName() + "\"", "42804");
            }
            if (!sameColumnType(pp, match)) {
                throw new MemgresException("child table \"" + partName
                        + "\" has different type for column \"" + pp.getName() + "\"", "42804");
            }
        }
    }

    /** Normalized type-identity comparison for ATTACH PARTITION. Uses the regtype
     * display name (so serial/int, bigserial/bigint collapse to their real type and
     * typmod is ignored — matching PG, which never rejects on length/precision here),
     * plus enum identity, array element type, and domain identity. */
    private boolean sameColumnType(Column a, Column b) {
        if (!a.getType().toRegtypeDisplay().equalsIgnoreCase(b.getType().toRegtypeDisplay())) {
            return false;
        }
        if (!equalsIgnoreCaseNullable(a.getEnumTypeName(), b.getEnumTypeName())) {
            return false;
        }
        if (!equalsIgnoreCaseNullable(a.getDomainTypeName(), b.getDomainTypeName())) {
            return false;
        }
        DataType ae = a.getArrayElementType();
        DataType be = b.getArrayElementType();
        if (ae == null ? be != null : (be == null || ae != be)) {
            return false;
        }
        return true;
    }

    private static boolean equalsIgnoreCaseNullable(String a, String b) {
        if (a == null) return b == null;
        return a.equalsIgnoreCase(b);
    }

    /** C4b: All existing rows must satisfy the partition's bounds. */
    private void validateExistingRowBounds(Table partition, Table parent, String partName) {
        if (partition.getRows().isEmpty()) return;
        String partCol = parent.getPartitionColumn();
        if (partCol == null) return;
        int colIdx = partition.getColumnIndex(partCol);
        if (colIdx < 0) return;
        String strategy = parent.getPartitionStrategy();
        if (strategy == null) return;
        for (Object[] row : partition.getRows()) {
            Object value = row[colIdx];
            if (!rowSatisfiesBounds(value, partition, strategy)) {
                throw new MemgresException("partition constraint of relation \""
                        + partName + "\" is violated by some row", "23514");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private boolean rowSatisfiesBounds(Object value, Table partition, String strategy) {
        switch (strategy.toUpperCase()) {
            case "RANGE":
                if (value == null) return false; // NULL never matches RANGE
                if (partition.getPartitionLower() == null || partition.getPartitionUpper() == null) return true;
                Comparable<Object> cv = (Comparable<Object>) value;
                Object lower = partition.getPartitionLower();
                Object upper = partition.getPartitionUpper();
                return ((Comparable) value).compareTo(lower) >= 0
                        && ((Comparable) value).compareTo(upper) < 0;
            case "LIST":
                if (partition.getPartitionValues() == null) return true;
                for (Object pv : partition.getPartitionValues()) {
                    if (value == null && pv == null) return true;
                    if (value != null && value.equals(pv)) return true;
                }
                return false;
            default:
                return true; // HASH or unknown — skip validation
        }
    }

    /** Build a row as the parent would see it (mapping partition columns to parent positions). */
    private Object[] buildParentRow(Table parent, Table partition, Object[] partRow) {
        Object[] parentRow = new Object[parent.getColumns().size()];
        for (int i = 0; i < parent.getColumns().size(); i++) {
            String colName = parent.getColumns().get(i).getName();
            int partIdx = partition.getColumnIndex(colName);
            if (partIdx >= 0 && partIdx < partRow.length) {
                parentRow[i] = partRow[partIdx];
            }
        }
        return parentRow;
    }

    private void setTriggerEnabled(Table table, String triggerName, boolean disabled) {
        List<PgTrigger> triggers = executor.database.getTriggersForTable(table.getName());
        if ("ALL".equalsIgnoreCase(triggerName)) {
            for (PgTrigger t : triggers) {
                t.setDisabled(disabled);
            }
        } else {
            for (PgTrigger t : triggers) {
                if (t.getName().equalsIgnoreCase(triggerName)) {
                    t.setDisabled(disabled);
                }
            }
        }
    }

    /**
     * After renaming a column, rewrite FOREIGN KEY constraints on ANY table (including
     * self-references) whose referenced columns point at the renamed column of this table.
     * Without this, FK enforcement against the renamed column silently breaks.
     */
    private void rewriteIncomingForeignKeys(String tableName, String schemaName, String oldCol, String newCol) {
        for (Schema sch : executor.database.getSchemas().values()) {
            for (Table t : sch.getTables().values()) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                            && sc.getReferencesTable() != null
                            && sc.getReferencesTable().equalsIgnoreCase(tableName)
                            && (sc.getReferencesSchema() == null
                                || sc.getReferencesSchema().equalsIgnoreCase(schemaName))) {
                        sc.renameReferencedColumn(oldCol, newCol);
                    }
                }
            }
        }
    }

    /**
     * After renaming a column, rewrite database-level index metadata (pg_indexes /
     * USING INDEX lookups) for indexes on this table so their column lists — including
     * expression columns — reference the new column name.
     */
    private void rewriteIndexMetadata(String tableName, String schemaName, String oldCol, String newCol) {
        String qualified = schemaName + "." + tableName;
        for (Map.Entry<String, List<String>> entry : executor.database.getIndexColumns().entrySet()) {
            String idxTable = executor.database.getIndexTable(entry.getKey());
            if (idxTable == null || !idxTable.equalsIgnoreCase(qualified)) continue;
            List<String> cols = entry.getValue();
            if (cols == null || cols.isEmpty()) continue;
            boolean changed = false;
            List<String> updated = new ArrayList<>(cols.size());
            for (String c : cols) {
                String u;
                if (c.equalsIgnoreCase(oldCol)) {
                    u = newCol;
                } else {
                    u = StoredConstraint.renameIdentifier(c, oldCol, newCol);
                }
                if (!u.equals(c)) changed = true;
                updated.add(u);
            }
            if (changed) entry.setValue(updated);
        }
    }

    /**
     * After renaming a column, rewrite any view whose sourceSQL references
     * the base table so that occurrences of the old column name are replaced
     * with the new column name.  The view is then re-parsed and re-registered.
     */
    private void rewriteDependentViews(String tableName, String oldCol, String newCol) {
        for (Database.ViewDef vd : new ArrayList<>(executor.database.getViews().values())) {
            // Get SQL text from sourceSQL or unparse from the AST
            String sql = vd.sourceSQL;
            if (sql == null || sql.isEmpty()) {
                sql = SqlUnparser.toSql(vd.query);
            }
            if (sql == null || sql.isEmpty()) continue;
            String sqlLower = sql.toLowerCase();
            if (!sqlLower.contains(tableName.toLowerCase())) continue;
            if (!sqlLower.contains(oldCol.toLowerCase())) continue;
            // Replace old column name with new, word-boundary aware
            String updated = sql.replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(oldCol) + "\\b", newCol);
            if (updated.equals(sql)) continue;
            try {
                Statement parsed = com.memgres.engine.parser.Parser.parse(updated);
                // Keep regular-view column metadata (used by catalogs) in sync with the
                // rewritten output names. Materialized views keep their own column names.
                List<Column> newCachedCols = vd.cachedColumns;
                if (!vd.materialized && vd.cachedColumns != null) {
                    newCachedCols = new ArrayList<>();
                    for (Column c : vd.cachedColumns) {
                        if (c.getName().equalsIgnoreCase(oldCol)) {
                            newCachedCols.add(new Column(newCol, c.getType(), c.isNullable(), c.isPrimaryKey(),
                                    c.getDefaultValue(), c.getEnumTypeName(), c.getPrecision(), c.getScale(),
                                    c.getGeneratedExpr(), c.isVirtual(), c.getDomainTypeName(),
                                    c.getCompositeTypeName(), c.getArrayElementType()));
                        } else {
                            newCachedCols.add(c);
                        }
                    }
                }
                Database.ViewDef newView = new Database.ViewDef(
                        vd.name, vd.schemaName, parsed, vd.orReplace, vd.materialized,
                        newCachedCols, vd.cachedRows, updated, vd.checkOption, vd.reloptions, vd.populated);
                executor.database.addView(newView);
            } catch (Exception ignored) {
                // If re-parse fails, leave the view as-is
            }
        }
    }
}
