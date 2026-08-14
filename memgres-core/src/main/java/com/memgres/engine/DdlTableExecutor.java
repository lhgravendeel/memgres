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

    /** The first schema on the search path that holds a relation of this name. */
    private String schemaOfRelation(String bareName) {
        for (String schema : executor.searchPathSchemas()) {
            if (RelationNamespace.kindOf(executor.database, schema, bareName) != null) return schema;
        }
        return executor.defaultSchema();
    }

    /**
     * The columns a composite type gives a LIKE clause, or null when the name is not one.
     *
     * <p>PostgreSQL takes a composite type here as readily as a relation: what LIKE copies is a
     * row's shape, and a stand-alone composite has one. Sending the name to the relation resolver
     * instead reached the type's {@code pg_class} row and refused it for its kind, so a definition
     * PostgreSQL accepts was rejected.
     */
    private Table compositeLikeSource(String schemaName, String written) {
        int dot = written.indexOf('.');
        String schema = dot > 0 ? written.substring(0, dot) : schemaName;
        String bare = dot > 0 ? written.substring(dot + 1) : written;
        if (!RelationNamespace.COMPOSITE.equals(
                RelationNamespace.kindOf(executor.database, schema, bare))) {
            return null;
        }
        List<CreateTypeStmt.CompositeField> fields =
                executor.database.getCompositeType(schema + "." + bare);
        if (fields == null) return null;
        List<Column> cols = new ArrayList<>();
        for (CreateTypeStmt.CompositeField field : fields) {
            DdlExecutor.ResolvedType fieldType = ddl.resolveColumnType(field.typeName(), null);
            cols.add(new Column(field.name(), fieldType.dataType(), true, false, null,
                    fieldType.enumTypeName(), null, null, null, false,
                    fieldType.domainTypeName(), fieldType.compositeTypeName(),
                    fieldType.arrayElementType()));
        }
        return new Table(bare, cols);
    }

    // ---- CREATE TABLE ----

    QueryResult executeCreateTable(CreateTableStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.creationSchema();
        if (stmt.temporary()) {
            // A temporary relation lives in the session's own schema and nowhere else, so a
            // qualifier naming another schema contradicts the word TEMP rather than choosing where
            // the table goes. pg_temp is the alias that schema answers to.
            String tempSchema = executor.session != null
                    ? executor.session.getTempSchemaName() : "pg_temp";
            if (stmt.schema() != null && !stmt.schema().equalsIgnoreCase(tempSchema)
                    && !"pg_temp".equalsIgnoreCase(stmt.schema())) {
                throw new MemgresException(
                        "cannot create temporary relation in non-temporary schema", "42P16");
            }
            schemaName = tempSchema;
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
        // The schema a session's temporary relations live in comes into being with the first of
        // them, and PostgreSQL writes it as it writes any other schema.
        boolean temporarySchemaMadeWithIt =
                stmt.temporary() && executor.database.getSchema(schemaName) == null;
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
        // A table carries a row type of its own name, so a name an enum, a domain or a range
        // already answers to is taken for it even though no relation holds it.
        TypeNamespace.requireCreatableRowType(executor.database, schemaName, stmt.name());

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
            // A CHECK is inherited under the name it was declared with, and the child stores one
            // constraint per name — so two parents may contribute the same name only when they
            // mean the same thing, or one of the two rules is silently lost.
            Map<String, String> inheritedChecks = new LinkedHashMap<>();
            for (String parentName : stmt.inherits()) {
                Table parent = executor.resolveTable(schemaName, parentName);
                if (!seenParents.add(parent.getName().toLowerCase())) {
                    throw new MemgresException("relation \"" + parentName
                            + "\" would be inherited from more than once", "42P07");
                }
                parentTables.add(parent);
                for (StoredConstraint parentCheck : parent.getConstraints()) {
                    if (parentCheck.getType() != StoredConstraint.Type.CHECK
                            || parentCheck.getName() == null) {
                        continue;
                    }
                    String checkText = parentCheck.getCheckExpr() == null ? ""
                            : SqlUnparser.exprToSql(parentCheck.getCheckExpr());
                    String priorText = inheritedChecks.put(
                            parentCheck.getName().toLowerCase(), checkText);
                    if (priorText != null && !priorText.equals(checkText)) {
                        throw new MemgresException("check constraint name \""
                                + parentCheck.getName()
                                + "\" appears multiple times but with different expressions",
                                "42710");
                    }
                }
                for (Column col : parent.getColumns()) {
                    // A NOT NULL written NO INHERIT answers for the declaring relation's rows and
                    // no others, so the column reaches the child with nothing on it: the child
                    // takes a row PostgreSQL lets it take, and holds no constraint for it.
                    if (!col.isNullable() && parent.isNotNullNoInherit(col.getName())) {
                        col = col.withNullable(true);
                    }
                    Column existing = null;
                    int existingIdx = -1;
                    for (int ci = 0; ci < inheritedColumns.size(); ci++) {
                        if (inheritedColumns.get(ci).getName().equalsIgnoreCase(col.getName())) {
                            existing = inheritedColumns.get(ci);
                            existingIdx = ci;
                            break;
                        }
                    }
                    if (existing == null) { inheritedColumns.add(col); continue; }
                    // Two parents contributing one column have to agree about what it holds:
                    // the child gets one column, so a second parent's wider type has nowhere
                    // to go and its rows would silently change shape on the way in.
                    if (existing.getType() != col.getType()) {
                        // Which two types disagreed is the detail: the message names the column,
                        // and a reader with two parents in front of them has to be told which of
                        // the two the child would have had to store.
                        throw new MemgresException("inherited column \"" + col.getName()
                                + "\" has a type conflict\n  Detail: "
                                + existing.getType().toRegtypeDisplay() + " versus "
                                + col.getType().toRegtypeDisplay(), "42804");
                    }
                    // PostgreSQL ORs the parents' not-null flags: a column one parent declares
                    // NOT NULL is NOT NULL on the child whichever parent contributed it first.
                    // Letting the first parent's column stand threw the other's rule away, and
                    // the child then accepted a row PostgreSQL refuses.
                    if (!col.isNullable() && existing.isNullable()) {
                        inheritedColumns.set(existingIdx, existing.withNullable(false));
                    }
                    // A default the child does not override would have to be picked from two
                    // parents, and there is no rule for choosing.
                    if (!childDeclared.contains(col.getName().toLowerCase())
                            && existing.getDefaultValue() != null && col.getDefaultValue() != null
                            && !existing.getDefaultValue().equals(col.getDefaultValue())) {
                        // PostgreSQL says how to settle it: a default written on the child leaves
                        // nothing to choose between.
                        throw new MemgresException("column \"" + col.getName()
                                + "\" inherits conflicting default values"
                                + "\n  Hint: To resolve the conflict, specify a default explicitly.",
                                "42611");
                    }
                }
            }
        }

        // Handle LIKE tables
        List<StoredConstraint> likeConstraints = new ArrayList<>();
        // The columns a LIKE brings in are written into this definition as though they had been
        // spelled out, so PostgreSQL refuses a name two LIKEs bring, or one a LIKE and the
        // definition both name. An inherited column is merged instead, and is not counted here.
        Set<String> likeColumnNames = new HashSet<>();
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
                // LIKE copies the relation the name reaches, and for a view that is the view's own
                // column list. The DML resolver rewrites an auto-updatable view to the table
                // underneath it, so going through it copied the base table's columns, in the base
                // table's order, for a view that projects two of them.
                // LIKE copies a relation's columns, so the relation has to be one that has some:
                // a sequence and an index are found by name and then refused.
                DdlDefinitionChecks.requireLikeableSource(
                        executor.database, schemaName, likeTableName);
                Database.ViewDef likeView = executor.database.getView(likeTableName);
                Table likeTable;
                String likeTableSchema;
                Table likeComposite = compositeLikeSource(schemaName, likeTableName);
                if (likeComposite != null) {
                    likeTable = likeComposite;
                    likeTableSchema = likeTableName.indexOf('.') > 0
                            ? likeTableName.substring(0, likeTableName.indexOf('.')).toLowerCase()
                            : schemaName.toLowerCase();
                } else if (likeView != null && likeView.cachedColumns() != null) {
                    likeTable = new Table(likeView.name(),
                            new ArrayList<Column>(likeView.cachedColumns()));
                    likeTableSchema = likeView.schemaName() != null
                            ? likeView.schemaName().toLowerCase() : "public";
                } else {
                    likeTable = executor.resolveTable(schemaName, likeTableName);
                    // A column comment is kept under its table's own schema, so the source's
                    // schema has to be the one INCLUDING COMMENTS reads from.
                    likeTableSchema = likeTableName.indexOf('.') > 0
                            ? likeTableName.substring(0, likeTableName.indexOf('.')).toLowerCase()
                            : schemaOfRelation(likeTable.getName());
                }
                // LIKE copies the shape of a column and nothing more unless the option that
                // carries the rest is written out. Adding the source column object itself gave
                // the new table the source's defaults, its identity and its generation
                // expression, none of which PostgreSQL copies unasked.
                for (Column col : likeTable.getColumns()) {
                    if (!likeColumnNames.add(col.getName().toLowerCase())) {
                        throw PgErrors.duplicateColumn(col.getName());
                    }
                    boolean exists = inheritedColumns.stream()
                            .anyMatch(c -> c.getName().equalsIgnoreCase(col.getName()));
                    if (!exists) {
                        inheritedColumns.add(likeColumnCopy(col, likeOptions));
                        // A NOT NULL constraint travels with the column, under the name it has
                        // on the source table: PostgreSQL copies the constraint, not the rule.
                        if (!col.isNullable()) {
                            likeNotNullNames.put(col.getName().toLowerCase(),
                                    CatalogConstraintBuilder.notNullConstraintName(
                                            likeTable, col.getName()));
                        }
                        if (wants(likeOptions, "COMMENTS")) {
                            String srcComment = executor.database.getComment("column",
                                    Database.commentKey(likeTableSchema,
                                            likeTable.getName() + "." + col.getName()));
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
            // What each column's type is comes first: PostgreSQL settles every written type name
            // while it is still reading the definition, ahead of the names the columns were given
            // and of anything their DEFAULTs say. A width the type cannot have is part of settling
            // that, and so is reported from here too.
            DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(def.typeName(), def.precision());
            // A collation the database does not hold is not one a column can be declared with,
            // and PostgreSQL settles that where the clause is written -- resolving the name the
            // clause holds before asking whether the type it was written on carries a collation
            // at all, which is the last thing it asks about the clause.
            DdlDefinitionChecks.requireCollationExists(executor.database, def.collation);
            DdlDefinitionChecks.rejectUncollatableType(def.typeName(), resolved, def.collation);
            // A LIKE has already written its source's columns into this definition, so one of them
            // and a written column of the same name clash exactly as two written ones do.
            if (!definedColumnNames.add(def.name().toLowerCase())
                    || likeColumnNames.contains(def.name().toLowerCase())) {
                throw new MemgresException("column \"" + def.name() + "\" specified more than once", "42701");
            }
            DdlDefinitionChecks.rejectSystemColumnName(def.name());
            DdlDefinitionChecks.validateDefaultExpression(def.defaultExpr());

            DataType dataType = resolved.dataType();
            String enumTypeName = resolved.enumTypeName();
            String domainTypeName = resolved.domainTypeName();
            String compositeTypeName = resolved.compositeTypeName();
            DataType arrayElementType = resolved.arrayElementType();
            // A domain's own NOT NULL stays on the domain: PostgreSQL leaves attnotnull false for
            // a column merely declared with one, and files a single NOT NULL constraint against
            // the domain rather than one against every table that uses it. The value is still
            // rejected -- the domain's constraints are checked on every write.
            boolean notNull = def.notNull();

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
                // SEQUENCE NAME names the relation that feeds the column; without one the name is
                // composed the way PostgreSQL composes it. A qualified name says which schema the
                // sequence goes in, which need not be the table's.
                String writtenSeq = def.identitySequenceName();
                int seqDot = writtenSeq == null ? -1 : writtenSeq.lastIndexOf('.');
                String seqSchema = seqDot > 0 ? writtenSeq.substring(0, seqDot) : schemaName;
                String seqName = writtenSeq == null ? stmt.name() + "_" + def.name() + "_seq"
                        : (seqDot > 0 ? writtenSeq.substring(seqDot + 1) : writtenSeq);
                // An identity sequence is bounded by the column's own type, as a serial's is, and
                // every other option written for it is checked in PostgreSQL's own order. ALTER
                // TABLE ADD COLUMN builds the same sequence from the same helper, so an identity
                // column means the same thing whichever statement declared it.
                Sequence seq = buildIdentitySequence(def, dataType, seqName, seqSchema, stmt.name());
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(seqSchema, "sequence", seqName);
                if (def.identityOptionsWritten()) {
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
                seq.setSchemaName(schemaName);
                // The sequence belongs to this column and dies with it, which is what a later DROP
                // COLUMN or DROP TABLE reads rather than composing the name over again.
                seq.ownedBy(stmt.name(), def.name(), true);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                defaultVal = "nextval('" + seqName + "'::regclass)";
                notNull = true;
            }
            if (def.defaultExpr() != null) {
                defaultVal = DdlExecutor.exprToDefaultString(def.defaultExpr());
                if (dataType != null && TypeCoercion.categoryOf(dataType) == TypeCoercion.TypeCategory.NUMERIC) {
                    // Only a bare string literal is read with the column type's input function:
                    // it is still of type unknown, so the number is what it has to name. A literal
                    // that carries a type of its own -- true, 1.5 -- is not read as a number at
                    // all, and reading it made DEFAULT true on an integer column a complaint about
                    // input syntax where PostgreSQL says the types do not match.
                    if (DdlDefinitionChecks.isUntypedLiteral(def.defaultExpr())
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
                // A DEFAULT is read with no row in scope, so the only thing in it to resolve is
                // the calls it makes -- and PostgreSQL resolves those where the default is
                // written rather than at the first insert that takes it.
                StoredExprNames.read(ddl, def.defaultExpr(), null, null, false, false);
                // And whatever else the expression is, it has to produce a value this column can
                // hold. PostgreSQL settles that here, where the column is defined, rather than at
                // the first row that takes the default: a column whose default it cannot hold
                // leaves every INSERT that omits the column failing on a value nobody wrote.
                DdlDefinitionChecks.requireDefaultExprFits(def.defaultExpr(), resolved, def.name(),
                        columns);
            }

            // Override inherited column if exists
            int existingIdx = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equalsIgnoreCase(def.name())) {
                    existingIdx = i;
                    break;
                }
            }

            // Restating an inherited column adds to it and never loosens it: a child cannot be
            // less strict than the relation it takes its rows from, so PostgreSQL keeps the NOT
            // NULL a parent declares whatever the child's own column list leaves out. The rule
            // stays the parent's -- only the parent may withdraw it -- and a child that says NOT
            // NULL as well declares a second one of its own beside it.
            if (!notNull) {
                for (Table inheritParent : parentTables) {
                    if (CatalogConstraintBuilder.declaresNotNull(inheritParent, def.name())) {
                        notNull = true;
                        break;
                    }
                }
            }

            // A DEFAULT and a generation expression are both evaluated for one row at a time, with
            // no other row in sight; PostgreSQL names each context in its own words.
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    def.defaultExpr(), "DEFAULT expressions", null);
            Expression generated = null;
            if (def.generatedExpr() != null) {
                // A generation expression is kept as the text it was written as, so its type names
                // are read here rather than when the statement itself was parsed.
                List<String> generatedTypeSchemas = new ArrayList<>();
                try {
                    generated = com.memgres.engine.parser.Parser.parseExpression(
                            def.generatedExpr(), generatedTypeSchemas);
                } catch (RuntimeException ignored) {
                    // An expression that will not parse is reported by whatever reads it next
                }
                SchemaQualifier.rejectMissingTypeSchemas(
                        executor.database, executor.session, executor.getSystemCatalog(), generatedTypeSchemas);
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
                // A subquery is one in the parse tree, not in the text: a column named
                // "selected" and a literal spelling 'select' are neither of them a query, and
                // PostgreSQL takes both. Only an expression that would not parse is judged by
                // its text, there being no tree to ask.
                boolean hasSubquery = generated != null
                        ? AstWalk.anyMatch(generated, node -> node instanceof SelectStmt
                                || node instanceof SetOpStmt)
                        : def.generatedExpr().toLowerCase().replaceAll("\\s+", "").contains("select");
                if (hasSubquery) {
                    throw new MemgresException("cannot use subquery in column generation expression", "0A000");
                }
                // What the expression produces has to be a value the column can hold. A bare
                // literal is read with the column type's input function, and PostgreSQL reads it
                // when the column is defined -- exactly as it reads a DEFAULT written there.
                DdlDefinitionChecks.requireGenerationExprFits(generated, resolved, def.name(),
                        columns);
            }

            Integer colPrecision = def.precision() != null ? def.precision() : resolved.domainPrecision();
            Integer colScale = def.scale() != null ? def.scale() : resolved.domainScale();
            // The collation the definition names travels with the column, so the catalogue can
            // report it and a DROP of that collation can see that something depends on it.
            Column col = new Column(def.name(), dataType, !notNull, def.primaryKey(), defaultVal,
                    enumTypeName, colPrecision, colScale, def.generatedExpr(), def.generatedVirtual(),
                    domainTypeName, compositeTypeName, arrayElementType);
            col.setCollation(def.collation);
            // A range is a type of its own, as an enum and a composite are, and the column is a
            // column of it however its values are carried underneath.
            col.setRangeTypeName(resolved.rangeTypeName());
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
                    // The parent's type comes first: it is the one already holding rows, and the
                    // second is what this definition asked for.
                    throw new MemgresException("column \"" + def.name() + "\" has a type conflict"
                            + "\n  Detail: " + inheritedCol.getType().toRegtypeDisplay()
                            + " versus " + col.getType().toRegtypeDisplay(), "42804");
                }
                columns.set(existingIdx, col);
            } else {
                columns.add(col);
            }
        }

        // Validate generated column expressions
        validateGeneratedColumns(stmt.columns(), columns, stmt.name(), schemaName);
        rejectKeysOnVirtualColumns(stmt, columns);

        // A NOT NULL written NO INHERIT is a rule about this relation's rows alone, and one taken
        // from a parent is a rule the relation holds on that parent's behalf: PostgreSQL refuses
        // the definition rather than merge the two. It settles the not-null constraints after it
        // has read every column definition, so a fault in any of those is what the writer is told
        // about first.
        for (ColumnDef notInherited : stmt.columns()) {
            if (!notInherited.notNullNoInherit()) continue;
            for (Table inheritParent : parentTables) {
                if (!CatalogConstraintBuilder.declaresNotNull(inheritParent, notInherited.name())) {
                    continue;
                }
                MemgresException conflict = PgErrors.datatypeMismatch(
                        "cannot define not-null constraint with NO INHERIT on column \""
                                + notInherited.name() + "\"").suppressPosition();
                conflict.setDetail("The column has an inherited not-null constraint.");
                throw conflict;
            }
        }
        // A partitioned parent holds no rows of its own, so there is nothing for UNLOGGED to
        // mean; PostgreSQL refuses the combination outright rather than silently ignoring it.
        if (stmt.unlogged() && stmt.partitionBy() != null) {
            throw new MemgresException("partitioned tables cannot be unlogged", "0A000");
        }
        Table table = new Table(stmt.name(), columns);
        if (stmt.unlogged()) table.setUnlogged(true);
        // A typed table goes on belonging to its type: the type may not be dropped while the
        // table stands, and no column may be added to the table beside the ones the type declares.
        table.setOfTypeName(stmt.ofType());
        if (stmt.withOptions() != null && !stmt.withOptions().isEmpty()) {
            // A storage parameter is read while the table is being defined: an unknown name or a
            // value outside the range stops the statement rather than being stored as written.
            DdlIndexValidator.checkRelOptions("heap", stmt.withOptions());
            // A boolean or enumerated value is stored in the case PostgreSQL reads it back in: the
            // lexer hands keyword tokens over upper-cased, and pg_class.reloptions then reported
            // FALSE where PostgreSQL reports false.
            table.setReloptions(DdlIndexValidator.normalizeRelOptions("heap", stmt.withOptions()));
        }

        // Set up inheritance links
        for (Table parent : parentTables) {
            table.setParentTable(parent);
            parent.addChild(table);
        }

        // A NOT NULL written NO INHERIT answers for this relation's rows alone, and nothing
        // declared under it later takes it: PostgreSQL hands the column down bare.
        for (ColumnDef def : stmt.columns()) {
            if (def.notNullNoInherit()) table.markNotNullNoInherit(def.name());
        }

        // What the child's own column list named is the child's own, and the rest it holds only
        // because a parent declares it. PostgreSQL keeps the two questions apart -- attislocal and
        // conislocal say the relation declared it, the inheritance counts say how many parents
        // also do -- so a child that restates an inherited column, or restates NOT NULL on one,
        // is told it declared that much for itself and holds a constraint under its own name.
        for (Column inherited : inheritedColumns) {
            ColumnDef restated = null;
            for (ColumnDef def : stmt.columns()) {
                if (def.name().equalsIgnoreCase(inherited.getName())) { restated = def; break; }
            }
            if (restated == null) table.markColumnInherited(inherited.getName());
            if (!inherited.isNullable()
                    && (restated == null || !(restated.notNull() || restated.primaryKey()))) {
                table.markNotNullInherited(inherited.getName());
            }
        }

        // Every row of a partition is a row of the partitioned table, so a NOT NULL declared on
        // the partitioned table has to reach the partitions: one marked NO INHERIT asks for a rule
        // the hierarchy cannot express, and PostgreSQL says so rather than storing a declaration
        // the partitions would quietly ignore. It says it before it reads the partition key or any
        // of the CHECK constraints, so a fault in either of those is not what the writer is told.
        if (stmt.partitionBy() != null) {
            for (ColumnDef notInherited : stmt.columns()) {
                if (!notInherited.notNullNoInherit()) continue;
                throw new MemgresException(
                        "not-null constraints on partitioned tables cannot be NO INHERIT", "0A000")
                        .suppressPosition();
            }
        }

        // Set up partitioning
        if (stmt.partitionBy() != null) {
            table.setPartitionStrategy(stmt.partitionBy());
            String partCol = stmt.partitionColumn();
            if (partCol != null) {
                List<String> partKeys = splitTopLevel(partCol);
                // PostgreSQL analyses every key expression of the list before it judges any key
                // against the relation, so a call that may not stand in a key is reported even
                // when an earlier key is one the relation cannot be partitioned on at all.
                for (String col : partKeys) {
                    String trimmed = col.trim();
                    if (trimmed.contains("(")) analysePartitionKeyExpression(trimmed, table);
                }
                for (String col : partKeys) {
                    String trimmed = col.trim();
                    // An expression key decides which partition a row lives in, so it has to
                    // give the same answer every time it is asked. A volatile or stable one
                    // may route a row to one partition on INSERT and look for it in another.
                    if (trimmed.contains("(")) {
                        checkPartitionKeyExpression(trimmed, columns, table);
                        continue;
                    }
                    // Every relation carries the system columns whether or not the definition
                    // named them, so PostgreSQL finds one written here and refuses it for what it
                    // is rather than reporting a column the relation does not have.
                    if (table.getColumnIndex(trimmed) < 0
                            && DdlDefinitionChecks.isSystemColumnName(trimmed)) {
                        throw new MemgresException("cannot use system column \"" + trimmed
                                + "\" in partition key", "42P17");
                    }
                    if (table.getColumnIndex(trimmed) < 0) {
                        throw new MemgresException("column \"" + trimmed + "\" named in partition key does not exist", "42703");
                    }
                    rejectGeneratedPartitionKeyColumn(table, trimmed);
                }
            }
            table.setPartitionColumn(partCol);
        }

        schema.addTable(table);
        // A relation created under a name that once carried rules starts clean: the pg_class row
        // that remembered them went with the relation that was dropped. Only this schema's are
        // forgotten -- another schema's relation of the name is a different relation.
        executor.database.clearRuleHistory(schemaName, stmt.name());
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
            executor.session.registerOnCommitDeleteRows(schemaName, stmt.name(), table);
        }

        // Store column-level constraints
        for (ColumnDef def : stmt.columns()) {
            // A NOT NULL the definition named answers to that name, not to the generated one; the
            // column is already non-nullable by here, which is what the name attaches to.
            if (def.notNullName() != null) {
                table.setNotNullConstraintName(def.name(), def.notNullName());
            }
            // CREATE TABLE and ALTER TABLE ADD COLUMN declare these in exactly the same words, so
            // they store exactly the same constraints; keeping a second copy of the rules here is
            // what let the two drift apart.
            storeInlineColumnConstraints(table, def, schemaName, stmt.name(),
                    new ArrayList<StoredConstraint>());
        }

        // Store table-level constraints
        if (stmt.constraints() != null) {
            for (TableConstraint tc : stmt.constraints()) {
                if (tc.type() == TableConstraint.ConstraintType.NOT_NULL) {
                    for (String colName : tc.columns()) {
                        table.alterColumnNullable(colName, false);
                        // The name the clause was written with is the constraint's own -- it is
                        // what pg_constraint lists and what DROP CONSTRAINT asks for. The column
                        // has to be non-nullable first for the name to attach to anything.
                        if (tc.name() != null) table.setNotNullConstraintName(colName, tc.name());
                    }
                    continue;
                }
                if (tc.type() == TableConstraint.ConstraintType.CHECK) {
                    // A CHECK answers yes or no about one row, and a set-returning call answers
                    // with rows: there is nothing there for the constraint to test.
                    if (executor.selectExecutor.containsSrf(tc.checkExpr())) {
                        throw PgErrors.notImplemented(
                                "set-returning functions are not allowed in check constraints");
                    }
                    // What the predicate is refused for is decided by where the faults are
                    // written, not by the order these checks run in: PostgreSQL settles each name
                    // and each call at the node it stands at, so a column nothing answers to
                    // written ahead of a sub-query is what it complains about.
                    StoredExprNames.read(ddl, tc.checkExpr(), table, null, true, true);
                    // A CHECK is tested against the row being written, on its own; it can see no
                    // other row, so nothing in it may need a group or a finished result — and no
                    // call in it may carry a clause only an aggregate has a use for.
                    executor.selectExecutor.placementCheck.rejectStoredDefinition(
                            tc.checkExpr(), "check constraints", "check constraint");
                    DdlDefinitionChecks.requireBooleanPredicate(tc.checkExpr(), table, "CHECK");
                    // And it is stored against this table's columns, so every name in it has to be
                    // one of them and none may be a system column.
                    ddl.validateExprColumnRefs(tc.checkExpr(), table, null, true, true);
                    DdlDefinitionChecks.rejectSystemColumnInCheck(tc.checkExpr());
                    // A partition always carries the partitioned table's CHECK constraints, so one
                    // declared NO INHERIT there asks for a rule the hierarchy cannot express --
                    // the refusal ALTER TABLE ... ADD CONSTRAINT already makes. Ordinary
                    // inheritance can express it and PostgreSQL accepts it there. The rule is read
                    // after the expression it tests, so a name the relation does not have is still
                    // what the writer is told about first.
                    if (tc.noInherit() && table.getPartitionStrategy() != null) {
                        throw new MemgresException(
                                "cannot add NO INHERIT constraint to partitioned table \""
                                        + stmt.name() + "\"", "42P16").suppressPosition();
                    }
                    // A written constraint name is used as it stands, so two of them in one
                    // statement left a name DROP CONSTRAINT could reach only one of.
                    if (tc.name() != null && table.getConstraint(tc.name()) != null) {
                        throw new MemgresException("check constraint \"" + tc.name()
                                + "\" already exists", "42710");
                    }
                }
                // A key over a column the table does not have was stored with an attribute number
                // nothing answers to, and enforced nothing; a column named twice is a fault in the
                // key rather than a repetition to collapse.
                if (tc.type() == TableConstraint.ConstraintType.PRIMARY_KEY
                        || tc.type() == TableConstraint.ConstraintType.UNIQUE
                        || tc.type() == TableConstraint.ConstraintType.EXCLUDE) {
                    DdlDefinitionChecks.validateKeyColumns(table, tc.columns(),
                            tc.type() == TableConstraint.ConstraintType.PRIMARY_KEY ? "primary key"
                                    : tc.type() == TableConstraint.ConstraintType.UNIQUE ? "unique"
                                    : "exclusion");
                    DdlDefinitionChecks.requireKeyColumnsExist(table, tc.includedColumns());
                    if (tc.type() == TableConstraint.ConstraintType.EXCLUDE) {
                        DdlDefinitionChecks.requireExclusionCapableAccessMethod(tc.excludeMethod());
                        // And with an operator the index can compare either way round.
                        DdlDefinitionChecks.requireCommutativeExclusionOperators(
                                table, tc.excludeElements());
                        // And with one the index's own operator class knows about.
                        DdlDefinitionChecks.requireExclusionOperatorInFamily(
                                table, tc.excludeElements(), tc.excludeMethod());
                    }
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

        // A CHECK belongs to the whole hierarchy, so the child gets its own copy of each of its
        // parents' -- enforced over the child's own rows, under the name the parent gave it,
        // which is the name a violation reports. NO INHERIT says the rule was never going to
        // travel, and a rule the child restates under the same name is the child's own.
        for (Table inheritedParent : parentTables) {
            for (StoredConstraint parentCheck : inheritedParent.getConstraints()) {
                if (parentCheck.getType() != StoredConstraint.Type.CHECK
                        || parentCheck.isNoInherit() || parentCheck.getName() == null) {
                    continue;
                }
                if (table.getConstraint(parentCheck.getName()) != null) continue;
                StoredConstraint inherited = parentCheck.copyForPartition(stmt.name());
                inherited.setInheritedFrom(inheritedParent.getName());
                table.addConstraint(inherited);
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
                    Database.commentKey(schemaName, stmt.name() + "." + cc.getKey()),
                    cc.getValue());
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
                executor.database.addIndex(schemaName, newIdxName, new ArrayList<>(srcCols));
                executor.database.addIndexMeta(schemaName, newIdxName,
                        schemaName + "." + newTableName, isUnique, method, null);
            }
        }

        executor.database.setObjectOwner("table:" + schemaName + "." + stmt.name(), executor.currentRole());
        // M11: Apply ALTER DEFAULT PRIVILEGES to newly created table
        applyDefaultPrivileges(schemaName, stmt.name(), executor.currentRole());
        // What the definition cost is settled here, where the columns and constraints it turned
        // out to have are in hand: a column wide enough to be moved out of line, a key that needs
        // an index behind it and a serial's sequence are each written under a command identifier
        // of their own, so the row the next statement writes reads a higher cmin for each.
        if (executor.session != null) {
            executor.session.noteCatalogRowsWritten(CommandIdCost.forCreatedTable(
                    executor.database, stmt, table, schemaName, temporarySchemaMadeWithIt));
        }
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
        // A partition declares nothing of its own: the NOT NULL on its columns is the partitioned
        // table's rule, and PostgreSQL goes on reporting that table's constraint name for it.
        for (Column partCol : parent.getColumns()) {
            if (!partCol.isNullable()) partition.markNotNullInherited(partCol.getName());
        }

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
                // A UNIQUE that is really a unique index on the partitioned table travels with the
                // indexes below, under the name PostgreSQL derives for the partition's own index.
                // Copying it here as well indexed the partition twice for one declaration, and
                // named a duplicate key after a constraint PostgreSQL holds no row for.
                if (sc.isFromIndex()) continue;
                partition.addConstraint(sc.copyForPartition(stmt.name()));
            } else if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                    || sc.getType() == StoredConstraint.Type.CHECK) {
                // The rows live in the partition, so the constraints the parent declares have to
                // be enforced there. Copying only the key constraints left a foreign key on a
                // partitioned table checking nothing at all: a row could name a parent row that
                // was never there, and deleting the referenced row left it behind.
                StoredConstraint inherited = sc.copyForPartition(stmt.name());
                inherited.setInheritedFrom(parent.getName());
                partition.addConstraint(inherited);
            }
        }

        if (stmt.partitionBounds() != null && !stmt.partitionBounds().isEmpty()) {
            applyPartitionBounds(partition, parent, stmt.partitionBounds(), stmt.name());
        }

        if (stmt.partitionBy() != null) {
            partition.setPartitionStrategy(stmt.partitionBy());
            partition.setPartitionColumn(stmt.partitionColumn());
        }

        // A default partition holds exactly the rows no other partition claims, so a new
        // partition narrows what it may hold: a row the default is already holding which the new
        // bounds would now cover makes the default's own constraint false. ATTACH PARTITION asks
        // the same question, and PostgreSQL refuses both rather than stranding the row where
        // nothing would find it.
        if (stmt.partitionBounds() != null && !stmt.partitionBounds().isEmpty()) {
            DdlAlterTableExecutor.validateDefaultPartitionRows(parent, partition, stmt.name());
        }

        // Attach to the parent only after bound validation succeeded, so a rejected
        // partition (e.g. overlapping bounds) doesn't linger in the parent's routing list
        parent.addPartition(partition);
        schema.addTable(partition);
        // An index on a partitioned table is a rule about the whole hierarchy, so a partition
        // created after it gets its own copy -- the copy CREATE INDEX itself makes for the
        // partitions that already exist. Without this the index reached only the partitions that
        // happened to be declared first.
        copyParentIndexes(parent, partition, schemaName, schemaName);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));
        if (executor.session != null) {
            executor.session.noteCatalogRowsWritten(
                    CommandIdCost.forCreatedPartition(executor.database, schemaName, partition));
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * Give a partition its own copy of every index registered on the partitioned table, named
     * and parented the way CREATE INDEX names the copies it makes for partitions that already
     * exist, so a partition is indexed the same whichever of the two statements came first.
     */
    void copyParentIndexes(Table parent, Table partition, String parentSchema,
                           String partitionSchema) {
        String parentQualified = parentSchema + "." + parent.getName();
        for (String indexKey : new ArrayList<>(executor.database.getIndexColumns().keySet())) {
            String owner = executor.database.getIndexTable(indexKey);
            if (owner == null || !owner.equalsIgnoreCase(parentQualified)) continue;
            copyParentIndex(parent, partition, partitionSchema, indexKey);
        }
    }

    /**
     * Give one partition its own copy of one index on the partitioned table. The name is the one
     * PostgreSQL derives for the partition, not the parent's index name: an index belongs to the
     * relation whose rows it reads, and for a unique index that name is what a duplicate key is
     * reported against.
     */
    void copyParentIndex(Table parent, Table partition, String partitionSchema, String indexKey) {
        List<String> indexCols = executor.database.getIndexColumns(indexKey);
        if (indexCols == null || indexCols.isEmpty()) return;
        boolean unique = executor.database.isUniqueIndex(indexKey);
        String whereClause = executor.database.getIndexWhereClause(indexKey);
        // An index the partition already carries over the same columns is the copy: PostgreSQL
        // attaches that one to the partitioned table's index rather than building a second index
        // over the same rows, so a table indexed before it joined the hierarchy keeps what it has.
        String adopted = matchingIndexOn(partitionSchema, partition, indexCols, unique, whereClause);
        if (adopted != null) {
            String adoptedKey = Database.idxKey(partitionSchema, adopted);
            executor.database.setIndexParent(adoptedKey, indexKey);
            copyIndexToSubPartitions(partition, adoptedKey);
            return;
        }
        // The columns an INCLUDE clause carries belong to the index and take part in the name
        // PostgreSQL derives for it, so the copy is given both.
        List<String> includeCols = executor.database.getIndexIncludeColumns(indexKey);
        String childIdxName = IndexNameChooser.choose(executor.database, partitionSchema,
                partition.getName(), partition, indexCols, includeCols);
        executor.database.addIndex(partitionSchema, childIdxName, new ArrayList<>(indexCols));
        executor.database.addIndexMeta(partitionSchema, childIdxName,
                partitionSchema + "." + partition.getName(), unique,
                executor.database.getIndexMethod(indexKey), whereClause);
        executor.database.setIndexIncludeColumns(partitionSchema, childIdxName, includeCols);
        executor.database.registerSchemaObject(partitionSchema, "index", childIdxName);
        executor.database.setIndexParent(Database.idxKey(partitionSchema, childIdxName), indexKey);
        // A unique index rejects a duplicate through the constraint it records, not through
        // the index alone, so the partition takes the parent's copy as well -- under the name
        // of its own index, which is the name PostgreSQL reports for the violation.
        if (unique) {
            StoredConstraint source = uniqueIndexConstraint(parent, indexCols);
            if (source != null && partition.getConstraint(childIdxName) == null) {
                StoredConstraint copy = source.copyForPartition(partition.getName());
                copy.setName(childIdxName);
                partition.addConstraint(copy);
            }
        }
        // A partition that is itself partitioned holds no rows of its own, so the copy it has just
        // been given is in turn a rule about its own partitions: PostgreSQL carries an index down
        // every branch of the hierarchy until it reaches the tables that do hold rows. Stopping one
        // level below the index left every leaf under an intermediate partitioned table unindexed.
        copyIndexToSubPartitions(partition, Database.idxKey(partitionSchema, childIdxName));
        // Only a plain column list can be looked up in the row store, so an expression or a
        // partial index is registered and left at that -- the same test CREATE INDEX makes
        // before it builds one for a partition.
        if (whereClause != null) return;
        int[] colIndices = new int[indexCols.size()];
        for (int ci = 0; ci < indexCols.size(); ci++) {
            colIndices[ci] = partition.getColumnIndex(indexCols.get(ci));
            if (colIndices[ci] < 0) return;
        }
        if (partition.getIndex(childIdxName) == null) {
            partition.buildIndex(new TableIndex(childIdxName, colIndices, unique));
        }
    }

    /**
     * An index this relation already carries that reads the same columns under the same rules and
     * is not already some other index's copy. PostgreSQL takes such an index as the partition's
     * copy of the partitioned table's, which is why a second index over one column gets a copy of
     * its own rather than sharing the first one's.
     */
    private String matchingIndexOn(String partitionSchema, Table partition, List<String> cols,
                                   boolean unique, String whereClause) {
        String qualified = partitionSchema + "." + partition.getName();
        for (String key : new ArrayList<>(executor.database.getIndexColumns().keySet())) {
            String owner = executor.database.getIndexTable(key);
            if (owner == null || !owner.equalsIgnoreCase(qualified)) continue;
            if (executor.database.getIndexParentMap().containsKey(key)) continue;
            if (executor.database.isUniqueIndex(key) != unique) continue;
            String candidateWhere = executor.database.getIndexWhereClause(key);
            if (whereClause == null ? candidateWhere != null
                    : !whereClause.equalsIgnoreCase(candidateWhere)) {
                continue;
            }
            if (!sameColumns(executor.database.getIndexColumns(key), cols)) continue;
            return Database.idxName(key);
        }
        return null;
    }

    /**
     * The constraint CREATE UNIQUE INDEX left behind on the table it built the index on, found by
     * the columns the index reads. It is the constraint that refuses a duplicate row, so a
     * partition that copies the index has to copy this with it or the index enforces nothing.
     */
    private static StoredConstraint uniqueIndexConstraint(Table table, List<String> indexCols) {
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.UNIQUE || !sc.isFromIndex()) continue;
            if (sameColumns(sc.getColumns(), indexCols)) return sc;
        }
        return null;
    }

    /** The same columns in the same order, compared the way SQL compares unquoted names. */
    private static boolean sameColumns(List<String> left, List<String> right) {
        if (left == null || right == null || left.size() != right.size()) return false;
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i) == null || !left.get(i).equalsIgnoreCase(right.get(i))) return false;
        }
        return true;
    }

    /**
     * A partition that is itself partitioned is not where the rows are, so the copy of an index it
     * has just been given is passed on to its own partitions, and to theirs, until the tables that
     * hold rows are reached. Each level is named after the relation it indexes, which is why the
     * names lengthen one segment at a time down a hierarchy.
     */
    private void copyIndexToSubPartitions(Table partition, String indexKey) {
        if (partition.getPartitionStrategy() == null) return;
        for (Table sub : new ArrayList<>(partition.getPartitions())) {
            copyParentIndex(partition, sub, sub.getSchemaName(), indexKey);
        }
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
                        + "  Detail: " + constraintKind + " constraint on table \"" + table.getName()
                        + "\" lacks column \"" + partKey + "\" which is part of the partition key.",
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
                    MemgresException e = new MemgresException(
                            "every hash partition modulus must be a factor of the next larger modulus",
                            "42P17");
                    // Which of the two moduli is the larger decides how PostgreSQL words the
                    // relation that fails to hold between them.
                    e.setDetail("The new modulus " + modulus
                            + (modulus > otherModulus ? " is not divisible by " : " is not a factor of ")
                            + otherModulus + ", the modulus of existing partition \""
                            + existingPart.getName() + "\".");
                    throw e;
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
    private Object coerceBoundToKeyType(Object value, List<Column> keyCols, int keyIndex) {
        if (value == null || value instanceof PartitionBound) return value;
        if (keyCols == null || keyIndex < 0 || keyIndex >= keyCols.size()) return value;
        Column keyCol = keyCols.get(keyIndex);
        Object coerced;
        try {
            coerced = TypeCoercion.coerce(value, keyCol.getType());
        } catch (MemgresException e) {
            // Keeping a bound this engine could not read as the key's type would leave a
            // partition that routing can never match; PG rejects the definition instead.
            throw e;
        } catch (Exception e) {
            coerced = value;
        }
        // An enum orders by the position its labels were declared in, not by their text. Left as
        // a plain string the bound compared lexicographically, so FROM ('lo') TO ('hi') read as an
        // empty range and a row of the type found no partition at all.
        if (keyCol.getType() == DataType.ENUM && keyCol.getEnumTypeName() != null
                && !(coerced instanceof AstExecutor.PgEnum)) {
            CustomEnum boundEnum = executor.database.getCustomEnum(keyCol.getEnumTypeName());
            if (boundEnum != null) {
                String label = String.valueOf(coerced);
                if (!boundEnum.isValidLabel(label)) {
                    throw new MemgresException("invalid input value for enum "
                            + TypeNamespace.display(executor.database, executor.session,
                                    keyCol.getEnumTypeName()) + ": \"" + label + "\"", "22P02");
                }
                coerced = new AstExecutor.PgEnum(label, keyCol.getEnumTypeName(),
                        boundEnum.ordinal(label));
            }
        }
        // A bound is a value of the key column's type, so a domain's CHECK applies to it. Storing
        // one the domain refuses builds a partition no row could ever be routed to.
        if (keyCol.getDomainTypeName() != null) {
            executor.castValue(coerced, keyCol.getDomainTypeName());
        }
        return coerced;
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
            else if (v instanceof String || v instanceof AstExecutor.PgEnum) {
                sb.append('\'').append(v).append('\'');
            } else sb.append(v);
        }
        return sb.toString();
    }

    /**
     * The sequence that feeds an identity column, built from the options written for it.
     *
     * <p>The whole option list goes through the checking CREATE SEQUENCE uses, because PostgreSQL
     * applies the same rules to both: CACHE 0, a MINVALUE above the MAXVALUE and a zero INCREMENT
     * are refused in the same words and the same order. An unstated bound comes from the column's
     * own type, so an int identity still stops at 2147483647 and a smallint one at 32767.
     */
    static Sequence buildIdentitySequence(ColumnDef def, DataType columnType, String seqName,
                                          String schemaName, String tableName) {
        String asType = columnType == DataType.SMALLINT ? "smallint"
                : columnType == DataType.BIGINT ? "bigint" : "integer";
        DdlSequenceValidator.Params p = DdlSequenceValidator.forCreate(asType,
                def.identityIncrement(), def.identityMinValue(), def.identityMaxValue(),
                def.identityStart(), def.identityCache());
        Sequence seq = new Sequence(seqName, p.startWith, p.incrementBy, p.minValue, p.maxValue);
        DdlSequenceValidator.apply(seq, p);
        if (def.identityCycle() != null) seq.setCycle(def.identityCycle().booleanValue());
        seq.setSchemaName(schemaName);
        // The sequence exists to feed this column, so it goes when the column does. Composing
        // <table>_<column>_seq again at drop time answered for a name a renamed column no longer
        // has, and left the sequence behind.
        seq.ownedBy(tableName, def.name(), true);
        return seq;
    }

    /**
     * Store the constraints a column's own definition carries — PRIMARY KEY, UNIQUE and
     * REFERENCES — under the names PostgreSQL gives them. CREATE TABLE and ALTER TABLE ADD COLUMN
     * declare them in exactly the same words, so they store exactly the same constraints; the
     * ALTER path read none of them, and accepted duplicate keys and unparented rows in silence.
     *
     * @param added collects what was stored, so a caller that has to undo can take it back out
     */
    void storeInlineColumnConstraints(Table table, ColumnDef def, String schemaName,
                                      String tableName, List<StoredConstraint> added) {
        if (def.primaryKey()) {
            StoredConstraint pk = StoredConstraint.primaryKey(
                    def.primaryKeyName() != null ? def.primaryKeyName() : tableName + "_pkey",
                    Cols.listOf(def.name()));
            // A column-level key carries its own DEFERRABLE, exactly as a table-level one does.
            pk.setDeferrable(def.deferrable());
            pk.setInitiallyDeferred(def.initiallyDeferred());
            table.addConstraint(pk);
            added.add(pk);
        }
        if (def.unique()) {
            StoredConstraint uq = StoredConstraint.unique(
                    def.uniqueName() != null ? def.uniqueName()
                            : tableName + "_" + def.name() + "_key",
                    Cols.listOf(def.name()));
            uq.setDeferrable(def.deferrable());
            uq.setInitiallyDeferred(def.initiallyDeferred());
            table.addConstraint(uq);
            added.add(uq);
        }
        if (def.referencesTable() != null) {
            added.add(addColumnForeignKey(table, def, schemaName, tableName));
        }
    }

    StoredConstraint addColumnForeignKey(Table table, ColumnDef def, String schemaName, String tableName) {
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
        return fk;
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
        rejectSystemColumnInPartitionKeyExpression(keyExpr, table);
        rejectGeneratedPartitionKeyColumn(table, lowestGeneratedColumnNamed(keyExpr, table));
        DdlExecutor.checkExpressionImmutability(keyExpr, executor.database,
                PARTITION_KEY_NOT_IMMUTABLE);
        rejectTimeZoneDependentCast(keyExpr, columns);
    }

    /**
     * What PostgreSQL settles about a key expression while it is still analysing the statement,
     * before it has looked at any key against the relation: the names it holds and the calls that
     * may not stand in something evaluated for one row with no query around it.
     */
    private void analysePartitionKeyExpression(String keyText, Table table) {
        Expression keyExpr;
        try {
            keyExpr = com.memgres.engine.parser.Parser.parseExpression(keyText);
        } catch (RuntimeException notAnExpression) {
            return; // whatever reads it next reports it
        }
        // Both faults come out of the one pass PostgreSQL makes over the expression, from the
        // leaves outwards, so whichever of them it reaches first is the one the writer is told
        // about: (sum(i) + nosuch) is the aggregate and (nosuch + sum(i)) is the name.
        final PlacementCheck placement = executor.selectExecutor.placementCheck;
        placement.forEachAnalysed(keyExpr, node -> {
            placement.rejectStoredDefinition(
                    node, "partition key expressions", "partition key expression");
            rejectUnknownPartitionKeyColumn(node, table);
        });
    }

    /** A key expression reads the row it routes, so every name in it has to be a column. */
    private void rejectUnknownPartitionKeyColumn(Object node, Table table) {
        if (!(node instanceof ColumnRef)) return;
        ColumnRef ref = (ColumnRef) node;
        if (ref.table() != null) return;
        // A system column resolves like any other, and is refused for being one rather than
        // for not being there.
        if (DdlDefinitionChecks.isSystemColumnName(ref.column())) return;
        if (table.getColumnIndex(ref.column()) < 0) {
            throw new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
        }
    }

    /**
     * A key expression is stored against the relation's own columns, and a system column is none
     * of them: what it holds is settled by where the row was written rather than by the row.
     */
    private void rejectSystemColumnInPartitionKeyExpression(Object node, Table table) {
        AstWalk.forEach(node, n -> {
            if (!(n instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) n;
            if (table.getColumnIndex(ref.column()) >= 0) return;
            if (!DdlDefinitionChecks.isSystemColumnName(ref.column())) return;
            throw new MemgresException(
                    "partition key expressions cannot contain system column references", "42P17");
        });
    }

    /**
     * The generated column an expression key reads that stands earliest in the relation.
     * PostgreSQL collects the columns an expression names into a set ordered by their place in the
     * relation, so the one it reports is the first of them and not the first one written.
     */
    private String lowestGeneratedColumnNamed(Object node, Table table) {
        int[] lowest = new int[] { Integer.MAX_VALUE };
        AstWalk.forEach(node, n -> {
            if (!(n instanceof ColumnRef)) return;
            int idx = table.getColumnIndex(((ColumnRef) n).column());
            if (idx >= 0 && idx < lowest[0] && table.getColumns().get(idx).isGenerated()) {
                lowest[0] = idx;
            }
        });
        return lowest[0] == Integer.MAX_VALUE ? null : table.getColumns().get(lowest[0]).getName();
    }

    /**
     * Refuse a partition key that reads a generated column.
     *
     * <p>PostgreSQL routes a row to its partition before any trigger runs and before a stored
     * generated column is computed, and a virtual one is never stored at all, so the key would be
     * read while it still holds nothing to route by.
     */
    private void rejectGeneratedPartitionKeyColumn(Table table, String columnName) {
        if (columnName == null) return;
        int idx = table.getColumnIndex(columnName);
        if (idx < 0 || !table.getColumns().get(idx).isGenerated()) return;
        MemgresException ex =
                new MemgresException("cannot use generated column in partition key", "42P17");
        ex.setDetail("Column \"" + table.getColumns().get(idx).getName()
                + "\" is a generated column.");
        throw ex;
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

    private void validateGeneratedColumns(List<ColumnDef> columnDefs, List<Column> columns,
                                          String relationName, String schemaName) {
        Set<String> generatedColNames = new HashSet<>();
        Set<String> allColNames = new HashSet<>();
        for (ColumnDef def : columnDefs) {
            if (def.generatedExpr() != null) generatedColNames.add(def.name().toLowerCase());
            allColNames.add(def.name().toLowerCase());
        }
        Table declared = new Table(relationName, columns);
        // The relation answers to the schema it is being created in, so an expression naming that
        // schema in front of the relation is naming this relation and not another schema's of the
        // same name. Judging every definition against public refused a self-reference written out
        // in full anywhere else.
        declared.setSchemaName(schemaName);
        for (ColumnDef def : columnDefs) {
            if (def.generatedExpr() != null) {
                Set<String> selfNamed = selfQualifiers(def.generatedExpr(), declared);
                List<String> referencedIdents = DdlExecutor.extractIdentifiers(def.generatedExpr());
                for (String ident : referencedIdents) {
                    String identLower = ident.toLowerCase();
                    if (selfNamed.contains(identLower)) continue;
                    if (!allColNames.contains(identLower)) {
                        if (!DdlExecutor.isSqlKeywordOrFunction(identLower)) {
                            throw new MemgresException("column \"" + ident + "\" does not exist", "42703");
                        }
                    } else if (generatedColNames.contains(identLower)) {
                        MemgresException e = new MemgresException(
                                "cannot use generated column \"" + ident + "\" in column generation expression", "42P17");
                        e.setDetail("A generated column cannot reference another generated column.");
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * The names a generation expression uses as a qualifier that stand for the relation being
     * defined.
     *
     * <p>PostgreSQL lets the expression name that relation — {@code GENERATED ALWAYS AS (t.a)} —
     * and stores a reference to the relation rather than the name that was written, so the
     * qualifier is judged where it is written and is not a name to go looking for a column under
     * afterwards. A qualifier naming anything else is a relation the statement never mentioned,
     * which PostgreSQL reports as the missing FROM-clause entry it is; the scan over the
     * expression's text below cannot tell the two apart, so the parse tree is asked instead.
     *
     * <p>The relation may be named in full — {@code public.t.a} — and PostgreSQL takes the whole
     * name down to the relation, so the schema written in front of it belongs to that name too
     * rather than being a name of its own to go looking for a column under.
     */
    private Set<String> selfQualifiers(String written, final Table declared) {
        final Set<String> named = new HashSet<>();
        Expression parsed;
        try {
            parsed = com.memgres.engine.parser.Parser.parseExpression(written);
        } catch (RuntimeException notReadable) {
            return named; // an expression that will not parse is judged by its text alone
        }
        StoredExprNames.read(ddl, parsed, declared, null, false, true);
        AstWalk.forEach(parsed, new java.util.function.Consumer<Object>() {
            @Override
            public void accept(Object node) {
                if (!(node instanceof ColumnRef)) return;
                ColumnRef ref = (ColumnRef) node;
                String qualifier = ref.table();
                if (qualifier != null && qualifier.equalsIgnoreCase(declared.getName())) {
                    named.add(qualifier.toLowerCase());
                    // A schema written in front of it has already been judged above, so what is
                    // left here is this relation's own schema, spelled out.
                    if (ref.schema() != null) named.add(ref.schema().toLowerCase());
                }
            }
        });
        return named;
    }

    // ---- DROP TABLE ----

    QueryResult executeDropTable(DropTableStmt stmt) {
        // One DROP naming several tables drops them together, so a table in the list is not a
        // dependency that blocks another one in the same list — PostgreSQL takes the whole set.
        // Every name in the list is resolved to a relation before any of them goes, so a name that
        // carries a schema of its own is looked for in that schema and a relation the list names
        // twice is dropped once rather than reported missing the second time.
        List<String[]> targets = new ArrayList<>();
        Set<String> together = new HashSet<>();
        Set<String> alreadyNamed = new HashSet<>();
        addDropTarget(targets, together, alreadyNamed, stmt.schema(), stmt.name());
        if (stmt.additionalTables() != null) {
            for (String tableName : stmt.additionalTables()) {
                addDropTarget(targets, together, alreadyNamed, null, tableName);
            }
        }
        // A table is removed by whoever owns it, and every table named here is judged before any
        // of them goes: a DROP that refuses part way through has already taken the rest.
        for (String[] target : targets) {
            requireDropTargetResolves(target[0], target[1], stmt.ifExists());
        }
        for (String[] target : targets) requireDropOwner(target[0], target[1]);
        // Which of the names reach a relation is settled here, before any of them goes: a name
        // whose relation another name in the same list takes with it — a partition of a
        // partitioned table, an inheritance child, a relation a CASCADE reaches — is gone by the
        // time its own turn comes. PostgreSQL settles the whole set of names before it deletes
        // any of it and deletes each relation once, so the second name is neither an error nor
        // something to report as skipped.
        boolean[] wasThere = new boolean[targets.size()];
        for (int i = 0; i < targets.size(); i++) {
            wasThere[i] = dropTargetPresent(targets.get(i)[0], targets.get(i)[1]);
        }
        for (int i = 0; i < targets.size(); i++) {
            String[] target = targets.get(i);
            if (wasThere[i] && !dropTargetPresent(target[0], target[1])) continue;
            dropSingleTable(target[0], target[1], stmt.ifExists(), stmt.cascade(), together,
                    targets.size() > 1);
        }
        return QueryResult.command(QueryResult.Type.DROP_TABLE, 0);
    }

    /**
     * One name of a DROP TABLE list, as the schema it was written with and the bare name, unless
     * the list already named the same relation.
     */
    private void addDropTarget(List<String[]> targets, Set<String> together, Set<String> seen,
                               String schemaHint, String written) {
        String schema = schemaHint != null ? schemaHint : writtenSchema(written);
        String bare = RelationNamespace.bareName(written);
        together.add(bare.toLowerCase());
        // The schema the name reaches goes in beside the bare one, so that a dependent in another
        // schema that happens to share a name with something the list drops is still a dependent.
        together.add(dropTargetKey(schema, bare));
        if (!seen.add(dropTargetKey(schema, bare))) return;
        targets.add(new String[]{ schema, bare });
    }

    /** The schema a name in a DROP TABLE list carries, or null where it was written bare. */
    private static String writtenSchema(String name) {
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : null;
    }

    /**
     * Which relation a name in a DROP TABLE list stands for, resolved the way the drop itself
     * resolves it. Every name in the list is resolved before the first of them goes, because a
     * table dropped part way through would change what a later name reaches.
     */
    private String dropTargetKey(String schemaHint, String name) {
        String schemaName = schemaHint != null ? schemaHint : executor.defaultSchema();
        String tempSchema = executor.session != null ? executor.session.getTempSchemaName() : "pg_temp";
        Schema pgTemp = executor.database.getSchema(tempSchema);
        if (pgTemp != null && pgTemp.getTable(name) != null) schemaName = tempSchema;
        return schemaName.toLowerCase() + "." + name.toLowerCase();
    }

    /**
     * Whether one name in a DROP TABLE list still reaches a relation, resolved the way the drop
     * itself resolves it. Asked once before the first relation goes and again when each name's
     * turn comes, because what the list drops is settled by the first answer.
     */
    private boolean dropTargetPresent(String schemaHint, String name) {
        String tempSchema = executor.session != null ? executor.session.getTempSchemaName() : "pg_temp";
        Schema pgTemp = executor.database.getSchema(tempSchema);
        if (pgTemp != null && pgTemp.getTable(name) != null) return true;
        Schema schema = executor.database.getSchema(
                schemaHint != null ? schemaHint : executor.defaultSchema());
        return schema != null && schema.getTable(name) != null;
    }

    /**
     * What one name in a DROP TABLE list reaches, established before any of the tables goes.
     * PostgreSQL resolves the whole list first, so a name that reaches nothing -- or reaches a
     * relation DROP TABLE is not the command for -- is reported ahead of whatever another name in
     * the list is being kept alive by. The notice IF EXISTS leaves behind is not written here: the
     * drop itself writes it, once, for the name it skipped.
     */
    private void requireDropTargetResolves(String schemaHint, String name, boolean ifExists) {
        String schemaName = schemaHint != null ? schemaHint : executor.defaultSchema();
        if (executor.database.getSchema(schemaName) == null) return;
        String tempSchema = executor.session != null ? executor.session.getTempSchemaName() : "pg_temp";
        Schema pgTemp = executor.database.getSchema(tempSchema);
        if (pgTemp != null && pgTemp.getTable(name) != null) return;
        Schema schema = executor.database.getSchema(schemaName);
        if (schema.getTable(name) != null) return;
        RelationNamespace.requireKindForDrop(executor.database, schemaName, name,
                RelationNamespace.TABLE);
        if (!ifExists) {
            throw new MemgresException("table \"" + name + "\" does not exist", "42P01");
        }
    }

    /**
     * Everything below the relation, generation by generation, each named against the relation it
     * takes its columns from rather than against the one the statement named.
     *
     * <p>PostgreSQL follows the dependency graph rather than looking at one generation, so a
     * grandchild is listed too and is told it depends on its own parent. It follows a child's own
     * dependents before the child's siblings, which is why the lines come out depth first. A
     * relation reached down two paths is named once, against the last parent that reached it,
     * which is where PostgreSQL leaves it in the list.
     *
     * @param together the names the same statement is dropping, whose own lines are left out --
     *                 they are going anyway -- while what depends on them is still reported
     */
    private void collectInheritanceDependents(Table parent, String parentShownName,
                                              Set<String> together, Map<Table, String> found) {
        for (Table child : parent.getChildren()) {
            if (!together.contains(child.getName().toLowerCase())) {
                // Taking the relation out before putting it back moves it to the end of the order
                // it was found in, which is where PostgreSQL leaves a relation reached a second
                // time down another path -- named against the parent that reached it last.
                found.remove(child);
                found.put(child, "table " + child.getName()
                        + " depends on table " + parentShownName);
            }
            collectInheritanceDependents(child,
                    visibleName(child.getSchemaName(), child.getName()), together, found);
        }
    }

    /** The ownership a DROP TABLE needs on one of the names it lists, if that name is a table. */
    private void requireDropOwner(String schemaHint, String name) {
        String schemaName = schemaHint != null ? schemaHint : executor.defaultSchema();
        Schema schema = executor.database.getSchema(schemaName);
        if (schema == null || schema.getTable(RelationNamespace.bareName(name)) == null) return;
        executor.requireTableOwner(schemaName, RelationNamespace.bareName(name));
    }

    void dropSingleTable(String schemaHint, String name, boolean ifExists, boolean cascade) {
        dropSingleTable(schemaHint, name, ifExists, cascade,
                java.util.Collections.<String>emptySet(), false);
    }

    void dropSingleTable(String schemaHint, String name, boolean ifExists, boolean cascade,
                         Set<String> together, boolean severalNamed) {
        dropSingleTable(schemaHint, name, ifExists, cascade, together, severalNamed, null);
    }

    /**
     * @param namedAs how PostgreSQL names the object in a refusal, where that is not this
     *                relation: a partition goes because the statement named the partitioned
     *                table, so the refusal names the table the statement wrote
     */
    void dropSingleTable(String schemaHint, String name, boolean ifExists, boolean cascade,
                         Set<String> together, boolean severalNamed, String namedAs) {
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
                RelationNamespace.requireKindForDrop(executor.database, schemaName, name,
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
                    // Everything in the way goes on one detail, the way PostgreSQL reports it: a
                    // script told only of the first dependent learns of the rest by dropping that
                    // one and being refused again.
                    List<String> dependents = new ArrayList<>();
                    // A relation carries a composite type of its own name, and a routine written in
                    // terms of that type -- answering with rows of it, or taking one -- is recorded
                    // against the type rather than against the relation. PostgreSQL reports those
                    // first, ahead of everything recorded against the relation itself, and its line
                    // says which of the two the dependency is on.
                    List<PgFunction> byBody = routinesNamingInBody(name);
                    for (TypeDependents.Dependent d : notNamedInBody(TypeDependents.writtenIn(
                            executor.database, executor.systemCatalog,
                            executor.searchPathSchemas(), rowTypeOf(schemaName, name)), byBody)) {
                        dependents.add(d.described() + " depends on type "
                                + d.typeShown(visibleName(schemaName, name)));
                    }
                    // Check FK dependencies: any table in any schema referencing this table
                    for (Schema s : executor.database.getSchemas().values()) {
                        for (Table otherTable : s.getTables().values()) {
                            if (otherTable == droppedTable) continue;
                            // A table the same DROP takes down takes its foreign keys with it, so
                            // it is not something that would be left pointing at this one.
                            if (together.contains((s.getName() + "." + otherTable.getName())
                                    .toLowerCase())) continue;
                            for (StoredConstraint sc : otherTable.getConstraints()) {
                                if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                                if (!sc.getReferencesTable().equalsIgnoreCase(name)) continue;
                                if (sc.getReferencesSchema() != null
                                        && !sc.getReferencesSchema().equalsIgnoreCase(schemaName)) continue;
                                dependents.add("constraint " + sc.getName() + " on table "
                                        + visibleName(s.getName(), otherTable.getName())
                                        + " depends on table " + visibleName(schemaName, name));
                            }
                        }
                    }
                    // An inheritance child reads its parent's definition, so the parent cannot go
                    // while the child is still there — and a reader of either would otherwise find
                    // a child whose inherited columns come from a table that no longer exists.
                    Map<Table, String> below = new LinkedHashMap<Table, String>();
                    collectInheritanceDependents(droppedTable,
                            visibleName(schemaName, name), together, below);
                    dependents.addAll(below.values());
                    dependents.addAll(ViewDependencies.dependencyLines(executor.database,
                            executor.systemCatalog, schemaName, name, "table",
                            executor.searchPathSchemas(), together));
                    // A rule that writes to this relation needs it: PostgreSQL records the
                    // dependency when the rule is written, and refuses the drop that would leave
                    // the rule -- and every write to the relation it sits on -- pointing at
                    // nothing. The rule's own relation is named as the reader would have to write
                    // it, which is qualified when the search path does not reach it.
                    for (String[] rule : executor.database.rulesDependingOn(schemaName, name)) {
                        // A rule goes with the relation it is written on, so one whose relation
                        // the same DROP takes down will not outlive this one either.
                        if (rule[2] != null
                                && together.contains((rule[2] + "." + rule[1]).toLowerCase())) {
                            continue;
                        }
                        dependents.add("rule " + rule[0] + " on table " + visibleName(rule[2], rule[1])
                                + " depends on table " + visibleName(schemaName, name));
                    }
                    // A routine whose body PostgreSQL parsed where the routine was defined -- a
                    // SQL-standard body, written BEGIN ATOMIC or RETURN -- left a dependency on
                    // every relation that body names, and those come last in what the refusal
                    // reports. The text of a PL/pgSQL body, or of a SQL body written as a string,
                    // is resolved when the routine is called and records nothing at all, so a
                    // relation it merely mentions -- through %ROWTYPE, %TYPE, or by name -- is not
                    // something standing in the way of the drop.
                    for (PgFunction fn : byBody) {
                        dependents.add("function " + functionSignature(fn)
                                + " depends on table " + visibleName(schemaName, name));
                    }
                    if (!dependents.isEmpty()) {
                        MemgresException e = new MemgresException(
                                DdlObjectExecutor.cannotDropMessage(severalNamed,
                                        namedAs != null ? namedAs
                                                : "table " + visibleName(schemaName, name)), "2BP01");
                        e.setDetail(DdlObjectExecutor.dependencyDetail(dependents));
                        e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
                        throw e;
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
                                // A constraint on a table the same DROP takes down goes with that
                                // table, and PostgreSQL reports only what it had to take besides
                                // the objects the statement named.
                                if (!together.contains((s.getName() + "." + otherTable.getName())
                                        .toLowerCase())) {
                                    cascaded.add("constraint " + fkName + " on table "
                                            + otherTable.getName());
                                }
                                otherTable.removeConstraint(fkName);
                            }
                        }
                    }
                    // An inheritance child goes with its parent, and so does everything reading
                    // either — all of it named in one notice, because that is what PostgreSQL
                    // reports for one DROP.
                    List<Table> descendants = inheritanceDescendants(droppedTable);
                    List<String> viewsToDrop = new ArrayList<>();
                    for (String v : ViewDependencies.cascadeDependents(executor.database,
                            executor.systemCatalog, schemaName, name)) {
                        if (!viewsToDrop.contains(v)) viewsToDrop.add(v);
                    }
                    for (Table child : descendants) {
                        String childSchema = findSchemaNameOf(child, schemaName);
                        for (String v : ViewDependencies.cascadeDependents(
                                executor.database, executor.systemCatalog, childSchema,
                                child.getName())) {
                            if (!viewsToDrop.contains(v)) viewsToDrop.add(v);
                        }
                    }
                    for (String v : viewsToDrop) {
                        // A relation the same DROP names is not something the cascade reached:
                        // PostgreSQL settles the whole set of names first and reports only what
                        // it had to take besides. A materialized view is named by the kind it
                        // really is, because that is the kind PostgreSQL recorded it under.
                        Database.ViewDef going = executor.database.getView(v);
                        if (!together.contains(RelationNamespace.bareName(v).toLowerCase())) {
                            cascaded.add((going != null && going.materialized()
                                    ? "materialized view " : "view ")
                                    + RelationNamespace.bareName(v));
                        }
                        executor.database.removeView(v);
                    }
                    for (Table child : descendants) {
                        if (together.contains(child.getName().toLowerCase())) continue;
                        cascaded.add("table " + child.getName());
                    }
                    // The whole tree is accounted for here, so each child is dropped with no
                    // children of its own left — one DROP reports one cascade, not one per level.
                    droppedTable.getChildren().clear();
                    for (Table child : descendants) child.getChildren().clear();
                    for (int d = descendants.size() - 1; d >= 0; d--) {
                        Table child = descendants.get(d);
                        dropSingleTable(findSchemaNameOf(child, schemaName), child.getName(), true,
                                true, together, severalNamed, namedAs);
                    }
                    // CASCADE takes away exactly what the refusal above names and nothing else: a
                    // routine recorded against the relation's composite type, which PostgreSQL
                    // lists ahead of everything, and one whose parsed body names the relation,
                    // which it lists last. Reading a body's text for the relation's name took away
                    // every routine that so much as mentioned it -- a PL/pgSQL function that reads
                    // the relation is not something PostgreSQL recorded anything about, and it
                    // stays where it is.
                    List<Session.UndoEntry> routineUndo = new ArrayList<>();
                    List<PgFunction> cascadeByBody = routinesNamingInBody(name);
                    List<String> byType = new ArrayList<>();
                    for (TypeDependents.Dependent d : notNamedInBody(TypeDependents.writtenIn(
                            executor.database, executor.systemCatalog,
                            executor.searchPathSchemas(), rowTypeOf(schemaName, name)),
                            cascadeByBody)) {
                        byType.add(d.described());
                        TypeDependents.remove(executor.database, d, routineUndo);
                    }
                    cascaded.addAll(0, byType);
                    for (PgFunction fn : cascadeByBody) {
                        cascaded.add("function " + functionSignature(fn));
                        routineUndo.add(new Session.DropFunctionUndo(Database.schemaOf(fn),
                                fn.getName(), java.util.Collections.singletonList(fn)));
                        executor.database.removeFunctionOverload(fn);
                    }
                    for (Session.UndoEntry entry : routineUndo) executor.recordUndo(entry);
                    // A rule that writes to this relation is refused above without CASCADE, so
                    // CASCADE has to take it away. Leaving it registered left the relation it
                    // sits on unwritable -- every INSERT still ran the rule, which reached for
                    // a table that had just been dropped -- and pg_rules still listed it.
                    for (String[] rule : executor.database.rulesDependingOn(schemaName, name)) {
                        cascaded.add("rule " + rule[0] + " on table " + visibleName(rule[2], rule[1]));
                        // The rule sits on a relation of its own, so it is in no relation's
                        // snapshot. PostgreSQL rolls a catalogue change back whole, and without a
                        // record of its own the rollback left that relation -- which the statement
                        // never named -- without the rule it was written with.
                        executor.recordUndo(new Session.DropRulesUndo(
                                executor.database.snapshotRule(rule[2], rule[0], rule[1])));
                        executor.database.removeRule(rule[2], rule[0], rule[1]);
                    }
                    DdlObjectExecutor.noticeDropCascades(executor, cascaded);
                }
                // PG drops all partitions together with a partitioned parent (no CASCADE needed).
                // A partition goes because the statement named the table it belongs to, so what
                // stands in a partition's way is reported against the name the statement wrote,
                // and a relation the same statement drops is no reason to refuse.
                if (!droppedTable.getPartitions().isEmpty()) {
                    String shownAs = namedAs != null ? namedAs
                            : "table " + visibleName(schemaName, name);
                    for (Table part : new ArrayList<>(droppedTable.getPartitions())) {
                        String partSchemaName = findSchemaNameOf(part, schemaName);
                        dropSingleTable(partSchemaName, part.getName(), true, cascade, together,
                                severalNamed, shownAs);
                    }
                }
                // Dropping a partition must remove it from the parent's routing list so
                // routed INSERTs can't land in (and reads can't see) a dropped table
                Table partitionParent = droppedTable.getPartitionParent();
                if (partitionParent != null) {
                    partitionParent.removePartition(droppedTable);
                }
                // Dropping an inheritance child must unlink it from every table it was declared
                // under, for the same reason: a parent still listing a child that is gone is a
                // dependency on nothing, and that parent could then never be dropped. PostgreSQL
                // deletes one pg_inherits row per parent, and a table declared under two parents
                // has two of them — taking the child off the first one alone left the second
                // refusing its own drop for a child that was no longer there.
                for (Table inheritanceParent : droppedTable.getInheritParents()) {
                    inheritanceParent.removeChild(droppedTable);
                }
                executor.recordUndo(new Session.DropTableUndo(schemaName, name, droppedTable,
                        executor.database.getTriggersForTable(schemaName, name),
                        executor.database.snapshotRulesOn(schemaName, name)));
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
            // A rule belongs to the relation it is written on and is dropped with it. Leaving one
            // registered left pg_rules describing a rule on a relation that was no longer there,
            // and taking every rule of the name took another schema's relation's rules as well.
            executor.database.dropRulesOn(schemaName, name);
            // Drop the sequences this table owns. A serial or identity column's sequence belongs
            // to the table, and so does one attached with ALTER SEQUENCE ... OWNED BY; an
            // independently created sequence a DEFAULT merely names does not. Composing
            // <table>_<column>_seq instead answered for a name a renamed column no longer has, so
            // the sequence outlived the table, and missed an OWNED BY sequence entirely.
            for (Sequence owned : new ArrayList<>(executor.database.getSequences().values())) {
                boolean belongsHere = name.equalsIgnoreCase(owned.getOwnedByTable())
                        && owned.getSchemaName().equalsIgnoreCase(schemaName);
                if (!belongsHere && droppedTable != null && owned.isInternal()) {
                    // A sequence made before ownership was recorded is still named by the default
                    // of the column it feeds, whatever that column is called now.
                    for (Column col : droppedTable.getColumns()) {
                        if (owned.getName().equalsIgnoreCase(Sequence.nameInDefault(col.getDefaultValue()))) {
                            belongsHere = true;
                            break;
                        }
                    }
                }
                if (!belongsHere) continue;
                executor.database.removeSequence(owned.getSchemaName(), owned.getName());
                executor.database.unregisterSchemaObject(owned.getSchemaName(), "sequence", owned.getName());
                executor.database.removeObjectOwner("sequence:" + owned.getName());
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
     * Every routine the database holds, in the order they were created.
     *
     * <p>PostgreSQL numbers a pg_proc row when the routine is created and reports what depends on
     * a relation in the order those numbers run, so three routines written one after another are
     * refused in that order. A hash map remembers no order at all, and gave a different one for
     * every set of names.
     */
    private List<PgFunction> routinesInCreationOrder() {
        List<PgFunction> all = new ArrayList<>(executor.database.getFunctions().values());
        Collections.sort(all, new java.util.Comparator<PgFunction>() {
            @Override
            public int compare(PgFunction a, PgFunction b) {
                return Integer.compare(routineOid(a), routineOid(b));
            }
        });
        return all;
    }

    /** The OID of a routine's own pg_proc row, which is when it was created. */
    private int routineOid(PgFunction fn) {
        return executor.systemCatalog.getOid(
                CatalogCoreBuilder.routineOidKey(executor.database, fn));
    }

    /**
     * The type a relation carries of its own name, as a declaration elsewhere would have written
     * it. RETURNS &lt;relation&gt;, an argument of the relation's type and an argument of an array
     * of it are all written in that type, and PostgreSQL records the dependency against the type
     * rather than against the relation.
     */
    private static TypeDependents.Names rowTypeOf(String schemaName, String name) {
        final String bare = RelationNamespace.bareName(name).toLowerCase();
        final String qualified = schemaName.toLowerCase() + "." + bare;
        return written -> {
            String lower = written.toLowerCase();
            return lower.equals(bare) || lower.equals(qualified);
        };
    }

    /**
     * The routines whose parsed body names this relation, in the order they were created, which is
     * the order PostgreSQL reports them in.
     */
    private List<PgFunction> routinesNamingInBody(String name) {
        List<PgFunction> found = new ArrayList<>();
        for (PgFunction fn : executor.database.getAllFunctionOverloads()) {
            if (parsedBodyNames(fn, name)) found.add(fn);
        }
        java.util.Collections.sort(found, new java.util.Comparator<PgFunction>() {
            @Override
            public int compare(PgFunction a, PgFunction b) {
                return Integer.compare(executor.systemCatalog.getOid("proc:" + a.getName()),
                        executor.systemCatalog.getOid("proc:" + b.getName()));
            }
        });
        return found;
    }

    /**
     * The same routines, without those the relation itself is already in the way for. A routine
     * that both answers with the relation's row type and names the relation in a parsed body leaves
     * PostgreSQL two dependency records and one object, and the object is named against the
     * relation rather than against the type.
     */
    private static List<TypeDependents.Dependent> notNamedInBody(
            List<TypeDependents.Dependent> byRowType, List<PgFunction> byBody) {
        List<TypeDependents.Dependent> kept = new ArrayList<>();
        for (TypeDependents.Dependent d : byRowType) {
            if (d.routine != null && byBody.contains(d.routine)) continue;
            kept.add(d);
        }
        return kept;
    }

    /**
     * Whether the relation is named by a body PostgreSQL parsed when the routine was defined.
     *
     * <p>A SQL-standard body -- BEGIN ATOMIC ... END, or RETURN expr -- is parsed then, so what it
     * names is recorded and the relation cannot go without taking the routine with it. Any other
     * body is text until the routine is called: nothing in the catalogue records what it mentions,
     * and PostgreSQL neither refuses the drop for it nor takes the routine away with CASCADE.
     */
    private static boolean parsedBodyNames(PgFunction fn, String tableName) {
        if (!fn.isAtomicBody() && !fn.isSqlStandardBody()) return false;
        String body = fn.getBody();
        if (body == null) return false;
        return java.util.regex.Pattern.compile(
                "\\b" + java.util.regex.Pattern.quote(tableName.toLowerCase()) + "\\b",
                java.util.regex.Pattern.CASE_INSENSITIVE).matcher(body).find();
    }

    /**
     * The routine written the way PostgreSQL writes it where a dependency names one. How a routine
     * is written is settled in one place, so a routine reached through the relation's row type and
     * one reached through its parsed body come out spelled alike.
     */
    private String functionSignature(PgFunction fn) {
        return TypeDependents.routineSignature(executor.searchPathSchemas(), fn);
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
                        new ArrayList<>(child.getRows()), child.getSerialCounter(),
                        child.getTupleIdCounter()));
                List<PgTrigger> childTriggers = executor.database.getTriggersForTable(child.getName());
                fireTruncateStatementTriggers(childTriggers, PgTrigger.Timing.BEFORE, child);
                child.deleteAll();
                child.resetTupleIdCounter(0);
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

    /**
     * The relations one name in a TRUNCATE list empties: the relation itself and, unless ONLY was
     * written, every partition and every inheritance child below it. A child's rows are the
     * parent's rows, so a TRUNCATE that stopped at the partitions left them behind.
     */
    private static List<Table> truncateTargetsOf(Table table, boolean only) {
        List<Table> targets = new ArrayList<>();
        if (only) {
            targets.add(table);
        } else {
            DmlPartitionHelper.collectRelationAndDescendants(table, targets);
        }
        return targets;
    }

    QueryResult executeTruncate(TruncateStmt stmt) {
        int totalCount = 0;
        // PG lets one TRUNCATE name every table in a reference graph, and then nothing is left
        // dangling. Resolve the whole list first so a referencing table listed alongside its
        // parent does not block the parent, which is how fixture teardown clears related tables.
        Set<Table> alsoTruncating = Collections.newSetFromMap(new IdentityHashMap<Table, Boolean>());
        for (int nameIdx = 0; nameIdx < stmt.tables().size(); nameIdx++) {
            Table t = resolveTruncateTarget(stmt.tables().get(nameIdx));
            if (t == null) continue;
            alsoTruncating.addAll(truncateTargetsOf(t, stmt.only(nameIdx)));
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
                        // A partitioned parent holds no rows itself, and an inheritance child's
                        // rows are the parent's rows too: TRUNCATE names a relation and empties
                        // everything under it, partitions and children alike. ONLY is the one way
                        // to ask for the named relation alone.
                        List<Table> truncateTargets = truncateTargetsOf(table, truncateOnly);
                        for (Table target : truncateTargets) {
                            String targetSchema = target == table ? schemaName : findSchemaNameOf(target, schemaName);
                            executor.recordUndo(new Session.TruncateUndo(targetSchema, target.getName(),
                                    new ArrayList<>(target.getRows()), target.getSerialCounter(),
                                    target.getTupleIdCounter()));
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
                            // TRUNCATE gives the relation a new file, so the next row written
                            // into it lives at the first line pointer again.
                            target.resetTupleIdCounter(0);
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
                                    Sequence seq = executor.database.getSequenceFor(
                                            schemaName, seqName);
                                    if (seq != null) {
                                        // C11: Record undo so ROLLBACK restores the sequence
                                        executor.recordUndo(new Session.SequenceRestartUndo(
                                                seq.qualifiedName(), seq.currValRaw(), seq.isCalled()));
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
                // complaint from a name that resolves to nothing, and PostgreSQL says so. An
                // index and a sequence own a name in a schema exactly as a view does, and
                // reporting either as missing sent the reader after a relation that is there.
                if (executor.database.hasView(bareName)) {
                    throw new MemgresException("\"" + bareName + "\" is not a table", "42809");
                }
                for (String schemaName : searchSchemas) {
                    RelationNamespace.requireKind(executor.database, schemaName, bareName,
                            RelationNamespace.TABLE);
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
        // that remembered them went with the relation that was dropped. Only this schema's are
        // forgotten -- another schema's relation of the name is a different relation.
        executor.database.clearRuleHistory(schemaName, stmt.name());
        executor.database.markUncommittedObject(table, executor.session);
        executor.recordUndo(new Session.CreateTableUndo(schemaName, stmt.name()));

        // The rows go in under a command identifier of their own. PostgreSQL moves the counter on
        // once the relation's catalogue rows are written, so that the routine filling the
        // relation can see the relation it is filling, and it does that whether or not the query
        // turned out to have any rows. A definition that asks for no data never opens the
        // relation and spends nothing beyond what the relation itself cost -- which is more than
        // one when its columns are wide enough to need a TOAST table behind them.
        if (executor.session != null) {
            executor.session.noteCatalogRowsWritten(
                    CommandIdCost.forRelationBuiltByQuery(columns, stmt.withData()));
        }
        int rowCount = 0;
        if (stmt.withData()) {
            for (Object[] row : result.getRows()) {
                Object[] copy = row.clone();
                table.insertRow(copy);
                executor.recordUndo(new Session.InsertUndo(schemaName, table.getName(), copy));
                recordCreatedRowIdentity(schemaName, table, copy);
                rowCount++;
            }
        }

        return QueryResult.command(QueryResult.Type.SELECT_INTO, rowCount);
    }

    /**
     * Give a row the query wrote at creation time the place it holds in the new relation.
     *
     * <p>PostgreSQL fills the relation through the routine an INSERT writes through, so these rows
     * take line pointers one after another and the relation goes on from where they left off: the
     * row written after them lands beyond them rather than on top of the first, and a tuple id
     * still names one row once some of them have been deleted. Where such a row sat was recorded
     * nowhere, so it was counted off by its position instead and the relation went on handing out
     * line pointers from one.
     */
    private void recordCreatedRowIdentity(String schemaName, Table table, Object[] row) {
        if (executor.session == null || executor.database == null) return;
        executor.database.setRowInsertMeta(schemaName + "." + table.getName(), table, row,
                executor.session.getTransactionId(),
                executor.session.getCommandId()
                        + CommandIdCost.rowsOfRelationBuiltByQuery(table.getColumns()));
    }
}
