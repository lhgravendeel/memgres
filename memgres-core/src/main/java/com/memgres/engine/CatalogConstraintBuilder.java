package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds constraint, index, dependency, and related metadata pg_catalog tables.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogConstraintBuilder {

    final Database database;
    final OidSupplier oids;

    CatalogConstraintBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    Table buildPgConstraint() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("conname", DataType.TEXT),
                colNN("connamespace", DataType.INTEGER),
                colNN("contype", DataType.CHAR),
                colNN("conrelid", DataType.INTEGER),
                colNN("confrelid", DataType.INTEGER),
                col("conkey", DataType.INT4_ARRAY),
                col("confkey", DataType.INT4_ARRAY),
                col("condeferrable", DataType.BOOLEAN),
                col("condeferred", DataType.BOOLEAN),
                col("convalidated", DataType.BOOLEAN),
                col("conislocal", DataType.BOOLEAN),
                col("conindid", DataType.INTEGER),
                col("confupdtype", DataType.CHAR),
                col("confdeltype", DataType.CHAR),
                col("connoinherit", DataType.BOOLEAN),
                col("conenforced", DataType.BOOLEAN),
                col("conbin", DataType.TEXT),
                col("conexclop", DataType.TEXT),
                col("conperiod", DataType.BOOLEAN),
                col("conparentid", DataType.INTEGER),
                col("contypid", DataType.INTEGER),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_constraint", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            int nsOid = oids.oid("ns:" + schemaEntry.getKey());
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (StoredConstraint sc : t.getConstraints()) {
                    // UNIQUE constraints from CREATE UNIQUE INDEX (not ADD CONSTRAINT) are not in pg_constraint
                    if (sc.getType() == StoredConstraint.Type.UNIQUE && sc.isFromIndex()) continue;
                    String contype;
                    switch (sc.getType()) {
                        case PRIMARY_KEY:
                            contype = "p";
                            break;
                        case UNIQUE:
                            contype = "u";
                            break;
                        case CHECK:
                            contype = "c";
                            break;
                        case FOREIGN_KEY:
                            contype = "f";
                            break;
                        case EXCLUDE:
                            contype = "x";
                            break;
                        default:
                            throw new IllegalStateException("Unknown constraint type: " + sc.getType());
                    }
                    int confrelid = 0;
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                        confrelid = oids.oid("rel:" + schemaEntry.getKey() + "." + sc.getReferencesTable());
                    }
                    // Convert column names to attnum array string
                    List<Object> conkey = columnNamesToAttnums(t, sc.getColumns());
                    List<Object> confkey = null;
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                        Table refTable = findTable(database, sc.getReferencesTable());
                        if (refTable != null) {
                            confkey = columnNamesToAttnums(refTable, sc.getReferencesColumns());
                        }
                    }
                    int conindid = 0;
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                        conindid = oids.oid("rel:" + schemaEntry.getKey() + "." + sc.getName());
                    } else if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                        // For FK constraints, conindid should reference the PK/UNIQUE index on the referenced table
                        Table refTable = findTable(database, sc.getReferencesTable());
                        if (refTable != null) {
                            for (StoredConstraint refCon : refTable.getConstraints()) {
                                if (refCon.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                                    conindid = oids.oid("rel:" + schemaEntry.getKey() + "." + refCon.getName());
                                    break;
                                }
                            }
                        }
                    }
                    // In PG, confupdtype/confdeltype are always a single char (' ' for non-FK)
                    String confupdtype = " ";
                    String confdeltype = " ";
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                        confupdtype = fkActionCode(sc.getOnUpdate());
                        confdeltype = fkActionCode(sc.getOnDelete());
                    }
                    // In PG, connoinherit is true for PK/UNIQUE/CHECK, false for FK
                    boolean connoinherit = sc.getType() != StoredConstraint.Type.FOREIGN_KEY;
                    table.insertRow(new Object[]{
                            oids.oid("con:" + t.getName() + "." + sc.getName()),
                            sc.getName(),
                            nsOid,
                            contype,
                            relOid,
                            confrelid,
                            conkey,
                            confkey,
                            sc.isDeferrable(), sc.isInitiallyDeferred(), sc.isConvalidated(), // condeferrable, condeferred, convalidated
                            true, conindid,
                            confupdtype,
                            confdeltype,
                            connoinherit,
                            !sc.isNotEnforced(), // conenforced: true = enforced (default), false = not enforced
                            null, null, false, 0, 0, 1 // conbin, conexclop, conperiod, conparentid, contypid, xmin
                    });
                }
                // PG 18: NOT NULL constraints are tracked in pg_constraint with contype='n'
                // Collect columns covered by UNIQUE constraints promoted from index (USING INDEX)
                java.util.Set<String> promotedUniqueColumns = new java.util.HashSet<>();
                for (StoredConstraint usc : t.getConstraints()) {
                    if (usc.getType() == StoredConstraint.Type.UNIQUE && usc.isPromotedFromIndex()) {
                        for (String c : usc.getColumns()) promotedUniqueColumns.add(c.toLowerCase());
                    }
                }
                for (Column c : t.getColumns()) {
                    boolean isPromotedUnique = promotedUniqueColumns.contains(c.getName().toLowerCase());
                    // Emit NOT NULL for all NOT NULL columns (including PK columns),
                    // but skip columns covered by UNIQUE constraints promoted from index
                    if (!c.isNullable() && !isPromotedUnique) {
                        String conname = t.getName() + "_" + c.getName() + "_not_null";
                        List<Object> nnConkey = columnNamesToAttnums(t, Cols.listOf(c.getName()));
                        table.insertRow(new Object[]{
                                oids.oid("con:" + t.getName() + "." + conname),
                                conname,
                                nsOid,
                                "n",
                                relOid,
                                0,
                                nnConkey,
                                null,
                                false, false, true,
                                true, 0,
                                " ", " ", false,
                                true, // conenforced
                                null, null, false, 0, 0, 1
                        });
                    }
                }
            }
        }

        // Domain CHECK constraints (contypid points to domain type OID)
        int publicNsOid = oids.oid("ns:public");
        for (DomainType dom : database.getDomains().values()) {
            int domTypeOid = oids.oid("type:" + dom.getName());
            // Inline CHECK (from CREATE DOMAIN ... CHECK(...))
            if (dom.getCheckExpression() != null) {
                String conname = dom.getName() + "_check";
                table.insertRow(new Object[]{
                        oids.oid("con:domain:" + dom.getName() + "." + conname),
                        conname,
                        publicNsOid,
                        "c",        // contype = CHECK
                        0,          // conrelid (0 for domain constraints)
                        0,          // confrelid
                        null, null, // conkey, confkey
                        false, false, true,
                        true, 0,
                        " ", " ", true, // connoinherit = true for domain constraints
                        true, // conenforced
                        dom.getCheckExpression(), null, false, 0, domTypeOid, 1
                });
            }
            // Named constraints (from ALTER DOMAIN ADD CONSTRAINT)
            for (DomainType.NamedConstraint nc : dom.getNamedConstraints()) {
                table.insertRow(new Object[]{
                        oids.oid("con:domain:" + dom.getName() + "." + nc.name()),
                        nc.name(),
                        publicNsOid,
                        "c",
                        0, 0,
                        null, null,
                        false, false, true,
                        true, 0,
                        " ", " ", true,
                        true, // conenforced
                        nc.rawCheckExpr(), null, false, 0, domTypeOid, 1
                });
            }
            // NOT NULL constraint on domain
            if (dom.isNotNull()) {
                String conname = dom.getName() + "_not_null";
                table.insertRow(new Object[]{
                        oids.oid("con:domain:" + dom.getName() + "." + conname),
                        conname,
                        publicNsOid,
                        "n",
                        0, 0,
                        null, null,
                        false, false, true,
                        true, 0,
                        " ", " ", true,
                        true, // conenforced
                        null, null, false, 0, domTypeOid, 1
                });
            }
        }

        return table;
    }

    Table buildPgIndex() {
        List<Column> cols = Cols.listOf(
                colNN("indexrelid", DataType.INTEGER),
                colNN("indrelid", DataType.INTEGER),
                colNN("indisunique", DataType.BOOLEAN),
                colNN("indisprimary", DataType.BOOLEAN),
                col("indimmediate", DataType.BOOLEAN),
                col("indkey", DataType.TEXT),
                col("indnkeyatts", DataType.SMALLINT),
                col("indnatts", DataType.SMALLINT),
                col("indisvalid", DataType.BOOLEAN),
                col("indisready", DataType.BOOLEAN),
                col("indislive", DataType.BOOLEAN),
                col("indexprs", DataType.TEXT),
                col("indpred", DataType.TEXT),
                col("indisclustered", DataType.BOOLEAN),
                col("indisreplident", DataType.BOOLEAN),
                col("indoption", DataType.TEXT),
                col("indnullsnotdistinct", DataType.BOOLEAN),
                col("indclass", DataType.TEXT),
                col("indcollation", DataType.TEXT)
        );
        Table table = new Table("pg_index", cols);
        // Populate from stored index metadata
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexName = idx.getKey();
            List<String> indexCols = idx.getValue();
            // Use stored table name to find the exact table for this index
            String storedTableQualified = database.getIndexTable(indexName);
            // Find the table that this index belongs to
            for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
                for (Map.Entry<String, com.memgres.engine.Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                    com.memgres.engine.Table t = tableEntry.getValue();
                    // Only match this index to its actual table (using stored metadata)
                    if (storedTableQualified != null) {
                        String qualifiedName = schemaEntry.getKey() + "." + t.getName();
                        if (!qualifiedName.equalsIgnoreCase(storedTableQualified)) continue;
                    }
                    boolean hasExprCols = false;
                    StringBuilder indkey = new StringBuilder();
                    StringBuilder exprParts = new StringBuilder();
                    boolean allResolved = true;
                    for (String colName : indexCols) {
                        int colIdx = t.getColumnIndex(colName);
                        if (colIdx < 0) {
                            // Expression column (e.g., lower(email)); use 0 per PostgreSQL convention
                            hasExprCols = true;
                            if (indkey.length() > 0) indkey.append(" ");
                            indkey.append(0);
                            if (exprParts.length() > 0) exprParts.append(", ");
                            exprParts.append(colName);
                        } else {
                            if (indkey.length() > 0) indkey.append(" ");
                            indkey.append(colIdx + 1); // 1-based
                        }
                    }
                    if (indkey.length() > 0) {
                        int indexOid = oids.oid("rel:" + schemaEntry.getKey() + "." + indexName);
                        int tableOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                        // Check uniqueness from both constraint metadata and explicit index flags
                        boolean isUnique = database.isUniqueIndex(indexName) ||
                                t.getConstraints().stream()
                                .anyMatch(sc -> sc.getName().equalsIgnoreCase(indexName) &&
                                        (sc.getType() == StoredConstraint.Type.UNIQUE || sc.getType() == StoredConstraint.Type.PRIMARY_KEY));
                        boolean isPrimary = t.getConstraints().stream()
                                .anyMatch(sc -> sc.getName().equalsIgnoreCase(indexName) && sc.getType() == StoredConstraint.Type.PRIMARY_KEY);
                        // Store indkey as a PgVector (0-based int2vector)
                        List<Object> indkeyElems = new java.util.ArrayList<>();
                        for (String num : indkey.toString().split(" ")) {
                            indkeyElems.add(Integer.parseInt(num));
                        }
                        PgVector indkeyVec = new PgVector(indkeyElems);
                        // Get WHERE predicate for partial indexes
                        String whereClause = database.getIndexWhereClause(indexName);
                        String indexprs = hasExprCols ? exprParts.toString() : null;
                        // Build indoption, indclass, indcollation as PgVectors (0-based oidvector)
                        List<Object> optionElems = new java.util.ArrayList<>();
                        List<Object> classElems = new java.util.ArrayList<>();
                        List<Object> collElems = new java.util.ArrayList<>();
                        for (int ic = 0; ic < indkeyElems.size(); ic++) {
                            optionElems.add(0);   // default options (no DESC, no NULLS FIRST)
                            classElems.add(1978);  // default btree opclass OID (int4_ops=1978 in PG)
                            collElems.add(0);      // default collation
                        }
                        table.insertRow(new Object[]{indexOid, tableOid, isUnique, isPrimary,
                                true, // indimmediate
                                indkeyVec,
                                (short) indkeyElems.size(), (short) indkeyElems.size(), true, true, true, indexprs, whereClause, false,
                                false, // indisreplident
                                new PgVector(optionElems),
                                false, new PgVector(classElems), new PgVector(collElems)});
                        break;
                    }
                }
            }
        }
        // Also add indexes from PK and UNIQUE constraints (implicit indexes)
        Set<String> existingIndexNames = new HashSet<>(database.getIndexColumns().keySet());
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, com.memgres.engine.Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                com.memgres.engine.Table t = tableEntry.getValue();
                int tableOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                        String indexName = sc.getName();
                        if (existingIndexNames.contains(indexName.toLowerCase())) continue;
                        List<Object> indkeyList = new java.util.ArrayList<>();
                        for (String colName : sc.getColumns()) {
                            int colIdx = t.getColumnIndex(colName);
                            if (colIdx >= 0) {
                                indkeyList.add(colIdx + 1);
                            }
                        }
                        if (!indkeyList.isEmpty()) {
                            int indexOid = oids.oid("rel:" + schemaEntry.getKey() + "." + indexName);
                            PgVector indkeyVec = new PgVector(indkeyList);
                            List<Object> optElems = new java.util.ArrayList<>();
                            List<Object> clsElems = new java.util.ArrayList<>();
                            List<Object> colElems = new java.util.ArrayList<>();
                            for (int ic = 0; ic < indkeyList.size(); ic++) {
                                optElems.add(0);
                                clsElems.add(1978);
                                colElems.add(0);
                            }
                            table.insertRow(new Object[]{
                                    indexOid, tableOid,
                                    true, // isUnique
                                    sc.getType() == StoredConstraint.Type.PRIMARY_KEY, // isPrimary
                                    true, // indimmediate
                                    indkeyVec,
                                    (short) indkeyList.size(), (short) indkeyList.size(),
                                    true, true, true, null, null, false,
                                    false, // indisreplident
                                    new PgVector(optElems),
                                    false, new PgVector(clsElems), new PgVector(colElems)
                            });
                        }
                    }
                }
            }
        }

        return table;
    }

    Table buildPgAttrdef() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("adrelid", DataType.INTEGER),
                colNN("adnum", DataType.SMALLINT),
                col("adbin", DataType.TEXT),
                col("adsrc", DataType.TEXT)
        );
        Table table = new Table("pg_attrdef", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (int i = 0; i < t.getColumns().size(); i++) {
                    Column c = t.getColumns().get(i);
                    if (c.getDefaultValue() != null || c.getType() == DataType.SERIAL
                            || c.getType() == DataType.BIGSERIAL || c.getType() == DataType.SMALLSERIAL) {
                        String formatted = formatColumnDefault(c);
                        String defaultExpr = formatted != null ? formatted
                                : "nextval('" + t.getName() + "_" + c.getName() + "_seq'::regclass)";
                        table.insertRow(new Object[]{
                                oids.oid("attrdef:" + t.getName() + "." + c.getName()),
                                relOid, (short) (i + 1), defaultExpr, defaultExpr
                        });
                    }
                }
            }
        }
        return table;
    }

    Table buildPgDepend() {
        List<Column> cols = Cols.listOf(
                colNN("classid", DataType.INTEGER),
                colNN("objid", DataType.INTEGER),
                colNN("objsubid", DataType.INTEGER),
                colNN("refclassid", DataType.INTEGER),
                colNN("refobjid", DataType.INTEGER),
                colNN("refobjsubid", DataType.INTEGER),
                colNN("deptype", DataType.CHAR)
        );
        Table table = new Table("pg_depend", cols);
        int pgClassOid = oids.oid("rel:pg_catalog.pg_class");

        // Dependencies for serial/identity sequences -> their owning table+column
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int tableOid = oids.oid("rel:" + schemaName + "." + t.getName());
                for (int i = 0; i < t.getColumns().size(); i++) {
                    Column c = t.getColumns().get(i);
                    String seqName = null;
                    if (c.getType() == DataType.SERIAL || c.getType() == DataType.BIGSERIAL
                            || c.getType() == DataType.SMALLSERIAL) {
                        seqName = t.getName() + "_" + c.getName() + "_seq";
                    } else if (c.getDefaultValue() != null && c.getDefaultValue().contains("__identity__")) {
                        seqName = t.getName() + "_" + c.getName() + "_seq";
                    }
                    if (seqName != null) {
                        int seqOid = oids.oid("rel:" + schemaName + "." + seqName);
                        // 'a' = auto dependency (sequence owned by table column)
                        table.insertRow(new Object[]{pgClassOid, seqOid, 0, pgClassOid, tableOid, i + 1, "a"});
                    }
                }
            }
        }

        // View dependencies: rewrite rule -> referenced table (via pg_rewrite)
        int pgRewriteClassOid = oids.oid("rel:pg_catalog.pg_rewrite");
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int ruleOid = oids.oid("rule:_RETURN_" + vd.name());
            // Extract referenced tables from the view's SELECT statement
            if (vd.query() instanceof com.memgres.engine.parser.ast.SelectStmt && ((com.memgres.engine.parser.ast.SelectStmt) vd.query()).from() != null) {
                com.memgres.engine.parser.ast.SelectStmt sel = (com.memgres.engine.parser.ast.SelectStmt) vd.query();
                for (com.memgres.engine.parser.ast.SelectStmt.FromItem fromItem : sel.from()) {
                    collectViewDependencies(fromItem, vSchema, pgRewriteClassOid, ruleOid, pgClassOid, table);
                }
            }
        }

        // plpgsql extension dependencies (deptype='e' = extension member)
        int pgExtensionClassOid = oids.oid("rel:pg_catalog.pg_extension");
        int pgLanguageClassOid = oids.oid("rel:pg_catalog.pg_language");
        int pgProcClassOid = oids.oid("rel:pg_catalog.pg_proc");
        int plpgsqlExtOid = oids.oid("ext:plpgsql");
        // plpgsql language depends on plpgsql extension
        table.insertRow(new Object[]{pgLanguageClassOid, oids.oid("lang:plpgsql"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});
        // plpgsql handler procs depend on plpgsql extension
        table.insertRow(new Object[]{pgProcClassOid, oids.oid("proc:plpgsql_call_handler"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});
        table.insertRow(new Object[]{pgProcClassOid, oids.oid("proc:plpgsql_inline_handler"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});
        table.insertRow(new Object[]{pgProcClassOid, oids.oid("proc:plpgsql_validator"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});

        return table;
    }

    Table buildPgRewrite() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("rulename", DataType.TEXT),
                colNN("ev_class", DataType.INTEGER),
                col("ev_type", DataType.CHAR),
                col("ev_enabled", DataType.CHAR),
                col("is_instead", DataType.BOOLEAN),
                col("ev_qual", DataType.TEXT),
                col("ev_action", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_rewrite", cols);
        // Views have implicit _RETURN rules in PG
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int relOid = oids.oid("rel:" + vSchema + "." + vd.name());
            table.insertRow(new Object[]{
                    oids.oid("rule:_RETURN_" + vd.name()), "_RETURN", relOid,
                    "1", "O", true, null, null, 1
            });
        }
        return table;
    }

    Table buildPgDescription() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.INTEGER),
                colNN("classoid", DataType.INTEGER),
                colNN("objsubid", DataType.INTEGER),
                col("description", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_description", cols);
        // classoid = OID of the catalog table in pg_class (must match ::regclass resolution)
        int pgAmClassOid = oids.oid("rel:pg_catalog.pg_am");
        int pgLanguageClassOid = oids.oid("rel:pg_catalog.pg_language");
        int pgExtensionClassOid = oids.oid("rel:pg_catalog.pg_extension");

        // Access method descriptions
        table.insertRow(new Object[]{2, pgAmClassOid, 0, "heap table access method", 1});
        table.insertRow(new Object[]{403, pgAmClassOid, 0, "b-tree index access method", 1});
        table.insertRow(new Object[]{405, pgAmClassOid, 0, "hash index access method", 1});
        table.insertRow(new Object[]{783, pgAmClassOid, 0, "GiST index access method", 1});
        table.insertRow(new Object[]{2742, pgAmClassOid, 0, "GIN index access method", 1});
        table.insertRow(new Object[]{4000, pgAmClassOid, 0, "SP-GiST index access method", 1});
        table.insertRow(new Object[]{3580, pgAmClassOid, 0, "block range index (BRIN) access method", 1});

        // Language descriptions
        table.insertRow(new Object[]{oids.oid("lang:internal"), pgLanguageClassOid, 0, "built-in functions", 1});
        table.insertRow(new Object[]{oids.oid("lang:c"), pgLanguageClassOid, 0, "dynamically-loaded C functions", 1});
        table.insertRow(new Object[]{oids.oid("lang:sql"), pgLanguageClassOid, 0, "SQL-language functions", 1});
        table.insertRow(new Object[]{oids.oid("lang:plpgsql"), pgLanguageClassOid, 0, "PL/pgSQL procedural language", 1});

        // Extension descriptions
        table.insertRow(new Object[]{oids.oid("ext:plpgsql"), pgExtensionClassOid, 0, "PL/pgSQL procedural language", 1});

        // Proc descriptions for handler functions
        int pgProcClassOid = oids.oid("rel:pg_catalog.pg_proc");
        table.insertRow(new Object[]{oids.oid("proc:heap_tableam_handler"), pgProcClassOid, 0, "heap table access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:bthandler"), pgProcClassOid, 0, "b-tree index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:hashhandler"), pgProcClassOid, 0, "hash index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:gisthandler"), pgProcClassOid, 0, "GiST index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:ginhandler"), pgProcClassOid, 0, "GIN index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:spghandler"), pgProcClassOid, 0, "SP-GiST index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:brinhandler"), pgProcClassOid, 0, "BRIN index access method handler", 1});

        // User-defined comments (from COMMENT ON statements)
        int pgClassClassOid = oids.oid("rel:pg_catalog.pg_class");
        int pgNamespaceClassOid = oids.oid("rel:pg_catalog.pg_namespace");
        for (Map.Entry<String, String> entry : database.getComments().entrySet()) {
            String key = entry.getKey(); // "type:name"
            String desc = entry.getValue();
            int colonIdx = key.indexOf(':');
            if (colonIdx < 0) continue;
            String objType = key.substring(0, colonIdx);
            String objName = key.substring(colonIdx + 1);

            switch (objType) {
                case "table":
                case "relation": {
                    // Find the table across schemas to get its OID
                    for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                        Table t = se.getValue().getTable(objName);
                        if (t != null) {
                            int tblOid = oids.oid("rel:" + se.getKey() + "." + objName);
                            table.insertRow(new Object[]{tblOid, pgClassClassOid, 0, desc, 1});
                            break;
                        }
                    }
                    break;
                }
                case "view": {
                    for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                        // Views are stored in database.getViews() not in schema tables
                        // but their OIDs are in rel:schema.name
                        int vOid = oids.oid("rel:" + se.getKey() + "." + objName);
                        if (vOid != 0 && database.hasView(objName)) {
                            table.insertRow(new Object[]{vOid, pgClassClassOid, 0, desc, 1});
                            break;
                        }
                    }
                    break;
                }
                case "index": {
                    // Find the index OID
                    for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                        int idxOid = oids.oid("rel:" + se.getKey() + "." + objName);
                        if (idxOid != 0) {
                            table.insertRow(new Object[]{idxOid, pgClassClassOid, 0, desc, 1});
                            break;
                        }
                    }
                    break;
                }
                case "column": {
                    // objName is "tablename.colname"
                    int dotIdx = objName.lastIndexOf('.');
                    if (dotIdx > 0) {
                        String tblName = objName.substring(0, dotIdx);
                        String colName = objName.substring(dotIdx + 1);
                        for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                            Table t = se.getValue().getTable(tblName);
                            if (t != null) {
                                int tblOid = oids.oid("rel:" + se.getKey() + "." + tblName);
                                int colIdx = t.getColumnIndex(colName);
                                if (colIdx >= 0) {
                                    table.insertRow(new Object[]{tblOid, pgClassClassOid, colIdx + 1, desc, 1});
                                }
                                break;
                            }
                        }
                    }
                    break;
                }
                case "schema": {
                    if (database.getSchema(objName) != null) {
                        int nsOid = oids.oid("ns:" + objName);
                        table.insertRow(new Object[]{nsOid, pgNamespaceClassOid, 0, desc, 1});
                    }
                    break;
                }
            }
        }

        return table;
    }

    Table buildPgTrigger() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("tgrelid", DataType.INTEGER),
                colNN("tgname", DataType.TEXT),
                col("tgfoid", DataType.INTEGER),
                col("tgtype", DataType.SMALLINT),
                col("tgenabled", DataType.CHAR),
                col("tgisinternal", DataType.BOOLEAN),
                col("tgconstrrelid", DataType.INTEGER),
                col("tgdeferrable", DataType.BOOLEAN),
                col("tginitdeferred", DataType.BOOLEAN),
                col("tgargs", DataType.TEXT),
                col("tgattr", DataType.TEXT),
                col("tgconstraint", DataType.INTEGER),
                col("tgoldtable", DataType.TEXT),
                col("tgnewtable", DataType.TEXT),
                col("tgparentid", DataType.INTEGER),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_trigger", cols);
        // Group triggers by name to combine multiple events into one pg_trigger row
        Map<String, List<PgTrigger>> byName = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, List<PgTrigger>> entry : database.getAllTriggers().entrySet()) {
            for (PgTrigger trigger : entry.getValue()) {
                byName.computeIfAbsent(trigger.getName(), k -> new java.util.ArrayList<>()).add(trigger);
            }
        }
        for (Map.Entry<String, List<PgTrigger>> entry : byName.entrySet()) {
            PgTrigger first = entry.getValue().get(0);
            String trigSchema = first.getSchemaName() != null ? first.getSchemaName() : "public";
            Table t = null;
            Schema schema = database.getSchemas().get(trigSchema);
            if (schema != null) t = schema.getTable(first.getTableName());
            if (t == null) {
                for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                    Table candidate = se.getValue().getTable(first.getTableName());
                    if (candidate != null) { t = candidate; trigSchema = se.getKey(); break; }
                }
            }
            int relOid = t != null ? oids.oid("rel:" + trigSchema + "." + first.getTableName()) : 0;

            // Build tgtype bitmask: bit 0=ROW, bit 1=BEFORE, bit 2=AFTER, bit 3=INSERT, bit 4=DELETE, bit 5=UPDATE, bit 6=TRUNCATE
            int tgtype = first.isForEachStatement() ? 0 : 1; // bit 0 = FOR EACH ROW
            if (first.getTiming() == PgTrigger.Timing.BEFORE) tgtype |= (1 << 1);
            else if (first.getTiming() == PgTrigger.Timing.AFTER) tgtype |= (1 << 2);
            for (PgTrigger trig : entry.getValue()) {
                switch (trig.getEvent()) {
                    case INSERT:
                        tgtype |= (1 << 3);
                        break;
                    case DELETE:
                        tgtype |= (1 << 4);
                        break;
                    case UPDATE:
                        tgtype |= (1 << 5);
                        break;
                    case TRUNCATE:
                        tgtype |= (1 << 6);
                        break;
                }
            }

            // Resolve trigger function OID from pg_proc
            int tgfoid = oids.oid("proc:" + first.getFunctionName());

            table.insertRow(new Object[]{
                    oids.oid("trig:" + first.getName()), relOid, first.getName(),
                    tgfoid, (short) tgtype, "O", false, 0, false, false,
                    "", null, 0, null, null, 0, 1
            });
        }
        return table;
    }

    /** Recursively collect table references from FROM items for view dependency tracking. */
    void collectViewDependencies(com.memgres.engine.parser.ast.SelectStmt.FromItem fromItem,
                                  String defaultSchema,
                                  int rewriteClassOid, int ruleOid,
                                  int pgClassOid, Table depTable) {
        if (fromItem instanceof com.memgres.engine.parser.ast.SelectStmt.TableRef) {
            com.memgres.engine.parser.ast.SelectStmt.TableRef tr = (com.memgres.engine.parser.ast.SelectStmt.TableRef) fromItem;
            String tSchema = tr.schema() != null ? tr.schema() : defaultSchema;
            String tName = tr.table();
            // Verify the table exists in some schema
            boolean found = false;
            for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                if (se.getKey().equalsIgnoreCase(tSchema) && se.getValue().getTable(tName) != null) {
                    found = true;
                    break;
                }
            }
            if (found) {
                int tableOid = oids.oid("rel:" + tSchema + "." + tName);
                // deptype 'n' = normal dependency
                depTable.insertRow(new Object[]{rewriteClassOid, ruleOid, 0, pgClassOid, tableOid, 0, "n"});
            }
        } else if (fromItem instanceof com.memgres.engine.parser.ast.SelectStmt.JoinFrom) {
            com.memgres.engine.parser.ast.SelectStmt.JoinFrom jf = (com.memgres.engine.parser.ast.SelectStmt.JoinFrom) fromItem;
            collectViewDependencies(jf.left(), defaultSchema, rewriteClassOid, ruleOid, pgClassOid, depTable);
            collectViewDependencies(jf.right(), defaultSchema, rewriteClassOid, ruleOid, pgClassOid, depTable);
        } else if (fromItem instanceof com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom) {
            com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom sq = (com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom) fromItem;
            if (sq.subquery() instanceof com.memgres.engine.parser.ast.SelectStmt && ((com.memgres.engine.parser.ast.SelectStmt) sq.subquery()).from() != null) {
                com.memgres.engine.parser.ast.SelectStmt sub = (com.memgres.engine.parser.ast.SelectStmt) sq.subquery();
                for (com.memgres.engine.parser.ast.SelectStmt.FromItem fi : sub.from()) {
                    collectViewDependencies(fi, defaultSchema, rewriteClassOid, ruleOid, pgClassOid, depTable);
                }
            }
        }
    }
}
