package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds core catalog tables that deal with relational metadata:
 * pg_class, pg_attribute, pg_type, pg_namespace, pg_enum, pg_proc.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogCoreBuilder {

    final Database database;
    final OidSupplier oids;

    CatalogCoreBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    Table buildPgClass() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("relname", DataType.TEXT),
                colNN("relnamespace", DataType.INTEGER),
                col("reltype", DataType.INTEGER),
                col("reloftype", DataType.INTEGER),
                colNN("relowner", DataType.INTEGER),
                col("relam", DataType.INTEGER),
                col("relfilenode", DataType.INTEGER),
                col("reltablespace", DataType.INTEGER),
                col("relpages", DataType.INTEGER),
                col("reltuples", DataType.DOUBLE_PRECISION),
                col("relallvisible", DataType.INTEGER),
                col("relallfrozen", DataType.INTEGER),
                col("reltoastrelid", DataType.INTEGER),
                col("relhasindex", DataType.BOOLEAN),
                col("relisshared", DataType.BOOLEAN),
                col("relpersistence", DataType.CHAR),
                colNN("relkind", DataType.CHAR),
                col("relnatts", DataType.SMALLINT),
                col("relchecks", DataType.SMALLINT),
                col("relhasrules", DataType.BOOLEAN),
                col("relhastriggers", DataType.BOOLEAN),
                col("relhassubclass", DataType.BOOLEAN),
                col("relrowsecurity", DataType.BOOLEAN),
                col("relforcerowsecurity", DataType.BOOLEAN),
                col("relhasoids", DataType.BOOLEAN),
                col("relispopulated", DataType.BOOLEAN),
                col("relreplident", DataType.CHAR),
                col("relispartition", DataType.BOOLEAN),
                col("relrewrite", DataType.INTEGER),
                col("relfrozenxid", DataType.INTEGER),
                col("relminmxid", DataType.INTEGER),
                col("relacl", DataType.ACLITEM_ARRAY),
                col("reloptions", DataType.TEXT),
                col("relpartbound", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_class", cols);

        // System catalog tables (pg_catalog schema)
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        String[] systemTables = {
            "pg_class", "pg_attribute", "pg_type", "pg_namespace", "pg_constraint",
            "pg_index", "pg_proc", "pg_description", "pg_settings", "pg_tables",
            "pg_views", "pg_sequences", "pg_am", "pg_database", "pg_roles",
            "pg_stat_activity", "pg_enum", "pg_trigger", "pg_depend", "pg_attrdef",
            "pg_locks", "pg_stat_user_tables", "pg_stat_user_indexes",
            // Additional system catalog tables present in PG 18
            "pg_aggregate", "pg_amop", "pg_amproc", "pg_auth_members",
            "pg_authid", "pg_cast", "pg_collation", "pg_conversion",
            "pg_default_acl", "pg_event_trigger", "pg_extension",
            "pg_foreign_data_wrapper", "pg_foreign_server", "pg_foreign_table",
            "pg_inherits", "pg_init_privs", "pg_language", "pg_largeobject",
            "pg_largeobject_metadata", "pg_matviews", "pg_opclass", "pg_operator",
            "pg_opfamily", "pg_partitioned_table", "pg_policy",
            "pg_publication", "pg_publication_rel", "pg_range", "pg_replication_origin",
            "pg_rewrite", "pg_seclabel", "pg_sequence", "pg_shdepend",
            "pg_shdescription", "pg_shseclabel", "pg_statistic",
            "pg_statistic_ext", "pg_statistic_ext_data", "pg_subscription",
            "pg_subscription_rel", "pg_tablespace", "pg_transform",
            "pg_ts_config", "pg_ts_config_map", "pg_ts_dict", "pg_ts_parser",
            "pg_ts_template", "pg_user_mapping",
            "pg_stat_all_indexes", "pg_stat_all_tables",
            "pg_stat_bgwriter", "pg_stat_database",
            "pg_statio_all_indexes", "pg_statio_all_sequences",
            "pg_statio_all_tables", "pg_stat_replication",
            "pg_stat_wal_receiver", "pg_stat_xact_all_tables",
            "pg_stat_xact_user_tables",
            // System indexes (PG includes indexes in pg_class)
            "pg_type_oid_index", "pg_attribute_relid_attnum_index",
            "pg_proc_oid_index", "pg_class_oid_index",
            "pg_namespace_oid_index", "pg_constraint_oid_index",
            "pg_index_indrelid_index", "pg_index_indexrelid_index",
            "pg_description_o_c_o_index", "pg_depend_depender_index",
            "pg_depend_reference_index", "pg_attrdef_adrelid_adnum_index",
            "pg_trigger_tgrelid_index", "pg_enum_oid_index",
            "pg_cast_source_target_index", "pg_collation_oid_index",
            "pg_am_oid_index", "pg_database_oid_index",
            // Additional system views
            "pg_stat_sys_tables", "pg_stat_sys_indexes",
            "pg_statio_sys_tables", "pg_statio_sys_indexes",
            "pg_statio_sys_sequences", "pg_statio_user_tables",
            "pg_statio_user_indexes", "pg_statio_user_sequences",
            "pg_stat_xact_sys_tables", "pg_prepared_statements",
            "pg_cursors", "pg_available_extensions",
            "pg_available_extension_versions", "pg_prepared_xacts",
            "pg_shmem_allocations", "pg_backend_memory_contexts",
            "pg_config", "pg_file_settings",
            "pg_hba_file_rules", "pg_timezone_names"
        };
        for (String sysTable : systemTables) {
            int sysOid = oids.oid("rel:pg_catalog." + sysTable);
            table.insertRow(new Object[]{
                    sysOid, sysTable, pgCatalogNs,
                    0, 0,            // reltype, reloftype
                    10,              // relowner
                    0,               // relam
                    sysOid,          // relfilenode (= oid for system tables)
                    0,               // reltablespace
                    0, 0.0, 0, 0, 0,   // relpages, reltuples, relallvisible, relallfrozen, reltoastrelid
                    false, false, "p", "r", // relhasindex, relisshared, relpersistence, relkind
                    (short) 0, (short) 0,   // relnatts, relchecks
                    false, false, false, false, false, // relhasrules..relforcerowsecurity
                    false,                  // relhasoids (removed in PG 12, always false)
                    true, "d", false,       // relispopulated, relreplident, relispartition
                    0, 0, 0,                // relrewrite, relfrozenxid, relminmxid
                    null, null, null, 1     // relacl, reloptions, relpartbound, xmin
            });
        }

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            int nsOid = oids.oid("ns:" + schemaEntry.getKey());
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int ownerOid = resolveOwnerOid(database, oids, "table:" + schemaEntry.getKey() + "." + t.getName());
                int tblOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                // Count CHECK constraints
                short checkCount = 0;
                boolean hasTriggers = false;
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.CHECK) checkCount++;
                }
                if (database.getAllTriggers().containsKey(t.getName())) hasTriggers = true;
                boolean hasIdx = !t.getConstraints().isEmpty() || database.getIndexColumns().keySet().stream()
                        .anyMatch(idx -> { String ti = database.getIndexTable(idx); return ti != null && ti.endsWith("." + t.getName()); });
                // Partition metadata for pg_class
                String relkind = t.getPartitionStrategy() != null ? "p" : "r";
                boolean relispartition = t.getPartitionParent() != null;
                String relpartbound = relispartition ? formatPartitionBound(t) : null;
                table.insertRow(new Object[]{
                        tblOid, t.getName(), nsOid,
                        0, 0,            // reltype, reloftype
                        ownerOid,
                        2,               // relam (heap=2)
                        tblOid,          // relfilenode
                        0,               // reltablespace
                        0, (double) t.getRows().size(), 0, 0, 0, // relpages, reltuples, relallvisible, relallfrozen, reltoastrelid
                        hasIdx, false, "p", relkind,          // relhasindex, relisshared, relpersistence, relkind
                        (short) t.getColumns().size(), checkCount, // relnatts, relchecks
                        false, hasTriggers, false, false, false, // relhasrules..relforcerowsecurity
                        false,              // relhasoids
                        true, "d", relispartition, // relispopulated, relreplident, relispartition
                        0, 0, 0,            // relrewrite, relfrozenxid, relminmxid
                        null, null, relpartbound, 1 // relacl, reloptions, relpartbound, xmin
                });
            }
        }

        // Views
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int viewOwnerOid = resolveOwnerOid(database, oids, "view:" + vSchema + "." + vd.name());
            int vOid = oids.oid("rel:" + vSchema + "." + vd.name());
            table.insertRow(new Object[]{
                    vOid, vd.name(), oids.oid("ns:" + vSchema),
                    0, 0, viewOwnerOid, 0, vOid, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", "v",
                    (short) 0, (short) 0,
                    true, false, false, false, false,
                    false, // relhasoids
                    true, "n", false,
                    0, 0, 0,
                    null, null, null, 1
            });
        }

        // Sequences - explicit sequences (use public namespace)
        for (String seqName : database.getSequences().keySet()) {
            int seqOwnerOid = resolveOwnerOid(database, oids, "sequence:" + seqName);
            int sOid = oids.oid("rel:public." + seqName);
            table.insertRow(new Object[]{
                    sOid, seqName, oids.oid("ns:public"),
                    0, 0, seqOwnerOid, 0, sOid, 0,
                    1, 1.0, 0, 0, 0,
                    false, false, "p", "S",
                    (short) 3, (short) 0,   // sequences have 3 columns (last_value, log_cnt, is_called)
                    false, false, false, false, false,
                    false, // relhasoids
                    true, "n", false, 0, 0, 0,
                    null, null, null, 1
            });
        }
        // Sequences - implicit from SERIAL/BIGSERIAL/SMALLSERIAL and identity columns
        for (Map.Entry<String, Schema> seqSchemaEntry : database.getSchemas().entrySet()) {
            String seqSchemaName = seqSchemaEntry.getKey();
            int seqNsOid = oids.oid("ns:" + seqSchemaName);
            for (Map.Entry<String, Table> seqTableEntry : seqSchemaEntry.getValue().getTables().entrySet()) {
                Table seqT = seqTableEntry.getValue();
                for (Column seqCol : seqT.getColumns()) {
                    String implicitSeqName = null;
                    if (seqCol.getType() == DataType.SERIAL || seqCol.getType() == DataType.BIGSERIAL || seqCol.getType() == DataType.SMALLSERIAL) {
                        implicitSeqName = seqT.getName() + "_" + seqCol.getName() + "_seq";
                    } else if (seqCol.getDefaultValue() != null && seqCol.getDefaultValue().contains("__identity__")) {
                        implicitSeqName = seqT.getName() + "_" + seqCol.getName() + "_seq";
                    }
                    if (implicitSeqName != null) {
                        int isOid = oids.oid("rel:" + seqSchemaName + "." + implicitSeqName);
                        table.insertRow(new Object[]{
                                isOid, implicitSeqName, seqNsOid,
                                0, 0, 10, 0, isOid, 0,
                                1, 1.0, 0, 0, 0,
                                false, false, "p", "S",
                                (short) 3, (short) 0,
                                false, false, false, false, false,
                                false, // relhasoids
                                true, "n", false, 0, 0, 0,
                                null, null, null, 1
                        });
                    }
                }
            }
        }

        // Indexes (from explicit CREATE INDEX)
        Set<String> addedIndexNames = new HashSet<>();
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexName = idx.getKey();
            addedIndexNames.add(indexName.toLowerCase());
            String storedTableQualified = database.getIndexTable(indexName);
            String indexSchema = "public";
            if (storedTableQualified != null) {
                String[] parts = storedTableQualified.split("\\.", 2);
                if (parts.length == 2) {
                    indexSchema = parts[0];
                    String tableName = parts[1];
                    Schema schema = database.getSchema(indexSchema);
                    if (schema == null || schema.getTable(tableName) == null) continue;
                }
            } else {
                for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                    for (Map.Entry<String, Table> te : se.getValue().getTables().entrySet()) {
                        boolean allFound = true;
                        for (String colName : idx.getValue()) {
                            if (te.getValue().getColumnIndex(colName) < 0) { allFound = false; break; }
                        }
                        if (allFound) { indexSchema = se.getKey(); break; }
                    }
                }
            }
            int idxOid = oids.oid("rel:" + indexSchema + "." + indexName);
            short idxNatts = (short) idx.getValue().size();
            table.insertRow(new Object[]{
                    idxOid, indexName, oids.oid("ns:" + indexSchema),
                    0, 0, 10, 403, idxOid, 0,  // relam=403 (btree)
                    1, 0.0, 0, 0, 0,
                    false, false, "p", "i",
                    idxNatts, (short) 0,
                    false, false, false, false, false,
                    false, // relhasoids
                    true, "n", false, 0, 0, 0,
                    null, null, null, 1
            });
        }

        // Indexes from PK/UNIQUE constraints (implicit indexes)
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                for (StoredConstraint sc : tableEntry.getValue().getConstraints()) {
                    if ((sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE)
                            && !addedIndexNames.contains(sc.getName().toLowerCase())) {
                        int ciOid = oids.oid("rel:" + schemaEntry.getKey() + "." + sc.getName());
                        short ciNatts = (short) (sc.getColumns() != null ? sc.getColumns().size() : 0);
                        table.insertRow(new Object[]{
                                ciOid, sc.getName(), oids.oid("ns:" + schemaEntry.getKey()),
                                0, 0, 10, 403, ciOid, 0,
                                1, 0.0, 0, 0, 0,
                                false, false, "p", "i",
                                ciNatts, (short) 0,
                                false, false, false, false, false,
                                false, // relhasoids
                                true, "n", false, 0, 0, 0,
                                null, null, null, 1
                        });
                    }
                }
            }
        }

        return table;
    }

    /** Format a partition bound expression for relpartbound (PG-compatible syntax). */
    private static String formatPartitionBound(Table t) {
        if (t.isDefaultPartition()) return "DEFAULT";
        if (t.getPartitionLower() != null && t.getPartitionUpper() != null) {
            return "FOR VALUES FROM (" + formatBoundValue(t.getPartitionLower())
                    + ") TO (" + formatBoundValue(t.getPartitionUpper()) + ")";
        }
        if (t.getPartitionValues() != null) {
            StringBuilder sb = new StringBuilder("FOR VALUES IN (");
            for (int i = 0; i < t.getPartitionValues().size(); i++) {
                if (i > 0) sb.append(", ");
                Object v = t.getPartitionValues().get(i);
                if (v instanceof String) sb.append("'").append(v).append("'");
                else sb.append(v);
            }
            sb.append(")");
            return sb.toString();
        }
        if (t.getPartitionModulus() != null && t.getPartitionRemainder() != null) {
            return "FOR VALUES WITH (modulus " + t.getPartitionModulus()
                    + ", remainder " + t.getPartitionRemainder() + ")";
        }
        return null;
    }

    private static String formatBoundValue(Object val) {
        if (val instanceof String) {
            String s = (String) val;
            if (s.equalsIgnoreCase("MINVALUE") || s.equalsIgnoreCase("MAXVALUE")) return s;
            return "'" + s + "'";
        }
        return String.valueOf(val);
    }

    Table buildPgAttribute() {
        List<Column> cols = Cols.listOf(
                colNN("attrelid", DataType.INTEGER),
                colNN("attname", DataType.TEXT),
                colNN("atttypid", DataType.INTEGER),
                colNN("attnum", DataType.SMALLINT),
                colNN("attnotnull", DataType.BOOLEAN),
                col("atttypmod", DataType.INTEGER),
                col("attlen", DataType.SMALLINT),
                colNN("attisdropped", DataType.BOOLEAN),
                colNN("atthasdef", DataType.BOOLEAN),
                col("attidentity", DataType.CHAR),
                col("attgenerated", DataType.CHAR),
                col("attcollation", DataType.INTEGER),
                col("xmin", DataType.INTEGER),
                col("attislocal", DataType.BOOLEAN),
                col("attinhcount", DataType.INTEGER),
                col("attfdwoptions", DataType.TEXT),
                col("attndims", DataType.INTEGER),
                col("attacl", DataType.ACLITEM_ARRAY),
                col("attoptions", DataType.TEXT_ARRAY),
                col("attstattarget", DataType.SMALLINT),
                col("attstorage", DataType.CHAR),
                col("attcompression", DataType.CHAR),
                col("atthasmissing", DataType.BOOLEAN),
                col("attmissingval", DataType.TEXT),
                col("attalign", DataType.CHAR)
        );
        Table table = new Table("pg_attribute", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (int i = 0; i < t.getColumns().size(); i++) {
                    Column c = t.getColumns().get(i);
                    // Determine identity type
                    // SERIAL/BIGSERIAL/SMALLSERIAL are NOT identity columns (attidentity stays empty)
                    // Only actual GENERATED AS IDENTITY columns get 'd' or 'a'
                    String identity = "";
                    DataType colType = c.getType();
                    boolean hasDefault = c.getDefaultValue() != null
                            || colType == DataType.SERIAL || colType == DataType.BIGSERIAL || colType == DataType.SMALLSERIAL
                            || !identity.isEmpty()
                            || c.isGenerated();
                    // Determine attlen from the type's typlen
                    short attlen;
                    switch (colType) {
                        case BOOLEAN:
                            attlen = (short) 1;
                            break;
                        case SMALLINT:
                        case SMALLSERIAL:
                            attlen = (short) 2;
                            break;
                        case INTEGER:
                        case SERIAL:
                        case REAL:
                            attlen = (short) 4;
                            break;
                        case BIGINT:
                        case BIGSERIAL:
                        case DOUBLE_PRECISION:
                            attlen = (short) 8;
                            break;
                        case NAME:
                            attlen = (short) 64;
                            break;
                        default:
                            attlen = (short) -1;
                            break;
                    }
                    // Determine storage type based on data type
                    // p = plain, x = extended, e = external, m = main
                    String storage;
                    switch (colType) {
                        case TEXT:
                        case VARCHAR:
                        case BYTEA:
                        case JSON:
                        case JSONB:
                        case XML:
                            storage = "x";
                            break;
                        case NUMERIC:
                            storage = "m";
                            break;
                        default:
                            storage = "p";
                            break;
                    }
                    // Compute atttypmod: varchar(n) → n+4, char(n) → n+4, numeric(p,s) → (p<<16|s)+4
                    int typmod = -1;
                    if ((colType == DataType.VARCHAR || colType == DataType.CHAR) && c.getPrecision() != null) {
                        typmod = c.getPrecision() + 4;
                    } else if (colType == DataType.NUMERIC && c.getPrecision() != null) {
                        int scale = c.getScale() != null ? c.getScale() : 0;
                        typmod = (c.getPrecision() << 16 | scale) + 4;
                    }
                    // Resolve atttypid: use custom type OID for enums/domains
                    int atttypid = c.getType().getOid();
                    if (colType == DataType.ENUM && c.getEnumTypeName() != null) {
                        atttypid = oids.oid("type:" + c.getEnumTypeName());
                    } else if (c.getDomainTypeName() != null) {
                        atttypid = oids.oid("type:" + c.getDomainTypeName());
                    }
                    table.insertRow(new Object[]{
                            relOid,
                            c.getName(),
                            atttypid,
                            (short) (i + 1),
                            !c.isNullable(),
                            typmod,
                            attlen,
                            false,
                            hasDefault,
                            identity,  // attidentity
                            c.isVirtual() ? "v" : c.isGenerated() ? "s" : "",  // attgenerated
                            0,         // attcollation
                            1, true, 0, null, 0, null,  // xmin, attislocal, attinhcount, attfdwoptions, attndims, attacl
                            null,      // attoptions
                            (short) -1, // attstattarget
                            storage,   // attstorage
                            "",        // attcompression
                            false,     // atthasmissing
                            null,      // attmissingval
                            "i"        // attalign
                    });
                }
            }
        }
        return table;
    }

    Table buildPgType() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("typname", DataType.TEXT),
                colNN("typnamespace", DataType.INTEGER),
                col("typowner", DataType.INTEGER),
                col("typlen", DataType.SMALLINT),
                col("typbyval", DataType.BOOLEAN),
                col("typtype", DataType.CHAR),
                col("typcategory", DataType.CHAR),
                col("typispreferred", DataType.BOOLEAN),
                col("typisdefined", DataType.BOOLEAN),
                col("typdelim", DataType.CHAR),
                col("typrelid", DataType.INTEGER),
                col("typsubscript", DataType.TEXT),
                col("typelem", DataType.INTEGER),
                col("typarray", DataType.INTEGER),
                col("typinput", DataType.TEXT),
                col("typoutput", DataType.TEXT),
                col("typreceive", DataType.TEXT),
                col("typsend", DataType.TEXT),
                col("typmodin", DataType.TEXT),
                col("typmodout", DataType.TEXT),
                col("typanalyze", DataType.TEXT),
                col("typalign", DataType.CHAR),
                col("typstorage", DataType.CHAR),
                col("typnotnull", DataType.BOOLEAN),
                col("typbasetype", DataType.INTEGER),
                col("typtypmod", DataType.INTEGER),
                col("typndims", DataType.INTEGER),
                col("typcollation", DataType.INTEGER),
                col("typdefaultbin", DataType.TEXT),
                col("typdefault", DataType.TEXT),
                col("typacl", DataType.ACLITEM_ARRAY),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_type", cols);
        int pgCatalogOid = oids.oid("ns:pg_catalog");

        for (DataType dt : DataType.values()) {
            if (dt == DataType.ENUM || dt == DataType.SERIAL || dt == DataType.BIGSERIAL
                    || dt == DataType.SMALLSERIAL
                    || dt == DataType.TEXT_ARRAY || dt == DataType.INT4_ARRAY
                    || dt == DataType.ACLITEM_ARRAY) continue;
            String cat;
            switch (dt) {
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case REAL:
                case DOUBLE_PRECISION:
                case NUMERIC:
                case MONEY:
                    cat = "N";
                    break;
                case BOOLEAN:
                    cat = "B";
                    break;
                case VARCHAR:
                case CHAR:
                case TEXT:
                case NAME:
                    cat = "S";
                    break;
                case DATE:
                case TIMESTAMP:
                case TIMESTAMPTZ:
                case TIME:
                case INTERVAL:
                    cat = "D";
                    break;
                default:
                    cat = "U";
                    break;
            }
            short typlen;
            switch (dt) {
                case BOOLEAN:
                    typlen = (short) 1;
                    break;
                case SMALLINT:
                    typlen = (short) 2;
                    break;
                case INTEGER:
                    typlen = (short) 4;
                    break;
                case BIGINT:
                    typlen = (short) 8;
                    break;
                case REAL:
                    typlen = (short) 4;
                    break;
                case DOUBLE_PRECISION:
                    typlen = (short) 8;
                    break;
                case NAME:
                    typlen = (short) 64;
                    break;
                default:
                    typlen = (short) -1;
                    break;
            }
            boolean isPreferred;
            switch (dt) {
                case DOUBLE_PRECISION:
                case TEXT:
                case BOOLEAN:
                case TIMESTAMPTZ:
                    isPreferred = true;
                    break;
                default:
                    isPreferred = false;
                    break;
            }
            // Collation OID: only for string types
            int collation = (cat.equals("S")) ? 100 : 0;
            // typarray: point base types to their array type OID
            int typarray;
            switch (dt) {
                case TEXT:
                    typarray = 1009;
                    break;
                case INTEGER:
                    typarray = 1007;
                    break;
                default:
                    typarray = 0;
                    break;
            }
            // typbyval: passed by value for small fixed-size types
            boolean typbyval;
            switch (dt) {
                case BOOLEAN:
                case SMALLINT:
                case INTEGER:
                case REAL:
                    typbyval = true;
                    break;
                case BIGINT:
                case DOUBLE_PRECISION:
                    typbyval = true;
                    break;
                default:
                    typbyval = false;
                    break;
            }
            // typalign: 'c'=char, 's'=short, 'i'=int, 'd'=double
            String typalign;
            switch (dt) {
                case BOOLEAN:
                case CHAR:
                    typalign = "c";
                    break;
                case SMALLINT:
                    typalign = "s";
                    break;
                case INTEGER:
                case REAL:
                case DATE:
                    typalign = "i";
                    break;
                default:
                    typalign = "d";
                    break;
            }
            // typstorage: 'p'=plain, 'x'=extended, 'e'=external, 'm'=main
            String typstorage;
            switch (dt) {
                case BOOLEAN:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case REAL:
                case DOUBLE_PRECISION:
                case DATE:
                    typstorage = "p";
                    break;
                case TEXT:
                case VARCHAR:
                case BYTEA:
                case JSON:
                case JSONB:
                case XML:
                    typstorage = "x";
                    break;
                default:
                    typstorage = "p";
                    break;
            }
            String pgName = dt.getPgName();
            table.insertRow(new Object[]{
                    dt.getOid(), pgName, pgCatalogOid,
                    10,          // typowner
                    typlen, typbyval, "b", cat, isPreferred, true, ",",
                    0,           // typrelid
                    null,        // typsubscript
                    0,           // typelem
                    typarray,
                    pgName + "in", pgName + "out",       // typinput, typoutput
                    pgName + "recv", pgName + "send",    // typreceive, typsend
                    "-", "-", "-",                        // typmodin, typmodout, typanalyze
                    typalign, typstorage,
                    false, 0, -1, 0, collation,          // typnotnull, typbasetype, typtypmod, typndims, typcollation
                    null, null, null, 1                   // typdefaultbin, typdefault, typacl, xmin
            });
        }

        // Array types, manually added with correct typelem and typcategory='A'
        // _text (OID 1009): text[]
        table.insertRow(new Object[]{
                1009, "_text", pgCatalogOid, 10,
                (short) -1, false, "b", "A", false, true, ",",
                0, "array_subscript_handler", 25, 0,
                "array_in", "array_out", "array_recv", "array_send",
                "-", "-", "-", "d", "x",
                false, 0, -1, 0, 100, null, null, null, 1
        });
        // _int4 (OID 1007): integer[]
        table.insertRow(new Object[]{
                1007, "_int4", pgCatalogOid, 10,
                (short) -1, false, "b", "A", false, true, ",",
                0, "array_subscript_handler", 23, 0,
                "array_in", "array_out", "array_recv", "array_send",
                "-", "-", "-", "i", "x",
                false, 0, -1, 0, 0, null, null, null, 1
        });
        // aclitem base type (OID 1033)
        table.insertRow(new Object[]{
                1033, "aclitem", pgCatalogOid, 10,
                (short) 12, false, "b", "U", false, true, ",",
                0, null, 0, 1034,
                "aclitemin", "aclitemout", "-", "-",
                "-", "-", "-", "i", "p",
                false, 0, -1, 0, 0, null, null, null, 1
        });
        // _aclitem (OID 1034): aclitem[]
        table.insertRow(new Object[]{
                1034, "_aclitem", pgCatalogOid, 10,
                (short) -1, false, "b", "A", false, true, ",",
                0, "array_subscript_handler", 1033, 0,
                "array_in", "array_out", "array_recv", "array_send",
                "-", "-", "-", "i", "x",
                false, 0, -1, 0, 0, null, null, null, 1
        });

        // Add custom enum types
        for (CustomEnum ce : database.getCustomEnums().values()) {
            // Determine the schema this enum belongs to via the schema object registry
            int enumNsOid = oids.oid("ns:public"); // default to public
            for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                java.util.Set<String> objs = database.getSchemaObjects(se.getKey());
                if (objs != null && objs.contains("enum:" + ce.getName().toLowerCase())) {
                    enumNsOid = oids.oid("ns:" + se.getKey());
                    break;
                }
            }
            table.insertRow(new Object[]{
                    oids.oid("type:" + ce.getName()), ce.getName(), enumNsOid, 10,
                    (short) 4, true, "e", "E", false, true, ",",
                    0, null, 0, 0,
                    "enum_in", "enum_out", "enum_recv", "enum_send",
                    "-", "-", "-", "i", "p",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // Add domain types
        for (DomainType dom : database.getDomains().values()) {
            int domNsOid = oids.oid("ns:public");
            // Resolve base type OID
            int baseTypeOid = 0;
            String baseTypeCat = "U";
            for (DataType dt : DataType.values()) {
                if (dt.getPgName().equalsIgnoreCase(dom.getBaseTypeName())
                        || dt.name().equalsIgnoreCase(dom.getBaseTypeName())) {
                    baseTypeOid = dt.getOid();
                    switch (dt) {
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case REAL:
                        case DOUBLE_PRECISION:
                        case NUMERIC:
                        case MONEY:
                            baseTypeCat = "N";
                            break;
                        case BOOLEAN:
                            baseTypeCat = "B";
                            break;
                        case VARCHAR:
                        case CHAR:
                        case TEXT:
                        case NAME:
                            baseTypeCat = "S";
                            break;
                        case DATE:
                        case TIMESTAMP:
                        case TIMESTAMPTZ:
                        case TIME:
                        case INTERVAL:
                            baseTypeCat = "D";
                            break;
                        default:
                            baseTypeCat = "U";
                            break;
                    }
                    break;
                }
            }
            table.insertRow(new Object[]{
                    oids.oid("type:" + dom.getName()), dom.getName(), domNsOid, 10,
                    (short) -1, false, "d", baseTypeCat, false, true, ",",
                    0, null, 0, 0,
                    "domain_in", "domain_out", "domain_recv", "domain_send",
                    "-", "-", "-", "i", "x",
                    dom.isNotNull(), baseTypeOid, -1, 0, 0, null,
                    dom.getDefaultValue(), null, 1
            });
        }

        return table;
    }

    Table buildPgNamespace() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("nspname", DataType.TEXT),
                colNN("nspowner", DataType.INTEGER),
                col("xmin", DataType.INTEGER),
                col("nspacl", DataType.ACLITEM_ARRAY)
        );
        Table table = new Table("pg_namespace", cols);

        // Built-in namespaces; nspacl=NULL means "default ACL", which is what pg_dump expects
        table.insertRow(new Object[]{oids.oid("ns:pg_catalog"), "pg_catalog", 10, 1, null});
        table.insertRow(new Object[]{oids.oid("ns:information_schema"), "information_schema", 10, 1, null});
        table.insertRow(new Object[]{oids.oid("ns:pg_toast"), "pg_toast", 10, 1, null});

        for (String schemaName : database.getSchemas().keySet()) {
            table.insertRow(new Object[]{oids.oid("ns:" + schemaName), schemaName, 10, 1, null});
        }
        return table;
    }

    Table buildPgEnum() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("enumtypid", DataType.INTEGER),
                colNN("enumsortorder", DataType.DOUBLE_PRECISION),
                colNN("enumlabel", DataType.TEXT)
        );
        Table table = new Table("pg_enum", cols);
        for (Map.Entry<String, CustomEnum> entry : database.getCustomEnums().entrySet()) {
            CustomEnum ce = entry.getValue();
            int typid = oids.oid("type:" + ce.getName());
            List<String> labels = ce.getLabels();
            for (int i = 0; i < labels.size(); i++) {
                table.insertRow(new Object[]{
                        oids.oid("enum:" + ce.getName() + ":" + labels.get(i)),
                        typid, (double) (i + 1), labels.get(i)
                });
            }
        }
        return table;
    }

    Table buildPgProc() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("proname", DataType.TEXT),
                colNN("pronamespace", DataType.INTEGER),
                col("proowner", DataType.INTEGER),
                col("prolang", DataType.INTEGER),
                col("procost", DataType.DOUBLE_PRECISION),
                col("prorows", DataType.DOUBLE_PRECISION),
                col("provariadic", DataType.INTEGER),
                col("prosupport", DataType.TEXT),
                col("prokind", DataType.CHAR),
                col("prosecdef", DataType.BOOLEAN),
                col("proleakproof", DataType.BOOLEAN),
                col("proisstrict", DataType.BOOLEAN),
                col("proretset", DataType.BOOLEAN),
                col("provolatile", DataType.CHAR),
                col("proparallel", DataType.CHAR),
                col("pronargs", DataType.SMALLINT),
                col("pronargdefaults", DataType.SMALLINT),
                col("prorettype", DataType.INTEGER),
                col("proargtypes", DataType.TEXT),
                col("proallargtypes", DataType.TEXT),
                col("proargmodes", DataType.TEXT),
                col("proargnames", DataType.TEXT),
                col("proargdefaults", DataType.TEXT),
                col("protrftypes", DataType.TEXT),
                col("prosrc", DataType.TEXT),
                col("probin", DataType.TEXT),
                col("prosqlbody", DataType.TEXT),
                col("proconfig", DataType.TEXT),
                col("proacl", DataType.ACLITEM_ARRAY),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_proc", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        int cLangOid = oids.oid("lang:c");
        int internalLangOid = oids.oid("lang:internal");

        int amHandlerType = oids.oid("type:index_am_handler");
        // Built-in handler functions for pg_am (access methods)
        String[] amHandlers = {"heap_tableam_handler", "bthandler", "hashhandler",
                "gisthandler", "ginhandler", "spghandler", "brinhandler"};
        for (String h : amHandlers) {
            int hLang = h.equals("heap_tableam_handler") ? cLangOid : internalLangOid;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + h), h, pgCatalogNs, 10, hLang, 1.0, 0.0,
                    0, "-", "f", false, false, true, false, "v", "u",
                    (short) 1, (short) 0, amHandlerType,
                    "2281", null, null, null, null, null,
                    h, null, null, null, null, 1
            });
        }

        // Language handler/validator/inline functions (referenced by pg_language)
        int langHandlerType = oids.oid("type:language_handler");
        int voidType = oids.oid("type:void");
        int sqlLangOid = oids.oid("lang:sql");
        // Validators: return void, take oid arg
        String[] validators = {"fmgr_internal_validator", "fmgr_c_validator", "fmgr_sql_validator"};
        for (String v : validators) {
            table.insertRow(new Object[]{
                    oids.oid("proc:" + v), v, pgCatalogNs, 10, internalLangOid, 1.0, 0.0,
                    0, "-", "f", false, false, true, false, "v", "u",
                    (short) 1, (short) 0, voidType,
                    "26", null, null, null, null, null,
                    v, null, null, null, null, 1
            });
        }
        // PL/pgSQL call handler: returns language_handler
        table.insertRow(new Object[]{
                oids.oid("proc:plpgsql_call_handler"), "plpgsql_call_handler", pgCatalogNs, 10,
                cLangOid, 1.0, 0.0, 0, "-", "f", false, false, true, false, "v", "u",
                (short) 0, (short) 0, langHandlerType,
                null, null, null, null, null, null,
                "plpgsql_call_handler", null, null, null, null, 1
        });
        // PL/pgSQL inline handler: returns void, takes internal arg
        table.insertRow(new Object[]{
                oids.oid("proc:plpgsql_inline_handler"), "plpgsql_inline_handler", pgCatalogNs, 10,
                cLangOid, 1.0, 0.0, 0, "-", "f", false, false, true, false, "v", "u",
                (short) 1, (short) 0, voidType,
                "2281", null, null, null, null, null,
                "plpgsql_inline_handler", null, null, null, null, 1
        });
        // PL/pgSQL validator: returns void, takes oid arg
        table.insertRow(new Object[]{
                oids.oid("proc:plpgsql_validator"), "plpgsql_validator", pgCatalogNs, 10,
                cLangOid, 1.0, 0.0, 0, "-", "f", false, false, true, false, "v", "u",
                (short) 1, (short) 0, voidType,
                "26", null, null, null, null, null,
                "plpgsql_validator", null, null, null, null, 1
        });

        int publicNs = oids.oid("ns:public");
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            PgFunction fn = entry.getValue();
            String funcSchema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
            int funcNs = funcSchema.equals("pg_catalog") ? pgCatalogNs : oids.oid("ns:" + funcSchema);
            String lang = fn.getLanguage() != null ? fn.getLanguage().toLowerCase() : "plpgsql";
            int langOid;
            switch (lang) {
                case "sql":
                    langOid = oids.oid("lang:sql");
                    break;
                case "c":
                    langOid = oids.oid("lang:c");
                    break;
                case "internal":
                    langOid = internalLangOid;
                    break;
                default:
                    langOid = oids.oid("lang:plpgsql");
                    break;
            }
            String kind = fn.isProcedure() ? "p" : "f";
            // Count arguments
            short nargs = 0;
            String argTypes = null;
            if (fn.getParams() != null && !fn.getParams().isEmpty()) {
                nargs = (short) fn.getParams().size();
            }
            String fnOwner = fn.getOwner();
            int fnOwnerOid = (fnOwner != null && !fnOwner.isEmpty()) ? oids.oid("role:" + fnOwner) : 10;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + fn.getName()), fn.getName(), funcNs, fnOwnerOid,
                    langOid, fn.getCost(), fn.getRows(), 0, "-", kind,
                    fn.isSecurityDefiner(), fn.isLeakproof(), fn.isStrict(), false,
                    fn.getVolatility() != null ? fn.getVolatility().substring(0, 1).toLowerCase() : "v",
                    fn.getParallel() != null ? fn.getParallel().substring(0, 1).toLowerCase() : "u",
                    nargs, (short) 0, 0,
                    argTypes, null, null, null, null, null,
                    fn.getBody(), null, null, null, null, 1
            });
        }
        return table;
    }
}
