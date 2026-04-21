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
            "pg_parameter_acl",
            "pg_stat_all_indexes", "pg_stat_all_tables",
            "pg_stat_bgwriter", "pg_stat_database",
            "pg_statio_all_indexes", "pg_statio_all_sequences",
            "pg_statio_all_tables", "pg_stat_replication",
            "pg_stat_wal_receiver", "pg_stat_xact_all_tables",
            "pg_stat_xact_user_tables",
            "pg_replication_slots",
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
                        hasIdx, false, t.isUnlogged() ? "u" : "p", relkind,          // relhasindex, relisshared, relpersistence, relkind
                        (short) t.getColumns().size(), checkCount, // relnatts, relchecks
                        false, hasTriggers, false, t.isRlsEnabled(), t.isRlsForced(), // relhasrules..relforcerowsecurity
                        false,              // relhasoids
                        true, String.valueOf(t.getReplicaIdentity()), relispartition, // relispopulated, relreplident, relispartition
                        0, 0, 0,            // relrewrite, relfrozenxid, relminmxid
                        buildRelacl(t.getName()), buildTableReloptions(t), relpartbound, 1 // relacl, reloptions, relpartbound, xmin
                });
            }
        }

        // Views
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int viewOwnerOid = resolveOwnerOid(database, oids, "view:" + vSchema + "." + vd.name());
            int vOid = oids.oid("rel:" + vSchema + "." + vd.name());
            // Build reloptions array string from view options
            String viewRelOptions = null;
            if (vd.reloptions() != null && !vd.reloptions().isEmpty()) {
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (Map.Entry<String, String> opt : vd.reloptions().entrySet()) {
                    if (!first) sb.append(",");
                    sb.append(opt.getKey()).append("=").append(opt.getValue());
                    first = false;
                }
                sb.append("}");
                viewRelOptions = sb.toString();
            }
            table.insertRow(new Object[]{
                    vOid, vd.name(), oids.oid("ns:" + vSchema),
                    0, 0, viewOwnerOid, 0, vOid, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", vd.materialized() ? "m" : "v",
                    (short) 0, (short) 0,
                    true, false, false, false, false,
                    false, // relhasoids
                    true, "n", false,
                    0, 0, 0,
                    null, viewRelOptions, null, 1
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
            // Build reloptions array from index storage parameters
            Map<String, String> idxOpts = database.getIndexReloptions(indexName);
            Object reloptionsVal = null;
            if (idxOpts != null && !idxOpts.isEmpty()) {
                List<String> optList = new ArrayList<>();
                for (Map.Entry<String, String> oe : idxOpts.entrySet()) {
                    optList.add(oe.getKey() + "=" + oe.getValue());
                }
                reloptionsVal = optList;
            }
            String idxMethod = database.getIndexMethod(indexName);
            int relamOid = resolveAccessMethodOid(idxMethod);
            // Determine if this is a partitioned index (index on a partitioned table)
            String idxRelkind = "i";
            if (storedTableQualified != null) {
                String[] qParts = storedTableQualified.split("\\.", 2);
                if (qParts.length == 2) {
                    Schema idxSchema = database.getSchema(qParts[0]);
                    if (idxSchema != null) {
                        Table idxTable = idxSchema.getTable(qParts[1]);
                        if (idxTable != null && idxTable.getPartitionStrategy() != null) {
                            idxRelkind = "I";
                        }
                    }
                }
            }
            table.insertRow(new Object[]{
                    idxOid, indexName, oids.oid("ns:" + indexSchema),
                    0, 0, 10, relamOid, idxOid, 0,  // relam from access method
                    1, 0.0, 0, 0, 0,
                    false, false, "p", idxRelkind,
                    idxNatts, (short) 0,
                    false, false, false, false, false,
                    false, // relhasoids
                    true, "n", false, 0, 0, 0,
                    null, reloptionsVal, null, 1
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
                        String ciRelkind = tableEntry.getValue().getPartitionStrategy() != null ? "I" : "i";
                        table.insertRow(new Object[]{
                                ciOid, sc.getName(), oids.oid("ns:" + schemaEntry.getKey()),
                                0, 0, 10, 403, ciOid, 0,
                                1, 0.0, 0, 0, 0,
                                false, false, "p", ciRelkind,
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

        // Foreign tables (relkind='f')
        int publicNsOid = oids.oid("ns:public");
        for (Database.FdwForeignTable ft : database.getForeignTables().values()) {
            int ftOid = oids.oid("rel:public." + ft.tableName);
            short ftNatts = (short) (ft.columns != null ? ft.columns.size() : 0);
            table.insertRow(new Object[]{
                    ftOid, ft.tableName, publicNsOid,
                    0, 0, 10, 0, ftOid, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", "f",
                    ftNatts, (short) 0,
                    false, false, false, false, false,
                    false, true, "d", false,
                    0, 0, 0,
                    null, null, null, 1
            });
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
                    if (c.getDefaultValue() != null) {
                        if (c.getDefaultValue().contains("__identity__:always")) {
                            identity = "a";
                        } else if (c.getDefaultValue().contains("__identity__")) {
                            identity = "d";
                        }
                    }
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
                    // Use column-level overrides if set
                    String effectiveStorage = c.getAttStorageOverride() != null ? c.getAttStorageOverride() : storage;
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
                            c.getAttStattarget(), // attstattarget
                            effectiveStorage,   // attstorage
                            c.getAttCompression(),        // attcompression
                            c.isAttHasMissing(), // atthasmissing
                            null,      // attmissingval
                            "i"        // attalign
                    });
                }
            }
        }

        // Foreign table columns
        for (Database.FdwForeignTable ft : database.getForeignTables().values()) {
            int ftRelOid = oids.oid("rel:public." + ft.tableName);
            if (ft.columns != null) {
                for (int i = 0; i < ft.columns.size(); i++) {
                    String[] colParts = ft.columns.get(i);
                    String colName = colParts[0];
                    String colTypeName = colParts.length > 1 ? colParts[1] : "text";
                    int typOid = resolveTypeOidByName(colTypeName);
                    table.insertRow(new Object[]{
                            ftRelOid, colName, typOid, (short) (i + 1),
                            false, -1, (short) -1, false, false,
                            "", "", 0, 1, true, 0, null, 0, null,
                            null, (short) -1, "p", "", false, null, "i"
                    });
                }
            }
        }

        // Composite type attributes
        for (Map.Entry<String, java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ctEntry
                : database.getCompositeTypes().entrySet()) {
            String ctName = ctEntry.getKey();
            int ctRelOid = oids.oid("rel:public." + ctName);
            java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields = ctEntry.getValue();
            for (int i = 0; i < fields.size(); i++) {
                com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField f = fields.get(i);
                int atttypid = resolveTypeOidByName(f.typeName());
                table.insertRow(new Object[]{
                        ctRelOid, f.name(), atttypid, (short) (i + 1),
                        false, -1, (short) -1, false, false,
                        "", "", 0, 1, true, 0, null, 0, null,
                        null, (short) -1, "p", "", false, null, "i"
                });
            }
        }

        // Add pg_attribute entries for key catalog tables so queries like
        // "SELECT ... FROM pg_attribute WHERE attrelid = 'pg_aggregate'::regclass" work.
        // pg_index attributes (indkey must be int2vector = OID 22)
        addCatalogTableAttributesTyped(table, "pg_index", new String[]{
                "indexrelid", "indrelid", "indisunique", "indisprimary",
                "indimmediate", "indkey", "indnkeyatts", "indnatts",
                "indisvalid", "indisready", "indislive", "indexprs",
                "indpred", "indisclustered", "indisreplident", "indoption",
                "indnullsnotdistinct", "indclass", "indcollation"
        }, new int[]{
                26, 26, 16, 16,    // oid, oid, bool, bool
                16, 22, 21, 21,    // bool, int2vector, int2, int2
                16, 16, 16, 25,    // bool, bool, bool, text
                25, 16, 16, 22,    // text, bool, bool, int2vector
                16, 30, 30         // bool, oidvector, oidvector
        });

        // pg_proc attributes (proargmodes must be _char=OID 1002, proargnames must be _text=OID 1009)
        addCatalogTableAttributesTyped(table, "pg_proc", new String[]{
                "oid", "proname", "pronamespace", "proowner",
                "prolang", "procost", "prorows", "provariadic",
                "prosupport", "prokind", "prosecdef", "proleakproof",
                "proisstrict", "proretset", "provolatile", "proparallel",
                "pronargs", "pronargdefaults", "prorettype", "proargtypes",
                "proallargtypes", "proargmodes", "proargnames", "proargdefaults",
                "protrftypes", "prosrc", "probin", "prosqlbody",
                "proconfig", "proacl", "xmin"
        }, new int[]{
                26, 25, 26, 26,       // oid, text, oid, oid
                26, 701, 701, 26,     // oid, float8, float8, oid
                25, 18, 16, 16,       // text, char, bool, bool
                16, 16, 18, 18,       // bool, bool, char, char
                21, 21, 26, 30,       // int2, int2, oid, oidvector
                26, 1002, 1009, 25,   // oid, _char, _text, text
                26, 25, 25, 25,       // oid, text, text, text
                25, 1034, 26          // text, _aclitem, oid (xmin)
        });

        addCatalogTableAttributes(table, "pg_aggregate", new String[]{
                "aggfnoid", "aggtransfn", "aggtranstype", "aggfinalfn",
                "agginitval", "aggsortop", "aggfinalextra", "aggtransspace",
                "aggmtransfn", "aggminvtransfn", "aggmtranstype", "aggmtransspace",
                "aggmfinalfn", "aggmfinalextra", "aggminitval", "aggkind",
                "aggnumdirectargs", "aggcombinefn", "aggserialfn", "aggdeserialfn"
        });
        addCatalogTableAttributesTyped(table, "pg_authid", new String[]{
                "oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole",
                "rolcreatedb", "rolcanlogin", "rolreplication", "rolconnlimit",
                "rolvaliduntil", "rolbypassrls", "rolconfig", "rolpassword"
        }, new int[]{
                26, 25, 16, 16, 16,    // oid=oid, rolname=text, rolsuper=bool, rolinherit=bool, rolcreaterole=bool
                16, 16, 16, 23,        // rolcreatedb=bool, rolcanlogin=bool, rolreplication=bool, rolconnlimit=int4
                1184, 16, 25, 25       // rolvaliduntil=timestamptz, rolbypassrls=bool, rolconfig=text, rolpassword=text
        });

        return table;
    }

    /** Helper: add pg_attribute rows for a system catalog table. */
    private void addCatalogTableAttributes(Table attrTable, String catalogName, String[] colNames) {
        int relOid = oids.oid("rel:pg_catalog." + catalogName);
        for (int i = 0; i < colNames.length; i++) {
            attrTable.insertRow(new Object[]{
                    relOid, colNames[i], 0, (short) (i + 1),
                    false, -1, (short) -1, false, false,
                    "", "", 0, 1, true, 0, null, 0, null,
                    null, (short) -1, "p", "", false, null, "i"
            });
        }
    }

    /** Helper: add pg_attribute rows with specific type OIDs for a system catalog table. */
    private void addCatalogTableAttributesTyped(Table attrTable, String catalogName, String[] colNames, int[] typeOids) {
        int relOid = oids.oid("rel:pg_catalog." + catalogName);
        for (int i = 0; i < colNames.length; i++) {
            attrTable.insertRow(new Object[]{
                    relOid, colNames[i], typeOids[i], (short) (i + 1),
                    false, -1, (short) -1, false, false,
                    "", "", 0, 1, true, 0, null, 0, null,
                    null, (short) -1, "p", "", false, null, "i"
            });
        }
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

        // Pseudo-types: trigger, event_trigger, void, record, etc.
        String[][] pseudoTypes = {
                {"trigger", "2279"},
                {"event_trigger", "3838"},
                {"void", "2278"},
                {"record", "2249"},
                {"any", "2276"},
                {"anyelement", "2283"},
                {"anyarray", "2277"},
                {"internal", "2281"},
                {"cstring", "2275"},
        };
        for (String[] pt : pseudoTypes) {
            String ptName = pt[0];
            int ptOid = Integer.parseInt(pt[1]);
            table.insertRow(new Object[]{
                    ptOid, ptName, pgCatalogOid, 10,
                    (short) 4, true, "p", "P", false, true, ",",
                    0, null, 0, 0,
                    ptName + "_in", ptName + "_out", "-", "-",
                    "-", "-", "-", "i", "p",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

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

        // Add composite types
        for (Map.Entry<String, java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ctEntry
                : database.getCompositeTypes().entrySet()) {
            String ctName = ctEntry.getKey();
            int ctNsOid = oids.oid("ns:public");
            int ctRelOid = oids.oid("rel:public." + ctName);
            table.insertRow(new Object[]{
                    oids.oid("type:" + ctName), ctName, ctNsOid, 10,
                    (short) -1, false, "c", "C", false, true, ",",
                    ctRelOid, null, 0, 0,
                    "record_in", "record_out", "record_recv", "record_send",
                    "-", "-", "-", "d", "x",
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

        // Add user-defined range types
        for (Map.Entry<String, String> rangeEntry : database.getRangeTypes().entrySet()) {
            String rangeName = rangeEntry.getKey();
            int rangeNsOid = oids.oid("ns:public");
            table.insertRow(new Object[]{
                    oids.oid("type:" + rangeName), rangeName, rangeNsOid, 10,
                    (short) -1, false, "r", "R", false, true, ",",
                    0, null, 0, 0,
                    "range_in", "range_out", "range_recv", "range_send",
                    "-", "-", "-", "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
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
            int ownerOid = CatalogHelper.resolveOwnerOid(database, oids, "schema:" + schemaName);
            java.util.List<String> acl = database.getSchemaAcl(schemaName);
            String aclText = acl != null && !acl.isEmpty() ? "{" + String.join(",", acl) + "}" : null;
            table.insertRow(new Object[]{oids.oid("ns:" + schemaName), schemaName, ownerOid, 1, aclText});
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

        // Built-in aggregate functions (prokind='a')
        String[] builtinAggs = {"count", "sum", "avg", "min", "max", "array_agg",
                "string_agg", "bool_and", "bool_or", "every", "json_agg", "jsonb_agg",
                "json_object_agg", "jsonb_object_agg", "xmlagg", "bit_and", "bit_or"};
        int anyType = 2276; // anyelement pseudo-type OID
        for (String aggName : builtinAggs) {
            int aggProcOid = oids.oid("proc:" + aggName);
            int retType = aggName.equals("count") ? 20 /* int8 */ : anyType;
            table.insertRow(new Object[]{
                    aggProcOid, aggName, pgCatalogNs, 10, internalLangOid, 1.0, 0.0,
                    0, "-", "a", false, false, false, false, "i", "u",
                    (short) 1, (short) 0, retType,
                    String.valueOf(anyType), null, null, null, null, null,
                    aggName, null, null, null, null, 1
            });
        }

        int publicNs = oids.oid("ns:public");
        // Iterate all function overloads (not just last-added per name)
        List<PgFunction> allFuncs = new java.util.ArrayList<>();
        Map<String, Integer> overloadIndex = new java.util.HashMap<>();
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            List<PgFunction> overloads = database.getFunctionOverloads(entry.getKey());
            if (overloads != null && !overloads.isEmpty()) {
                for (PgFunction f : overloads) {
                    if (!allFuncs.contains(f)) allFuncs.add(f);
                }
            } else {
                if (!allFuncs.contains(entry.getValue())) allFuncs.add(entry.getValue());
            }
        }
        for (PgFunction fn : allFuncs) {
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
            // Count arguments and build proargnames, proargmodes, proallargtypes
            short nargs = 0;
            String argTypes = null;
            String proargnames = null;
            String proargmodes = null;
            String proallargtypes = null;
            if (fn.getParams() != null && !fn.getParams().isEmpty()) {
                nargs = (short) fn.getParams().size();
                // Build proargnames: {name1,name2,...} — populated when any param has a name
                boolean hasNames = fn.getParams().stream().anyMatch(p -> p.name() != null && !p.name().isEmpty());
                if (hasNames) {
                    StringBuilder namesBuilder = new StringBuilder("{");
                    for (int pi = 0; pi < fn.getParams().size(); pi++) {
                        if (pi > 0) namesBuilder.append(",");
                        PgFunction.Param p = fn.getParams().get(pi);
                        namesBuilder.append(p.name() != null ? p.name() : "");
                    }
                    namesBuilder.append("}");
                    proargnames = namesBuilder.toString();
                }
                // Build proargmodes: {i,o,...} — populated when any param is not IN
                boolean hasNonIn = fn.getParams().stream().anyMatch(p -> p.mode() != null
                        && !p.mode().equalsIgnoreCase("IN") && !p.mode().isEmpty());
                if (hasNonIn) {
                    StringBuilder modesBuilder = new StringBuilder("{");
                    StringBuilder allTypesBuilder = new StringBuilder("{");
                    for (int pi = 0; pi < fn.getParams().size(); pi++) {
                        if (pi > 0) { modesBuilder.append(","); allTypesBuilder.append(","); }
                        PgFunction.Param p = fn.getParams().get(pi);
                        String mode = p.mode() != null ? p.mode().toLowerCase() : "i";
                        switch (mode) {
                            case "in": mode = "i"; break;
                            case "out": mode = "o"; break;
                            case "inout": mode = "b"; break;
                            case "variadic": mode = "v"; break;
                            default: break; // already single-char
                        }
                        modesBuilder.append(mode);
                        allTypesBuilder.append(resolveTypeOidByName(p.typeName()));
                    }
                    modesBuilder.append("}");
                    allTypesBuilder.append("}");
                    proargmodes = modesBuilder.toString();
                    proallargtypes = allTypesBuilder.toString();
                }
            }
            String fnOwner = fn.getOwner();
            int fnOwnerOid = (fnOwner != null && !fnOwner.isEmpty()) ? oids.oid("role:" + fnOwner) : 10;
            // Build proconfig from function-level SET clauses (e.g., "work_mem=256MB")
            String proconfig = null;
            if (fn.getSetClauses() != null && !fn.getSetClauses().isEmpty()) {
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (Map.Entry<String, String> sc : fn.getSetClauses().entrySet()) {
                    if (!first) sb.append(",");
                    sb.append(sc.getKey()).append("=").append(sc.getValue());
                    first = false;
                }
                sb.append("}");
                proconfig = sb.toString();
            }
            // Build proargdefaults: populate when any param has a default expression
            String proargdefaults = null;
            if (fn.getParams() != null) {
                StringBuilder defs = new StringBuilder();
                for (PgFunction.Param p : fn.getParams()) {
                    if (p.defaultExpr() != null && !p.defaultExpr().isEmpty()) {
                        if (defs.length() > 0) defs.append(" ");
                        defs.append("({CONST :constvalue ").append(p.defaultExpr()).append("})");
                    }
                }
                if (defs.length() > 0) proargdefaults = defs.toString();
            }
            // prosqlbody: populated for BEGIN ATOMIC functions
            String prosqlbody = fn.isAtomicBody() ? fn.getBody() : null;
            // Use unique OID key for overloaded functions (append param count)
            int idx = overloadIndex.merge(fn.getName(), 0, (a, b) -> a + 1);
            String oidKey = idx == 0 ? "proc:" + fn.getName() : "proc:" + fn.getName() + "#" + idx;
            table.insertRow(new Object[]{
                    oids.oid(oidKey), fn.getName(), funcNs, fnOwnerOid,
                    langOid, fn.getCost(), fn.getRows(), 0, "-", kind,
                    fn.isSecurityDefiner(), fn.isLeakproof(), fn.isStrict(), false,
                    fn.getVolatility() != null ? fn.getVolatility().substring(0, 1).toLowerCase() : "v",
                    fn.getParallel() != null ? fn.getParallel().substring(0, 1).toLowerCase() : "u",
                    nargs, (short) 0, 0,
                    argTypes, proallargtypes, proargmodes, proargnames, proargdefaults, null,
                    fn.getBody(), null, prosqlbody, proconfig, null, 1
            });
        }

        // Event trigger helper functions (built-in)
        String[] eventTriggerHelpers = {
                "pg_event_trigger_ddl_commands",
                "pg_event_trigger_dropped_objects",
                "pg_event_trigger_table_rewrite_oid",
                "pg_event_trigger_table_rewrite_reason"
        };
        for (String etHelper : eventTriggerHelpers) {
            table.insertRow(new Object[]{
                    oids.oid("proc:" + etHelper), etHelper, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, false, false, "v", "u",
                    (short) 0, (short) 0, voidType,
                    null, null, null, null, null, null,
                    etHelper, null, null, null, null, 1
            });
        }

        // Built-in extension functions (uuid-ossp, pgcrypto, pg_trgm, fuzzystrmatch, unaccent, json)
        String[] extensionFunctions = {
                "uuid_generate_v1", "uuid_generate_v3", "uuid_generate_v4", "uuid_generate_v5",
                "uuid_nil", "uuid_ns_dns", "uuid_ns_url",
                "digest", "hmac", "gen_salt", "gen_random_uuid",
                "show_trgm", "similarity",
                "levenshtein", "soundex",
                "unaccent",
                "json_strip_nulls", "jsonb_strip_nulls",
                "jsonb_object", "json_populate_record", "json_populate_recordset",
                "jsonb_populate_record", "jsonb_populate_recordset",
                "jsonb_path_match", "jsonb_path_match_tz",
                "row_to_json", "to_json", "to_jsonb",
                "uuidv4",
                "unicode_version", "unicode_assigned"
        };
        for (String extFn : extensionFunctions) {
            table.insertRow(new Object[]{
                    oids.oid("proc:" + extFn), extFn, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, false, false, "i", "u",
                    (short) 0, (short) 0, 0,
                    null, null, null, null, null, null,
                    extFn, null, null, null, null, 1
            });
        }

        // Replication / WAL functions (stubs for pg_proc visibility)
        String[] replicationFunctions = {
                "pg_create_logical_replication_slot",
                "pg_create_physical_replication_slot",
                "pg_drop_replication_slot",
                "pg_logical_slot_get_changes",
                "pg_logical_slot_peek_changes",
                "pg_replication_slot_advance",
                "pg_switch_wal",
                "pg_walfile_name",
                "pg_last_wal_receive_lsn",
                "pg_last_wal_replay_lsn",
                "pg_backup_start",
                "pg_backup_stop",
                "pg_promote",
                "pg_create_restore_point",
                "pg_wal_replay_pause",
                "pg_wal_replay_resume"
        };
        for (String replFn : replicationFunctions) {
            table.insertRow(new Object[]{
                    oids.oid("proc:" + replFn), replFn, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, false, false, "v", "u",
                    (short) 0, (short) 0, 0,
                    null, null, null, null, null, null,
                    replFn, null, null, null, null, 1
            });
        }

        // Large object functions
        // has_largeobject_privilege(user oid, loid oid, privilege text) → boolean
        table.insertRow(new Object[]{
                oids.oid("proc:has_largeobject_privilege"), "has_largeobject_privilege", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "s", "u",
                (short) 3, (short) 0, 16, // returns boolean (OID 16)
                "26 26 25", null, null, null, null, null,
                "has_largeobject_privilege", null, null, null, null, 1
        });
        // has_largeobject_privilege(user name, loid oid, privilege text) → boolean
        table.insertRow(new Object[]{
                oids.oid("proc:has_largeobject_privilege_name"), "has_largeobject_privilege", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "s", "u",
                (short) 3, (short) 0, 16, // returns boolean (OID 16)
                "19 26 25", null, null, null, null, null,
                "has_largeobject_privilege", null, null, null, null, 1
        });
        // has_largeobject_privilege(loid oid, privilege text) → boolean (current user)
        table.insertRow(new Object[]{
                oids.oid("proc:has_largeobject_privilege_2"), "has_largeobject_privilege", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "s", "u",
                (short) 2, (short) 0, 16, // returns boolean (OID 16)
                "26 25", null, null, null, null, null,
                "has_largeobject_privilege", null, null, null, null, 1
        });
        // lo_export(loid oid, filename text) → int4
        table.insertRow(new Object[]{
                oids.oid("proc:lo_export"), "lo_export", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 2, (short) 0, 23, // returns int4 (OID 23)
                "26 25", null, null, null, null, null,
                "lo_export", null, null, null, null, 1
        });
        // lo_import(filename text) → oid
        table.insertRow(new Object[]{
                oids.oid("proc:lo_import"), "lo_import", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 1, (short) 0, 26, // returns oid (OID 26)
                "25", null, null, null, null, null,
                "lo_import", null, null, null, null, 1
        });
        // lo_import(filename text, loid oid) → oid (2-arg overload)
        table.insertRow(new Object[]{
                oids.oid("proc:lo_import_2"), "lo_import", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 2, (short) 0, 26, // returns oid (OID 26)
                "25 26", null, null, null, null, null,
                "lo_import", null, null, null, null, 1
        });
        // lo_truncate64(fd int4, len int8) → int4
        table.insertRow(new Object[]{
                oids.oid("proc:lo_truncate64"), "lo_truncate64", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 2, (short) 0, 23, // returns int4 (OID 23)
                "23 20", null, null, null, null, null,
                "lo_truncate64", null, null, null, null, 1
        });

        // UUID functions
        // uuid_extract_timestamp(uuid) → timestamptz
        table.insertRow(new Object[]{
                oids.oid("proc:uuid_extract_timestamp"), "uuid_extract_timestamp", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, true, false, "i", "s",
                (short) 1, (short) 0, 1184, // returns timestamptz (OID 1184)
                "2950", null, null, null, null, null,
                "uuid_extract_timestamp", null, null, null, null, 1
        });
        // uuid_extract_version(uuid) → int4
        table.insertRow(new Object[]{
                oids.oid("proc:uuid_extract_version"), "uuid_extract_version", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, true, false, "i", "s",
                (short) 1, (short) 0, 23, // returns int4 (OID 23)
                "2950", null, null, null, null, null,
                "uuid_extract_version", null, null, null, null, 1
        });

        // pg_control_* functions (return record)
        String[] pgControlFunctions = {
                "pg_control_checkpoint", "pg_control_init",
                "pg_control_recovery", "pg_control_system"
        };
        for (String ctlFn : pgControlFunctions) {
            table.insertRow(new Object[]{
                    oids.oid("proc:" + ctlFn), ctlFn, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, true, true, "s", "u",
                    (short) 0, (short) 0, 2249, // returns record (OID 2249)
                    null, null, null, null, null, null,
                    ctlFn, null, null, null, null, 1
            });
        }

        // User-defined aggregates: emit with prokind='a'
        for (Map.Entry<String, PgAggregate> aggEntry : database.getUserAggregates().entrySet()) {
            PgAggregate agg = aggEntry.getValue();
            // Determine namespace from the aggregate's schema
            String aggSchema = agg.getSchemaName() != null ? agg.getSchemaName() : "public";
            int aggNs = aggSchema.equals("pg_catalog") ? pgCatalogNs : oids.oid("ns:" + aggSchema);
            // Determine arg count from aggregate's argTypes
            short aggNargs = agg.getArgTypes() != null ? (short) agg.getArgTypes().length : 0;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + agg.getName()), agg.getName(), aggNs, 10,
                    oids.oid("lang:internal"), 1.0, 0.0, 0, "-", "a",
                    false, false, false, false, "i", "u",
                    aggNargs, (short) 0, 0,
                    null, null, null, null, null, null,
                    null, null, null, null, null, 1
            });
        }

        return table;
    }

    /**
     * Build an aclitem[] string for a table based on granted privileges.
     * Returns null if no privileges have been granted, or a PG-style aclitem array string.
     */
    private String buildRelacl(String tableName) {
        Map<String, Set<String>> allPrivs = database.getAllRolePrivileges();
        // Collect grants: grantee -> set of privilege abbreviations
        Map<String, Set<String>> aclEntries = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : allPrivs.entrySet()) {
            String role = entry.getKey();
            for (String priv : entry.getValue()) {
                // Format: "PRIVILEGE:OBJECTTYPE:OBJECTNAME"
                String[] parts = priv.split(":", 3);
                if (parts.length == 3 && parts[1].equalsIgnoreCase("TABLE")
                        && parts[2].equalsIgnoreCase(tableName)) {
                    String abbrev = privAbbrev(parts[0]);
                    if (abbrev != null) {
                        aclEntries.computeIfAbsent(role, k -> new java.util.LinkedHashSet<>()).add(abbrev);
                    }
                }
            }
        }
        if (aclEntries.isEmpty()) return null;
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Set<String>> entry : aclEntries.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            String grantee = entry.getKey().equalsIgnoreCase("public") ? "" : entry.getKey();
            sb.append(grantee).append("=");
            for (String a : entry.getValue()) sb.append(a);
            sb.append("/").append("memgres"); // grantor
        }
        sb.append("}");
        return sb.toString();
    }

    /** Build a PG text-array string for table storage parameters (reloptions). */
    private String buildTableReloptions(Table t) {
        Map<String, String> opts = t.getReloptions();
        if (opts == null || opts.isEmpty()) return null;
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : opts.entrySet()) {
            if (!first) sb.append(",");
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /** Map a privilege name to its PG aclitem abbreviation. */
    private static String privAbbrev(String priv) {
        switch (priv.toUpperCase()) {
            case "SELECT": return "r";
            case "INSERT": return "a";
            case "UPDATE": return "w";
            case "DELETE": return "d";
            case "TRUNCATE": return "D";
            case "REFERENCES": return "x";
            case "TRIGGER": return "t";
            case "ALL": return "arwdDxt";
            case "USAGE": return "U";
            default: return null;
        }
    }

    private int resolveAccessMethodOid(String method) {
        if (method == null) return 403; // default btree
        switch (method.toLowerCase()) {
            case "btree": return 403;
            case "hash": return 405;
            case "gist": return 783;
            case "gin": return 2742;
            case "spgist": return 4000;
            case "brin": return 3580;
            default: return oids.oid("am:" + method.toLowerCase());
        }
    }

    /** Resolve a type name (e.g., "int", "text", "integer") to its PG OID. */
    private int resolveTypeOidByName(String typeName) {
        if (typeName == null) return 0;
        String lower = typeName.toLowerCase().trim();
        // Handle common aliases
        switch (lower) {
            case "int": case "integer": case "int4": return 23;
            case "bigint": case "int8": return 20;
            case "smallint": case "int2": return 21;
            case "text": return 25;
            case "varchar": case "character varying": return 1043;
            case "char": case "character": case "bpchar": return 1042;
            case "boolean": case "bool": return 16;
            case "float4": case "real": return 700;
            case "float8": case "double precision": return 701;
            case "numeric": case "decimal": return 1700;
            case "date": return 1082;
            case "timestamp": case "timestamp without time zone": return 1114;
            case "timestamptz": case "timestamp with time zone": return 1184;
            case "time": case "time without time zone": return 1083;
            case "interval": return 1186;
            case "uuid": return 2950;
            case "json": return 114;
            case "jsonb": return 3802;
            case "bytea": return 17;
            case "void": return 2278;
            case "record": return 2249;
            case "trigger": return 2279;
            default: break;
        }
        // Try matching against DataType enum
        for (DataType dt : DataType.values()) {
            if (dt.getPgName().equalsIgnoreCase(lower) || dt.name().equalsIgnoreCase(lower)) {
                return dt.getOid();
            }
        }
        return 0;
    }
}
