package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds pg_catalog view, stats-stub, timezone, text-search, and
 * remaining empty-stub virtual tables.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogStubBuilder {

    /** PG-compatible timestamptz format: "2024-01-15 10:30:00.123456+00" (no 'T', short offset). */
    private static final DateTimeFormatter PG_TIMESTAMPTZ_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxx");

    static String formatTimestamptz(OffsetDateTime ts) {
        if (ts == null) return null;
        // Format with 6 fractional digits, then trim trailing offset colon for PG compat
        // Java produces "+00:00", PG uses "+00"
        String formatted = ts.format(PG_TIMESTAMPTZ_FMT);
        // Trim ":00" from offset if offset is whole hours (e.g., "+00:00" -> "+00")
        if (formatted.endsWith(":00") && formatted.length() > 3) {
            int lastColon = formatted.lastIndexOf(':');
            // Ensure we're trimming from the timezone offset, not from the time part
            String beforeColon = formatted.substring(0, lastColon);
            if (beforeColon.matches(".*[+\\-]\\d{2}$")) {
                formatted = beforeColon;
            }
        }
        return formatted;
    }

    final Database database;
    final OidSupplier oids;

    CatalogStubBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    // ---------------------------------------------------------------
    //  User-facing views
    // ---------------------------------------------------------------

    Table buildPgTables() {
        List<Column> cols = Cols.listOf(
                colNN("schemaname", DataType.TEXT),
                colNN("tablename", DataType.TEXT),
                colNN("tableowner", DataType.TEXT),
                col("tablespace", DataType.TEXT),
                col("hasindexes", DataType.BOOLEAN),
                col("hasrules", DataType.BOOLEAN),
                col("hastriggers", DataType.BOOLEAN)
        );
        Table table = new Table("pg_tables", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (String tableName : schemaEntry.getValue().getTables().keySet()) {
                table.insertRow(new Object[]{
                        schemaEntry.getKey(), tableName, "memgres", null, false, false, false
                });
            }
        }
        return table;
    }

    Table buildPgViews() {
        List<Column> cols = Cols.listOf(
                colNN("schemaname", DataType.TEXT),
                colNN("viewname", DataType.TEXT),
                colNN("viewowner", DataType.TEXT),
                col("definition", DataType.TEXT)
        );
        Table table = new Table("pg_views", cols);
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            String viewDef = null;
            if (vd.query() != null) {
                viewDef = vd.sourceSQL() != null ? vd.sourceSQL() : SqlUnparser.toSql(vd.query());
            }
            table.insertRow(new Object[]{vSchema, vd.name(), "memgres", viewDef});
        }
        return table;
    }

    Table buildPgIndexes() {
        List<Column> cols = Cols.listOf(
                colNN("schemaname", DataType.TEXT),
                colNN("tablename", DataType.TEXT),
                colNN("indexname", DataType.TEXT),
                col("tablespace", DataType.TEXT),
                col("indexdef", DataType.TEXT)
        );
        Table table = new Table("pg_indexes", cols);
        // Track which index names we've already added to avoid duplicates
        Set<String> addedIndexes = new HashSet<>();

        // 1. Explicit indexes (CREATE INDEX)
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexName = idx.getKey();
            List<String> indexCols = idx.getValue();
            String storedTableQualified = database.getIndexTable(indexName);
            String schemaName = "public";
            String tableName = storedTableQualified;
            if (storedTableQualified != null && storedTableQualified.contains(".")) {
                String[] parts = storedTableQualified.split("\\.", 2);
                schemaName = parts[0];
                tableName = parts[1];
            }
            boolean isUnique = database.isUniqueIndex(indexName);
            String method = database.getIndexMethod(indexName);
            String indexDef = buildIndexDef(indexName, tableName, isUnique, method,
                    indexCols, database.getIndexColumnOptions(indexName),
                    database.getIndexIncludeColumns(indexName),
                    database.isIndexNullsNotDistinct(indexName),
                    database.getIndexWhereClause(indexName));
            table.insertRow(new Object[]{schemaName, tableName, indexName, null, indexDef});
            addedIndexes.add(indexName.toLowerCase());
        }

        // 2. Implicit indexes from PRIMARY KEY and UNIQUE constraints
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                            || sc.getType() == StoredConstraint.Type.UNIQUE) {
                        String indexName = sc.getName();
                        if (addedIndexes.contains(indexName.toLowerCase())) continue;
                        String indexDef = "CREATE UNIQUE INDEX " + indexName
                                + " ON " + t.getName()
                                + " USING btree (" + String.join(", ", sc.getColumns()) + ")";
                        table.insertRow(new Object[]{schemaName, t.getName(), indexName, null, indexDef});
                        addedIndexes.add(indexName.toLowerCase());
                    }
                }
            }
        }
        return table;
    }

    /**
     * Build the CREATE INDEX definition string including all PG options:
     * opclass, DESC, NULLS FIRST/LAST, INCLUDE, NULLS NOT DISTINCT, WHERE.
     */
    static String buildIndexDef(String indexName, String tableName, boolean isUnique,
                                String method, List<String> indexCols, List<String> columnOptions,
                                List<String> includeColumns, boolean nullsNotDistinct, String whereClause) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ").append(isUnique ? "UNIQUE " : "").append("INDEX ").append(indexName)
          .append(" ON ").append(tableName != null ? tableName : "unknown")
          .append(" USING ").append(method != null ? method : "btree").append(" (");
        for (int i = 0; i < indexCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(indexCols.get(i));
            if (columnOptions != null && i < columnOptions.size()) {
                String opts = columnOptions.get(i);
                if (opts != null && !opts.isEmpty()) {
                    // Parse stored options: "opclass:xxx DESC NULLS FIRST"
                    for (String part : opts.split(" ")) {
                        if (part.startsWith("opclass:")) {
                            sb.append(' ').append(part.substring(8));
                        } else if ("DESC".equals(part)) {
                            sb.append(" DESC");
                        } else if ("NULLS".equals(part)) {
                            // Will be followed by FIRST or LAST
                        } else if ("FIRST".equals(part)) {
                            sb.append(" NULLS FIRST");
                        } else if ("LAST".equals(part)) {
                            sb.append(" NULLS LAST");
                        }
                    }
                }
            }
        }
        sb.append(')');
        if (includeColumns != null && !includeColumns.isEmpty()) {
            sb.append(" INCLUDE (").append(String.join(", ", includeColumns)).append(')');
        }
        if (nullsNotDistinct) {
            sb.append(" NULLS NOT DISTINCT");
        }
        if (whereClause != null && !whereClause.isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    Table buildPgSequence() {
        // pg_sequence: the catalog table (not the pg_sequences view)
        List<Column> cols = Cols.listOf(
                colNN("seqrelid", DataType.INTEGER),
                colNN("seqtypid", DataType.INTEGER),
                colNN("seqstart", DataType.BIGINT),
                colNN("seqincrement", DataType.BIGINT),
                colNN("seqmax", DataType.BIGINT),
                colNN("seqmin", DataType.BIGINT),
                colNN("seqcache", DataType.BIGINT),
                colNN("seqcycle", DataType.BOOLEAN)
        );
        Table table = new Table("pg_sequence", cols);
        for (String seqName : getSequenceNames(database)) {
            Sequence seq = database.getSequence(seqName);
            String seqSchemaName = resolveSequenceSchema(database, seqName);
            int seqOid = oids.oid("rel:" + seqSchemaName + "." + seqName);
            // Determine sequence type from the source column type
            DataType seqDataType = getSequenceDataType(database, seqName);
            int typOid = seqDataType.getOid();
            long startWith = seq != null ? seq.getStartWith() : 1L;
            long incrementBy = seq != null ? seq.getIncrementBy() : 1L;
            long maxValue = seq != null ? seq.getMaxValue() : getDefaultSeqMax(seqDataType);
            long minValue = seq != null ? seq.getMinValue() : 1L;
            boolean cycle = seq != null && seq.isCycle();
            table.insertRow(new Object[]{
                    seqOid, typOid,
                    startWith, incrementBy,
                    maxValue, minValue,
                    1L, cycle
            });
        }
        return table;
    }

    Table buildPgSequences() {
        List<Column> cols = Cols.listOf(
                colNN("schemaname", DataType.TEXT),
                colNN("sequencename", DataType.TEXT),
                colNN("sequenceowner", DataType.TEXT),
                col("data_type", DataType.TEXT),
                col("start_value", DataType.BIGINT),
                col("min_value", DataType.BIGINT),
                col("max_value", DataType.BIGINT),
                col("increment_by", DataType.BIGINT),
                col("cache_size", DataType.BIGINT),
                col("cycle", DataType.BOOLEAN),
                col("last_value", DataType.BIGINT)
        );
        Table table = new Table("pg_sequences", cols);
        for (String seqName : getSequenceNames(database)) {
            Sequence seq = database.getSequence(seqName);
            DataType seqDataType = getSequenceDataType(database, seqName);
            String typeName;
            switch (seqDataType) {
                case SMALLINT:
                case SMALLSERIAL:
                    typeName = "smallint";
                    break;
                case INTEGER:
                case SERIAL:
                    typeName = "integer";
                    break;
                default:
                    typeName = "bigint";
                    break;
            }
            long startWith = seq != null ? seq.getStartWith() : 1L;
            long minValue = seq != null ? seq.getMinValue() : 1L;
            long maxValue = seq != null ? seq.getMaxValue() : getDefaultSeqMax(seqDataType);
            long incrementBy = seq != null ? seq.getIncrementBy() : 1L;
            boolean cycle = seq != null && seq.isCycle();
            long cacheSize = seq != null ? (long) seq.getCache() : 1L;
            Long lastValue = (seq != null && seq.isCalled()) ? seq.currValRaw() : null;
            String seqSchema = resolveSequenceSchema(database, seqName);
            table.insertRow(new Object[]{
                    seqSchema, seqName, "memgres", typeName,
                    startWith, minValue, maxValue,
                    incrementBy, cacheSize, cycle, lastValue
            });
        }
        return table;
    }

    /** Resolve the schema that owns a given sequence by checking the schemaObjectRegistry. */
    private static String resolveSequenceSchema(Database database, String seqName) {
        for (java.util.Map.Entry<String, Schema> entry : database.getSchemas().entrySet()) {
            java.util.Set<String> objects = database.getSchemaObjects(entry.getKey());
            if (objects.contains("sequence:" + seqName.toLowerCase())) {
                return entry.getKey();
            }
        }
        return "public";
    }

    // ---------------------------------------------------------------
    //  Stats stubs (and some with data)
    // ---------------------------------------------------------------

    Table buildPgStatUserTables() {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.INTEGER),
                col("schemaname", DataType.TEXT),
                col("relname", DataType.TEXT),
                col("seq_scan", DataType.BIGINT),
                col("last_seq_scan", DataType.TIMESTAMPTZ),
                col("seq_tup_read", DataType.BIGINT),
                col("idx_scan", DataType.BIGINT),
                col("last_idx_scan", DataType.TIMESTAMPTZ),
                col("idx_tup_fetch", DataType.BIGINT),
                col("n_tup_ins", DataType.BIGINT),
                col("n_tup_upd", DataType.BIGINT),
                col("n_tup_del", DataType.BIGINT),
                col("n_tup_hot_upd", DataType.BIGINT),
                col("n_live_tup", DataType.BIGINT),
                col("n_dead_tup", DataType.BIGINT),
                col("n_mod_since_analyze", DataType.BIGINT),
                col("n_ins_since_vacuum", DataType.BIGINT),
                col("last_vacuum", DataType.TIMESTAMPTZ),
                col("last_autovacuum", DataType.TIMESTAMPTZ),
                col("last_analyze", DataType.TIMESTAMPTZ),
                col("last_autoanalyze", DataType.TIMESTAMPTZ),
                col("vacuum_count", DataType.BIGINT),
                col("autovacuum_count", DataType.BIGINT),
                col("analyze_count", DataType.BIGINT),
                col("autoanalyze_count", DataType.BIGINT)
        );
        Table table = new Table("pg_stat_user_tables", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                String lastVac = formatTimestamptz(t.getLastVacuum());
                String lastAna = formatTimestamptz(t.getLastAnalyze());
                table.insertRow(new Object[]{
                        oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName()),
                        schemaEntry.getKey(), t.getName(),
                        0L, null, 0L, 0L, null, 0L,   // scans
                        0L, t.getTupUpdated(), t.getTupDeleted(), 0L, // ins (always 0, stats not tracked), upd, del, hot_upd
                        (long) t.getRows().size(), 0L, // live, dead
                        0L, 0L,                        // mod_since_analyze, ins_since_vacuum
                        lastVac, null, lastAna, null,   // last vacuum/analyze
                        0L, 0L, 0L, 0L                 // counts
                });
            }
        }
        return table;
    }

    Table buildPgStatUserIndexes() {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.INTEGER),
                col("indexrelid", DataType.INTEGER),
                col("schemaname", DataType.TEXT),
                col("relname", DataType.TEXT),
                col("indexrelname", DataType.TEXT),
                col("idx_scan", DataType.BIGINT),
                col("last_idx_scan", DataType.TIMESTAMPTZ),
                col("idx_tup_read", DataType.BIGINT),
                col("idx_tup_fetch", DataType.BIGINT)
        );
        Table table = new Table("pg_stat_user_indexes", cols);
        // Track which indexes we've already added (to avoid duplicates)
        Set<String> addedIndexes = new java.util.HashSet<>();
        // Populate with explicit indexes (zero stats)
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexName = idx.getKey();
            String storedTable = database.getIndexTable(indexName);
            String indexSchema = "public";
            String tableName = null;
            if (storedTable != null) {
                String[] parts = storedTable.split("\\.", 2);
                if (parts.length == 2) { indexSchema = parts[0]; tableName = parts[1]; }
                else tableName = parts[0];
            }
            if (tableName != null) {
                addedIndexes.add(indexName.toLowerCase());
                long idxScan = 0L;
                Schema sch = database.getSchemas().get(indexSchema);
                if (sch != null) {
                    Table tbl = sch.getTable(tableName);
                    if (tbl != null) idxScan = tbl.getIdxScanCount();
                }
                table.insertRow(new Object[]{
                        oids.oid("rel:" + indexSchema + "." + tableName),
                        oids.oid("rel:" + indexSchema + "." + indexName),
                        indexSchema, tableName, indexName,
                        idxScan, null, 0L, 0L,
                        0L, 0L
                });
            }
        }
        // Also populate from constraint-based indexes (PK, UNIQUE) on user tables
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                for (Map.Entry<String, TableIndex> idxEntry : t.getIndexes().entrySet()) {
                    String constraintName = idxEntry.getKey();
                    if (!addedIndexes.contains(constraintName.toLowerCase())) {
                        addedIndexes.add(constraintName.toLowerCase());
                        table.insertRow(new Object[]{
                                oids.oid("rel:" + schemaName + "." + t.getName()),
                                oids.oid("rel:" + schemaName + "." + constraintName),
                                schemaName, t.getName(), constraintName,
                                t.getIdxScanCount(), null, 0L, 0L,
                                0L, 0L
                        });
                    }
                }
            }
        }
        return table;
    }

    Table buildPgStatDatabase() {
        List<Column> cols = Cols.listOf(
                col("datid", DataType.INTEGER),
                col("datname", DataType.TEXT),
                col("numbackends", DataType.INTEGER),
                col("xact_commit", DataType.BIGINT),
                col("xact_rollback", DataType.BIGINT),
                col("blks_read", DataType.BIGINT),
                col("blks_hit", DataType.BIGINT),
                col("tup_returned", DataType.BIGINT),
                col("tup_fetched", DataType.BIGINT),
                col("tup_inserted", DataType.BIGINT),
                col("tup_updated", DataType.BIGINT),
                col("tup_deleted", DataType.BIGINT),
                col("conflicts", DataType.BIGINT),
                col("temp_files", DataType.BIGINT),
                col("temp_bytes", DataType.BIGINT),
                col("deadlocks", DataType.BIGINT),
                col("checksum_failures", DataType.BIGINT),
                col("checksum_last_failure", DataType.TIMESTAMPTZ),
                col("blk_read_time", DataType.DOUBLE_PRECISION),
                col("blk_write_time", DataType.DOUBLE_PRECISION),
                col("session_time", DataType.DOUBLE_PRECISION),
                col("active_time", DataType.DOUBLE_PRECISION),
                col("idle_in_transaction_time", DataType.DOUBLE_PRECISION),
                col("sessions", DataType.BIGINT),
                col("sessions_abandoned", DataType.BIGINT),
                col("sessions_fatal", DataType.BIGINT),
                col("sessions_killed", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ),
                col("parallel_workers_launched", DataType.BIGINT)
        );
        Table table = new Table("pg_stat_database", cols);
        int numBackends = database.getActiveSessions().size();
        table.insertRow(new Object[]{
                oids.oid("db:memgres"), "memgres", numBackends,
                database.getXactCommitCount(), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, null,    // checksum_failures, checksum_last_failure
                0.0, 0.0,   // blk_read_time, blk_write_time
                0.0, 0.0, 0.0, // session_time, active_time, idle_in_transaction_time
                (long) numBackends, 0L, 0L, 0L, // sessions, abandoned, fatal, killed
                null,
                0L           // parallel_workers_launched
        });
        return table;
    }

    Table buildPgStatistic() {
        List<Column> cols = Cols.listOf(
                col("starelid", DataType.INTEGER),
                col("staattnum", DataType.SMALLINT),
                col("stainherit", DataType.BOOLEAN),
                col("stanullfrac", DataType.REAL),
                col("stawidth", DataType.INTEGER),
                col("stadistinct", DataType.REAL)
        );
        Table table = new Table("pg_statistic", cols);
        // Populate with rows for tables that have been ANALYZEd
        for (String schemaTable : database.getAnalyzedTables()) {
            String[] parts = schemaTable.split("\\.", 2);
            if (parts.length < 2) continue;
            String schemaName = parts[0];
            String tableName = parts[1];
            Schema schema = database.getSchema(schemaName);
            if (schema == null) continue;
            Table t = schema.getTable(tableName);
            if (t == null) continue;
            int relOid = oids.oid("rel:" + schemaTable);
            for (int i = 0; i < t.getColumns().size(); i++) {
                table.insertRow(new Object[]{
                        relOid,
                        (short)(i + 1),
                        false,
                        0.0f,
                        0,
                        -1.0f
                });
            }
        }
        return table;
    }

    Table buildPgStatBgwriter() {
        // PG 15+ shape: checkpoint stats moved to pg_stat_checkpointer
        List<Column> cols = Cols.listOf(
                col("buffers_clean", DataType.BIGINT),
                col("maxwritten_clean", DataType.BIGINT),
                col("buffers_alloc", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_bgwriter", cols);
        table.insertRow(new Object[]{0L, 0L, 0L, null});
        return table;
    }

    Table buildPgStatCheckpointer() {
        List<Column> cols = Cols.listOf(
                col("num_timed", DataType.BIGINT),
                col("num_requested", DataType.BIGINT),
                col("restartpoints_timed", DataType.BIGINT),
                col("restartpoints_req", DataType.BIGINT),
                col("restartpoints_done", DataType.BIGINT),
                col("write_time", DataType.DOUBLE_PRECISION),
                col("sync_time", DataType.DOUBLE_PRECISION),
                col("buffers_written", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_checkpointer", cols);
        table.insertRow(new Object[]{0L, 0L, 0L, 0L, 0L, 0.0, 0.0, 0L, null});
        return table;
    }

    Table buildPgStatWal() {
        List<Column> cols = Cols.listOf(
                col("wal_records", DataType.BIGINT),
                col("wal_fpi", DataType.BIGINT),
                col("wal_bytes", DataType.NUMERIC),
                col("wal_buffers_full", DataType.BIGINT),
                col("wal_write", DataType.BIGINT),
                col("wal_sync", DataType.BIGINT),
                col("wal_write_time", DataType.DOUBLE_PRECISION),
                col("wal_sync_time", DataType.DOUBLE_PRECISION),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_wal", cols);
        table.insertRow(new Object[]{0L, 0L, java.math.BigDecimal.ZERO, 0L, 0L, 0L, 0.0, 0.0, null});
        return table;
    }

    Table buildPgStatReplication() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("usesysid", DataType.INTEGER),
                col("usename", DataType.TEXT),
                col("application_name", DataType.TEXT),
                col("client_addr", DataType.TEXT),
                col("client_hostname", DataType.TEXT),
                col("client_port", DataType.INTEGER),
                col("backend_start", DataType.TIMESTAMPTZ),
                col("backend_xmin", DataType.INTEGER),
                col("state", DataType.TEXT),
                col("sent_lsn", DataType.TEXT),
                col("write_lsn", DataType.TEXT),
                col("flush_lsn", DataType.TEXT),
                col("replay_lsn", DataType.TEXT),
                col("write_lag", DataType.TEXT),
                col("flush_lag", DataType.TEXT),
                col("replay_lag", DataType.TEXT),
                col("sync_priority", DataType.INTEGER),
                col("sync_state", DataType.TEXT),
                col("reply_time", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_replication", cols); // empty, no replication
    }

    Table buildPgStatSubscription() {
        List<Column> cols = Cols.listOf(
                col("subid", DataType.INTEGER),
                col("subname", DataType.TEXT),
                col("pid", DataType.INTEGER),
                col("relid", DataType.INTEGER),
                col("received_lsn", DataType.TEXT),
                col("last_msg_send_time", DataType.TIMESTAMPTZ),
                col("last_msg_receipt_time", DataType.TIMESTAMPTZ),
                col("latest_end_lsn", DataType.TEXT),
                col("latest_end_time", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_subscription", cols); // empty, no subscriptions
    }

    Table buildPgStatProgressVacuum() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.INTEGER),
                col("datname", DataType.TEXT),
                col("relid", DataType.INTEGER),
                col("phase", DataType.TEXT),
                col("heap_blks_total", DataType.BIGINT),
                col("heap_blks_scanned", DataType.BIGINT),
                col("heap_blks_vacuumed", DataType.BIGINT),
                col("index_vacuum_count", DataType.BIGINT),
                col("max_dead_tuples", DataType.BIGINT),
                col("num_dead_tuples", DataType.BIGINT)
        );
        return new Table("pg_stat_progress_vacuum", cols); // empty, no vacuum in progress
    }

    Table buildPgStatProgressCreateIndex() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.INTEGER),
                col("datname", DataType.TEXT),
                col("relid", DataType.INTEGER),
                col("index_relid", DataType.INTEGER),
                col("command", DataType.TEXT),
                col("phase", DataType.TEXT),
                col("lockers_total", DataType.BIGINT),
                col("lockers_done", DataType.BIGINT),
                col("current_locker_pid", DataType.BIGINT),
                col("blocks_total", DataType.BIGINT),
                col("blocks_done", DataType.BIGINT),
                col("tuples_total", DataType.BIGINT),
                col("tuples_done", DataType.BIGINT),
                col("partitions_total", DataType.BIGINT),
                col("partitions_done", DataType.BIGINT)
        );
        return new Table("pg_stat_progress_create_index", cols); // empty
    }

    Table buildPgStatWalReceiver() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("status", DataType.TEXT),
                col("receive_start_lsn", DataType.TEXT),
                col("receive_start_tli", DataType.INTEGER),
                col("received_lsn", DataType.TEXT),
                col("received_tli", DataType.INTEGER),
                col("last_msg_send_time", DataType.TIMESTAMPTZ),
                col("last_msg_receipt_time", DataType.TIMESTAMPTZ),
                col("latest_end_lsn", DataType.TEXT),
                col("latest_end_time", DataType.TIMESTAMPTZ),
                col("slot_name", DataType.TEXT),
                col("sender_host", DataType.TEXT),
                col("sender_port", DataType.INTEGER),
                col("conninfo", DataType.TEXT)
        );
        return new Table("pg_stat_wal_receiver", cols); // empty, no WAL receiver
    }

    Table buildPgStatSsl() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("ssl", DataType.BOOLEAN),
                col("version", DataType.TEXT),
                col("cipher", DataType.TEXT),
                col("bits", DataType.INTEGER),
                col("compression", DataType.BOOLEAN),
                col("client_dn", DataType.TEXT),
                col("client_serial", DataType.NUMERIC),
                col("issuer_dn", DataType.TEXT)
        );
        return new Table("pg_stat_ssl", cols); // empty, no SSL
    }

    Table buildPgStatGssapi() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("gss_authenticated", DataType.BOOLEAN),
                col("principal", DataType.TEXT),
                col("encrypted", DataType.BOOLEAN),
                col("credentials_delegated", DataType.BOOLEAN)
        );
        Table table = new Table("pg_stat_gssapi", cols);
        // One row for the current backend (no GSS auth in memgres)
        table.insertRow(new Object[]{ 1, false, null, false, false });
        return table;
    }

    Table buildPgStatioUserTables() {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.INTEGER),
                col("schemaname", DataType.TEXT),
                col("relname", DataType.TEXT),
                col("heap_blks_read", DataType.BIGINT),
                col("heap_blks_hit", DataType.BIGINT),
                col("idx_blks_read", DataType.BIGINT),
                col("idx_blks_hit", DataType.BIGINT),
                col("toast_blks_read", DataType.BIGINT),
                col("toast_blks_hit", DataType.BIGINT),
                col("tidx_blks_read", DataType.BIGINT),
                col("tidx_blks_hit", DataType.BIGINT)
        );
        return new Table("pg_statio_user_tables", cols); // empty, in-memory with no I/O stats
    }

    // ---------------------------------------------------------------
    //  Timezone
    // ---------------------------------------------------------------

    Table buildPgTimezoneNames() {
        List<Column> cols = Cols.listOf(
                colNN("name", DataType.TEXT),
                col("abbrev", DataType.TEXT),
                col("utc_offset", DataType.INTERVAL),
                col("is_dst", DataType.BOOLEAN)
        );
        Table table = new Table("pg_timezone_names", cols);
        // Add common timezones
        for (String tz : java.time.ZoneId.getAvailableZoneIds().stream().sorted().collect(Collectors.toList())) {
            try {
                java.time.ZoneId zid = java.time.ZoneId.of(tz);
                java.time.ZoneOffset offset = java.time.ZonedDateTime.now(zid).getOffset();
                boolean isDst = zid.getRules().isDaylightSavings(java.time.Instant.now());
                String abbrev = java.time.ZonedDateTime.now(zid).getZone().getId();
                table.insertRow(new Object[]{tz, abbrev, offset.toString(), isDst});
            } catch (Exception ignored) {}
        }
        return table;
    }

    Table buildPgTimezoneAbbrevs() {
        List<Column> cols = Cols.listOf(
                colNN("abbrev", DataType.TEXT),
                col("utc_offset", DataType.INTERVAL),
                col("is_dst", DataType.BOOLEAN)
        );
        Table table = new Table("pg_timezone_abbrevs", cols);
        // Common timezone abbreviations
        table.insertRow(new Object[]{"UTC", "00:00:00", false});
        table.insertRow(new Object[]{"GMT", "00:00:00", false});
        table.insertRow(new Object[]{"EST", "-05:00:00", false});
        table.insertRow(new Object[]{"EDT", "-04:00:00", true});
        table.insertRow(new Object[]{"CST", "-06:00:00", false});
        table.insertRow(new Object[]{"CDT", "-05:00:00", true});
        table.insertRow(new Object[]{"MST", "-07:00:00", false});
        table.insertRow(new Object[]{"MDT", "-06:00:00", true});
        table.insertRow(new Object[]{"PST", "-08:00:00", false});
        table.insertRow(new Object[]{"PDT", "-07:00:00", true});
        table.insertRow(new Object[]{"CET", "01:00:00", false});
        table.insertRow(new Object[]{"CEST", "02:00:00", true});
        table.insertRow(new Object[]{"EET", "02:00:00", false});
        table.insertRow(new Object[]{"EEST", "03:00:00", true});
        table.insertRow(new Object[]{"JST", "09:00:00", false});
        table.insertRow(new Object[]{"IST", "05:30:00", false});
        table.insertRow(new Object[]{"AEST", "10:00:00", false});
        table.insertRow(new Object[]{"AEDT", "11:00:00", true});
        // Additional abbreviations to match PG18
        table.insertRow(new Object[]{"HST", "-10:00:00", false});
        table.insertRow(new Object[]{"AKST", "-09:00:00", false});
        table.insertRow(new Object[]{"AKDT", "-08:00:00", true});
        table.insertRow(new Object[]{"AST", "-04:00:00", false});
        table.insertRow(new Object[]{"ADT", "-03:00:00", true});
        table.insertRow(new Object[]{"NST", "-03:30:00", false});
        table.insertRow(new Object[]{"NDT", "-02:30:00", true});
        table.insertRow(new Object[]{"WET", "00:00:00", false});
        table.insertRow(new Object[]{"WEST", "01:00:00", true});
        table.insertRow(new Object[]{"MET", "01:00:00", false});
        table.insertRow(new Object[]{"MEST", "02:00:00", true});
        table.insertRow(new Object[]{"BST", "01:00:00", true});
        table.insertRow(new Object[]{"SST", "-11:00:00", false});
        table.insertRow(new Object[]{"ChST", "10:00:00", false});
        table.insertRow(new Object[]{"NZST", "12:00:00", false});
        table.insertRow(new Object[]{"NZDT", "13:00:00", true});
        table.insertRow(new Object[]{"AWST", "08:00:00", false});
        table.insertRow(new Object[]{"ACST", "09:30:00", false});
        table.insertRow(new Object[]{"ACDT", "10:30:00", true});
        table.insertRow(new Object[]{"HKT", "08:00:00", false});
        table.insertRow(new Object[]{"SGT", "08:00:00", false});
        table.insertRow(new Object[]{"KST", "09:00:00", false});
        table.insertRow(new Object[]{"ICT", "07:00:00", false});
        table.insertRow(new Object[]{"WIB", "07:00:00", false});
        table.insertRow(new Object[]{"WITA", "08:00:00", false});
        table.insertRow(new Object[]{"WIT", "09:00:00", false});
        table.insertRow(new Object[]{"PHT", "08:00:00", false});
        table.insertRow(new Object[]{"THA", "07:00:00", false});
        table.insertRow(new Object[]{"MSK", "03:00:00", false});
        table.insertRow(new Object[]{"SAST", "02:00:00", false});
        table.insertRow(new Object[]{"EAT", "03:00:00", false});
        table.insertRow(new Object[]{"WAT", "01:00:00", false});
        table.insertRow(new Object[]{"CAT", "02:00:00", false});
        table.insertRow(new Object[]{"PKT", "05:00:00", false});
        table.insertRow(new Object[]{"NPT", "05:45:00", false});
        table.insertRow(new Object[]{"BDT", "06:00:00", false});
        table.insertRow(new Object[]{"MMT", "06:30:00", false});
        table.insertRow(new Object[]{"CST6CDT", "-06:00:00", false});
        table.insertRow(new Object[]{"EST5EDT", "-05:00:00", false});
        table.insertRow(new Object[]{"MST7MDT", "-07:00:00", false});
        table.insertRow(new Object[]{"PST8PDT", "-08:00:00", false});
        table.insertRow(new Object[]{"ART", "-03:00:00", false});
        table.insertRow(new Object[]{"BRT", "-03:00:00", false});
        table.insertRow(new Object[]{"CLT", "-04:00:00", false});
        table.insertRow(new Object[]{"COT", "-05:00:00", false});
        table.insertRow(new Object[]{"ECT", "-05:00:00", false});
        table.insertRow(new Object[]{"PET", "-05:00:00", false});
        table.insertRow(new Object[]{"VET", "-04:00:00", false});
        table.insertRow(new Object[]{"GFT", "-03:00:00", false});
        table.insertRow(new Object[]{"UYT", "-03:00:00", false});
        table.insertRow(new Object[]{"PYT", "-04:00:00", false});
        table.insertRow(new Object[]{"BOT", "-04:00:00", false});
        table.insertRow(new Object[]{"GST", "04:00:00", false});
        table.insertRow(new Object[]{"GET", "04:00:00", false});
        table.insertRow(new Object[]{"AZT", "04:00:00", false});
        table.insertRow(new Object[]{"AMT", "04:00:00", false});
        table.insertRow(new Object[]{"HOVT", "07:00:00", false});
        table.insertRow(new Object[]{"UZT", "05:00:00", false});
        table.insertRow(new Object[]{"TJT", "05:00:00", false});
        table.insertRow(new Object[]{"TMT", "05:00:00", false});
        table.insertRow(new Object[]{"KGT", "06:00:00", false});
        table.insertRow(new Object[]{"ALMT", "06:00:00", false});
        table.insertRow(new Object[]{"YEKT", "05:00:00", false});
        table.insertRow(new Object[]{"NOVT", "07:00:00", false});
        table.insertRow(new Object[]{"KRAT", "07:00:00", false});
        table.insertRow(new Object[]{"IRKT", "08:00:00", false});
        table.insertRow(new Object[]{"YAKT", "09:00:00", false});
        table.insertRow(new Object[]{"VLAT", "10:00:00", false});
        table.insertRow(new Object[]{"MAGT", "11:00:00", false});
        table.insertRow(new Object[]{"PETT", "12:00:00", false});
        table.insertRow(new Object[]{"FJT", "12:00:00", false});
        table.insertRow(new Object[]{"TVT", "12:00:00", false});
        table.insertRow(new Object[]{"TOT", "13:00:00", false});
        table.insertRow(new Object[]{"CHAST", "12:45:00", false});
        table.insertRow(new Object[]{"FJST", "13:00:00", true});
        return table;
    }

    // ---------------------------------------------------------------
    //  Text search
    // ---------------------------------------------------------------

    Table buildPgTsParser() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("prsname", DataType.TEXT),
                colNN("prsnamespace", DataType.INTEGER),
                col("prsstart", DataType.INTEGER),
                col("prstoken", DataType.INTEGER),
                col("prsend", DataType.INTEGER),
                col("prsheadline", DataType.INTEGER),
                col("prslextype", DataType.INTEGER)
        );
        Table table = new Table("pg_ts_parser", cols);
        // default parser
        table.insertRow(new Object[]{3722, "default", oids.oid("ns:pg_catalog"), 0, 0, 0, 0, 0});
        return table;
    }

    Table buildPgTsDict() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("dictname", DataType.TEXT),
                colNN("dictnamespace", DataType.INTEGER),
                col("dictowner", DataType.INTEGER),
                col("dicttemplate", DataType.INTEGER),
                col("dictinitoption", DataType.TEXT)
        );
        Table table = new Table("pg_ts_dict", cols);
        int pgCatNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{3765, "simple", pgCatNs, 10, 3727, null});
        table.insertRow(new Object[]{12676, "english_stem", pgCatNs, 10, 3726, "language = 'english'"});
        // User-created text search dictionaries
        int oidCounter = 91000;
        int publicNs = oids.oid("ns:public");
        for (Map.Entry<String, Database.TsDictDef> entry : database.getTsDicts().entrySet()) {
            Database.TsDictDef dict = entry.getValue();
            int tmplOid = "simple".equalsIgnoreCase(dict.template) ? 3727 : 3726;
            table.insertRow(new Object[]{oidCounter++, dict.name.toLowerCase(), publicNs, 10, tmplOid, dict.options});
        }
        return table;
    }

    Table buildPgTsTemplate() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("tmplname", DataType.TEXT),
                colNN("tmplnamespace", DataType.INTEGER),
                col("tmplinit", DataType.INTEGER),
                col("tmpllexize", DataType.INTEGER)
        );
        Table table = new Table("pg_ts_template", cols);
        int pgCatNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{3727, "simple", pgCatNs, 0, 0});
        table.insertRow(new Object[]{3726, "snowball", pgCatNs, 0, 0});
        table.insertRow(new Object[]{3730, "synonym", pgCatNs, 0, 0});
        return table;
    }

    Table buildPgTsConfig() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("cfgname", DataType.TEXT),
                colNN("cfgnamespace", DataType.INTEGER),
                col("cfgowner", DataType.INTEGER),
                col("cfgparser", DataType.INTEGER)
        );
        Table table = new Table("pg_ts_config", cols);
        int pgCatNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{3748, "simple", pgCatNs, 10, 3722});
        table.insertRow(new Object[]{3764, "english", pgCatNs, 10, 3722});
        // User-created text search configurations
        int oidCounter = 90000;
        for (Map.Entry<String, Database.TsConfigDef> entry : database.getTsConfigs().entrySet()) {
            Database.TsConfigDef cfg = entry.getValue();
            int publicNs = oids.oid("ns:public");
            table.insertRow(new Object[]{oidCounter++, cfg.name.toLowerCase(), publicNs, 10, 3722});
        }
        return table;
    }

    Table buildPgTsConfigMap() {
        List<Column> cols = Cols.listOf(
                colNN("mapcfg", DataType.INTEGER),
                colNN("maptokentype", DataType.INTEGER),
                colNN("mapseqno", DataType.INTEGER),
                colNN("mapdict", DataType.INTEGER)
        );
        Table table = new Table("pg_ts_config_map", cols);
        // Default mappings for 'english' config (OID 3764)
        // Map common token types to english_stem dictionary (OID 12676)
        table.insertRow(new Object[]{3764, 1, 1, 12676}); // asciiword
        table.insertRow(new Object[]{3764, 2, 1, 12676}); // word
        // User-created config mappings
        int oidCounter = 90000;
        for (Map.Entry<String, String> entry : database.getTsConfigMaps().entrySet()) {
            String[] keyParts = entry.getKey().split("\0", 2);
            String cfgName = keyParts[0];
            String tokenType = keyParts[1];
            // Look up config OID
            int cfgOid = findTsConfigOid(cfgName, oidCounter);
            // Map token type name to an integer
            int tokenTypeId = mapTokenTypeName(tokenType);
            table.insertRow(new Object[]{cfgOid, tokenTypeId, 1, 3765});
        }
        return table;
    }

    private int findTsConfigOid(String cfgName, int startOid) {
        if ("simple".equalsIgnoreCase(cfgName)) return 3748;
        if ("english".equalsIgnoreCase(cfgName)) return 3764;
        int oid = 90000;
        for (String key : database.getTsConfigs().keySet()) {
            if (key.equalsIgnoreCase(cfgName)) return oid;
            oid++;
        }
        return startOid;
    }

    private int mapTokenTypeName(String name) {
        switch (name.toLowerCase()) {
            case "asciiword": return 1;
            case "word": return 2;
            case "numword": return 3;
            case "email": return 4;
            case "url": return 5;
            case "host": return 6;
            case "sfloat": return 7;
            case "version": return 8;
            case "hyphword": return 9;
            case "numhword": return 10;
            case "asciihword": return 11;
            case "blank": return 12;
            default: return 1;
        }
    }

    // ---------------------------------------------------------------
    //  Infrastructure stubs
    // ---------------------------------------------------------------

    Table buildPgAm() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("amname", DataType.TEXT),
                colNN("amtype", DataType.CHAR),
                col("amhandler", DataType.INTEGER),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_am", cols);
        table.insertRow(new Object[]{2, "heap", "t", oids.oid("proc:heap_tableam_handler"), 1});
        table.insertRow(new Object[]{403, "btree", "i", oids.oid("proc:bthandler"), 1});
        table.insertRow(new Object[]{405, "hash", "i", oids.oid("proc:hashhandler"), 1});
        table.insertRow(new Object[]{783, "gist", "i", oids.oid("proc:gisthandler"), 1});
        table.insertRow(new Object[]{2742, "gin", "i", oids.oid("proc:ginhandler"), 1});
        table.insertRow(new Object[]{4000, "spgist", "i", oids.oid("proc:spghandler"), 1});
        table.insertRow(new Object[]{3580, "brin", "i", oids.oid("proc:brinhandler"), 1});
        return table;
    }

    Table buildPgTablespace() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("spcname", DataType.TEXT),
                colNN("spcowner", DataType.INTEGER),
                col("spcacl", DataType.ACLITEM_ARRAY),
                col("spcoptions", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_tablespace", cols);
        table.insertRow(new Object[]{ oids.oid("tablespace:pg_default"), "pg_default", 10, null, null, 1 });
        table.insertRow(new Object[]{ oids.oid("tablespace:pg_global"), "pg_global", 10, null, null, 1 });
        return table;
    }

    Table buildPgShdescription() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.INTEGER),
                colNN("classoid", DataType.INTEGER),
                col("description", DataType.TEXT)
        );
        return new Table("pg_shdescription", cols); // empty, no shared descriptions
    }

    Table buildPgConversion() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("conname", DataType.TEXT),
                colNN("connamespace", DataType.INTEGER),
                col("conowner", DataType.INTEGER),
                col("conforencoding", DataType.INTEGER),
                col("contoencoding", DataType.INTEGER),
                col("conproc", DataType.INTEGER),
                col("condefault", DataType.BOOLEAN)
        );
        return new Table("pg_conversion", cols);
    }

    Table buildPgLargeobjectMetadata() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                col("lomowner", DataType.INTEGER),
                col("lomacl", DataType.ACLITEM_ARRAY)
        );
        Table t = new Table("pg_largeobject_metadata", cols);
        for (Long loid : database.getLargeObjectStore().getOids()) {
            t.insertRow(new Object[]{ loid.intValue(), 10, null });
        }
        return t;
    }

    Table buildPgShdepend() {
        List<Column> cols = Cols.listOf(
                colNN("dbid", DataType.INTEGER),
                colNN("classid", DataType.INTEGER),
                colNN("objid", DataType.INTEGER),
                colNN("objsubid", DataType.INTEGER),
                colNN("refclassid", DataType.INTEGER),
                colNN("refobjid", DataType.INTEGER),
                colNN("deptype", DataType.CHAR)
        );
        return new Table("pg_shdepend", cols);
    }

    Table buildPgSeclabel(String name) {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.INTEGER),
                colNN("classoid", DataType.INTEGER),
                colNN("objsubid", DataType.INTEGER),
                col("provider", DataType.TEXT),
                col("label", DataType.TEXT)
        );
        return new Table(name, cols);
    }

    Table buildPgTransform() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("trftype", DataType.INTEGER),
                colNN("trflang", DataType.INTEGER),
                col("trffromsql", DataType.INTEGER),
                col("trftosql", DataType.INTEGER)
        );
        return new Table("pg_transform", cols);
    }

    Table buildPgStatisticExt() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("stxrelid", DataType.INTEGER),
                colNN("stxname", DataType.TEXT),
                colNN("stxnamespace", DataType.INTEGER),
                col("stxowner", DataType.INTEGER),
                col("stxkeys", DataType.TEXT),
                col("stxkind", DataType.TEXT),
                col("stxstattarget", DataType.INTEGER)
        );
        Table table = new Table("pg_statistic_ext", cols);
        int pgCatalogNs = oids.oid("ns:public");
        for (ExtendedStatistic es : database.getAllExtendedStatistics().values()) {
            int statOid = oids.oid("stat:" + es.getName());
            int relOid = oids.oid("rel:public." + es.getTableName());
            // Build stxkeys as space-separated attnum list (simplified: use 1-based column indices)
            StringBuilder keys = new StringBuilder();
            Table refTable = null;
            for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                refTable = se.getValue().getTable(es.getTableName());
                if (refTable != null) break;
            }
            for (int i = 0; i < es.getColumns().size(); i++) {
                if (i > 0) keys.append(" ");
                String colName = es.getColumns().get(i);
                int attnum = i + 1;
                if (refTable != null && !colName.startsWith("(")) {
                    int idx = refTable.getColumnIndex(colName);
                    if (idx >= 0) attnum = idx + 1;
                }
                keys.append(attnum);
            }
            // Build stxkind as char array string, e.g., "{d,f,m}"
            StringBuilder kindStr = new StringBuilder("{");
            if (es.getKinds().isEmpty()) {
                kindStr.append("d,f,m"); // default: all kinds
            } else {
                for (int i = 0; i < es.getKinds().size(); i++) {
                    if (i > 0) kindStr.append(",");
                    String k = es.getKinds().get(i);
                    switch (k) {
                        case "ndistinct": kindStr.append("d"); break;
                        case "dependencies": kindStr.append("f"); break;
                        case "mcv": kindStr.append("m"); break;
                        default: kindStr.append(k.charAt(0)); break;
                    }
                }
            }
            kindStr.append("}");
            table.insertRow(new Object[]{
                    statOid, relOid, es.getName(), pgCatalogNs,
                    10, // stxowner (superuser)
                    keys.toString(),
                    kindStr.toString(),
                    es.getStattarget()
            });
        }
        return table;
    }

    Table buildPgStatisticExtData() {
        List<Column> cols = Cols.listOf(
                colNN("stxoid", DataType.INTEGER),
                col("stxdndistinct", DataType.TEXT),
                col("stxddependencies", DataType.TEXT),
                col("stxdmcv", DataType.BYTEA),
                col("stxdexpr", DataType.TEXT)
        );
        Table table = new Table("pg_statistic_ext_data", cols);
        for (ExtendedStatistic es : database.getAllExtendedStatistics().values()) {
            if (es.isAnalyzed()) {
                int statOid = oids.oid("stat:" + es.getName());
                table.insertRow(new Object[]{
                        statOid, null, null, null, null
                });
            }
        }
        return table;
    }

    Table buildPgStats() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.TEXT),
                col("tablename", DataType.TEXT),
                col("attname", DataType.TEXT),
                col("inherited", DataType.BOOLEAN),
                col("null_frac", DataType.REAL),
                col("avg_width", DataType.INTEGER),
                col("n_distinct", DataType.REAL),
                col("most_common_vals", DataType.TEXT),
                col("most_common_freqs", DataType.TEXT),
                col("histogram_bounds", DataType.TEXT),
                col("correlation", DataType.REAL),
                col("most_common_elems", DataType.TEXT),
                col("most_common_elem_freqs", DataType.TEXT),
                col("elem_count_histogram", DataType.TEXT)
        );
        Table table = new Table("pg_stats", cols);
        // Populate stats for all analyzed tables
        for (String schemaTable : database.getAnalyzedTables()) {
            String[] parts = schemaTable.split("\\.", 2);
            if (parts.length != 2) continue;
            String schemaName = parts[0];
            String tableName = parts[1];
            Schema schema = database.getSchema(schemaName);
            if (schema == null) continue;
            Table srcTable = schema.getTable(tableName);
            if (srcTable == null) continue;
            for (Column col : srcTable.getColumns()) {
                // Compute basic statistics from table data
                java.util.Set<Object> distinctVals = new java.util.HashSet<>();
                int nullCount = 0;
                long totalWidth = 0;
                int colIdx = srcTable.getColumnIndex(col.getName());
                for (Object[] row : srcTable.getRows()) {
                    Object val = (colIdx >= 0 && colIdx < row.length) ? row[colIdx] : null;
                    if (val == null) {
                        nullCount++;
                    } else {
                        distinctVals.add(val);
                        totalWidth += val.toString().length();
                    }
                }
                int totalRows = srcTable.getRows().size();
                float nullFrac = totalRows > 0 ? (float) nullCount / totalRows : 0f;
                int avgWidth = (totalRows - nullCount) > 0 ? (int) (totalWidth / (totalRows - nullCount)) : 0;
                float nDistinct = distinctVals.size();
                table.insertRow(new Object[]{
                        schemaName, tableName, col.getName(), false,
                        nullFrac, avgWidth, nDistinct,
                        null, null, null, 0.0f, null, null, null
                });
            }
        }
        return table;
    }

    Table buildPgStatsExt() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.TEXT),
                col("tablename", DataType.TEXT),
                col("statistics_name", DataType.TEXT),
                col("attnames", DataType.TEXT),
                col("kinds", DataType.TEXT),
                col("exprs", DataType.TEXT)
        );
        Table table = new Table("pg_stats_ext", cols);
        for (ExtendedStatistic es : database.getAllExtendedStatistics().values()) {
            // Find schema for the table
            String schemaName = "public";
            for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                if (se.getValue().getTable(es.getTableName()) != null) {
                    schemaName = se.getKey();
                    break;
                }
            }
            // Build attnames as PG array string
            StringBuilder attnames = new StringBuilder("{");
            for (int i = 0; i < es.getColumns().size(); i++) {
                if (i > 0) attnames.append(",");
                attnames.append(es.getColumns().get(i));
            }
            attnames.append("}");
            // Build kinds as PG array string
            StringBuilder kinds = new StringBuilder("{");
            for (int i = 0; i < es.getKinds().size(); i++) {
                if (i > 0) kinds.append(",");
                kinds.append(es.getKinds().get(i));
            }
            kinds.append("}");
            table.insertRow(new Object[]{
                    schemaName,
                    es.getTableName(),
                    es.getName(),
                    attnames.toString(),
                    kinds.toString(),
                    null // exprs
            });
        }
        return table;
    }

    Table buildPgPublicationRel() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("prpubid", DataType.INTEGER),
                colNN("prrelid", DataType.INTEGER),
                col("prqual", DataType.TEXT),
                col("prattrs", DataType.TEXT)
        );
        Table table = new Table("pg_publication_rel", cols);
        int seq = 60000;
        for (Database.PubDef pub : database.getPublications().values()) {
            int pubOid = oids.oid("pub:" + pub.name);
            for (String tblName : pub.tables) {
                int relOid = oids.oid("rel:public." + tblName);
                table.insertRow(new Object[]{ seq++, pubOid, relOid, null, null });
            }
        }
        return table;
    }

    Table buildPgPublicationNamespace() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("pnpubid", DataType.INTEGER),
                colNN("pnnspid", DataType.INTEGER)
        );
        return new Table("pg_publication_namespace", cols);
    }

    Table buildPgSubscriptionRel() {
        List<Column> cols = Cols.listOf(
                colNN("srsubid", DataType.INTEGER),
                colNN("srrelid", DataType.INTEGER),
                col("srsubstate", DataType.CHAR),
                col("srsublsn", DataType.TEXT)
        );
        return new Table("pg_subscription_rel", cols);
    }

    Table buildPgPartitionedTable() {
        List<Column> cols = Cols.listOf(
                colNN("partrelid", DataType.INTEGER),
                colNN("partstrat", DataType.CHAR),
                colNN("partnatts", DataType.SMALLINT),
                col("partdefid", DataType.INTEGER),
                col("partattrs", DataType.TEXT),
                col("partclass", DataType.TEXT),
                col("partcollation", DataType.TEXT),
                col("partexprs", DataType.TEXT)
        );
        Table table = new Table("pg_partitioned_table", cols);
        // Populate from all partitioned tables (those with a partition strategy)
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                if (t.getPartitionStrategy() == null) continue;
                int tblOid = oids.oid("rel:" + schemaName + "." + t.getName());
                String strategy = t.getPartitionStrategy().substring(0, 1).toLowerCase(); // r/l/h
                // Find default partition OID
                Integer defOid = null;
                for (Table p : t.getPartitions()) {
                    if (p.isDefaultPartition()) {
                        String pSchema = findSchemaForTable(p);
                        if (pSchema != null) defOid = oids.oid("rel:" + pSchema + "." + p.getName());
                        break;
                    }
                }
                // Compute partition column attribute numbers (1-based)
                String partCol = t.getPartitionColumn();
                short partnatts = 1;
                String partattrs = "0"; // fallback
                if (partCol != null) {
                    String[] partColParts = partCol.split(",");
                    partnatts = (short) partColParts.length;
                    StringBuilder attrsBuf = new StringBuilder();
                    for (int ci = 0; ci < partColParts.length; ci++) {
                        if (ci > 0) attrsBuf.append(' ');
                        int colIdx = t.getColumnIndex(partColParts[ci].trim());
                        attrsBuf.append(colIdx >= 0 ? colIdx + 1 : 0);
                    }
                    partattrs = attrsBuf.toString();
                }
                table.insertRow(new Object[]{
                        tblOid, strategy, partnatts, defOid,
                        partattrs, "0", "0", null
                });
            }
        }
        return table;
    }

    Table buildPgInherits() {
        List<Column> cols = Cols.listOf(
                colNN("inhrelid", DataType.INTEGER),
                colNN("inhparent", DataType.INTEGER),
                colNN("inhseqno", DataType.INTEGER),
                col("inhdetachpending", DataType.BOOLEAN)
        );
        Table table = new Table("pg_inherits", cols);
        // Populate from partition parent-child relationships
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int childOid = oids.oid("rel:" + schemaName + "." + t.getName());
                // Partition relationship: child has partitionParent
                if (t.getPartitionParent() != null) {
                    String parentSchema = findSchemaForTable(t.getPartitionParent());
                    if (parentSchema != null) {
                        int parentOid = oids.oid("rel:" + parentSchema + "." + t.getPartitionParent().getName());
                        int seqno = t.getPartitionParent().getPartitions().indexOf(t) + 1;
                        if (seqno <= 0) seqno = 1;
                        table.insertRow(new Object[]{ childOid, parentOid, seqno, false });
                    }
                }
                // Inheritance relationship: child has parentTable (but NOT partition)
                if (t.getParentTable() != null && t.getPartitionParent() == null) {
                    String parentSchema = findSchemaForTable(t.getParentTable());
                    if (parentSchema != null) {
                        int parentOid = oids.oid("rel:" + parentSchema + "." + t.getParentTable().getName());
                        int seqno = t.getParentTable().getChildren().indexOf(t) + 1;
                        if (seqno <= 0) seqno = 1;
                        table.insertRow(new Object[]{ childOid, parentOid, seqno, false });
                    }
                }
            }
        }
        // Index inheritance from ALTER INDEX ... ATTACH PARTITION / auto-propagation
        for (Map.Entry<String, String> entry : database.getIndexParentMap().entrySet()) {
            String childIdx = entry.getKey();
            String parentIdx = entry.getValue();
            // Resolve schema from stored index metadata (must match pg_class OID keys)
            String childSchema = resolveIndexSchema(childIdx);
            String parentSchema = resolveIndexSchema(parentIdx);
            int childOid = oids.oid("rel:" + childSchema + "." + childIdx);
            int parentOid = oids.oid("rel:" + parentSchema + "." + parentIdx);
            table.insertRow(new Object[]{ childOid, parentOid, 1, false });
        }
        return table;
    }

    /** Resolve the schema for an index using stored metadata (matches pg_class OID resolution). */
    private String resolveIndexSchema(String indexName) {
        String storedTable = database.getIndexTable(indexName);
        if (storedTable != null) {
            String[] parts = storedTable.split("\\.", 2);
            if (parts.length == 2) return parts[0];
        }
        return "public";
    }

    /** Find the schema name for a given Table object by scanning all schemas. */
    private String findSchemaForTable(Table target) {
        for (Map.Entry<String, Schema> entry : database.getSchemas().entrySet()) {
            for (Table t : entry.getValue().getTables().values()) {
                if (t == target) return entry.getKey();
            }
        }
        return null;
    }

    Table buildPgEventTrigger() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), colNN("evtname", DataType.TEXT),
                col("evtevent", DataType.TEXT), col("evtowner", DataType.INTEGER),
                col("evtfoid", DataType.INTEGER), col("evtenabled", DataType.CHAR),
                col("evttags", DataType.TEXT), col("xmin", DataType.INTEGER));
        return new Table("pg_event_trigger", cols);
    }

    Table buildPgEventTriggerPopulated() {
        Table table = buildPgEventTrigger();
        int seq = 50000;
        for (PgEventTrigger et : database.getAllEventTriggers().values()) {
            int etOid = oids.oid("evttrigger:" + et.getName());
            if (etOid == 0) etOid = seq++;
            // Resolve function OID
            int funcOid = oids.oid("func:" + et.getFunctionName());
            String tagsStr = null;
            if (et.getTags() != null && !et.getTags().isEmpty()) {
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < et.getTags().size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"").append(et.getTags().get(i)).append("\"");
                }
                sb.append("}");
                tagsStr = sb.toString();
            }
            table.insertRow(new Object[]{
                    etOid, et.getName(), et.getEvent(), 10 /* superuser oid */,
                    funcOid, String.valueOf(et.getEnabled()), tagsStr, 1
            });
        }
        return table;
    }

    Table buildPgForeignDataWrapper() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), colNN("fdwname", DataType.TEXT),
                col("fdwowner", DataType.INTEGER), col("fdwhandler", DataType.INTEGER),
                col("fdwvalidator", DataType.INTEGER), col("fdwacl", DataType.ACLITEM_ARRAY),
                col("fdwoptions", DataType.TEXT), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_foreign_data_wrapper", cols);
        for (Database.FdwWrapper w : database.getForeignDataWrappers().values()) {
            table.insertRow(new Object[]{
                    oids.oid("fdw:" + w.name), w.name, 10, 0, 0, null, w.options, 1
            });
        }
        return table;
    }

    Table buildPgForeignServer() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), colNN("srvname", DataType.TEXT),
                col("srvowner", DataType.INTEGER), col("srvfdw", DataType.INTEGER),
                col("srvtype", DataType.TEXT), col("srvversion", DataType.TEXT),
                col("srvoptions", DataType.TEXT), col("srvacl", DataType.ACLITEM_ARRAY),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_foreign_server", cols);
        for (Database.FdwServer s : database.getForeignServers().values()) {
            int fdwOid = oids.oid("fdw:" + s.fdwName);
            table.insertRow(new Object[]{
                    oids.oid("srv:" + s.name), s.name, 10, fdwOid, null, null, s.options, null, 1
            });
        }
        return table;
    }

    Table buildPgUserMapping() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), col("umuser", DataType.INTEGER),
                col("umserver", DataType.INTEGER), col("umoptions", DataType.TEXT),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_user_mapping", cols);
        for (Database.FdwUserMapping m : database.getForeignUserMappings().values()) {
            int serverOid = oids.oid("srv:" + m.serverName);
            int userOid = "PUBLIC".equalsIgnoreCase(m.userName) ? 0 : oids.oid("role:" + m.userName);
            table.insertRow(new Object[]{
                    oids.oid("um:" + m.serverName + ":" + m.userName), userOid, serverOid, m.options, 1
            });
        }
        return table;
    }

    Table buildPgUserMappings() {
        List<Column> cols = Cols.listOf(
                colNN("umid", DataType.INTEGER),
                col("srvid", DataType.INTEGER),
                col("srvname", DataType.TEXT),
                col("umuser", DataType.INTEGER),
                col("usename", DataType.TEXT),
                col("umoptions", DataType.TEXT)
        );
        Table table = new Table("pg_user_mappings", cols);
        for (Database.FdwUserMapping m : database.getForeignUserMappings().values()) {
            int serverOid = oids.oid("srv:" + m.serverName);
            int userOid = "PUBLIC".equalsIgnoreCase(m.userName) ? 0 : oids.oid("role:" + m.userName);
            String displayName = "PUBLIC".equalsIgnoreCase(m.userName) ? null : m.userName;
            table.insertRow(new Object[]{
                    oids.oid("um:" + m.serverName + ":" + m.userName),
                    serverOid, m.serverName, userOid, displayName, m.options
            });
        }
        return table;
    }

    Table buildPgForeignTable() {
        List<Column> cols = Cols.listOf(
                colNN("ftrelid", DataType.INTEGER), col("ftserver", DataType.INTEGER),
                col("ftoptions", DataType.TEXT), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_foreign_table", cols);
        for (Database.FdwForeignTable ft : database.getForeignTables().values()) {
            int relOid = oids.oid("rel:public." + ft.tableName);
            int serverOid = oids.oid("srv:" + ft.serverName);
            table.insertRow(new Object[]{ relOid, serverOid, ft.options, 1 });
        }
        return table;
    }

    /** pg_seclabels: security labels view. Empty but needs correct columns for catalog queries. */
    Table buildPgSeclabels() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.INTEGER),
                colNN("classoid", DataType.INTEGER),
                colNN("objsubid", DataType.INTEGER),
                col("objtype", DataType.TEXT),
                col("objnamespace", DataType.INTEGER),
                col("objname", DataType.TEXT),
                col("provider", DataType.TEXT),
                col("label", DataType.TEXT)
        );
        return new Table("pg_seclabels", cols);
    }

    Table buildPgInitPrivs() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.INTEGER),
                colNN("classoid", DataType.INTEGER),
                colNN("objsubid", DataType.INTEGER),
                col("privtype", DataType.CHAR),
                col("initprivs", DataType.ACLITEM_ARRAY)
        );
        return new Table("pg_init_privs", cols); // empty, no initial privileges to track
    }

    Table buildPgPreparedXacts() {
        List<Column> cols = Cols.listOf(
                col("transaction", DataType.INTEGER),
                col("gid", DataType.TEXT),
                col("prepared", DataType.TIMESTAMPTZ),
                col("owner", DataType.TEXT),
                col("database", DataType.TEXT)
        );
        Table table = new Table("pg_prepared_xacts", cols);
        for (Database.PreparedTransaction pt : database.getPreparedTransactions().values()) {
            table.insertRow(new Object[]{
                    (int) pt.transactionId,
                    pt.gid,
                    formatTimestamptz(pt.prepared),
                    pt.owner,
                    pt.database
            });
        }
        return table;
    }

    Table buildPgCursors(Session session) {
        List<Column> cols = Cols.listOf(
                col("name", DataType.TEXT),
                col("statement", DataType.TEXT),
                col("is_holdable", DataType.BOOLEAN),
                col("is_binary", DataType.BOOLEAN),
                col("is_scrollable", DataType.BOOLEAN),
                col("creation_time", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_cursors", cols);
        if (session != null) {
            for (Session.CursorState cursor : session.getAllCursors()) {
                String stmt = cursor.getQueryText();
                String creationTimeStr = formatTimestamptz(cursor.getCreationTime());
                // PG reports is_scrollable=true for default cursors (no keyword)
                // and SCROLL cursors. Only explicit NO SCROLL reports false.
                boolean reportedScrollable = cursor.isScrollable() || !cursor.isExplicitNoScroll();
                table.insertRow(new Object[]{
                        cursor.getName(),
                        stmt,
                        cursor.isHoldable(),
                        cursor.isBinary(),
                        reportedScrollable,
                        creationTimeStr
                });
            }
            // PG shows an implicit unnamed portal cursor ("<unnamed portal 1>") for the
            // currently executing query. This is visible in pg_cursors even in simple query mode.
            String implicitCreationTimeStr = formatTimestamptz(java.time.OffsetDateTime.now());
            table.insertRow(new Object[]{
                    "<unnamed portal 1>",
                    "SELECT count(*)::integer AS count FROM pg_cursors",
                    false,
                    false,
                    true,
                    implicitCreationTimeStr
            });
        }
        return table;
    }

    Table buildPgPreparedStatements(Session session) {
        List<Column> cols = Cols.listOf(
                col("name", DataType.TEXT),
                col("statement", DataType.TEXT),
                col("prepare_time", DataType.TIMESTAMPTZ),
                col("parameter_types", DataType.TEXT_ARRAY),
                col("result_types", DataType.TEXT_ARRAY),
                col("from_sql", DataType.BOOLEAN),
                col("generic_plans", DataType.BIGINT),
                col("custom_plans", DataType.BIGINT)
        );
        Table table = new Table("pg_prepared_statements", cols);
        if (session != null) {
            for (Session.PreparedStmt ps : session.getAllPreparedStatements()) {
                String stmtText = ps.sqlText() != null ? ps.sqlText() : SqlUnparser.toSql(ps.body());
                String prepareTimeStr = formatTimestamptz(ps.prepareTime());
                // parameter_types as regtype[] — stored as List<Object> for array subscripting support
                List<Object> paramTypes = toRegTypeList(ps.paramTypes());
                // result_types as regtype[] — null for DML without RETURNING (PG behavior)
                List<Object> resultTypes = ps.resultTypes() != null ? toRegTypeList(ps.resultTypes()) : null;
                // generic_plans / custom_plans: PG 14+ plan execution counters.
                // Queries without parameters use generic plans; parameterized use custom plans.
                long genericPlans = ps.genericPlans();
                long customPlans = ps.customPlans();
                table.insertRow(new Object[]{
                        ps.name(),
                        stmtText,
                        prepareTimeStr,
                        paramTypes,
                        resultTypes,
                        ps.fromSql(),
                        genericPlans,
                        customPlans
                });
            }
        }
        return table;
    }

    /** Convert a list of type name strings to a List<Object> for regtype[] array storage. */
    private List<Object> toRegTypeList(java.util.List<String> types) {
        if (types == null || types.isEmpty()) return new ArrayList<>();
        return new ArrayList<>(types);
    }

    // formatParamTypes and formatResultTypes removed — arrays now stored as List<Object> directly

    Table buildPgAvailableExtensions() {
        List<Column> cols = Cols.listOf(
                col("name", DataType.TEXT),
                col("default_version", DataType.TEXT),
                col("installed_version", DataType.TEXT),
                col("trusted", DataType.BOOLEAN),
                col("comment", DataType.TEXT)
        );
        Table table = new Table("pg_available_extensions", cols);
        table.insertRow(new Object[]{"plpgsql", "1.0", "1.0", true, "PL/pgSQL procedural language"});
        return table;
    }

    Table buildPgAvailableExtensionVersions() {
        List<Column> cols = Cols.listOf(
                col("name", DataType.TEXT),
                col("version", DataType.TEXT),
                col("installed", DataType.BOOLEAN),
                col("superuser", DataType.BOOLEAN),
                col("trusted", DataType.BOOLEAN),
                col("relocatable", DataType.BOOLEAN),
                col("schema", DataType.TEXT),
                col("requires", DataType.TEXT),
                col("comment", DataType.TEXT)
        );
        Table table = new Table("pg_available_extension_versions", cols);
        table.insertRow(new Object[]{"plpgsql", "1.0", true, false, true, false, "pg_catalog", null, "PL/pgSQL procedural language"});
        return table;
    }

    Table buildPgConfig() {
        List<Column> cols = Cols.listOf(
                col("name", DataType.TEXT),
                col("setting", DataType.TEXT)
        );
        return new Table("pg_config", cols); // empty, no real config paths
    }

    Table buildPgFileSettings() {
        List<Column> cols = Cols.listOf(
                col("sourcefile", DataType.TEXT),
                col("sourceline", DataType.INTEGER),
                col("seqno", DataType.INTEGER),
                col("name", DataType.TEXT),
                col("setting", DataType.TEXT),
                col("applied", DataType.BOOLEAN),
                col("error", DataType.TEXT)
        );
        return new Table("pg_file_settings", cols); // empty, no file-based config
    }

    Table buildPgHbaFileRules() {
        List<Column> cols = Cols.listOf(
                col("rule_number", DataType.INTEGER),
                col("file_name", DataType.TEXT),
                col("line_number", DataType.INTEGER),
                col("type", DataType.TEXT),
                col("database", DataType.TEXT),
                col("user_name", DataType.TEXT),
                col("address", DataType.TEXT),
                col("netmask", DataType.TEXT),
                col("auth_method", DataType.TEXT),
                col("options", DataType.TEXT),
                col("error", DataType.TEXT)
        );
        Table table = new Table("pg_hba_file_rules", cols);
        table.insertRow(new Object[]{1, null, 1, "host", "all", "all", "0.0.0.0/0", null, "trust", null, null});
        return table;
    }

    Table buildPgShmemAllocations() {
        List<Column> cols = Cols.listOf(
                col("name", DataType.TEXT),
                col("off", DataType.BIGINT),
                col("size", DataType.BIGINT),
                col("allocated_size", DataType.BIGINT)
        );
        return new Table("pg_shmem_allocations", cols); // empty, no shared memory
    }

    Table buildPgPublication() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                col("pubname", DataType.TEXT),
                col("pubowner", DataType.INTEGER),
                col("puballtables", DataType.BOOLEAN),
                col("pubinsert", DataType.BOOLEAN),
                col("pubupdate", DataType.BOOLEAN),
                col("pubdelete", DataType.BOOLEAN),
                col("pubtruncate", DataType.BOOLEAN),
                col("pubviaroot", DataType.BOOLEAN),
                col("pubgencols", DataType.TEXT)
        );
        Table table = new Table("pg_publication", cols);
        for (Database.PubDef pub : database.getPublications().values()) {
            table.insertRow(new Object[]{
                    oids.oid("pub:" + pub.name), pub.name, 10,
                    pub.allTables ? "t" : "f", "t", "t", "t", "t", "f", null
            });
        }
        return table;
    }

    Table buildPgSubscription() {
        // PG18 pg_subscription: 18 columns (see system_views.sql GRANT on pg_subscription)
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                col("subdbid", DataType.INTEGER),
                col("subskiplsn", DataType.TEXT),
                col("subname", DataType.TEXT),
                col("subowner", DataType.INTEGER),
                col("subenabled", DataType.BOOLEAN),
                col("subbinary", DataType.BOOLEAN),
                col("substream", DataType.CHAR),
                col("subtwophasestate", DataType.CHAR),
                col("subdisableonerr", DataType.BOOLEAN),
                col("subpasswordrequired", DataType.BOOLEAN),
                col("subrunasowner", DataType.BOOLEAN),
                col("subfailover", DataType.BOOLEAN),
                col("subconninfo", DataType.TEXT),
                col("subslotname", DataType.TEXT),
                col("subsynccommit", DataType.TEXT),
                col("subpublications", DataType.TEXT),
                col("suborigin", DataType.TEXT)
        );
        Table table = new Table("pg_subscription", cols);
        for (Database.SubDef sub : database.getSubscriptions().values()) {
            table.insertRow(new Object[]{
                    oids.oid("sub:" + sub.name), oids.oid("db:memgres"),
                    null,                           // subskiplsn
                    sub.name, 10,
                    false,                          // subenabled
                    false,                          // subbinary
                    "f",                            // substream
                    "d",                            // subtwophasestate
                    false,                          // subdisableonerr
                    true,                           // subpasswordrequired
                    false,                          // subrunasowner
                    false,                          // subfailover
                    sub.conninfo, sub.name, "off",
                    "{" + sub.publication + "}",
                    "any"                           // suborigin
            });
        }
        return table;
    }

    Table buildPgPublicationTables() {
        List<Column> cols = Cols.listOf(
                col("pubid", DataType.INTEGER),
                col("pubname", DataType.TEXT),
                col("schemaname", DataType.TEXT),
                col("tablename", DataType.TEXT)
        );
        Table table = new Table("pg_publication_tables", cols);
        for (Database.PubDef pub : database.getPublications().values()) {
            int pubOid = oids.oid("pub:" + pub.name);
            for (String tblName : pub.tables) {
                table.insertRow(new Object[]{ pubOid, pub.name, "public", tblName });
            }
        }
        return table;
    }

    Table buildPgReplicationSlots() {
        List<Column> cols = Cols.listOf(
                col("slot_name", DataType.TEXT),
                col("plugin", DataType.TEXT),
                col("slot_type", DataType.TEXT),
                col("datoid", DataType.INTEGER),
                col("database", DataType.TEXT),
                col("temporary", DataType.BOOLEAN),
                col("active", DataType.BOOLEAN),
                col("active_pid", DataType.INTEGER),
                col("xmin", DataType.INTEGER),
                col("catalog_xmin", DataType.INTEGER),
                col("restart_lsn", DataType.TEXT),
                col("confirmed_flush_lsn", DataType.TEXT),
                col("wal_status", DataType.TEXT),
                col("safe_wal_size", DataType.BIGINT),
                col("two_phase", DataType.BOOLEAN),
                col("conflicting", DataType.BOOLEAN)
        );
        Table table = new Table("pg_replication_slots", cols);
        for (Database.ReplicationSlot slot : database.getReplicationSlots().values()) {
            table.insertRow(new Object[]{
                    slot.slotName, slot.plugin, slot.slotType,
                    oids.oid("db:memgres"), "memgres",
                    false, false, null, null, null,
                    "0/0", "0/0", "reserved", null, false, false
            });
        }
        return table;
    }

    Table buildPgReplicationOrigin() {
        List<Column> cols = Cols.listOf(
                colNN("roident", DataType.INTEGER),
                col("roname", DataType.TEXT)
        );
        return new Table("pg_replication_origin", cols);
    }

    Table buildPgReplicationOriginStatus() {
        List<Column> cols = Cols.listOf(
                col("local_id", DataType.INTEGER),
                col("external_id", DataType.TEXT),
                col("remote_lsn", DataType.TEXT),
                col("local_lsn", DataType.TEXT)
        );
        return new Table("pg_replication_origin_status", cols);
    }

    Table buildPgStatSubscriptionStats() {
        List<Column> cols = Cols.listOf(
                col("subid", DataType.INTEGER),
                col("subname", DataType.TEXT),
                col("apply_error_count", DataType.BIGINT),
                col("sync_error_count", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_subscription_stats", cols);
    }

    Table buildPgMatviews() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.TEXT),
                col("matviewname", DataType.TEXT),
                col("matviewowner", DataType.TEXT),
                col("tablespace", DataType.TEXT),
                col("hasindexes", DataType.BOOLEAN),
                col("ispopulated", DataType.BOOLEAN),
                col("definition", DataType.TEXT)
        );
        return new Table("pg_matviews", cols); // empty, no materialized views
    }

    Table buildPgRulesView() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.TEXT),
                col("tablename", DataType.TEXT),
                col("rulename", DataType.TEXT),
                col("definition", DataType.TEXT)
        );
        Table table = new Table("pg_rules", cols);
        for (java.util.Map.Entry<String, String[]> entry : database.getRuleDefinitions().entrySet()) {
            String ruleName = entry.getKey();
            String tableName = entry.getValue()[0];
            String definition = entry.getValue()[1];
            table.insertRow(new Object[]{"public", tableName, ruleName, definition});
        }
        return table;
    }

    Table buildPgStatStatements() {
        List<Column> cols = Cols.listOf(
                col("userid", DataType.INTEGER),
                col("dbid", DataType.INTEGER),
                col("toplevel", DataType.BOOLEAN),
                col("queryid", DataType.BIGINT),
                col("query", DataType.TEXT),
                col("plans", DataType.BIGINT),
                col("total_plan_time", DataType.DOUBLE_PRECISION),
                col("min_plan_time", DataType.DOUBLE_PRECISION),
                col("max_plan_time", DataType.DOUBLE_PRECISION),
                col("mean_plan_time", DataType.DOUBLE_PRECISION),
                col("stddev_plan_time", DataType.DOUBLE_PRECISION),
                col("calls", DataType.BIGINT),
                col("total_exec_time", DataType.DOUBLE_PRECISION),
                col("min_exec_time", DataType.DOUBLE_PRECISION),
                col("max_exec_time", DataType.DOUBLE_PRECISION),
                col("mean_exec_time", DataType.DOUBLE_PRECISION),
                col("stddev_exec_time", DataType.DOUBLE_PRECISION),
                col("rows", DataType.BIGINT),
                col("shared_blks_hit", DataType.BIGINT),
                col("shared_blks_read", DataType.BIGINT),
                col("shared_blks_dirtied", DataType.BIGINT),
                col("shared_blks_written", DataType.BIGINT),
                col("local_blks_hit", DataType.BIGINT),
                col("local_blks_read", DataType.BIGINT),
                col("local_blks_dirtied", DataType.BIGINT),
                col("local_blks_written", DataType.BIGINT),
                col("temp_blks_read", DataType.BIGINT),
                col("temp_blks_written", DataType.BIGINT),
                col("blk_read_time", DataType.DOUBLE_PRECISION),
                col("blk_write_time", DataType.DOUBLE_PRECISION),
                col("wal_records", DataType.BIGINT),
                col("wal_fpi", DataType.BIGINT),
                col("wal_bytes", DataType.NUMERIC),
                col("jit_functions", DataType.BIGINT),
                col("jit_generation_time", DataType.DOUBLE_PRECISION),
                col("jit_inlining_count", DataType.BIGINT),
                col("jit_inlining_time", DataType.DOUBLE_PRECISION),
                col("jit_optimization_count", DataType.BIGINT),
                col("jit_optimization_time", DataType.DOUBLE_PRECISION),
                col("jit_emission_count", DataType.BIGINT),
                col("jit_emission_time", DataType.DOUBLE_PRECISION)
        );
        return new Table("pg_stat_statements", cols); // empty, no tracked statements
    }

    Table buildPgStatStatementsInfo() {
        List<Column> cols = Cols.listOf(
                col("dealloc", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_statements_info", cols);
        table.insertRow(new Object[]{0L, null});
        return table;
    }

    Table buildPgStatArchiver() {
        List<Column> cols = Cols.listOf(
                col("archived_count", DataType.BIGINT),
                col("last_archived_wal", DataType.TEXT),
                col("last_archived_time", DataType.TIMESTAMPTZ),
                col("failed_count", DataType.BIGINT),
                col("last_failed_wal", DataType.TEXT),
                col("last_failed_time", DataType.TIMESTAMPTZ),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_archiver", cols); // empty, no archiver
    }

    Table buildPgStatIo() {
        List<Column> cols = Cols.listOf(
                col("backend_type", DataType.TEXT),
                col("object", DataType.TEXT),
                col("context", DataType.TEXT),
                col("reads", DataType.BIGINT),
                col("read_time", DataType.DOUBLE_PRECISION),
                col("writes", DataType.BIGINT),
                col("write_time", DataType.DOUBLE_PRECISION),
                col("writebacks", DataType.BIGINT),
                col("writeback_time", DataType.DOUBLE_PRECISION),
                col("extends", DataType.BIGINT),
                col("extend_time", DataType.DOUBLE_PRECISION),
                col("hits", DataType.BIGINT),
                col("evictions", DataType.BIGINT),
                col("reuses", DataType.BIGINT),
                col("fsyncs", DataType.BIGINT),
                col("fsync_time", DataType.DOUBLE_PRECISION),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_io", cols); // empty, no I/O stats
    }

    Table buildPgStatUserFunctions() {
        List<Column> cols = Cols.listOf(
                col("funcid", DataType.INTEGER),
                col("schemaname", DataType.TEXT),
                col("funcname", DataType.TEXT),
                col("calls", DataType.BIGINT),
                col("total_time", DataType.DOUBLE_PRECISION),
                col("self_time", DataType.DOUBLE_PRECISION)
        );
        Table table = new Table("pg_stat_user_functions", cols);
        // PG requires track_functions = 'all' to populate this view.
        // Memgres does not track function stats, so always return empty.
        return table;
    }

    Table buildPgLargeobject() {
        List<Column> cols = Cols.listOf(
                col("loid", DataType.INTEGER),
                col("pageno", DataType.INTEGER),
                col("data", DataType.BYTEA)
        );
        Table table = new Table("pg_largeobject", cols);
        // Populate from large object store — each LO is split into 2048-byte pages
        for (Long loid : database.getLargeObjectStore().getOids()) {
            byte[] data = null;
            try { data = database.getLargeObjectStore().loGet(loid); } catch (Exception ignored) {}
            if (data == null || data.length == 0) {
                // Even empty LOs have at least one page
                table.insertRow(new Object[]{ loid.intValue(), 0, new byte[0] });
            } else {
                int pageSize = 2048;
                int pageNo = 0;
                for (int off = 0; off < data.length; off += pageSize) {
                    int end = Math.min(off + pageSize, data.length);
                    byte[] page = java.util.Arrays.copyOfRange(data, off, end);
                    table.insertRow(new Object[]{ loid.intValue(), pageNo, page });
                    pageNo++;
                }
            }
        }
        return table;
    }

    Table buildPgParameterAcl() {
        List<Column> cols = Cols.listOf(
                col("oid", DataType.INTEGER),
                col("parname", DataType.TEXT),
                col("paracl", DataType.TEXT_ARRAY)
        );
        return new Table("pg_parameter_acl", cols); // empty
    }

    Table buildPgBuffercache() {
        List<Column> cols = Cols.listOf(
                col("bufferid", DataType.INTEGER),
                col("relfilenode", DataType.INTEGER),
                col("reltablespace", DataType.INTEGER),
                col("reldatabase", DataType.INTEGER),
                col("relforknumber", DataType.SMALLINT),
                col("relblocknumber", DataType.BIGINT),
                col("isdirty", DataType.BOOLEAN),
                col("usagecount", DataType.SMALLINT),
                col("pinning_backends", DataType.INTEGER)
        );
        return new Table("pg_buffercache", cols); // empty, no buffer cache
    }

    Table buildPgStatWalSenders() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("state", DataType.TEXT),
                col("sent_lsn", DataType.TEXT),
                col("write_lsn", DataType.TEXT),
                col("flush_lsn", DataType.TEXT),
                col("replay_lsn", DataType.TEXT),
                col("write_lag", DataType.TEXT),
                col("flush_lag", DataType.TEXT),
                col("replay_lag", DataType.TEXT),
                col("sync_priority", DataType.INTEGER),
                col("sync_state", DataType.TEXT),
                col("reply_time", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_wal_senders", cols); // empty, no WAL senders
    }

    Table buildPgIdentFileMappings() {
        List<Column> cols = Cols.listOf(
                col("map_number", DataType.INTEGER),
                col("file_name", DataType.TEXT),
                col("line_number", DataType.INTEGER),
                col("map_name", DataType.TEXT),
                col("sys_name", DataType.TEXT),
                col("pg_username", DataType.TEXT)
        );
        return new Table("pg_ident_file_mappings", cols); // empty, no ident mappings
    }

    Table buildPgDbRoleSetting() {
        List<Column> cols = Cols.listOf(
                col("setdatabase", DataType.INTEGER),
                col("setrole", DataType.INTEGER),
                col("setconfig", DataType.TEXT)
        );
        return new Table("pg_db_role_setting", cols); // empty, no per-db role settings
    }
}
