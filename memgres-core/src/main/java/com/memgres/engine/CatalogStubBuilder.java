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

    /**
     * The text-search objects PostgreSQL pins an OID for, named once so the rows that point at
     * each other cannot drift apart.
     *
     * <p>All but two are written into PostgreSQL's own .dat files and are the same number on
     * every server: the default parser is 3722, the simple dictionary 3765 and the simple
     * template 3727, and the simple configuration 3748. snowball and english_stem are created at
     * initdb instead, so their numbers are not fixed anywhere and memgres only has to keep its
     * own two rows agreeing with each other.
     */
    private static final int PARSER_DEFAULT = 3722;
    private static final int DICT_SIMPLE = 3765;
    private static final int DICT_ENGLISH_STEM = 12676;
    private static final int TMPL_SIMPLE = 3727;
    private static final int TMPL_SNOWBALL = 14801;
    private static final int CFG_SIMPLE = 3748;
    private static final int CFG_ENGLISH = 3764;

    /**
     * An array column that also records what it is an array of.
     *
     * <p>{@link CatalogHelper#col} leaves the element type unset, and a column with an array type
     * but no element type is described by its internal typname: {@code pg_typeof} answered
     * {@code _text} where PostgreSQL answers {@code text[]}. Recording the element is what lets
     * the column be named the way a client could write it back in SQL.
     */
    private static Column arrayCol(String name, DataType arrayType, DataType elementType) {
        return new Column(name, arrayType, true, false, null, null, null, null, null, null, null,
                elementType);
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
                colNN("schemaname", DataType.NAME),
                colNN("tablename", DataType.NAME),
                colNN("tableowner", DataType.NAME),
                col("tablespace", DataType.NAME),
                col("hasindexes", DataType.BOOLEAN),
                col("hasrules", DataType.BOOLEAN),
                col("hastriggers", DataType.BOOLEAN),
                col("rowsecurity", DataType.BOOLEAN)
        );
        Table table = new Table("pg_tables", cols);
        // M22: tables on either side of a FK carry internal RI triggers in PG.
        java.util.Set<String> fkReferencedTables = new java.util.HashSet<>();
        for (Schema sch : database.getSchemas().values()) {
            for (Table tt : sch.getTables().values()) {
                for (StoredConstraint sc : tt.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                        fkReferencedTables.add(sc.getReferencesTable().toLowerCase());
                    }
                }
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                String tableName = tableEntry.getKey();
                Table t = tableEntry.getValue();
                // M22: compute hasindexes/hastriggers from actual state
                final String tblSchema = schemaEntry.getKey();
                boolean hasIdx = !t.getConstraints().isEmpty() || database.getIndexColumns().keySet().stream()
                        .anyMatch(idx -> { String ti = database.getIndexTable(idx); return ti != null && ti.equalsIgnoreCase(tblSchema + "." + tableName); });
                boolean hasFk = false;
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) { hasFk = true; break; }
                }
                // Triggers are held under the bare table name, so a same-named table in another
                // schema was reported as carrying the other's triggers. Each trigger knows the
                // schema its table lives in, and only its own table's count.
                boolean hasOwnTrigger = false;
                java.util.List<PgTrigger> named = database.getAllTriggers().get(tableName);
                if (named != null) {
                    for (PgTrigger trig : named) {
                        String trigSchema = trig.getSchemaName();
                        if (trigSchema == null || trigSchema.equalsIgnoreCase(tblSchema)) {
                            hasOwnTrigger = true;
                            break;
                        }
                    }
                }
                boolean hasTrig = hasOwnTrigger
                        || hasFk || fkReferencedTables.contains(tableName.toLowerCase());
                table.insertRow(new Object[]{
                        schemaEntry.getKey(), tableName, "memgres", null, hasIdx, false, hasTrig,
                        t.isRlsEnabled()
                });
            }
        }
        return table;
    }

    Table buildPgViews() {
        List<Column> cols = Cols.listOf(
                colNN("schemaname", DataType.NAME),
                colNN("viewname", DataType.NAME),
                colNN("viewowner", DataType.NAME),
                col("definition", DataType.TEXT)
        );
        Table table = new Table("pg_views", cols);
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            String viewDef = null;
            if (vd.query() != null) {
                String raw = vd.sourceSQL() != null ? vd.sourceSQL() : SqlUnparser.toSql(vd.query());
                // M19: pg_views.definition uses PG's pretty multi-line form (with trailing ;).
                viewDef = SqlUnparser.prettyViewDef(raw) + ";";
            }
            table.insertRow(new Object[]{vSchema, vd.name(), "memgres", viewDef});
        }
        return table;
    }

    Table buildPgIndexes() {
        List<Column> cols = Cols.listOf(
                colNN("schemaname", DataType.NAME),
                colNN("tablename", DataType.NAME),
                colNN("indexname", DataType.NAME),
                col("tablespace", DataType.NAME),
                col("indexdef", DataType.TEXT)
        );
        Table table = new Table("pg_indexes", cols);
        // Track which index names we've already added to avoid duplicates
        Set<String> addedIndexes = new HashSet<>();

        // 1. Explicit indexes (CREATE INDEX)
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexKey = idx.getKey();
            String indexName = Database.idxName(indexKey);
            List<String> indexCols = idx.getValue();
            String storedTableQualified = database.getIndexTable(indexKey);
            String schemaName = "public";
            String tableName = storedTableQualified;
            if (storedTableQualified != null && storedTableQualified.contains(".")) {
                String[] parts = storedTableQualified.split("\\.", 2);
                schemaName = parts[0];
                tableName = parts[1];
            }
            boolean isUnique = database.isUniqueIndex(indexKey);
            String method = database.getIndexMethod(indexKey);
            String indexDef = buildIndexDef(indexName, storedTableQualified, isUnique, method,
                    CatalogHelper.deparseIndexColumns(database, storedTableQualified, indexCols),
                    database.getIndexColumnOptions(indexKey),
                    database.getIndexIncludeColumns(indexKey),
                    database.isIndexNullsNotDistinct(indexKey),
                    CatalogHelper.deparseIndexPredicate(database, storedTableQualified,
                            database.getIndexWhereClause(indexKey)));
            table.insertRow(new Object[]{schemaName, tableName, indexName, null, indexDef});
            addedIndexes.add(Database.idxKey(schemaName, indexName).toLowerCase());
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
                        if (addedIndexes.contains(
                                Database.idxKey(schemaName, indexName).toLowerCase())) continue;
                        String indexDef = "CREATE UNIQUE INDEX " + indexName
                                + " ON " + schemaName + "." + t.getName()
                                + " USING btree (" + String.join(", ", sc.getColumns()) + ")"
                                // The index a UNIQUE NULLS NOT DISTINCT constraint creates is
                                // itself a NULLS NOT DISTINCT index, and reads back as one.
                                + (sc.isNullsNotDistinct() ? " NULLS NOT DISTINCT" : "");
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
        return buildIndexDef(indexName, tableName, isUnique, method, indexCols, columnOptions,
                includeColumns, nullsNotDistinct, whereClause, null);
    }

    /**
     * The definition an index was created with, including the storage parameters it carries.
     *
     * <p>The parameters were left out, so a definition read back from the catalogue could not
     * recreate the index it described: a fillfactor set at creation vanished from the text.
     */
    static String buildIndexDef(String indexName, String tableName, boolean isUnique,
                                String method, List<String> indexCols, List<String> columnOptions,
                                List<String> includeColumns, boolean nullsNotDistinct,
                                String whereClause, java.util.Map<String, String> reloptions) {
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
                        if (part.startsWith("collate:")) {
                            sb.append(" COLLATE \"").append(part.substring(8).replace("\"", "")).append('"');
                        } else if (part.startsWith("opclass:")) {
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
        if (reloptions != null && !reloptions.isEmpty()) {
            StringBuilder opts = new StringBuilder();
            for (java.util.Map.Entry<String, String> e : reloptions.entrySet()) {
                if (opts.length() > 0) opts.append(", ");
                opts.append(e.getKey()).append("='").append(e.getValue()).append('\'');
            }
            sb.append(" WITH (").append(opts).append(')');
        }
        if (whereClause != null && !whereClause.isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    Table buildPgSequence() {
        // pg_sequence: the catalog table (not the pg_sequences view)
        List<Column> cols = Cols.listOf(
                colNN("seqrelid", DataType.OID),
                colNN("seqtypid", DataType.OID),
                colNN("seqstart", DataType.BIGINT),
                colNN("seqincrement", DataType.BIGINT),
                colNN("seqmax", DataType.BIGINT),
                colNN("seqmin", DataType.BIGINT),
                colNN("seqcache", DataType.BIGINT),
                colNN("seqcycle", DataType.BOOLEAN)
        );
        Table table = new Table("pg_sequence", cols);
        for (String qualified : getSequenceNames(database)) {
            Sequence seq = database.getSequence(qualified);
            int seqOid = oids.oid("rel:" + qualified);
            // Determine sequence type from the source column type
            DataType seqDataType = getSequenceDataType(database, qualified);
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
                colNN("schemaname", DataType.NAME),
                colNN("sequencename", DataType.NAME),
                colNN("sequenceowner", DataType.NAME),
                col("data_type", DataType.REGTYPE),
                col("start_value", DataType.BIGINT),
                col("min_value", DataType.BIGINT),
                col("max_value", DataType.BIGINT),
                col("increment_by", DataType.BIGINT),
                col("cache_size", DataType.BIGINT),
                col("cycle", DataType.BOOLEAN),
                col("last_value", DataType.BIGINT)
        );
        Table table = new Table("pg_sequences", cols);
        for (String qualified : getSequenceNames(database)) {
            String seqName = CatalogHelper.nameOf(qualified);
            String seqSchema = CatalogHelper.schemaOf(qualified);
            Sequence seq = database.getSequence(qualified);
            DataType seqDataType = getSequenceDataType(database, qualified);
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
            table.insertRow(new Object[]{
                    seqSchema, seqName, "memgres", typeName,
                    startWith, minValue, maxValue,
                    incrementBy, cacheSize, cycle, lastValue
            });
        }
        return table;
    }

    // ---------------------------------------------------------------
    //  Stats stubs (and some with data)
    // ---------------------------------------------------------------

    Table buildPgStatUserTables() {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.OID),
                col("schemaname", DataType.NAME),
                col("relname", DataType.NAME),
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
                col("n_tup_newpage_upd", DataType.BIGINT),
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
                col("autoanalyze_count", DataType.BIGINT),
                col("total_vacuum_time", DataType.DOUBLE_PRECISION),
                col("total_autovacuum_time", DataType.DOUBLE_PRECISION),
                col("total_analyze_time", DataType.DOUBLE_PRECISION),
                col("total_autoanalyze_time", DataType.DOUBLE_PRECISION)
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
                        0L, t.getTupUpdated(), t.getTupDeleted(), 0L, 0L, // ins, upd, del, hot_upd, newpage_upd
                        (long) t.getRows().size(), 0L, // live, dead
                        0L, 0L,                        // mod_since_analyze, ins_since_vacuum
                        lastVac, null, lastAna, null,   // last vacuum/analyze
                        0L, 0L, 0L, 0L,                // counts
                        0.0, 0.0, 0.0, 0.0             // total vacuum/analyze times
                });
            }
        }
        // L12: PG includes materialized views in pg_stat_user_tables
        for (Database.ViewDef vd : database.getViews().values()) {
            if (!vd.materialized()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            table.insertRow(new Object[]{
                    oids.oid("rel:" + vSchema + "." + vd.name()),
                    vSchema, vd.name(),
                    0L, null, 0L, 0L, null, 0L,
                    0L, 0L, 0L, 0L, 0L,
                    0L, 0L,
                    0L, 0L,
                    null, null, null, null,
                    0L, 0L, 0L, 0L,
                    0.0, 0.0, 0.0, 0.0
            });
        }
        return table;
    }

    /**
     * The transaction-local table counters. PostgreSQL keeps these in their own view with a
     * shorter column list than pg_stat_all_tables, so they need their own shape rather than
     * borrowing one that reports lifetime vacuum times a transaction cannot have.
     */
    Table buildPgStatXactTables(String name) {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.OID),
                col("schemaname", DataType.NAME),
                col("relname", DataType.NAME),
                col("seq_scan", DataType.BIGINT),
                col("seq_tup_read", DataType.BIGINT),
                col("idx_scan", DataType.BIGINT),
                col("idx_tup_fetch", DataType.BIGINT),
                col("n_tup_ins", DataType.BIGINT),
                col("n_tup_upd", DataType.BIGINT),
                col("n_tup_del", DataType.BIGINT),
                col("n_tup_hot_upd", DataType.BIGINT),
                col("n_tup_newpage_upd", DataType.BIGINT)
        );
        Table table = new Table(name, cols);
        if (name.contains("_sys_")) return table;
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Table t : schemaEntry.getValue().getTables().values()) {
                table.insertRow(new Object[]{
                        oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName()),
                        schemaEntry.getKey(), t.getName(),
                        0L, 0L, 0L, 0L,
                        0L, t.getTupUpdated(), t.getTupDeleted(), 0L, 0L
                });
            }
        }
        return table;
    }

    /** Block-level index counters; a different view from pg_stat_*_indexes, with its own columns. */
    Table buildPgStatioIndexes(String name) {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.OID),
                col("indexrelid", DataType.OID),
                col("schemaname", DataType.NAME),
                col("relname", DataType.NAME),
                col("indexrelname", DataType.NAME),
                col("idx_blks_read", DataType.BIGINT),
                col("idx_blks_hit", DataType.BIGINT)
        );
        return new Table(name, cols); // empty, in-memory with no I/O stats
    }

    /** Block-level sequence counters. */
    Table buildPgStatioSequences(String name) {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.OID),
                col("schemaname", DataType.NAME),
                col("relname", DataType.NAME),
                col("blks_read", DataType.BIGINT),
                col("blks_hit", DataType.BIGINT)
        );
        return new Table(name, cols); // empty, in-memory with no I/O stats
    }

    Table buildPgStatUserIndexes() {
        List<Column> cols = Cols.listOf(
                col("relid", DataType.OID),
                col("indexrelid", DataType.OID),
                col("schemaname", DataType.NAME),
                col("relname", DataType.NAME),
                col("indexrelname", DataType.NAME),
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
            String indexKey = idx.getKey();
            String indexName = Database.idxName(indexKey);
            String storedTable = database.getIndexTable(indexKey);
            String indexSchema = "public";
            String tableName = null;
            if (storedTable != null) {
                String[] parts = storedTable.split("\\.", 2);
                if (parts.length == 2) { indexSchema = parts[0]; tableName = parts[1]; }
                else tableName = parts[0];
            }
            if (tableName != null) {
                addedIndexes.add(Database.idxKey(indexSchema, indexName).toLowerCase());
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
                    String ciKey = Database.idxKey(schemaName, constraintName).toLowerCase();
                    if (!addedIndexes.contains(ciKey)) {
                        addedIndexes.add(ciKey);
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
                col("datid", DataType.OID),
                col("datname", DataType.NAME),
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
                col("parallel_workers_to_launch", DataType.BIGINT),
                col("parallel_workers_launched", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
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
                0L, 0L,      // parallel_workers_to_launch, parallel_workers_launched
                null         // stats_reset
        });
        return table;
    }

    Table buildPgStatistic() {
        // PostgreSQL keeps five numbered slots per column; a query that reads a histogram or a
        // most-common-value list names them, so a pg_statistic without them describes nothing.
        List<Column> cols = new java.util.ArrayList<>(Cols.listOf(
                col("starelid", DataType.OID),
                col("staattnum", DataType.SMALLINT),
                col("stainherit", DataType.BOOLEAN),
                col("stanullfrac", DataType.REAL),
                col("stawidth", DataType.INTEGER),
                col("stadistinct", DataType.REAL)
        ));
        for (int i = 1; i <= 5; i++) cols.add(col("stakind" + i, DataType.SMALLINT));
        for (int i = 1; i <= 5; i++) cols.add(col("staop" + i, DataType.OID));
        for (int i = 1; i <= 5; i++) cols.add(col("stacoll" + i, DataType.OID));
        for (int i = 1; i <= 5; i++) cols.add(col("stanumbers" + i, DataType.FLOAT4_ARRAY));
        for (int i = 1; i <= 5; i++) cols.add(col("stavalues" + i, DataType.ANYARRAY));
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
                // Every one of the five numbered slots is a column of this relation, so a row has
                // to carry them whether or not the slot holds a statistic. Supplying only the
                // first six values left the row shorter than the relation it belongs to, and
                // reading stakind1 off the end of it was an internal error rather than the zero
                // PostgreSQL answers for an unused slot.
                Object[] row = new Object[cols.size()];
                row[0] = relOid;
                row[1] = (short) (i + 1);
                row[2] = false;
                row[3] = 0.0f;
                row[4] = 0;
                row[5] = -1.0f;
                // stakind/staop/stacoll are zero in an unused slot; stanumbers/stavalues are null.
                for (int slot = 6; slot < 6 + 5; slot++) row[slot] = (short) 0;
                for (int slot = 11; slot < 11 + 10; slot++) row[slot] = 0;
                table.insertRow(row);
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
                col("num_done", DataType.BIGINT),
                col("restartpoints_timed", DataType.BIGINT),
                col("restartpoints_req", DataType.BIGINT),
                col("restartpoints_done", DataType.BIGINT),
                col("write_time", DataType.DOUBLE_PRECISION),
                col("sync_time", DataType.DOUBLE_PRECISION),
                col("buffers_written", DataType.BIGINT),
                col("slru_written", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_checkpointer", cols);
        table.insertRow(new Object[]{0L, 0L, 0L, 0L, 0L, 0L, 0.0, 0.0, 0L, 0L, null});
        return table;
    }

    Table buildPgStatWal() {
        // PG 18 dropped the write/sync counters from this view; they moved to pg_stat_io.
        List<Column> cols = Cols.listOf(
                col("wal_records", DataType.BIGINT),
                col("wal_fpi", DataType.BIGINT),
                col("wal_bytes", DataType.NUMERIC),
                col("wal_buffers_full", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_wal", cols);
        table.insertRow(new Object[]{0L, 0L, java.math.BigDecimal.ZERO, 0L, null});
        return table;
    }

    Table buildPgStatReplication() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("usesysid", DataType.OID),
                col("usename", DataType.NAME),
                col("application_name", DataType.TEXT),
                col("client_addr", DataType.INET),
                col("client_hostname", DataType.TEXT),
                col("client_port", DataType.INTEGER),
                col("backend_start", DataType.TIMESTAMPTZ),
                col("backend_xmin", DataType.XID),
                col("state", DataType.TEXT),
                col("sent_lsn", DataType.PG_LSN),
                col("write_lsn", DataType.PG_LSN),
                col("flush_lsn", DataType.PG_LSN),
                col("replay_lsn", DataType.PG_LSN),
                col("write_lag", DataType.INTERVAL),
                col("flush_lag", DataType.INTERVAL),
                col("replay_lag", DataType.INTERVAL),
                col("sync_priority", DataType.INTEGER),
                col("sync_state", DataType.TEXT),
                col("reply_time", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_replication", cols); // empty, no replication
    }

    Table buildPgStatSubscription() {
        List<Column> cols = Cols.listOf(
                col("subid", DataType.OID),
                col("subname", DataType.NAME),
                col("worker_type", DataType.TEXT),
                col("pid", DataType.INTEGER),
                col("leader_pid", DataType.INTEGER),
                col("relid", DataType.OID),
                col("received_lsn", DataType.PG_LSN),
                col("last_msg_send_time", DataType.TIMESTAMPTZ),
                col("last_msg_receipt_time", DataType.TIMESTAMPTZ),
                col("latest_end_lsn", DataType.PG_LSN),
                col("latest_end_time", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_subscription", cols); // empty, no subscriptions
    }

    Table buildPgStatProgressVacuum() {
        // PG 17 replaced the dead-tuple counts with byte-based ones and added index progress.
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.OID),
                col("datname", DataType.NAME),
                col("relid", DataType.OID),
                col("phase", DataType.TEXT),
                col("heap_blks_total", DataType.BIGINT),
                col("heap_blks_scanned", DataType.BIGINT),
                col("heap_blks_vacuumed", DataType.BIGINT),
                col("index_vacuum_count", DataType.BIGINT),
                col("max_dead_tuple_bytes", DataType.BIGINT),
                col("dead_tuple_bytes", DataType.BIGINT),
                col("num_dead_item_ids", DataType.BIGINT),
                col("indexes_total", DataType.BIGINT),
                col("indexes_processed", DataType.BIGINT),
                col("delay_time", DataType.DOUBLE_PRECISION)
        );
        return new Table("pg_stat_progress_vacuum", cols); // empty, no vacuum in progress
    }

    /** The remaining pg_stat_progress_* views: no operation is ever in progress, but a
     *  monitoring query names their columns and has to resolve. */
    Table buildPgStatProgressAnalyze() {
        return new Table("pg_stat_progress_analyze", Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.OID),
                col("datname", DataType.NAME),
                col("relid", DataType.OID),
                col("phase", DataType.TEXT),
                col("sample_blks_total", DataType.BIGINT),
                col("sample_blks_scanned", DataType.BIGINT),
                col("ext_stats_total", DataType.BIGINT),
                col("ext_stats_computed", DataType.BIGINT),
                col("child_tables_total", DataType.BIGINT),
                col("child_tables_done", DataType.BIGINT),
                col("current_child_table_relid", DataType.OID),
                col("delay_time", DataType.DOUBLE_PRECISION)));
    }

    Table buildPgStatProgressCluster() {
        return new Table("pg_stat_progress_cluster", Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.OID),
                col("datname", DataType.NAME),
                col("relid", DataType.OID),
                col("command", DataType.TEXT),
                col("phase", DataType.TEXT),
                col("cluster_index_relid", DataType.OID),
                col("heap_tuples_scanned", DataType.BIGINT),
                col("heap_tuples_written", DataType.BIGINT),
                col("heap_blks_total", DataType.BIGINT),
                col("heap_blks_scanned", DataType.BIGINT),
                col("index_rebuild_count", DataType.BIGINT)));
    }

    Table buildPgStatProgressBasebackup() {
        return new Table("pg_stat_progress_basebackup", Cols.listOf(
                col("pid", DataType.INTEGER),
                col("phase", DataType.TEXT),
                col("backup_total", DataType.BIGINT),
                col("backup_streamed", DataType.BIGINT),
                col("tablespaces_total", DataType.BIGINT),
                col("tablespaces_streamed", DataType.BIGINT)));
    }

    Table buildPgStatProgressCopy() {
        return new Table("pg_stat_progress_copy", Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.OID),
                col("datname", DataType.NAME),
                col("relid", DataType.OID),
                col("command", DataType.TEXT),
                col("type", DataType.TEXT),
                col("bytes_processed", DataType.BIGINT),
                col("bytes_total", DataType.BIGINT),
                col("tuples_processed", DataType.BIGINT),
                col("tuples_excluded", DataType.BIGINT),
                col("tuples_skipped", DataType.BIGINT)));
    }

    /** The memory-context view PG exposes for a backend; memgres allocates on the JVM heap. */
    Table buildPgBackendMemoryContexts() {
        return new Table("pg_backend_memory_contexts", Cols.listOf(
                col("name", DataType.TEXT),
                col("ident", DataType.TEXT),
                col("type", DataType.TEXT),
                col("level", DataType.INTEGER),
                col("path", DataType.INT4_ARRAY),
                col("total_bytes", DataType.BIGINT),
                col("total_nblocks", DataType.BIGINT),
                col("free_bytes", DataType.BIGINT),
                col("free_chunks", DataType.BIGINT),
                col("used_bytes", DataType.BIGINT)));
    }

    Table buildPgStatProgressCreateIndex() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("datid", DataType.OID),
                col("datname", DataType.NAME),
                col("relid", DataType.OID),
                col("index_relid", DataType.OID),
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
                col("receive_start_lsn", DataType.PG_LSN),
                col("receive_start_tli", DataType.INTEGER),
                col("written_lsn", DataType.PG_LSN),
                col("flushed_lsn", DataType.PG_LSN),
                col("received_tli", DataType.INTEGER),
                col("last_msg_send_time", DataType.TIMESTAMPTZ),
                col("last_msg_receipt_time", DataType.TIMESTAMPTZ),
                col("latest_end_lsn", DataType.PG_LSN),
                col("latest_end_time", DataType.TIMESTAMPTZ),
                // text here, unlike pg_replication_slots.slot_name, which is a name: the receiver
                // reports the slot it was configured with, not one this server holds a row for.
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
                col("relid", DataType.OID),
                col("schemaname", DataType.NAME),
                col("relname", DataType.NAME),
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

    /**
     * pg_ts_parser. The five prs* columns are regprocs in PostgreSQL, naming the C functions that
     * drive the parser. memgres's parser is not reachable as a function, so each is reported as
     * InvalidOid — which is what PostgreSQL itself writes when there is no function — and prints
     * as '-' rather than as a function name memgres could not have called.
     */
    Table buildPgTsParser() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("prsname", DataType.NAME),
                colNN("prsnamespace", DataType.OID),
                col("prsstart", DataType.REGPROC),
                col("prstoken", DataType.REGPROC),
                col("prsend", DataType.REGPROC),
                col("prsheadline", DataType.REGPROC),
                col("prslextype", DataType.REGPROC)
        );
        Table table = new Table("pg_ts_parser", cols);
        // default parser
        RegprocValue none = new RegprocValue(0, "-");
        table.insertRow(new Object[]{3722, "default", oids.oid("ns:pg_catalog"),
                none, none, none, none, none});
        return table;
    }

    Table buildPgTsDict() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("dictname", DataType.NAME),
                colNN("dictnamespace", DataType.OID),
                col("dictowner", DataType.OID),
                col("dicttemplate", DataType.OID),
                col("dictinitoption", DataType.TEXT)
        );
        Table table = new Table("pg_ts_dict", cols);
        int pgCatNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{3765, "simple", pgCatNs, 10, TMPL_SIMPLE, null});
        table.insertRow(new Object[]{DICT_ENGLISH_STEM, "english_stem", pgCatNs, 10, TMPL_SNOWBALL,
                "language = 'english'"});
        // User-created text search dictionaries
        int oidCounter = 91000;
        int publicNs = oids.oid("ns:public");
        for (Map.Entry<String, Database.TsDictDef> entry : database.getTsDicts().entrySet()) {
            Database.TsDictDef dict = entry.getValue();
            int tmplOid = "simple".equalsIgnoreCase(dict.template) ? TMPL_SIMPLE : TMPL_SNOWBALL;
            table.insertRow(new Object[]{oidCounter++, dict.name.toLowerCase(), publicNs, 10, tmplOid, dict.options});
        }
        return table;
    }

    Table buildPgTsTemplate() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("tmplname", DataType.NAME),
                colNN("tmplnamespace", DataType.OID),
                col("tmplinit", DataType.REGPROC),
                col("tmpllexize", DataType.REGPROC)
        );
        Table table = new Table("pg_ts_template", cols);
        int pgCatNs = oids.oid("ns:pg_catalog");
        RegprocValue none = new RegprocValue(0, "-");
        table.insertRow(new Object[]{TMPL_SIMPLE, "simple", pgCatNs, none, none});
        // snowball is created at initdb rather than pinned in a .dat file, so PostgreSQL's OID
        // for it is not a fixed number and memgres cannot match it. 3726 was worse than
        // arbitrary: PostgreSQL hands that OID to the function dsimple_lexize, so a client that
        // resolved it against a real server landed on something else entirely. Numbered out of
        // the way instead.
        table.insertRow(new Object[]{TMPL_SNOWBALL, "snowball", pgCatNs, none, none});
        table.insertRow(new Object[]{3730, "synonym", pgCatNs, none, none});
        return table;
    }

    Table buildPgTsConfig() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("cfgname", DataType.NAME),
                colNN("cfgnamespace", DataType.OID),
                col("cfgowner", DataType.OID),
                col("cfgparser", DataType.OID)
        );
        Table table = new Table("pg_ts_config", cols);
        int pgCatNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{CFG_SIMPLE, "simple", pgCatNs, 10, PARSER_DEFAULT});
        table.insertRow(new Object[]{CFG_ENGLISH, "english", pgCatNs, 10, PARSER_DEFAULT});
        // User-created text search configurations
        int oidCounter = 90000;
        for (Map.Entry<String, Database.TsConfigDef> entry : database.getTsConfigs().entrySet()) {
            Database.TsConfigDef cfg = entry.getValue();
            int publicNs = oids.oid("ns:public");
            table.insertRow(new Object[]{oidCounter++, cfg.name.toLowerCase(), publicNs, 10,
                    PARSER_DEFAULT});
        }
        return table;
    }

    /**
     * The token types a shipped configuration has a dictionary for.
     *
     * <p>PostgreSQL maps nineteen of the parser's twenty-three: everything the default parser can
     * emit except blank, tag, protocol and entity, which carry no lexeme. memgres listed two —
     * asciiword and word — and lexized all nineteen anyway, so the catalog said the server would
     * drop an email address or a URL from a tsvector that it in fact indexes, and a tool that
     * read the mapping to decide what a configuration covers was told the wrong answer nineteen
     * times over. See {@link #buildPgTsConfigMap()}.
     */
    private static final int[] MAPPED_TOKEN_TYPES = {
            1,  // asciiword
            2,  // word
            3,  // numword
            4,  // email
            5,  // url
            6,  // host
            7,  // sfloat
            8,  // version
            9,  // hword_numpart
            10, // hword_part
            11, // hword_asciipart
            15, // numhword
            16, // asciihword
            17, // hword
            18, // url_path
            19, // file
            20, // float
            21, // int
            22, // uint
    };

    /**
     * The token types the {@code english} configuration runs through {@code english_stem}.
     *
     * <p>A snowball stemmer is for words. PostgreSQL sends the six word-shaped token types through
     * it and every other one — an email address, a URL, a host name, a version string, a number —
     * through {@code simple}, because stemming those would change a value that has to come back
     * out as it went in. memgres's own text search already agrees: {@code to_tsvector('english',
     * 'running user@example.com 12345')} keeps the address and the number whole and stems only the
     * word. Naming english_stem for all nineteen said the opposite of what the engine does.
     */
    private static final java.util.Set<Integer> ENGLISH_STEMMED_TOKEN_TYPES =
            new java.util.HashSet<Integer>(Arrays.asList(
                    1,  // asciiword
                    2,  // word
                    10, // hword_part
                    11, // hword_asciipart
                    16, // asciihword
                    17  // hword
            ));

    Table buildPgTsConfigMap() {
        List<Column> cols = Cols.listOf(
                colNN("mapcfg", DataType.OID),
                colNN("maptokentype", DataType.INTEGER),
                colNN("mapseqno", DataType.INTEGER),
                colNN("mapdict", DataType.OID)
        );
        Table table = new Table("pg_ts_config_map", cols);
        // The two shipped configurations, each mapping every token type that carries a lexeme:
        // 'simple' through the simple dictionary, 'english' through english_stem for the word
        // shapes and simple for everything else. Both are backed — to_tsvector('simple', ...) and
        // to_tsvector('english', ...) lexize all of them, and by these dictionaries.
        for (int tokenType : MAPPED_TOKEN_TYPES) {
            table.insertRow(new Object[]{CFG_SIMPLE, tokenType, 1, DICT_SIMPLE});
        }
        for (int tokenType : MAPPED_TOKEN_TYPES) {
            table.insertRow(new Object[]{CFG_ENGLISH, tokenType, 1,
                    ENGLISH_STEMMED_TOKEN_TYPES.contains(tokenType)
                            ? DICT_ENGLISH_STEM : DICT_SIMPLE});
        }
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
            // A name the parser does not emit is not a mapping: writing it down as asciiword
            // claimed a rule the configuration never had.
            if (tokenTypeId < 0) continue;
            table.insertRow(new Object[]{cfgOid, tokenTypeId, 1, DICT_SIMPLE});
        }
        return table;
    }

    private int findTsConfigOid(String cfgName, int startOid) {
        if ("simple".equalsIgnoreCase(cfgName)) return CFG_SIMPLE;
        if ("english".equalsIgnoreCase(cfgName)) return CFG_ENGLISH;
        int oid = 90000;
        for (String key : database.getTsConfigs().keySet()) {
            if (key.equalsIgnoreCase(cfgName)) return oid;
            oid++;
        }
        return startOid;
    }

    /**
     * The token-type number the default parser gives an alias, or -1 for a name it never emits.
     *
     * <p>These have to be the numbers {@code ts_token_type('default')} answers with, because that
     * is what a {@code pg_ts_config_map JOIN ts_token_type} — the only way to read the mapping as
     * names — joins on. Three were wrong: hword_numpart, hword_part and hword_asciipart are 9, 10
     * and 11, and numhword, asciihword and hword are 15, 16 and 17, so a configuration mapped on
     * numhword was recorded as mapping hword_part instead.
     */
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
            case "hword_numpart": return 9;
            case "hword_part": return 10;
            case "hword_asciipart": return 11;
            case "blank": return 12;
            case "tag": return 13;
            case "protocol": return 14;
            case "numhword": return 15;
            case "asciihword": return 16;
            case "hword": return 17;
            case "url_path": return 18;
            case "file": return 19;
            case "float": return 20;
            case "int": return 21;
            case "uint": return 22;
            case "entity": return 23;
            default: return -1;
        }
    }

    // ---------------------------------------------------------------
    //  Infrastructure stubs
    // ---------------------------------------------------------------

    /**
     * pg_am. amhandler is a regproc, and a regproc prints the function's name: PostgreSQL answers
     * {@code bthandler} for {@code amhandler::text}, and memgres answered the raw OID because the
     * column held a bare Integer rather than the name-and-OID pair {@link CatalogHelper#regproc}
     * makes. The join to pg_proc landed either way, so only a client that read the column as
     * text — which is how psql's \dA and every "what handles this index" query print it — saw it.
     */
    Table buildPgAm() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("amname", DataType.NAME),
                colNN("amtype", DataType.INTERNAL_CHAR),
                col("amhandler", DataType.REGPROC),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_am", cols);
        // The access-method OIDs are PostgreSQL's own. amhandler is not: it has to be the OID
        // pg_proc gave the handler function, and pg_proc mints those from the user-object
        // counter, so the number here is above 16384 where PostgreSQL's is 330 for bthandler.
        // It resolves — amhandler::regproc reads back "bthandler", and a join to pg_proc lands
        // on a row — which is what a client follows the column for; only a comparison of the
        // raw number against PostgreSQL's would notice. Fixing it means giving the handler
        // functions their real OIDs in pg_proc, which is CatalogCoreBuilder's to do.
        table.insertRow(new Object[]{2, "heap", "t", handler("heap_tableam_handler"), 1});
        table.insertRow(new Object[]{403, "btree", "i", handler("bthandler"), 1});
        table.insertRow(new Object[]{405, "hash", "i", handler("hashhandler"), 1});
        table.insertRow(new Object[]{783, "gist", "i", handler("gisthandler"), 1});
        table.insertRow(new Object[]{2742, "gin", "i", handler("ginhandler"), 1});
        table.insertRow(new Object[]{4000, "spgist", "i", handler("spghandler"), 1});
        table.insertRow(new Object[]{3580, "brin", "i", handler("brinhandler"), 1});
        return table;
    }

    /** A regproc value: the OID pg_proc gave the function, printing as the function's name. */
    private RegprocValue handler(String procName) {
        return new RegprocValue(oids.oid("proc:" + procName), procName);
    }

    /**
     * The two tablespaces every PostgreSQL cluster has.
     *
     * <p>spcoptions is {@code text[]}, not text: it is the same {@code {key=value}} option array
     * every other catalog carries one of, and a client that read the column as an array — the
     * value already answers to unnest and to subscripting — was told by pg_attribute that it was
     * a scalar, so a typed read of it failed on the advertised OID alone.
     *
     * <p>The two OIDs are still minted from the user-object counter. PostgreSQL pins them at 1663
     * and 1664 in {@code pg_tablespace.dat}, the same number on every server ever initdb'd, and a
     * query that joins on the literal finds nothing here. Correcting it means moving
     * pg_database.dattablespace in the same step, which is CatalogSecurityBuilder's to do.
     */
    Table buildPgTablespace() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("spcname", DataType.NAME),
                colNN("spcowner", DataType.OID),
                col("spcacl", DataType.ACLITEM_ARRAY),
                arrayCol("spcoptions", DataType.TEXT_ARRAY, DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_tablespace", cols);
        table.insertRow(new Object[]{ oids.oid("tablespace:pg_default"), "pg_default", 10, null, null, 1 });
        table.insertRow(new Object[]{ oids.oid("tablespace:pg_global"), "pg_global", 10, null, null, 1 });
        return table;
    }

    Table buildPgShdescription() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.OID),
                colNN("classoid", DataType.OID),
                col("description", DataType.TEXT)
        );
        return new Table("pg_shdescription", cols); // empty, no shared descriptions
    }

    Table buildPgConversion() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("conname", DataType.NAME),
                colNN("connamespace", DataType.OID),
                col("conowner", DataType.OID),
                col("conforencoding", DataType.INTEGER),
                col("contoencoding", DataType.INTEGER),
                col("conproc", DataType.REGPROC),
                col("condefault", DataType.BOOLEAN)
        );
        return new Table("pg_conversion", cols);
    }

    Table buildPgLargeobjectMetadata() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                col("lomowner", DataType.OID),
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
                colNN("dbid", DataType.OID),
                colNN("classid", DataType.OID),
                colNN("objid", DataType.OID),
                colNN("objsubid", DataType.INTEGER),
                colNN("refclassid", DataType.OID),
                colNN("refobjid", DataType.OID),
                colNN("deptype", DataType.INTERNAL_CHAR)
        );
        return new Table("pg_shdepend", cols);
    }

    Table buildPgSeclabel(String name) {
        // A shared label is on a whole object, so pg_shseclabel carries no objsubid.
        boolean shared = name.startsWith("pg_sh");
        List<Column> cols = shared
                ? Cols.listOf(
                        colNN("objoid", DataType.OID),
                        colNN("classoid", DataType.OID),
                        col("provider", DataType.TEXT),
                        col("label", DataType.TEXT))
                : Cols.listOf(
                        colNN("objoid", DataType.OID),
                        colNN("classoid", DataType.OID),
                        colNN("objsubid", DataType.INTEGER),
                        col("provider", DataType.TEXT),
                        col("label", DataType.TEXT));
        return new Table(name, cols);
    }

    Table buildPgTransform() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("trftype", DataType.OID),
                colNN("trflang", DataType.OID),
                col("trffromsql", DataType.REGPROC),
                col("trftosql", DataType.REGPROC)
        );
        return new Table("pg_transform", cols);
    }

    /**
     * pg_statistic_ext. stxkeys is an {@code int2vector} of attribute numbers, not text: the
     * space-separated form memgres already writes is exactly int2vector's, so only the declared
     * type was wrong — and a client that subscripted the column to read the third key was told
     * it was reading a string. PostgreSQL also orders stxstattarget before stxkind.
     */
    Table buildPgStatisticExt() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("stxrelid", DataType.OID),
                colNN("stxname", DataType.NAME),
                colNN("stxnamespace", DataType.OID),
                col("stxowner", DataType.OID),
                col("stxkeys", DataType.INT2VECTOR),
                col("stxstattarget", DataType.SMALLINT),
                col("stxkind", DataType.INTERNAL_CHAR_ARRAY),
                col("stxexprs", DataType.PG_NODE_TREE)
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
                    (short) es.getStattarget(),
                    kindStr.toString(),
                    null // stxexprs: no expression statistics
            });
        }
        return table;
    }

    Table buildPgStatisticExtData() {
        List<Column> cols = Cols.listOf(
                colNN("stxoid", DataType.OID),
                colNN("stxdinherit", DataType.BOOLEAN),
                col("stxdndistinct", DataType.PG_NDISTINCT),
                col("stxddependencies", DataType.PG_DEPENDENCIES),
                col("stxdmcv", DataType.PG_MCV_LIST),
                col("stxdexpr", DataType.ANYARRAY)
        );
        Table table = new Table("pg_statistic_ext_data", cols);
        for (ExtendedStatistic es : database.getAllExtendedStatistics().values()) {
            if (es.isAnalyzed()) {
                int statOid = oids.oid("stat:" + es.getName());
                table.insertRow(new Object[]{
                        statOid, false, null, null, null, null
                });
            }
        }
        return table;
    }

    Table buildPgStats() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.NAME),
                col("tablename", DataType.NAME),
                col("attname", DataType.NAME),
                col("inherited", DataType.BOOLEAN),
                col("null_frac", DataType.REAL),
                col("avg_width", DataType.INTEGER),
                col("n_distinct", DataType.REAL),
                col("most_common_vals", DataType.ANYARRAY),
                col("most_common_freqs", DataType.FLOAT4_ARRAY),
                col("histogram_bounds", DataType.ANYARRAY),
                col("correlation", DataType.REAL),
                col("most_common_elems", DataType.ANYARRAY),
                col("most_common_elem_freqs", DataType.FLOAT4_ARRAY),
                col("elem_count_histogram", DataType.FLOAT4_ARRAY),
                col("range_length_histogram", DataType.ANYARRAY),
                col("range_empty_frac", DataType.REAL),
                col("range_bounds_histogram", DataType.ANYARRAY)
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
                        null, null, null, 0.0f, null, null, null,
                        null, null, null
                });
            }
        }
        return table;
    }

    Table buildPgStatsExt() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.NAME),
                col("tablename", DataType.NAME),
                col("statistics_schemaname", DataType.NAME),
                col("statistics_name", DataType.NAME),
                col("statistics_owner", DataType.NAME),
                col("attnames", DataType.NAME_ARRAY),
                col("exprs", DataType.TEXT_ARRAY),
                col("kinds", DataType.INTERNAL_CHAR_ARRAY),
                col("inherited", DataType.BOOLEAN),
                col("n_distinct", DataType.PG_NDISTINCT),
                col("dependencies", DataType.PG_DEPENDENCIES),
                col("most_common_vals", DataType.TEXT_ARRAY),
                col("most_common_val_nulls", DataType.BOOL_ARRAY),
                col("most_common_freqs", DataType.FLOAT8_ARRAY),
                col("most_common_base_freqs", DataType.FLOAT8_ARRAY)
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
                    "public",              // statistics_schemaname
                    es.getName(),
                    "memgres",             // statistics_owner
                    attnames.toString(),
                    null,                  // exprs
                    kinds.toString(),
                    false,                 // inherited
                    null, null,            // n_distinct, dependencies
                    null, null, null, null // most_common_*
            });
        }
        return table;
    }

    /**
     * pg_publication_rel. prattrs is the {@code int2vector} of the column list a publication was
     * given, and prqual the row filter's parsed form; both were declared text.
     */
    Table buildPgPublicationRel() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("prpubid", DataType.OID),
                colNN("prrelid", DataType.OID),
                col("prqual", DataType.PG_NODE_TREE),
                col("prattrs", DataType.INT2VECTOR)
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
                colNN("oid", DataType.OID),
                colNN("pnpubid", DataType.OID),
                colNN("pnnspid", DataType.OID)
        );
        return new Table("pg_publication_namespace", cols);
    }

    Table buildPgSubscriptionRel() {
        List<Column> cols = Cols.listOf(
                colNN("srsubid", DataType.OID),
                colNN("srrelid", DataType.OID),
                col("srsubstate", DataType.INTERNAL_CHAR),
                col("srsublsn", DataType.PG_LSN)
        );
        return new Table("pg_subscription_rel", cols);
    }

    /**
     * pg_partitioned_table. partattrs is an {@code int2vector} and partclass and partcollation are
     * {@code oidvector}s, each with one entry per partition key column — PostgreSQL's own
     * {@code pg_get_partkeydef} walks them in step with partnatts. memgres declared all three
     * text and wrote a single {@code 0} into the two oidvectors however many key columns there
     * were, so a two-column partition key described one operator class and a reader stepping
     * through the vector ran off the end.
     */
    Table buildPgPartitionedTable() {
        List<Column> cols = Cols.listOf(
                colNN("partrelid", DataType.OID),
                colNN("partstrat", DataType.INTERNAL_CHAR),
                colNN("partnatts", DataType.SMALLINT),
                col("partdefid", DataType.OID),
                col("partattrs", DataType.INT2VECTOR),
                col("partclass", DataType.OIDVECTOR),
                col("partcollation", DataType.OIDVECTOR),
                col("partexprs", DataType.PG_NODE_TREE)
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
                // One entry per key column, the way PostgreSQL writes them. The operator class
                // is still reported as 0 — memgres does not record which one a partition key was
                // resolved through — but the vector is now the length the rest of the row says
                // it is.
                StringBuilder zeros = new StringBuilder();
                for (int ci = 0; ci < partnatts; ci++) {
                    if (ci > 0) zeros.append(' ');
                    zeros.append('0');
                }
                table.insertRow(new Object[]{
                        tblOid, strategy, partnatts, defOid,
                        partattrs, zeros.toString(), zeros.toString(), null
                });
            }
        }
        return table;
    }

    Table buildPgInherits() {
        List<Column> cols = Cols.listOf(
                colNN("inhrelid", DataType.OID),
                colNN("inhparent", DataType.OID),
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
            int childOid = oids.oid("rel:" + childSchema + "." + Database.idxName(childIdx));
            int parentOid = oids.oid("rel:" + parentSchema + "." + Database.idxName(parentIdx));
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

    /**
     * pg_event_trigger. evttags is the list of command tags the trigger is restricted to, and
     * PostgreSQL types it {@code text[]} — the value memgres writes is already an array literal
     * that answers to array_length and unnest, so only the column's declared type was wrong, and
     * a client reading it as an array was told it was a scalar.
     */
    Table buildPgEventTrigger() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("evtname", DataType.NAME),
                col("evtevent", DataType.NAME), col("evtowner", DataType.OID),
                col("evtfoid", DataType.OID), col("evtenabled", DataType.INTERNAL_CHAR),
                arrayCol("evttags", DataType.TEXT_ARRAY, DataType.TEXT), col("xmin", DataType.INTEGER));
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

    /**
     * pg_foreign_data_wrapper. The four option columns of the foreign-data catalogs — fdwoptions,
     * srvoptions, umoptions, ftoptions — are all {@code text[]} in PostgreSQL, holding the
     * {@code {key=value}} pairs the OPTIONS clause wrote. memgres declared each of them text
     * while storing exactly that array literal, so {@code unnest(fdwoptions)} worked and
     * {@code getArray} on the column did not.
     */
    Table buildPgForeignDataWrapper() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("fdwname", DataType.NAME),
                col("fdwowner", DataType.OID), col("fdwhandler", DataType.OID),
                col("fdwvalidator", DataType.OID), col("fdwacl", DataType.ACLITEM_ARRAY),
                arrayCol("fdwoptions", DataType.TEXT_ARRAY, DataType.TEXT), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_foreign_data_wrapper", cols);
        for (Database.FdwWrapper w : database.getForeignDataWrappers().values()) {
            table.insertRow(new Object[]{
                    oids.oid("fdw:" + w.name), w.name, 10, 0, 0, null, w.options, 1
            });
        }
        return table;
    }

    /**
     * pg_foreign_server. PostgreSQL orders the columns srvacl then srvoptions; memgres had them
     * the other way round, so attnum, ordinal_position and {@code SELECT *} all disagreed with
     * PostgreSQL and anything reading the row positionally read the ACL out of the options.
     */
    Table buildPgForeignServer() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("srvname", DataType.NAME),
                col("srvowner", DataType.OID), col("srvfdw", DataType.OID),
                col("srvtype", DataType.TEXT), col("srvversion", DataType.TEXT),
                col("srvacl", DataType.ACLITEM_ARRAY), arrayCol("srvoptions", DataType.TEXT_ARRAY, DataType.TEXT),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_foreign_server", cols);
        for (Database.FdwServer s : database.getForeignServers().values()) {
            int fdwOid = oids.oid("fdw:" + s.fdwName);
            table.insertRow(new Object[]{
                    oids.oid("srv:" + s.name), s.name, 10, fdwOid, null, null, null, s.options, 1
            });
        }
        return table;
    }

    Table buildPgUserMapping() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), col("umuser", DataType.OID),
                col("umserver", DataType.OID), arrayCol("umoptions", DataType.TEXT_ARRAY, DataType.TEXT),
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
                colNN("umid", DataType.OID),
                col("srvid", DataType.OID),
                col("srvname", DataType.NAME),
                col("umuser", DataType.OID),
                col("usename", DataType.NAME),
                arrayCol("umoptions", DataType.TEXT_ARRAY, DataType.TEXT)
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
                colNN("ftrelid", DataType.OID), col("ftserver", DataType.OID),
                arrayCol("ftoptions", DataType.TEXT_ARRAY, DataType.TEXT), col("xmin", DataType.INTEGER));
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
                colNN("objoid", DataType.OID),
                colNN("classoid", DataType.OID),
                colNN("objsubid", DataType.INTEGER),
                col("objtype", DataType.TEXT),
                col("objnamespace", DataType.OID),
                col("objname", DataType.TEXT),
                col("provider", DataType.TEXT),
                col("label", DataType.TEXT)
        );
        return new Table("pg_seclabels", cols);
    }

    Table buildPgInitPrivs() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.OID),
                colNN("classoid", DataType.OID),
                colNN("objsubid", DataType.INTEGER),
                col("privtype", DataType.INTERNAL_CHAR),
                col("initprivs", DataType.ACLITEM_ARRAY)
        );
        return new Table("pg_init_privs", cols); // empty, no initial privileges to track
    }

    Table buildPgPreparedXacts() {
        List<Column> cols = Cols.listOf(
                col("transaction", DataType.XID),
                col("gid", DataType.TEXT),
                col("prepared", DataType.TIMESTAMPTZ),
                col("owner", DataType.NAME),
                col("database", DataType.NAME)
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
                // Both are regtype[]: they hold types, and a reader that asks pg_typeof gets that.
                col("parameter_types", DataType.REGTYPE_ARRAY),
                col("result_types", DataType.REGTYPE_ARRAY),
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

    /**
     * A list of type names as regtype[] holds them: under the name a reader would write.
     *
     * <p>The declared spelling was kept verbatim, so a parameter written {@code int} was reported
     * as "int" where PostgreSQL reports "integer" — regtype prints a type's own name, not the
     * alias the statement happened to use.
     */
    private List<Object> toRegTypeList(java.util.List<String> types) {
        if (types == null || types.isEmpty()) return new ArrayList<>();
        List<Object> named = new ArrayList<>();
        for (String type : types) {
            named.add(type == null ? null : canonicalTypeName(type));
        }
        return named;
    }

    /** The name regtype prints for a type written as {@code written}. */
    private static String canonicalTypeName(String written) {
        String bare = written.trim();
        String suffix = "";
        while (bare.endsWith("[]")) {
            bare = bare.substring(0, bare.length() - 2).trim();
            suffix = suffix + "[]";
        }
        DataType type = DataType.fromPgName(bare.toLowerCase());
        if (type == null) return written;
        String name = type.getPgName();
        // The catalogue spellings are what pg_type holds; regtype prints the readable name.
        if (name.startsWith("_")) return written;
        switch (type) {
            case INTEGER: return "integer" + suffix;
            case SMALLINT: return "smallint" + suffix;
            case BIGINT: return "bigint" + suffix;
            case REAL: return "real" + suffix;
            case DOUBLE_PRECISION: return "double precision" + suffix;
            case BOOLEAN: return "boolean" + suffix;
            case CHAR: return "character" + suffix;
            case VARCHAR: return "character varying" + suffix;
            case TIMESTAMP: return "timestamp without time zone" + suffix;
            case TIMESTAMPTZ: return "timestamp with time zone" + suffix;
            case TIME: return "time without time zone" + suffix;
            case TIMETZ: return "time with time zone" + suffix;
            default: return name + suffix;
        }
    }

    // formatParamTypes and formatResultTypes removed — arrays now stored as List<Object> directly

    Table buildPgAvailableExtensions() {
        List<Column> cols = Cols.listOf(
                col("name", DataType.NAME),
                col("default_version", DataType.TEXT),
                col("installed_version", DataType.TEXT),
                col("comment", DataType.TEXT)
        );
        Table table = new Table("pg_available_extensions", cols);
        table.insertRow(new Object[]{"plpgsql", "1.0", "1.0", "PL/pgSQL procedural language"});
        return table;
    }

    /**
     * pg_available_extension_versions. {@code requires} lists the extensions a version depends
     * on, one identifier per element, and PostgreSQL types it {@code name[]}.
     */
    Table buildPgAvailableExtensionVersions() {
        List<Column> cols = Cols.listOf(
                col("name", DataType.NAME),
                col("version", DataType.TEXT),
                col("installed", DataType.BOOLEAN),
                col("superuser", DataType.BOOLEAN),
                col("trusted", DataType.BOOLEAN),
                col("relocatable", DataType.BOOLEAN),
                col("schema", DataType.NAME),
                arrayCol("requires", DataType.NAME_ARRAY, DataType.NAME),
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

    /**
     * pg_hba_file_rules. A host-based-authentication line names a list of databases and a list of
     * roles, so PostgreSQL types database, user_name and options {@code text[]} and writes
     * {@code {all}} rather than {@code all} — a rule for two databases is two array elements and
     * cannot be told from one database named "a,b" once it is flattened to a string. memgres
     * declared all three text and wrote the bare word, so a client that read the column as an
     * array got nothing to unnest.
     */
    Table buildPgHbaFileRules() {
        List<Column> cols = Cols.listOf(
                col("rule_number", DataType.INTEGER),
                col("file_name", DataType.TEXT),
                col("line_number", DataType.INTEGER),
                col("type", DataType.TEXT),
                arrayCol("database", DataType.TEXT_ARRAY, DataType.TEXT),
                arrayCol("user_name", DataType.TEXT_ARRAY, DataType.TEXT),
                col("address", DataType.TEXT),
                col("netmask", DataType.TEXT),
                col("auth_method", DataType.TEXT),
                arrayCol("options", DataType.TEXT_ARRAY, DataType.TEXT),
                col("error", DataType.TEXT)
        );
        Table table = new Table("pg_hba_file_rules", cols);
        table.insertRow(new Object[]{1, null, 1, "host", "{all}", "{all}", "0.0.0.0/0", null,
                "trust", null, null});
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
                colNN("oid", DataType.OID),
                col("pubname", DataType.NAME),
                col("pubowner", DataType.OID),
                col("puballtables", DataType.BOOLEAN),
                col("pubinsert", DataType.BOOLEAN),
                col("pubupdate", DataType.BOOLEAN),
                col("pubdelete", DataType.BOOLEAN),
                col("pubtruncate", DataType.BOOLEAN),
                col("pubviaroot", DataType.BOOLEAN),
                col("pubgencols", DataType.INTERNAL_CHAR)
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
                colNN("oid", DataType.OID),
                col("subdbid", DataType.OID),
                col("subskiplsn", DataType.PG_LSN),
                col("subname", DataType.NAME),
                col("subowner", DataType.OID),
                col("subenabled", DataType.BOOLEAN),
                col("subbinary", DataType.BOOLEAN),
                col("substream", DataType.INTERNAL_CHAR),
                col("subtwophasestate", DataType.INTERNAL_CHAR),
                col("subdisableonerr", DataType.BOOLEAN),
                col("subpasswordrequired", DataType.BOOLEAN),
                col("subrunasowner", DataType.BOOLEAN),
                col("subfailover", DataType.BOOLEAN),
                col("subconninfo", DataType.TEXT),
                col("subslotname", DataType.NAME),
                col("subsynccommit", DataType.TEXT),
                // The publications a subscription follows are a text[], and the value written
                // here has always been the array literal for them.
                arrayCol("subpublications", DataType.TEXT_ARRAY, DataType.TEXT),
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
        // The view PostgreSQL publishes: which columns are replicated and under what filter,
        // keyed by publication name. There is no pubid column to join on.
        List<Column> cols = Cols.listOf(
                col("pubname", DataType.NAME),
                col("schemaname", DataType.NAME),
                col("tablename", DataType.NAME),
                col("attnames", DataType.NAME_ARRAY),
                col("rowfilter", DataType.TEXT)
        );
        Table table = new Table("pg_publication_tables", cols);
        for (Database.PubDef pub : database.getPublications().values()) {
            for (String tblName : pub.tables) {
                StringBuilder attnames = new StringBuilder("{");
                Table src = null;
                for (Schema sch : database.getSchemas().values()) {
                    src = sch.getTable(tblName);
                    if (src != null) break;
                }
                if (src != null) {
                    for (int i = 0; i < src.getColumns().size(); i++) {
                        if (i > 0) attnames.append(",");
                        attnames.append(src.getColumns().get(i).getName());
                    }
                }
                attnames.append("}");
                table.insertRow(new Object[]{ pub.name, "public", tblName, attnames.toString(), null });
            }
        }
        return table;
    }

    Table buildPgReplicationSlots() {
        List<Column> cols = Cols.listOf(
                col("slot_name", DataType.NAME),
                col("plugin", DataType.NAME),
                col("slot_type", DataType.TEXT),
                col("datoid", DataType.OID),
                col("database", DataType.NAME),
                col("temporary", DataType.BOOLEAN),
                col("active", DataType.BOOLEAN),
                col("active_pid", DataType.INTEGER),
                col("xmin", DataType.XID),
                col("catalog_xmin", DataType.XID),
                col("restart_lsn", DataType.PG_LSN),
                col("confirmed_flush_lsn", DataType.PG_LSN),
                col("wal_status", DataType.TEXT),
                col("safe_wal_size", DataType.BIGINT),
                col("two_phase", DataType.BOOLEAN),
                col("two_phase_at", DataType.PG_LSN),
                col("inactive_since", DataType.TIMESTAMPTZ),
                col("conflicting", DataType.BOOLEAN),
                col("invalidation_reason", DataType.TEXT),
                col("failover", DataType.BOOLEAN),
                col("synced", DataType.BOOLEAN)
        );
        Table table = new Table("pg_replication_slots", cols);
        for (Database.ReplicationSlot slot : database.getReplicationSlots().values()) {
            table.insertRow(new Object[]{
                    slot.slotName, slot.plugin, slot.slotType,
                    oids.oid("db:memgres"), "memgres",
                    false, false, null, null, null,
                    "0/0", "0/0", "reserved", null, false,
                    null, null, false, null, false, false
            });
        }
        return table;
    }

    Table buildPgReplicationOrigin() {
        List<Column> cols = Cols.listOf(
                colNN("roident", DataType.OID),
                col("roname", DataType.TEXT)
        );
        return new Table("pg_replication_origin", cols);
    }

    Table buildPgReplicationOriginStatus() {
        List<Column> cols = Cols.listOf(
                col("local_id", DataType.OID),
                col("external_id", DataType.TEXT),
                col("remote_lsn", DataType.PG_LSN),
                col("local_lsn", DataType.PG_LSN)
        );
        return new Table("pg_replication_origin_status", cols);
    }

    Table buildPgStatSubscriptionStats() {
        List<Column> cols = Cols.listOf(
                col("subid", DataType.OID),
                col("subname", DataType.NAME),
                col("apply_error_count", DataType.BIGINT),
                col("sync_error_count", DataType.BIGINT),
                // PG 18 counts each kind of apply conflict separately.
                col("confl_insert_exists", DataType.BIGINT),
                col("confl_update_origin_differs", DataType.BIGINT),
                col("confl_update_exists", DataType.BIGINT),
                col("confl_update_missing", DataType.BIGINT),
                col("confl_delete_origin_differs", DataType.BIGINT),
                col("confl_delete_missing", DataType.BIGINT),
                col("confl_multiple_unique_conflicts", DataType.BIGINT),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        return new Table("pg_stat_subscription_stats", cols);
    }

    Table buildPgMatviews() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.NAME),
                col("matviewname", DataType.NAME),
                col("matviewowner", DataType.NAME),
                col("tablespace", DataType.NAME),
                col("hasindexes", DataType.BOOLEAN),
                col("ispopulated", DataType.BOOLEAN),
                col("definition", DataType.TEXT)
        );
        Table table = new Table("pg_matviews", cols);
        for (Database.ViewDef vd : database.getViews().values()) {
            if (!vd.materialized()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            String owner = database.getObjectOwner("view:" + vSchema + "." + vd.name());
            if (owner == null) owner = "memgres";
            String definition = null;
            if (vd.query() != null) {
                definition = vd.sourceSQL() != null ? vd.sourceSQL() : SqlUnparser.toSql(vd.query());
                if (definition != null) definition = definition + ";";
            }
            table.insertRow(new Object[]{
                    vSchema, vd.name(), owner, null, false, vd.populated(), definition
            });
        }
        return table;
    }

    Table buildPgRulesView() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.NAME),
                col("tablename", DataType.NAME),
                col("rulename", DataType.NAME),
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
                col("dbid", DataType.OID),
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
                col("read_bytes", DataType.NUMERIC),
                col("read_time", DataType.DOUBLE_PRECISION),
                col("writes", DataType.BIGINT),
                col("write_bytes", DataType.NUMERIC),
                col("write_time", DataType.DOUBLE_PRECISION),
                col("writebacks", DataType.BIGINT),
                col("writeback_time", DataType.DOUBLE_PRECISION),
                col("extends", DataType.BIGINT),
                col("extend_bytes", DataType.NUMERIC),
                col("extend_time", DataType.DOUBLE_PRECISION),
                col("hits", DataType.BIGINT),
                col("evictions", DataType.BIGINT),
                col("reuses", DataType.BIGINT),
                col("fsyncs", DataType.BIGINT),
                col("fsync_time", DataType.DOUBLE_PRECISION),
                col("stats_reset", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_stat_io", cols);
        // The view is one row per way a backend can read or write, whether or not it has: a
        // monitoring query reads the counters off the rows it expects to find, and finding none
        // at all is not the same as finding them at zero. The combinations are PostgreSQL's own,
        // read off the reference server; memgres does none of this I/O, so every counter is 0.
        List<Object[]> rows = new ArrayList<>();
        for (String combination : IO_COMBINATIONS) {
            String[] parts = combination.split("\\|");
            Object[] row = new Object[cols.size()];
            row[0] = parts[0];
            row[1] = parts[1];
            row[2] = parts[2];
            for (int i = 3; i < cols.size() - 1; i++) {
                DataType type = cols.get(i).getType();
                if (type == DataType.NUMERIC) row[i] = java.math.BigDecimal.ZERO;
                else if (type == DataType.DOUBLE_PRECISION) row[i] = Double.valueOf(0);
                else row[i] = Long.valueOf(0);
            }
            row[cols.size() - 1] = null;   // stats_reset: never reset, because never counted
            rows.add(row);
        }
        table.replaceAllRows(rows);
        return table;
    }

    /**
     * Every backend type, object and context PostgreSQL 18 reports I/O for, as it reports them.
     *
     * <p>Not every combination exists — a walwriter only touches the WAL, and only the backends
     * that can run one report a vacuum context — so the list is the reference server's own rather
     * than the product of the three columns.
     */
    private static final String[] IO_COMBINATIONS = {
        "autovacuum launcher|relation|bulkread",
        "autovacuum launcher|relation|init",
        "autovacuum launcher|relation|normal",
        "autovacuum launcher|wal|init",
        "autovacuum launcher|wal|normal",
        "autovacuum worker|relation|bulkread",
        "autovacuum worker|relation|init",
        "autovacuum worker|relation|normal",
        "autovacuum worker|relation|vacuum",
        "autovacuum worker|wal|init",
        "autovacuum worker|wal|normal",
        "background worker|relation|bulkread",
        "background worker|relation|bulkwrite",
        "background worker|relation|init",
        "background worker|relation|normal",
        "background worker|relation|vacuum",
        "background worker|temp relation|normal",
        "background worker|wal|init",
        "background worker|wal|normal",
        "background writer|relation|init",
        "background writer|relation|normal",
        "background writer|wal|init",
        "background writer|wal|normal",
        "checkpointer|relation|init",
        "checkpointer|relation|normal",
        "checkpointer|wal|init",
        "checkpointer|wal|normal",
        "client backend|relation|bulkread",
        "client backend|relation|bulkwrite",
        "client backend|relation|init",
        "client backend|relation|normal",
        "client backend|relation|vacuum",
        "client backend|temp relation|normal",
        "client backend|wal|init",
        "client backend|wal|normal",
        "io worker|relation|bulkread",
        "io worker|relation|bulkwrite",
        "io worker|relation|init",
        "io worker|relation|normal",
        "io worker|relation|vacuum",
        "io worker|temp relation|normal",
        "io worker|wal|init",
        "io worker|wal|normal",
        "slotsync worker|relation|bulkread",
        "slotsync worker|relation|bulkwrite",
        "slotsync worker|relation|init",
        "slotsync worker|relation|normal",
        "slotsync worker|relation|vacuum",
        "slotsync worker|temp relation|normal",
        "slotsync worker|wal|init",
        "slotsync worker|wal|normal",
        "standalone backend|relation|bulkread",
        "standalone backend|relation|bulkwrite",
        "standalone backend|relation|init",
        "standalone backend|relation|normal",
        "standalone backend|relation|vacuum",
        "standalone backend|wal|init",
        "standalone backend|wal|normal",
        "startup|relation|bulkread",
        "startup|relation|bulkwrite",
        "startup|relation|init",
        "startup|relation|normal",
        "startup|relation|vacuum",
        "startup|wal|init",
        "startup|wal|normal",
        "walreceiver|wal|init",
        "walreceiver|wal|normal",
        "walsender|relation|bulkread",
        "walsender|relation|bulkwrite",
        "walsender|relation|init",
        "walsender|relation|normal",
        "walsender|relation|vacuum",
        "walsender|temp relation|normal",
        "walsender|wal|init",
        "walsender|wal|normal",
        "walsummarizer|wal|init",
        "walsummarizer|wal|normal",
        "walwriter|wal|init",
        "walwriter|wal|normal",
    };

    /**
     * {@code pg_aios}, PostgreSQL 18's view of the asynchronous I/O in flight.
     *
     * <p>It is empty on a server that has none outstanding, which is almost always and is always
     * true of memgres. What matters is that it is there to be selected from: a query against a
     * view that does not exist fails, where one against an empty view answers nothing.
     */
    Table buildPgAios() {
        List<Column> cols = Cols.listOf(
                col("pid", DataType.INTEGER),
                col("io_id", DataType.INTEGER),
                col("io_generation", DataType.BIGINT),
                col("state", DataType.TEXT),
                col("operation", DataType.TEXT),
                col("off", DataType.BIGINT),
                col("length", DataType.BIGINT),
                col("target", DataType.TEXT),
                col("handle_data_len", DataType.SMALLINT),
                col("raw_result", DataType.INTEGER),
                col("result", DataType.TEXT),
                col("target_desc", DataType.TEXT),
                col("f_sync", DataType.BOOLEAN),
                col("f_localmem", DataType.BOOLEAN),
                col("f_buffered", DataType.BOOLEAN)
        );
        return new Table("pg_aios", cols);   // nothing is ever in flight
    }

    Table buildPgStatUserFunctions() {
        List<Column> cols = Cols.listOf(
                col("funcid", DataType.INTEGER),
                col("schemaname", DataType.NAME),
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
                col("oid", DataType.OID),
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
                col("sent_lsn", DataType.PG_LSN),
                col("write_lsn", DataType.PG_LSN),
                col("flush_lsn", DataType.PG_LSN),
                col("replay_lsn", DataType.PG_LSN),
                col("write_lag", DataType.INTERVAL),
                col("flush_lag", DataType.INTERVAL),
                col("replay_lag", DataType.INTERVAL),
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
                col("pg_username", DataType.TEXT),
                col("error", DataType.TEXT)
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
