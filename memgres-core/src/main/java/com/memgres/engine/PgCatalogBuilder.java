package com.memgres.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds pg_catalog virtual tables from the current database metadata.
 * Delegates to specialized builder classes for each category of catalog table.
 */
public class PgCatalogBuilder {

    private final CatalogCoreBuilder core;
    private final CatalogConstraintBuilder constraints;
    private final CatalogSecurityBuilder security;
    private final CatalogTypeSystemBuilder typeSystem;
    private final CatalogStubBuilder stubs;

    /**
     * The columns each pg_catalog relation has, keyed by relation name. Built once by asking
     * every builder for its table and keeping only the shape, so pg_attribute describes what the
     * server actually answers rather than a second list that can drift from it.
     */
    private static volatile Map<String, List<Column>> shapes;

    /** True while {@link #catalogShapes()} is running, so pg_attribute does not recurse into itself. */
    private static final ThreadLocal<Boolean> COMPUTING_SHAPES = new ThreadLocal<Boolean>();

    public PgCatalogBuilder(Database database, OidSupplier oids) {
        this.core = new CatalogCoreBuilder(database, oids);
        this.constraints = new CatalogConstraintBuilder(database, oids);
        this.security = new CatalogSecurityBuilder(database, oids);
        this.typeSystem = new CatalogTypeSystemBuilder(database, oids);
        this.stubs = new CatalogStubBuilder(database, oids);
    }

    /** True when the catalog shapes are being collected; see {@link #COMPUTING_SHAPES}. */
    static boolean collectingShapes() {
        return Boolean.TRUE.equals(COMPUTING_SHAPES.get());
    }

    /**
     * The column list of every pg_catalog relation memgres publishes. A relation with no builder
     * behind it is absent from the map, which is how pg_class learns not to advertise it.
     */
    Map<String, List<Column>> catalogShapes() {
        Map<String, List<Column>> cached = shapes;
        if (cached != null) return cached;
        // Re-entered from a builder that is only being asked for its columns: it has no need of
        // the shapes to answer that, and handing it a half-built map would recurse forever.
        if (collectingShapes()) return Collections.emptyMap();
        Map<String, List<Column>> built = new LinkedHashMap<String, List<Column>>();
        COMPUTING_SHAPES.set(Boolean.TRUE);
        try {
            for (String name : PgCatalogRelations.ALL) {
                try {
                    Table t = build(name, null);
                    if (t != null && !t.getColumns().isEmpty()) {
                        built.put(name, t.getColumns());
                    }
                } catch (RuntimeException ignored) {
                    // A relation that cannot be built is one pg_class must not advertise.
                }
            }
        } finally {
            COMPUTING_SHAPES.remove();
        }
        cached = Collections.unmodifiableMap(built);
        shapes = cached;
        return cached;
    }

    /**
     * Build the requested pg_catalog table by name.
     * Returns an empty table for unrecognized names.
     */
    public Table build(String tableName) {
        return build(tableName, null);
    }

    /**
     * Build the requested pg_catalog table by name, with session context for session-scoped views.
     */
    public Table build(String tableName, Session session) {
        return inPgColumnOrder(tableName, withoutRowHeaderColumns(buildRelation(tableName, session)));
    }

    /**
     * The relation with its columns in the order PostgreSQL numbers them, where the two differ.
     * See {@link PgCatalogRelations#attnumOrder}. A column memgres has that PostgreSQL does not
     * keeps its place at the end rather than being dropped.
     */
    private static Table inPgColumnOrder(String tableName, Table t) {
        String[] order = PgCatalogRelations.attnumOrder(tableName);
        if (t == null || order == null) return t;
        List<Column> have = t.getColumns();
        List<Integer> at = new java.util.ArrayList<Integer>();
        boolean[] taken = new boolean[have.size()];
        for (String name : order) {
            for (int i = 0; i < have.size(); i++) {
                if (!taken[i] && have.get(i).getName().equalsIgnoreCase(name)) {
                    at.add(i);
                    taken[i] = true;
                    break;
                }
            }
        }
        for (int i = 0; i < have.size(); i++) {
            if (!taken[i]) at.add(i);
        }
        boolean unchanged = true;
        for (int i = 0; i < at.size(); i++) {
            if (at.get(i) != i) { unchanged = false; break; }
        }
        if (unchanged) return t;
        List<Column> reordered = new java.util.ArrayList<Column>();
        for (Integer i : at) reordered.add(have.get(i));
        Table out = new Table(t.getName(), reordered);
        for (Object[] row : t.getRows()) {
            Object[] moved = new Object[at.size()];
            for (int i = 0; i < at.size(); i++) {
                int from = at.get(i);
                moved[i] = from < row.length ? row[from] : null;
            }
            out.insertRow(moved);
        }
        return out;
    }

    /**
     * The relation without the row-header columns memgres carries alongside it.
     *
     * <p>PostgreSQL answers {@code SELECT xmin FROM pg_class} but does not include xmin in
     * {@code SELECT *}, and keeps it out of pg_attribute — it is a system column, not one of the
     * relation's own. Carrying it in the column list made the relation return one more column
     * than the catalog says it has, so what pg_class and pg_attribute describe and what selecting
     * from the relation returns disagreed about twenty-seven relations. The value is still
     * reachable by name: RowContext resolves the row-header columns before it looks at the
     * relation's own.
     */
    private static Table withoutRowHeaderColumns(Table t) {
        if (t == null) return null;
        List<Column> kept = new java.util.ArrayList<Column>();
        List<Integer> keptAt = new java.util.ArrayList<Integer>();
        List<Column> all = t.getColumns();
        for (int i = 0; i < all.size(); i++) {
            if (CatalogCoreBuilder.isSystemColumn(all.get(i))) continue;
            kept.add(all.get(i));
            keptAt.add(i);
        }
        if (kept.size() == all.size()) return t;
        Table stripped = new Table(t.getName(), kept);
        for (Object[] row : t.getRows()) {
            Object[] out = new Object[kept.size()];
            for (int i = 0; i < keptAt.size(); i++) {
                int at = keptAt.get(i);
                out[i] = at < row.length ? row[at] : null;
            }
            stripped.insertRow(out);
        }
        return stripped;
    }

    private Table buildRelation(String tableName, Session session) {
        switch (tableName) {
            case "pg_class":
                return core.buildPgClass();
            case "pg_attribute":
                return core.buildPgAttribute();
            case "pg_type":
                return core.buildPgType();
            case "pg_namespace":
                return core.buildPgNamespace();
            case "pg_enum":
                return core.buildPgEnum();
            case "pg_proc":
                return core.buildPgProc();
            case "pg_constraint":
                return constraints.buildPgConstraint();
            case "pg_index":
                return constraints.buildPgIndex();
            case "pg_attrdef":
                return constraints.buildPgAttrdef();
            case "pg_depend":
                return constraints.buildPgDepend();
            case "pg_rewrite":
                return constraints.buildPgRewrite();
            case "pg_description":
                return constraints.buildPgDescription();
            case "pg_trigger":
                return constraints.buildPgTrigger();
            case "pg_roles":
                return security.buildPgRoles();
            case "pg_authid":
                return security.buildPgAuthid();
            case "pg_user":
                return security.buildPgUser();
            case "pg_shadow":
                return security.buildLoginRoleView("pg_shadow", "usename", "usesysid");
            case "pg_group":
                return security.buildPgGroup();
            case "pg_auth_members":
                return security.buildPgAuthMembers();
            case "pg_default_acl":
                return security.buildPgDefaultAcl();
            case "pg_policy":
                return security.buildPgPolicy();
            case "pg_policies":
                return security.buildPgPolicies();
            case "pg_stat_activity":
                return security.buildPgStatActivity();
            case "pg_locks":
                return security.buildPgLocks();
            case "pg_database":
                return security.buildPgDatabase();
            case "pg_settings":
                return security.buildPgSettings(session != null ? session.getGucSettings() : null);
            case "pg_cast":
                return typeSystem.buildPgCast();
            case "pg_operator":
                return typeSystem.buildPgOperator();
            case "pg_opclass":
                return typeSystem.buildPgOpclass();
            case "pg_opfamily":
                return typeSystem.buildPgOpfamily();
            case "pg_aggregate":
                return typeSystem.buildPgAggregate();
            case "pg_amop":
                return typeSystem.buildPgAmop();
            case "pg_amproc":
                return typeSystem.buildPgAmproc();
            case "pg_language":
                return typeSystem.buildPgLanguage();
            case "pg_extension":
                return typeSystem.buildPgExtension();
            case "pg_collation":
                return typeSystem.buildPgCollation();
            case "pg_range":
                return typeSystem.buildPgRange();
            case "pg_tables":
                return stubs.buildPgTables();
            case "pg_views":
                return stubs.buildPgViews();
            case "pg_indexes":
                return stubs.buildPgIndexes();
            case "pg_sequence":
                return stubs.buildPgSequence();
            case "pg_sequences":
                return stubs.buildPgSequences();
            case "pg_am":
                return stubs.buildPgAm();
            case "pg_tablespace":
                return stubs.buildPgTablespace();
            case "pg_shdescription":
                return stubs.buildPgShdescription();
            case "pg_inherits":
                return stubs.buildPgInherits();
            case "pg_event_trigger":
                return stubs.buildPgEventTriggerPopulated();
            case "pg_foreign_data_wrapper":
                return stubs.buildPgForeignDataWrapper();
            case "pg_foreign_server":
                return stubs.buildPgForeignServer();
            case "pg_user_mapping":
                return stubs.buildPgUserMapping();
            case "pg_user_mappings":
                return stubs.buildPgUserMappings();
            case "pg_foreign_table":
                return stubs.buildPgForeignTable();
            case "pg_timezone_names":
                return stubs.buildPgTimezoneNames();
            case "pg_timezone_abbrevs":
                return stubs.buildPgTimezoneAbbrevs();
            case "pg_stat_user_tables":
            case "pg_stat_all_tables":
                return stubs.buildPgStatUserTables();
            case "pg_stat_sys_tables": {
                Table t = stubs.buildPgStatUserTables();
                return new Table(tableName, t.getColumns());
            }
            case "pg_stat_xact_user_tables":
            case "pg_stat_xact_all_tables":
            case "pg_stat_xact_sys_tables":
                return stubs.buildPgStatXactTables(tableName);
            case "pg_stat_user_indexes":
            case "pg_stat_all_indexes":
                return stubs.buildPgStatUserIndexes();
            case "pg_stat_sys_indexes": {
                Table t = stubs.buildPgStatUserIndexes();
                return new Table(tableName, t.getColumns());
            }
            case "pg_statio_all_indexes":
            case "pg_statio_user_indexes":
            case "pg_statio_sys_indexes":
                return stubs.buildPgStatioIndexes(tableName);
            case "pg_statio_all_sequences":
            case "pg_statio_user_sequences":
            case "pg_statio_sys_sequences":
                return stubs.buildPgStatioSequences(tableName);
            case "pg_statio_sys_tables": {
                Table t = stubs.buildPgStatioUserTables();
                return new Table(tableName, t.getColumns());
            }
            case "pg_stat_progress_analyze":
                return stubs.buildPgStatProgressAnalyze();
            case "pg_stat_progress_cluster":
                return stubs.buildPgStatProgressCluster();
            case "pg_stat_progress_basebackup":
                return stubs.buildPgStatProgressBasebackup();
            case "pg_stat_progress_copy":
                return stubs.buildPgStatProgressCopy();
            case "pg_backend_memory_contexts":
                return stubs.buildPgBackendMemoryContexts();
            case "pg_stat_database":
                return stubs.buildPgStatDatabase();
            case "pg_stat_bgwriter":
                return stubs.buildPgStatBgwriter();
            case "pg_stat_checkpointer":
                return stubs.buildPgStatCheckpointer();
            case "pg_stat_wal":
                return stubs.buildPgStatWal();
            case "pg_stat_replication":
                return stubs.buildPgStatReplication();
            case "pg_stat_subscription":
                return stubs.buildPgStatSubscription();
            case "pg_stat_progress_vacuum":
                return stubs.buildPgStatProgressVacuum();
            case "pg_stat_progress_create_index":
                return stubs.buildPgStatProgressCreateIndex();
            case "pg_stat_wal_receiver":
                return stubs.buildPgStatWalReceiver();
            case "pg_stat_ssl":
                return stubs.buildPgStatSsl();
            case "pg_stat_gssapi":
                return stubs.buildPgStatGssapi();
            case "pg_statio_user_tables":
            case "pg_statio_all_tables":
                return stubs.buildPgStatioUserTables();
            case "pg_prepared_xacts":
                return stubs.buildPgPreparedXacts();
            case "pg_cursors":
                return stubs.buildPgCursors(session);
            case "pg_prepared_statements":
                return stubs.buildPgPreparedStatements(session);
            case "pg_available_extensions":
                return stubs.buildPgAvailableExtensions();
            case "pg_available_extension_versions":
                return stubs.buildPgAvailableExtensionVersions();
            case "pg_config":
                return stubs.buildPgConfig();
            case "pg_file_settings":
                return stubs.buildPgFileSettings();
            case "pg_hba_file_rules":
                return stubs.buildPgHbaFileRules();
            case "pg_shmem_allocations":
                return stubs.buildPgShmemAllocations();
            case "pg_publication":
                return stubs.buildPgPublication();
            case "pg_subscription":
                return stubs.buildPgSubscription();
            case "pg_matviews":
                return stubs.buildPgMatviews();
            case "pg_rules":
                return stubs.buildPgRulesView();
            case "pg_seclabels":
                return stubs.buildPgSeclabels();
            case "pg_init_privs":
                return stubs.buildPgInitPrivs();
            case "pg_ts_parser":
                return stubs.buildPgTsParser();
            case "pg_ts_dict":
                return stubs.buildPgTsDict();
            case "pg_ts_template":
                return stubs.buildPgTsTemplate();
            case "pg_ts_config":
                return stubs.buildPgTsConfig();
            case "pg_ts_config_map":
                return stubs.buildPgTsConfigMap();
            case "pg_conversion":
                return stubs.buildPgConversion();
            case "pg_largeobject_metadata":
                return stubs.buildPgLargeobjectMetadata();
            case "pg_shdepend":
                return stubs.buildPgShdepend();
            case "pg_seclabel":
            case "pg_shseclabel":
                return stubs.buildPgSeclabel(tableName);
            case "pg_transform":
                return stubs.buildPgTransform();
            case "pg_statistic":
                return stubs.buildPgStatistic();
            case "pg_statistic_ext":
                return stubs.buildPgStatisticExt();
            case "pg_statistic_ext_data":
                return stubs.buildPgStatisticExtData();
            case "pg_stats":
                return stubs.buildPgStats();
            case "pg_stats_ext":
                return stubs.buildPgStatsExt();
            case "pg_publication_rel":
                return stubs.buildPgPublicationRel();
            case "pg_publication_tables":
                return stubs.buildPgPublicationTables();
            case "pg_publication_namespace":
                return stubs.buildPgPublicationNamespace();
            case "pg_subscription_rel":
                return stubs.buildPgSubscriptionRel();
            case "pg_replication_slots":
                return stubs.buildPgReplicationSlots();
            case "pg_replication_origin":
                return stubs.buildPgReplicationOrigin();
            case "pg_replication_origin_status":
                return stubs.buildPgReplicationOriginStatus();
            case "pg_stat_subscription_stats":
                return stubs.buildPgStatSubscriptionStats();
            case "pg_partitioned_table":
                return stubs.buildPgPartitionedTable();
            case "pg_stat_statements":
            case "pg_stat_statements_info":
                throw new MemgresException("pg_stat_statements must be loaded via \"shared_preload_libraries\"", "55000");
            case "pg_stat_archiver":
                return stubs.buildPgStatArchiver();
            case "pg_stat_io":
                return stubs.buildPgStatIo();
            case "pg_stat_user_functions":
            case "pg_stat_xact_user_functions":
                return stubs.buildPgStatUserFunctions();
            case "pg_largeobject":
                return stubs.buildPgLargeobject();
            case "pg_parameter_acl":
                return stubs.buildPgParameterAcl();
            case "pg_buffercache":
                return stubs.buildPgBuffercache();
            case "pg_stat_wal_senders":
                return stubs.buildPgStatWalSenders();
            case "pg_ident_file_mappings":
                return stubs.buildPgIdentFileMappings();
            case "pg_db_role_setting":
                return stubs.buildPgDbRoleSetting();
            case "pg_catalog":
                return emptyTable(tableName);
            default:
                // Return null for unknown pg_ tables so they produce a proper
                // "relation does not exist" error (matching PG behavior).
                return null;
        }
    }
}
