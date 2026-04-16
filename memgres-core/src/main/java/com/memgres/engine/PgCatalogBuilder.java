package com.memgres.engine;

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

    public PgCatalogBuilder(Database database, OidSupplier oids) {
        this.core = new CatalogCoreBuilder(database, oids);
        this.constraints = new CatalogConstraintBuilder(database, oids);
        this.security = new CatalogSecurityBuilder(database, oids);
        this.typeSystem = new CatalogTypeSystemBuilder(database, oids);
        this.stubs = new CatalogStubBuilder(database, oids);
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
            case "pg_authid":
                return security.buildPgRoles();
            case "pg_user":
                return security.buildPgUser();
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
                return security.buildPgSettings();
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
                return stubs.buildPgEventTrigger();
            case "pg_foreign_data_wrapper":
                return stubs.buildPgForeignDataWrapper();
            case "pg_foreign_server":
                return stubs.buildPgForeignServer();
            case "pg_user_mapping":
                return stubs.buildPgUserMapping();
            case "pg_foreign_table":
                return stubs.buildPgForeignTable();
            case "pg_timezone_names":
                return stubs.buildPgTimezoneNames();
            case "pg_timezone_abbrevs":
                return stubs.buildPgTimezoneAbbrevs();
            case "pg_stat_user_tables":
            case "pg_stat_all_tables":
            case "pg_stat_xact_user_tables":
            case "pg_stat_xact_all_tables":
                return stubs.buildPgStatUserTables();
            case "pg_stat_user_indexes":
            case "pg_stat_all_indexes":
            case "pg_statio_all_indexes":
                return stubs.buildPgStatUserIndexes();
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
            case "pg_statio_user_indexes":
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
            case "pg_publication_rel":
                return stubs.buildPgPublicationRel();
            case "pg_publication_namespace":
                return stubs.buildPgPublicationNamespace();
            case "pg_subscription_rel":
                return stubs.buildPgSubscriptionRel();
            case "pg_partitioned_table":
                return stubs.buildPgPartitionedTable();
            case "pg_catalog":
                return emptyTable(tableName);
            default:
                return emptyTable(tableName);
        }
    }
}
