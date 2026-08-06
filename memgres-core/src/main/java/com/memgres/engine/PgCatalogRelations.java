package com.memgres.engine;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The relations memgres publishes in {@code pg_catalog}, and what kind each one is.
 *
 * <p>pg_class and pg_attribute both have to agree about this list: a relation named in one and
 * absent from the other is either a relation nothing can introspect or an attribute belonging to
 * nothing. Keeping the names in a single place is what makes the two agree, and lets
 * pg_class.relnatts be counted from the columns the relation actually has rather than guessed.
 *
 * <p>PostgreSQL 18 carries 64 real catalog tables; everything else under pg_catalog that is not
 * an index is a view over them. Reporting a view as relkind 'r' puts it in front of every tool
 * that lists tables by relkind, so the distinction is recorded here rather than inferred from
 * the name.
 */
final class PgCatalogRelations {

    private PgCatalogRelations() {
    }

    /** The 64 relations PostgreSQL 18 stores as real tables under pg_catalog. */
    private static final Set<String> TABLES = new HashSet<String>();

    /** Every relation memgres publishes under pg_catalog, tables first, in listing order. */
    static final Set<String> ALL = new LinkedHashSet<String>();

    /** Index relations pg_class lists; PostgreSQL keeps indexes in pg_class too. */
    static final Set<String> INDEXES = new LinkedHashSet<String>();

    private static void tables(String... names) {
        for (String n : names) {
            TABLES.add(n);
            ALL.add(n);
        }
    }

    private static void views(String... names) {
        Collections.addAll(ALL, names);
    }

    static {
        tables(
                "pg_aggregate", "pg_am", "pg_amop", "pg_amproc", "pg_attrdef", "pg_attribute",
                "pg_auth_members", "pg_authid", "pg_cast", "pg_class", "pg_collation",
                "pg_constraint", "pg_conversion", "pg_database", "pg_db_role_setting",
                "pg_default_acl", "pg_depend", "pg_description", "pg_enum", "pg_event_trigger",
                "pg_extension", "pg_foreign_data_wrapper", "pg_foreign_server",
                "pg_foreign_table", "pg_index", "pg_inherits", "pg_init_privs", "pg_language",
                "pg_largeobject", "pg_largeobject_metadata", "pg_namespace", "pg_opclass",
                "pg_operator", "pg_opfamily", "pg_parameter_acl", "pg_partitioned_table",
                "pg_policy", "pg_proc", "pg_publication", "pg_publication_namespace",
                "pg_publication_rel", "pg_range", "pg_replication_origin", "pg_rewrite",
                "pg_seclabel", "pg_sequence", "pg_shdepend", "pg_shdescription",
                "pg_shseclabel", "pg_statistic", "pg_statistic_ext", "pg_statistic_ext_data",
                "pg_subscription", "pg_subscription_rel", "pg_tablespace", "pg_transform",
                "pg_trigger", "pg_ts_config", "pg_ts_config_map", "pg_ts_dict", "pg_ts_parser",
                "pg_ts_template", "pg_type", "pg_user_mapping");

        views(
                "pg_available_extension_versions", "pg_available_extensions",
                "pg_backend_memory_contexts", "pg_config", "pg_cursors", "pg_file_settings",
                "pg_group", "pg_hba_file_rules", "pg_ident_file_mappings", "pg_indexes",
                "pg_locks", "pg_matviews", "pg_policies", "pg_prepared_statements",
                "pg_prepared_xacts", "pg_publication_tables", "pg_replication_origin_status",
                "pg_replication_slots", "pg_roles", "pg_rules", "pg_seclabels", "pg_sequences",
                "pg_settings", "pg_shadow", "pg_shmem_allocations", "pg_stat_activity",
                "pg_stat_all_indexes", "pg_stat_all_tables", "pg_stat_archiver",
                "pg_stat_bgwriter", "pg_stat_checkpointer", "pg_stat_database",
                "pg_aios", "pg_stat_gssapi", "pg_stat_io", "pg_stat_progress_analyze",
                "pg_stat_progress_basebackup", "pg_stat_progress_cluster",
                "pg_stat_progress_copy", "pg_stat_progress_create_index",
                "pg_stat_progress_vacuum", "pg_stat_replication", "pg_stat_ssl",
                "pg_stat_subscription", "pg_stat_subscription_stats", "pg_stat_sys_indexes",
                "pg_stat_sys_tables", "pg_stat_user_functions", "pg_stat_user_indexes",
                "pg_stat_user_tables", "pg_stat_wal", "pg_stat_wal_receiver",
                "pg_stat_xact_all_tables", "pg_stat_xact_sys_tables",
                "pg_stat_xact_user_functions", "pg_stat_xact_user_tables",
                "pg_statio_all_indexes", "pg_statio_all_sequences", "pg_statio_all_tables",
                "pg_statio_sys_indexes", "pg_statio_sys_sequences", "pg_statio_sys_tables",
                "pg_statio_user_indexes", "pg_statio_user_sequences", "pg_statio_user_tables",
                "pg_stats", "pg_stats_ext", "pg_tables", "pg_timezone_abbrevs",
                "pg_timezone_names", "pg_user", "pg_user_mappings", "pg_views");

        Collections.addAll(INDEXES,
                "pg_type_oid_index", "pg_attribute_relid_attnum_index",
                "pg_proc_oid_index", "pg_class_oid_index",
                "pg_namespace_oid_index", "pg_constraint_oid_index",
                "pg_index_indrelid_index", "pg_index_indexrelid_index",
                "pg_description_o_c_o_index", "pg_depend_depender_index",
                "pg_depend_reference_index", "pg_attrdef_adrelid_adnum_index",
                "pg_trigger_tgrelid_index", "pg_enum_oid_index",
                "pg_cast_source_target_index", "pg_collation_oid_index",
                "pg_am_oid_index", "pg_database_oid_index");
    }

    /**
     * The order PostgreSQL 18 numbers a relation's columns in, for the relations where memgres
     * built them in another one.
     *
     * <p>Column order is not cosmetic: it is what pg_attribute.attnum reports, what
     * information_schema.columns.ordinal_position reports, and the order {@code SELECT *} answers
     * in. A tool that reads a catalog row positionally — pg_dump's queries, a driver's own
     * bootstrap — reads the wrong column when the order differs, and a diff of two servers'
     * catalogs shows every row as changed.
     *
     * <p>Listing the order here rather than rewriting each builder's row arrays keeps the
     * builders readable and puts what PostgreSQL says in one reviewable place.
     */
    private static final java.util.Map<String, String[]> ATTNUM_ORDER =
            new java.util.HashMap<String, String[]>();

    private static void order(String relname, String columns) {
        ATTNUM_ORDER.put(relname, columns.split(",\\s*"));
    }

    static {
        order("pg_attribute", "attrelid, attname, atttypid, attlen, attnum, atttypmod, attndims,"
                + " attbyval, attalign, attstorage, attcompression, attnotnull, atthasdef,"
                + " atthasmissing, attidentity, attgenerated, attisdropped, attislocal,"
                + " attinhcount, attcollation, attstattarget, attacl, attoptions, attfdwoptions,"
                + " attmissingval");
        order("pg_constraint", "oid, conname, connamespace, contype, condeferrable, condeferred,"
                + " conenforced, convalidated, conrelid, contypid, conindid, conparentid,"
                + " confrelid, confupdtype, confdeltype, confmatchtype, conislocal, coninhcount,"
                + " connoinherit, conperiod, conkey, confkey, conpfeqop, conppeqop, conffeqop,"
                + " confdelsetcols, conexclop, conbin");
        order("pg_index", "indexrelid, indrelid, indnatts, indnkeyatts, indisunique,"
                + " indnullsnotdistinct, indisprimary, indisexclusion, indimmediate,"
                + " indisclustered, indisvalid, indcheckxmin, indisready, indislive,"
                + " indisreplident, indkey, indcollation, indclass, indoption, indexprs, indpred");
        order("pg_trigger", "oid, tgrelid, tgparentid, tgname, tgfoid, tgtype, tgenabled,"
                + " tgisinternal, tgconstrrelid, tgconstrindid, tgconstraint, tgdeferrable,"
                + " tginitdeferred, tgnargs, tgattr, tgargs, tgqual, tgoldtable, tgnewtable");
        order("pg_database", "oid, datname, datdba, encoding, datlocprovider, datistemplate,"
                + " datallowconn, dathasloginevt, datconnlimit, datfrozenxid, datminmxid,"
                + " dattablespace, datcollate, datctype, datlocale, daticurules, datcollversion,"
                + " datacl");
        order("pg_roles", "rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin,"
                + " rolreplication, rolconnlimit, rolpassword, rolvaliduntil, rolbypassrls,"
                + " rolconfig, oid");
        order("pg_am", "oid, amname, amhandler, amtype");
        order("pg_opclass", "oid, opcmethod, opcname, opcnamespace, opcowner, opcfamily,"
                + " opcintype, opcdefault, opckeytype");
        order("pg_opfamily", "oid, opfmethod, opfname, opfnamespace, opfowner");
        order("pg_collation", "oid, collname, collnamespace, collowner, collprovider,"
                + " collisdeterministic, collencoding, collcollate, collctype, colllocale,"
                + " collicurules, collversion");
        order("pg_sequences", "schemaname, sequencename, sequenceowner, data_type, start_value,"
                + " min_value, max_value, increment_by, cycle, cache_size, last_value");
        order("pg_settings", "name, setting, unit, category, short_desc, extra_desc, context,"
                + " vartype, source, min_val, max_val, enumvals, boot_val, reset_val, sourcefile,"
                + " sourceline, pending_restart");
    }

    /** PostgreSQL's column order for a relation, or null when memgres already builds it in one. */
    static String[] attnumOrder(String relname) {
        return ATTNUM_ORDER.get(relname);
    }

    /** 'r' for a stored catalog table, 'v' for a view over them. */
    static String relkind(String relname) {
        return TABLES.contains(relname) ? "r" : "v";
    }
}
