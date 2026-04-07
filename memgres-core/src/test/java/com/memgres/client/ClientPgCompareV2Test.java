package com.memgres.client;

import com.memgres.core.Memgres;

import java.sql.*;
import java.util.*;

/**
 * Comprehensive comparison of client introspection queries between Memgres and PG 18.
 * Uses extended query protocol (PreparedStatement, NO preferQueryMode=simple).
 * Run as main(). Requires PG 18 on localhost:5432 with memgrestest/memgres/memgres.
 */
public class ClientPgCompareV2Test {

    static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest";
    static final String PG_USER = "memgres";
    static final String PG_PASS = "memgres";

    static int totalOk = 0;
    static int totalDiff = 0;
    static int totalMemErr = 0;
    static int totalPgErr = 0;
    static int totalBothErr = 0;

    public static void main(String[] args) throws Exception {
        // Start Memgres
        Memgres memgres = Memgres.builder().port(0).build().start();
        String memUrl = memgres.getJdbcUrl(); // No preferQueryMode=simple
        Connection memConn = DriverManager.getConnection(memUrl, memgres.getUser(), memgres.getPassword());
        memConn.setAutoCommit(true);

        // Connect to PG 18 (no preferQueryMode=simple)
        Connection pgConn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
        pgConn.setAutoCommit(true);

        // Setup: create same table on both
        execSafe(memConn, "CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
        execSafe(pgConn, "DROP TABLE IF EXISTS users");
        execSafe(pgConn, "CREATE TABLE users (user_id serial PRIMARY KEY, username text)");

        // Get namespace OIDs for public schema
        int memNsOid = getOid(memConn, "SELECT oid FROM pg_namespace WHERE nspname = 'public'");
        int pgNsOid = getOid(pgConn, "SELECT oid FROM pg_namespace WHERE nspname = 'public'");
        System.out.println("Memgres public nsOid=" + memNsOid + ", PG public nsOid=" + pgNsOid);

        // Get namespace OIDs for pg_catalog
        int memCatOid = getOid(memConn, "SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'");
        int pgCatOid = getOid(pgConn, "SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'");
        System.out.println("Memgres pg_catalog nsOid=" + memCatOid + ", PG pg_catalog nsOid=" + pgCatOid);

        // Get txid from each
        String memTxid = getStr(memConn, "select case when pg_catalog.pg_is_in_recovery() then null else (pg_catalog.txid_current() % 4294967296)::varchar::bigint end as current_txid");
        String pgTxid = getStr(pgConn, "select case when pg_catalog.pg_is_in_recovery() then null else (pg_catalog.txid_current() % 4294967296)::varchar::bigint end as current_txid");
        System.out.println("Memgres txid=" + memTxid + ", PG txid=" + pgTxid);

        System.out.println("\n=== Client Query Comparison V2: Memgres vs PG 18 (Extended Protocol) ===\n");

        // =====================================================================
        // GLOBAL QUERIES (no parameters)
        // =====================================================================

        // 1. version
        comparePs("01_version",
                "select version()",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 2. current_db
        comparePs("02_current_db",
                "select current_database() as a, current_schemas(false) as b",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 3. current_all
        comparePs("03_current_all",
                "select current_database(), current_schema(), current_user",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 4. show_txn_iso
        comparePs("04_show_txn_iso",
                "SHOW TRANSACTION ISOLATION LEVEL",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 5. show_datestyle
        comparePs("05_show_datestyle",
                "show DateStyle",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 6. usesuper
        comparePs("06_usesuper",
                "select usesuper from pg_user where usename = current_user",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 7. startup_time
        comparePs("07_startup_time",
                "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 8. pg_database
        comparePs("08_pg_database",
                "select N.oid::bigint as id, datname as name, D.description, datistemplate as is_template, " +
                "datallowconn as allow_connections, pg_catalog.pg_get_userbyid(N.datdba) as \"owner\" " +
                "from pg_catalog.pg_database N left join pg_catalog.pg_shdescription D on N.oid = D.objoid " +
                "order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 9. pg_roles
        comparePs("09_pg_roles",
                "select R.oid::bigint as role_id, rolname as role_name, " +
                "rolsuper is_super, rolinherit is_inherit, " +
                "rolcreaterole can_createrole, rolcreatedb can_createdb, " +
                "rolcanlogin can_login, rolreplication is_replication, " +
                "rolconnlimit conn_limit, rolvaliduntil valid_until, " +
                "rolbypassrls bypass_rls, rolconfig config, " +
                "D.description " +
                "from pg_catalog.pg_roles R left join pg_catalog.pg_shdescription D on D.objoid = R.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 10. pg_auth_members
        comparePs("10_pg_auth_members",
                "select member id, roleid role_id, admin_option from pg_catalog.pg_auth_members order by id, roleid::text",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 11. pg_tablespace
        comparePs("11_pg_tablespace",
                "select T.oid::bigint as id, T.spcname as name, " +
                "T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner, " +
                "pg_catalog.pg_tablespace_location(T.oid) as location, " +
                "T.spcoptions as options, D.description as comment " +
                "from pg_catalog.pg_tablespace T left join pg_catalog.pg_shdescription D on D.objoid = T.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 12. pg_namespace
        comparePs("12_pg_namespace",
                "select N.oid::bigint as id, N.xmin as state_number, nspname as name, D.description, " +
                "pg_catalog.pg_get_userbyid(N.nspowner) as \"owner\" " +
                "from pg_catalog.pg_namespace N left join pg_catalog.pg_description D on N.oid = D.objoid " +
                "order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 13. pg_event_trigger
        comparePs("13_pg_event_trigger",
                "select t.oid as id, t.xmin as state_number, t.evtname as name, t.evtevent as event, " +
                "t.evtfoid as routine_id, pg_catalog.pg_get_userbyid(t.evtowner) as owner, " +
                "t.evttags as tags, case when t.evtenabled = 'D' then 1 else 0 end as is_disabled " +
                "from pg_catalog.pg_event_trigger t",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 14. pg_fdw
        comparePs("14_pg_fdw",
                "select fdw.oid as id, fdw.xmin as state_number, fdw.fdwname as name, " +
                "pr.proname as handler, nspc.nspname as handler_schema, " +
                "pr2.proname as validator, nspc2.nspname as validator_schema, " +
                "fdw.fdwoptions as options, pg_catalog.pg_get_userbyid(fdw.fdwowner) as \"owner\" " +
                "from pg_catalog.pg_foreign_data_wrapper fdw " +
                "left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid " +
                "left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid " +
                "left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid " +
                "left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 15. pg_foreign_server
        comparePs("15_pg_foreign_server",
                "select srv.oid as id, srv.srvfdw as fdw_id, srv.xmin as state_number, " +
                "srv.srvname as name, srv.srvtype as type, srv.srvversion as version, " +
                "srv.srvoptions as options, pg_catalog.pg_get_userbyid(srv.srvowner) as \"owner\" " +
                "from pg_catalog.pg_foreign_server srv",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 16. pg_user_mapping
        comparePs("16_pg_user_mapping",
                "select oid as id, umserver as server_id, " +
                "case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as \"user\", " +
                "umoptions as options from pg_catalog.pg_user_mapping order by server_id",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 17. pg_am
        comparePs("17_pg_am",
                "select A.oid as access_method_id, A.xmin as state_number, " +
                "A.amname as access_method_name, " +
                "A.amhandler::oid as handler_id, " +
                "pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name, " +
                "A.amtype as access_method_type " +
                "from pg_am A join pg_proc P on A.amhandler::oid = P.oid " +
                "join pg_namespace N on P.pronamespace = N.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 18. pg_extension
        comparePs("18_pg_extension",
                "select E.oid as id, E.xmin as state_number, extname as name, " +
                "extversion as version, extnamespace as schema_id, nspname as schema_name, " +
                "array(select unnest from unnest(available_versions) where unnest > extversion) as available_updates " +
                "from pg_catalog.pg_extension E " +
                "join pg_namespace N on E.extnamespace = N.oid " +
                "left join (select name, array_agg(version) as available_versions " +
                "from pg_available_extension_versions() group by name) V on E.extname = V.name",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 19. pg_language
        comparePs("19_pg_language",
                "select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted, " +
                "h.proname as handler, hs.nspname as handlerSchema, " +
                "i.proname as inline, isc.nspname as inlineSchema, " +
                "v.proname as validator, vs.nspname as validatorSchema " +
                "from pg_catalog.pg_language l " +
                "left join pg_catalog.pg_proc h on h.oid = lanplcallfoid " +
                "left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace " +
                "left join pg_catalog.pg_proc i on i.oid = laninline " +
                "left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace " +
                "left join pg_catalog.pg_proc v on v.oid = lanvalidator " +
                "left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace " +
                "order by lanname",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 20. desc_global
        comparePs("20_desc_global",
                "select D.objoid id, case " +
                "when 'pg_catalog.pg_event_trigger'::regclass = classoid then 'T' " +
                "when 'pg_catalog.pg_am'::regclass = classoid then 'A' " +
                "when 'pg_catalog.pg_cast'::regclass = classoid then 'C' " +
                "when 'pg_catalog.pg_foreign_data_wrapper'::regclass = classoid then 'W' " +
                "when 'pg_catalog.pg_foreign_server'::regclass = classoid then 'S' " +
                "when 'pg_catalog.pg_language'::regclass = classoid then 'L' " +
                "when 'pg_catalog.pg_extension'::regclass = classoid then 'E' " +
                "end as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D " +
                "where classoid in ( " +
                "'pg_catalog.pg_event_trigger'::regclass, " +
                "'pg_catalog.pg_am'::regclass, " +
                "'pg_catalog.pg_cast'::regclass, " +
                "'pg_catalog.pg_foreign_data_wrapper'::regclass, " +
                "'pg_catalog.pg_foreign_server'::regclass, " +
                "'pg_catalog.pg_language'::regclass, " +
                "'pg_catalog.pg_extension'::regclass )",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 21. acl_global
        comparePs("21_acl_global",
                "select T.oid as object_id, T.fdwacl as acl from pg_catalog.pg_foreign_data_wrapper T " +
                "union all select T.oid as object_id, T.lanacl as acl from pg_catalog.pg_language T " +
                "union all select T.oid as object_id, T.nspacl as acl from pg_catalog.pg_namespace T " +
                "union all select T.oid as object_id, T.srvacl as acl from pg_catalog.pg_foreign_server T",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 22. pg_cast
        comparePs("22_pg_cast",
                "select C.oid, C.xmin as state_number, C.castsource as castsource_id, " +
                "pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name, " +
                "C.casttarget as casttarget_id, " +
                "pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name, " +
                "C.castfunc as castfunc_id, " +
                "pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name, " +
                "C.castcontext, C.castmethod " +
                "from pg_cast C " +
                "left outer join pg_proc F on C.castfunc::oid = F.oid " +
                "left outer join pg_namespace FN on F.pronamespace = FN.oid " +
                "join pg_type S on C.castsource::oid = S.oid " +
                "join pg_namespace SN on S.typnamespace = SN.oid " +
                "join pg_type T on C.casttarget::oid = T.oid " +
                "join pg_namespace TN on T.typnamespace = TN.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 23. extension_depend
        comparePs("23_extension_depend",
                "select E.oid as extension_id, D.objid as member_id " +
                "from pg_extension E join pg_depend D on E.oid = D.refobjid and " +
                "D.refclassid = 'pg_extension'::regclass::oid " +
                "where D.deptype = 'e' order by extension_id",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 24. tablespace_db_acl
        comparePs("24_tablespace_db_acl",
                "select T.oid as object_id, T.spcacl as acl from pg_catalog.pg_tablespace T " +
                "union all " +
                "select T.oid as object_id, T.datacl as acl from pg_catalog.pg_database T",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 25. timezone
        comparePs("25_timezone",
                "select name, is_dst from pg_catalog.pg_timezone_names " +
                "union distinct " +
                "select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 26. pg_locks
        comparePs("26_pg_locks",
                "select L.transactionid::varchar::bigint as transaction_id " +
                "from pg_catalog.pg_locks L " +
                "where L.transactionid is not null " +
                "order by pg_catalog.age(L.transactionid) desc limit 1",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // 27. txid
        comparePs("27_txid",
                "select case when pg_catalog.pg_is_in_recovery() then null " +
                "else (pg_catalog.txid_current() % 4294967296)::varchar::bigint end as current_txid",
                memConn, pgConn, new Object[]{}, new Object[]{});


        // =====================================================================
        // PARAMETERIZED QUERIES (bind nsOid for public schema, "1" for xid)
        // =====================================================================

        // 28. big_union_oids: 7 params all nsOid
        comparePs("28_big_union_oids",
                "select T.oid as oid, relnamespace as schemaId, " +
                "pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind " +
                "from pg_catalog.pg_class T where relnamespace in ( ?::oid ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S') " +
                "union all " +
                "select T.oid, T.typnamespace, 'T' as kind " +
                "from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid " +
                "where T.typnamespace in ( ?::oid ) " +
                "and ( T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or " +
                "(T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or " +
                "T.typtype = 'p' and not T.typisdefined ) " +
                "union all " +
                "select oid, collnamespace, 'C' as kind from pg_catalog.pg_collation where collnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, oprnamespace, 'O' as kind from pg_catalog.pg_operator where oprnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, opcnamespace, 'c' as kind from pg_catalog.pg_opclass where opcnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, opfnamespace, 'F' as kind from pg_catalog.pg_opfamily where opfnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end as kind " +
                "from pg_catalog.pg_proc where pronamespace in ( ?::oid )",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // 29. big_union_names: 7 params all nsOid
        comparePs("29_big_union_names",
                "select T.oid as oid, relnamespace as schemaId, " +
                "pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name " +
                "from pg_catalog.pg_class T where relnamespace in ( ?::oid ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S') " +
                "union all " +
                "select T.oid, T.typnamespace, 'T', T.typname " +
                "from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid " +
                "where T.typnamespace in ( ?::oid ) " +
                "and ( T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or " +
                "(T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or " +
                "T.typtype = 'p' and not T.typisdefined ) " +
                "union all " +
                "select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( ?::oid ) " +
                "union all " +
                "select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end, proname " +
                "from pg_catalog.pg_proc where pronamespace in ( ?::oid ) " +
                "order by schemaId",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // 30. proc_args: 1 param nsOid
        comparePs("30_proc_args",
                "select pronamespace as schemaId, oid as majorOid, proargnames as argNames, " +
                "proargmodes as argModes, array_length(proargtypes, 1) as nArgs " +
                "from pg_catalog.pg_proc where pronamespace in ( ?::oid ) order by schemaId",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 31. column_names CTE: 1 param nsOid
        comparePs("31_column_names",
                "with T as ( select T.oid as oid, T.relkind as kind, T.relnamespace as schemaId " +
                "from pg_catalog.pg_class T " +
                "where T.relnamespace in ( ?::oid ) and T.relkind in ('r', 'm', 'v', 'f', 'p') ) " +
                "select T.schemaId as schemaId, T.oid as majorOid, " +
                "pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind, " +
                "C.attnum as position, C.attname as name " +
                "from T join pg_catalog.pg_attribute C on T.oid = C.attrelid " +
                "where C.attnum > 0 and not C.attisdropped " +
                "order by schemaId, majorOid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 32. sequence_listing: 2 params: nsOid, "1"
        comparePs("32_sequence_listing",
                "select cls.xmin as sequence_state_number, sq.seqrelid as sequence_id, " +
                "cls.relname as sequence_name, pg_catalog.format_type(sq.seqtypid, null) as data_type, " +
                "sq.seqstart as start_value, sq.seqincrement as inc_value, " +
                "sq.seqmin as min_value, sq.seqmax as max_value, " +
                "sq.seqcache as cache_size, sq.seqcycle as cycle_option, " +
                "pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\" " +
                "from pg_catalog.pg_sequence sq join pg_class cls on sq.seqrelid = cls.oid " +
                "where cls.relnamespace = ?::oid " +
                "and pg_catalog.age(cls.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 33. type_listing: 2 params: nsOid, "1"
        comparePs("33_type_listing",
                "select T.oid as type_id, T.xmin as type_state_number, T.typname as type_name, " +
                "T.typtype as type_sub_kind, T.typcategory as type_category, T.typrelid as class_id, " +
                "T.typbasetype as base_type_id, " +
                "case when T.typtype in ('c','e') then null " +
                "else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def, " +
                "T.typndims as dimensions_number, T.typdefault as default_expression, " +
                "T.typnotnull as mandatory, pg_catalog.pg_get_userbyid(T.typowner) as \"owner\" " +
                "from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid " +
                "where T.typnamespace = ?::oid " +
                "and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "and (T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or " +
                "(T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or " +
                "T.typtype = 'p' and not T.typisdefined) order by 1",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 34. table_listing: 2 params: nsOid, "1"
        comparePs("34_table_listing",
                "select T.relkind as table_kind, T.relname as table_name, T.oid as table_id, " +
                "T.xmin as table_state_number, false as table_with_oids, " +
                "T.reltablespace as tablespace_id, T.reloptions as options, " +
                "T.relpersistence as persistence, " +
                "(select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors, " +
                "(select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors, " +
                "T.relispartition as is_partition, " +
                "pg_catalog.pg_get_partkeydef(T.oid) as partition_key, " +
                "pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression, " +
                "T.relam am_id, pg_catalog.pg_get_userbyid(T.relowner) as \"owner\" " +
                "from pg_catalog.pg_class T " +
                "where relnamespace = ?::oid and relkind in ('r', 'm', 'v', 'f', 'p') " +
                "and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "order by table_kind, table_id",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 35. foreign_table: 2 params: nsOid, "1"
        comparePs("35_foreign_table",
                "select ft.ftrelid as table_id, srv.srvname as table_server, " +
                "ft.ftoptions as table_options, pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\" " +
                "from pg_catalog.pg_foreign_table ft " +
                "left outer join pg_catalog.pg_foreign_server srv on ft.ftserver = srv.oid " +
                "join pg_catalog.pg_class cls on ft.ftrelid = cls.oid " +
                "where cls.relnamespace = ?::oid " +
                "and pg_catalog.age(ft.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "order by table_id",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 36. type_resolution CTE: 7 params: nsOid, "1", nsOid, "1", nsOid, "1", "1"
        comparePs("36_type_resolution",
                "with schema_procs as (select prorettype, proargtypes, proallargtypes " +
                "from pg_catalog.pg_proc where pronamespace = ?::oid " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) ), " +
                "schema_opers as (select oprleft, oprright, oprresult " +
                "from pg_catalog.pg_operator where oprnamespace = ?::oid " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) ), " +
                "schema_aggregates as (select A.aggtranstype, A.aggmtranstype " +
                "from pg_catalog.pg_aggregate A join pg_catalog.pg_proc P on A.aggfnoid = P.oid " +
                "where P.pronamespace = ?::oid " +
                "and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) or " +
                "pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)) ), " +
                "schema_arg_types as ( " +
                "select prorettype as type_id from schema_procs " +
                "union select distinct unnest(proargtypes) as type_id from schema_procs " +
                "union select distinct unnest(proallargtypes) as type_id from schema_procs " +
                "union select oprleft as type_id from schema_opers where oprleft is not null " +
                "union select oprright as type_id from schema_opers where oprright is not null " +
                "union select oprresult as type_id from schema_opers where oprresult is not null " +
                "union select aggtranstype::oid as type_id from schema_aggregates " +
                "union select aggmtranstype::oid as type_id from schema_aggregates ) " +
                "select type_id, pg_catalog.format_type(type_id, null) as type_spec " +
                "from schema_arg_types where type_id <> 0",
                memConn, pgConn,
                new Object[]{memNsOid, "1", memNsOid, "1", memNsOid, "1", "1"},
                new Object[]{pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", "1"});

        // 37. routine_listing: 2 params: nsOid, "1"
        comparePs("37_routine_listing",
                "with languages as (select oid as lang_oid, lanname as lang from pg_catalog.pg_language), " +
                "routines as (select proname as r_name, prolang as lang_oid, oid as r_id, " +
                "xmin as r_state_number, proargnames as arg_names, proargmodes as arg_modes, " +
                "proargtypes::int[] as in_arg_types, proallargtypes::int[] as all_arg_types, " +
                "pg_catalog.pg_get_expr(proargdefaults, 0) as arg_defaults, " +
                "provariadic as arg_variadic_id, prorettype as ret_type_id, proretset as ret_set, " +
                "prokind as kind, provolatile as volatile_kind, proisstrict as is_strict, " +
                "prosecdef as is_security_definer, proconfig as configuration_parameters, " +
                "procost as cost, pg_catalog.pg_get_userbyid(proowner) as \"owner\", " +
                "prorows as rows, proleakproof as is_leakproof, proparallel as concurrency_kind " +
                "from pg_catalog.pg_proc where pronamespace = ?::oid " +
                "and not (prokind = 'a') " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) ) " +
                "select * from routines natural join languages",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 38. aggregate_listing: 3 params: nsOid, "1", "1"
        comparePs("38_aggregate_listing",
                "select P.oid as aggregate_id, P.xmin as state_number, P.proname as aggregate_name, " +
                "P.proargnames as arg_names, P.proargmodes as arg_modes, " +
                "P.proargtypes::int[] as in_arg_types, P.proallargtypes::int[] as all_arg_types, " +
                "A.aggtransfn::oid as transition_function_id, " +
                "A.aggtransfn::regproc::text as transition_function_name, " +
                "A.aggtranstype as transition_type, " +
                "A.aggfinalfn::oid as final_function_id, " +
                "case when A.aggfinalfn::oid = 0 then null else A.aggfinalfn::regproc::varchar end as final_function_name, " +
                "case when A.aggfinalfn::oid = 0 then 0 else P.prorettype end as final_return_type, " +
                "A.agginitval as initial_value, A.aggsortop as sort_operator_id, " +
                "case when A.aggsortop = 0 then null else A.aggsortop::regoper::varchar end as sort_operator_name, " +
                "pg_catalog.pg_get_userbyid(P.proowner) as \"owner\", " +
                "A.aggfinalextra as final_extra, A.aggtransspace as state_size, " +
                "A.aggmtransfn::oid as moving_transition_id, " +
                "case when A.aggmtransfn::oid = 0 then null else A.aggmtransfn::regproc::varchar end as moving_transition_name, " +
                "A.aggminvtransfn::oid as inverse_transition_id, " +
                "case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name, " +
                "A.aggmtranstype::oid as moving_state_type, A.aggmtransspace as moving_state_size, " +
                "A.aggmfinalfn::oid as moving_final_id, " +
                "case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name, " +
                "A.aggmfinalextra as moving_final_extra, A.aggminitval as moving_initial_value, " +
                "A.aggkind as aggregate_kind, A.aggnumdirectargs as direct_args, " +
                "A.aggcombinefn::oid as combine_function_id, " +
                "case when A.aggcombinefn::oid = 0 then null else A.aggcombinefn::regproc::varchar end as combine_function_name, " +
                "A.aggserialfn::oid as serialization_function_id, " +
                "case when A.aggserialfn::oid = 0 then null else A.aggserialfn::regproc::varchar end as serialization_function_name, " +
                "A.aggdeserialfn::oid as deserialization_function_id, " +
                "case when A.aggdeserialfn::oid = 0 then null else A.aggdeserialfn::regproc::varchar end as deserialization_function_name, " +
                "P.proparallel as concurrency_kind " +
                "from pg_catalog.pg_aggregate A join pg_catalog.pg_proc P on A.aggfnoid = P.oid " +
                "where P.pronamespace = ?::oid " +
                "and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) or " +
                "pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)) " +
                "order by P.oid",
                memConn, pgConn,
                new Object[]{memNsOid, "1", "1"}, new Object[]{pgNsOid, "1", "1"});

        // 39. operator_listing: 2 params: nsOid, "1"
        comparePs("39_operator_listing",
                "select O.oid as op_id, O.xmin as state_number, oprname as op_name, " +
                "oprkind as op_kind, oprleft as arg_left_type_id, oprright as arg_right_type_id, " +
                "oprresult as arg_result_type_id, oprcode::oid as main_id, oprcode::varchar as main_name, " +
                "oprrest::oid as restrict_id, oprrest::varchar as restrict_name, " +
                "oprjoin::oid as join_id, oprjoin::varchar as join_name, " +
                "oprcom::oid as com_id, oprcom::regoper::varchar as com_name, " +
                "oprnegate::oid as neg_id, oprnegate::regoper::varchar as neg_name, " +
                "oprcanmerge as merges, oprcanhash as hashes, " +
                "pg_catalog.pg_get_userbyid(O.oprowner) as \"owner\" " +
                "from pg_catalog.pg_operator O where oprnamespace = ?::oid " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 40. collation_listing: 2 params: nsOid, "1"
        comparePs("40_collation_listing",
                "select oid as id, xmin as state_number, collname as name, " +
                "collcollate as lc_collate, collctype as lc_ctype, " +
                "pg_catalog.pg_get_userbyid(collowner) as \"owner\" " +
                "from pg_catalog.pg_collation where collnamespace = ?::oid " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 41. opclass_listing: 2 params: nsOid, "1"
        comparePs("41_opclass_listing",
                "select O.oid as id, O.xmin as state_number, opcname as name, " +
                "opcintype::regtype::varchar as in_type, " +
                "case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type, " +
                "opcdefault as is_default, opcfamily as family_id, opfname as family, " +
                "opcmethod as access_method_id, pg_catalog.pg_get_userbyid(O.opcowner) as \"owner\" " +
                "from pg_catalog.pg_opclass O join pg_catalog.pg_opfamily F on F.oid = opcfamily " +
                "where opcnamespace = ?::oid " +
                "and pg_catalog.age(O.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 42. opfamily_listing: 2 params: nsOid, "1"
        comparePs("42_opfamily_listing",
                "select O.oid as id, O.xmin as state_number, opfname as name, " +
                "opfmethod as access_method_id, pg_catalog.pg_get_userbyid(O.opfowner) as \"owner\" " +
                "from pg_catalog.pg_opfamily O where opfnamespace = ?::oid " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 43. amop_oids: 1 param: nsOid
        comparePs("43_amop_oids",
                "select pg_amop.oid from pg_catalog.pg_amop " +
                "join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily " +
                "where opfnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 44. amop_listing: 3 params: nsOid, nsOid, "1"
        comparePs("44_amop_listing",
                "select O.oid as id, O.amopstrategy as strategy, O.amopopr as op_id, " +
                "O.amopopr::regoperator::varchar as op_sig, " +
                "O.amopsortfamily as sort_family_id, SF.opfname as sort_family, " +
                "O.amopfamily as family_id, C.oid as class_id " +
                "from pg_catalog.pg_amop O " +
                "left join pg_opfamily F on O.amopfamily = F.oid " +
                "left join pg_opfamily SF on O.amopsortfamily = SF.oid " +
                "left join pg_depend D on D.classid = 'pg_amop'::regclass and O.oid = D.objid and D.objsubid = 0 " +
                "left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0 " +
                "where C.opcnamespace = ?::oid or C.opcnamespace is null and F.opfnamespace = ?::oid " +
                "and pg_catalog.age(O.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "order by C.oid, F.oid",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, "1"}, new Object[]{pgNsOid, pgNsOid, "1"});

        // 45. amproc_oids: 1 param: nsOid
        comparePs("45_amproc_oids",
                "select pg_amproc.oid from pg_catalog.pg_amproc " +
                "join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily " +
                "where opfnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 46. amproc_listing: 3 params: nsOid, nsOid, "1"
        comparePs("46_amproc_listing",
                "select P.oid as id, P.amprocnum as num, P.amproc::oid as proc_id, " +
                "P.amproc::regprocedure::varchar as proc_sig, " +
                "P.amproclefttype::regtype::varchar as left_type, " +
                "P.amprocrighttype::regtype::varchar as right_type, " +
                "P.amprocfamily as family_id, C.oid as class_id " +
                "from pg_catalog.pg_amproc P " +
                "left join pg_opfamily F on P.amprocfamily = F.oid " +
                "left join pg_depend D on D.classid = 'pg_amproc'::regclass and P.oid = D.objid and D.objsubid = 0 " +
                "left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0 " +
                "where C.opcnamespace = ?::oid or C.opcnamespace is null and F.opfnamespace = ?::oid " +
                "and pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "order by C.oid, F.oid",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, "1"}, new Object[]{pgNsOid, pgNsOid, "1"});

        // 47. column_listing: 3 params: nsOid, "1", "1"
        comparePs("47_column_listing",
                "with T as ( select distinct T.oid as table_id, T.relname as table_name " +
                "from pg_catalog.pg_class T, pg_catalog.pg_attribute A " +
                "where T.relnamespace = ?::oid and T.relkind in ('r', 'm', 'v', 'f', 'p') " +
                "and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "or pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)) " +
                "and A.attrelid = T.oid ) " +
                "select T.table_id, C.attnum as column_position, C.attname as column_name, " +
                "C.xmin as column_state_number, C.atttypmod as type_mod, " +
                "C.attndims as dimensions_number, " +
                "pg_catalog.format_type(C.atttypid, C.atttypmod) as type_spec, " +
                "C.atttypid as type_id, C.attnotnull as mandatory, " +
                "pg_catalog.pg_get_expr(D.adbin, T.table_id) as column_default_expression, " +
                "not C.attislocal as column_is_inherited, C.attfdwoptions as options, " +
                "C.attisdropped as column_is_dropped, C.attidentity as identity_kind, " +
                "C.attgenerated as generated " +
                "from T join pg_catalog.pg_attribute C on T.table_id = C.attrelid " +
                "left join pg_catalog.pg_attrdef D on (C.attrelid, C.attnum) = (D.adrelid, D.adnum) " +
                "where attnum > 0 order by table_id, attnum",
                memConn, pgConn,
                new Object[]{memNsOid, "1", "1"}, new Object[]{pgNsOid, "1", "1"});

        // 48. index_oids: 1 param: nsOid
        comparePs("48_index_oids",
                "select IX.indexrelid from pg_catalog.pg_index IX, pg_catalog.pg_class IC " +
                "where IC.oid = IX.indrelid and IC.relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 49. index_listing: 2 params: nsOid, "1"
        comparePs("49_index_listing",
                "select tab.oid table_id, tab.relkind table_kind, ind_stor.relname index_name, " +
                "ind_head.indexrelid index_id, ind_stor.xmin state_number, " +
                "ind_head.indisunique is_unique, ind_head.indisprimary is_primary, " +
                "ind_head.indnullsnotdistinct nulls_not_distinct, " +
                "pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition, " +
                "(select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors, " +
                "ind_stor.reltablespace tablespace_id, opcmethod as access_method_id " +
                "from pg_catalog.pg_class tab " +
                "join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid " +
                "join pg_catalog.pg_class ind_stor on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid " +
                "left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass) " +
                "where tab.relnamespace = ?::oid and tab.relkind in ('r', 'm', 'v', 'p') " +
                "and ind_stor.relkind in ('i', 'I') " +
                "and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 50. index_columns: 2 params: nsOid, "1"
        comparePs("50_index_columns",
                "select ind_head.indexrelid index_id, " +
                "k col_idx, k <= indnkeyatts in_key, " +
                "ind_head.indkey[k-1] column_position, " +
                "ind_head.indoption[k-1] column_options, " +
                "ind_head.indcollation[k-1] as collation, " +
                "colln.nspname as collation_schema, collname as collation_str, " +
                "ind_head.indclass[k-1] as opclass, " +
                "case when opcdefault then null else opcn.nspname end as opclass_schema, " +
                "case when opcdefault then null else opcname end as opclass_str, " +
                "case when indexprs is null then null " +
                "when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true) " +
                "else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true) end as expression, " +
                "amcanorder can_order " +
                "from pg_catalog.pg_index ind_head " +
                "join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid " +
                "cross join unnest(ind_head.indkey) with ordinality u(u, k) " +
                "left join pg_catalog.pg_collation on pg_collation.oid = ind_head.indcollation[k-1] " +
                "left join pg_catalog.pg_namespace colln on collnamespace = colln.oid " +
                "cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder " +
                "left join pg_catalog.pg_opclass on pg_opclass.oid = ind_head.indclass[k-1] " +
                "left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid " +
                "where ind_stor.relnamespace = ?::oid and ind_stor.relkind in ('i', 'I') " +
                "and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "order by index_id, k",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 51. constraint_oids: 1 param: nsOid
        comparePs("51_constraint_oids",
                "select oid from pg_catalog.pg_constraint where conrelid != 0 and connamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 52. constraint_listing: 4 params: nsOid, nsOid, "1", "1"
        comparePs("52_constraint_listing",
                "select T.oid table_id, relkind table_kind, C.oid::bigint con_id, " +
                "C.xmin::varchar::bigint con_state_id, conname con_name, contype con_kind, " +
                "conkey con_columns, conindid index_id, confrelid ref_table_id, " +
                "condeferrable is_deferrable, condeferred is_init_deferred, " +
                "confupdtype on_update, confdeltype on_delete, connoinherit no_inherit, " +
                "pg_catalog.pg_get_expr(conbin, T.oid) con_expression, " +
                "confkey ref_columns, conexclop::int[] excl_operators, " +
                "array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str " +
                "from pg_catalog.pg_constraint C join pg_catalog.pg_class T on C.conrelid = T.oid " +
                "where relkind in ('r', 'v', 'f', 'p') and relnamespace = ?::oid " +
                "and contype in ('p', 'u', 'f', 'c', 'x') and connamespace = ?::oid " +
                "and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "or pg_catalog.age(c.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, "1", "1"},
                new Object[]{pgNsOid, pgNsOid, "1", "1"});

        // 53. pg_type_elem: 1 param: type OID (use 23=integer)
        comparePs("53_pg_type_elem",
                "SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname " +
                "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e ON t.typelem = e.oid " +
                "JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = ?",
                memConn, pgConn,
                new Object[]{23}, new Object[]{23});

        // 54. rewrite_oids: 1 param: nsOid
        comparePs("54_rewrite_oids",
                "select RU.oid from pg_catalog.pg_rewrite RU, pg_catalog.pg_class RC " +
                "where RC.oid = RU.ev_class and RC.relnamespace = ?::oid " +
                "and not RU.rulename = '_RETURN'",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 55. rule_listing: 2 params: nsOid, "1"
        comparePs("55_rule_listing",
                "select R.ev_class as table_id, R.oid as rule_id, R.xmin as rule_state_number, " +
                "R.rulename as rule_name, pg_catalog.translate(ev_type,'1234','SUID') as rule_event_code, " +
                "R.ev_enabled as rule_fire_mode, R.is_instead as rule_is_instead " +
                "from pg_catalog.pg_rewrite R where R.ev_class in ( select oid from pg_catalog.pg_class " +
                "where relnamespace = ?::oid ) " +
                "and pg_catalog.age(R.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "and R.rulename != '_RETURN'::name order by R.ev_class::bigint, ev_type",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 56. policy_oids: 1 param: nsOid
        comparePs("56_policy_oids",
                "select P.oid from pg_catalog.pg_policy P " +
                "join pg_catalog.pg_class C on polrelid = C.oid where relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 57. policy_listing: 2 params: nsOid, "1"
        comparePs("57_policy_listing",
                "select P.oid id, P.xmin as state_number, polname policyname, polrelid table_id, " +
                "polpermissive as permissive, polroles roles, polcmd cmd, " +
                "pg_get_expr(polqual, polrelid) qual, " +
                "pg_get_expr(polwithcheck, polrelid) with_check " +
                "from pg_catalog.pg_policy P join pg_catalog.pg_class C on polrelid = C.oid " +
                "where relnamespace = ?::oid " +
                "and pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "order by polrelid",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 58. trigger_oids: 1 param: nsOid
        comparePs("58_trigger_oids",
                "select TG.oid from pg_catalog.pg_trigger TG, pg_catalog.pg_class TC " +
                "where TC.oid = TG.tgrelid and TC.relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 59. trigger_listing: 2 params: nsOid, "1"
        comparePs("59_trigger_listing",
                "select T.tgrelid as table_id, T.oid as trigger_id, T.xmin as trigger_state_number, " +
                "T.tgname as trigger_name, T.tgfoid as function_id, " +
                "pg_catalog.encode(T.tgargs, 'escape') as function_args, " +
                "T.tgtype as bits, T.tgdeferrable as is_deferrable, " +
                "T.tginitdeferred as is_init_deferred, T.tgenabled as trigger_fire_mode, " +
                "T.tgattr as columns, T.tgconstraint != 0 as is_constraint, " +
                "T.tgoldtable as old_table_name, T.tgnewtable as new_table_name, " +
                "pg_catalog.pg_get_triggerdef(T.oid, true) as source_code " +
                "from pg_catalog.pg_trigger T " +
                "join pg_catalog.pg_class TAB on TAB.oid = T.tgrelid and TAB.relnamespace = ?::oid " +
                "where true " +
                "and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "and not T.tgisinternal",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 60. description_oids: 11 params: ALL nsOid
        comparePs("60_description_oids",
                "select D.objoid id, pg_catalog.array_agg(D.objsubid) sub_ids " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_class C on D.objoid = C.oid " +
                "where C.relnamespace = ?::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass " +
                "group by D.objoid " +
                "union all " +
                "select T.oid id, pg_catalog.array_agg(D.objsubid) " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid " +
                "left join pg_catalog.pg_class C on T.typrelid = C.oid " +
                "where T.typnamespace = ?::oid and (C.relkind = 'c' or C.relkind is null) " +
                "group by T.oid " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_constraint C on D.objoid = C.oid " +
                "where C.connamespace = ?::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_trigger T on T.oid = D.objoid " +
                "join pg_catalog.pg_class C on C.oid = T.tgrelid " +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_rewrite R on R.oid = D.objoid " +
                "join pg_catalog.pg_class C on C.oid = R.ev_class " +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_proc P on P.oid = D.objoid " +
                "where P.pronamespace = ?::oid and D.classoid = 'pg_catalog.pg_proc'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_operator O on O.oid = D.objoid " +
                "where O.oprnamespace = ?::oid and D.classoid = 'pg_catalog.pg_operator'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_opclass O on O.oid = D.objoid " +
                "where O.opcnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_opfamily O on O.oid = D.objoid " +
                "where O.opfnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_collation C on C.oid = D.objoid " +
                "where C.collnamespace = ?::oid and D.classoid = 'pg_catalog.pg_collation'::regclass " +
                "union all " +
                "select D.objoid id, array[D.objsubid] " +
                "from pg_catalog.pg_description D " +
                "join pg_catalog.pg_policy P on P.oid = D.objoid " +
                "join pg_catalog.pg_class C on P.polrelid = C.oid " +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_policy'::regclass",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // 61. description_full: 22 params: alternating nsOid, "1"
        comparePs("61_description_full",
                "select D.objoid id, C.relkind::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_class C on D.objoid = C.oid " +
                "where C.relnamespace = ?::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select T.oid id, 'T'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid " +
                "left join pg_catalog.pg_class C on T.typrelid = C.oid " +
                "where T.typnamespace = ?::oid and (C.relkind = 'c' or C.relkind is null) " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, pg_catalog.translate(C.contype, 'pufc', 'kkxz')::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_constraint C on D.objoid = C.oid " +
                "where C.connamespace = ?::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 't'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_trigger T on T.oid = D.objoid " +
                "join pg_catalog.pg_class C on C.oid = T.tgrelid " +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'R'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_rewrite R on R.oid = D.objoid " +
                "join pg_catalog.pg_class C on C.oid = R.ev_class " +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'F'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_proc P on P.oid = D.objoid " +
                "where P.pronamespace = ?::oid and D.classoid = 'pg_catalog.pg_proc'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'O'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_operator O on O.oid = D.objoid " +
                "where O.oprnamespace = ?::oid and D.classoid = 'pg_catalog.pg_operator'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'f'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_opfamily O on O.oid = D.objoid " +
                "where O.opfnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'c'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_opclass O on O.oid = D.objoid " +
                "where O.opcnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'C'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_collation C on C.oid = D.objoid " +
                "where C.collnamespace = ?::oid and D.classoid = 'pg_catalog.pg_collation'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "union all " +
                "select D.objoid id, 'P'::char as kind, D.objsubid sub_id, D.description " +
                "from pg_catalog.pg_description D join pg_catalog.pg_policy P on P.oid = D.objoid " +
                "join pg_catalog.pg_class C on P.polrelid = C.oid " +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_policy'::regclass " +
                "and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1", memNsOid, "1"},
                new Object[]{pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1", pgNsOid, "1"});

        // 62. acl_schema: 6 params: nsOid, "1", nsOid, "1", nsOid, "1"
        comparePs("62_acl_schema",
                "select T.oid as object_id, T.relacl as acl " +
                "from pg_catalog.pg_class T where relnamespace = ?::oid " +
                "and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid) " +
                "union all " +
                "select T.oid as object_id, T.proacl as acl " +
                "from pg_catalog.pg_proc T where pronamespace = ?::oid " +
                "and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid) " +
                "union all " +
                "select T.oid as object_id, T.typacl as acl " +
                "from pg_catalog.pg_type T where typnamespace = ?::oid " +
                "and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid) " +
                "order by object_id",
                memConn, pgConn,
                new Object[]{memNsOid, "1", memNsOid, "1", memNsOid, "1"},
                new Object[]{pgNsOid, "1", pgNsOid, "1", pgNsOid, "1"});

        // 63. attr_acl: 2 params: nsOid, "1"
        comparePs("63_attr_acl",
                "select T.oid as object_id, A.attnum as attr_position, A.attacl as acl " +
                "from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid " +
                "where relnamespace = ?::oid and attnum > 0 " +
                "and pg_catalog.age(A.xmin) <= pg_catalog.age(?::varchar::xid) " +
                "order by object_id, attr_position",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 64. depend_seq: 1 param: nsOid
        comparePs("64_depend_seq",
                "select D.objid as dependent_id, D.refobjid as owner_id, " +
                "D.refobjsubid as owner_subobject_id " +
                "from pg_depend D " +
                "join pg_class C_SEQ on D.objid = C_SEQ.oid and D.classid = 'pg_class'::regclass::oid " +
                "join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid " +
                "where C_SEQ.relkind = 'S' and C_TAB.relkind = 'r' " +
                "and D.refobjsubid <> 0 and (D.deptype = 'a' or D.deptype = 'i') " +
                "and C_TAB.relnamespace = ?::oid order by owner_id",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // 65. view_listing: 3 params: nsOid, "1", "1"
        comparePs("65_view_listing",
                "select T.relkind as view_kind, T.oid as view_id, " +
                "pg_catalog.pg_get_viewdef(T.oid, true) as source_text " +
                "from pg_catalog.pg_class T join pg_catalog.pg_namespace N on T.relnamespace = N.oid " +
                "where N.oid = ?::oid and T.relkind in ('m','v') " +
                "and (pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "or exists(select A.attrelid from pg_catalog.pg_attribute A " +
                "where A.attrelid = T.oid and pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)))",
                memConn, pgConn,
                new Object[]{memNsOid, "1", "1"}, new Object[]{pgNsOid, "1", "1"});

        // 66. rule_source: 2 params: nsOid, "1"
        comparePs("66_rule_source",
                "with A as ( select oid as table_id, pg_catalog.upper(relkind) as table_kind " +
                "from pg_catalog.pg_class where relnamespace = ?::oid " +
                "and relkind in ('r', 'm', 'v', 'f', 'p') ) " +
                "select table_kind, table_id, R.oid as rule_id, " +
                "pg_catalog.pg_get_ruledef(R.oid, true) as source_text " +
                "from A join pg_catalog.pg_rewrite R on A.table_id = R.ev_class " +
                "where R.rulename != '_RETURN'::name " +
                "and pg_catalog.age(R.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});

        // 67. routine_source: 2 params: nsOid, "1"
        comparePs("67_routine_source",
                "with system_languages as ( select oid as lang " +
                "from pg_catalog.pg_language where lanname in ('c','internal') ) " +
                "select oid as id, pg_catalog.pg_get_function_arguments(oid) as arguments_def, " +
                "pg_catalog.pg_get_function_result(oid) as result_def, " +
                "pg_catalog.pg_get_function_sqlbody(oid) as sqlbody_def, " +
                "prosrc as source_text " +
                "from pg_catalog.pg_proc where pronamespace = ?::oid " +
                "and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) " +
                "and not (prokind = 'a') " +
                "and prolang not in (select lang from system_languages) " +
                "and prosrc is not null",
                memConn, pgConn,
                new Object[]{memNsOid, "1"}, new Object[]{pgNsOid, "1"});


        // =====================================================================
        // SUMMARY
        // =====================================================================
        System.out.println("\n========================================");
        System.out.println("SUMMARY");
        System.out.println("========================================");
        System.out.println("OK:           " + totalOk);
        System.out.println("Differences:  " + totalDiff);
        System.out.println("MEM errors:   " + totalMemErr);
        System.out.println("PG errors:    " + totalPgErr);
        System.out.println("Both errors:  " + totalBothErr);
        System.out.println("Total:        " + (totalOk + totalDiff + totalMemErr + totalPgErr + totalBothErr));

        // Cleanup
        execSafe(pgConn, "DROP TABLE IF EXISTS users");
        memConn.close();
        pgConn.close();
        memgres.close();
    }

    // =========================================================================
    // Core comparison logic using PreparedStatement
    // =========================================================================

    static void comparePs(String name, String sql, Connection memConn, Connection pgConn,
                          Object[] memParams, Object[] pgParams) {
        System.out.println("--- " + name + " ---");
        List<Map<String, String>> memRows = runPs(memConn, sql, memParams, "MEM");
        List<Map<String, String>> pgRows = runPs(pgConn, sql, pgParams, "PG");

        if (memRows == null && pgRows == null) {
            System.out.println("  BOTH ERROR");
            totalBothErr++;
        } else if (memRows == null) {
            System.out.println("  *** MEMGRES ERROR, PG returned " + pgRows.size() + " rows");
            totalMemErr++;
        } else if (pgRows == null) {
            System.out.println("  *** PG ERROR, Memgres returned " + memRows.size() + " rows");
            totalPgErr++;
        } else if (memRows.size() != pgRows.size()) {
            System.out.println("  *** ROW COUNT DIFF: Memgres=" + memRows.size() + " PG=" + pgRows.size());
            printRowDiff(memRows, pgRows);
            totalDiff++;
        } else {
            // Compare column names
            if (!memRows.isEmpty() && !pgRows.isEmpty()) {
                Set<String> memCols = memRows.get(0).keySet();
                Set<String> pgCols = pgRows.get(0).keySet();
                if (!memCols.equals(pgCols)) {
                    System.out.println("  *** COLUMN DIFF: Memgres=" + memCols + " PG=" + pgCols);
                    totalDiff++;
                    System.out.println();
                    return;
                }
            }
            // Check for value differences
            int diffs = 0;
            for (int i = 0; i < Math.min(memRows.size(), pgRows.size()); i++) {
                Map<String, String> mr = memRows.get(i);
                Map<String, String> pr = pgRows.get(i);
                for (String col : mr.keySet()) {
                    if (!pr.containsKey(col)) continue;
                    if (isSkippedColumn(col)) continue;
                    String mv = mr.get(col);
                    String pv = pr.get(col);
                    if (!Objects.equals(mv, pv)) {
                        if (diffs < 20) {
                            System.out.println("  row " + i + " col " + col + ": MEM=" + trunc(mv) + "  PG=" + trunc(pv));
                        }
                        diffs++;
                    }
                }
            }
            if (diffs == 0) {
                System.out.println("  OK (" + memRows.size() + " rows)");
                totalOk++;
            } else {
                System.out.println("  *** " + diffs + " value differences in " + memRows.size() + " rows");
                totalDiff++;
            }
        }
        System.out.println();
    }

    static boolean isSkippedColumn(String col) {
        String lc = col.toLowerCase();
        // Skip OID-valued columns
        if (lc.contains("oid")) return true;
        if (lc.endsWith("_id")) return true;
        if (lc.equals("id")) return true;
        if (lc.contains("state_number")) return true;
        if (lc.equals("xmin")) return true;
        if (lc.equals("schemaid")) return true;
        if (lc.equals("schemaoid")) return true;
        if (lc.equals("handler_id")) return true;
        if (lc.equals("family_id")) return true;
        if (lc.equals("access_method_id")) return true;
        // Additional columns that contain OIDs
        if (lc.equals("con_state_id")) return true;
        if (lc.equals("index_id")) return true;
        if (lc.equals("table_id")) return true;
        if (lc.equals("sequence_id")) return true;
        if (lc.equals("class_id")) return true;
        if (lc.equals("base_type_id")) return true;
        if (lc.equals("type_id")) return true;
        if (lc.equals("trigger_id")) return true;
        if (lc.equals("rule_id")) return true;
        if (lc.equals("view_id")) return true;
        if (lc.equals("schema_id")) return true;
        // xid/state columns
        if (lc.contains("state_number")) return true;
        if (lc.equals("column_state_number")) return true;
        if (lc.equals("trigger_state_number")) return true;
        if (lc.equals("rule_state_number")) return true;
        if (lc.equals("type_state_number")) return true;
        if (lc.equals("sequence_state_number")) return true;
        if (lc.equals("r_state_number")) return true;
        if (lc.equals("table_state_number")) return true;
        return false;
    }

    static List<Map<String, String>> runPs(Connection conn, String sql, Object[] params, String label) {
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                Object p = params[i];
                if (p instanceof Integer) {
                    ps.setInt(i + 1, (Integer) p);
                } else if (p instanceof String) {
                    ps.setString(i + 1, (String) p);
                } else if (p instanceof Long) {
                    ps.setLong(i + 1, (Long) p);
                } else {
                    ps.setObject(i + 1, p);
                }
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            List<Map<String, String>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int c = 1; c <= colCount; c++) {
                    row.put(md.getColumnLabel(c), rs.getString(c));
                }
                rows.add(row);
            }
            rs.close();
            ps.close();
            return rows;
        } catch (SQLException e) {
            String msg = e.getMessage() != null ? e.getMessage().replaceAll("\\n", " ") : "null";
            System.out.println("  " + label + " ERROR: " + msg.substring(0, Math.min(200, msg.length())));
            return null;
        }
    }

    static void printRowDiff(List<Map<String, String>> memRows, List<Map<String, String>> pgRows) {
        // Find a "name" column
        String nameCol = findNameColumn(memRows);
        if (nameCol == null) nameCol = findNameColumn(pgRows);

        if (nameCol != null) {
            Set<String> memNames = new TreeSet<>();
            for (Map<String, String> r : memRows) { String v = r.get(nameCol); if (v != null) memNames.add(v); }
            Set<String> pgNames = new TreeSet<>();
            for (Map<String, String> r : pgRows) { String v = r.get(nameCol); if (v != null) pgNames.add(v); }
            Set<String> onlyMem = new TreeSet<>(memNames); onlyMem.removeAll(pgNames);
            Set<String> onlyPg = new TreeSet<>(pgNames); onlyPg.removeAll(memNames);
            if (!onlyMem.isEmpty()) {
                System.out.println("  Only in MEM (" + nameCol + "): " + (onlyMem.size() > 30 ? onlyMem.size() + " items" : onlyMem));
            }
            if (!onlyPg.isEmpty()) {
                System.out.println("  Only in PG (" + nameCol + "): " + (onlyPg.size() > 30 ? onlyPg.size() + " items" : onlyPg));
            }
        } else {
            if (memRows.size() <= 5) System.out.println("  MEM rows: " + memRows);
            if (pgRows.size() <= 5) System.out.println("  PG rows: " + pgRows);
        }
    }

    static String findNameColumn(List<Map<String, String>> rows) {
        if (rows == null || rows.isEmpty()) return null;
        for (String col : rows.get(0).keySet()) {
            if (col.contains("name") && !col.contains("schema") && !col.contains("handler") && !col.contains("validator")) {
                return col;
            }
        }
        // Fallback: look for "kind" column
        for (String col : rows.get(0).keySet()) {
            if (col.equals("kind")) return col;
        }
        return null;
    }

    static void execSafe(Connection conn, String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            System.out.println("  EXEC ERROR: " + e.getMessage());
        }
    }

    static int getOid(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    static String getStr(Connection conn, String sql) {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) return rs.getString(1);
            return "null";
        } catch (SQLException e) {
            return "ERROR: " + e.getMessage();
        }
    }

    static String trunc(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 77) + "..." : s;
    }
}
