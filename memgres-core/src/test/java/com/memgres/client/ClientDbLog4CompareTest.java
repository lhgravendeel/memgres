package com.memgres.client;

import com.memgres.core.Memgres;

import java.sql.*;
import java.util.*;

/**
 * Runs all queries from db-log-4.txt against both Memgres and PG 18, comparing results.
 * Database is EMPTY (no user tables). Tests system catalog behavior only.
 * Uses extended query protocol (PreparedStatement, NO preferQueryMode=simple).
 * Run as main(). Requires PG 18 on localhost:5432 with memgrestest/memgres/memgres.
 */
public class ClientDbLog4CompareTest {

    static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest";
    static final String PG_USER = "memgres";
    static final String PG_PASS = "memgres";

    static int totalOk = 0;
    static int totalDiff = 0;
    static int totalMemErr = 0;
    static int totalPgErr = 0;
    static int totalBothErr = 0;
    static List<String> okQueries = new ArrayList<>();
    static List<String> diffQueries = new ArrayList<>();
    static List<String> memErrQueries = new ArrayList<>();
    static List<String> pgErrQueries = new ArrayList<>();
    static List<String> bothErrQueries = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        // Start Memgres (no preferQueryMode=simple, uses extended protocol)
        Memgres memgres = Memgres.builder().port(0).build().start();
        String memUrl = memgres.getJdbcUrl();
        Connection memConn = DriverManager.getConnection(memUrl, memgres.getUser(), memgres.getPassword());
        memConn.setAutoCommit(true);

        // Connect to PG 18 (no preferQueryMode=simple)
        Connection pgConn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
        pgConn.setAutoCommit(true);

        // NO user tables; database is empty. Only system catalog queries.

        // Get namespace OIDs for public schema
        int memNsOid = getOid(memConn, "SELECT oid FROM pg_namespace WHERE nspname = 'public'");
        int pgNsOid = getOid(pgConn, "SELECT oid FROM pg_namespace WHERE nspname = 'public'");
        System.out.println("Memgres public nsOid=" + memNsOid + ", PG public nsOid=" + pgNsOid);

        // Get txid from each (for xid-parameterized queries)
        String memTxid = getStr(memConn, "select case when pg_catalog.pg_is_in_recovery() then null else (pg_catalog.txid_current() % 4294967296)::varchar::bigint end as current_txid");
        String pgTxid = getStr(pgConn, "select case when pg_catalog.pg_is_in_recovery() then null else (pg_catalog.txid_current() % 4294967296)::varchar::bigint end as current_txid");
        System.out.println("Memgres txid=" + memTxid + ", PG txid=" + pgTxid);

        System.out.println("\n=== db-log-4.txt Query Comparison: Memgres vs PG 18 (Extended Protocol, Empty DB) ===\n");

        // =====================================================================
        // GLOBAL QUERIES (no parameters): queries 1-27 from db-log-4.txt
        // =====================================================================

        // Q2: select version()
        comparePs("Q02_version",
                "select version()",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q3: current_database/schemas
        comparePs("Q03_current_db_schemas",
                "select current_database() as a, current_schemas(false) as b",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q4: current_database/schema/user
        comparePs("Q04_current_all",
                "select current_database(), current_schema(), current_user",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q5: startup_time
        comparePs("Q05_startup_time",
                "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q6: pg_locks transaction_id
        comparePs("Q06_pg_locks_txid",
                "select L.transactionid::varchar::bigint as transaction_id\n" +
                "from pg_catalog.pg_locks L\n" +
                "where L.transactionid is not null\n" +
                "order by pg_catalog.age(L.transactionid) desc\n" +
                "limit 1",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q7: txid_current
        comparePs("Q07_txid_current",
                "select case\n" +
                "  when pg_catalog.pg_is_in_recovery()\n" +
                "    then null\n" +
                "  else\n" +
                "    (pg_catalog.txid_current() % 4294967296)::varchar::bigint\n" +
                "  end as current_txid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q8: pg_database listing
        comparePs("Q08_pg_database",
                "select N.oid::bigint as id,\n" +
                "       datname as name,\n" +
                "       D.description,\n" +
                "       datistemplate as is_template,\n" +
                "       datallowconn as allow_connections,\n" +
                "       pg_catalog.pg_get_userbyid(N.datdba) as \"owner\"\n" +
                "from pg_catalog.pg_database N\n" +
                "  left join pg_catalog.pg_shdescription D on N.oid = D.objoid\n" +
                "order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q9: show DateStyle
        comparePs("Q09_show_datestyle",
                "show DateStyle",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q10: timezone_names union
        comparePs("Q10_timezone_names",
                "select name, is_dst from pg_catalog.pg_timezone_names\n" +
                "union distinct\n" +
                "select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q11: pg_roles listing
        comparePs("Q11_pg_roles",
                "select R.oid::bigint as role_id, rolname as role_name,\n" +
                "  rolsuper is_super, rolinherit is_inherit,\n" +
                "  rolcreaterole can_createrole, rolcreatedb can_createdb,\n" +
                "  rolcanlogin can_login, rolreplication is_replication,\n" +
                "  rolconnlimit conn_limit, rolvaliduntil valid_until,\n" +
                "  rolbypassrls bypass_rls, rolconfig config,\n" +
                "  D.description\n" +
                "from pg_catalog.pg_roles R\n" +
                "  left join pg_catalog.pg_shdescription D on D.objoid = R.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q12: pg_auth_members
        comparePs("Q12_pg_auth_members",
                "select member id, roleid role_id, admin_option\n" +
                "          from pg_catalog.pg_auth_members order by id, roleid::text",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q13: pg_tablespace
        comparePs("Q13_pg_tablespace",
                "select T.oid::bigint as id, T.spcname as name,\n" +
                "       T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,\n" +
                "       pg_catalog.pg_tablespace_location(T.oid) as location,\n" +
                "       T.spcoptions as options,\n" +
                "       D.description as comment\n" +
                "from pg_catalog.pg_tablespace T\n" +
                "  left join pg_catalog.pg_shdescription D on D.objoid = T.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q14: tablespace+database ACL union
        comparePs("Q14_tablespace_db_acl",
                "select T.oid as object_id,\n" +
                "                 T.spcacl as acl\n" +
                "          from pg_catalog.pg_tablespace T\n" +
                "          union all\n" +
                "          select T.oid as object_id,\n" +
                "                 T.datacl as acl\n" +
                "          from pg_catalog.pg_database T",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q15: pg_namespace listing
        comparePs("Q15_pg_namespace",
                "select N.oid::bigint as id,\n" +
                "       N.xmin as state_number,\n" +
                "       nspname as name,\n" +
                "       D.description,\n" +
                "       pg_catalog.pg_get_userbyid(N.nspowner) as \"owner\"\n" +
                "from pg_catalog.pg_namespace N\n" +
                "  left join pg_catalog.pg_description D on N.oid = D.objoid\n" +
                "order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q16: usesuper from pg_user
        comparePs("Q16_usesuper",
                "select usesuper\nfrom pg_user\nwhere usename = current_user",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q17: pg_event_trigger listing
        comparePs("Q17_pg_event_trigger",
                "select t.oid as id,\n" +
                "       t.xmin as state_number,\n" +
                "       t.evtname as name,\n" +
                "       t.evtevent as event,\n" +
                "       t.evtfoid as routine_id,\n" +
                "       pg_catalog.pg_get_userbyid(t.evtowner) as owner,\n" +
                "       t.evttags as tags,\n" +
                "       case when t.evtenabled = 'D' then 1 else 0 end as is_disabled\n" +
                "from pg_catalog.pg_event_trigger t",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q18: pg_foreign_data_wrapper listing
        comparePs("Q18_pg_fdw",
                "select fdw.oid as id,\n" +
                "       fdw.xmin as state_number,\n" +
                "       fdw.fdwname as name,\n" +
                "       pr.proname as handler,\n" +
                "       nspc.nspname as handler_schema,\n" +
                "       pr2.proname as validator,\n" +
                "       nspc2.nspname as validator_schema,\n" +
                "       fdw.fdwoptions as options,\n" +
                "       pg_catalog.pg_get_userbyid(fdw.fdwowner) as \"owner\"\n" +
                "from pg_catalog.pg_foreign_data_wrapper fdw\n" +
                "     left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid\n" +
                "     left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid\n" +
                "     left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid\n" +
                "     left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q19: pg_foreign_server listing
        comparePs("Q19_pg_foreign_server",
                "select srv.oid as id,\n" +
                "       srv.srvfdw as fdw_id,\n" +
                "       srv.xmin as state_number,\n" +
                "       srv.srvname as name,\n" +
                "       srv.srvtype as type,\n" +
                "       srv.srvversion as version,\n" +
                "       srv.srvoptions as options,\n" +
                "       pg_catalog.pg_get_userbyid(srv.srvowner) as \"owner\"\n" +
                "from pg_catalog.pg_foreign_server srv",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q20: pg_user_mapping listing
        comparePs("Q20_pg_user_mapping",
                "select oid as id,\n" +
                "       umserver as server_id,\n" +
                "       case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as user,\n" +
                "       umoptions as options\n" +
                "from pg_catalog.pg_user_mapping\n" +
                "order by server_id",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q21: pg_am listing
        comparePs("Q21_pg_am",
                "select A.oid as access_method_id,\n" +
                "       A.xmin as state_number,\n" +
                "       A.amname as access_method_name\n" +
                "       ,\n" +
                "       A.amhandler::oid as handler_id,\n" +
                "       pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,\n" +
                "       A.amtype as access_method_type\n" +
                "\n" +
                "from pg_am A\n" +
                "     join pg_proc P on A.amhandler::oid = P.oid\n" +
                "     join pg_namespace N on P.pronamespace = N.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q22: pg_extension listing
        comparePs("Q22_pg_extension",
                "select E.oid        as id,\n" +
                "       E.xmin       as state_number,\n" +
                "       extname      as name,\n" +
                "       extversion   as version,\n" +
                "       extnamespace as schema_id,\n" +
                "       nspname      as schema_name\n" +
                "       ,\n" +
                "       array(select unnest\n" +
                "             from unnest(available_versions)\n" +
                "             where unnest > extversion) as available_updates\n" +
                "\n" +
                "from pg_catalog.pg_extension E\n" +
                "       join pg_namespace N on E.extnamespace = N.oid\n" +
                "       left join (select name, array_agg(version) as available_versions\n" +
                "                  from pg_available_extension_versions()\n" +
                "                  group by name) V on E.extname = V.name",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q23: pg_language listing
        comparePs("Q23_pg_language",
                "select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted,\n" +
                "       h.proname as handler, hs.nspname as handlerSchema,\n" +
                "       i.proname as inline, isc.nspname as inlineSchema,\n" +
                "       v.proname as validator, vs.nspname as validatorSchema\n" +
                "from pg_catalog.pg_language l\n" +
                "    left join pg_catalog.pg_proc h on h.oid = lanplcallfoid\n" +
                "    left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace\n" +
                "    left join pg_catalog.pg_proc i on i.oid = laninline\n" +
                "    left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace\n" +
                "    left join pg_catalog.pg_proc v on v.oid = lanvalidator\n" +
                "    left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace\n" +
                "order by lanname",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q24: pg_description global
        comparePs("Q24_desc_global",
                "select D.objoid id, case\n" +
                "    when 'pg_catalog.pg_event_trigger'::regclass = classoid then 'T'\n" +
                "    when 'pg_catalog.pg_am'::regclass = classoid then 'A'\n" +
                "    when 'pg_catalog.pg_cast'::regclass = classoid then 'C'\n" +
                "    when 'pg_catalog.pg_foreign_data_wrapper'::regclass = classoid then 'W'\n" +
                "    when 'pg_catalog.pg_foreign_server'::regclass = classoid then 'S'\n" +
                "    when 'pg_catalog.pg_language'::regclass = classoid then 'L'\n" +
                "    when 'pg_catalog.pg_extension'::regclass = classoid then 'E'\n" +
                "\n" +
                "  end as kind,\n" +
                "  D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "where classoid in (\n" +
                "  'pg_catalog.pg_event_trigger'::regclass,\n" +
                "  'pg_catalog.pg_am'::regclass,\n" +
                "  'pg_catalog.pg_cast'::regclass,\n" +
                "  'pg_catalog.pg_foreign_data_wrapper'::regclass,\n" +
                "  'pg_catalog.pg_foreign_server'::regclass,\n" +
                "  'pg_catalog.pg_language'::regclass\n" +
                "  ,\n" +
                "  'pg_catalog.pg_extension'::regclass\n" +
                "\n" +
                ")",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q25: ACL union (fdw/language/namespace/foreign_server)
        comparePs("Q25_acl_global",
                "select T.oid as object_id,\n" +
                "                 T.fdwacl as acl\n" +
                "          from pg_catalog.pg_foreign_data_wrapper T\n" +
                "          union all\n" +
                "          select T.oid as object_id,\n" +
                "                 T.lanacl as acl\n" +
                "          from pg_catalog.pg_language T\n" +
                "          union all\n" +
                "          select T.oid as object_id,\n" +
                "                 T.nspacl as acl\n" +
                "          from pg_catalog.pg_namespace T\n" +
                "          union all\n" +
                "          select T.oid as object_id,\n" +
                "                 T.srvacl as acl\n" +
                "          from pg_catalog.pg_foreign_server T",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q26: pg_cast listing
        comparePs("Q26_pg_cast",
                "select C.oid,\n" +
                "       C.xmin as state_number,\n" +
                "       C.castsource as castsource_id,\n" +
                "       pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name,\n" +
                "       C.casttarget as casttarget_id,\n" +
                "       pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name,\n" +
                "       C.castfunc as castfunc_id,\n" +
                "       pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name,\n" +
                "       C.castcontext,\n" +
                "       C.castmethod\n" +
                "from pg_cast C\n" +
                "     left outer join pg_proc F on C.castfunc::oid = F.oid\n" +
                "     left outer join pg_namespace FN on F.pronamespace = FN.oid\n" +
                "     join pg_type S on C.castsource::oid = S.oid\n" +
                "     join pg_namespace SN on S.typnamespace = SN.oid\n" +
                "     join pg_type T on C.casttarget::oid = T.oid\n" +
                "     join pg_namespace TN on T.typnamespace = TN.oid",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // Q27: pg_depend extension members
        comparePs("Q27_extension_depend",
                "select E.oid   as extension_id,\n" +
                "       D.objid as member_id\n" +
                "from pg_extension E\n" +
                "     join pg_depend D on E.oid = D.refobjid and\n" +
                "                         D.refclassid = 'pg_extension'::regclass::oid\n" +
                "where D.deptype = 'e'\n" +
                "order by extension_id",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // =====================================================================
        // PARAMETERIZED QUERIES (bind nsOid for public schema)
        // All use ?::oid for namespace OID parameters
        // =====================================================================

        // Q28: object listing big UNION, 7 params all nsOid
        comparePs("Q28_big_union_oids",
                "select T.oid as oid,\n" +
                "       relnamespace as schemaId,\n" +
                "       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind\n" +
                "from pg_catalog.pg_class T\n" +
                "where relnamespace in ( ? )\n" +
                "  and relkind in ('r', 'm', 'v', 'p', 'f', 'S')\n" +
                "union all\n" +
                "select T.oid,\n" +
                "       T.typnamespace,\n" +
                "       'T' as kind\n" +
                "from pg_catalog.pg_type T\n" +
                "     left outer join pg_catalog.pg_class C on T.typrelid = C.oid\n" +
                "where T.typnamespace in ( ? )\n" +
                "  and ( T.typtype in ('d','e') or\n" +
                "        C.relkind = 'c'::\"char\" or\n" +
                "        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or\n" +
                "        T.typtype = 'p' and not T.typisdefined )\n" +
                "union all\n" +
                "select oid,\n" +
                "       collnamespace,\n" +
                "       'C' as kind\n" +
                "from pg_catalog.pg_collation\n" +
                "where collnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       oprnamespace,\n" +
                "       'O' as kind\n" +
                "from pg_catalog.pg_operator\n" +
                "where oprnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       opcnamespace,\n" +
                "       'c' as kind\n" +
                "from pg_catalog.pg_opclass\n" +
                "where opcnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       opfnamespace,\n" +
                "       'F' as kind\n" +
                "from pg_catalog.pg_opfamily\n" +
                "where opfnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       pronamespace,\n" +
                "       case when prokind != 'a' then 'R'\n" +
                "            else 'a'\n" +
                "            end as kind\n" +
                "from pg_catalog.pg_proc\n" +
                "where pronamespace in ( ? )",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // Q29: object listing with names UNION, 7 params all nsOid
        comparePs("Q29_big_union_names",
                "select T.oid as oid,\n" +
                "       relnamespace as schemaId,\n" +
                "       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind,\n" +
                "       relname as name\n" +
                "from pg_catalog.pg_class T\n" +
                "where relnamespace in ( ? )\n" +
                "  and relkind in ('r', 'm', 'v', 'p', 'f', 'S')\n" +
                "union all\n" +
                "select T.oid,\n" +
                "       T.typnamespace,\n" +
                "       'T',\n" +
                "       T.typname\n" +
                "from pg_catalog.pg_type T\n" +
                "     left outer join pg_catalog.pg_class C on T.typrelid = C.oid\n" +
                "where T.typnamespace in ( ? )\n" +
                "  and ( T.typtype in ('d','e') or\n" +
                "        C.relkind = 'c'::\"char\" or\n" +
                "        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or\n" +
                "        T.typtype = 'p' and not T.typisdefined )\n" +
                "union all\n" +
                "select oid,\n" +
                "       collnamespace,\n" +
                "       'C',\n" +
                "       collname\n" +
                "from pg_catalog.pg_collation\n" +
                "where collnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       oprnamespace,\n" +
                "       'O',\n" +
                "       oprname\n" +
                "from pg_catalog.pg_operator\n" +
                "where oprnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       opcnamespace,\n" +
                "       'c',\n" +
                "       opcname\n" +
                "from pg_catalog.pg_opclass\n" +
                "where opcnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       opfnamespace,\n" +
                "       'F',\n" +
                "       opfname\n" +
                "from pg_catalog.pg_opfamily\n" +
                "where opfnamespace in ( ? )\n" +
                "union all\n" +
                "select oid,\n" +
                "       pronamespace,\n" +
                "       case when prokind != 'a' then 'R'\n" +
                "            else 'a'\n" +
                "            end,\n" +
                "       proname\n" +
                "from pg_catalog.pg_proc\n" +
                "where pronamespace in ( ? )\n" +
                "order by schemaId",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // Q30: pg_proc arg info, 1 param nsOid
        comparePs("Q30_proc_args",
                "select pronamespace as schemaId,\n" +
                "       oid as majorOid,\n" +
                "       proargnames as argNames,\n" +
                "       proargmodes as argModes,\n" +
                "       array_length(proargtypes, 1) as nArgs\n" +
                "from pg_catalog.pg_proc\n" +
                "where pronamespace in ( ? )\n" +
                "order by schemaId",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q31: pg_attribute CTE, 1 param nsOid
        comparePs("Q31_column_names",
                "with T as ( select T.oid as oid,\n" +
                "                                     T.relkind as kind,\n" +
                "                                     T.relnamespace as schemaId\n" +
                "            from pg_catalog.pg_class T\n" +
                "            where T.relnamespace in ( ? )\n" +
                "              and T.relkind in ('r', 'm', 'v', 'f', 'p')\n" +
                "          )\n" +
                "select T.schemaId as schemaId,\n" +
                "       T.oid as majorOid,\n" +
                "       pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind,\n" +
                "       C.attnum as position,\n" +
                "       C.attname as name\n" +
                "from T\n" +
                "     join pg_catalog.pg_attribute C on T.oid = C.attrelid\n" +
                "where C.attnum > 0\n" +
                "  and not C.attisdropped\n" +
                "order by schemaId, majorOid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q32: pg_sequence listing, 1 param nsOid
        comparePs("Q32_sequence_listing",
                "select cls.xmin as sequence_state_number,\n" +
                "       sq.seqrelid as sequence_id,\n" +
                "       cls.relname as sequence_name,\n" +
                "       pg_catalog.format_type(sq.seqtypid, null) as data_type,\n" +
                "       sq.seqstart as start_value,\n" +
                "       sq.seqincrement as inc_value,\n" +
                "       sq.seqmin as min_value,\n" +
                "       sq.seqmax as max_value,\n" +
                "       sq.seqcache as cache_size,\n" +
                "       sq.seqcycle as cycle_option,\n" +
                "       pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\"\n" +
                "from pg_catalog.pg_sequence sq\n" +
                "    join pg_class cls on sq.seqrelid = cls.oid\n" +
                "    where cls.relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q33: pg_type listing, 1 param nsOid
        comparePs("Q33_type_listing",
                "select T.oid as type_id,\n" +
                "       T.xmin as type_state_number,\n" +
                "       T.typname as type_name,\n" +
                "       T.typtype as type_sub_kind,\n" +
                "       T.typcategory as type_category,\n" +
                "       T.typrelid as class_id,\n" +
                "       T.typbasetype as base_type_id,\n" +
                "       case when T.typtype in ('c','e') then null\n" +
                "            else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def,\n" +
                "       T.typndims as dimensions_number,\n" +
                "       T.typdefault as default_expression,\n" +
                "       T.typnotnull as mandatory,\n" +
                "       pg_catalog.pg_get_userbyid(T.typowner) as \"owner\"\n" +
                "from pg_catalog.pg_type T\n" +
                "         left outer join pg_catalog.pg_class C\n" +
                "             on T.typrelid = C.oid\n" +
                "where T.typnamespace = ?::oid\n" +
                "  and (T.typtype in ('d','e') or\n" +
                "       C.relkind = 'c'::\"char\" or\n" +
                "       (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or\n" +
                "       T.typtype = 'p' and not T.typisdefined)\n" +
                "order by 1",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q34: table listing, 1 param nsOid
        comparePs("Q34_table_listing",
                "select T.relkind as table_kind,\n" +
                "       T.relname as table_name,\n" +
                "       T.oid as table_id,\n" +
                "       T.xmin as table_state_number,\n" +
                "       false as table_with_oids,\n" +
                "       T.reltablespace as tablespace_id,\n" +
                "       T.reloptions as options,\n" +
                "       T.relpersistence as persistence,\n" +
                "       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,\n" +
                "       (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,\n" +
                "       T.relispartition as is_partition,\n" +
                "       pg_catalog.pg_get_partkeydef(T.oid) as partition_key,\n" +
                "       pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression,\n" +
                "       T.relam am_id,\n" +
                "       pg_catalog.pg_get_userbyid(T.relowner) as \"owner\"\n" +
                "from pg_catalog.pg_class T\n" +
                "where relnamespace = ?::oid\n" +
                "       and relkind in ('r', 'm', 'v', 'f', 'p')\n" +
                "order by table_kind, table_id",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q35: pg_foreign_table, 1 param nsOid
        comparePs("Q35_foreign_table",
                "select ft.ftrelid as table_id,\n" +
                "       srv.srvname as table_server,\n" +
                "       ft.ftoptions as table_options,\n" +
                "       pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\"\n" +
                "from pg_catalog.pg_foreign_table ft\n" +
                "     left outer join pg_catalog.pg_foreign_server srv on ft.ftserver = srv.oid\n" +
                "     join pg_catalog.pg_class cls on ft.ftrelid = cls.oid\n" +
                "where cls.relnamespace = ?::oid\n" +
                "order by table_id",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q36: schema_arg_types CTE, 3 params all nsOid
        comparePs("Q36_schema_arg_types",
                "with schema_procs as (select prorettype, proargtypes, proallargtypes\n" +
                "                      from pg_catalog.pg_proc\n" +
                "                      where pronamespace = ?::oid),\n" +
                "     schema_opers as (select oprleft, oprright, oprresult\n" +
                "                      from pg_catalog.pg_operator\n" +
                "                      where oprnamespace = ?::oid),\n" +
                "     schema_aggregates as (select A.aggtranstype , A.aggmtranstype\n" +
                "                           from pg_catalog.pg_aggregate A\n" +
                "                           join pg_catalog.pg_proc P\n" +
                "                             on A.aggfnoid = P.oid\n" +
                "                           where P.pronamespace = ?::oid),\n" +
                "     schema_arg_types as ( select prorettype as type_id\n" +
                "                           from schema_procs\n" +
                "                           union\n" +
                "                           select distinct unnest(proargtypes) as type_id\n" +
                "                           from schema_procs\n" +
                "                           union\n" +
                "                           select distinct unnest(proallargtypes) as type_id\n" +
                "                           from schema_procs\n" +
                "                           union\n" +
                "                           select oprleft as type_id\n" +
                "                           from schema_opers\n" +
                "                           where oprleft is not null\n" +
                "                           union\n" +
                "                           select oprright as type_id\n" +
                "                           from schema_opers\n" +
                "                           where oprright is not null\n" +
                "                           union\n" +
                "                           select oprresult as type_id\n" +
                "                           from schema_opers\n" +
                "                           where oprresult is not null\n" +
                "                           union\n" +
                "                           select aggtranstype::oid as type_id\n" +
                "                           from schema_aggregates\n" +
                "                           union\n" +
                "                           select aggmtranstype::oid as type_id\n" +
                "                           from schema_aggregates\n" +
                "\n" +
                "                           )\n" +
                "select type_id, pg_catalog.format_type(type_id, null) as type_spec\n" +
                "from schema_arg_types\n" +
                "where type_id <> 0",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid});

        // Q37: routines, 1 param nsOid
        comparePs("Q37_routines",
                "with languages as (select oid as lang_oid, lanname as lang\n" +
                "                   from pg_catalog.pg_language),\n" +
                "     routines as (select proname as r_name,\n" +
                "                         prolang as lang_oid,\n" +
                "                         oid as r_id,\n" +
                "                         xmin as r_state_number,\n" +
                "                         proargnames as arg_names,\n" +
                "                         proargmodes as arg_modes,\n" +
                "                         proargtypes::int[] as in_arg_types,\n" +
                "                         proallargtypes::int[] as all_arg_types,\n" +
                "                         pg_catalog.pg_get_expr(proargdefaults, 0) as arg_defaults,\n" +
                "                         provariadic as arg_variadic_id,\n" +
                "                         prorettype as ret_type_id,\n" +
                "                         proretset as ret_set,\n" +
                "                         prokind as kind,\n" +
                "                         provolatile as volatile_kind,\n" +
                "                         proisstrict as is_strict,\n" +
                "                         prosecdef as is_security_definer,\n" +
                "                         proconfig as configuration_parameters,\n" +
                "                         procost as cost,\n" +
                "                         pg_catalog.pg_get_userbyid(proowner) as \"owner\",\n" +
                "                         prorows as rows ,\n" +
                "                         proleakproof as is_leakproof  ,\n" +
                "                         proparallel as concurrency_kind\n" +
                "                  from pg_catalog.pg_proc\n" +
                "                  where pronamespace = ?::oid\n" +
                "                    and not (prokind = 'a'))\n" +
                "select *\n" +
                "from routines natural join languages",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q38: aggregates, 1 param nsOid
        comparePs("Q38_aggregates",
                "select P.oid as aggregate_id,\n" +
                "       P.xmin as state_number,\n" +
                "       P.proname as aggregate_name,\n" +
                "       P.proargnames as arg_names,\n" +
                "       P.proargmodes as arg_modes,\n" +
                "       P.proargtypes::int[] as in_arg_types,\n" +
                "       P.proallargtypes::int[] as all_arg_types,\n" +
                "       A.aggtransfn::oid as transition_function_id,\n" +
                "       A.aggtransfn::regproc::text as transition_function_name,\n" +
                "       A.aggtranstype as transition_type,\n" +
                "       A.aggfinalfn::oid as final_function_id,\n" +
                "       case when A.aggfinalfn::oid = 0 then null else A.aggfinalfn::regproc::varchar end as final_function_name,\n" +
                "       case when A.aggfinalfn::oid = 0 then 0 else P.prorettype end as final_return_type,\n" +
                "       A.agginitval as initial_value,\n" +
                "       A.aggsortop as sort_operator_id,\n" +
                "       case when A.aggsortop = 0 then null else A.aggsortop::regoper::varchar end as sort_operator_name,\n" +
                "       pg_catalog.pg_get_userbyid(P.proowner) as \"owner\"\n" +
                "       ,\n" +
                "       A.aggfinalextra as final_extra,\n" +
                "       A.aggtransspace as state_size,\n" +
                "       A.aggmtransfn::oid as moving_transition_id,\n" +
                "       case when A.aggmtransfn::oid = 0 then null else A.aggmtransfn::regproc::varchar end as moving_transition_name,\n" +
                "       A.aggminvtransfn::oid as inverse_transition_id,\n" +
                "       case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name,\n" +
                "       A.aggmtranstype::oid as moving_state_type,\n" +
                "       A.aggmtransspace as moving_state_size,\n" +
                "       A.aggmfinalfn::oid as moving_final_id,\n" +
                "       case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name,\n" +
                "       A.aggmfinalextra as moving_final_extra,\n" +
                "       A.aggminitval as moving_initial_value,\n" +
                "       A.aggkind as aggregate_kind,\n" +
                "       A.aggnumdirectargs as direct_args\n" +
                "\n" +
                "       ,\n" +
                "       A.aggcombinefn::oid as combine_function_id,\n" +
                "       case when A.aggcombinefn::oid = 0 then null else A.aggcombinefn::regproc::varchar end as combine_function_name,\n" +
                "       A.aggserialfn::oid as serialization_function_id,\n" +
                "       case when A.aggserialfn::oid = 0 then null else A.aggserialfn::regproc::varchar end as serialization_function_name,\n" +
                "       A.aggdeserialfn::oid as deserialization_function_id,\n" +
                "       case when A.aggdeserialfn::oid = 0 then null else A.aggdeserialfn::regproc::varchar end as deserialization_function_name,\n" +
                "       P.proparallel as concurrency_kind\n" +
                "\n" +
                "from pg_catalog.pg_aggregate A\n" +
                "     join pg_catalog.pg_proc P\n" +
                "       on A.aggfnoid = P.oid\n" +
                "where P.pronamespace = ?::oid\n" +
                "order by P.oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q39: operators, 1 param nsOid
        comparePs("Q39_operators",
                "select O.oid as op_id,\n" +
                "       O.xmin as state_number,\n" +
                "       oprname as op_name,\n" +
                "       oprkind as op_kind,\n" +
                "       oprleft as arg_left_type_id,\n" +
                "       oprright as arg_right_type_id,\n" +
                "       oprresult as arg_result_type_id,\n" +
                "       oprcode::oid as main_id,\n" +
                "       oprcode::varchar as main_name,\n" +
                "       oprrest::oid as restrict_id,\n" +
                "       oprrest::varchar as restrict_name,\n" +
                "       oprjoin::oid as join_id,\n" +
                "       oprjoin::varchar as join_name,\n" +
                "       oprcom::oid as com_id,\n" +
                "       oprcom::regoper::varchar as com_name,\n" +
                "       oprnegate::oid as neg_id,\n" +
                "       oprnegate::regoper::varchar as neg_name,\n" +
                "       oprcanmerge as merges,\n" +
                "       oprcanhash as hashes,\n" +
                "       pg_catalog.pg_get_userbyid(O.oprowner) as \"owner\"\n" +
                "from pg_catalog.pg_operator O\n" +
                "where oprnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q40: collation listing, 1 param nsOid
        comparePs("Q40_collation",
                "select oid as id,\n" +
                "       xmin as state_number,\n" +
                "       collname as name,\n" +
                "       collcollate as lc_collate,\n" +
                "       collctype as lc_ctype,\n" +
                "       pg_catalog.pg_get_userbyid(collowner) as \"owner\"\n" +
                "from pg_catalog.pg_collation\n" +
                "where collnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q41: opclass listing, 1 param nsOid
        comparePs("Q41_opclass",
                "select O.oid as id,\n" +
                "       O.xmin as state_number,\n" +
                "       opcname as name,\n" +
                "       opcintype::regtype::varchar as in_type,\n" +
                "       case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type,\n" +
                "       opcdefault as is_default,\n" +
                "       opcfamily as family_id,\n" +
                "       opfname as family,\n" +
                "       opcmethod as access_method_id,\n" +
                "       pg_catalog.pg_get_userbyid(O.opcowner) as \"owner\"\n" +
                "from pg_catalog.pg_opclass O\n" +
                "     join pg_catalog.pg_opfamily F on F.oid = opcfamily\n" +
                "where opcnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q42: opfamily listing, 1 param nsOid
        comparePs("Q42_opfamily",
                "select O.oid as id,\n" +
                "       O.xmin as state_number,\n" +
                "       opfname as name,\n" +
                "       opfmethod as access_method_id,\n" +
                "       pg_catalog.pg_get_userbyid(O.opfowner) as \"owner\"\n" +
                "from pg_catalog.pg_opfamily O\n" +
                "where opfnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q43: pg_amop oids, 1 param nsOid
        comparePs("Q43_amop_oids",
                "select pg_amop.oid\n" +
                "from pg_catalog.pg_amop\n" +
                "         join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily\n" +
                "where opfnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q44: pg_amop details, 2 params both nsOid
        comparePs("Q44_amop_details",
                "select O.oid as id,\n" +
                "       O.amopstrategy as strategy,\n" +
                "       O.amopopr as op_id,\n" +
                "       O.amopopr::regoperator::varchar as op_sig,\n" +
                "       O.amopsortfamily as sort_family_id,\n" +
                "       SF.opfname as sort_family,\n" +
                "       O.amopfamily as family_id,\n" +
                "       C.oid as class_id\n" +
                "from pg_catalog.pg_amop O\n" +
                "     left join pg_opfamily F on O.amopfamily = F.oid\n" +
                "     left join pg_opfamily SF on O.amopsortfamily = SF.oid\n" +
                "     left join pg_depend D on D.classid = 'pg_amop'::regclass and O.oid = D.objid and D.objsubid = 0\n" +
                "     left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0\n" +
                "where C.opcnamespace = ?::oid or C.opcnamespace is null and F.opfnamespace = ?::oid\n" +
                "order by C.oid, F.oid",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid});

        // Q45: pg_amproc oids, 1 param nsOid
        comparePs("Q45_amproc_oids",
                "select pg_amproc.oid\n" +
                "from pg_catalog.pg_amproc\n" +
                "         join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily\n" +
                "where opfnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q46: pg_amproc details, 2 params both nsOid
        comparePs("Q46_amproc_details",
                "select P.oid as id,\n" +
                "       P.amprocnum as num,\n" +
                "       P.amproc::oid as proc_id,\n" +
                "       P.amproc::regprocedure::varchar as proc_sig,\n" +
                "       P.amproclefttype::regtype::varchar as left_type,\n" +
                "       P.amprocrighttype::regtype::varchar as right_type,\n" +
                "       P.amprocfamily as family_id,\n" +
                "       C.oid as class_id\n" +
                "from pg_catalog.pg_amproc P\n" +
                "     left join pg_opfamily F on P.amprocfamily = F.oid\n" +
                "     left join pg_depend D on D.classid = 'pg_amproc'::regclass and P.oid = D.objid and D.objsubid = 0\n" +
                "     left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0\n" +
                "where C.opcnamespace = ?::oid or C.opcnamespace is null and F.opfnamespace = ?::oid\n" +
                "order by C.oid, F.oid",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid});

        // Q47: index listing (oids only), 1 param nsOid
        comparePs("Q47_index_oids",
                "select IX.indexrelid\n" +
                "from pg_catalog.pg_index IX,\n" +
                "     pg_catalog.pg_class IC\n" +
                "where IC.oid = IX.indrelid\n" +
                "  and IC.relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q48: index head, 1 param nsOid
        comparePs("Q48_index_head",
                "select tab.oid               table_id,\n" +
                "       tab.relkind           table_kind,\n" +
                "       ind_stor.relname      index_name,\n" +
                "       ind_head.indexrelid   index_id,\n" +
                "       ind_stor.xmin         state_number,\n" +
                "       ind_head.indisunique  is_unique,\n" +
                "       ind_head.indisprimary is_primary,\n" +
                "       ind_head.indnullsnotdistinct nulls_not_distinct,\n" +
                "       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,\n" +
                "       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,\n" +
                "       ind_stor.reltablespace tablespace_id,\n" +
                "       opcmethod as access_method_id\n" +
                "from pg_catalog.pg_class tab\n" +
                "         join pg_catalog.pg_index ind_head\n" +
                "              on ind_head.indrelid = tab.oid\n" +
                "         join pg_catalog.pg_class ind_stor\n" +
                "              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid\n" +
                "         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)\n" +
                "where tab.relnamespace = ?::oid\n" +
                "        and tab.relkind in ('r', 'm', 'v', 'p')\n" +
                "        and ind_stor.relkind in ('i', 'I')",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q49: index_columns. THIS IS THE "k" ERROR SOURCE, 1 param nsOid
        System.out.println("*** SPECIAL FOCUS: Q49 index_columns (the 'k' column error) ***");
        comparePs("Q49_index_columns_KEY",
                "select ind_head.indexrelid index_id,\n" +
                "       k col_idx,\n" +
                "       k <= indnkeyatts in_key,\n" +
                "       ind_head.indkey[k-1] column_position,\n" +
                "       ind_head.indoption[k-1] column_options,\n" +
                "       ind_head.indcollation[k-1] as collation,\n" +
                "       colln.nspname as collation_schema,\n" +
                "       collname as collation_str,\n" +
                "       ind_head.indclass[k-1] as opclass,\n" +
                "       case when opcdefault then null else opcn.nspname end as opclass_schema,\n" +
                "       case when opcdefault then null else opcname end as opclass_str,\n" +
                "       case\n" +
                "           when indexprs is null then null\n" +
                "           when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)\n" +
                "           else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)\n" +
                "       end as expression,\n" +
                "       amcanorder can_order\n" +
                "from pg_catalog.pg_index ind_head\n" +
                "         join pg_catalog.pg_class ind_stor\n" +
                "              on ind_stor.oid = ind_head.indexrelid\n" +
                "cross join unnest(ind_head.indkey) with ordinality u(u, k)\n" +
                "left join pg_catalog.pg_collation\n" +
                "on pg_collation.oid = ind_head.indcollation[k-1]\n" +
                "left join pg_catalog.pg_namespace colln on collnamespace = colln.oid\n" +
                "cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder\n" +
                "         left join pg_catalog.pg_opclass\n" +
                "                   on pg_opclass.oid = ind_head.indclass[k-1]\n" +
                "         left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid\n" +
                "where ind_stor.relnamespace = ?::oid\n" +
                "  and ind_stor.relkind in ('i', 'I')\n" +
                "order by index_id, k",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q50: constraint oids, 1 param nsOid
        comparePs("Q50_constraint_oids",
                "select oid\nfrom pg_catalog.pg_constraint\nwhere conrelid != 0 and connamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q51: constraint details, 2 params both nsOid
        comparePs("Q51_constraint_details",
                "select T.oid table_id,\n" +
                "       relkind table_kind,\n" +
                "       C.oid::bigint con_id,\n" +
                "       C.xmin::varchar::bigint con_state_id,\n" +
                "       conname con_name,\n" +
                "       contype con_kind,\n" +
                "       conkey con_columns,\n" +
                "       conindid index_id,\n" +
                "       confrelid ref_table_id,\n" +
                "       condeferrable is_deferrable,\n" +
                "       condeferred is_init_deferred,\n" +
                "       confupdtype on_update,\n" +
                "       confdeltype on_delete,\n" +
                "       connoinherit no_inherit,\n" +
                "       pg_catalog.pg_get_expr(conbin, T.oid) con_expression,\n" +
                "       confkey ref_columns,\n" +
                "       conexclop::int[] excl_operators,\n" +
                "       array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str\n" +
                "from pg_catalog.pg_constraint C\n" +
                "         join pg_catalog.pg_class T\n" +
                "              on C.conrelid = T.oid\n" +
                "where relkind in ('r', 'v', 'f', 'p')\n" +
                "  and relnamespace = ?::oid\n" +
                "  and contype in ('p', 'u', 'f', 'c', 'x')\n" +
                "  and connamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid});

        // Q52: rewrite oids, 1 param nsOid
        comparePs("Q52_rewrite_oids",
                "select RU.oid\n" +
                "from pg_catalog.pg_rewrite RU,\n" +
                "     pg_catalog.pg_class RC\n" +
                "where RC.oid = RU.ev_class\n" +
                "  and RC.relnamespace = ?::oid\n" +
                "  and not RU.rulename = '_RETURN'",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q53: policy oids, 1 param nsOid
        comparePs("Q53_policy_oids",
                "select P.oid\nfrom pg_catalog.pg_policy P\n     join pg_catalog.pg_class C on polrelid = C.oid\nwhere relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q54: policy details, 1 param nsOid
        comparePs("Q54_policy_details",
                "select\n" +
                "       P.oid id,\n" +
                "       P.xmin as state_number,\n" +
                "       polname policyname,\n" +
                "       polrelid table_id,\n" +
                "       polpermissive as permissive,\n" +
                "       polroles roles,\n" +
                "       polcmd cmd,\n" +
                "       pg_get_expr(polqual, polrelid) qual,\n" +
                "       pg_get_expr(polwithcheck, polrelid) with_check\n" +
                "from pg_catalog.pg_policy P\n" +
                "       join pg_catalog.pg_class C on polrelid = C.oid\n" +
                "where relnamespace = ?::oid\n" +
                "order by polrelid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q55: trigger oids, 1 param nsOid
        comparePs("Q55_trigger_oids",
                "select TG.oid\n" +
                "from pg_catalog.pg_trigger TG,\n" +
                "     pg_catalog.pg_class TC\n" +
                "where TC.oid = TG.tgrelid\n" +
                "  and TC.relnamespace = ?::oid",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q56: pg_description objects, 11 params all nsOid
        comparePs("Q56_desc_objects",
                "select D.objoid id, pg_catalog.array_agg(D.objsubid) sub_ids\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_class C on D.objoid = C.oid\n" +
                "where C.relnamespace = ?::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass\n" +
                "group by D.objoid\n" +
                "union all\n" +
                "select T.oid id, pg_catalog.array_agg(D.objsubid)\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid\n" +
                "  left join pg_catalog.pg_class C on T.typrelid = C.oid\n" +
                "where T.typnamespace = ?::oid and (C.relkind = 'c' or C.relkind is null)\n" +
                "group by T.oid\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_constraint C on D.objoid = C.oid\n" +
                "where C.connamespace = ?::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_trigger T on T.oid = D.objoid\n" +
                "  join pg_catalog.pg_class C on C.oid = T.tgrelid\n" +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_rewrite R on R.oid = D.objoid\n" +
                "  join pg_catalog.pg_class C on C.oid = R.ev_class\n" +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_proc P on P.oid = D.objoid\n" +
                "where P.pronamespace = ?::oid and D.classoid = 'pg_catalog.pg_proc'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_operator O on O.oid = D.objoid\n" +
                "where O.oprnamespace = ?::oid and D.classoid = 'pg_catalog.pg_operator'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_opclass O on O.oid = D.objoid\n" +
                "where O.opcnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_opfamily O on O.oid = D.objoid\n" +
                "where O.opfnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_collation C on C.oid = D.objoid\n" +
                "where C.collnamespace = ?::oid and D.classoid = 'pg_catalog.pg_collation'::regclass\n" +
                "\n" +
                "union all\n" +
                "select D.objoid id, array[D.objsubid]\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_policy P on P.oid = D.objoid\n" +
                "  join pg_catalog.pg_class C on P.polrelid = C.oid\n" +
                "where C.relnamespace = ?::oid  and D.classoid = 'pg_catalog.pg_policy'::regclass",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // Q57: pg_description details, 11 params all nsOid
        comparePs("Q57_desc_details",
                "select D.objoid id, C.relkind::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_class C on D.objoid = C.oid\n" +
                "where C.relnamespace = ?::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass\n" +
                "union all\n" +
                "select T.oid id, 'T'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid\n" +
                "  left join pg_catalog.pg_class C on T.typrelid = C.oid\n" +
                "where T.typnamespace = ?::oid and (C.relkind = 'c' or C.relkind is null)\n" +
                "union all\n" +
                "select D.objoid id, pg_catalog.translate(C.contype, 'pufc', 'kkxz')::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_constraint C on D.objoid = C.oid\n" +
                "where C.connamespace = ?::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass\n" +
                "union all\n" +
                "select D.objoid id, 't'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_trigger T on T.oid = D.objoid\n" +
                "  join pg_catalog.pg_class C on C.oid = T.tgrelid\n" +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass\n" +
                "union all\n" +
                "select D.objoid id, 'R'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_rewrite R on R.oid = D.objoid\n" +
                "  join pg_catalog.pg_class C on C.oid = R.ev_class\n" +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass\n" +
                "union all\n" +
                "select D.objoid id, 'F'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_proc P on P.oid = D.objoid\n" +
                "where P.pronamespace = ?::oid and D.classoid = 'pg_catalog.pg_proc'::regclass\n" +
                "union all\n" +
                "select D.objoid id, 'O'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_operator O on O.oid = D.objoid\n" +
                "where O.oprnamespace = ?::oid and D.classoid = 'pg_catalog.pg_operator'::regclass\n" +
                "union all\n" +
                "select D.objoid id, 'f'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_opfamily O on O.oid = D.objoid\n" +
                "where O.opfnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass\n" +
                "union all\n" +
                "select D.objoid id, 'c'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_opclass O on O.oid = D.objoid\n" +
                "where O.opcnamespace = ?::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass\n" +
                "  union all\n" +
                "select D.objoid id, 'C'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "  join pg_catalog.pg_collation C on C.oid = D.objoid\n" +
                "where C.collnamespace = ?::oid and D.classoid = 'pg_catalog.pg_collation'::regclass\n" +
                "\n" +
                "  union all\n" +
                "select D.objoid id, 'P'::char as kind, D.objsubid sub_id, D.description\n" +
                "from pg_catalog.pg_description D\n" +
                "       join pg_catalog.pg_policy P on P.oid = D.objoid\n" +
                "       join pg_catalog.pg_class C on P.polrelid = C.oid\n" +
                "where C.relnamespace = ?::oid and D.classoid = 'pg_catalog.pg_policy'::regclass",
                memConn, pgConn,
                new Object[]{memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid, memNsOid},
                new Object[]{pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid, pgNsOid});

        // Q58: ACL with xid, 6 params: nsOid, xid, nsOid, xid, nsOid, xid
        // Use "0" for xid to get all rows (age(0::xid) is very large)
        comparePs("Q58_acl_with_xid",
                "select T.oid as object_id,\n" +
                "                 T.relacl as acl\n" +
                "          from pg_catalog.pg_class T\n" +
                "          where relnamespace = ?::oid\n" +
                "            and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)\n" +
                "          union all\n" +
                "          select T.oid as object_id,\n" +
                "                 T.proacl as acl\n" +
                "          from pg_catalog.pg_proc T\n" +
                "          where pronamespace = ?::oid\n" +
                "            and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)\n" +
                "          union all\n" +
                "          select T.oid as object_id,\n" +
                "                 T.typacl as acl\n" +
                "          from pg_catalog.pg_type T\n" +
                "          where typnamespace = ?::oid\n" +
                "            and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)\n" +
                "          order by object_id",
                memConn, pgConn,
                new Object[]{memNsOid, "0", memNsOid, "0", memNsOid, "0"},
                new Object[]{pgNsOid, "0", pgNsOid, "0", pgNsOid, "0"});

        // Q59: attribute ACL, 2 params: nsOid, xid
        comparePs("Q59_attr_acl",
                "select T.oid as object_id,\n" +
                "               A.attnum as attr_position,\n" +
                "               A.attacl as acl\n" +
                "        from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid\n" +
                "        where relnamespace = ?::oid\n" +
                "          and attnum > 0\n" +
                "          and pg_catalog.age(A.xmin) <= pg_catalog.age(?::varchar::xid)\n" +
                "        order by object_id, attr_position",
                memConn, pgConn,
                new Object[]{memNsOid, "0"},
                new Object[]{pgNsOid, "0"});

        // Q60: pg_depend seq-to-table, 1 param nsOid
        comparePs("Q60_depend_seq",
                "select D.objid as dependent_id,\n" +
                "       D.refobjid as owner_id,\n" +
                "       D.refobjsubid as owner_subobject_id\n" +
                "from pg_depend D\n" +
                "  join pg_class C_SEQ on D.objid    = C_SEQ.oid and D.classid    = 'pg_class'::regclass::oid\n" +
                "  join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid\n" +
                "where C_SEQ.relkind = 'S'\n" +
                "  and C_TAB.relkind = 'r'\n" +
                "  and D.refobjsubid <> 0\n" +
                "  and (D.deptype = 'a' or D.deptype = 'i')\n" +
                "  and C_TAB.relnamespace = ?::oid\n" +
                "order by owner_id",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q61: pg_get_viewdef, 3 params: nsOid, xid, xid
        comparePs("Q61_viewdef",
                "select T.relkind as view_kind,\n" +
                "       T.oid as view_id,\n" +
                "       pg_catalog.pg_get_viewdef(T.oid, true) as source_text\n" +
                "from pg_catalog.pg_class T\n" +
                "     join pg_catalog.pg_namespace N on T.relnamespace = N.oid\n" +
                "where N.oid = ?::oid\n" +
                "  and T.relkind in ('m','v')\n" +
                "  and (pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) or exists(\n" +
                "  select A.attrelid from pg_catalog.pg_attribute A where A.attrelid = T.oid and pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)))",
                memConn, pgConn,
                new Object[]{memNsOid, "0", "0"},
                new Object[]{pgNsOid, "0", "0"});

        // Q62: function source, 1 param nsOid
        comparePs("Q62_function_source",
                "with system_languages as ( select oid as lang\n" +
                "                           from pg_catalog.pg_language\n" +
                "                           where lanname in ('c','internal') )\n" +
                "select oid as id,\n" +
                "       pg_catalog.pg_get_function_arguments(oid) as arguments_def,\n" +
                "       pg_catalog.pg_get_function_result(oid) as result_def,\n" +
                "       pg_catalog.pg_get_function_sqlbody(oid) as sqlbody_def,\n" +
                "       prosrc as source_text\n" +
                "from pg_catalog.pg_proc\n" +
                "where pronamespace = ?::oid\n" +
                "  and not (prokind = 'a')\n" +
                "  and prolang not in (select lang from system_languages)\n" +
                "  and prosrc is not null",
                memConn, pgConn,
                new Object[]{memNsOid}, new Object[]{pgNsOid});

        // Q63: SHOW TRANSACTION ISOLATION LEVEL
        comparePs("Q63_show_txn_iso",
                "SHOW TRANSACTION ISOLATION LEVEL",
                memConn, pgConn, new Object[]{}, new Object[]{});

        // =====================================================================
        // SUMMARY
        // =====================================================================
        System.out.println("\n========================================");
        System.out.println("SUMMARY (db-log-4.txt, empty database)");
        System.out.println("========================================");
        System.out.println("OK:           " + totalOk);
        System.out.println("Differences:  " + totalDiff);
        System.out.println("MEM errors:   " + totalMemErr);
        System.out.println("PG errors:    " + totalPgErr);
        System.out.println("Both errors:  " + totalBothErr);
        System.out.println("Total:        " + (totalOk + totalDiff + totalMemErr + totalPgErr + totalBothErr));

        System.out.println("\n--- OK queries (" + okQueries.size() + ") ---");
        for (String q : okQueries) System.out.println("  " + q);

        System.out.println("\n--- Diff queries (" + diffQueries.size() + ") ---");
        for (String q : diffQueries) System.out.println("  " + q);

        System.out.println("\n--- Memgres errors (" + memErrQueries.size() + ") ---");
        for (String q : memErrQueries) System.out.println("  " + q);

        System.out.println("\n--- PG errors (" + pgErrQueries.size() + ") ---");
        for (String q : pgErrQueries) System.out.println("  " + q);

        System.out.println("\n--- Both errors (" + bothErrQueries.size() + ") ---");
        for (String q : bothErrQueries) System.out.println("  " + q);

        // Cleanup
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
            bothErrQueries.add(name);
        } else if (memRows == null) {
            System.out.println("  *** MEMGRES ERROR, PG returned " + pgRows.size() + " rows");
            totalMemErr++;
            memErrQueries.add(name);
        } else if (pgRows == null) {
            System.out.println("  *** PG ERROR, Memgres returned " + memRows.size() + " rows");
            totalPgErr++;
            pgErrQueries.add(name);
        } else if (memRows.size() != pgRows.size()) {
            System.out.println("  *** ROW COUNT DIFF: Memgres=" + memRows.size() + " PG=" + pgRows.size());
            printRowDiff(memRows, pgRows);
            totalDiff++;
            diffQueries.add(name);
        } else {
            // Compare column names
            if (!memRows.isEmpty() && !pgRows.isEmpty()) {
                Set<String> memCols = memRows.get(0).keySet();
                Set<String> pgCols = pgRows.get(0).keySet();
                if (!memCols.equals(pgCols)) {
                    System.out.println("  *** COLUMN DIFF: Memgres=" + memCols + " PG=" + pgCols);
                    totalDiff++;
                    diffQueries.add(name);
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
                okQueries.add(name);
            } else {
                System.out.println("  *** " + diffs + " value differences in " + memRows.size() + " rows");
                totalDiff++;
                diffQueries.add(name);
            }
        }
        System.out.println();
    }

    static boolean isSkippedColumn(String col) {
        String lc = col.toLowerCase();
        // Skip OID-valued columns (they differ between Memgres and PG)
        if (lc.contains("oid")) return true;
        if (lc.endsWith("_id")) return true;
        if (lc.equals("id")) return true;
        if (lc.contains("state_number")) return true;
        if (lc.equals("xmin")) return true;
        if (lc.equals("schemaid")) return true;
        if (lc.equals("majoroid")) return true;
        if (lc.equals("handler_id")) return true;
        if (lc.equals("family_id")) return true;
        if (lc.equals("access_method_id")) return true;
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
        if (lc.equals("column_state_number")) return true;
        if (lc.equals("trigger_state_number")) return true;
        if (lc.equals("rule_state_number")) return true;
        if (lc.equals("type_state_number")) return true;
        if (lc.equals("sequence_state_number")) return true;
        if (lc.equals("r_state_number")) return true;
        if (lc.equals("table_state_number")) return true;
        // Version string will always differ
        if (lc.equals("version")) return true;
        // Startup time will always differ
        if (lc.equals("startup_time")) return true;
        // Transaction IDs differ
        if (lc.equals("transaction_id")) return true;
        if (lc.equals("current_txid")) return true;
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
            System.out.println("  " + label + " ERROR: " + msg.substring(0, Math.min(250, msg.length())));
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
        for (String col : rows.get(0).keySet()) {
            if (col.equals("kind")) return col;
        }
        return null;
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
