package com.memgres.client;

import com.memgres.core.Memgres;

import java.sql.*;
import java.util.*;

/**
 * Compares client introspection query results between Memgres and real PG 18.
 * Run as main(). Requires PG 18 on localhost:5432 with memgrestest/memgres/memgres.
 */
public class ClientPgCompareTest {

    static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest?preferQueryMode=simple";
    static final String PG_USER = "memgres";
    static final String PG_PASS = "memgres";

    public static void main(String[] args) throws Exception {
        // Start Memgres
        Memgres memgres = Memgres.builder().port(0).build().start();
        Connection memConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        memConn.setAutoCommit(true);

        // Connect to PG 18
        Connection pgConn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
        pgConn.setAutoCommit(true);

        // Setup: create same table on both
        exec(memConn, "CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
        exec(pgConn, "DROP TABLE IF EXISTS users");
        exec(pgConn, "CREATE TABLE users (user_id serial PRIMARY KEY, username text)");

        // Get namespace OIDs
        int memNsOid = getOid(memConn, "SELECT oid FROM pg_namespace WHERE nspname = 'public'");
        int pgNsOid = getOid(pgConn, "SELECT oid FROM pg_namespace WHERE nspname = 'public'");

        System.out.println("=== Client Query Comparison: Memgres vs PG 18 ===\n");

        // Non-parameterized queries (global scope)
        String[][] globalQueries = {
            {"version", "select version()"},
            {"current_db", "select current_database() as a, current_schemas(false) as b"},
            {"current_all", "select current_database(), current_schema(), current_user"},
            {"show_datestyle", "show DateStyle"},
            {"show_txn_iso", "SHOW TRANSACTION ISOLATION LEVEL"},
            {"pg_database", "select N.oid::bigint as id, datname as name, D.description, datistemplate as is_template, datallowconn as allow_connections, pg_catalog.pg_get_userbyid(N.datdba) as \"owner\" from pg_catalog.pg_database N left join pg_catalog.pg_shdescription D on N.oid = D.objoid order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end"},
            {"pg_roles", "select R.oid::bigint as role_id, rolname as role_name, rolsuper is_super, rolinherit is_inherit, rolcreaterole can_createrole, rolcreatedb can_createdb, rolcanlogin can_login, rolreplication is_replication, rolconnlimit conn_limit, rolvaliduntil valid_until, rolbypassrls bypass_rls, rolconfig config, D.description from pg_catalog.pg_roles R left join pg_catalog.pg_shdescription D on D.objoid = R.oid"},
            {"pg_auth_members", "select member id, roleid role_id, admin_option from pg_catalog.pg_auth_members order by id, roleid::text"},
            {"pg_tablespace", "select T.oid::bigint as id, T.spcname as name, T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner, pg_catalog.pg_tablespace_location(T.oid) as location, T.spcoptions as options, D.description as comment from pg_catalog.pg_tablespace T left join pg_catalog.pg_shdescription D on D.objoid = T.oid"},
            {"pg_namespace", "select N.oid::bigint as id, N.xmin as state_number, nspname as name, D.description, pg_catalog.pg_get_userbyid(N.nspowner) as \"owner\" from pg_catalog.pg_namespace N left join pg_catalog.pg_description D on N.oid = D.objoid order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end"},
            {"pg_usesuper", "select usesuper from pg_user where usename = current_user"},
            {"pg_am", "select A.oid as access_method_id, A.xmin as state_number, A.amname as access_method_name, A.amhandler::oid as handler_id, pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name, A.amtype as access_method_type from pg_am A join pg_proc P on A.amhandler::oid = P.oid join pg_namespace N on P.pronamespace = N.oid"},
            {"pg_event_trigger", "select t.oid as id, t.xmin as state_number, t.evtname as name, t.evtevent as event, t.evtfoid as routine_id, pg_catalog.pg_get_userbyid(t.evtowner) as owner, t.evttags as tags, case when t.evtenabled = 'D' then 1 else 0 end as is_disabled from pg_catalog.pg_event_trigger t"},
            {"pg_fdw", "select fdw.oid as id, fdw.xmin as state_number, fdw.fdwname as name, pr.proname as handler, nspc.nspname as handler_schema, pr2.proname as validator, nspc2.nspname as validator_schema, fdw.fdwoptions as options, pg_catalog.pg_get_userbyid(fdw.fdwowner) as \"owner\" from pg_catalog.pg_foreign_data_wrapper fdw left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid"},
            {"pg_foreign_server", "select srv.oid as id, srv.srvfdw as fdw_id, srv.xmin as state_number, srv.srvname as name, srv.srvtype as type, srv.srvversion as version, srv.srvoptions as options, pg_catalog.pg_get_userbyid(srv.srvowner) as \"owner\" from pg_catalog.pg_foreign_server srv"},
            {"pg_user_mapping", "select oid as id, umserver as server_id, case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as \"user\", umoptions as options from pg_catalog.pg_user_mapping order by server_id"},
            {"pg_language", "select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted, h.proname as handler, hs.nspname as handlerSchema, i.proname as inline, isc.nspname as inlineSchema, v.proname as validator, vs.nspname as validatorSchema from pg_catalog.pg_language l left join pg_catalog.pg_proc h on h.oid = lanplcallfoid left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace left join pg_catalog.pg_proc i on i.oid = laninline left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace left join pg_catalog.pg_proc v on v.oid = lanvalidator left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace order by lanname"},
            {"pg_cast", "select C.oid, C.xmin as state_number, C.castsource as castsource_id, pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name, C.casttarget as casttarget_id, pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name, C.castfunc as castfunc_id, pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name, C.castcontext, C.castmethod from pg_cast C left outer join pg_proc F on C.castfunc::oid = F.oid left outer join pg_namespace FN on F.pronamespace = FN.oid join pg_type S on C.castsource::oid = S.oid join pg_namespace SN on S.typnamespace = SN.oid join pg_type T on C.casttarget::oid = T.oid join pg_namespace TN on T.typnamespace = TN.oid"},
            {"acl_global", "select T.oid as object_id, T.fdwacl as acl from pg_catalog.pg_foreign_data_wrapper T union all select T.oid as object_id, T.lanacl as acl from pg_catalog.pg_language T union all select T.oid as object_id, T.nspacl as acl from pg_catalog.pg_namespace T union all select T.oid as object_id, T.srvacl as acl from pg_catalog.pg_foreign_server T"},
            {"desc_global", "select D.objoid id, case when 'pg_catalog.pg_event_trigger'::regclass = classoid then 'T' when 'pg_catalog.pg_am'::regclass = classoid then 'A' when 'pg_catalog.pg_cast'::regclass = classoid then 'C' when 'pg_catalog.pg_foreign_data_wrapper'::regclass = classoid then 'W' when 'pg_catalog.pg_foreign_server'::regclass = classoid then 'S' when 'pg_catalog.pg_language'::regclass = classoid then 'L' when 'pg_catalog.pg_extension'::regclass = classoid then 'E' end as kind, D.objsubid sub_id, D.description from pg_catalog.pg_description D where classoid in ( 'pg_catalog.pg_event_trigger'::regclass, 'pg_catalog.pg_am'::regclass, 'pg_catalog.pg_cast'::regclass, 'pg_catalog.pg_foreign_data_wrapper'::regclass, 'pg_catalog.pg_foreign_server'::regclass, 'pg_catalog.pg_language'::regclass, 'pg_catalog.pg_extension'::regclass )"},
        };

        for (String[] q : globalQueries) {
            compareQuery(q[0], q[1], memConn, pgConn);
        }

        // Parameterized queries (schema-scoped): substitute $N with actual nsOid
        String[][] schemaQueries = {
            {"table_listing", "select T.relkind as table_kind, T.relname as table_name, T.oid as table_id, T.xmin as table_state_number, false as table_with_oids, T.reltablespace as tablespace_id, T.reloptions as options, T.relpersistence as persistence, T.relispartition as is_partition, T.relam am_id, pg_catalog.pg_get_userbyid(T.relowner) as \"owner\" from pg_catalog.pg_class T where relnamespace = %d and relkind in ('r', 'm', 'v', 'f', 'p') order by table_kind, table_id"},
            {"column_listing", "with T as ( select distinct T.oid as table_id, T.relname as table_name from pg_catalog.pg_class T, pg_catalog.pg_attribute A where T.relnamespace = %d and T.relkind in ('r', 'm', 'v', 'f', 'p') and A.attrelid = T.oid ) select T.table_id, T.table_name, C.attnum as column_position, C.attname as column_name, C.atttypmod as type_mod, C.attndims as dimensions_number, pg_catalog.format_type(C.atttypid, C.atttypmod) as type_spec, C.atttypid as type_id, C.attnotnull as mandatory, pg_catalog.pg_get_expr(D.adbin, T.table_id) as column_default_expression, not C.attislocal as column_is_inherited, C.attfdwoptions as options, C.attisdropped as column_is_dropped, C.attidentity as identity_kind, C.attgenerated as generated from T join pg_catalog.pg_attribute C on T.table_id = C.attrelid left join pg_catalog.pg_attrdef D on (C.attrelid, C.attnum) = (D.adrelid, D.adnum) where attnum > 0 order by table_id, attnum"},
            {"index_listing", "select tab.oid table_id, tab.relkind table_kind, ind_stor.relname index_name, ind_head.indexrelid index_id, ind_stor.xmin state_number, ind_head.indisunique is_unique, ind_head.indisprimary is_primary, pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition, ind_stor.reltablespace tablespace_id, opcmethod as access_method_id from pg_catalog.pg_class tab join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid join pg_catalog.pg_class ind_stor on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass) where tab.relnamespace = %d and tab.relkind in ('r', 'm', 'v', 'p') and ind_stor.relkind in ('i', 'I')"},
            {"constraint_listing", "select T.oid table_id, relkind table_kind, C.oid::bigint con_id, conname con_name, contype con_kind, conkey con_columns, conindid index_id, confrelid ref_table_id, condeferrable is_deferrable, condeferred is_init_deferred, confupdtype on_update, confdeltype on_delete, connoinherit no_inherit, pg_catalog.pg_get_expr(conbin, T.oid) con_expression, confkey ref_columns from pg_catalog.pg_constraint C join pg_catalog.pg_class T on C.conrelid = T.oid where relkind in ('r', 'v', 'f', 'p') and relnamespace = %d and contype in ('p', 'u', 'f', 'c', 'x') and connamespace = %1$d"},
            {"sequence_listing", "select cls.relname as sequence_name, sq.seqrelid as sequence_id, pg_catalog.format_type(sq.seqtypid, null) as data_type, sq.seqstart as start_value, sq.seqincrement as inc_value, sq.seqmin as min_value, sq.seqmax as max_value, sq.seqcache as cache_size, sq.seqcycle as cycle_option, pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\" from pg_catalog.pg_sequence sq join pg_class cls on sq.seqrelid = cls.oid where cls.relnamespace = %d"},
            {"depend_seq", "select D.objid as dependent_id, D.refobjid as owner_id, D.refobjsubid as owner_subobject_id from pg_depend D join pg_class C_SEQ on D.objid = C_SEQ.oid and D.classid = 'pg_class'::regclass::oid join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid where C_SEQ.relkind = 'S' and C_TAB.relkind = 'r' and D.refobjsubid <> 0 and (D.deptype = 'a' or D.deptype = 'i') and C_TAB.relnamespace = %d order by owner_id"},
            {"trigger_listing", "select T.tgrelid as table_id, T.oid as trigger_id, T.tgname as trigger_name, T.tgfoid as function_id, T.tgtype as bits, T.tgdeferrable as is_deferrable, T.tginitdeferred as is_init_deferred, T.tgenabled as trigger_fire_mode, T.tgattr as columns, T.tgconstraint != 0 as is_constraint, not T.tgisinternal as is_user from pg_catalog.pg_trigger T join pg_catalog.pg_class TAB on TAB.oid = T.tgrelid and TAB.relnamespace = %d where not T.tgisinternal"},
            {"rule_listing", "select R.ev_class as table_id, R.oid as rule_id, R.rulename as rule_name, pg_catalog.translate(ev_type,'1234','SUID') as rule_event_code, R.ev_enabled as rule_fire_mode, R.is_instead as rule_is_instead from pg_catalog.pg_rewrite R where R.ev_class in ( select oid from pg_catalog.pg_class where relnamespace = %d ) and R.rulename != '_RETURN'::name order by R.ev_class::bigint, ev_type"},
            {"view_listing", "select T.relkind as view_kind, T.oid as view_id, pg_catalog.pg_get_viewdef(T.oid, true) as source_text from pg_catalog.pg_class T join pg_catalog.pg_namespace N on T.relnamespace = N.oid where N.oid = %d and T.relkind in ('m','v')"},
            {"type_listing", "select T.oid as type_id, T.typname as type_name, T.typtype as type_sub_kind, T.typcategory as type_category, T.typrelid as class_id, T.typbasetype as base_type_id, case when T.typtype in ('c','e') then null else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def, T.typndims as dimensions_number, T.typdefault as default_expression, T.typnotnull as mandatory, pg_catalog.pg_get_userbyid(T.typowner) as \"owner\" from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid where T.typnamespace = %d and (T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined) order by 1"},
            {"collation_listing", "select oid as id, collname as name, collcollate as lc_collate, collctype as lc_ctype, pg_catalog.pg_get_userbyid(collowner) as \"owner\" from pg_catalog.pg_collation where collnamespace = %d"},
            {"opclass_listing", "select O.oid as id, opcname as name, opcintype::regtype::varchar as in_type, case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type, opcdefault as is_default, opcfamily as family_id, opfname as family, opcmethod as access_method_id, pg_catalog.pg_get_userbyid(O.opcowner) as \"owner\" from pg_catalog.pg_opclass O join pg_catalog.pg_opfamily F on F.oid = opcfamily where opcnamespace = %d"},
            {"opfamily_listing", "select O.oid as id, opfname as name, opfmethod as access_method_id, pg_catalog.pg_get_userbyid(O.opfowner) as \"owner\" from pg_catalog.pg_opfamily O where opfnamespace = %d"},
            {"routine_listing", "with languages as (select oid as lang_oid, lanname as lang from pg_catalog.pg_language), routines as (select proname as r_name, prolang as lang_oid, oid as r_id, proargnames as arg_names, proargmodes as arg_modes, proargtypes::int[] as in_arg_types, proallargtypes::int[] as all_arg_types, provariadic as arg_variadic_id, prorettype as ret_type_id, proretset as ret_set, prokind as kind, provolatile as volatile_kind, proisstrict as is_strict, prosecdef as is_security_definer, proconfig as configuration_parameters, procost as cost, pg_catalog.pg_get_userbyid(proowner) as \"owner\", prorows as rows, proleakproof as is_leakproof, proparallel as concurrency_kind from pg_catalog.pg_proc where pronamespace = %d and not (prokind = 'a')) select * from routines natural join languages"},
            {"aggregate_listing", "select P.oid as aggregate_id, P.proname as aggregate_name from pg_catalog.pg_aggregate A join pg_catalog.pg_proc P on A.aggfnoid = P.oid where P.pronamespace = %d order by P.oid"},
            {"policy_listing", "select P.oid from pg_catalog.pg_policy P join pg_catalog.pg_class C on polrelid = C.oid where relnamespace = %d"},
            {"foreign_table", "select ft.ftrelid as table_id, srv.srvname as table_server, ft.ftoptions as table_options from pg_catalog.pg_foreign_table ft left outer join pg_catalog.pg_foreign_server srv on ft.ftserver = srv.oid join pg_catalog.pg_class cls on ft.ftrelid = cls.oid where cls.relnamespace = %d"},
            {"big_union_names", "select T.oid as oid, relnamespace as schemaId, pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name from pg_catalog.pg_class T where relnamespace in ( %1$d ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S') union all select T.oid, T.typnamespace, 'T', T.typname from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid where T.typnamespace in ( %1$d ) and ( T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined ) union all select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( %1$d ) union all select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( %1$d ) union all select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( %1$d ) union all select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( %1$d ) union all select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end, proname from pg_catalog.pg_proc where pronamespace in ( %1$d )"},
        };

        for (String[] q : schemaQueries) {
            String memSql = String.format(q[1], memNsOid);
            String pgSql = String.format(q[1], pgNsOid);
            compareQuery(q[0], memSql, pgSql, memConn, pgConn);
        }

        // Cleanup
        exec(pgConn, "DROP TABLE IF EXISTS users");
        memConn.close();
        pgConn.close();
        memgres.close();
    }

    static void compareQuery(String name, String sql, Connection memConn, Connection pgConn) {
        compareQuery(name, sql, sql, memConn, pgConn);
    }

    static void compareQuery(String name, String memSql, String pgSql, Connection memConn, Connection pgConn) {
        System.out.println("--- " + name + " ---");
        List<Map<String, String>> memRows = runQuery(memConn, memSql, "MEM");
        List<Map<String, String>> pgRows = runQuery(pgConn, pgSql, "PG");

        if (memRows == null && pgRows == null) {
            System.out.println("  BOTH: error or no-result");
        } else if (memRows == null) {
            System.out.println("  *** MEMGRES ERROR, PG returned " + pgRows.size() + " rows");
        } else if (pgRows == null) {
            System.out.println("  *** PG ERROR, Memgres returned " + memRows.size() + " rows");
        } else if (memRows.size() != pgRows.size()) {
            System.out.println("  *** ROW COUNT DIFF: Memgres=" + memRows.size() + " PG=" + pgRows.size());
            // Show what's in MEM but not PG and vice versa (by name column if available)
            printRowDiff(memRows, pgRows);
        } else {
            // Compare column names
            if (!memRows.isEmpty() && !pgRows.isEmpty()) {
                Set<String> memCols = memRows.get(0).keySet();
                Set<String> pgCols = pgRows.get(0).keySet();
                if (!memCols.equals(pgCols)) {
                    System.out.println("  *** COLUMN DIFF: Memgres=" + memCols + " PG=" + pgCols);
                }
            }
            // Check for value differences in key columns
            int diffs = 0;
            for (int i = 0; i < Math.min(memRows.size(), pgRows.size()); i++) {
                Map<String, String> mr = memRows.get(i);
                Map<String, String> pr = pgRows.get(i);
                for (String col : mr.keySet()) {
                    if (!pr.containsKey(col)) continue;
                    String mv = mr.get(col);
                    String pv = pr.get(col);
                    // Skip OID-valued columns (they'll always differ)
                    if (col.contains("oid") || col.contains("_id") || col.equals("id") || col.contains("state_number")
                            || col.equals("xmin") || col.equals("handler_id") || col.equals("schemaId")
                            || col.equals("family_id") || col.equals("access_method_id") || col.equals("index_id")
                            || col.equals("con_id") || col.equals("sequence_id") || col.equals("class_id")
                            || col.equals("base_type_id")) continue;
                    if (!Objects.equals(mv, pv)) {
                        if (diffs < 20) {
                            System.out.println("  row " + i + " col " + col + ": MEM=" + trunc(mv) + "  PG=" + trunc(pv));
                        }
                        diffs++;
                    }
                }
            }
            if (diffs == 0) {
                System.out.println("  OK (" + memRows.size() + " rows, values match)");
            } else {
                System.out.println("  *** " + diffs + " value differences in " + memRows.size() + " rows");
            }
        }
        System.out.println();
    }

    static void printRowDiff(List<Map<String, String>> memRows, List<Map<String, String>> pgRows) {
        // Find a "name" column to identify rows
        String nameCol = null;
        if (!memRows.isEmpty()) {
            for (String col : memRows.get(0).keySet()) {
                if (col.contains("name") && !col.contains("schema") && !col.contains("handler") && !col.contains("validator")) {
                    nameCol = col; break;
                }
            }
        }
        if (nameCol == null && !pgRows.isEmpty()) {
            for (String col : pgRows.get(0).keySet()) {
                if (col.contains("name") && !col.contains("schema") && !col.contains("handler") && !col.contains("validator")) {
                    nameCol = col; break;
                }
            }
        }
        if (nameCol != null) {
            Set<String> memNames = new TreeSet<>();
            for (Map<String, String> r : memRows) { String v = r.get(nameCol); if (v != null) memNames.add(v); }
            Set<String> pgNames = new TreeSet<>();
            for (Map<String, String> r : pgRows) { String v = r.get(nameCol); if (v != null) pgNames.add(v); }
            Set<String> onlyMem = new TreeSet<>(memNames); onlyMem.removeAll(pgNames);
            Set<String> onlyPg = new TreeSet<>(pgNames); onlyPg.removeAll(memNames);
            if (!onlyMem.isEmpty()) System.out.println("  Only in Memgres (" + nameCol + "): " + (onlyMem.size() > 30 ? onlyMem.size() + " items" : onlyMem));
            if (!onlyPg.isEmpty()) System.out.println("  Only in PG (" + nameCol + "): " + (onlyPg.size() > 30 ? onlyPg.size() + " items" : onlyPg));
        } else {
            if (memRows.size() <= 5) System.out.println("  Memgres rows: " + memRows);
            if (pgRows.size() <= 5) System.out.println("  PG rows: " + pgRows);
        }
    }

    static List<Map<String, String>> runQuery(Connection conn, String sql, String label) {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
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
            return rows;
        } catch (SQLException e) {
            System.out.println("  " + label + " ERROR: " + e.getMessage().replaceAll("\\n", " ").substring(0, Math.min(200, e.getMessage().length())));
            return null;
        }
    }

    static void exec(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static int getOid(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    static String trunc(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 77) + "..." : s;
    }
}
