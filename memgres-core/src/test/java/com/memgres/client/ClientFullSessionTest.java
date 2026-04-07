package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simulates the FULL database client session via extended protocol (PreparedStatement).
 * Sends every query from db-log-1.txt in order, on the same connection,
 * to catch state-dependent issues and protocol errors.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientFullSessionTest {

    static Memgres memgres;
    static Connection conn;
    static int nsOid;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Connection 1: create table (simple protocol for client setup)
        try (Connection setupConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            setupConn.setAutoCommit(true);
            try (Statement s = setupConn.createStatement()) {
                s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
            }
        }
        // Connection 3: introspection (extended protocol)
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        // Get nsOid via simple statement
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            rs.next();
            nsOid = rs.getInt(1);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // Helper for queries with ? params bound to nsOid
    private ResultSet execQuery(String sql, Object... params) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            if (params[i] instanceof Integer) ps.setInt(i + 1, (Integer) params[i]);
            else if (params[i] instanceof String) ps.setString(i + 1, (String) params[i]);
            else if (params[i] instanceof Long) ps.setLong(i + 1, (Long) params[i]);
        }
        return ps.executeQuery(); // This calls Describe + Execute
    }

    // Helper: execute (no result expected)
    private void exec(String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        }
    }

    // =========================================================================
    // Connection setup (same order as client log, connection 3)
    // =========================================================================

    @Test @Order(1) void setup_extra_float_digits() throws SQLException {
        exec("SET extra_float_digits = 3");
    }

    @Test @Order(2) void setup_app_name_empty() throws SQLException {
        exec("SET application_name = ''");
    }

    @Test @Order(3) void setup_version() throws SQLException {
        try (ResultSet rs = execQuery("select version()")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(4) void setup_app_name() throws SQLException {
        exec("SET application_name = 'test_client'");
    }

    @Test @Order(5) void setup_empty() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("")) {
            ps.execute(); // empty query
        } catch (SQLException e) {
            // OK - empty query might error
        }
    }

    @Test @Order(6) void setup_current_database() throws SQLException {
        try (ResultSet rs = execQuery("select current_database() as a, current_schemas(false) as b")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(7) void setup_current_all() throws SQLException {
        try (ResultSet rs = execQuery("select current_database(), current_schema(), current_user")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(8) void setup_startup_time() throws SQLException {
        try (ResultSet rs = execQuery("select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(9) void setup_pg_locks() throws SQLException {
        try (ResultSet rs = execQuery("""
                select L.transactionid::varchar::bigint as transaction_id
                from pg_catalog.pg_locks L
                where L.transactionid is not null
                order by pg_catalog.age(L.transactionid) desc
                limit 1""")) {
            // may return 0 rows
        }
    }

    @Test @Order(10) void setup_txid() throws SQLException {
        try (ResultSet rs = execQuery("""
                select case
                  when pg_catalog.pg_is_in_recovery()
                    then null
                  else
                    (pg_catalog.txid_current() % 4294967296)::varchar::bigint
                  end as current_txid""")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // Introspection queries (order from log)
    // =========================================================================

    @Test @Order(11) void q_pg_database() throws SQLException {
        try (ResultSet rs = execQuery("""
                select N.oid::bigint as id, datname as name, D.description,
                       datistemplate as is_template, datallowconn as allow_connections,
                       pg_catalog.pg_get_userbyid(N.datdba) as "owner"
                from pg_catalog.pg_database N
                  left join pg_catalog.pg_shdescription D on N.oid = D.objoid
                order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(12) void q_show_datestyle() throws SQLException {
        try (ResultSet rs = execQuery("show DateStyle")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(13) void q_timezone_union() throws SQLException {
        try (ResultSet rs = execQuery("""
                select name, is_dst from pg_catalog.pg_timezone_names
                union distinct
                select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(14) void q_pg_roles() throws SQLException {
        try (ResultSet rs = execQuery("""
                select R.oid::bigint as role_id, rolname as role_name,
                  rolsuper is_super, rolinherit is_inherit,
                  rolcreaterole can_createrole, rolcreatedb can_createdb,
                  rolcanlogin can_login, rolreplication is_replication,
                  rolconnlimit conn_limit, rolvaliduntil valid_until,
                  rolbypassrls bypass_rls, rolconfig config,
                  D.description
                from pg_catalog.pg_roles R
                  left join pg_catalog.pg_shdescription D on D.objoid = R.oid""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(15) void q_pg_auth_members() throws SQLException {
        try (ResultSet rs = execQuery("select member id, roleid role_id, admin_option from pg_catalog.pg_auth_members order by id, roleid::text")) {
            // may be empty
        }
    }

    @Test @Order(16) void q_pg_tablespace() throws SQLException {
        try (ResultSet rs = execQuery("""
                select T.oid::bigint as id, T.spcname as name,
                       T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
                       pg_catalog.pg_tablespace_location(T.oid) as location,
                       T.spcoptions as options,
                       D.description as comment
                from pg_catalog.pg_tablespace T
                  left join pg_catalog.pg_shdescription D on D.objoid = T.oid""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(17) void q_tablespace_acl() throws SQLException {
        try (ResultSet rs = execQuery("""
                select T.oid as object_id, T.spcacl as acl from pg_catalog.pg_tablespace T
                union all
                select T.oid as object_id, T.datacl as acl from pg_catalog.pg_database T""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(18) void q_pg_namespace() throws SQLException {
        try (ResultSet rs = execQuery("""
                select N.oid::bigint as id, N.xmin as state_number, nspname as name,
                       D.description, pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
                from pg_catalog.pg_namespace N
                  left join pg_catalog.pg_description D on N.oid = D.objoid
                order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(19) void q_usesuper() throws SQLException {
        try (ResultSet rs = execQuery("select usesuper from pg_user where usename = current_user")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(20) void q_pg_event_trigger() throws SQLException {
        try (ResultSet rs = execQuery("""
                select t.oid as id, t.xmin as state_number, t.evtname as name,
                       t.evtevent as event, t.evtfoid as routine_id,
                       pg_catalog.pg_get_userbyid(t.evtowner) as owner,
                       t.evttags as tags,
                       case when t.evtenabled = 'D' then 1 else 0 end as is_disabled
                from pg_catalog.pg_event_trigger t""")) {
            // may be empty
        }
    }

    @Test @Order(21) void q_pg_fdw() throws SQLException {
        try (ResultSet rs = execQuery("""
                select fdw.oid as id, fdw.xmin as state_number, fdw.fdwname as name,
                       pr.proname as handler, nspc.nspname as handler_schema,
                       pr2.proname as validator, nspc2.nspname as validator_schema,
                       fdw.fdwoptions as options,
                       pg_catalog.pg_get_userbyid(fdw.fdwowner) as "owner"
                from pg_catalog.pg_foreign_data_wrapper fdw
                     left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid
                     left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid
                     left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid
                     left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid""")) {
            // may be empty
        }
    }

    @Test @Order(22) void q_pg_foreign_server() throws SQLException {
        try (ResultSet rs = execQuery("""
                select srv.oid as id, srv.srvfdw as fdw_id, srv.xmin as state_number,
                       srv.srvname as name, srv.srvtype as type, srv.srvversion as version,
                       srv.srvoptions as options, pg_catalog.pg_get_userbyid(srv.srvowner) as "owner"
                from pg_catalog.pg_foreign_server srv""")) {
            // may be empty
        }
    }

    @Test @Order(23) void q_pg_user_mapping() throws SQLException {
        try (ResultSet rs = execQuery("""
                select oid as id, umserver as server_id,
                       case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as user,
                       umoptions as options
                from pg_catalog.pg_user_mapping order by server_id""")) {
            // may be empty
        }
    }

    @Test @Order(24) void q_pg_am() throws SQLException {
        try (ResultSet rs = execQuery("""
                select A.oid as access_method_id, A.xmin as state_number,
                       A.amname as access_method_name,
                       A.amhandler::oid as handler_id,
                       pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,
                       A.amtype as access_method_type
                from pg_am A join pg_proc P on A.amhandler::oid = P.oid
                     join pg_namespace N on P.pronamespace = N.oid""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(25) void q_pg_extension() throws SQLException {
        try (ResultSet rs = execQuery("""
                select E.oid as id, E.xmin as state_number,
                       extname as name, extversion as version,
                       extnamespace as schema_id, nspname as schema_name,
                       array(select unnest from unnest(available_versions)
                             where unnest > extversion) as available_updates
                from pg_catalog.pg_extension E
                       join pg_namespace N on E.extnamespace = N.oid
                       left join (select name, array_agg(version) as available_versions
                                  from pg_available_extension_versions()
                                  group by name) V on E.extname = V.name""")) {
            // may be empty
        }
    }

    @Test @Order(26) void q_pg_type_oid() throws SQLException {
        // This query uses $1 - test with ? param
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_type e ON t.typelem = e.oid
                JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.oid = ?""")) {
            ps.setInt(1, 23); // int4 type OID
            try (ResultSet rs = ps.executeQuery()) {
                // may or may not find array element type
            }
        }
    }

    @Test @Order(27) void q_pg_language() throws SQLException {
        try (ResultSet rs = execQuery("""
                select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted,
                       h.proname as handler, hs.nspname as handlerSchema,
                       i.proname as inline, isc.nspname as inlineSchema,
                       v.proname as validator, vs.nspname as validatorSchema
                from pg_catalog.pg_language l
                    left join pg_catalog.pg_proc h on h.oid = lanplcallfoid
                    left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace
                    left join pg_catalog.pg_proc i on i.oid = laninline
                    left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace
                    left join pg_catalog.pg_proc v on v.oid = lanvalidator
                    left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace
                order by lanname""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(28) void q_pg_description_regclass() throws SQLException {
        try (ResultSet rs = execQuery("""
                select D.objoid id, case
                    when 'pg_catalog.pg_event_trigger'::regclass = classoid then 'T'
                    when 'pg_catalog.pg_am'::regclass = classoid then 'A'
                    when 'pg_catalog.pg_cast'::regclass = classoid then 'C'
                    when 'pg_catalog.pg_foreign_data_wrapper'::regclass = classoid then 'W'
                    when 'pg_catalog.pg_foreign_server'::regclass = classoid then 'S'
                    when 'pg_catalog.pg_language'::regclass = classoid then 'L'
                    when 'pg_catalog.pg_extension'::regclass = classoid then 'E'
                  end as kind,
                  D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                where classoid in (
                  'pg_catalog.pg_event_trigger'::regclass,
                  'pg_catalog.pg_am'::regclass,
                  'pg_catalog.pg_cast'::regclass,
                  'pg_catalog.pg_foreign_data_wrapper'::regclass,
                  'pg_catalog.pg_foreign_server'::regclass,
                  'pg_catalog.pg_language'::regclass,
                  'pg_catalog.pg_extension'::regclass
                )""")) {
            // may be empty
        }
    }

    @Test @Order(29) void q_fdw_acl_union() throws SQLException {
        try (ResultSet rs = execQuery("""
                select T.oid as object_id, T.fdwacl as acl from pg_catalog.pg_foreign_data_wrapper T
                union all
                select T.oid as object_id, T.lanacl as acl from pg_catalog.pg_language T
                union all
                select T.oid as object_id, T.nspacl as acl from pg_catalog.pg_namespace T
                union all
                select T.oid as object_id, T.srvacl as acl from pg_catalog.pg_foreign_server T""")) {
            assertTrue(rs.next());
        }
    }

    @Test @Order(30) void q_pg_cast() throws SQLException {
        try (ResultSet rs = execQuery("""
                select C.oid, C.xmin as state_number,
                       C.castsource as castsource_id,
                       pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name,
                       C.casttarget as casttarget_id,
                       pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name,
                       C.castfunc as castfunc_id,
                       pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name,
                       C.castcontext, C.castmethod
                from pg_cast C
                     left outer join pg_proc F on C.castfunc::oid = F.oid
                     left outer join pg_namespace FN on F.pronamespace = FN.oid
                     join pg_type S on C.castsource::oid = S.oid
                     join pg_namespace SN on S.typnamespace = SN.oid
                     join pg_type T on C.casttarget::oid = T.oid
                     join pg_namespace TN on T.typnamespace = TN.oid""")) {
            // may be empty
        }
    }

    @Test @Order(31) void q_ext_depend() throws SQLException {
        try (ResultSet rs = execQuery("""
                select E.oid as extension_id, D.objid as member_id
                from pg_extension E
                     join pg_depend D on E.oid = D.refobjid and D.refclassid = 'pg_extension'::regclass::oid
                where D.deptype = 'e'
                order by extension_id""")) {
            // may be empty
        }
    }

    // =========================================================================
    // Parameterized queries with ? (simulating client Bind)
    // =========================================================================

    @Test @Order(40) void q_7branch_3col() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select T.oid as oid, relnamespace as schemaId,
                       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind
                from pg_catalog.pg_class T
                where relnamespace in ( ? ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
                union all
                select T.oid, T.typnamespace, 'T' as kind
                from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
                where T.typnamespace in ( ? )
                  and ( T.typtype in ('d','e') or C.relkind = 'c'::"char" or
                        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
                        T.typtype = 'p' and not T.typisdefined )
                union all
                select oid, collnamespace, 'C' from pg_catalog.pg_collation where collnamespace in ( ? )
                union all
                select oid, oprnamespace, 'O' from pg_catalog.pg_operator where oprnamespace in ( ? )
                union all
                select oid, opcnamespace, 'c' from pg_catalog.pg_opclass where opcnamespace in ( ? )
                union all
                select oid, opfnamespace, 'F' from pg_catalog.pg_opfamily where opfnamespace in ( ? )
                union all
                select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end
                from pg_catalog.pg_proc where pronamespace in ( ? )""")) {
            for (int i = 1; i <= 7; i++) ps.setInt(i, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(41) void q_7branch_4col() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select T.oid as oid, relnamespace as schemaId,
                       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name
                from pg_catalog.pg_class T
                where relnamespace in ( ? ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
                union all
                select T.oid, T.typnamespace, 'T', T.typname
                from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
                where T.typnamespace in ( ? )
                  and ( T.typtype in ('d','e') or C.relkind = 'c'::"char" or
                        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
                        T.typtype = 'p' and not T.typisdefined )
                union all
                select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( ? )
                union all
                select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( ? )
                union all
                select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( ? )
                union all
                select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( ? )
                union all
                select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end, proname
                from pg_catalog.pg_proc where pronamespace in ( ? )
                order by schemaId""")) {
            for (int i = 1; i <= 7; i++) ps.setInt(i, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(42) void q_pg_proc_args() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select pronamespace as schemaId, oid as majorOid,
                       proargnames as argNames, proargmodes as argModes,
                       array_length(proargtypes, 1) as nArgs
                from pg_catalog.pg_proc where pronamespace in ( ? )
                order by schemaId""")) {
            ps.setInt(1, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                // may be empty
            }
        }
    }

    @Test @Order(43) void q_cte_columns() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                with T as ( select T.oid as oid, T.relkind as kind, T.relnamespace as schemaId
                            from pg_catalog.pg_class T
                            where T.relnamespace in ( ? ) and T.relkind in ('r', 'm', 'v', 'f', 'p') )
                select T.schemaId as schemaId, T.oid as majorOid,
                       pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind,
                       C.attnum as position, C.attname as name
                from T join pg_catalog.pg_attribute C on T.oid = C.attrelid
                where C.attnum > 0 and not C.attisdropped
                order by schemaId, majorOid""")) {
            ps.setInt(1, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(44) void q_table_listing() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select T.relkind as table_kind, T.relname as table_name,
                       T.oid as table_id, T.xmin as table_state_number,
                       false as table_with_oids,
                       T.reltablespace as tablespace_id, T.reloptions as options,
                       T.relpersistence as persistence,
                       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,
                       (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,
                       T.relispartition as is_partition,
                       pg_catalog.pg_get_partkeydef(T.oid) as partition_key,
                       pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression,
                       T.relam am_id,
                       pg_catalog.pg_get_userbyid(T.relowner) as "owner"
                from pg_catalog.pg_class T
                where relnamespace = ?::oid and relkind in ('r', 'm', 'v', 'f', 'p')
                  and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)
                order by table_kind, table_id""")) {
            ps.setInt(1, nsOid);
            ps.setString(2, "1");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Should list at least the users table");
            }
        }
    }

    @Test @Order(45) void q_index_detail() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select tab.oid table_id, tab.relkind table_kind,
                       ind_stor.relname index_name, ind_head.indexrelid index_id,
                       ind_stor.xmin state_number,
                       ind_head.indisunique is_unique, ind_head.indisprimary is_primary,
                       ind_head.indnullsnotdistinct nulls_not_distinct,
                       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,
                       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,
                       ind_stor.reltablespace tablespace_id,
                       opcmethod as access_method_id
                from pg_catalog.pg_class tab
                     join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid
                     join pg_catalog.pg_class ind_stor on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
                     left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
                where tab.relnamespace = ?::oid
                  and tab.relkind in ('r', 'm', 'v', 'p') and ind_stor.relkind in ('i', 'I')
                  and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)""")) {
            ps.setInt(1, nsOid);
            ps.setString(2, "1");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(46) void q_index_columns() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select ind_head.indexrelid index_id, k col_idx,
                       k <= indnkeyatts in_key,
                       ind_head.indkey[k-1] column_position,
                       ind_head.indoption[k-1] column_options,
                       ind_head.indcollation[k-1] as collation,
                       colln.nspname as collation_schema, collname as collation_str,
                       ind_head.indclass[k-1] as opclass,
                       case when opcdefault then null else opcn.nspname end as opclass_schema,
                       case when opcdefault then null else opcname end as opclass_str,
                       case when indexprs is null then null
                            when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
                            else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true) end as expression,
                       amcanorder can_order
                from pg_catalog.pg_index ind_head
                     join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
                cross join unnest(ind_head.indkey) with ordinality u(u, k)
                left join pg_catalog.pg_collation on pg_collation.oid = ind_head.indcollation[k-1]
                left join pg_catalog.pg_namespace colln on collnamespace = colln.oid
                cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
                     left join pg_catalog.pg_opclass on pg_opclass.oid = ind_head.indclass[k-1]
                     left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid
                where ind_stor.relnamespace = ?::oid and ind_stor.relkind in ('i', 'I')
                  and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)
                order by index_id, k""")) {
            ps.setInt(1, nsOid);
            ps.setString(2, "1");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(47) void q_constraints() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select T.oid table_id, relkind table_kind,
                       C.oid::bigint con_id, C.xmin::varchar::bigint con_state_id,
                       conname con_name, contype con_kind, conkey con_columns,
                       conindid index_id, confrelid ref_table_id,
                       condeferrable is_deferrable, condeferred is_init_deferred,
                       confupdtype on_update, confdeltype on_delete,
                       connoinherit no_inherit,
                       pg_catalog.pg_get_expr(conbin, T.oid) con_expression,
                       confkey ref_columns,
                       conexclop::int[] excl_operators,
                       array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str
                from pg_catalog.pg_constraint C join pg_catalog.pg_class T on C.conrelid = T.oid
                where relkind in ('r', 'v', 'f', 'p') and relnamespace = ?::oid
                  and contype in ('p', 'u', 'f', 'c', 'x') and connamespace = ?::oid
                  and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647) or pg_catalog.age(c.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)""")) {
            ps.setInt(1, nsOid);
            ps.setInt(2, nsOid);
            ps.setString(3, "1");
            ps.setString(4, "1");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(48) void q_acl_union() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select T.oid as object_id, T.relacl as acl
                from pg_catalog.pg_class T
                where relnamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
                union all
                select T.oid as object_id, T.proacl as acl
                from pg_catalog.pg_proc T
                where pronamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
                union all
                select T.oid as object_id, T.typacl as acl
                from pg_catalog.pg_type T
                where typnamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
                order by object_id""")) {
            ps.setInt(1, nsOid);
            ps.setString(2, "1");
            ps.setInt(3, nsOid);
            ps.setString(4, "1");
            ps.setInt(5, nsOid);
            ps.setString(6, "1");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test @Order(49) void q_seq_depend() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                select D.objid as dependent_id, D.refobjid as owner_id, D.refobjsubid as owner_subobject_id
                from pg_depend D
                  join pg_class C_SEQ on D.objid = C_SEQ.oid and D.classid = 'pg_class'::regclass::oid
                  join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid
                where C_SEQ.relkind = 'S' and C_TAB.relkind = 'r'
                  and D.refobjsubid <> 0 and (D.deptype = 'a' or D.deptype = 'i')
                  and C_TAB.relnamespace = ?::oid
                order by owner_id""")) {
            ps.setInt(1, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                // should find users_user_id_seq → users dependency
            }
        }
    }

    @Test @Order(50) void q_show_transaction_isolation() throws SQLException {
        try (ResultSet rs = execQuery("SHOW TRANSACTION ISOLATION LEVEL")) {
            assertTrue(rs.next());
        }
    }
}
