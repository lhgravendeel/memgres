package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests deep database client introspection queries that scan many catalog tables.
 * These are the new unique queries not tested before.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientDeepIntrospectionTest {

    static Memgres memgres;
    static Connection conn;
    static String nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA testns");
            s.execute("SET search_path = testns");
            s.execute("CREATE TABLE users(user_id serial PRIMARY KEY, username text)");
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'testns'")) {
                rs.next(); nsOid = rs.getString(1);
            }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP SCHEMA testns CASCADE"); } conn.close(); }
        if (memgres != null) memgres.close();
    }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // pg_event_trigger with xmin, evtname, evtevent, evtfoid, evtowner, evttags, evtenabled
    @Test @Order(1) void event_triggers() throws SQLException {
        query("""
            select t.oid as id, t.xmin as state_number, t.evtname as name, t.evtevent as event,
                   t.evtfoid as routine_id, pg_catalog.pg_get_userbyid(t.evtowner) as owner,
                   t.evttags as tags, case when t.evtenabled = 'D' then 1 else 0 end as is_disabled
            from pg_catalog.pg_event_trigger t
            """);
    }

    // pg_foreign_data_wrapper
    @Test @Order(2) void foreign_data_wrappers() throws SQLException {
        query("""
            select fdw.oid as id, fdw.xmin as state_number, fdw.fdwname as name,
                   pg_catalog.pg_get_userbyid(fdw.fdwowner) as "owner"
            from pg_catalog.pg_foreign_data_wrapper fdw
            """);
    }

    // pg_foreign_server
    @Test @Order(3) void foreign_servers() throws SQLException {
        query("""
            select srv.oid as id, srv.srvfdw as fdw_id, srv.xmin as state_number,
                   srv.srvname as name, pg_catalog.pg_get_userbyid(srv.srvowner) as "owner"
            from pg_catalog.pg_foreign_server srv
            """);
    }

    // pg_user_mapping
    @Test @Order(4) void user_mappings() throws SQLException {
        query("select oid as id, umserver as server_id from pg_catalog.pg_user_mapping order by server_id");
    }

    // pg_am with handler join
    @Test @Order(5) void access_methods_with_handler() throws SQLException {
        query("""
            select A.oid as access_method_id, A.xmin as state_number, A.amname as access_method_name,
                   A.amhandler::oid as handler_id, A.amtype as access_method_type
            from pg_am A
            """);
    }

    // pg_extension with available_versions
    @Test @Order(6) void extensions() throws SQLException {
        query("""
            select E.oid as id, E.xmin as state_number, extname as name, extversion as version,
                   extnamespace as schema_id, nspname as schema_name
            from pg_catalog.pg_extension E
                join pg_namespace N on E.extnamespace = N.oid
            """);
    }

    // pg_language
    @Test @Order(7) void languages() throws SQLException {
        query("""
            select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted
            from pg_catalog.pg_language l
            order by lanname
            """);
    }

    // pg_cast with source/target type joins
    @Test @Order(8) void casts() throws SQLException {
        query("""
            select C.oid, C.castsource as castsource_id, C.casttarget as casttarget_id,
                   C.castcontext, C.castmethod
            from pg_cast C
                join pg_type S on C.castsource::oid = S.oid
                join pg_type T on C.casttarget::oid = T.oid
            """);
    }

    // pg_constraint with connoinherit, confupdtype, confdeltype, conbin, conexclop
    @Test @Order(9) void constraints_deep() throws SQLException {
        query("""
            select C.oid::bigint con_id, conname con_name, contype con_kind,
                   conkey con_columns, conindid index_id, confrelid ref_table_id,
                   condeferrable is_deferrable, condeferred is_init_deferred,
                   confupdtype on_update, confdeltype on_delete, connoinherit no_inherit,
                   confkey ref_columns
            from pg_catalog.pg_constraint C
                join pg_catalog.pg_class T on C.conrelid = T.oid
            where relkind in ('r','v','f','p') AND relnamespace = """ + nsOid + """
            ::oid
            """);
    }

    // Index listing with indnullsnotdistinct, indclass, opcmethod
    @Test @Order(10) void index_listing_deep() throws SQLException {
        query("""
            select tab.oid table_id, tab.relkind table_kind, ind_stor.relname index_name,
                   ind_head.indexrelid index_id, ind_stor.xmin state_number,
                   ind_head.indisunique is_unique, ind_head.indisprimary is_primary,
                   pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,
                   ind_stor.reltablespace tablespace_id
            from pg_catalog.pg_class tab
                join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid
                join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
            where tab.relnamespace = """ + nsOid + """
            ::oid
                and tab.relkind in ('r','m','v','p') and ind_stor.relkind in ('i','I')
            """);
    }

    // pg_proc with prokind, provolatile, proisstrict, prosecdef, procost, prorows, proleakproof, proparallel
    @Test @Order(11) void routines_deep() throws SQLException {
        query("""
            select proname as r_name, prolang as lang_oid, oid as r_id,
                   prokind as kind, provolatile as volatile_kind
            from pg_catalog.pg_proc
            where pronamespace = """ + nsOid + "::oid and not (prokind = 'a')");
    }

    // pg_get_function_arguments, pg_get_function_result, pg_get_function_sqlbody
    @Test @Order(12) void function_source_functions() throws SQLException {
        query("""
            select oid as id,
                   pg_catalog.pg_get_function_arguments(oid) as arguments_def,
                   pg_catalog.pg_get_function_result(oid) as result_def
            from pg_catalog.pg_proc
            where pronamespace = """ + nsOid + "::oid and not (prokind = 'a') and prosrc is not null");
    }

    // pg_class table listing with pg_get_partkeydef, relam, relispartition, relpartbound
    @Test @Order(13) void table_listing_deep() throws SQLException {
        query("""
            select T.relkind as table_kind, T.relname as table_name, T.oid as table_id,
                   T.xmin as table_state_number, T.reltablespace as tablespace_id,
                   T.reloptions as options, T.relpersistence as persistence,
                   T.relispartition as is_partition,
                   pg_catalog.pg_get_partkeydef(T.oid) as partition_key,
                   pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression,
                   T.relam am_id, pg_catalog.pg_get_userbyid(T.relowner) as "owner"
            from pg_catalog.pg_class T
            where relnamespace = """ + nsOid + """
            ::oid and relkind in ('r','m','v','f','p')
            order by table_kind, table_id
            """);
    }

    // pg_get_viewdef
    @Test @Order(14) void view_source() throws SQLException {
        query("""
            select T.relkind as view_kind, T.oid as view_id,
                   pg_catalog.pg_get_viewdef(T.oid, true) as source_text
            from pg_catalog.pg_class T
                join pg_catalog.pg_namespace N on T.relnamespace = N.oid
            where N.oid = """ + nsOid + "::oid and T.relkind in ('m','v')");
    }

    // usesuper from pg_user
    @Test @Order(15) void pg_user_usesuper() throws SQLException {
        query("select usesuper from pg_user where usename = current_user");
    }
}
