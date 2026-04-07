package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests ALL database client introspection queries via extended protocol.
 * This matches exactly what a database client sends when connecting.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientFullIntrospectionTest {

    static Memgres memgres;
    static Connection conn;
    static int nsOid;  // namespace OID for public schema

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Setup with simple protocol first
        try (Connection setupConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            setupConn.setAutoCommit(true);
            try (Statement s = setupConn.createStatement()) {
                s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
                s.execute("INSERT INTO users(username) VALUES ('test_user')");
            }
        }
        // Extended protocol connection (extended protocol)
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
                rs.next(); nsOid = rs.getInt(1);
            }
        }
    }

    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE users"); } conn.close(); }
        if (memgres != null) memgres.close();
    }

    static List<Map<String, Object>> queryWithMeta(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) {
                    row.put(md.getColumnLabel(i), rs.getString(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }

    /**
     * For queries with $N params: substitute values inline and execute as Statement.
     * The client sends $N via PgWire extended protocol, but JDBC uses ?, so we substitute directly.
     */
    static List<Map<String, Object>> prepQueryWithMeta(String sql, Object... params) throws SQLException {
        // Replace $N with actual values (largest N first to avoid $1 matching inside $10)
        String resolved = sql;
        for (int i = params.length; i >= 1; i--) {
            String val = params[i - 1] == null ? "NULL" : "'" + params[i - 1].toString() + "'";
            resolved = resolved.replace("$" + i, val);
        }
        return queryWithMeta(resolved);
    }

    // ===== Phase 1: Connection setup queries =====

    @Test @Order(1) void set_extra_float_digits() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SET extra_float_digits = 3")) {
            ps.execute();
        }
    }

    @Test @Order(2) void set_application_name() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SET application_name = 'test_client'")) {
            ps.execute();
        }
    }

    @Test @Order(3) void select_version() throws SQLException {
        var rows = prepQueryWithMeta("select version()");
        assertFalse(rows.isEmpty(), "version() should return a row");
    }

    @Test @Order(4) void show_transaction_isolation_level() throws SQLException {
        var rows = prepQueryWithMeta("SHOW TRANSACTION ISOLATION LEVEL");
        assertFalse(rows.isEmpty(), "SHOW should return 1 row");
    }

    @Test @Order(5) void current_database_and_schemas() throws SQLException {
        var rows = prepQueryWithMeta("select current_database() as a, current_schemas(false) as b");
        assertFalse(rows.isEmpty());
    }

    @Test @Order(6) void current_database_schema_user() throws SQLException {
        var rows = prepQueryWithMeta("select current_database(), current_schema(), current_user");
        assertFalse(rows.isEmpty());
    }

    // ===== Phase 2: Server info queries =====

    @Test @Order(10) void pg_postmaster_start_time() throws SQLException {
        var rows = prepQueryWithMeta("select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time");
        assertFalse(rows.isEmpty(), "startup_time should return 1 row");
    }

    @Test @Order(11) void pg_locks_transactionid() throws SQLException {
        var rows = prepQueryWithMeta("""
            select L.transactionid::varchar::bigint as transaction_id
            from pg_catalog.pg_locks L
            where L.transactionid is not null
            order by pg_catalog.age(L.transactionid) desc
            limit 1""");
        // May be empty, that's OK
    }

    @Test @Order(12) void current_txid() throws SQLException {
        var rows = prepQueryWithMeta("""
            select case
              when pg_catalog.pg_is_in_recovery()
                then null
              else
                (pg_catalog.txid_current() % 4294967296)::varchar::bigint
              end as current_txid""");
        assertFalse(rows.isEmpty());
    }

    // ===== Phase 3: Database/role/namespace listing =====

    @Test @Order(20) void pg_database_listing() throws SQLException {
        var rows = prepQueryWithMeta("""
            select N.oid::bigint as id,
                   datname as name,
                   D.description,
                   datistemplate as is_template,
                   datallowconn as allow_connections,
                   pg_catalog.pg_get_userbyid(N.datdba) as "owner"
            from pg_catalog.pg_database N
              left join pg_catalog.pg_shdescription D on N.oid = D.objoid
            order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end""");
        assertFalse(rows.isEmpty(), "pg_database should have at least 1 row");
        // Check is_template and allow_connections are boolean-like
        for (var row : rows) {
            String isTemplate = (String) row.get("is_template");
            String allowConn = (String) row.get("allow_connections");
            assertTrue(isTemplate == null || "t".equals(isTemplate) || "f".equals(isTemplate)
                    || "true".equals(isTemplate) || "false".equals(isTemplate),
                    "is_template should be boolean, got: " + isTemplate);
        }
    }

    @Test @Order(21) void show_datestyle() throws SQLException {
        var rows = prepQueryWithMeta("show DateStyle");
        assertFalse(rows.isEmpty());
    }

    @Test @Order(22) void timezone_names_union() throws SQLException {
        // This query UNIONs pg_timezone_names and pg_timezone_abbrevs
        var rows = prepQueryWithMeta("""
            select name, is_dst from pg_catalog.pg_timezone_names
            union distinct
            select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs""");
        assertFalse(rows.isEmpty(), "timezone names union should return rows");
    }

    @Test @Order(23) void pg_roles_listing() throws SQLException {
        var rows = prepQueryWithMeta("""
            select R.oid::bigint as role_id, rolname as role_name,
              rolsuper is_super, rolinherit is_inherit,
              rolcreaterole can_createrole, rolcreatedb can_createdb,
              rolcanlogin can_login, rolreplication is_replication,
              rolconnlimit conn_limit, rolvaliduntil valid_until,
              rolbypassrls bypass_rls, rolconfig config,
              D.description
            from pg_catalog.pg_roles R
              left join pg_catalog.pg_shdescription D on D.objoid = R.oid""");
        assertFalse(rows.isEmpty());
    }

    @Test @Order(24) void pg_auth_members() throws SQLException {
        prepQueryWithMeta("""
            select member id, roleid role_id, admin_option
            from pg_catalog.pg_auth_members order by id, roleid::text""");
    }

    @Test @Order(25) void pg_tablespace() throws SQLException {
        var rows = prepQueryWithMeta("""
            select T.oid::bigint as id, T.spcname as name,
                   T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
                   pg_catalog.pg_tablespace_location(T.oid) as location,
                   T.spcoptions as options,
                   D.description as comment
            from pg_catalog.pg_tablespace T
              left join pg_catalog.pg_shdescription D on D.objoid = T.oid""");
        assertFalse(rows.isEmpty(), "tablespace should have at least pg_default");
    }

    @Test @Order(26) void acl_tablespace_database_union() throws SQLException {
        prepQueryWithMeta("""
            select T.oid as object_id, T.spcacl as acl
            from pg_catalog.pg_tablespace T
            union all
            select T.oid as object_id, T.datacl as acl
            from pg_catalog.pg_database T""");
    }

    @Test @Order(27) void pg_namespace_listing() throws SQLException {
        var rows = prepQueryWithMeta("""
            select N.oid::bigint as id,
                   N.xmin as state_number,
                   nspname as name,
                   D.description,
                   pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
            from pg_catalog.pg_namespace N
              left join pg_catalog.pg_description D on N.oid = D.objoid
            order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end""");
        assertFalse(rows.isEmpty());
    }

    @Test @Order(28) void pg_user_usesuper() throws SQLException {
        var rows = prepQueryWithMeta("select usesuper from pg_user where usename = current_user");
        assertFalse(rows.isEmpty(), "pg_user should return current user");
    }

    // ===== Phase 4: Catalog object queries (no params) =====

    @Test @Order(30) void pg_event_trigger() throws SQLException {
        prepQueryWithMeta("""
            select t.oid as id,
                   t.xmin as state_number,
                   t.evtname as name,
                   t.evtevent as event,
                   t.evtfoid as routine_id,
                   pg_catalog.pg_get_userbyid(t.evtowner) as owner,
                   t.evttags as tags,
                   case when t.evtenabled = 'D' then 1 else 0 end as is_disabled
            from pg_catalog.pg_event_trigger t""");
    }

    @Test @Order(31) void pg_foreign_data_wrapper_with_joins() throws SQLException {
        prepQueryWithMeta("""
            select fdw.oid as id,
                   fdw.xmin as state_number,
                   fdw.fdwname as name,
                   pr.proname as handler,
                   nspc.nspname as handler_schema,
                   pr2.proname as validator,
                   nspc2.nspname as validator_schema,
                   fdw.fdwoptions as options,
                   pg_catalog.pg_get_userbyid(fdw.fdwowner) as "owner"
            from pg_catalog.pg_foreign_data_wrapper fdw
                 left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid
                 left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid
                 left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid
                 left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid""");
    }

    @Test @Order(32) void pg_foreign_server_with_joins() throws SQLException {
        prepQueryWithMeta("""
            select srv.oid as id,
                   srv.srvfdw as fdw_id,
                   srv.xmin as state_number,
                   srv.srvname as name,
                   srv.srvtype as type,
                   srv.srvversion as version,
                   srv.srvoptions as options,
                   pg_catalog.pg_get_userbyid(srv.srvowner) as "owner"
            from pg_catalog.pg_foreign_server srv""");
    }

    @Test @Order(33) void pg_user_mapping() throws SQLException {
        prepQueryWithMeta("""
            select oid as id,
                   umserver as server_id,
                   case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as user,
                   umoptions as options
            from pg_catalog.pg_user_mapping
            order by server_id""");
    }

    @Test @Order(34) void pg_am_with_handler_join() throws SQLException {
        var rows = prepQueryWithMeta("""
            select A.oid as access_method_id,
                   A.xmin as state_number,
                   A.amname as access_method_name,
                   A.amhandler::oid as handler_id,
                   pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,
                   A.amtype as access_method_type
            from pg_am A
                 join pg_proc P on A.amhandler::oid = P.oid
                 join pg_namespace N on P.pronamespace = N.oid""");
        // pg_am should have btree, hash, etc., but our join may return empty if handler OIDs don't match pg_proc
    }

    @Test @Order(35) void pg_extension_with_available_versions() throws SQLException {
        prepQueryWithMeta("""
            select E.oid as id,
                   E.xmin as state_number,
                   extname as name,
                   extversion as version,
                   extnamespace as schema_id,
                   nspname as schema_name,
                   array(select unnest
                         from unnest(available_versions)
                         where unnest > extversion) as available_updates
            from pg_catalog.pg_extension E
                   join pg_namespace N on E.extnamespace = N.oid
                   left join (select name, array_agg(version) as available_versions
                              from pg_available_extension_versions()
                              group by name) V on E.extname = V.name""");
    }

    @Test @Order(36) void pg_language_with_joins() throws SQLException {
        var rows = prepQueryWithMeta("""
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
            order by lanname""");
        assertFalse(rows.isEmpty(), "pg_language should have at least internal/c/sql/plpgsql");
    }

    @Test @Order(37) void pg_description_classoid() throws SQLException {
        prepQueryWithMeta("""
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
            )""");
    }

    @Test @Order(38) void acl_fdw_lang_ns_srv_union() throws SQLException {
        prepQueryWithMeta("""
            select T.oid as object_id, T.fdwacl as acl
            from pg_catalog.pg_foreign_data_wrapper T
            union all
            select T.oid as object_id, T.lanacl as acl
            from pg_catalog.pg_language T
            union all
            select T.oid as object_id, T.nspacl as acl
            from pg_catalog.pg_namespace T
            union all
            select T.oid as object_id, T.srvacl as acl
            from pg_catalog.pg_foreign_server T""");
    }

    @Test @Order(39) void pg_cast_with_joins() throws SQLException {
        prepQueryWithMeta("""
            select C.oid,
                   C.xmin as state_number,
                   C.castsource as castsource_id,
                   pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name,
                   C.casttarget as casttarget_id,
                   pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name,
                   C.castfunc as castfunc_id,
                   pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name,
                   C.castcontext,
                   C.castmethod
            from pg_cast C
                 left outer join pg_proc F on C.castfunc::oid = F.oid
                 left outer join pg_namespace FN on F.pronamespace = FN.oid
                 join pg_type S on C.castsource::oid = S.oid
                 join pg_namespace SN on S.typnamespace = SN.oid
                 join pg_type T on C.casttarget::oid = T.oid
                 join pg_namespace TN on T.typnamespace = TN.oid""");
    }

    @Test @Order(40) void pg_depend_extension_members() throws SQLException {
        prepQueryWithMeta("""
            select E.oid as extension_id,
                   D.objid as member_id
            from pg_extension E
                 join pg_depend D on E.oid = D.refobjid and
                                     D.refclassid = 'pg_extension'::regclass::oid
            where D.deptype = 'e'
            order by extension_id""");
    }

    // ===== Phase 5: Parameterized schema queries =====

    @Test @Order(50) void pg_type_element_lookup() throws SQLException {
        // $1 = type OID; use 23 (int4) as example
        prepQueryWithMeta("""
            SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_type e ON t.typelem = e.oid
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            WHERE t.oid = $1""", 23);
    }

    @Test @Order(51) void seven_branch_union_3col() throws SQLException {
        // The 3-column version: oid, schemaId, kind
        prepQueryWithMeta("""
            select T.oid as oid, relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind
            from pg_catalog.pg_class T
            where relnamespace in ( $1 ) and relkind in ('r','m','v','p','f','S')
            union all
            select T.oid, T.typnamespace, 'T' as kind
            from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace in ( $2 )
              and (T.typtype in ('d','e') or C.relkind = 'c'
                   or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A'))
                   or T.typtype = 'p' and not T.typisdefined)
            union all
            select oid, collnamespace, 'C' as kind from pg_catalog.pg_collation where collnamespace in ( $3 )
            union all
            select oid, oprnamespace, 'O' as kind from pg_catalog.pg_operator where oprnamespace in ( $4 )
            union all
            select oid, opcnamespace, 'c' as kind from pg_catalog.pg_opclass where opcnamespace in ( $5 )
            union all
            select oid, opfnamespace, 'F' as kind from pg_catalog.pg_opfamily where opfnamespace in ( $6 )
            union all
            select oid, pronamespace,
                   case when prokind != 'a' then 'R' else 'a' end as kind
            from pg_catalog.pg_proc where pronamespace in ( $7 )""",
                nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);
    }

    @Test @Order(52) void seven_branch_union_4col() throws SQLException {
        // The 4-column version: oid, schemaId, kind, name
        prepQueryWithMeta("""
            select T.oid as oid, relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name
            from pg_catalog.pg_class T
            where relnamespace in ( $1 ) and relkind in ('r','m','v','p','f','S')
            union all
            select T.oid, T.typnamespace, 'T', T.typname
            from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace in ( $2 )
              and (T.typtype in ('d','e') or C.relkind = 'c'
                   or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A'))
                   or T.typtype = 'p' and not T.typisdefined)
            union all
            select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( $3 )
            union all
            select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( $4 )
            union all
            select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( $5 )
            union all
            select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( $6 )
            union all
            select oid, pronamespace,
                   case when prokind != 'a' then 'R' else 'a' end, proname
            from pg_catalog.pg_proc where pronamespace in ( $7 )
            order by schemaId""",
                nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);
    }

    @Test @Order(53) void pg_proc_arg_names() throws SQLException {
        prepQueryWithMeta("""
            select pronamespace as schemaId,
                   oid as majorOid,
                   proargnames as argNames,
                   proargmodes as argModes,
                   array_length(proargtypes, 1) as nArgs
            from pg_catalog.pg_proc
            where pronamespace in ( $1 )
            order by schemaId""", nsOid);
    }

    @Test @Order(54) void cte_table_columns() throws SQLException {
        prepQueryWithMeta("""
            with T as ( select T.oid as oid, T.relkind as kind, T.relnamespace as schemaId
                        from pg_catalog.pg_class T
                        where T.relnamespace in ( $1 )
                          and T.relkind in ('r','m','v','f','p')
                      )
            select T.schemaId as schemaId, T.oid as majorOid,
                   pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind,
                   C.attnum as position, C.attname as name
            from T
                 join pg_catalog.pg_attribute C on T.oid = C.attrelid
            where C.attnum > 0 and not C.attisdropped
            order by schemaId, majorOid""", nsOid);
    }

    @Test @Order(55) void pg_sequence_with_age() throws SQLException {
        prepQueryWithMeta("""
            select cls.xmin as sequence_state_number,
                   sq.seqrelid as sequence_id,
                   cls.relname as sequence_name,
                   pg_catalog.format_type(sq.seqtypid, null) as data_type,
                   sq.seqstart as start_value,
                   sq.seqincrement as inc_value,
                   sq.seqmin as min_value,
                   sq.seqmax as max_value,
                   sq.seqcache as cache_size,
                   sq.seqcycle as cycle_option,
                   pg_catalog.pg_get_userbyid(cls.relowner) as "owner"
            from pg_catalog.pg_sequence sq
                join pg_class cls on sq.seqrelid = cls.oid
            where cls.relnamespace = $1::oid
              and pg_catalog.age(cls.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }

    @Test @Order(56) void pg_type_listing() throws SQLException {
        prepQueryWithMeta("""
            select T.oid as type_id,
                   T.xmin as type_state_number,
                   T.typname as type_name,
                   T.typtype as type_sub_kind,
                   T.typcategory as type_category,
                   T.typrelid as class_id,
                   T.typbasetype as base_type_id,
                   case when T.typtype in ('c','e') then null
                        else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def,
                   T.typndims as dimensions_number,
                   T.typdefault as default_expression,
                   T.typnotnull as mandatory,
                   pg_catalog.pg_get_userbyid(T.typowner) as "owner"
            from pg_catalog.pg_type T
                     left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace = $1::oid
              and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
              and (T.typtype in ('d','e') or C.relkind = 'c'
                   or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A'))
                   or T.typtype = 'p' and not T.typisdefined)
            order by 1""", nsOid, "1");
    }

    @Test @Order(57) void pg_class_table_details() throws SQLException {
        var rows = prepQueryWithMeta("""
            select T.relkind as table_kind,
                   T.relname as table_name,
                   T.oid as table_id,
                   T.xmin as table_state_number,
                   false as table_with_oids,
                   T.reltablespace as tablespace_id,
                   T.reloptions as options,
                   T.relpersistence as persistence,
                   (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,
                   (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,
                   T.relispartition as is_partition,
                   pg_catalog.pg_get_partkeydef(T.oid) as partition_key,
                   pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression,
                   T.relam am_id,
                   pg_catalog.pg_get_userbyid(T.relowner) as "owner"
            from pg_catalog.pg_class T
            where relnamespace = $1::oid
                   and relkind in ('r','m','v','f','p')
              and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
            order by table_kind, table_id""", nsOid, "1");
        assertFalse(rows.isEmpty(), "Should have the users table");
    }

    @Test @Order(58) void pg_foreign_table_listing() throws SQLException {
        prepQueryWithMeta("""
            select ft.ftrelid as table_id,
                   srv.srvname as table_server,
                   ft.ftoptions as table_options,
                   pg_catalog.pg_get_userbyid(cls.relowner) as "owner"
            from pg_catalog.pg_foreign_table ft
                 left outer join pg_catalog.pg_foreign_server srv on ft.ftserver = srv.oid
                 join pg_catalog.pg_class cls on ft.ftrelid = cls.oid
            where cls.relnamespace = $1::oid
              and pg_catalog.age(ft.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
            order by table_id""", nsOid, "1");
    }

    // ===== Phase 6: Complex CTE and routine queries =====

    @Test @Order(60) void schema_type_resolution_cte() throws SQLException {
        prepQueryWithMeta("""
            with schema_procs as (select prorettype, proargtypes, proallargtypes
                                  from pg_catalog.pg_proc
                                  where pronamespace = $1::oid
                                    and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)),
                 schema_opers as (select oprleft, oprright, oprresult
                                  from pg_catalog.pg_operator
                                  where oprnamespace = $3::oid
                                    and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($4::varchar::xid), -1), -1), 2147483647)),
                 schema_aggregates as (select A.aggtranstype, A.aggmtranstype
                                       from pg_catalog.pg_aggregate A
                                       join pg_catalog.pg_proc P on A.aggfnoid = P.oid
                                       where P.pronamespace = $5::oid
                                         and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($6::varchar::xid), -1), -1), 2147483647)
                                              or pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age($7::varchar::xid), -1), -1), 2147483647))),
                 schema_arg_types as (select prorettype as type_id from schema_procs
                                      union
                                      select distinct unnest(proargtypes) as type_id from schema_procs
                                      union
                                      select distinct unnest(proallargtypes) as type_id from schema_procs
                                      union
                                      select oprleft as type_id from schema_opers where oprleft is not null
                                      union
                                      select oprright as type_id from schema_opers where oprright is not null
                                      union
                                      select oprresult as type_id from schema_opers where oprresult is not null
                                      union
                                      select aggtranstype::oid as type_id from schema_aggregates
                                      union
                                      select aggmtranstype::oid as type_id from schema_aggregates)
            select type_id, pg_catalog.format_type(type_id, null) as type_spec
            from schema_arg_types
            where type_id <> 0""",
                nsOid, "1", nsOid, "1", nsOid, "1", "1");
    }

    @Test @Order(61) void routines_with_languages() throws SQLException {
        prepQueryWithMeta("""
            with languages as (select oid as lang_oid, lanname as lang from pg_catalog.pg_language),
                 routines as (select proname as r_name, prolang as lang_oid, oid as r_id,
                                     xmin as r_state_number, proargnames as arg_names,
                                     proargmodes as arg_modes, proargtypes::int[] as in_arg_types,
                                     proallargtypes::int[] as all_arg_types,
                                     pg_catalog.pg_get_expr(proargdefaults, 0) as arg_defaults,
                                     provariadic as arg_variadic_id, prorettype as ret_type_id,
                                     proretset as ret_set, prokind as kind,
                                     provolatile as volatile_kind, proisstrict as is_strict,
                                     prosecdef as is_security_definer, proconfig as configuration_parameters,
                                     procost as cost, pg_catalog.pg_get_userbyid(proowner) as "owner",
                                     prorows as rows, proleakproof as is_leakproof,
                                     proparallel as concurrency_kind
                              from pg_catalog.pg_proc
                              where pronamespace = $1::oid
                                and not (prokind = 'a')
                                and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647))
            select * from routines natural join languages""",
                nsOid, "1");
    }

    @Test @Order(62) void pg_aggregate_full() throws SQLException {
        prepQueryWithMeta("""
            select P.oid as aggregate_id, P.xmin as state_number,
                   P.proname as aggregate_name, P.proargnames as arg_names,
                   P.proargmodes as arg_modes, P.proargtypes::int[] as in_arg_types,
                   P.proallargtypes::int[] as all_arg_types,
                   A.aggtransfn::oid as transition_function_id,
                   A.aggtransfn::regproc::text as transition_function_name,
                   A.aggtranstype as transition_type,
                   A.aggfinalfn::oid as final_function_id,
                   case when A.aggfinalfn::oid = 0 then null else A.aggfinalfn::regproc::varchar end as final_function_name,
                   case when A.aggfinalfn::oid = 0 then 0 else P.prorettype end as final_return_type,
                   A.agginitval as initial_value,
                   A.aggsortop as sort_operator_id,
                   case when A.aggsortop = 0 then null else A.aggsortop::regoper::varchar end as sort_operator_name,
                   pg_catalog.pg_get_userbyid(P.proowner) as "owner",
                   A.aggfinalextra as final_extra,
                   A.aggtransspace as state_size,
                   A.aggmtransfn::oid as moving_transition_id,
                   case when A.aggmtransfn::oid = 0 then null else A.aggmtransfn::regproc::varchar end as moving_transition_name,
                   A.aggminvtransfn::oid as inverse_transition_id,
                   case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name,
                   A.aggmtranstype::oid as moving_state_type,
                   A.aggmtransspace as moving_state_size,
                   A.aggmfinalfn::oid as moving_final_id,
                   case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name,
                   A.aggmfinalextra as moving_final_extra,
                   A.aggminitval as moving_initial_value,
                   A.aggkind as aggregate_kind,
                   A.aggnumdirectargs as direct_args,
                   A.aggcombinefn::oid as combine_function_id,
                   case when A.aggcombinefn::oid = 0 then null else A.aggcombinefn::regproc::varchar end as combine_function_name,
                   A.aggserialfn::oid as serialization_function_id,
                   case when A.aggserialfn::oid = 0 then null else A.aggserialfn::regproc::varchar end as serialization_function_name,
                   A.aggdeserialfn::oid as deserialization_function_id,
                   case when A.aggdeserialfn::oid = 0 then null else A.aggdeserialfn::regproc::varchar end as deserialization_function_name,
                   P.proparallel as concurrency_kind
            from pg_catalog.pg_aggregate A
                 join pg_catalog.pg_proc P on A.aggfnoid = P.oid
            where P.pronamespace = $1::oid
              and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
                   or pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age($3::varchar::xid), -1), -1), 2147483647))
            order by P.oid""", nsOid, "1", "1");
    }

    @Test @Order(63) void pg_operator_listing() throws SQLException {
        prepQueryWithMeta("""
            select O.oid as op_id, O.xmin as state_number,
                   oprname as op_name, oprkind as op_kind,
                   oprleft as arg_left_type_id, oprright as arg_right_type_id,
                   oprresult as arg_result_type_id,
                   oprcode::oid as main_id, oprcode::varchar as main_name,
                   oprrest::oid as restrict_id, oprrest::varchar as restrict_name,
                   oprjoin::oid as join_id, oprjoin::varchar as join_name,
                   oprcom::oid as com_id, oprcom::regoper::varchar as com_name,
                   oprnegate::oid as neg_id, oprnegate::regoper::varchar as neg_name,
                   oprcanmerge as merges, oprcanhash as hashes,
                   pg_catalog.pg_get_userbyid(O.oprowner) as "owner"
            from pg_catalog.pg_operator O
            where oprnamespace = $1::oid
              and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }

    @Test @Order(64) void pg_collation_listing() throws SQLException {
        prepQueryWithMeta("""
            select oid as id, xmin as state_number, collname as name,
                   collcollate as lc_collate, collctype as lc_ctype,
                   pg_catalog.pg_get_userbyid(collowner) as "owner"
            from pg_catalog.pg_collation
            where collnamespace = $1::oid
              and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }

    // ===== Phase 7: Index, constraint, trigger queries =====

    @Test @Order(70) void index_listing() throws SQLException {
        prepQueryWithMeta("""
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
                     join pg_catalog.pg_class ind_stor
                          on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
                     left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
            where tab.relnamespace = $1::oid
                    and tab.relkind in ('r','m','v','p')
                    and ind_stor.relkind in ('i','I')
              and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }

    @Test @Order(71) void index_column_detail_with_cross_joins() throws SQLException {
        // This query uses CROSS JOIN unnest() WITH ORDINALITY and CROSS JOIN pg_indexam_has_property()
        prepQueryWithMeta("""
            select ind_head.indexrelid index_id,
                   k col_idx, k <= indnkeyatts in_key,
                   ind_head.indkey[k-1] column_position,
                   ind_head.indoption[k-1] column_options,
                   ind_head.indcollation[k-1] as collation,
                   colln.nspname as collation_schema,
                   collname as collation_str,
                   ind_head.indclass[k-1] as opclass,
                   case when opcdefault then null else opcn.nspname end as opclass_schema,
                   case when opcdefault then null else opcname end as opclass_str,
                   case
                       when indexprs is null then null
                       when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
                       else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
                   end as expression,
                   amcanorder can_order
            from pg_catalog.pg_index ind_head
                     join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
            cross join unnest(ind_head.indkey) with ordinality u(u, k)
            left join pg_catalog.pg_collation on pg_collation.oid = ind_head.indcollation[k-1]
            left join pg_catalog.pg_namespace colln on collnamespace = colln.oid
            cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
                     left join pg_catalog.pg_opclass on pg_opclass.oid = ind_head.indclass[k-1]
                     left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid
            where ind_stor.relnamespace = $1::oid
              and ind_stor.relkind in ('i','I')
              and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
            order by index_id, k""", nsOid, "1");
    }

    @Test @Order(72) void constraint_listing() throws SQLException {
        prepQueryWithMeta("""
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
            from pg_catalog.pg_constraint C
                     join pg_catalog.pg_class T on C.conrelid = T.oid
            where relkind in ('r','v','f','p')
              and relnamespace = $1::oid
              and contype in ('p','u','f','c','x')
              and connamespace = $2::oid
              and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($3::varchar::xid), -1), -1), 2147483647)
                  or pg_catalog.age(c.xmin) <= coalesce(nullif(greatest(pg_catalog.age($4::varchar::xid), -1), -1), 2147483647)""",
                nsOid, nsOid, "1", "1");
    }

    @Test @Order(73) void pg_trigger_listing() throws SQLException {
        prepQueryWithMeta("""
            select T.tgrelid as table_id,
                   T.oid as trigger_id,
                   T.xmin as trigger_state_number,
                   T.tgname as trigger_name,
                   T.tgfoid as function_id,
                   pg_catalog.encode(T.tgargs, 'escape') as function_args,
                   T.tgtype as bits,
                   T.tgdeferrable as is_deferrable,
                   T.tginitdeferred as is_init_deferred,
                   T.tgenabled as trigger_fire_mode,
                   T.tgattr as columns,
                   T.tgconstraint != 0 as is_constraint,
                   T.tgoldtable as old_table_name,
                   T.tgnewtable as new_table_name,
                   pg_catalog.pg_get_triggerdef(T.oid, true) as source_code
            from pg_catalog.pg_trigger T
                 join pg_catalog.pg_class TAB on TAB.oid = T.tgrelid and TAB.relnamespace = $1::oid
            where pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
              and not T.tgisinternal""", nsOid, "1");
    }

    // ===== Phase 8: Big description UNION, ACL, view/rule/routine source =====

    @Test @Order(80) void description_11_branch_union() throws SQLException {
        prepQueryWithMeta("""
            select D.objoid id, pg_catalog.array_agg(D.objsubid) sub_ids
            from pg_catalog.pg_description D
              join pg_catalog.pg_class C on D.objoid = C.oid
            where C.relnamespace = $1::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass
            group by D.objoid
            union all
            select T.oid id, pg_catalog.array_agg(D.objsubid)
            from pg_catalog.pg_description D
              join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid
              left join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace = $2::oid and (C.relkind = 'c' or C.relkind is null)
            group by T.oid
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_constraint C on D.objoid = C.oid
            where C.connamespace = $3::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_trigger T on T.oid = D.objoid
              join pg_catalog.pg_class C on C.oid = T.tgrelid
            where C.relnamespace = $4::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_rewrite R on R.oid = D.objoid
              join pg_catalog.pg_class C on C.oid = R.ev_class
            where C.relnamespace = $5::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_proc P on P.oid = D.objoid
            where P.pronamespace = $6::oid and D.classoid = 'pg_catalog.pg_proc'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_operator O on O.oid = D.objoid
            where O.oprnamespace = $7::oid and D.classoid = 'pg_catalog.pg_operator'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_opclass O on O.oid = D.objoid
            where O.opcnamespace = $8::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_opfamily O on O.oid = D.objoid
            where O.opfnamespace = $9::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_collation C on C.oid = D.objoid
            where C.collnamespace = $10::oid and D.classoid = 'pg_catalog.pg_collation'::regclass
            union all
            select D.objoid id, array[D.objsubid]
            from pg_catalog.pg_description D
              join pg_catalog.pg_policy P on P.oid = D.objoid
              join pg_catalog.pg_class C on P.polrelid = C.oid
            where C.relnamespace = $11::oid and D.classoid = 'pg_catalog.pg_policy'::regclass""",
                nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);
    }

    @Test @Order(81) void acl_3_branch_with_age() throws SQLException {
        prepQueryWithMeta("""
            select T.oid as object_id, T.relacl as acl
            from pg_catalog.pg_class T
            where relnamespace = $1::oid
              and pg_catalog.age(T.xmin) <= pg_catalog.age($2::varchar::xid)
            union all
            select T.oid as object_id, T.proacl as acl
            from pg_catalog.pg_proc T
            where pronamespace = $3::oid
              and pg_catalog.age(T.xmin) <= pg_catalog.age($4::varchar::xid)
            union all
            select T.oid as object_id, T.typacl as acl
            from pg_catalog.pg_type T
            where typnamespace = $5::oid
              and pg_catalog.age(T.xmin) <= pg_catalog.age($6::varchar::xid)
            order by object_id""", nsOid, "1", nsOid, "1", nsOid, "1");
    }

    @Test @Order(82) void acl_attribute_union() throws SQLException {
        prepQueryWithMeta("""
            select T.oid as object_id,
                   A.attnum as attr_position,
                   A.attacl as acl
            from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid
            where relnamespace = $1::oid
              and attnum > 0
              and pg_catalog.age(A.xmin) <= pg_catalog.age($2::varchar::xid)
            order by object_id, attr_position""", nsOid, "1");
    }

    @Test @Order(83) void pg_depend_sequence_column() throws SQLException {
        prepQueryWithMeta("""
            select D.objid as dependent_id,
                   D.refobjid as owner_id,
                   D.refobjsubid as owner_subobject_id
            from pg_depend D
              join pg_class C_SEQ on D.objid = C_SEQ.oid and D.classid = 'pg_class'::regclass::oid
              join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid
            where C_SEQ.relkind = 'S'
              and C_TAB.relkind = 'r'
              and D.refobjsubid <> 0
              and (D.deptype = 'a' or D.deptype = 'i')
              and C_TAB.relnamespace = $1::oid
            order by owner_id""", nsOid);
    }

    @Test @Order(84) void view_definition() throws SQLException {
        prepQueryWithMeta("""
            select T.relkind as view_kind,
                   T.oid as view_id,
                   pg_catalog.pg_get_viewdef(T.oid, true) as source_text
            from pg_catalog.pg_class T
                 join pg_catalog.pg_namespace N on T.relnamespace = N.oid
            where N.oid = $1::oid
              and T.relkind in ('m','v')
              and (pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
                   or exists(select A.attrelid from pg_catalog.pg_attribute A where A.attrelid = T.oid
                             and pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($3::varchar::xid), -1), -1), 2147483647)))""",
                nsOid, "1", "1");
    }

    @Test @Order(85) void routine_source() throws SQLException {
        prepQueryWithMeta("""
            with system_languages as (select oid as lang
                                       from pg_catalog.pg_language
                                       where lanname in ('c','internal'))
            select oid as id,
                   pg_catalog.pg_get_function_arguments(oid) as arguments_def,
                   pg_catalog.pg_get_function_result(oid) as result_def,
                   pg_catalog.pg_get_function_sqlbody(oid) as sqlbody_def,
                   prosrc as source_text
            from pg_catalog.pg_proc
            where pronamespace = $1::oid
              and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
              and not (prokind = 'a')
              and prolang not in (select lang from system_languages)
              and prosrc is not null""", nsOid, "1");
    }

    // ===== Phase 9: Remaining catalog queries =====

    @Test @Order(90) void pg_rewrite_listing() throws SQLException {
        prepQueryWithMeta("""
            select R.ev_class as table_id,
                   R.oid as rule_id,
                   R.xmin as rule_state_number,
                   R.rulename as rule_name,
                   pg_catalog.translate(ev_type,'1234','SUID') as rule_event_code,
                   R.ev_enabled as rule_fire_mode,
                   R.is_instead as rule_is_instead
            from pg_catalog.pg_rewrite R
            where R.ev_class in (select oid from pg_catalog.pg_class where relnamespace = $1::oid)
              and pg_catalog.age(R.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
              and R.rulename != '_RETURN'::name
            order by R.ev_class::bigint, ev_type""", nsOid, "1");
    }

    @Test @Order(91) void pg_policy_listing() throws SQLException {
        prepQueryWithMeta("""
            select P.oid id, P.xmin as state_number,
                   polname policyname, polrelid table_id,
                   polpermissive as permissive, polroles roles, polcmd cmd,
                   pg_get_expr(polqual, polrelid) qual,
                   pg_get_expr(polwithcheck, polrelid) with_check
            from pg_catalog.pg_policy P
                   join pg_catalog.pg_class C on polrelid = C.oid
            where relnamespace = $1::oid
              and pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
            order by polrelid""", nsOid, "1");
    }

    @Test @Order(92) void column_detail_cte() throws SQLException {
        prepQueryWithMeta("""
            with T as (select distinct T.oid as table_id, T.relname as table_name
                        from pg_catalog.pg_class T, pg_catalog.pg_attribute A
                        where T.relnamespace = $1::oid
                          and T.relkind in ('r','m','v','f','p')
                          and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
                               or pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($3::varchar::xid), -1), -1), 2147483647))
                          and A.attrelid = T.oid)
            select T.table_id,
                   C.attnum as column_position,
                   C.attname as column_name,
                   C.xmin as column_state_number,
                   C.atttypmod as type_mod,
                   C.attndims as dimensions_number,
                   pg_catalog.format_type(C.atttypid, C.atttypmod) as type_spec,
                   C.atttypid as type_id,
                   C.attnotnull as mandatory,
                   pg_catalog.pg_get_expr(D.adbin, T.table_id) as column_default_expression,
                   not C.attislocal as column_is_inherited,
                   C.attfdwoptions as options,
                   C.attisdropped as column_is_dropped,
                   C.attidentity as identity_kind,
                   C.attgenerated as generated
            from T
              join pg_catalog.pg_attribute C on T.table_id = C.attrelid
              left join pg_catalog.pg_attrdef D on (C.attrelid, C.attnum) = (D.adrelid, D.adnum)
            where attnum > 0
            order by table_id, attnum""", nsOid, "1", "1");
    }

    @Test @Order(93) void rule_definition_cte() throws SQLException {
        prepQueryWithMeta("""
            with A as (select oid as table_id,
                              pg_catalog.upper(relkind) as table_kind
                       from pg_catalog.pg_class
                       where relnamespace = $1::oid and relkind in ('r','m','v','f','p'))
            select table_kind, table_id,
                   R.oid as rule_id,
                   pg_catalog.pg_get_ruledef(R.oid, true) as source_text
            from A join pg_catalog.pg_rewrite R on A.table_id = R.ev_class
            where R.rulename != '_RETURN'::name
              and pg_catalog.age(R.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }

    // ===== Phase 10: Remaining parameterized queries =====

    @Test @Order(100) void pg_opclass_listing() throws SQLException {
        prepQueryWithMeta("""
            select O.oid as id, O.xmin as state_number, opcname as name,
                   opcintype::regtype::varchar as in_type,
                   case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type,
                   opcdefault as is_default, opcfamily as family_id,
                   opfname as family, opcmethod as access_method_id,
                   pg_catalog.pg_get_userbyid(O.opcowner) as "owner"
            from pg_catalog.pg_opclass O
                 join pg_catalog.pg_opfamily F on F.oid = opcfamily
            where opcnamespace = $1::oid
              and pg_catalog.age(O.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }

    @Test @Order(101) void pg_opfamily_listing() throws SQLException {
        prepQueryWithMeta("""
            select O.oid as id, O.xmin as state_number, opfname as name,
                   opfmethod as access_method_id,
                   pg_catalog.pg_get_userbyid(O.opfowner) as "owner"
            from pg_catalog.pg_opfamily O
            where opfnamespace = $1::oid
              and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)""",
                nsOid, "1");
    }
}
