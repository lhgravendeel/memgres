package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

/**
 * Tests the database client protocol flow using extended query protocol (PreparedStatement).
 * This test connects WITHOUT ?preferQueryMode=simple, so every PreparedStatement
 * uses Parse/Bind/Describe/Execute - the real extended protocol path.
 *
 * The goal is to identify which queries trigger "No results were returned by the query."
 * (PSQLException when executeQuery() is called but server sends CommandComplete instead of RowDescription).
 */
class ClientProtocolFlowTest {

    static Memgres memgres;
    static Connection conn;       // extended protocol (no preferQueryMode=simple)
    static Connection simpleConn; // simple protocol for setup queries
    static long nsOid;

    static final List<String> SUCCESSES = new ArrayList<>();
    static final List<String> FAILURES = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();

        // Simple connection for setup
        simpleConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        simpleConn.setAutoCommit(true);

        // Extended protocol connection (the one under test)
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Get the public namespace OID
        try (Statement s = simpleConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            rs.next();
            nsOid = rs.getLong(1);
        }
        System.out.println("Public namespace OID: " + nsOid);
    }

    @AfterAll
    static void tearDown() throws Exception {
        System.out.println("\n========================================");
        System.out.println("RESULTS SUMMARY");
        System.out.println("========================================");
        System.out.println("Successes: " + SUCCESSES.size());
        System.out.println("Failures:  " + FAILURES.size());
        if (!FAILURES.isEmpty()) {
            System.out.println("\nFAILED QUERIES:");
            for (String f : FAILURES) {
                System.out.println("  - " + f);
            }
        }
        System.out.println("========================================\n");

        if (conn != null) conn.close();
        if (simpleConn != null) simpleConn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * Run a SELECT query via extended protocol (PreparedStatement.executeQuery).
     * This is where "No results were returned" would surface.
     */
    private void tryExtendedQuery(String label, String sql, long... params) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setLong(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                // Just drain the result set
                int cols = rs.getMetaData().getColumnCount();
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                System.out.println("[OK]   " + label + " -> " + rows + " rows, " + cols + " cols");
                SUCCESSES.add(label);
            }
        } catch (SQLException e) {
            String msg = e.getMessage();
            System.out.println("[FAIL] " + label + " -> " + msg);
            FAILURES.add(label + " :: " + msg);
        }
    }

    /**
     * Run a SET/SHOW statement via extended protocol using execute() (not executeQuery()).
     * SET returns no result set, so we must use execute().
     */
    private void tryExtendedExecute(String label, String sql) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
            System.out.println("[OK]   " + label);
            SUCCESSES.add(label);
        } catch (SQLException e) {
            String msg = e.getMessage();
            System.out.println("[FAIL] " + label + " -> " + msg);
            FAILURES.add(label + " :: " + msg);
        }
    }

    /**
     * Run a SHOW statement via extended protocol using executeQuery() - SHOW returns a result set.
     */
    private void tryExtendedShow(String label, String sql) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                System.out.println("[OK]   " + label + " -> " + rows + " rows");
                SUCCESSES.add(label);
            }
        } catch (SQLException e) {
            String msg = e.getMessage();
            System.out.println("[FAIL] " + label + " -> " + msg);
            FAILURES.add(label + " :: " + msg);
        }
    }

    // =========================================================================
    // Q1-Q2: SET statements (use execute, not executeQuery)
    // =========================================================================

    @Test @Order(1)
    void q01_set_extra_float_digits() {
        tryExtendedExecute("Q01: SET extra_float_digits = 3",
                "SET extra_float_digits = 3");
    }

    @Test @Order(2)
    void q02_set_application_name_empty() {
        tryExtendedExecute("Q02: SET application_name = ''",
                "SET application_name = ''");
    }

    // =========================================================================
    // Q3: select version()
    // =========================================================================

    @Test @Order(3)
    void q03_select_version() {
        tryExtendedQuery("Q03: select version()",
                "select version()");
    }

    // =========================================================================
    // Q4: SET application_name
    // =========================================================================

    @Test @Order(4)
    void q04_set_application_name() {
        tryExtendedExecute("Q04: SET application_name = 'test_client'",
                "SET application_name = 'test_client'");
    }

    // =========================================================================
    // Q5: select current_database() as a, current_schemas(false) as b
    // =========================================================================

    @Test @Order(5)
    void q05_current_database_schemas() {
        tryExtendedQuery("Q05: select current_database() as a, current_schemas(false) as b",
                "select current_database() as a, current_schemas(false) as b");
    }

    // =========================================================================
    // Q6: select current_database(), current_schema(), current_user
    // =========================================================================

    @Test @Order(6)
    void q06_current_database_schema_user() {
        tryExtendedQuery("Q06: select current_database(), current_schema(), current_user",
                "select current_database(), current_schema(), current_user");
    }

    // =========================================================================
    // Q7: pg_postmaster_start_time
    // =========================================================================

    @Test @Order(7)
    void q07_startup_time() {
        tryExtendedQuery("Q07: pg_postmaster_start_time",
                "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time");
    }

    // =========================================================================
    // Q8: pg_locks
    // =========================================================================

    @Test @Order(8)
    void q08_pg_locks() {
        tryExtendedQuery("Q08: pg_locks transactionid",
                """
                select L.transactionid::varchar::bigint as transaction_id
                from pg_catalog.pg_locks L
                where L.transactionid is not null
                order by pg_catalog.age(L.transactionid) desc
                limit 1""");
    }

    // =========================================================================
    // Q9: txid_current
    // =========================================================================

    @Test @Order(9)
    void q09_txid_current() {
        tryExtendedQuery("Q09: txid_current",
                """
                select case
                  when pg_catalog.pg_is_in_recovery()
                    then null
                  else
                    (pg_catalog.txid_current() % 4294967296)::varchar::bigint
                  end as current_txid""");
    }

    // =========================================================================
    // Q10: pg_database
    // =========================================================================

    @Test @Order(10)
    void q10_pg_database() {
        tryExtendedQuery("Q10: pg_database",
                """
                select N.oid::bigint as id,
                       datname as name,
                       D.description,
                       datistemplate as is_template,
                       datallowconn as allow_connections,
                       pg_catalog.pg_get_userbyid(N.datdba) as "owner"
                from pg_catalog.pg_database N
                  left join pg_catalog.pg_shdescription D on N.oid = D.objoid
                order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end""");
    }

    // =========================================================================
    // Q11: show DateStyle
    // =========================================================================

    @Test @Order(11)
    void q11_show_datestyle() {
        tryExtendedShow("Q11: show DateStyle",
                "show DateStyle");
    }

    // =========================================================================
    // Q12: pg_timezone_names union pg_timezone_abbrevs
    // =========================================================================

    @Test @Order(12)
    void q12_timezone_names() {
        tryExtendedQuery("Q12: pg_timezone_names union pg_timezone_abbrevs",
                """
                select name, is_dst from pg_catalog.pg_timezone_names
                union distinct
                select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs""");
    }

    // =========================================================================
    // Q13: pg_roles
    // =========================================================================

    @Test @Order(13)
    void q13_pg_roles() {
        tryExtendedQuery("Q13: pg_roles",
                """
                select R.oid::bigint as role_id, rolname as role_name,
                  rolsuper is_super, rolinherit is_inherit,
                  rolcreaterole can_createrole, rolcreatedb can_createdb,
                  rolcanlogin can_login, rolreplication is_replication,
                  rolconnlimit conn_limit, rolvaliduntil valid_until,
                  rolbypassrls bypass_rls, rolconfig config,
                  D.description
                from pg_catalog.pg_roles R
                  left join pg_catalog.pg_shdescription D on D.objoid = R.oid""");
    }

    // =========================================================================
    // Q14: pg_auth_members
    // =========================================================================

    @Test @Order(14)
    void q14_pg_auth_members() {
        tryExtendedQuery("Q14: pg_auth_members",
                """
                select member id, roleid role_id, admin_option
                from pg_catalog.pg_auth_members order by id, roleid::text""");
    }

    // =========================================================================
    // Q15: pg_tablespace
    // =========================================================================

    @Test @Order(15)
    void q15_pg_tablespace() {
        tryExtendedQuery("Q15: pg_tablespace",
                """
                select T.oid::bigint as id, T.spcname as name,
                       T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
                       pg_catalog.pg_tablespace_location(T.oid) as location,
                       T.spcoptions as options,
                       D.description as comment
                from pg_catalog.pg_tablespace T
                  left join pg_catalog.pg_shdescription D on D.objoid = T.oid""");
    }

    // =========================================================================
    // Q16: acl union (tablespace + database)
    // =========================================================================

    @Test @Order(16)
    void q16_acl_tablespace_database() {
        tryExtendedQuery("Q16: acl tablespace+database",
                """
                select T.oid as object_id,
                       T.spcacl as acl
                from pg_catalog.pg_tablespace T
                union all
                select T.oid as object_id,
                       T.datacl as acl
                from pg_catalog.pg_database T""");
    }

    // =========================================================================
    // Q17: pg_namespace
    // =========================================================================

    @Test @Order(17)
    void q17_pg_namespace() {
        tryExtendedQuery("Q17: pg_namespace",
                """
                select N.oid::bigint as id,
                       N.xmin as state_number,
                       nspname as name,
                       D.description,
                       pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
                from pg_catalog.pg_namespace N
                  left join pg_catalog.pg_description D on N.oid = D.objoid
                order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end""");
    }

    // =========================================================================
    // Q18: pg_user usesuper
    // =========================================================================

    @Test @Order(18)
    void q18_pg_user_usesuper() {
        tryExtendedQuery("Q18: pg_user usesuper",
                """
                select usesuper
                from pg_user
                where usename = current_user""");
    }

    // =========================================================================
    // Q19: pg_event_trigger
    // =========================================================================

    @Test @Order(19)
    void q19_pg_event_trigger() {
        tryExtendedQuery("Q19: pg_event_trigger",
                """
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

    // =========================================================================
    // Q20: pg_foreign_data_wrapper
    // =========================================================================

    @Test @Order(20)
    void q20_pg_foreign_data_wrapper() {
        tryExtendedQuery("Q20: pg_foreign_data_wrapper",
                """
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

    // =========================================================================
    // Q21: pg_foreign_server
    // =========================================================================

    @Test @Order(21)
    void q21_pg_foreign_server() {
        tryExtendedQuery("Q21: pg_foreign_server",
                """
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

    // =========================================================================
    // Q22: pg_user_mapping
    // =========================================================================

    @Test @Order(22)
    void q22_pg_user_mapping() {
        tryExtendedQuery("Q22: pg_user_mapping",
                """
                select oid as id,
                       umserver as server_id,
                       case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as user,
                       umoptions as options
                from pg_catalog.pg_user_mapping
                order by server_id""");
    }

    // =========================================================================
    // Q23: pg_am
    // =========================================================================

    @Test @Order(23)
    void q23_pg_am() {
        tryExtendedQuery("Q23: pg_am",
                """
                select A.oid as access_method_id,
                       A.xmin as state_number,
                       A.amname as access_method_name,
                       A.amhandler::oid as handler_id,
                       pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,
                       A.amtype as access_method_type
                from pg_am A
                     join pg_proc P on A.amhandler::oid = P.oid
                     join pg_namespace N on P.pronamespace = N.oid""");
    }

    // =========================================================================
    // Q24: pg_extension with pg_available_extension_versions
    // =========================================================================

    @Test @Order(24)
    void q24_pg_extension() {
        tryExtendedQuery("Q24: pg_extension",
                """
                select E.oid        as id,
                       E.xmin       as state_number,
                       extname      as name,
                       extversion   as version,
                       extnamespace as schema_id,
                       nspname      as schema_name,
                       array(select unnest
                             from unnest(available_versions)
                             where unnest > extversion) as available_updates
                from pg_catalog.pg_extension E
                       join pg_namespace N on E.extnamespace = N.oid
                       left join (select name, array_agg(version) as available_versions
                                  from pg_available_extension_versions()
                                  group by name) V on E.extname = V.name""");
    }

    // =========================================================================
    // Q25: pg_type element lookup ($1 parameterized)
    // =========================================================================

    @Test @Order(25)
    void q25_pg_type_element() {
        // This query uses $1 as a type OID - use 1009 (text array _text)
        tryExtendedQuery("Q25: pg_type element lookup",
                """
                SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_type e ON t.typelem = e.oid
                JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.oid = $1""", 1009);
    }

    // =========================================================================
    // Q26: pg_language
    // =========================================================================

    @Test @Order(26)
    void q26_pg_language() {
        tryExtendedQuery("Q26: pg_language",
                """
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
    }

    // =========================================================================
    // Q27: pg_description for event_trigger/am/cast/fdw/server/language/extension
    // =========================================================================

    @Test @Order(27)
    void q27_pg_description_global() {
        tryExtendedQuery("Q27: pg_description global objects",
                """
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

    // =========================================================================
    // Q28: acl union (fdw, language, namespace, server)
    // =========================================================================

    @Test @Order(28)
    void q28_acl_fdw_lang_ns_srv() {
        tryExtendedQuery("Q28: acl fdw+language+namespace+server",
                """
                select T.oid as object_id,
                       T.fdwacl as acl
                from pg_catalog.pg_foreign_data_wrapper T
                union all
                select T.oid as object_id,
                       T.lanacl as acl
                from pg_catalog.pg_language T
                union all
                select T.oid as object_id,
                       T.nspacl as acl
                from pg_catalog.pg_namespace T
                union all
                select T.oid as object_id,
                       T.srvacl as acl
                from pg_catalog.pg_foreign_server T""");
    }

    // =========================================================================
    // Q29: pg_cast
    // =========================================================================

    @Test @Order(29)
    void q29_pg_cast() {
        tryExtendedQuery("Q29: pg_cast",
                """
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

    // =========================================================================
    // Q30: pg_extension members (pg_depend)
    // =========================================================================

    @Test @Order(30)
    void q30_extension_members() {
        tryExtendedQuery("Q30: pg_extension members",
                """
                select E.oid   as extension_id,
                       D.objid as member_id
                from pg_extension E
                     join pg_depend D on E.oid = D.refobjid and
                                         D.refclassid = 'pg_extension'::regclass::oid
                where D.deptype = 'e'
                order by extension_id""");
    }

    // =========================================================================
    // Q31: schema objects with null namespace (initial probe)
    // =========================================================================

    @Test @Order(31)
    void q31_schema_objects_null() {
        tryExtendedQuery("Q31: schema objects null ns",
                """
                select T.oid as oid,
                       relnamespace as schemaId,
                       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind
                from pg_catalog.pg_class T
                where relnamespace in ( null )
                  and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
                union all
                select T.oid,
                       T.typnamespace,
                       'T' as kind
                from pg_catalog.pg_type T
                     left outer join pg_catalog.pg_class C on T.typrelid = C.oid
                where T.typnamespace in ( null )
                  and ( T.typtype in ('d','e') or
                        C.relkind = 'c'::"char" or
                        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
                        T.typtype = 'p' and not T.typisdefined )
                union all
                select oid,
                       collnamespace,
                       'C' as kind
                from pg_catalog.pg_collation
                where collnamespace in ( null )
                union all
                select oid,
                       oprnamespace,
                       'O' as kind
                from pg_catalog.pg_operator
                where oprnamespace in ( null )
                union all
                select oid,
                       opcnamespace,
                       'c' as kind
                from pg_catalog.pg_opclass
                where opcnamespace in ( null )
                union all
                select oid,
                       opfnamespace,
                       'F' as kind
                from pg_catalog.pg_opfamily
                where opfnamespace in ( null )
                union all
                select oid,
                       pronamespace,
                       case when prokind != 'a' then 'R'
                            else 'a'
                            end as kind
                from pg_catalog.pg_proc
                where pronamespace in ( null )""");
    }

    // =========================================================================
    // Q32: schema objects with $1..$7 (parameterized)
    // =========================================================================

    @Test @Order(32)
    void q32_schema_objects_parameterized() {
        tryExtendedQuery("Q32: schema objects parameterized",
                """
                select T.oid as oid,
                       relnamespace as schemaId,
                       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind,
                       relname as name
                from pg_catalog.pg_class T
                where relnamespace in ( $1 )
                  and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
                union all
                select T.oid,
                       T.typnamespace,
                       'T',
                       T.typname
                from pg_catalog.pg_type T
                     left outer join pg_catalog.pg_class C on T.typrelid = C.oid
                where T.typnamespace in ( $2 )
                  and ( T.typtype in ('d','e') or
                        C.relkind = 'c'::"char" or
                        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
                        T.typtype = 'p' and not T.typisdefined )
                union all
                select oid,
                       collnamespace,
                       'C',
                       collname
                from pg_catalog.pg_collation
                where collnamespace in ( $3 )
                union all
                select oid,
                       oprnamespace,
                       'O',
                       oprname
                from pg_catalog.pg_operator
                where oprnamespace in ( $4 )
                union all
                select oid,
                       opcnamespace,
                       'c',
                       opcname
                from pg_catalog.pg_opclass
                where opcnamespace in ( $5 )
                union all
                select oid,
                       opfnamespace,
                       'F',
                       opfname
                from pg_catalog.pg_opfamily
                where opfnamespace in ( $6 )
                union all
                select oid,
                       pronamespace,
                       case when prokind != 'a' then 'R'
                            else 'a'
                            end,
                       proname
                from pg_catalog.pg_proc
                where pronamespace in ( $7 )
                order by schemaId""",
                nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);
    }

    // =========================================================================
    // Q33: pg_proc argnames/argmodes
    // =========================================================================

    @Test @Order(33)
    void q33_pg_proc_args() {
        tryExtendedQuery("Q33: pg_proc argnames/argmodes",
                """
                select pronamespace as schemaId,
                       oid as majorOid,
                       proargnames as argNames,
                       proargmodes as argModes,
                       array_length(proargtypes, 1) as nArgs
                from pg_catalog.pg_proc
                where pronamespace in ( $1 )
                order by schemaId""", nsOid);
    }

    // =========================================================================
    // Q34: CTE pg_attribute columns
    // =========================================================================

    @Test @Order(34)
    void q34_pg_attribute_columns() {
        tryExtendedQuery("Q34: pg_attribute columns via CTE",
                """
                with T as ( select T.oid as oid,
                                   T.relkind as kind,
                                   T.relnamespace as schemaId
                            from pg_catalog.pg_class T
                            where T.relnamespace in ( $1 )
                              and T.relkind in ('r', 'm', 'v', 'f', 'p')
                          )
                select T.schemaId as schemaId,
                       T.oid as majorOid,
                       pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind,
                       C.attnum as position,
                       C.attname as name
                from T
                     join pg_catalog.pg_attribute C on T.oid = C.attrelid
                where C.attnum > 0
                  and not C.attisdropped
                order by schemaId, majorOid""", nsOid);
    }

    // =========================================================================
    // Q35: pg_sequence
    // =========================================================================

    @Test @Order(35)
    void q35_pg_sequence() {
        tryExtendedQuery("Q35: pg_sequence",
                """
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
                    where cls.relnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q36: pg_type details
    // =========================================================================

    @Test @Order(36)
    void q36_pg_type_details() {
        tryExtendedQuery("Q36: pg_type details",
                """
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
                         left outer join pg_catalog.pg_class C
                             on T.typrelid = C.oid
                where T.typnamespace = $1::oid
                  and (T.typtype in ('d','e') or
                       C.relkind = 'c'::"char" or
                       (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
                       T.typtype = 'p' and not T.typisdefined)
                order by 1""", nsOid);
    }

    // =========================================================================
    // Q37: pg_class tables
    // =========================================================================

    @Test @Order(37)
    void q37_pg_class_tables() {
        tryExtendedQuery("Q37: pg_class tables",
                """
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
                       and relkind in ('r', 'm', 'v', 'f', 'p')
                order by table_kind, table_id""", nsOid);
    }

    // =========================================================================
    // Q38: pg_foreign_table
    // =========================================================================

    @Test @Order(38)
    void q38_pg_foreign_table() {
        tryExtendedQuery("Q38: pg_foreign_table",
                """
                select ft.ftrelid as table_id,
                       srv.srvname as table_server,
                       ft.ftoptions as table_options,
                       pg_catalog.pg_get_userbyid(cls.relowner) as "owner"
                from pg_catalog.pg_foreign_table ft
                     left outer join pg_catalog.pg_foreign_server srv on ft.ftserver = srv.oid
                     join pg_catalog.pg_class cls on ft.ftrelid = cls.oid
                where cls.relnamespace = $1::oid
                order by table_id""", nsOid);
    }

    // =========================================================================
    // Q39: schema_arg_types CTE
    // =========================================================================

    @Test @Order(39)
    void q39_schema_arg_types() {
        tryExtendedQuery("Q39: schema_arg_types CTE",
                """
                with schema_procs as (select prorettype, proargtypes, proallargtypes
                                      from pg_catalog.pg_proc
                                      where pronamespace = $1::oid),
                     schema_opers as (select oprleft, oprright, oprresult
                                      from pg_catalog.pg_operator
                                      where oprnamespace = $2::oid),
                     schema_aggregates as (select A.aggtranstype , A.aggmtranstype
                                           from pg_catalog.pg_aggregate A
                                           join pg_catalog.pg_proc P
                                             on A.aggfnoid = P.oid
                                           where P.pronamespace = $3::oid),
                     schema_arg_types as ( select prorettype as type_id
                                           from schema_procs
                                           union
                                           select distinct unnest(proargtypes) as type_id
                                           from schema_procs
                                           union
                                           select distinct unnest(proallargtypes) as type_id
                                           from schema_procs
                                           union
                                           select oprleft as type_id
                                           from schema_opers
                                           where oprleft is not null
                                           union
                                           select oprright as type_id
                                           from schema_opers
                                           where oprright is not null
                                           union
                                           select oprresult as type_id
                                           from schema_opers
                                           where oprresult is not null
                                           union
                                           select aggtranstype::oid as type_id
                                           from schema_aggregates
                                           union
                                           select aggmtranstype::oid as type_id
                                           from schema_aggregates
                                           )
                select type_id, pg_catalog.format_type(type_id, null) as type_spec
                from schema_arg_types
                where type_id <> 0""",
                nsOid, nsOid, nsOid);
    }

    // =========================================================================
    // Q40: routines (pg_proc natural join pg_language)
    // =========================================================================

    @Test @Order(40)
    void q40_routines() {
        tryExtendedQuery("Q40: routines natural join languages",
                """
                with languages as (select oid as lang_oid, lanname as lang
                                   from pg_catalog.pg_language),
                     routines as (select proname as r_name,
                                         prolang as lang_oid,
                                         oid as r_id,
                                         xmin as r_state_number,
                                         proargnames as arg_names,
                                         proargmodes as arg_modes,
                                         proargtypes::int[] as in_arg_types,
                                         proallargtypes::int[] as all_arg_types,
                                         pg_catalog.pg_get_expr(proargdefaults, 0) as arg_defaults,
                                         provariadic as arg_variadic_id,
                                         prorettype as ret_type_id,
                                         proretset as ret_set,
                                         prokind as kind,
                                         provolatile as volatile_kind,
                                         proisstrict as is_strict,
                                         prosecdef as is_security_definer,
                                         proconfig as configuration_parameters,
                                         procost as cost,
                                         pg_catalog.pg_get_userbyid(proowner) as "owner",
                                         prorows as rows,
                                         proleakproof as is_leakproof,
                                         proparallel as concurrency_kind
                                  from pg_catalog.pg_proc
                                  where pronamespace = $1::oid
                                    and not (prokind = 'a'))
                select *
                from routines natural join languages""", nsOid);
    }

    // =========================================================================
    // Q41: pg_aggregate
    // =========================================================================

    @Test @Order(41)
    void q41_pg_aggregate() {
        tryExtendedQuery("Q41: pg_aggregate",
                """
                select P.oid as aggregate_id,
                       P.xmin as state_number,
                       P.proname as aggregate_name,
                       P.proargnames as arg_names,
                       P.proargmodes as arg_modes,
                       P.proargtypes::int[] as in_arg_types,
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
                       A.aggmtranstype::oid as moving_state_type,
                       A.aggmtransspace as moving_state_size,
                       A.aggmfinalfn::oid as moving_final_id,
                       case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name,
                       A.aggmfinalextra as moving_final_extra,
                       A.aggminitval as moving_initial_value,
                       A.aggminvtransfn::oid as inverse_transition_id,
                       case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name,
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
                     join pg_catalog.pg_proc P
                       on A.aggfnoid = P.oid
                where P.pronamespace = $1::oid
                order by P.oid""", nsOid);
    }

    // =========================================================================
    // Q42: pg_operator
    // =========================================================================

    @Test @Order(42)
    void q42_pg_operator() {
        tryExtendedQuery("Q42: pg_operator",
                """
                select O.oid as op_id,
                       O.xmin as state_number,
                       oprname as op_name,
                       oprkind as op_kind,
                       oprleft as arg_left_type_id,
                       oprright as arg_right_type_id,
                       oprresult as arg_result_type_id,
                       oprcode::oid as main_id,
                       oprcode::varchar as main_name,
                       oprrest::oid as restrict_id,
                       oprrest::varchar as restrict_name,
                       oprjoin::oid as join_id,
                       oprjoin::varchar as join_name,
                       oprcom::oid as com_id,
                       oprcom::regoper::varchar as com_name,
                       oprnegate::oid as neg_id,
                       oprnegate::regoper::varchar as neg_name,
                       oprcanmerge as merges,
                       oprcanhash as hashes,
                       pg_catalog.pg_get_userbyid(O.oprowner) as "owner"
                from pg_catalog.pg_operator O
                where oprnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q43: pg_collation
    // =========================================================================

    @Test @Order(43)
    void q43_pg_collation() {
        tryExtendedQuery("Q43: pg_collation",
                """
                select oid as id,
                       xmin as state_number,
                       collname as name,
                       collcollate as lc_collate,
                       collctype as lc_ctype,
                       pg_catalog.pg_get_userbyid(collowner) as "owner"
                from pg_catalog.pg_collation
                where collnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q44: pg_opclass
    // =========================================================================

    @Test @Order(44)
    void q44_pg_opclass() {
        tryExtendedQuery("Q44: pg_opclass",
                """
                select O.oid as id,
                       O.xmin as state_number,
                       opcname as name,
                       opcintype::regtype::varchar as in_type,
                       case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type,
                       opcdefault as is_default,
                       opcfamily as family_id,
                       opfname as family,
                       opcmethod as access_method_id,
                       pg_catalog.pg_get_userbyid(O.opcowner) as "owner"
                from pg_catalog.pg_opclass O
                     join pg_catalog.pg_opfamily F on F.oid = opcfamily
                where opcnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q45: pg_opfamily
    // =========================================================================

    @Test @Order(45)
    void q45_pg_opfamily() {
        tryExtendedQuery("Q45: pg_opfamily",
                """
                select O.oid as id,
                       O.xmin as state_number,
                       opfname as name,
                       opfmethod as access_method_id,
                       pg_catalog.pg_get_userbyid(O.opfowner) as "owner"
                from pg_catalog.pg_opfamily O
                where opfnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q46: pg_amop oids
    // =========================================================================

    @Test @Order(46)
    void q46_pg_amop_oids() {
        tryExtendedQuery("Q46: pg_amop oids",
                """
                select pg_amop.oid
                from pg_catalog.pg_amop
                         join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily
                where opfnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q47: pg_amop details
    // =========================================================================

    @Test @Order(47)
    void q47_pg_amop_details() {
        tryExtendedQuery("Q47: pg_amop details",
                """
                select O.oid as id,
                       O.amopstrategy as strategy,
                       O.amopopr as op_id,
                       O.amopopr::regoperator::varchar as op_sig,
                       O.amopsortfamily as sort_family_id,
                       SF.opfname as sort_family,
                       O.amopfamily as family_id,
                       C.oid as class_id
                from pg_catalog.pg_amop O
                     left join pg_opfamily F on O.amopfamily = F.oid
                     left join pg_opfamily SF on O.amopsortfamily = SF.oid
                     left join pg_depend D on D.classid = 'pg_amop'::regclass and O.oid = D.objid and D.objsubid = 0
                     left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0
                where C.opcnamespace = $1::oid or C.opcnamespace is null and F.opfnamespace = $2::oid
                order by C.oid, F.oid""", nsOid, nsOid);
    }

    // =========================================================================
    // Q48: pg_amproc oids
    // =========================================================================

    @Test @Order(48)
    void q48_pg_amproc_oids() {
        tryExtendedQuery("Q48: pg_amproc oids",
                """
                select pg_amproc.oid
                from pg_catalog.pg_amproc
                         join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily
                where opfnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q49: pg_amproc details
    // =========================================================================

    @Test @Order(49)
    void q49_pg_amproc_details() {
        tryExtendedQuery("Q49: pg_amproc details",
                """
                select P.oid as id,
                       P.amprocnum as num,
                       P.amproc::oid as proc_id,
                       P.amproc::regprocedure::varchar as proc_sig,
                       P.amproclefttype::regtype::varchar as left_type,
                       P.amprocrighttype::regtype::varchar as right_type,
                       P.amprocfamily as family_id,
                       C.oid as class_id
                from pg_catalog.pg_amproc P
                     left join pg_opfamily F on P.amprocfamily = F.oid
                     left join pg_depend D on D.classid = 'pg_amproc'::regclass and P.oid = D.objid and D.objsubid = 0
                     left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0
                where C.opcnamespace = $1::oid or C.opcnamespace is null and F.opfnamespace = $2::oid
                order by C.oid, F.oid""", nsOid, nsOid);
    }

    // =========================================================================
    // Q50: pg_index indexrelids
    // =========================================================================

    @Test @Order(50)
    void q50_pg_index_oids() {
        tryExtendedQuery("Q50: pg_index indexrelids",
                """
                select IX.indexrelid
                from pg_catalog.pg_index IX,
                     pg_catalog.pg_class IC
                where IC.oid = IX.indrelid
                  and IC.relnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q51: index details
    // =========================================================================

    @Test @Order(51)
    void q51_index_details() {
        tryExtendedQuery("Q51: index details",
                """
                select tab.oid               table_id,
                       tab.relkind           table_kind,
                       ind_stor.relname      index_name,
                       ind_head.indexrelid   index_id,
                       ind_stor.xmin         state_number,
                       ind_head.indisunique  is_unique,
                       ind_head.indisprimary is_primary,
                       ind_head.indnullsnotdistinct nulls_not_distinct,
                       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,
                       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,
                       ind_stor.reltablespace tablespace_id,
                       opcmethod as access_method_id
                from pg_catalog.pg_class tab
                         join pg_catalog.pg_index ind_head
                              on ind_head.indrelid = tab.oid
                         join pg_catalog.pg_class ind_stor
                              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
                         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
                where tab.relnamespace = $1::oid
                        and tab.relkind in ('r', 'm', 'v', 'p')
                        and ind_stor.relkind in ('i', 'I')""", nsOid);
    }

    // =========================================================================
    // Q52: index column details (complex cross join + unnest)
    // =========================================================================

    @Test @Order(52)
    void q52_index_columns() {
        tryExtendedQuery("Q52: index column details",
                """
                select ind_head.indexrelid index_id,
                       k col_idx,
                       k <= indnkeyatts in_key,
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
                         join pg_catalog.pg_class ind_stor
                              on ind_stor.oid = ind_head.indexrelid
                cross join unnest(ind_head.indkey) with ordinality u(u, k)
                left join pg_catalog.pg_collation
                on pg_collation.oid = ind_head.indcollation[k-1]
                left join pg_catalog.pg_namespace colln on collnamespace = colln.oid
                cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
                         left join pg_catalog.pg_opclass
                                   on pg_opclass.oid = ind_head.indclass[k-1]
                         left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid
                where ind_stor.relnamespace = $1::oid
                  and ind_stor.relkind in ('i', 'I')
                order by index_id, k""", nsOid);
    }

    // =========================================================================
    // Q53: pg_constraint oids
    // =========================================================================

    @Test @Order(53)
    void q53_pg_constraint_oids() {
        tryExtendedQuery("Q53: pg_constraint oids",
                """
                select oid
                from pg_catalog.pg_constraint
                where conrelid != 0 and connamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q54: pg_constraint details
    // =========================================================================

    @Test @Order(54)
    void q54_pg_constraint_details() {
        tryExtendedQuery("Q54: pg_constraint details",
                """
                select T.oid table_id,
                       relkind table_kind,
                       C.oid::bigint con_id,
                       C.xmin::varchar::bigint con_state_id,
                       conname con_name,
                       contype con_kind,
                       conkey con_columns,
                       conindid index_id,
                       confrelid ref_table_id,
                       condeferrable is_deferrable,
                       condeferred is_init_deferred,
                       confupdtype on_update,
                       confdeltype on_delete,
                       connoinherit no_inherit,
                       pg_catalog.pg_get_expr(conbin, T.oid) con_expression,
                       confkey ref_columns,
                       conexclop::int[] excl_operators,
                       array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str
                from pg_catalog.pg_constraint C
                         join pg_catalog.pg_class T
                              on C.conrelid = T.oid
                where relkind in ('r', 'v', 'f', 'p')
                  and relnamespace = $1::oid
                  and contype in ('p', 'u', 'f', 'c', 'x')
                  and connamespace = $2::oid""", nsOid, nsOid);
    }

    // =========================================================================
    // Q55: pg_rewrite oids
    // =========================================================================

    @Test @Order(55)
    void q55_pg_rewrite_oids() {
        tryExtendedQuery("Q55: pg_rewrite oids",
                """
                select RU.oid
                from pg_catalog.pg_rewrite RU,
                     pg_catalog.pg_class RC
                where RC.oid = RU.ev_class
                  and RC.relnamespace = $1::oid
                  and not RU.rulename = '_RETURN'""", nsOid);
    }

    // =========================================================================
    // Q56: pg_policy oids
    // =========================================================================

    @Test @Order(56)
    void q56_pg_policy_oids() {
        tryExtendedQuery("Q56: pg_policy oids",
                """
                select P.oid
                from pg_catalog.pg_policy P
                     join pg_catalog.pg_class C on polrelid = C.oid
                where relnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q57: pg_policy details
    // =========================================================================

    @Test @Order(57)
    void q57_pg_policy_details() {
        tryExtendedQuery("Q57: pg_policy details",
                """
                select
                       P.oid id,
                       P.xmin as state_number,
                       polname policyname,
                       polrelid table_id,
                       polpermissive as permissive,
                       polroles roles,
                       polcmd cmd,
                       pg_get_expr(polqual, polrelid) qual,
                       pg_get_expr(polwithcheck, polrelid) with_check
                from pg_catalog.pg_policy P
                       join pg_catalog.pg_class C on polrelid = C.oid
                where relnamespace = $1::oid
                order by polrelid""", nsOid);
    }

    // =========================================================================
    // Q58: pg_trigger oids
    // =========================================================================

    @Test @Order(58)
    void q58_pg_trigger_oids() {
        tryExtendedQuery("Q58: pg_trigger oids",
                """
                select TG.oid
                from pg_catalog.pg_trigger TG,
                     pg_catalog.pg_class TC
                where TC.oid = TG.tgrelid
                  and TC.relnamespace = $1::oid""", nsOid);
    }

    // =========================================================================
    // Q59: pg_description union (11 params)
    // =========================================================================

    @Test @Order(59)
    void q59_pg_description_union_sub_ids() {
        tryExtendedQuery("Q59: pg_description union sub_ids",
                """
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
                where C.relnamespace = $11::oid  and D.classoid = 'pg_catalog.pg_policy'::regclass""",
                nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);
    }

    // =========================================================================
    // Q60: pg_description union with descriptions (11 params)
    // =========================================================================

    @Test @Order(60)
    void q60_pg_description_union_descriptions() {
        tryExtendedQuery("Q60: pg_description union descriptions",
                """
                select D.objoid id, C.relkind::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_class C on D.objoid = C.oid
                where C.relnamespace = $1::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass
                union all
                select T.oid id, 'T'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid
                  left join pg_catalog.pg_class C on T.typrelid = C.oid
                where T.typnamespace = $2::oid and (C.relkind = 'c' or C.relkind is null)
                union all
                select D.objoid id, pg_catalog.translate(C.contype, 'pufc', 'kkxz')::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_constraint C on D.objoid = C.oid
                where C.connamespace = $3::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass
                union all
                select D.objoid id, 't'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_trigger T on T.oid = D.objoid
                  join pg_catalog.pg_class C on C.oid = T.tgrelid
                where C.relnamespace = $4::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass
                union all
                select D.objoid id, 'R'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_rewrite R on R.oid = D.objoid
                  join pg_catalog.pg_class C on C.oid = R.ev_class
                where C.relnamespace = $5::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass
                union all
                select D.objoid id, 'F'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_proc P on P.oid = D.objoid
                where P.pronamespace = $6::oid and D.classoid = 'pg_catalog.pg_proc'::regclass
                union all
                select D.objoid id, 'O'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_operator O on O.oid = D.objoid
                where O.oprnamespace = $7::oid and D.classoid = 'pg_catalog.pg_operator'::regclass
                union all
                select D.objoid id, 'f'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_opfamily O on O.oid = D.objoid
                where O.opfnamespace = $8::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass
                union all
                select D.objoid id, 'c'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_opclass O on O.oid = D.objoid
                where O.opcnamespace = $9::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass
                union all
                select D.objoid id, 'C'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                  join pg_catalog.pg_collation C on C.oid = D.objoid
                where C.collnamespace = $10::oid and D.classoid = 'pg_catalog.pg_collation'::regclass
                union all
                select D.objoid id, 'P'::char as kind, D.objsubid sub_id, D.description
                from pg_catalog.pg_description D
                       join pg_catalog.pg_policy P on P.oid = D.objoid
                       join pg_catalog.pg_class C on P.polrelid = C.oid
                where C.relnamespace = $11::oid and D.classoid = 'pg_catalog.pg_policy'::regclass""",
                nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);
    }

    // =========================================================================
    // Q61: acl class/proc/type (6 params with xid)
    // =========================================================================

    @Test @Order(61)
    void q61_acl_class_proc_type() {
        // This query uses $N::varchar::xid - we pass nsOid for oid params and 1 for xid params
        // But we need to use setObject for the xid params differently
        // Actually: all params are passed as longs, the ::varchar::xid cast is in the SQL
        tryExtendedQuery("Q61: acl class+proc+type",
                """
                select T.oid as object_id,
                       T.relacl as acl
                from pg_catalog.pg_class T
                where relnamespace = $1::oid
                  and pg_catalog.age(T.xmin) <= pg_catalog.age($2::varchar::xid)
                union all
                select T.oid as object_id,
                       T.proacl as acl
                from pg_catalog.pg_proc T
                where pronamespace = $3::oid
                  and pg_catalog.age(T.xmin) <= pg_catalog.age($4::varchar::xid)
                union all
                select T.oid as object_id,
                       T.typacl as acl
                from pg_catalog.pg_type T
                where typnamespace = $5::oid
                  and pg_catalog.age(T.xmin) <= pg_catalog.age($6::varchar::xid)
                order by object_id""",
                nsOid, 1, nsOid, 1, nsOid, 1);
    }

    // =========================================================================
    // Q62: pg_attribute acl (2 params with xid)
    // =========================================================================

    @Test @Order(62)
    void q62_pg_attribute_acl() {
        tryExtendedQuery("Q62: pg_attribute acl",
                """
                select T.oid as object_id,
                       A.attnum as attr_position,
                       A.attacl as acl
                from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid
                where relnamespace = $1::oid
                  and attnum > 0
                  and pg_catalog.age(A.xmin) <= pg_catalog.age($2::varchar::xid)
                order by object_id, attr_position""",
                nsOid, 1);
    }

    // =========================================================================
    // Q63: pg_depend sequence->table
    // =========================================================================

    @Test @Order(63)
    void q63_pg_depend_seq_table() {
        tryExtendedQuery("Q63: pg_depend sequence->table",
                """
                select D.objid as dependent_id,
                       D.refobjid as owner_id,
                       D.refobjsubid as owner_subobject_id
                from pg_depend D
                  join pg_class C_SEQ on D.objid    = C_SEQ.oid and D.classid    = 'pg_class'::regclass::oid
                  join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid
                where C_SEQ.relkind = 'S'
                  and C_TAB.relkind = 'r'
                  and D.refobjsubid <> 0
                  and (D.deptype = 'a' or D.deptype = 'i')
                  and C_TAB.relnamespace = $1::oid
                order by owner_id""", nsOid);
    }

    // =========================================================================
    // Q64: pg_get_viewdef (3 params with xid)
    // =========================================================================

    @Test @Order(64)
    void q64_pg_get_viewdef() {
        tryExtendedQuery("Q64: pg_get_viewdef",
                """
                select T.relkind as view_kind,
                       T.oid as view_id,
                       pg_catalog.pg_get_viewdef(T.oid, true) as source_text
                from pg_catalog.pg_class T
                     join pg_catalog.pg_namespace N on T.relnamespace = N.oid
                where N.oid = $1::oid
                  and T.relkind in ('m','v')
                  and (pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647) or exists(
                  select A.attrelid from pg_catalog.pg_attribute A where A.attrelid = T.oid and pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($3::varchar::xid), -1), -1), 2147483647)))""",
                nsOid, 1, 1);
    }

    // =========================================================================
    // Q65: pg_proc source text (system_languages CTE)
    // =========================================================================

    @Test @Order(65)
    void q65_pg_proc_source() {
        tryExtendedQuery("Q65: pg_proc source text",
                """
                with system_languages as ( select oid as lang
                                           from pg_catalog.pg_language
                                           where lanname in ('c','internal') )
                select oid as id,
                       pg_catalog.pg_get_function_arguments(oid) as arguments_def,
                       pg_catalog.pg_get_function_result(oid) as result_def,
                       pg_catalog.pg_get_function_sqlbody(oid) as sqlbody_def,
                       prosrc as source_text
                from pg_catalog.pg_proc
                where pronamespace = $1::oid
                  and not (prokind = 'a')
                  and prolang not in (select lang from system_languages)
                  and prosrc is not null""", nsOid);
    }

    // =========================================================================
    // Q66: SHOW TRANSACTION ISOLATION LEVEL
    // =========================================================================

    @Test @Order(66)
    void q66_show_transaction_isolation_level() {
        tryExtendedShow("Q66: SHOW TRANSACTION ISOLATION LEVEL",
                "SHOW TRANSACTION ISOLATION LEVEL");
    }
}
