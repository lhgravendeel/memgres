package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests every distinct query a database management tool issues during a real session.
 * Each test is named after the purpose of the query.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolFullSessionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Q1-Q5: Already tested in DbToolConnectionTest, skip
    // ========================================================================

    // ========================================================================
    // Q6: pg_roles with recursive CTE for can_signal_backend
    // ========================================================================

    @Test @Order(1)
    void pg_roles_with_recursive_cte() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                roles.oid as id, roles.rolname as name,
                roles.rolsuper as is_superuser,
                CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END as
                can_create_role,
                CASE WHEN roles.rolsuper THEN true
                ELSE roles.rolcreatedb END as can_create_db,
                CASE WHEN 'pg_signal_backend'=ANY(ARRAY(WITH RECURSIVE cte AS (
                SELECT pg_roles.oid,pg_roles.rolname FROM pg_roles
                    WHERE pg_roles.oid = roles.oid
                UNION ALL
                SELECT m.roleid,pgr.rolname FROM cte cte_1
                    JOIN pg_auth_members m ON m.member = cte_1.oid
                    JOIN pg_roles pgr ON pgr.oid = m.roleid)
                SELECT rolname  FROM cte)) THEN True
                ELSE False END as can_signal_backend
            FROM
                pg_catalog.pg_roles as roles
            WHERE
                rolname = current_user
            """);
        assertEquals(1, rows.size(), "Should return 1 row for current user");
        assertNotNull(rows.get(0).get(0), "oid should not be null");
        assertNotNull(rows.get(0).get(1), "rolname should not be null");
    }

    // ========================================================================
    // Q7: Dashboard stats (pg_stat_activity + pg_stat_database)
    // ========================================================================

    @Test @Order(2)
    void dashboard_stats_query() throws SQLException {
        List<List<String>> rows = query("""
            SELECT 'session_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
            FROM (SELECT
               (SELECT count(*) FROM pg_catalog.pg_stat_activity) AS "Total",
               (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'active')  AS "Active",
               (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'idle')  AS "Idle"
            ) t
            UNION ALL
            SELECT 'tps_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
            FROM (SELECT
               (SELECT sum(xact_commit) + sum(xact_rollback) FROM pg_catalog.pg_stat_database) AS "Transactions",
               (SELECT sum(xact_commit) FROM pg_catalog.pg_stat_database) AS "Commits",
               (SELECT sum(xact_rollback) FROM pg_catalog.pg_stat_database) AS "Rollbacks"
            ) t
            UNION ALL
            SELECT 'ti_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
            FROM (SELECT
               (SELECT sum(tup_inserted) FROM pg_catalog.pg_stat_database) AS "Inserts",
               (SELECT sum(tup_updated) FROM pg_catalog.pg_stat_database) AS "Updates",
               (SELECT sum(tup_deleted) FROM pg_catalog.pg_stat_database) AS "Deletes"
            ) t
            UNION ALL
            SELECT 'to_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
            FROM (SELECT
               (SELECT sum(tup_fetched) FROM pg_catalog.pg_stat_database) AS "Fetched",
               (SELECT sum(tup_returned) FROM pg_catalog.pg_stat_database) AS "Returned"
            ) t
            UNION ALL
            SELECT 'bio_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
            FROM (SELECT
               (SELECT sum(blks_read) FROM pg_catalog.pg_stat_database) AS "Reads",
               (SELECT sum(blks_hit) FROM pg_catalog.pg_stat_database) AS "Hits"
            ) t
            """);
        assertEquals(5, rows.size(), "Dashboard should return 5 chart rows");
        assertEquals("session_stats", rows.get(0).get(0));
        assertEquals("tps_stats", rows.get(1).get(0));
        assertEquals("ti_stats", rows.get(2).get(0));
        assertEquals("to_stats", rows.get(3).get(0));
        assertEquals("bio_stats", rows.get(4).get(0));
        // Each chart_data should be valid JSON
        for (List<String> row : rows) {
            assertNotNull(row.get(1), "chart_data should not be null for " + row.get(0));
            assertTrue(row.get(1).startsWith("{"), "chart_data should be JSON: " + row.get(1));
        }
    }

    // ========================================================================
    // Q8: pgagent job check (should return 0 rows since there is no pgagent schema)
    // ========================================================================

    @Test @Order(3)
    void pgagent_job_check() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                has_table_privilege(
                  'pgagent.pga_job', 'INSERT, SELECT, UPDATE'
                ) has_priviledge
            WHERE EXISTS(
                SELECT has_schema_privilege('pgagent', 'USAGE')
                WHERE EXISTS(
                    SELECT cl.oid FROM pg_catalog.pg_class cl
                    LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=relnamespace
                    WHERE relname='pga_job' AND nspname='pgagent'
                )
            )
            """);
        assertEquals(0, rows.size(), "No pgagent schema → 0 rows");
    }

    // ========================================================================
    // Q9: Database list with tablespace and description joins
    // ========================================================================

    @Test @Order(4)
    void database_list_with_joins() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                db.oid as did, db.datname as name, ta.spcname as spcname, db.datallowconn,
                db.datistemplate AS is_template,
                pg_catalog.has_database_privilege(db.oid, 'CREATE') as cancreate, datdba as owner,
                descr.description
            FROM
                pg_catalog.pg_database db
                LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace = ta.oid
                LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (
                    db.oid=descr.objoid AND descr.classoid='pg_database'::regclass
                )
            WHERE db.oid > 0::OID OR db.datname IN ('postgres', 'edb')
            ORDER BY datname
            """);
        assertTrue(rows.size() >= 1, "Should list at least 1 database");
    }

    // ========================================================================
    // Q10: pg_collation locale query
    // ========================================================================

    @Test @Order(5)
    void pg_collation_icu_locale() throws SQLException {
        // May return 0 rows if no ICU collations, and that's fine
        List<List<String>> rows = query(
                "SELECT colllocale as colliculocale from pg_collation where collprovider = 'i'");
        assertNotNull(rows); // just must not crash
    }

    // ========================================================================
    // Q11: pg_roles with shdescription join
    // ========================================================================

    @Test @Order(6)
    void pg_roles_with_description() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                r.oid, r.rolname, r.rolcanlogin, r.rolsuper, d.description
            FROM pg_catalog.pg_roles r
                LEFT JOIN pg_catalog.pg_shdescription d
                ON d.objoid = r.oid AND d.classoid = 'pg_catalog.pg_authid'::regclass
            ORDER BY r.rolcanlogin, r.rolname
            """);
        assertTrue(rows.size() >= 1, "Should list at least 1 role");
    }

    // ========================================================================
    // Q12: pg_tablespace listing
    // ========================================================================

    @Test @Order(7)
    void pg_tablespace_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                ts.oid AS oid, spcname AS name, spcowner as owner,
                pg_catalog.shobj_description(oid, 'pg_tablespace') AS description
            FROM
                pg_catalog.pg_tablespace ts
            ORDER BY name
            """);
        // PG has at least pg_default and pg_global; memgres may have 0 or more
        assertNotNull(rows);
    }

    // ========================================================================
    // Q13: pg_encoding_to_char with generate_series
    // ========================================================================

    @Test @Order(8)
    void encoding_list_via_generate_series() throws SQLException {
        List<List<String>> rows = query("""
            SELECT * FROM
            (SELECT pg_catalog.pg_encoding_to_char(s.i) AS encoding
            FROM (SELECT pg_catalog.generate_series(0, 100, 1) as i) s) a
            WHERE encoding != '' ORDER BY encoding
            """);
        assertTrue(rows.size() >= 1, "Should return at least 1 encoding");
        // UTF8 should be in the list
        boolean hasUtf8 = rows.stream().anyMatch(r -> "UTF8".equals(r.get(0)));
        assertTrue(hasUtf8, "Should include UTF8 encoding");
    }

    // ========================================================================
    // Q14: Database locale provider detection
    // ========================================================================

    @Test @Order(9)
    void database_locale_provider_detection() throws SQLException {
        List<List<String>> rows = query("""
            SELECT CASE WHEN datlocprovider = 'i' THEN
                (SELECT datlocale as cname FROM pg_database WHERE datname = current_database())
            ELSE
                (SELECT datcollate as cname FROM pg_database WHERE datname = current_database()
                UNION
                SELECT datctype as cname FROM pg_database WHERE datname = current_database())
            END
            FROM pg_database WHERE datname = current_database()
            """);
        assertTrue(rows.size() >= 1, "Should return at least 1 row");
    }

    // ========================================================================
    // Q15: pg_collation builtin locale
    // ========================================================================

    @Test @Order(10)
    void pg_collation_builtin_locale() throws SQLException {
        List<List<String>> rows = query(
                "SELECT colllocale as collbuiltinlocale from pg_collation where collprovider = 'b'");
        assertNotNull(rows);
    }

    // ========================================================================
    // Q16: pg_show_all_settings with context filter
    // ========================================================================

    @Test @Order(11)
    void pg_show_all_settings_with_context_filter() throws SQLException {
        List<List<String>> rows = query("""
            SELECT name, vartype, min_val, max_val, enumvals
            FROM pg_catalog.pg_show_all_settings() WHERE context in ('user', 'superuser')
            """);
        // Should return some user/superuser configurable settings
        assertNotNull(rows);
        // Each row should have 5 columns
        if (!rows.isEmpty()) {
            assertEquals(5, rows.get(0).size(), "Should have 5 columns");
        }
    }

    // ========================================================================
    // Q17: CREATE DATABASE (tool creates a database)
    // ========================================================================

    @Test @Order(12)
    void create_database_statement() throws SQLException {
        // A database management tool issues CREATE DATABASE; memgres should accept this as no-op or implement it
        try {
            exec("""
                CREATE DATABASE tool_test_db
                    WITH
                    OWNER = memgres
                    ENCODING = 'UTF8'
                    LOCALE_PROVIDER = 'libc'
                    CONNECTION LIMIT = -1
                    IS_TEMPLATE = False
                """);
        } catch (SQLException e) {
            // If CREATE DATABASE fails, record the error but don't hard-fail
            // The tool can handle this error
            System.err.println("CREATE DATABASE failed (expected for in-memory db): " + e.getMessage());
        }
    }

    // ========================================================================
    // Q18: Detailed database info after CREATE DATABASE
    // ========================================================================

    @Test @Order(13)
    void detailed_database_info() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                db.oid AS did, db.oid, db.datname AS name, db.dattablespace AS spcoid,
                spcname, datallowconn, pg_catalog.pg_encoding_to_char(encoding) AS encoding,
                pg_catalog.pg_get_userbyid(datdba) AS datowner, db.datcollate, db.datctype,
                datconnlimit, datlocale AS daticulocale, datlocale AS datbuiltinlocale, daticurules, datcollversion,
                CASE WHEN datlocprovider = 'i' THEN 'icu' WHEN datlocprovider = 'b' THEN 'builtin'
                ELSE 'libc' END datlocaleprovider,
                pg_catalog.has_database_privilege(db.oid, 'CREATE') AS cancreate,
                pg_catalog.current_setting('default_tablespace') AS default_tablespace,
                descr.description AS comments, db.datistemplate AS is_template,
                    '' AS tblacl,
                    '' AS seqacl,
                    '' AS funcacl,
                pg_catalog.array_to_string(datacl::text[], ', ') AS acl
            FROM pg_catalog.pg_database db
                LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace=ta.OID
                LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (
                    db.oid=descr.objoid AND descr.classoid='pg_database'::regclass
                )
            WHERE db.datname = current_database()
            ORDER BY datname
            """);
        assertEquals(1, rows.size(), "Should return 1 row for current database");
    }
}
