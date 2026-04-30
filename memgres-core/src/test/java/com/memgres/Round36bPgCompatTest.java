package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 36b: Fix remaining PG 18 behavioral gaps.
 *
 * Part A: Bug fixes (Memgres producing wrong results)
 * Part B: PG 18 compat rejections (Memgres accepts syntax/functions PG 18 doesn't)
 */
class Round36bPgCompatTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarStr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // Part A: Bug fixes
    // =========================================================================

    @Test
    void pgOpfamily_ginIntegerOps_shouldNotExist() throws SQLException {
        // PG 18 does not have integer_ops for GIN access method
        exec("CREATE EXTENSION IF NOT EXISTS btree_gin");
        int count = scalarInt(
                "SELECT count(*)::int FROM pg_opfamily f JOIN pg_am a ON a.oid=f.opfmethod "
                        + "WHERE a.amname='gin' AND f.opfname='integer_ops'");
        assertEquals(0, count, "GIN integer_ops should not exist in pg_opfamily");
    }

    @Test
    void pgOpfamily_gistIntegerOps_shouldNotExist() throws SQLException {
        // PG 18 does not have integer_ops for GiST access method
        exec("CREATE EXTENSION IF NOT EXISTS btree_gist");
        int count = scalarInt(
                "SELECT count(*)::int FROM pg_opfamily f JOIN pg_am a ON a.oid=f.opfmethod "
                        + "WHERE a.amname='gist' AND f.opfname='integer_ops'");
        assertEquals(0, count, "GiST integer_ops should not exist in pg_opfamily");
    }

    @Test
    void rangeAgg_filter_returnsNonNull() throws SQLException {
        // Use exact data from comparison report
        exec("CREATE TABLE r36b_ra (r int4range, incl boolean)");
        exec("INSERT INTO r36b_ra VALUES ('[1,5)', true), ('[10,20)', true), ('[30,40)', false)");
        String result = scalarStr(
                "SELECT (range_agg(r) FILTER (WHERE incl)::text LIKE '%[1,5)%')::text AS ok FROM r36b_ra");
        assertEquals("true", result, "range_agg with FILTER should find [1,5) in result");
        exec("DROP TABLE r36b_ra");
    }

    @Test
    void stringAgg_distinct_orderBy_returnsResult() throws SQLException {
        exec("CREATE TABLE r36b_sagg (v numeric)");
        exec("INSERT INTO r36b_sagg VALUES (1), (1.0), (2)");
        String result = scalarStr(
                "SELECT (string_agg(DISTINCT v::text, ',' ORDER BY v::text) LIKE '%2%')::text FROM r36b_sagg");
        assertEquals("true", result, "string_agg DISTINCT ORDER BY should contain '2'");
        exec("DROP TABLE r36b_sagg");
    }

    @Test
    void ctid_uniquePerRow() throws SQLException {
        // Use schema setup matching comparison report
        exec("DROP SCHEMA IF EXISTS r36b_sc CASCADE");
        exec("CREATE SCHEMA r36b_sc");
        exec("SET search_path = r36b_sc, public");
        exec("CREATE TABLE r36b_ctid (id int)");
        exec("INSERT INTO r36b_ctid VALUES (1),(2),(3)");
        int distinctCount = scalarInt(
                "SELECT count(DISTINCT ctid)::int FROM r36b_ctid");
        assertEquals(3, distinctCount, "Each row should have a distinct ctid");
        exec("SET search_path = public");
        exec("DROP SCHEMA r36b_sc CASCADE");
    }

    @Test
    void fkCascade_setNull_blockedByPublication() throws SQLException {
        // When a child table is published (via FOR ALL TABLES) and has no replica identity,
        // DELETE on parent with ON DELETE SET NULL should fail with 55000
        exec("CREATE TABLE r36b_fkp (a int PRIMARY KEY)");
        exec("CREATE TABLE r36b_fkc (a int REFERENCES r36b_fkp(a) ON DELETE SET NULL)");
        exec("INSERT INTO r36b_fkp VALUES (1)");
        exec("INSERT INTO r36b_fkc VALUES (1)");
        exec("CREATE PUBLICATION r36b_pub FOR ALL TABLES");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DELETE FROM r36b_fkp WHERE a=1"));
            assertEquals("55000", ex.getSQLState(),
                    "FK cascade SET NULL on published table without replica identity should fail");
        } finally {
            exec("DROP PUBLICATION IF EXISTS r36b_pub");
            exec("DROP TABLE IF EXISTS r36b_fkc");
            exec("DROP TABLE IF EXISTS r36b_fkp");
        }
    }

    // =========================================================================
    // Part B: PG 18 compatibility rejections
    // =========================================================================

    // -- IS JSON subtypes (PG 18 only supports VALUE/OBJECT/ARRAY/SCALAR) --

    @Test
    void isJsonNumber_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT ('42' IS JSON NUMBER)::text"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test
    void isJsonString_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT ('\"abc\"' IS JSON STRING)::text"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test
    void isJsonBoolean_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT ('true' IS JSON BOOLEAN)::text"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test
    void isJsonNull_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT ('null' IS JSON NULL)::text"));
        assertEquals("42601", ex.getSQLState());
    }

    // -- IGNORE NULLS on window functions (not supported in PG 18) --

    @Test
    void ignoreNulls_lag_rejected() throws SQLException {
        exec("CREATE TABLE r36b_wf (id int, v int)");
        exec("INSERT INTO r36b_wf VALUES (1, 10), (2, NULL), (3, 30)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalarStr("SELECT lag(v, 1) IGNORE NULLS OVER (ORDER BY id)::text FROM r36b_wf LIMIT 1"));
            assertEquals("42601", ex.getSQLState());
        } finally {
            exec("DROP TABLE r36b_wf");
        }
    }

    @Test
    void ignoreNulls_lead_rejected() throws SQLException {
        exec("CREATE TABLE r36b_wf2 (id int, v int)");
        exec("INSERT INTO r36b_wf2 VALUES (1, 10), (2, NULL), (3, 30)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalarStr("SELECT lead(v, 1) IGNORE NULLS OVER (ORDER BY id)::text FROM r36b_wf2 LIMIT 1"));
            assertEquals("42601", ex.getSQLState());
        } finally {
            exec("DROP TABLE r36b_wf2");
        }
    }

    @Test
    void ignoreNulls_firstValue_rejected() throws SQLException {
        exec("CREATE TABLE r36b_wf3 (id int, v int)");
        exec("INSERT INTO r36b_wf3 VALUES (1, NULL), (2, 20)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalarStr("SELECT first_value(v) IGNORE NULLS OVER (ORDER BY id)::text FROM r36b_wf3 LIMIT 1"));
            assertEquals("42601", ex.getSQLState());
        } finally {
            exec("DROP TABLE r36b_wf3");
        }
    }

    @Test
    void ignoreNulls_nthValue_rejected() throws SQLException {
        exec("CREATE TABLE r36b_wf4 (id int, v int)");
        exec("INSERT INTO r36b_wf4 VALUES (1, 10), (2, NULL), (3, 30)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalarStr("SELECT nth_value(v, 2) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text FROM r36b_wf4 LIMIT 1"));
            assertEquals("42601", ex.getSQLState());
        } finally {
            exec("DROP TABLE r36b_wf4");
        }
    }

    // -- FROM FIRST / FROM LAST (not supported in PG 18) --

    @Test
    void fromFirst_rejected() throws SQLException {
        exec("CREATE TABLE r36b_ff (id int, v int)");
        exec("INSERT INTO r36b_ff VALUES (1, 10), (2, 20)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalarStr("SELECT nth_value(v, 2) FROM FIRST OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text FROM r36b_ff LIMIT 1"));
            assertEquals("42601", ex.getSQLState());
        } finally {
            exec("DROP TABLE r36b_ff");
        }
    }

    @Test
    void fromLast_rejected() throws SQLException {
        exec("CREATE TABLE r36b_fl (id int, v int)");
        exec("INSERT INTO r36b_fl VALUES (1, 10), (2, 20)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalarStr("SELECT nth_value(v, 2) FROM LAST OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text FROM r36b_fl LIMIT 1"));
            assertEquals("42601", ex.getSQLState());
        } finally {
            exec("DROP TABLE r36b_fl");
        }
    }

    // -- UNION CORRESPONDING (not supported in PG 18) --

    @Test
    void unionCorresponding_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT count(*)::text FROM (SELECT 1 AS a, 2 AS b UNION CORRESPONDING SELECT 3 AS b, 4 AS a) q"));
        assertEquals("42601", ex.getSQLState());
    }

    // -- Mutual recursive CTEs (not supported in PG 18) --

    @Test
    void mutualRecursiveCte_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("WITH RECURSIVE a(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM b WHERE n < 3), "
                        + "b(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM a WHERE n < 3) "
                        + "SELECT (count(*) > 0)::text FROM a"));
        assertEquals("0A000", ex.getSQLState());
    }

    // -- json LIKE operator (not supported in PG 18) --

    @Test
    void jsonLike_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT (row_to_json(x, true) LIKE E'%\\n%') AS ok FROM (SELECT 1 AS a) x"));
        assertEquals("42883", ex.getSQLState());
    }

    // -- array_sample 3-arg (not supported in PG 18) --

    @Test
    void arraySample_3arg_rejected() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarStr("SELECT array_sample(ARRAY[1,2,3,4,5], 3, 42)::text"));
        assertEquals("42883", ex.getSQLState());
    }
}
