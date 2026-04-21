package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category X: Contrib extensions parallel to hstore/pg_trgm.
 *
 * Covers:
 *  - fuzzystrmatch (levenshtein, soundex, metaphone, dmetaphone)
 *  - unaccent
 *  - intarray operators
 *  - btree_gin / btree_gist
 *  - tablefunc (crosstab / connectby)
 *  - pg_stat_statements
 *  - pg_buffercache
 *  - pgrowlocks
 */
class Round18ContribExtensionsTest {

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

    private static int int1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // X1. fuzzystrmatch
    // =========================================================================

    @Test
    void fuzzystrmatch_levenshtein_works() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch");
        int d = int1("SELECT levenshtein('kitten','sitting')");
        assertEquals(3, d,
                "fuzzystrmatch.levenshtein('kitten','sitting') must be 3; got " + d);
    }

    @Test
    void fuzzystrmatch_soundex_works() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch");
        String s = str("SELECT soundex('Robert')");
        assertEquals("R163", s,
                "fuzzystrmatch.soundex('Robert') must be 'R163'; got '" + s + "'");
    }

    // =========================================================================
    // X2. unaccent
    // =========================================================================

    @Test
    void unaccent_function_works() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS unaccent");
        String s = str("SELECT unaccent('café')");
        assertEquals("cafe", s,
                "unaccent('café') must be 'cafe'; got '" + s + "'");
    }

    // =========================================================================
    // X3. intarray
    // =========================================================================

    @Test
    void intarray_intersection_operator_works() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS intarray");
        // int[] & int[] intersection
        String s = str("SELECT (ARRAY[1,2,3]::int[] & ARRAY[2,3,4]::int[])::text");
        assertEquals("{2,3}", s,
                "intarray int[] & int[] must intersect; got '" + s + "'");
    }

    // =========================================================================
    // X4. btree_gin
    // =========================================================================

    @Test
    void btree_gin_install() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS btree_gin");
        // btree_gin should register integer_ops opfamily for gin.
        int n = int1(
                "SELECT count(*)::int FROM pg_opfamily f " +
                        "JOIN pg_am a ON a.oid=f.opfmethod " +
                        "WHERE a.amname='gin' AND f.opfname='integer_ops'");
        assertTrue(n >= 1,
                "btree_gin must install gin integer_ops opfamily; got " + n);
    }

    // =========================================================================
    // X5. btree_gist
    // =========================================================================

    @Test
    void btree_gist_install() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS btree_gist");
        int n = int1(
                "SELECT count(*)::int FROM pg_opfamily f " +
                        "JOIN pg_am a ON a.oid=f.opfmethod " +
                        "WHERE a.amname='gist' AND f.opfname='integer_ops'");
        assertTrue(n >= 1,
                "btree_gist must install gist integer_ops opfamily; got " + n);
    }

    // =========================================================================
    // X6. tablefunc.crosstab
    // =========================================================================

    @Test
    void tablefunc_crosstab_registered() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS tablefunc");
        int n = int1("SELECT count(*)::int FROM pg_proc WHERE proname='crosstab'");
        assertTrue(n >= 1, "tablefunc.crosstab must be registered; got " + n);
    }

    // =========================================================================
    // X7. pg_stat_statements
    // =========================================================================

    @Test
    void pg_stat_statements_view_queryable() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        // Memgres correctly rejects queries to pg_stat_statements when not loaded
        // via shared_preload_libraries, matching PG 18 behavior (SQLSTATE 55000).
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT count(*) FROM pg_stat_statements");
            }
        });
        assertEquals("55000", ex.getSQLState(),
                "Expected SQLSTATE 55000 but got: " + ex.getSQLState());
    }

    // =========================================================================
    // X8. pg_buffercache
    // =========================================================================

    @Test
    void pg_buffercache_view_queryable() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_buffercache");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM pg_buffercache")) {
            assertTrue(rs.next(),
                    "pg_buffercache view must be queryable after CREATE EXTENSION");
        }
    }

    // =========================================================================
    // X9. pgrowlocks
    // =========================================================================

    @Test
    void pgrowlocks_registered() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgrowlocks");
        int n = int1("SELECT count(*)::int FROM pg_proc WHERE proname='pgrowlocks'");
        assertTrue(n >= 1, "pgrowlocks function must be registered; got " + n);
    }
}
