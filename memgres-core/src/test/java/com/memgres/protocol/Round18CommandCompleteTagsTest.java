package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AE: CommandComplete tag shape.
 *
 * Covers:
 *  - MERGE statement returns "MERGE n" → pgjdbc reports executeUpdate() count
 *  - SELECT INTO returns a row-count via tag
 *  - CREATE TABLE AS with data returns a row-count tag in PG
 *
 * pgjdbc surfaces the CommandComplete count via Statement.getUpdateCount() for
 * DML, and via executeUpdate() return value for MERGE. We assert row-count
 * correctness at that level.
 */
class Round18CommandCompleteTagsTest {

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

    // =========================================================================
    // AE1. MERGE returns affected-row count
    // =========================================================================

    @Test
    void merge_command_returns_affected_row_count() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_mc_src");
        exec("DROP TABLE IF EXISTS r18_mc_tgt");
        exec("CREATE TABLE r18_mc_tgt(id int PRIMARY KEY, v int)");
        exec("CREATE TABLE r18_mc_src(id int PRIMARY KEY, v int)");
        exec("INSERT INTO r18_mc_tgt VALUES (1, 10)");
        exec("INSERT INTO r18_mc_src VALUES (1, 11), (2, 22)");
        try (Statement s = conn.createStatement()) {
            int n = s.executeUpdate(
                    "MERGE INTO r18_mc_tgt t USING r18_mc_src s ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET v = s.v " +
                            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)");
            assertEquals(2, n,
                    "MERGE command tag must report 2 affected rows; got " + n);
        }
    }

    // =========================================================================
    // AE2. SELECT INTO returns row count
    // =========================================================================

    @Test
    void select_into_returns_row_count_via_tag() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_si_src");
        exec("DROP TABLE IF EXISTS r18_si_dst");
        exec("CREATE TABLE r18_si_src(a int)");
        exec("INSERT INTO r18_si_src VALUES (1),(2),(3)");
        try (Statement s = conn.createStatement()) {
            // pgjdbc: SELECT INTO surfaces as executeUpdate() with row count
            int n = s.executeUpdate("SELECT * INTO r18_si_dst FROM r18_si_src");
            assertEquals(3, n,
                    "SELECT INTO must report 3 rows moved; got " + n);
        }
    }

    // =========================================================================
    // AE3. CREATE TABLE AS reports row count
    // =========================================================================

    @Test
    void create_table_as_returns_row_count() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_ct_src");
        exec("DROP TABLE IF EXISTS r18_ct_dst");
        exec("CREATE TABLE r18_ct_src(a int)");
        exec("INSERT INTO r18_ct_src VALUES (1),(2),(3),(4)");
        try (Statement s = conn.createStatement()) {
            int n = s.executeUpdate("CREATE TABLE r18_ct_dst AS SELECT * FROM r18_ct_src");
            assertEquals(4, n,
                    "CREATE TABLE AS must report 4 rows produced; got " + n);
        }
    }
}
