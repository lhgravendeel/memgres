package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RENAME operations on all object types + ALTER MATERIALIZED VIEW.
 */
class RenameAndAlterMatviewTest {

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

    private static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected row for: " + sql);
            return rs.getString(1);
        }
    }

    private static void expectError(String sql, String expectedSqlState) {
        try {
            exec(sql);
            fail("Expected error for: " + sql);
        } catch (SQLException e) {
            if (expectedSqlState != null) {
                assertEquals(expectedSqlState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + " — " + e.getMessage());
            }
        }
    }

    // =========================================================================
    // A. Table rename
    // =========================================================================

    @Test
    void rename_table() throws SQLException {
        exec("CREATE TABLE t_ren_a (id int PRIMARY KEY, val text)");
        exec("INSERT INTO t_ren_a VALUES (1, 'hello')");
        exec("ALTER TABLE t_ren_a RENAME TO t_ren_a_new");
        assertEquals("hello", query1("SELECT val FROM t_ren_a_new WHERE id = 1"));
        expectError("SELECT * FROM t_ren_a", "42P01");
    }

    // =========================================================================
    // B. Column rename
    // =========================================================================

    @Test
    void rename_column() throws SQLException {
        exec("CREATE TABLE t_ren_b (id int, val text)");
        exec("INSERT INTO t_ren_b VALUES (1, 'world')");
        exec("ALTER TABLE t_ren_b RENAME COLUMN val TO description");
        assertEquals("world", query1("SELECT description FROM t_ren_b WHERE id = 1"));
        expectError("SELECT val FROM t_ren_b", "42703");
    }

    // =========================================================================
    // C. Constraint rename
    // =========================================================================

    @Test
    void rename_constraint() throws SQLException {
        exec("CREATE TABLE t_ren_c (id int, val text, CONSTRAINT t_ren_c_uq UNIQUE (val))");
        exec("ALTER TABLE t_ren_c RENAME CONSTRAINT t_ren_c_uq TO t_ren_c_uq_new");
        assertEquals("t_ren_c_uq_new", query1(
            "SELECT conname FROM pg_constraint WHERE conrelid = 't_ren_c'::regclass AND contype = 'u'"));
    }

    // =========================================================================
    // D. View rename
    // =========================================================================

    @Test
    void rename_view() throws SQLException {
        exec("CREATE TABLE t_ren_d (id int, val text)");
        exec("INSERT INTO t_ren_d VALUES (1, 'x')");
        exec("CREATE VIEW v_ren_d AS SELECT * FROM t_ren_d");
        exec("ALTER VIEW v_ren_d RENAME TO v_ren_d_new");
        assertEquals("x", query1("SELECT val FROM v_ren_d_new"));
        expectError("SELECT * FROM v_ren_d", "42P01");
    }

    // =========================================================================
    // E. Materialized view rename
    // =========================================================================

    @Test
    void rename_materialized_view() throws SQLException {
        exec("CREATE TABLE t_ren_e (id int, val text)");
        exec("INSERT INTO t_ren_e VALUES (1, 'mv')");
        exec("CREATE MATERIALIZED VIEW mv_ren_e AS SELECT * FROM t_ren_e");
        exec("ALTER MATERIALIZED VIEW mv_ren_e RENAME TO mv_ren_e_new");
        assertEquals("mv", query1("SELECT val FROM mv_ren_e_new"));
        expectError("SELECT * FROM mv_ren_e", "42P01");
    }

    @Test
    void rename_materialized_view_if_exists_nonexistent() throws SQLException {
        exec("ALTER MATERIALIZED VIEW IF EXISTS mv_doesnt_exist RENAME TO mv_something");
    }

    @Test
    void rename_materialized_view_nonexistent_errors() {
        expectError("ALTER MATERIALIZED VIEW mv_doesnt_exist RENAME TO mv_something", "42P01");
    }

    @Test
    void refresh_after_rename() throws SQLException {
        exec("CREATE TABLE t_ren_e2 (id int, val text)");
        exec("INSERT INTO t_ren_e2 VALUES (1, 'before')");
        exec("CREATE MATERIALIZED VIEW mv_ren_e2 AS SELECT * FROM t_ren_e2");
        exec("ALTER MATERIALIZED VIEW mv_ren_e2 RENAME TO mv_ren_e2_new");
        exec("INSERT INTO t_ren_e2 VALUES (2, 'after')");
        exec("REFRESH MATERIALIZED VIEW mv_ren_e2_new");
        assertEquals("2", query1("SELECT count(*) FROM mv_ren_e2_new"));
    }

    // =========================================================================
    // F. ALTER MATERIALIZED VIEW — other actions
    // =========================================================================

    @Test
    void alter_matview_owner_to() throws SQLException {
        exec("CREATE ROLE ren_mv_owner2");
        exec("CREATE TABLE t_ren_f (id int)");
        exec("CREATE MATERIALIZED VIEW mv_ren_f AS SELECT * FROM t_ren_f");
        exec("ALTER MATERIALIZED VIEW mv_ren_f OWNER TO ren_mv_owner2");
        assertEquals("ren_mv_owner2", query1(
            "SELECT (SELECT rolname FROM pg_roles WHERE oid = c.relowner) FROM pg_class c WHERE c.relname = 'mv_ren_f'"));
    }

    @Test
    void alter_matview_set_storage_options() throws SQLException {
        exec("CREATE TABLE t_ren_g (id int)");
        exec("INSERT INTO t_ren_g VALUES (1)");
        exec("CREATE MATERIALIZED VIEW mv_ren_g AS SELECT * FROM t_ren_g");
        exec("ALTER MATERIALIZED VIEW mv_ren_g SET (fillfactor = 70)");
        assertEquals("1", query1("SELECT count(*) FROM mv_ren_g"));
    }

    // =========================================================================
    // G. Function rename
    // =========================================================================

    @Test
    void rename_function() throws SQLException {
        exec("CREATE FUNCTION fn_ren_g(x int) RETURNS int LANGUAGE sql AS $$ SELECT x * 2 $$");
        exec("ALTER FUNCTION fn_ren_g(int) RENAME TO fn_ren_g_new");
        assertEquals("10", query1("SELECT fn_ren_g_new(5)"));
        expectError("SELECT fn_ren_g(5)", "42883");
    }

    // =========================================================================
    // H. Procedure rename
    // =========================================================================

    @Test
    void rename_procedure() throws SQLException {
        exec("CREATE TABLE t_ren_h (msg text)");
        exec("CREATE PROCEDURE proc_ren_h(t text) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO t_ren_h VALUES (t); END; $$");
        exec("ALTER PROCEDURE proc_ren_h(text) RENAME TO proc_ren_h_new");
        exec("CALL proc_ren_h_new('test')");
        assertEquals("test", query1("SELECT msg FROM t_ren_h"));
    }

    // =========================================================================
    // I. Sequence rename
    // =========================================================================

    @Test
    void rename_sequence() throws SQLException {
        exec("CREATE SEQUENCE seq_ren_i START 100");
        exec("ALTER SEQUENCE seq_ren_i RENAME TO seq_ren_i_new");
        String val = query1("SELECT nextval('seq_ren_i_new')");
        assertTrue(Long.parseLong(val) >= 100);
        expectError("SELECT nextval('seq_ren_i')", "42P01");
    }

    // =========================================================================
    // J. Type rename
    // =========================================================================

    @Test
    void rename_enum_type() throws SQLException {
        exec("CREATE TYPE color_ren_j AS ENUM ('red', 'green', 'blue')");
        exec("ALTER TYPE color_ren_j RENAME TO color_ren_j_new");
        assertEquals("red", query1("SELECT 'red'::color_ren_j_new"));
    }

    @Test
    void rename_enum_value() throws SQLException {
        exec("CREATE TYPE fruit_ren_j AS ENUM ('apple', 'banana')");
        exec("ALTER TYPE fruit_ren_j RENAME VALUE 'apple' TO 'pear'");
        assertEquals("pear", query1("SELECT 'pear'::fruit_ren_j"));
        expectError("SELECT 'apple'::fruit_ren_j", "22P02");
    }

    // =========================================================================
    // K. Index rename
    // =========================================================================

    @Test
    void rename_index() throws SQLException {
        exec("CREATE TABLE t_ren_k (id int, val text)");
        exec("CREATE INDEX idx_ren_k ON t_ren_k (val)");
        exec("ALTER INDEX idx_ren_k RENAME TO idx_ren_k_new");
        assertEquals("idx_ren_k_new", query1(
            "SELECT indexname FROM pg_indexes WHERE indexname = 'idx_ren_k_new'"));
    }

    // =========================================================================
    // L. Database rename
    // =========================================================================

    @Test
    void rename_database() throws SQLException {
        exec("CREATE DATABASE db_ren_l");
        exec("ALTER DATABASE db_ren_l RENAME TO db_ren_l2");
        assertEquals("t", query1("SELECT count(*) > 0 FROM pg_database WHERE datname = 'db_ren_l2'"));
        exec("DROP DATABASE db_ren_l2");
    }

    // =========================================================================
    // M. Event trigger rename
    // =========================================================================

    @Test
    void rename_event_trigger() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION evt_fn_ren_m() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$");
        exec("CREATE EVENT TRIGGER evt_ren_m ON ddl_command_end EXECUTE FUNCTION evt_fn_ren_m()");
        exec("ALTER EVENT TRIGGER evt_ren_m RENAME TO evt_ren_m_new");
        assertEquals("evt_ren_m_new", query1(
            "SELECT evtname FROM pg_event_trigger WHERE evtname = 'evt_ren_m_new'"));
        exec("DROP EVENT TRIGGER evt_ren_m_new");
    }

    // =========================================================================
    // N. Policy rename
    // =========================================================================

    @Test
    void rename_policy() throws SQLException {
        exec("CREATE TABLE t_ren_n (id int)");
        exec("ALTER TABLE t_ren_n ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY pol_ren_n ON t_ren_n FOR SELECT USING (true)");
        exec("ALTER POLICY pol_ren_n ON t_ren_n RENAME TO pol_ren_n_new");
        assertEquals("pol_ren_n_new", query1(
            "SELECT polname FROM pg_policy WHERE polrelid = 't_ren_n'::regclass"));
    }

    // =========================================================================
    // O. Rename to existing name errors
    // =========================================================================

    @Test
    void rename_table_to_existing_errors() throws SQLException {
        exec("CREATE TABLE t_ren_o1 (id int)");
        exec("CREATE TABLE t_ren_o2 (id int)");
        expectError("ALTER TABLE t_ren_o1 RENAME TO t_ren_o2", "42P07");
    }

    @Test
    void rename_view_to_existing_errors() throws SQLException {
        exec("CREATE TABLE t_ren_o3 (id int)");
        exec("CREATE VIEW v_ren_o1 AS SELECT * FROM t_ren_o3");
        exec("CREATE VIEW v_ren_o2 AS SELECT * FROM t_ren_o3");
        expectError("ALTER VIEW v_ren_o1 RENAME TO v_ren_o2", "42P07");
    }
}
