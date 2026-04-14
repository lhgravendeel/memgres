package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for setup divergence fixes identified by FeatureComparisonReport.
 * Each test corresponds to a category of divergences between PG 18 and Memgres.
 */
class ConstraintAndIndexValidationTest {

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

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Category C: ALTER CONSTRAINT [NOT] ENFORCED on CHECK constraints
    // PG 18 only allows this for FOREIGN KEY, not CHECK
    // ========================================================================

    @Test
    void alter_check_constraint_not_enforced_rejected() throws SQLException {
        exec("CREATE TABLE catc_t1(id int PRIMARY KEY, val int CONSTRAINT chk_val CHECK (val > 0))");
        try {
            // PG 18: "cannot alter enforceability of constraint" (42809)
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("ALTER TABLE catc_t1 ALTER CONSTRAINT chk_val NOT ENFORCED"));
            assertEquals("42809", ex.getSQLState());
        } finally {
            exec("DROP TABLE catc_t1");
        }
    }

    @Test
    void alter_check_constraint_enforced_on_not_enforced_check_rejected() throws SQLException {
        exec("CREATE TABLE catc_t2(id int PRIMARY KEY, val int CONSTRAINT chk_val CHECK (val > 0) NOT ENFORCED)");
        try {
            // PG 18: cannot toggle CHECK enforceability, even from NOT ENFORCED to ENFORCED
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("ALTER TABLE catc_t2 ALTER CONSTRAINT chk_val ENFORCED"));
            assertEquals("42809", ex.getSQLState());
        } finally {
            exec("DROP TABLE catc_t2");
        }
    }

    @Test
    void alter_fk_constraint_not_enforced_still_allowed() throws SQLException {
        exec("CREATE TABLE catc_parent(id int PRIMARY KEY)");
        exec("CREATE TABLE catc_child(id int PRIMARY KEY, pid int, " +
             "CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES catc_parent(id))");
        try {
            // FK constraints should still support ALTER CONSTRAINT ... NOT ENFORCED
            exec("ALTER TABLE catc_child ALTER CONSTRAINT fk_pid NOT ENFORCED");
            // And toggling back should work if no violations
            exec("ALTER TABLE catc_child ALTER CONSTRAINT fk_pid ENFORCED");
        } finally {
            exec("DROP TABLE catc_child");
            exec("DROP TABLE catc_parent");
        }
    }

    // ========================================================================
    // Category A: Expression index should NOT require IMMUTABLE
    // PG allows VOLATILE/STABLE functions in expression indexes
    // ========================================================================

    @Test
    void volatile_function_in_expression_index_allowed() throws SQLException {
        exec("CREATE FUNCTION cata_vol(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x * 2 $$");
        exec("CREATE TABLE cata_t1(id int, val int)");
        try {
            // PG allows volatile functions in expression indexes
            exec("CREATE INDEX idx_cata_vol ON cata_t1 (cata_vol(val))");
            exec("DROP INDEX idx_cata_vol");
        } finally {
            exec("DROP TABLE cata_t1");
            exec("DROP FUNCTION cata_vol(integer)");
        }
    }

    @Test
    void stable_function_in_expression_index_allowed() throws SQLException {
        exec("CREATE FUNCTION cata_stable(x integer) RETURNS integer LANGUAGE sql STABLE AS $$ SELECT x + 1 $$");
        exec("CREATE TABLE cata_t2(id int, val int)");
        try {
            exec("CREATE INDEX idx_cata_stable ON cata_t2 (cata_stable(val))");
            exec("DROP INDEX idx_cata_stable");
        } finally {
            exec("DROP TABLE cata_t2");
            exec("DROP FUNCTION cata_stable(integer)");
        }
    }

    @Test
    void immutable_function_in_expression_index_still_allowed() throws SQLException {
        exec("CREATE FUNCTION cata_imm(x integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT x * 3 $$");
        exec("CREATE TABLE cata_t3(id int, val int)");
        try {
            exec("CREATE INDEX idx_cata_imm ON cata_t3 (cata_imm(val))");
            exec("DROP INDEX idx_cata_imm");
        } finally {
            exec("DROP TABLE cata_t3");
            exec("DROP FUNCTION cata_imm(integer)");
        }
    }

    // ========================================================================
    // Category B: Reject CREATE INDEX on virtual generated columns
    // PG 18: "indexes on virtual generated columns are not supported" (0A000)
    // ========================================================================

    @Test
    void index_on_virtual_generated_column_rejected() throws SQLException {
        exec("CREATE TABLE catb_t1(id int, a int, b int, total int GENERATED ALWAYS AS (a + b) VIRTUAL)");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("CREATE INDEX idx_catb_total ON catb_t1 (total)"));
            assertEquals("0A000", ex.getSQLState());
            assertTrue(ex.getMessage().toLowerCase().contains("virtual generated column"));
        } finally {
            exec("DROP TABLE catb_t1");
        }
    }

    @Test
    void unique_index_on_virtual_generated_column_rejected() throws SQLException {
        exec("CREATE TABLE catb_t2(id int, a int, b int, ab_sum int GENERATED ALWAYS AS (a + b) VIRTUAL)");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("CREATE UNIQUE INDEX idx_catb_unique ON catb_t2 (ab_sum)"));
            assertEquals("0A000", ex.getSQLState());
        } finally {
            exec("DROP TABLE catb_t2");
        }
    }

    @Test
    void partial_index_where_references_virtual_column_rejected() throws SQLException {
        exec("CREATE TABLE catb_t3(id int, val int, doubled int GENERATED ALWAYS AS (val * 2) VIRTUAL)");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("CREATE INDEX idx_catb_partial ON catb_t3 (id) WHERE doubled > 20"));
            assertEquals("0A000", ex.getSQLState());
        } finally {
            exec("DROP TABLE catb_t3");
        }
    }

    @Test
    void index_on_stored_generated_column_still_allowed() throws SQLException {
        exec("CREATE TABLE catb_t4(id int, a int, b int, total int GENERATED ALWAYS AS (a + b) STORED)");
        try {
            exec("CREATE INDEX idx_catb_stored ON catb_t4 (total)");
            exec("DROP INDEX idx_catb_stored");
        } finally {
            exec("DROP TABLE catb_t4");
        }
    }

    // ========================================================================
    // Category E: ALTER FUNCTION ROWS on non-SRF should error
    // PG 18: "ROWS is not applicable when function does not return a set" (22023)
    // ========================================================================

    @Test
    void alter_function_rows_on_non_srf_rejected() throws SQLException {
        exec("CREATE FUNCTION cate_scalar(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x $$");
        try {
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("ALTER FUNCTION cate_scalar(integer) ROWS 500"));
            assertEquals("22023", ex.getSQLState());
            assertTrue(ex.getMessage().toLowerCase().contains("rows"));
        } finally {
            exec("DROP FUNCTION cate_scalar(integer)");
        }
    }

    @Test
    void alter_function_rows_on_srf_still_allowed() throws SQLException {
        exec("CREATE FUNCTION cate_srf(n integer) RETURNS SETOF integer LANGUAGE sql AS $$ SELECT generate_series(1, n) $$");
        try {
            exec("ALTER FUNCTION cate_srf(integer) ROWS 500");
            assertEquals("500", scalar("SELECT prorows FROM pg_proc WHERE proname = 'cate_srf'"));
        } finally {
            exec("DROP FUNCTION cate_srf(integer)");
        }
    }

    // ========================================================================
    // Category G: ALTER TABLE ... RESET (storage_params) as no-op
    // ========================================================================

    @Test
    void alter_table_reset_storage_params_accepted() throws SQLException {
        exec("CREATE TABLE catg_t1(id int)");
        try {
            exec("ALTER TABLE catg_t1 SET (fillfactor = 70)");
            exec("ALTER TABLE catg_t1 RESET (fillfactor)");
        } finally {
            exec("DROP TABLE catg_t1");
        }
    }
}
