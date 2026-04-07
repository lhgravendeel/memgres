package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 4 (exact error capture) and section 11 (Java/JDBC):
 * Exception mapping and SQLSTATE handling.
 * Tests unique constraint, FK, syntax errors, serialization failures,
 * not-null/check violations, and verifies correct SQLSTATE codes.
 */
class SqlStateExceptionMappingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // --- 23505: unique_violation ---

    @Test void unique_violation_primary_key() throws Exception {
        exec("CREATE TABLE ss_pk(id int PRIMARY KEY)");
        exec("INSERT INTO ss_pk VALUES (1)");
        SQLException ex = assertThrows(SQLException.class, () -> exec("INSERT INTO ss_pk VALUES (1)"));
        assertEquals("23505", ex.getSQLState());
        assertTrue(ex.getMessage().contains("duplicate key"));
        exec("DROP TABLE ss_pk");
    }

    @Test void unique_violation_unique_constraint() throws Exception {
        exec("CREATE TABLE ss_uniq(id int, code text UNIQUE)");
        exec("INSERT INTO ss_uniq VALUES (1, 'A')");
        SQLException ex = assertThrows(SQLException.class, () -> exec("INSERT INTO ss_uniq VALUES (2, 'A')"));
        assertEquals("23505", ex.getSQLState());
        exec("DROP TABLE ss_uniq");
    }

    // --- 23503: foreign_key_violation ---

    @Test void fk_violation_on_insert() throws Exception {
        exec("CREATE TABLE ss_parent(id int PRIMARY KEY)");
        exec("CREATE TABLE ss_child(id int, parent_id int REFERENCES ss_parent(id))");
        exec("INSERT INTO ss_parent VALUES (1)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ss_child VALUES (1, 999)"));
        assertEquals("23503", ex.getSQLState());
        exec("DROP TABLE ss_child");
        exec("DROP TABLE ss_parent");
    }

    @Test void fk_violation_on_delete() throws Exception {
        exec("CREATE TABLE ss_par2(id int PRIMARY KEY)");
        exec("CREATE TABLE ss_ch2(id int, pid int REFERENCES ss_par2(id))");
        exec("INSERT INTO ss_par2 VALUES (1)");
        exec("INSERT INTO ss_ch2 VALUES (1, 1)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("DELETE FROM ss_par2 WHERE id = 1"));
        assertEquals("23503", ex.getSQLState());
        exec("DROP TABLE ss_ch2");
        exec("DROP TABLE ss_par2");
    }

    // --- 23502: not_null_violation ---

    @Test void not_null_violation() throws Exception {
        exec("CREATE TABLE ss_nn(id int NOT NULL)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ss_nn VALUES (NULL)"));
        assertEquals("23502", ex.getSQLState());
        assertTrue(ex.getMessage().contains("not-null"));
        exec("DROP TABLE ss_nn");
    }

    // --- 23514: check_violation ---

    @Test void check_violation() throws Exception {
        exec("CREATE TABLE ss_chk(id int, v int CHECK (v > 0))");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ss_chk VALUES (1, -1)"));
        assertEquals("23514", ex.getSQLState());
        assertTrue(ex.getMessage().contains("check constraint"));
        exec("DROP TABLE ss_chk");
    }

    // --- 42601: syntax_error ---

    @Test void syntax_error() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELCT 1"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test void syntax_error_missing_from() throws Exception {
        exec("CREATE TABLE ss_syn(id int)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT FROM"));
        assertEquals("42601", ex.getSQLState());
        exec("DROP TABLE IF EXISTS ss_syn");
    }

    // --- 42P01: undefined_table ---

    @Test void undefined_table() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT * FROM no_such_table_xyz"));
        assertEquals("42P01", ex.getSQLState());
    }

    // --- 42703: undefined_column ---

    @Test void undefined_column() throws Exception {
        exec("CREATE TABLE ss_ucol(id int)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT no_such_column FROM ss_ucol"));
        assertEquals("42703", ex.getSQLState());
        exec("DROP TABLE ss_ucol");
    }

    // --- 42P07: duplicate_table ---

    @Test void duplicate_table() throws Exception {
        exec("CREATE TABLE ss_dup(id int)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE ss_dup(id int)"));
        assertEquals("42P07", ex.getSQLState());
        exec("DROP TABLE ss_dup");
    }

    // --- 42704: undefined_object ---

    @Test void undefined_type() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE ss_utype(v no_such_type)"));
        assertEquals("42704", ex.getSQLState());
    }

    // --- 22P02: invalid_text_representation ---

    @Test void invalid_integer_input() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT 'abc'::integer"));
        assertEquals("22P02", ex.getSQLState());
    }

    // --- 22003: numeric_value_out_of_range ---

    @Test void integer_overflow() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT 2147483648::integer"));
        assertTrue(ex.getSQLState().equals("22003") || ex.getSQLState().equals("22P02"),
                "Should be out of range: " + ex.getSQLState());
    }

    // --- 22012: division_by_zero ---

    @Test void division_by_zero() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT 1/0"));
        assertEquals("22012", ex.getSQLState());
    }

    // --- 42702: ambiguous_column ---

    @Test void ambiguous_column_in_join() throws Exception {
        exec("CREATE TABLE ss_amb1(id int, v text)");
        exec("CREATE TABLE ss_amb2(id int, v text)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT id FROM ss_amb1 JOIN ss_amb2 ON ss_amb1.id = ss_amb2.id"));
        assertEquals("42702", ex.getSQLState());
        exec("DROP TABLE ss_amb1");
        exec("DROP TABLE ss_amb2");
    }

    // --- 42883: undefined_function ---

    @Test void undefined_function() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT no_such_function_xyz()"));
        assertEquals("42883", ex.getSQLState());
    }

    // --- 26000: invalid_sql_statement_name ---

    @Test void invalid_prepared_statement() throws Exception {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXECUTE no_such_prepared_stmt"));
        assertEquals("26000", ex.getSQLState());
    }

    // --- 3F000: invalid_schema_name ---

    @Test void invalid_schema() throws Exception {
        // PG does not validate search_path at SET time (schemas are resolved lazily).
        // Use CREATE TABLE in a nonexistent schema to trigger 3F000.
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE no_such_schema_xyz.t (id int)"));
        assertTrue(ex.getSQLState().equals("3F000") || ex.getSQLState().equals("42704"),
                "Expected schema error: " + ex.getSQLState());
    }

    // --- 23P01: exclusion_violation ---

    @Test void exclusion_constraint_violation() throws Exception {
        exec("CREATE TABLE ss_excl(room text, during tsrange, EXCLUDE USING gist (room WITH =, during WITH &&))");
        exec("INSERT INTO ss_excl VALUES ('A', '[2024-01-01, 2024-01-10)')");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO ss_excl VALUES ('A', '[2024-01-05, 2024-01-15)')"));
            assertEquals("23P01", ex.getSQLState());
        } finally {
            exec("DROP TABLE IF EXISTS ss_excl");
        }
    }

    // --- 428C9: generated_always ---

    @Test void generated_always_identity_violation() throws Exception {
        exec("CREATE TABLE ss_gen(id int GENERATED ALWAYS AS IDENTITY)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ss_gen(id) VALUES (1)"));
        assertEquals("428C9", ex.getSQLState());
        exec("DROP TABLE ss_gen");
    }

    // --- 21000: cardinality_violation ---

    @Test void subquery_returns_multiple_rows() throws Exception {
        exec("CREATE TABLE ss_card(id int)");
        exec("INSERT INTO ss_card VALUES (1),(2)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT (SELECT id FROM ss_card)"));
        assertEquals("21000", ex.getSQLState());
        exec("DROP TABLE ss_card");
    }
}
