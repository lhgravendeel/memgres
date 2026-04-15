package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class ErrorResponseFieldsCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP SCHEMA IF EXISTS errfield_test CASCADE");
            stmt.execute("CREATE SCHEMA errfield_test");
            stmt.execute("SET search_path = errfield_test, public");

            // Create the corrected errfield_diag function using err_sqlstate (not sqlstate)
            stmt.execute(
                "CREATE FUNCTION errfield_diag(sql_text text) RETURNS TABLE(\n" +
                "  err_sqlstate text, message text, detail text, hint text,\n" +
                "  schema_name text, table_name text, column_name text,\n" +
                "  constraint_name text, datatype_name text\n" +
                ")\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_state text; v_msg text; v_detail text; v_hint text;\n" +
                "  v_schema text; v_table text; v_column text;\n" +
                "  v_constraint text; v_datatype text;\n" +
                "BEGIN\n" +
                "  EXECUTE sql_text;\n" +
                "  err_sqlstate := 'OK'; message := 'no error';\n" +
                "  RETURN NEXT;\n" +
                "EXCEPTION WHEN OTHERS THEN\n" +
                "  GET STACKED DIAGNOSTICS\n" +
                "    v_state = RETURNED_SQLSTATE,\n" +
                "    v_msg = MESSAGE_TEXT,\n" +
                "    v_detail = PG_EXCEPTION_DETAIL,\n" +
                "    v_hint = PG_EXCEPTION_HINT,\n" +
                "    v_schema = SCHEMA_NAME,\n" +
                "    v_table = TABLE_NAME,\n" +
                "    v_column = COLUMN_NAME,\n" +
                "    v_constraint = CONSTRAINT_NAME,\n" +
                "    v_datatype = PG_DATATYPE_NAME;\n" +
                "  err_sqlstate := v_state;\n" +
                "  message := v_msg;\n" +
                "  detail := v_detail;\n" +
                "  hint := v_hint;\n" +
                "  schema_name := v_schema;\n" +
                "  table_name := v_table;\n" +
                "  column_name := v_column;\n" +
                "  constraint_name := v_constraint;\n" +
                "  datatype_name := v_datatype;\n" +
                "  RETURN NEXT;\n" +
                "END;\n" +
                "$$"
            );

            // Tables for PK violation tests
            stmt.execute("CREATE TABLE errfield_pk (id integer PRIMARY KEY)");
            stmt.execute("INSERT INTO errfield_pk VALUES (1)");

            // Tables for NOT NULL violation test
            stmt.execute("CREATE TABLE errfield_nn (id integer NOT NULL, val text NOT NULL)");

            // Table for CHECK constraint violation test
            stmt.execute("CREATE TABLE errfield_chk (val integer CONSTRAINT val_positive CHECK (val > 0))");

            // Tables for FK violation test
            stmt.execute("CREATE TABLE errfield_parent (id integer PRIMARY KEY)");
            stmt.execute("CREATE TABLE errfield_child (\n" +
                    "  id integer,\n" +
                    "  parent_id integer REFERENCES errfield_parent(id)\n" +
                    ")");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP SCHEMA IF EXISTS errfield_test CASCADE");
            } catch (Exception ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // Root cause test: PL/pgSQL should reject sqlstate as a variable name
    // =========================================================================

    @Test
    @DisplayName("PL/pgSQL should reject sqlstate as output column name (PG treats it as CONSTANT)")
    void testSqlstateRejectedAsVariableName() {
        // PG 18 rejects this: "variable sqlstate is declared CONSTANT"
        // Memgres incorrectly allows it. This test is INTENDED TO FAIL on Memgres.
        assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(
                    "CREATE FUNCTION test_sqlstate_var() RETURNS TABLE(sqlstate text) " +
                    "LANGUAGE plpgsql AS $$\n" +
                    "BEGIN\n" +
                    "  sqlstate := 'test';\n" +
                    "  RETURN NEXT;\n" +
                    "END;\n" +
                    "$$"
                );
            }
        });
    }

    // =========================================================================
    // Tests using the corrected errfield_diag function (with err_sqlstate)
    // =========================================================================

    @Test
    @DisplayName("errfield_diag with err_sqlstate works for PK violation - returns SQLSTATE 23505")
    void testPkViolationSqlstate() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT err_sqlstate FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)')")) {
            assertTrue(rs.next());
            assertEquals("23505", rs.getString("err_sqlstate"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns detail for PK violation")
    void testPkViolationHasDetail() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT (detail IS NOT NULL AND detail <> '') AS has_detail " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_detail"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns constraint_name for PK violation")
    void testPkViolationHasConstraint() throws Exception {
        // PG returns true, Memgres returns false - INTENDED TO FAIL
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT (constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_constraint"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns schema_name for PK violation")
    void testPkViolationHasSchema() throws Exception {
        // PG returns true, Memgres returns false - INTENDED TO FAIL
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT (schema_name IS NOT NULL AND schema_name <> '') AS has_schema " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_schema"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns table_name for PK violation")
    void testPkViolationHasTable() throws Exception {
        // PG returns true, Memgres returns false - INTENDED TO FAIL
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT (table_name IS NOT NULL AND table_name <> '') AS has_table " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_pk VALUES (1)')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_table"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns column_name for NOT NULL violation")
    void testNotNullViolationHasColumn() throws Exception {
        // PG populates column_name for NOT NULL violations; Memgres may not - INTENDED TO FAIL
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT err_sqlstate, " +
                     "(column_name IS NOT NULL AND column_name <> '') AS has_column " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_nn (id) VALUES (1)')")) {
            assertTrue(rs.next());
            assertEquals("23502", rs.getString("err_sqlstate"));
            assertTrue(rs.getBoolean("has_column"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns constraint_name for CHECK violation")
    void testCheckViolationHasConstraint() throws Exception {
        // PG populates constraint_name for CHECK violations; Memgres may not - INTENDED TO FAIL
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT err_sqlstate, " +
                     "(constraint_name IS NOT NULL AND constraint_name <> '') AS has_constraint " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_chk VALUES (-1)')")) {
            assertTrue(rs.next());
            assertEquals("23514", rs.getString("err_sqlstate"));
            assertTrue(rs.getBoolean("has_constraint"));
        }
    }

    @Test
    @DisplayName("errfield_diag returns detail for FK violation")
    void testFkViolationHasDetail() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT err_sqlstate, " +
                     "(detail IS NOT NULL AND detail <> '') AS has_detail " +
                     "FROM errfield_diag('INSERT INTO errfield_test.errfield_child VALUES (1, 999)')")) {
            assertTrue(rs.next());
            assertEquals("23503", rs.getString("err_sqlstate"));
            assertTrue(rs.getBoolean("has_detail"));
        }
    }
}
