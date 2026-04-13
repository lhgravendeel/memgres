package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 8 failures from virtual-generated-columns.sql where Memgres
 * diverges from PostgreSQL 18 behavior.
 *
 * Stmt  59 - SELECT from vg_immut should error (relation does not exist) but Memgres succeeds
 * Stmt  62 - SQLSTATE for stable-function rejection: PG=0A000, Memgres=42P17
 * Stmt  74 - Error message mismatch: expected "both default and generation expression specified"
 * Stmt  91 - DROP COLUMN on source of virtual col should error, Memgres succeeds
 * Stmt  93 - After CASCADE drop, pg_attribute count for col b: PG=0, Memgres=1
 * Stmt 111 - Division by zero in virtual col: PG errors, Memgres returns NULL
 * Stmt 124 - SELECT from vg_array: PG succeeds with {10,20}, Memgres errors (relation not found)
 * Stmt 149 - ALTER COLUMN TYPE on source col: PG errors, Memgres succeeds
 * Stmt 157 - FK referencing virtual column: PG errors (no unique constraint), Memgres succeeds
 */
class VirtualGeneratedColumnsCompat15Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS vg_test CASCADE");
            s.execute("CREATE SCHEMA vg_test");
            s.execute("SET search_path = vg_test, public");

            // -- Section 25: IMMUTABLE function + table for Stmt 59
            // PG 18 rejects user-defined functions in virtual columns (even IMMUTABLE)
            s.execute("CREATE FUNCTION vg_double(x integer) RETURNS integer "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT x * 2 $$");
            try {
                s.execute("CREATE TABLE vg_immut ("
                        + "id integer PRIMARY KEY, val integer, "
                        + "doubled integer GENERATED ALWAYS AS (vg_double(val)) VIRTUAL)");
                s.execute("INSERT INTO vg_immut (id, val) VALUES (1, 7)");
            } catch (SQLException ignored) {
                // Expected: PG 18 rejects UDFs in virtual columns
            }

            // -- Section 26: STABLE function for Stmt 62
            s.execute("CREATE FUNCTION vg_stable_fn(x integer) RETURNS integer "
                    + "LANGUAGE sql STABLE AS $$ SELECT x $$");

            // -- Section 38: DROP dependency table for Stmts 91/93
            s.execute("CREATE TABLE vg_drop_dep ("
                    + "id integer PRIMARY KEY, a integer, "
                    + "b integer GENERATED ALWAYS AS (a * 2) VIRTUAL)");

            // -- Section 41: Division by zero table for Stmt 111
            s.execute("CREATE TABLE vg_divzero ("
                    + "id integer PRIMARY KEY, a integer, b integer, "
                    + "ratio integer GENERATED ALWAYS AS (a / b) VIRTUAL)");
            s.execute("INSERT INTO vg_divzero (id, a, b) VALUES (1, 10, 2)");
            s.execute("INSERT INTO vg_divzero (id, a, b) VALUES (2, 10, 0)");

            // -- Section 49: ALTER source column TYPE for Stmt 149
            s.execute("CREATE TABLE vg_alter_type ("
                    + "id integer PRIMARY KEY, a integer, "
                    + "doubled integer GENERATED ALWAYS AS (a * 2) VIRTUAL)");
            s.execute("INSERT INTO vg_alter_type (id, a) VALUES (1, 5)");

            // -- Section 51: FK referencing VIRTUAL column for Stmt 157
            s.execute("CREATE TABLE vg_fk_parent ("
                    + "id integer PRIMARY KEY, val integer, "
                    + "doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS vg_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            String val = rs.getString(1);
            return val;
        }
    }

    /**
     * Stmt 59: SELECT doubled FROM vg_immut should error because PG 18 rejects
     * user-defined functions (even IMMUTABLE ones) in virtual generated columns,
     * so the table creation fails and the relation does not exist.
     * Memgres allows the function and returns the computed value (14).
     */
    @Test
    void testStmt59_immutableFunctionInVirtualCol_shouldErrorRelationNotExist() throws SQLException {
        // PG: ERROR [42P01]: relation "vg_immut" does not exist
        // Memgres: succeeds with doubled=14
        try {
            String result = query1("SELECT doubled FROM vg_immut");
            // If we reach here, Memgres succeeded -- this is the known divergence
            fail("Expected an error containing 'does not exist' but query succeeded with: " + result);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"),
                    "Expected error about relation not existing, got: " + e.getMessage());
        }
    }

    /**
     * Stmt 62: Creating a table with a STABLE function in a virtual generated column
     * should produce SQLSTATE 0A000 (feature_not_supported) as in PG.
     * Memgres returns 42P17 (invalid_object_definition) instead.
     */
    @Test
    void testStmt62_stableFunctionRejection_sqlstateShouldBe0A000() throws SQLException {
        // PG: ERROR [0A000]: generation expression uses user-defined function
        // Memgres: ERROR [42P17]: generation expression is not immutable
        try {
            exec("CREATE TABLE vg_stable_fail ("
                    + "id integer PRIMARY KEY, val integer, "
                    + "computed integer GENERATED ALWAYS AS (vg_stable_fn(val)) VIRTUAL)");
            fail("Expected an error when creating virtual column with STABLE function");
        } catch (SQLException e) {
            assertEquals("0A000", e.getSQLState(),
                    "SQLSTATE should be 0A000 (feature_not_supported) for STABLE function in virtual col, "
                            + "but got " + e.getSQLState() + ": " + e.getMessage());
        } finally {
            try { exec("DROP TABLE IF EXISTS vg_stable_fail"); } catch (SQLException ignored) {}
        }
    }

    /**
     * Stmt 74: Creating a table with both DEFAULT and GENERATED should produce an error
     * message containing "both default and generation expression specified".
     * Memgres says "a generated column is not allowed to have a default value" instead.
     */
    @Test
    void testStmt74_defaultAndGenerated_errorMessageShouldMatchPg() throws SQLException {
        // PG: "both default and generation expression specified"
        // Memgres: "a generated column is not allowed to have a default value"
        try {
            exec("CREATE TABLE vg_bad ("
                    + "id integer PRIMARY KEY, "
                    + "val integer DEFAULT 0 GENERATED ALWAYS AS (1) VIRTUAL)");
            fail("Expected an error when combining DEFAULT and GENERATED");
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertTrue(msg.contains("both default and generation expression specified"),
                    "Error message should contain 'both default and generation expression specified', "
                            + "got: " + e.getMessage());
        } finally {
            try { exec("DROP TABLE IF EXISTS vg_bad"); } catch (SQLException ignored) {}
        }
    }

    /**
     * Stmt 91: DROP COLUMN a on vg_drop_dep (where virtual col b depends on a)
     * should error with "other objects depend on it". Memgres succeeds silently.
     */
    @Test
    void testStmt91_dropSourceColumn_shouldErrorDependency() throws SQLException {
        // PG: ERROR [2BP01]: cannot drop column a ... other objects depend on it
        // Memgres: succeeds
        try {
            exec("ALTER TABLE vg_drop_dep DROP COLUMN a");
            fail("Expected an error about dependent objects when dropping source column of virtual column");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("other objects depend on it"),
                    "Error should mention dependent objects, got: " + e.getMessage());
        }
    }

    /**
     * Stmt 93: After DROP COLUMN a CASCADE on vg_drop_dep, the virtual column b
     * should also be dropped. pg_attribute should show 0 rows for column b.
     * Memgres returns 1 (column b survives the cascade).
     */
    @Test
    void testStmt93_cascadeDropShouldRemoveVirtualCol() throws SQLException {
        // First ensure the table is in a clean state by recreating it
        try { exec("DROP TABLE IF EXISTS vg_drop_dep"); } catch (SQLException ignored) {}
        exec("CREATE TABLE vg_drop_dep ("
                + "id integer PRIMARY KEY, a integer, "
                + "b integer GENERATED ALWAYS AS (a * 2) VIRTUAL)");

        // CASCADE drop should remove both column a and dependent virtual column b
        exec("ALTER TABLE vg_drop_dep DROP COLUMN a CASCADE");

        // PG: 0 (column b is gone), Memgres: 1 (column b still there)
        String cnt = query1(
                "SELECT count(*)::integer AS cnt FROM pg_attribute "
                        + "WHERE attrelid = 'vg_drop_dep'::regclass "
                        + "AND attname = 'b' AND NOT attisdropped");
        assertEquals("0", cnt,
                "After CASCADE drop of source column, virtual column b should also be dropped from pg_attribute");
    }

    /**
     * Stmt 111: Selecting a virtual column that computes a/b where b=0 should
     * raise a division-by-zero error at read time. Memgres returns NULL instead.
     */
    @Test
    void testStmt111_divisionByZeroInVirtualCol_shouldError() throws SQLException {
        // PG: ERROR [22012]: division by zero
        // Memgres: returns NULL
        try {
            String result = query1("SELECT ratio FROM vg_divzero WHERE id = 2");
            fail("Expected division by zero error but got result: " + result);
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("division by zero"),
                    "Expected division by zero error, got: " + e.getMessage());
        }
    }

    /**
     * Stmt 124: PG 18 supports ARRAY[a, b] in a virtual generated column and
     * returns {10,20}. Memgres fails to create the table so the relation does
     * not exist.
     */
    @Test
    void testStmt124_arrayExpressionInVirtualCol_shouldSucceed() throws SQLException {
        // PG: OK (pair) [{10,20}]
        // Memgres: ERROR [42P01]: relation "vg_array" does not exist
        try {
            exec("CREATE TABLE vg_array ("
                    + "id integer PRIMARY KEY, a integer, b integer, "
                    + "pair integer[] GENERATED ALWAYS AS (ARRAY[a, b]) VIRTUAL)");
        } catch (SQLException e) {
            fail("CREATE TABLE with ARRAY expression in virtual column should succeed, got: " + e.getMessage());
        }
        try {
            exec("INSERT INTO vg_array (id, a, b) VALUES (1, 10, 20)");
            String result = query1("SELECT pair::text FROM vg_array");
            assertEquals("{10,20}", result,
                    "Virtual column with ARRAY[a, b] should return {10,20}");
        } finally {
            try { exec("DROP TABLE IF EXISTS vg_array"); } catch (SQLException ignored) {}
        }
    }

    /**
     * Stmt 149: ALTER COLUMN a TYPE text on a column used by a virtual generated
     * column should error with "cannot alter type". Memgres allows it.
     */
    @Test
    void testStmt149_alterSourceColumnType_shouldError() throws SQLException {
        // PG: ERROR [0A000]: cannot alter type of a column used by a generated column
        // Memgres: succeeds
        try {
            exec("ALTER TABLE vg_alter_type ALTER COLUMN a TYPE text");
            fail("Expected an error about cannot alter type of a column used by a generated column");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("cannot alter type"),
                    "Error should mention 'cannot alter type', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 157: Creating a FK referencing a virtual generated column should error
     * because there is no unique constraint on that column.
     * Memgres allows the FK creation.
     */
    @Test
    void testStmt157_fkReferencingVirtualCol_shouldError() throws SQLException {
        // PG: ERROR [42830]: no unique constraint matching given keys for referenced table
        // Memgres: succeeds
        try {
            exec("CREATE TABLE vg_fk_child ("
                    + "id integer PRIMARY KEY, "
                    + "ref integer REFERENCES vg_fk_parent(doubled))");
            fail("Expected an error about no unique constraint for FK referencing a virtual column");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("no unique constraint"),
                    "Error should mention 'no unique constraint', got: " + e.getMessage());
        } finally {
            try { exec("DROP TABLE IF EXISTS vg_fk_child"); } catch (SQLException ignored) {}
        }
    }
}
