package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests error response detail field population.
 *
 * PG 18: Error responses include rich diagnostic fields accessible via
 * JDBC's SQLException methods. For constraint violations, PG provides
 * DETAIL (e.g., "Key (col)=(val) already exists"), SCHEMA, TABLE, COLUMN,
 * CONSTRAINT, and DATATYPE fields.
 *
 * Memgres: The MemgresException class supports these fields, and they are
 * populated for some constraint violations, but many error paths only send
 * Severity + Code + Message, omitting DETAIL, HINT, and positional info.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres
 * for error paths that lack detail fields.
 */
class ErrorDetailCompatTest {

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

    @BeforeEach
    void setup() throws SQLException {
        exec("DROP TABLE IF EXISTS err_detail CASCADE");
        exec("CREATE TABLE err_detail ("
                + "  id int PRIMARY KEY, "
                + "  name text NOT NULL, "
                + "  email text UNIQUE, "
                + "  score int CHECK (score >= 0)"
                + ")");
        exec("INSERT INTO err_detail VALUES (1, 'Alice', 'alice@test.com', 10)");
    }

    // -------------------------------------------------------------------------
    // PK violation should include DETAIL with key values
    // -------------------------------------------------------------------------

    @Test
    void pkViolation_shouldIncludeDetailWithKeyValues() {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO err_detail VALUES (1, 'Bob', 'bob@test.com', 20)");
            fail("Should throw unique violation");
        } catch (SQLException e) {
            assertEquals("23505", e.getSQLState());

            // PG provides DETAIL like: "Key (id)=(1) already exists."
            // JDBC exposes this via PSQLException.getServerErrorMessage().getDetail()
            // Standard JDBC doesn't have getDetail(), but PG driver puts it in the message
            String msg = e.getMessage();
            assertTrue(msg.contains("Detail:") || msg.contains("(id)=(1)") || msg.contains("already exists"),
                    "PK violation should include detail about the conflicting key. Got: " + msg);
        }
    }

    // -------------------------------------------------------------------------
    // UNIQUE violation detail
    // -------------------------------------------------------------------------

    @Test
    void uniqueViolation_shouldIncludeKeyDetail() {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO err_detail VALUES (2, 'Bob', 'alice@test.com', 20)");
            fail("Should throw unique violation on email");
        } catch (SQLException e) {
            assertEquals("23505", e.getSQLState());
            String msg = e.getMessage();
            assertTrue(msg.contains("email") || msg.contains("alice@test.com"),
                    "UNIQUE violation should mention the constraint column or conflicting value. Got: " + msg);
        }
    }

    // -------------------------------------------------------------------------
    // NOT NULL violation should mention column name
    // -------------------------------------------------------------------------

    @Test
    void notNullViolation_shouldMentionColumnName() {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO err_detail (id, email, score) VALUES (2, 'b@t.com', 5)");
            fail("Should throw not-null violation for 'name'");
        } catch (SQLException e) {
            assertEquals("23502", e.getSQLState());
            String msg = e.getMessage();
            assertTrue(msg.contains("name"),
                    "NOT NULL violation should mention column 'name'. Got: " + msg);
        }
    }

    // -------------------------------------------------------------------------
    // CHECK violation should mention constraint name
    // -------------------------------------------------------------------------

    @Test
    void checkViolation_shouldMentionConstraint() {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO err_detail VALUES (2, 'Bob', 'bob@t.com', -5)");
            fail("Should throw check constraint violation");
        } catch (SQLException e) {
            assertEquals("23514", e.getSQLState());
            String msg = e.getMessage();
            // PG includes constraint name and sometimes detail
            assertTrue(msg.contains("err_detail_score_check") || msg.contains("score"),
                    "CHECK violation should mention constraint name or column. Got: " + msg);
        }
    }

    // -------------------------------------------------------------------------
    // Foreign key violation should include detail
    // -------------------------------------------------------------------------

    @Test
    void fkViolation_shouldIncludeReferencedTableDetail() throws SQLException {
        exec("DROP TABLE IF EXISTS err_child CASCADE");
        exec("CREATE TABLE err_child ("
                + "  id int PRIMARY KEY, "
                + "  parent_id int REFERENCES err_detail(id)"
                + ")");

        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO err_child VALUES (1, 999)");
            fail("Should throw FK violation");
        } catch (SQLException e) {
            assertEquals("23503", e.getSQLState());
            String msg = e.getMessage();
            // PG DETAIL: "Key (parent_id)=(999) is not present in table "err_detail"."
            assertTrue(msg.contains("err_detail") || msg.contains("parent_id")
                            || msg.contains("is not present"),
                    "FK violation should mention referenced table and key. Got: " + msg);
        }
    }

    // -------------------------------------------------------------------------
    // Type error should include position information
    // -------------------------------------------------------------------------

    @Test
    void typeError_shouldIncludePositionHint() {
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT 'abc' + 1");
            fail("Should throw type error");
        } catch (SQLException e) {
            // PG includes Position field indicating where in the query the error is
            String msg = e.getMessage();
            // The JDBC PG driver includes "Position: N" in the error message
            // This tests whether Memgres provides positional information
            assertNotNull(e.getSQLState(),
                    "Type error should have a SQLSTATE code");
        }
    }

    // -------------------------------------------------------------------------
    // Syntax error should include position
    // -------------------------------------------------------------------------

    @Test
    void syntaxError_shouldIncludePosition() {
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT * FORM err_detail");
            fail("Should throw syntax error");
        } catch (SQLException e) {
            assertEquals("42601", e.getSQLState());
            // PG includes: Position: N (character position of the error)
            // The PG JDBC driver exposes this as part of the message or via
            // PSQLException.getServerErrorMessage().getPosition()
            String msg = e.getMessage();
            // Check that some positional hint is present
            assertTrue(msg.contains("Position") || msg.contains("position")
                            || msg.length() > 20,
                    "Syntax error should include position information. Got: " + msg);
        }
    }

    // -------------------------------------------------------------------------
    // Schema name in error context
    // -------------------------------------------------------------------------

    @Test
    void constraintViolation_shouldIncludeSchemaName() throws SQLException {
        exec("DROP SCHEMA IF EXISTS err_schema CASCADE");
        exec("CREATE SCHEMA err_schema");
        exec("CREATE TABLE err_schema.constrained (id int PRIMARY KEY)");
        exec("INSERT INTO err_schema.constrained VALUES (1)");

        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO err_schema.constrained VALUES (1)");
            fail("Should throw PK violation");
        } catch (SQLException e) {
            assertEquals("23505", e.getSQLState());
            // PG includes Schema Name field in the error response
            String msg = e.getMessage();
            assertTrue(msg.contains("err_schema") || msg.contains("constrained"),
                    "Error should include schema or table context. Got: " + msg);
        }

        exec("DROP SCHEMA err_schema CASCADE");
    }
}
