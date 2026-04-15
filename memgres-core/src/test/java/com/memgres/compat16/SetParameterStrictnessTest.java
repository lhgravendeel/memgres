package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests SET parameter strictness differences between Memgres and PostgreSQL 18.
 *
 * PG 18: SET with an unrecognized parameter name throws error 42704
 * ("unrecognized configuration parameter"). Only known GUC parameters are accepted.
 *
 * Memgres: Silently accepts ANY parameter name (intentional for pg_dump
 * compatibility, but differs from PG behavior).
 *
 * Additionally, PG validates parameter value types (e.g., SET work_mem = 'abc'
 * fails), while Memgres may accept invalid values.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class SetParameterStrictnessTest {

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

    // -------------------------------------------------------------------------
    // Unknown parameters should be rejected
    // -------------------------------------------------------------------------

    @Test
    void set_unknownParameter_shouldError42704() {
        // PG: ERROR 42704 "unrecognized configuration parameter"
        // Memgres: silently accepts
        try (Statement s = conn.createStatement()) {
            s.execute("SET completely_nonexistent_param = 'hello'");
            fail("SET with unknown parameter should throw error 42704, but succeeded");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "Unknown parameter should produce 42704, got: "
                            + e.getSQLState() + " - " + e.getMessage());
        }
    }

    @Test
    void set_typoInKnownParameter_shouldError42704() {
        // Common typo: 'work_meme' instead of 'work_mem'
        try (Statement s = conn.createStatement()) {
            s.execute("SET work_meme = '4MB'");
            fail("SET with misspelled parameter should throw error 42704");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "Misspelled parameter should produce 42704, got: " + e.getSQLState());
        }
    }

    @Test
    void set_anotherFakeParameter_shouldError42704() {
        try (Statement s = conn.createStatement()) {
            s.execute("SET my_custom.setting = 'value'");
            // PG: custom namespaced params (with dot) ARE allowed (custom_variable_classes)
            // But a flat name without a dot should fail
        } catch (SQLException e) {
            // If it's a dotted name, PG allows it as a custom variable
            // If flat name, PG rejects with 42704
        }

        // Flat (non-dotted) unknown parameter should definitely fail
        try (Statement s = conn.createStatement()) {
            s.execute("SET bogus_flat_param = 'x'");
            fail("SET with flat unknown parameter should throw error 42704");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "Unknown flat parameter should produce 42704, got: " + e.getSQLState());
        }
    }

    // -------------------------------------------------------------------------
    // Invalid parameter values should be rejected
    // -------------------------------------------------------------------------

    @Test
    void set_invalidBooleanValue_shouldError() {
        // PG: SET enable_seqscan = 'maybe' -> error (not a valid boolean)
        try (Statement s = conn.createStatement()) {
            s.execute("SET enable_seqscan = 'maybe'");
            fail("Invalid boolean value should throw an error");
        } catch (SQLException e) {
            // PG throws 22023 (invalid_parameter_value) for invalid values
            String state = e.getSQLState();
            assertTrue("22023".equals(state) || "42601".equals(state),
                    "Invalid boolean param should produce 22023 or 42601, got: " + state);
        }
    }

    @Test
    void set_invalidMemoryValue_shouldError() {
        // PG: SET work_mem = 'not_a_size' -> error
        try (Statement s = conn.createStatement()) {
            s.execute("SET work_mem = 'not_a_size'");
            fail("Invalid memory value should throw an error");
        } catch (SQLException e) {
            assertNotNull(e.getSQLState(), "Should have a SQLSTATE for invalid memory value");
        }
    }

    // -------------------------------------------------------------------------
    // SHOW of unknown parameter should also error
    // -------------------------------------------------------------------------

    @Test
    void show_unknownParameter_shouldError42704() {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SHOW completely_nonexistent_param");
            fail("SHOW of unknown parameter should throw error 42704");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "SHOW unknown parameter should produce 42704, got: " + e.getSQLState());
        }
    }

    // -------------------------------------------------------------------------
    // current_setting() with unknown parameter
    // -------------------------------------------------------------------------

    @Test
    void currentSetting_unknownParam_shouldErrorByDefault() {
        // PG: current_setting('bogus') -> error
        // PG: current_setting('bogus', true) -> NULL
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT current_setting('bogus_nonexistent_setting')");
            fail("current_setting with unknown param should throw error");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "current_setting unknown param should produce 42704, got: " + e.getSQLState());
        }
    }

    @Test
    void currentSetting_unknownParam_missingOkTrue_shouldReturnNull() throws SQLException {
        // With missing_ok = true, should return NULL instead of erroring
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT current_setting('bogus_nonexistent_setting', true)")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1),
                    "current_setting with missing_ok=true should return NULL for unknown param");
        }
    }

    // -------------------------------------------------------------------------
    // RESET of unknown parameter
    // -------------------------------------------------------------------------

    @Test
    void reset_unknownParameter_shouldError42704() {
        try (Statement s = conn.createStatement()) {
            s.execute("RESET completely_nonexistent_param");
            fail("RESET of unknown parameter should throw error 42704");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "RESET unknown param should produce 42704, got: " + e.getSQLState());
        }
    }
}
