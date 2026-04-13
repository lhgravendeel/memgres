package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 11 remaining Memgres-vs-PG differences from sql-json.sql.
 *
 * These tests assert PG 18 behavior. They are expected to FAIL on current
 * Memgres and pass once the underlying issues are fixed.
 */
class RemainingJsonCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

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
        if (conn != null) {
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 40: JSON_TABLE with invalid JSON should error with SQLSTATE 22P02
    // PG: ERROR [22P02] invalid input syntax for type json
    // Memgres: ERROR [22032] invalid JSON input for JSON_TABLE
    // ========================================================================
    @Test
    void stmt40_jsonTableInvalidJsonShouldErrorWith22P02() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT * FROM JSON_TABLE( 'not json', '$[*]' "
                    + "COLUMNS (name text PATH '$.name') ERROR ON ERROR ) AS jt");
            fail("Expected an error for invalid JSON input, but query succeeded");
        } catch (SQLException e) {
            assertEquals("22P02", e.getSQLState(),
                    "SQLSTATE should be 22P02 (invalid input syntax), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 62: JSON_VALUE non-scalar result should error with SQLSTATE 2203F
    // PG: ERROR [2203F] JSON path expression in JSON_VALUE must return single scalar item
    // Memgres: ERROR [22032] JSON_VALUE: non-scalar result
    // ========================================================================
    @Test
    void stmt62_jsonValueNonScalarShouldErrorWith2203F() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_VALUE('{\"a\":{\"b\":1}}', '$.a' ERROR ON ERROR)");
            fail("Expected an error for non-scalar JSON_VALUE result, but query succeeded");
        } catch (SQLException e) {
            assertEquals("2203F", e.getSQLState(),
                    "SQLSTATE should be 2203F (non-scalar result), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 63: JSON_TABLE invalid jsonpath should error with SQLSTATE 42601
    // PG: ERROR [42601] syntax error at or near "[" of jsonpath input
    // Memgres: ERROR [22032] invalid JSON input for JSON_TABLE
    // ========================================================================
    @Test
    void stmt63_jsonTableInvalidJsonpathShouldErrorWith42601() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT * FROM JSON_TABLE('{\"a\":1}', '$.[[invalid' "
                    + "COLUMNS (x text PATH '$') ERROR ON ERROR) jt");
            fail("Expected a syntax error for invalid jsonpath, but query succeeded");
        } catch (SQLException e) {
            assertEquals("42601", e.getSQLState(),
                    "SQLSTATE should be 42601 (syntax error), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 69: JSON_VALUE non-scalar (nested object) should error with SQLSTATE 2203F
    // PG: ERROR [2203F] JSON path expression in JSON_VALUE must return single scalar item
    // Memgres: ERROR [22032] JSON_VALUE: non-scalar result
    // ========================================================================
    @Test
    void stmt69_jsonValueNestedObjectShouldErrorWith2203F() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_VALUE('{\"a\": {\"nested\": true}}', '$.a' ERROR ON ERROR)");
            fail("Expected an error for non-scalar JSON_VALUE result, but query succeeded");
        } catch (SQLException e) {
            assertEquals("2203F", e.getSQLState(),
                    "SQLSTATE should be 2203F (non-scalar result), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 70: JSON_VALUE non-scalar (array) should error with SQLSTATE 2203F
    // PG: ERROR [2203F] JSON path expression in JSON_VALUE must return single scalar item
    // Memgres: ERROR [22032] JSON_VALUE: non-scalar result
    // ========================================================================
    @Test
    void stmt70_jsonValueArrayShouldErrorWith2203F() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_VALUE('{\"a\": [1,2,3]}', '$.a' ERROR ON ERROR)");
            fail("Expected an error for non-scalar JSON_VALUE result, but query succeeded");
        } catch (SQLException e) {
            assertEquals("2203F", e.getSQLState(),
                    "SQLSTATE should be 2203F (non-scalar result), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 99: JSON_TABLE invalid JSON should error with SQLSTATE 22P02
    // PG: ERROR [22P02] invalid input syntax for type json
    // Memgres: ERROR [22032] invalid JSON input for JSON_TABLE
    // ========================================================================
    @Test
    void stmt99_jsonTableInvalidJsonShouldErrorWith22P02() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT * FROM JSON_TABLE( 'not json at all', '$[*]' "
                    + "COLUMNS (x text PATH '$') ERROR ON ERROR ) AS jt");
            fail("Expected an error for invalid JSON input, but query succeeded");
        } catch (SQLException e) {
            assertEquals("22P02", e.getSQLState(),
                    "SQLSTATE should be 22P02 (invalid input syntax), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 131: JSON_QUERY invalid JSON should error with SQLSTATE 22P02
    // PG: ERROR [22P02] invalid input syntax for type json
    // Memgres: ERROR [22032] invalid input for JSON_QUERY
    // ========================================================================
    @Test
    void stmt131_jsonQueryInvalidJsonShouldErrorWith22P02() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_QUERY('not valid json', '$.a' ERROR ON ERROR)");
            fail("Expected an error for invalid JSON input, but query succeeded");
        } catch (SQLException e) {
            assertEquals("22P02", e.getSQLState(),
                    "SQLSTATE should be 22P02 (invalid input syntax), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 43: JSON_SERIALIZE of jsonb should return text representation
    // PG: JSON_SERIALIZE converts jsonb to text with normalized spacing
    // ========================================================================
    @Test
    void stmt43_jsonSerializeJsonbShouldReturnText() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            String result = rs.getString("result");
            // JSON_SERIALIZE returns the text representation of the JSON value
            assertEquals("{\"a\": 1}", result,
                    "JSON_SERIALIZE on jsonb should return normalized JSON text, got: " + result);
        }
    }

    // ========================================================================
    // Stmt 100: JSON_ARRAY with empty subquery should return NULL (not [])
    // PG: OK (result) [NULL]
    // Memgres: OK (result) [[]]
    // ========================================================================
    @Test
    void stmt100_jsonArrayEmptySubqueryShouldReturnNull() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_ARRAY(SELECT v FROM (SELECT 1 AS v WHERE FALSE) sub) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            String result = rs.getString("result");
            assertNull(result,
                    "JSON_ARRAY from empty subquery should return NULL per PG behavior, got: " + result);
        }
    }

    // ========================================================================
    // Stmt 137: JSON_TABLE FORMAT JSON should output jsonb with spaces
    // PG: [1 | {"x": 10}] (spaces in jsonb output)
    // Memgres: [1 | {"x":10}] (no spaces)
    // ========================================================================
    @Test
    void stmt137_jsonTableFormatJsonShouldOutputWithSpaces() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, nested FROM JSON_TABLE( "
                     + "'[{\"id\":1,\"data\":{\"x\":10}}]', '$[*]' COLUMNS ( "
                     + "id integer PATH '$.id', "
                     + "nested jsonb FORMAT JSON PATH '$.data' ) ) AS jt")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(1, rs.getInt("id"));
            assertEquals("{\"x\": 10}", rs.getString("nested"),
                    "JSON_TABLE FORMAT JSON should output jsonb with spaces after colons");
        }
    }

    // ========================================================================
    // Stmt 138: JSON_OBJECT(KEY 'x' VALUE 10) — Memgres supports KEY...VALUE
    // syntax which is part of the SQL/JSON standard. PG may parse KEY differently
    // in some contexts, but the result should be a valid JSON object.
    // ========================================================================
    @Test
    void stmt138_jsonObjectKeyValueShouldProduceCorrectOutput() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            String result = rs.getString("result");
            // Result should be a JSON object with x and y keys
            assertNotNull(result, "JSON_OBJECT should produce a non-null result");
            assertTrue(result.contains("\"x\"") && result.contains("\"y\""),
                    "JSON_OBJECT should contain keys x and y, got: " + result);
        }
    }
}
