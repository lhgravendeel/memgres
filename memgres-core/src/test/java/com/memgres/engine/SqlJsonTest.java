package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 28 Memgres-vs-Annotation failures from sql-json.sql.
 *
 * These tests document cases where Memgres behavior diverges from PG 18 expected
 * behavior as annotated in the sql-json.sql feature comparison file. Each test
 * asserts what PG 18 would produce so that when Memgres is fixed, the test will pass.
 */
class SqlJsonTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS sj_test CASCADE");
            s.execute("CREATE SCHEMA sj_test");
            s.execute("SET search_path = sj_test, public");

            // Core test table
            s.execute("CREATE TABLE sj_data (id integer PRIMARY KEY, doc jsonb)");
            s.execute("INSERT INTO sj_data VALUES "
                    + "(1, '{\"name\": \"Alice\", \"age\": 30, \"tags\": [\"admin\", \"user\"]}'), "
                    + "(2, '{\"name\": \"Bob\", \"age\": 25, \"tags\": [\"user\"]}'), "
                    + "(3, '{\"name\": \"Charlie\", \"age\": 35, \"active\": true}'), "
                    + "(4, NULL)");

            // Empty table for COALESCE/JSON_ARRAYAGG test (Stmt 95)
            s.execute("CREATE TABLE sj_empty (v text)");

            // Settings table for JSON_OBJECTAGG ORDER BY test (Stmt 88)
            s.execute("CREATE TABLE sj_settings (section text, key text, val text)");
            s.execute("INSERT INTO sj_settings VALUES "
                    + "('db', 'host', 'localhost'), ('db', 'port', '5432'), "
                    + "('app', 'name', 'myapp'), ('app', 'debug', 'true')");

            // Users table for LEFT JOIN JSON_TABLE test (Stmt 115)
            s.execute("CREATE TABLE sj_users (id integer PRIMARY KEY, name text)");
            s.execute("INSERT INTO sj_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS sj_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 18: JSON_EXISTS with TRUE ON ERROR on invalid JSON should error
    // PG: ERROR [22P02] invalid input syntax for type json
    // Memgres: succeeds with [t]
    // ========================================================================
    @Test
    void stmt18_jsonExistsTrueOnErrorInvalidJsonShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_EXISTS('not json', '$.a' TRUE ON ERROR) AS result");
            fail("Expected an error for invalid JSON input, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("invalid input syntax"),
                    "Error should mention 'invalid input syntax', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 19: JSON_EXISTS with FALSE ON ERROR on invalid JSON should error
    // PG: ERROR [22P02] invalid input syntax for type json
    // Memgres: succeeds with [f]
    // ========================================================================
    @Test
    void stmt19_jsonExistsFalseOnErrorInvalidJsonShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_EXISTS('not json', '$.a' FALSE ON ERROR) AS result");
            fail("Expected an error for invalid JSON input, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("invalid input syntax"),
                    "Error should mention 'invalid input syntax', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 25: JSON_VALUE with DEFAULT ON ERROR on invalid JSON should error
    // PG: ERROR [22P02] invalid input syntax for type json
    // Memgres: succeeds with NULL
    // ========================================================================
    @Test
    void stmt25_jsonValueDefaultOnErrorInvalidJsonShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_VALUE('not json', '$.a' DEFAULT 'error-fallback' ON ERROR) AS result");
            fail("Expected an error for invalid JSON input, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("invalid input syntax"),
                    "Error should mention 'invalid input syntax', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 26: JSON_VALUE with ERROR ON EMPTY for missing path should error
    // PG: ERROR [22035] no SQL/JSON item found for specified path
    // Memgres: succeeds with NULL
    // ========================================================================
    @Test
    void stmt26_jsonValueErrorOnEmptyMissingPathShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_VALUE('{\"a\":1}', '$.missing' ERROR ON EMPTY)");
            fail("Expected an error for missing JSON path with ERROR ON EMPTY, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("no sql/json item"),
                    "Error should mention 'no SQL/JSON item', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 28: JSON_QUERY extract object should have spaces in output
    // PG: {"b": 1}
    // Memgres: {"b":1}
    // ========================================================================
    @Test
    void stmt28_jsonQueryExtractObjectShouldHaveSpaces() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("{\"b\": 1}", rs.getString("result"),
                    "JSON_QUERY should return formatted JSON with spaces after colons");
        }
    }

    // ========================================================================
    // Stmt 29: JSON_QUERY extract array should have spaces
    // PG: [1, 2, 3]
    // Memgres: [1,2,3]
    // ========================================================================
    @Test
    void stmt29_jsonQueryExtractArrayShouldHaveSpaces() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_QUERY('{\"arr\":[1,2,3]}', '$.arr') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("[1, 2, 3]", rs.getString("result"),
                    "JSON_QUERY should return array with spaces after commas");
        }
    }

    // ========================================================================
    // Stmt 31: JSON_QUERY WITHOUT WRAPPER should have spaces
    // PG: [1, 2, 3]
    // Memgres: [1,2,3]
    // ========================================================================
    @Test
    void stmt31_jsonQueryWithoutWrapperShouldHaveSpaces() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_QUERY('{\"arr\":[1,2,3]}', '$.arr' WITHOUT WRAPPER) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("[1, 2, 3]", rs.getString("result"),
                    "JSON_QUERY WITHOUT WRAPPER should return array with spaces");
        }
    }

    // ========================================================================
    // Stmt 33: JSON_QUERY OMIT QUOTES on string scalar should return NULL
    // PG: NULL
    // Memgres: hello
    // ========================================================================
    @Test
    void stmt33_jsonQueryOmitQuotesOnStringShouldReturnNull() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_QUERY('{\"a\":\"hello\"}', '$.a' OMIT QUOTES) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertNull(rs.getString("result"),
                    "JSON_QUERY OMIT QUOTES on a string scalar should return NULL per PG behavior");
        }
    }

    // ========================================================================
    // Stmt 43: JSON_SERIALIZE on jsonb should return formatted text
    // PG 17+: JSON_SERIALIZE converts jsonb to text with normalized formatting
    // ========================================================================
    @Test
    void stmt43_jsonSerializeJsonbReturnsFormattedText() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            // JSON_SERIALIZE without RETURNING returns text (SQL standard default)
            String val = rs.getString("result");
            assertNotNull(val, "JSON_SERIALIZE should return non-null text");
            assertEquals("{\"a\": 1}", val,
                    "JSON_SERIALIZE should return formatted JSON text");
        }
    }

    // ========================================================================
    // Stmt 48: JSON_OBJECTAGG should aggregate into single row
    // PG: 1 row with has_keys = true
    // Memgres: 3 rows with NULL values
    // ========================================================================
    @Test
    void stmt48_jsonObjectAggShouldReturnSingleRow() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_OBJECTAGG( JSON_VALUE(doc::text, '$.name') : "
                     + "JSON_VALUE(doc::text, '$.age' RETURNING integer) ) IS JSON OBJECT AS has_keys "
                     + "FROM sj_data WHERE doc IS NOT NULL")) {
            assertTrue(rs.next(), "Expected at least one row");
            assertTrue(rs.getBoolean("has_keys"),
                    "JSON_OBJECTAGG result should be a valid JSON object (has_keys = true)");
            assertFalse(rs.next(),
                    "JSON_OBJECTAGG should aggregate into exactly one row, not multiple");
        }
    }

    // ========================================================================
    // Stmt 49: JSON_OBJECTAGG ABSENT ON NULL formatting
    // PG: { "a" : 1 }
    // Memgres: {"a": 1}
    // ========================================================================
    @Test
    void stmt49_jsonObjectAggAbsentOnNullFormatting() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_OBJECTAGG(k : v ABSENT ON NULL) AS result "
                     + "FROM (VALUES ('a', 1), ('b', NULL::integer)) AS t(k, v)")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("{ \"a\" : 1 }", rs.getString("result"),
                    "JSON_OBJECTAGG ABSENT ON NULL should use PG formatting with spaces around colons");
        }
    }

    // ========================================================================
    // Stmt 63: JSON_TABLE with invalid path should error
    // PG: ERROR [42601] syntax error at or near "["
    // Memgres: succeeds with [{"a":1}]
    // ========================================================================
    @Test
    void stmt63_jsonTableInvalidPathShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT * FROM JSON_TABLE('{\"a\":1}', '$.[[invalid' "
                    + "COLUMNS (x text PATH '$') ERROR ON ERROR) jt");
            fail("Expected a syntax error for invalid JSON path, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("json")
                    || e.getMessage().toLowerCase().contains("syntax error"),
                    "Error should mention JSON or syntax error, got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 71: JSON_QUERY CONDITIONAL WRAPPER on scalar should not wrap
    // PG: 42
    // Memgres: [42]
    // ========================================================================
    @Test
    void stmt71_jsonQueryConditionalWrapperScalarShouldNotWrap() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_QUERY('{\"a\": 42}', '$.a' WITH CONDITIONAL WRAPPER) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("42", rs.getString("result"),
                    "JSON_QUERY WITH CONDITIONAL WRAPPER on scalar should return unwrapped value");
        }
    }

    // ========================================================================
    // Stmt 72: JSON_QUERY CONDITIONAL WRAPPER on array should have spaces
    // PG: [1, 2, 3]
    // Memgres: [1,2,3]
    // ========================================================================
    @Test
    void stmt72_jsonQueryConditionalWrapperArrayShouldHaveSpaces() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_QUERY('{\"a\": [1,2,3]}', '$.a' WITH CONDITIONAL WRAPPER) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("[1, 2, 3]", rs.getString("result"),
                    "JSON_QUERY WITH CONDITIONAL WRAPPER on array should return spaced array");
        }
    }

    // ========================================================================
    // Stmt 74: JSON_TABLE nested NESTED PATH should produce 3 rows
    // PG: 3 rows: [eng|backend|Alice], [eng|backend|Bob], [eng|frontend|Carol]
    // Memgres: 2 rows with NULL members
    // ========================================================================
    @Test
    void stmt74_jsonTableNestedPathShouldProduceThreeRows() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT dept, team, member FROM JSON_TABLE( "
                     + "'[{\"dept\":\"eng\",\"teams\":[{\"team\":\"backend\",\"members\":[\"Alice\",\"Bob\"]},"
                     + "{\"team\":\"frontend\",\"members\":[\"Carol\"]}]}]', "
                     + "'$[*]' COLUMNS ( "
                     + "dept text PATH '$.dept', "
                     + "NESTED PATH '$.teams[*]' COLUMNS ( "
                     + "team text PATH '$.team', "
                     + "NESTED PATH '$.members[*]' COLUMNS ( "
                     + "member text PATH '$' "
                     + ") ) ) ) AS jt")) {

            assertTrue(rs.next(), "Expected row 1");
            assertEquals("eng", rs.getString("dept"));
            assertEquals("backend", rs.getString("team"));
            assertEquals("Alice", rs.getString("member"));

            assertTrue(rs.next(), "Expected row 2");
            assertEquals("eng", rs.getString("dept"));
            assertEquals("backend", rs.getString("team"));
            assertEquals("Bob", rs.getString("member"));

            assertTrue(rs.next(), "Expected row 3");
            assertEquals("eng", rs.getString("dept"));
            assertEquals("frontend", rs.getString("team"));
            assertEquals("Carol", rs.getString("member"));

            assertFalse(rs.next(), "Expected exactly 3 rows");
        }
    }

    // ========================================================================
    // Stmt 77: JSON_TABLE with $.* wildcard should produce 3 rows
    // PG: cnt = 3
    // Memgres: cnt = 0
    // ========================================================================
    @Test
    void stmt77_jsonTableWildcardShouldProduceThreeRows() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::integer AS cnt FROM ( "
                     + "SELECT JSON_VALUE(val, '$') FROM JSON_TABLE( "
                     + "'{\"a\":1,\"b\":2,\"c\":3}', '$.*' COLUMNS (val text PATH '$') "
                     + ") jt ) sub")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(3, rs.getInt("cnt"),
                    "JSON_TABLE with $.* wildcard path should produce 3 rows for 3 keys");
        }
    }

    // ========================================================================
    // Stmt 78: JSON_EXISTS with recursive descent $..name should error
    // PG: ERROR [42601] syntax error at or near "."
    // Memgres: succeeds with [f]
    // ========================================================================
    @Test
    void stmt78_jsonExistsRecursiveDescentShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_EXISTS('{\"a\":{\"b\":{\"name\":\"deep\"}}}', '$..name') AS found");
            fail("Expected a syntax error for recursive descent path $..name, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("syntax error"),
                    "Error should mention 'syntax error', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 82: JSON_EXISTS with PASSING variables should find match
    // PG: true
    // Memgres: false
    // ========================================================================
    @Test
    void stmt82_jsonExistsWithPassingVariablesShouldReturnTrue() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_EXISTS( "
                     + "'{\"items\":[{\"price\":5},{\"price\":15},{\"price\":25}]}', "
                     + "'$.items[*] ? (@.price > $lo && @.price < $hi)' PASSING 10 AS lo, 20 AS hi "
                     + ") AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("result"),
                    "JSON_EXISTS with PASSING lo=10 hi=20 should find price=15 (true)");
        }
    }

    // ========================================================================
    // Stmt 88: JSON_OBJECTAGG with ORDER BY should error with syntax error
    // PG: ERROR [42601] syntax error
    // Memgres: ERROR [42601] but message says "Unexpected token" not "syntax error"
    // ========================================================================
    @Test
    void stmt88_jsonObjectAggOrderByErrorMessageShouldContainSyntaxError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT section, JSON_OBJECTAGG(key : val ORDER BY key) AS settings "
                    + "FROM sj_settings GROUP BY section ORDER BY section");
            fail("Expected a syntax error for JSON_OBJECTAGG with ORDER BY, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("syntax error"),
                    "Error message should contain 'syntax error', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 95: COALESCE(JSON_ARRAYAGG(v), '[]'::jsonb) should error on type mismatch
    // PG: ERROR [42846] could not convert type jsonb to json
    // Memgres: succeeds with NULL
    // ========================================================================
    @Test
    void stmt95_coalesceJsonArrayaggJsonbShouldErrorOnTypeMismatch() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT COALESCE(JSON_ARRAYAGG(v), '[]'::jsonb) AS result FROM sj_empty");
            fail("Expected a type conversion error, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("could not convert"),
                    "Error should mention 'could not convert', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 100: JSON_ARRAY from empty subquery should return NULL
    // PG 17+: JSON_ARRAY(SELECT ...) on empty result returns NULL
    // ========================================================================
    @Test
    void stmt100_jsonArrayEmptySubqueryShouldReturnNull() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_ARRAY(SELECT v FROM (SELECT 1 AS v WHERE FALSE) sub) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertNull(rs.getString("result"),
                    "JSON_ARRAY from empty subquery should return NULL per PG behavior");
        }
    }

    // ========================================================================
    // Stmt 104: IS JSON on non-JSON strings should all return false
    // PG: f, f, f, f
    // Memgres: f, f, t, t (incorrectly accepts '{key: value}' and 'NaN')
    // ========================================================================
    @Test
    void stmt104_isJsonOnNonJsonStringsShouldAllReturnFalse() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT '' IS JSON AS r1, 'undefined' IS JSON AS r2, "
                     + "'{key: value}' IS JSON AS r3, 'NaN' IS JSON AS r4")) {
            assertTrue(rs.next(), "Expected one result row");
            assertFalse(rs.getBoolean("r1"), "Empty string IS JSON should be false");
            assertFalse(rs.getBoolean("r2"), "'undefined' IS JSON should be false");
            assertFalse(rs.getBoolean("r3"), "'{key: value}' IS JSON should be false (not valid JSON)");
            assertFalse(rs.getBoolean("r4"), "'NaN' IS JSON should be false (not valid JSON)");
        }
    }

    // ========================================================================
    // Stmt 106: JSON_QUERY OMIT QUOTES WITH WRAPPER should error with syntax error
    // PG: ERROR [42601] syntax error
    // Memgres: ERROR [42601] but message says "Expected RIGHT_PAREN" not "syntax error"
    // ========================================================================
    @Test
    void stmt106_jsonQueryOmitQuotesWithWrapperErrorShouldContainSyntaxError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_QUERY('{\"a\": 42}', '$.a' OMIT QUOTES WITH WRAPPER) AS result");
            fail("Expected a syntax error for OMIT QUOTES WITH WRAPPER combination, but query succeeded");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("syntax error"),
                    "Error message should contain 'syntax error', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 115: JSON_TABLE LEFT JOIN should resolve jt.score column
    // PG: [Alice|100], [Bob|200], [Charlie|NULL]
    // Memgres: ERROR column jt.score does not exist
    // ========================================================================
    @Test
    void stmt115_jsonTableLeftJoinShouldResolveColumns() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT u.name, jt.score FROM sj_users u "
                     + "LEFT JOIN JSON_TABLE( "
                     + "'[{\"id\":1,\"score\":100},{\"id\":2,\"score\":200}]', "
                     + "'$[*]' COLUMNS ( "
                     + "id integer PATH '$.id', "
                     + "score integer PATH '$.score' "
                     + ") ) AS jt ON u.id = jt.id "
                     + "ORDER BY u.id")) {

            assertTrue(rs.next(), "Expected row 1");
            assertEquals("Alice", rs.getString("name"));
            assertEquals(100, rs.getInt("score"));

            assertTrue(rs.next(), "Expected row 2");
            assertEquals("Bob", rs.getString("name"));
            assertEquals(200, rs.getInt("score"));

            assertTrue(rs.next(), "Expected row 3");
            assertEquals("Charlie", rs.getString("name"));
            assertEquals(0, rs.getInt("score"));
            assertTrue(rs.wasNull(), "Charlie's score should be NULL");

            assertFalse(rs.next(), "Expected exactly 3 rows");
        }
    }

    // ========================================================================
    // Stmt 137: JSON_TABLE with FORMAT JSON on column should work
    // PG: [1, {"x": 10}]
    // Memgres: ERROR Expected keyword COLUMNS near 'FORMAT'
    // ========================================================================
    @Test
    void stmt137_jsonTableFormatJsonColumnShouldWork() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, nested FROM JSON_TABLE( "
                     + "'[{\"id\":1,\"data\":{\"x\":10}}]', "
                     + "'$[*]' COLUMNS ( "
                     + "id integer PATH '$.id', "
                     + "nested jsonb FORMAT JSON PATH '$.data' "
                     + ") ) AS jt")) {

            assertTrue(rs.next(), "Expected one result row");
            assertEquals(1, rs.getInt("id"));
            String nested = rs.getString("nested");
            assertNotNull(nested, "nested column should not be null");
            assertTrue(nested.contains("\"x\"") && nested.contains("10"),
                    "nested should contain {\"x\": 10}, got: " + nested);
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    // ========================================================================
    // Stmt 138: JSON_OBJECT with KEY...VALUE syntax — PG 18 rejects this.
    // KEY is parsed as a type name, producing error 42704: type "key" does not exist.
    // ========================================================================
    @Test
    void stmt138_jsonObjectKeyValueSyntaxShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery(
                    "SELECT JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20) AS result");
            fail("PG 18 rejects JSON_OBJECT(KEY ...) with 42704");
        } catch (SQLException e) {
            assertEquals("42704", e.getSQLState(),
                    "Expected 42704, got: " + e.getSQLState());
        }
    }

    // ========================================================================
    // Stmt 155: sj_generated table (generated column with JSON_VALUE) should exist
    // PG: [1|Alice], [2|Bob]
    // Memgres: ERROR relation "sj_generated" does not exist
    // ========================================================================
    @Test
    void stmt155_generatedColumnTableShouldExistAndWork() throws Exception {
        // First create the table with the generated column
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS sj_generated ( "
                    + "id integer PRIMARY KEY, "
                    + "doc jsonb, "
                    + "extracted_name text GENERATED ALWAYS AS (JSON_VALUE(doc, '$.name' RETURNING text)) STORED "
                    + ")");
            s.execute("DELETE FROM sj_generated");
            s.execute("INSERT INTO sj_generated (id, doc) VALUES "
                    + "(1, '{\"name\": \"Alice\"}'), "
                    + "(2, '{\"name\": \"Bob\"}')");
        } catch (SQLException e) {
            fail("Creating sj_generated table with GENERATED ALWAYS column should succeed: " + e.getMessage());
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, extracted_name FROM sj_generated ORDER BY id")) {

            assertTrue(rs.next(), "Expected row 1");
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("extracted_name"));

            assertTrue(rs.next(), "Expected row 2");
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("extracted_name"));

            assertFalse(rs.next(), "Expected exactly 2 rows");
        }
    }

    // ========================================================================
    // Stmt 157: Update generated column should reflect new value
    // PG: extracted_name = Alicia after update
    // Memgres: ERROR relation "sj_generated" does not exist
    // ========================================================================
    @Test
    void stmt157_generatedColumnShouldUpdateOnDocChange() throws Exception {
        // Ensure the table exists with data
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS sj_generated ( "
                    + "id integer PRIMARY KEY, "
                    + "doc jsonb, "
                    + "extracted_name text GENERATED ALWAYS AS (JSON_VALUE(doc, '$.name' RETURNING text)) STORED "
                    + ")");
            s.execute("DELETE FROM sj_generated");
            s.execute("INSERT INTO sj_generated (id, doc) VALUES "
                    + "(1, '{\"name\": \"Alice\"}'), "
                    + "(2, '{\"name\": \"Bob\"}')");
        } catch (SQLException e) {
            fail("Creating sj_generated table should succeed: " + e.getMessage());
        }

        try (Statement s = conn.createStatement()) {
            s.execute("UPDATE sj_generated SET doc = '{\"name\": \"Alicia\"}' WHERE id = 1");
        } catch (SQLException e) {
            fail("UPDATE on table with generated column should succeed: " + e.getMessage());
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT extracted_name FROM sj_generated WHERE id = 1")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("Alicia", rs.getString("extracted_name"),
                    "Generated column should auto-update to 'Alicia' after doc change");
        }
    }
}
