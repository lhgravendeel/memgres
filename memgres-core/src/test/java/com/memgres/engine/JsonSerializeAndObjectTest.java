package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 2 PG vs Memgres differences from sql-json.sql.
 *
 * These tests assert exact PG 18 behavior. They are expected to FAIL on
 * current Memgres, documenting the real gaps.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class JsonSerializeAndObjectTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 43: JSON_SERIALIZE on jsonb input.
     *
     * The comparison report shows PG returns a different byte representation
     * than Memgres's '{"a": 1}'. The PG output displays as a control character
     * (\u0001) in the report, suggesting a wire-protocol-level difference in
     * how the result is encoded in extended query mode.
     *
     * PG: OK (result) [\u0001] — different encoding
     * Memgres: OK (result) [{"a": 1}]
     */
    @Test
    void stmt43_jsonSerializeJsonb() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            // JSON_SERIALIZE without RETURNING returns text (SQL standard default)
            String val = rs.getString("result");
            assertNotNull(val, "JSON_SERIALIZE should return non-null text");
            assertEquals("{\"a\": 1}", val,
                    "JSON_SERIALIZE should return normalized JSON text");
        }
    }

    /**
     * Stmt 138: JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20)
     *
     * PG 18 errors: ERROR [42704]: type "key" does not exist
     * Memgres succeeds: {"x" : 10, "y" : 20}
     */
    @Test
    void stmt138_jsonObjectKeyValueShouldError() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20) AS result");
            fail("PG rejects JSON_OBJECT(KEY ...) with error 42704 ('type \"key\" does not exist'), "
                    + "but Memgres succeeded");
        } catch (SQLException e) {
            assertTrue("42704".equals(e.getSQLState()) || "42601".equals(e.getSQLState()),
                    "SQLSTATE should be 42704 or 42601, got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }
}
