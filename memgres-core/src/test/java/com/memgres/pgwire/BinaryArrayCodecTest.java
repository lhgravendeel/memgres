package com.memgres.pgwire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bug 1 (binary arrays): PgWireBinaryCodec#writeBinaryValue has no array cases, so array
 * values fall through to the default branch and get written as TEXT bytes into a field
 * that the DataRow flagged as binary (format=1). pgjdbc then tries to decode the raw
 * ASCII text (e.g. "{10,20,30}") as the PG binary array wire format, which corrupts the
 * dimension/OID header and throws (or, worse, allocates a huge array and OOMs).
 *
 * These tests force binary result-format via a reused PreparedStatement (adaptive binary
 * transfer kicks in once the driver has re-executed the same statement prepareThreshold
 * times), matching the pattern used by Round13BinaryCodecGapsTest / AdaptiveBinaryTransferTest.
 */
class BinaryArrayCodecTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?binaryTransfer=true&prepareThreshold=1",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private Array readArrayViaReusedPreparedStatement(String sql, int iterations) throws SQLException {
        Array last = null;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < iterations; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    last = rs.getArray(1);
                }
            }
        }
        return last;
    }

    // =========================================================================
    // int4[] column
    // =========================================================================

    @Test
    void int4Array_columnRoundTrip_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_int4");
        exec("CREATE TABLE bac_int4 (id int PRIMARY KEY, nums int[])");
        exec("INSERT INTO bac_int4 VALUES (1, ARRAY[10,20,30])");

        Array result = readArrayViaReusedPreparedStatement(
                "SELECT nums FROM bac_int4 WHERE id = 1", 10);
        assertNotNull(result);
        Integer[] vals = (Integer[]) result.getArray();
        assertArrayEquals(new Integer[]{10, 20, 30}, vals);
    }

    @Test
    void int4Array_withNullElement_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_int4_null");
        exec("CREATE TABLE bac_int4_null (id int PRIMARY KEY, nums int[])");
        exec("INSERT INTO bac_int4_null VALUES (1, ARRAY[1,NULL,3])");

        Array result = readArrayViaReusedPreparedStatement(
                "SELECT nums FROM bac_int4_null WHERE id = 1", 10);
        assertNotNull(result);
        Integer[] vals = (Integer[]) result.getArray();
        assertArrayEquals(new Integer[]{1, null, 3}, vals);
    }

    @Test
    void int4Array_empty_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_int4_empty");
        exec("CREATE TABLE bac_int4_empty (id int PRIMARY KEY, nums int[])");
        exec("INSERT INTO bac_int4_empty VALUES (1, ARRAY[]::int[])");

        Array result = readArrayViaReusedPreparedStatement(
                "SELECT nums FROM bac_int4_empty WHERE id = 1", 10);
        assertNotNull(result);
        Integer[] vals = (Integer[]) result.getArray();
        assertEquals(0, vals.length);
    }

    @Test
    void int4Array_nullField_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_int4_nullfield");
        exec("CREATE TABLE bac_int4_nullfield (id int PRIMARY KEY, nums int[])");
        exec("INSERT INTO bac_int4_nullfield VALUES (1, NULL)");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT nums FROM bac_int4_nullfield WHERE id = 1")) {
            for (int i = 0; i < 10; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    Array result = rs.getArray(1);
                    assertNull(result, "array field should be SQL NULL on iteration " + i);
                    assertTrue(rs.wasNull());
                }
            }
        }
    }

    // =========================================================================
    // text[] column
    // =========================================================================

    @Test
    void textArray_columnRoundTrip_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_text");
        exec("CREATE TABLE bac_text (id int PRIMARY KEY, tags text[])");
        exec("INSERT INTO bac_text VALUES (1, ARRAY['alpha','beta','gamma'])");

        Array result = readArrayViaReusedPreparedStatement(
                "SELECT tags FROM bac_text WHERE id = 1", 10);
        assertNotNull(result);
        String[] vals = (String[]) result.getArray();
        assertArrayEquals(new String[]{"alpha", "beta", "gamma"}, vals);
    }

    @Test
    void textArray_withNullElement_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_text_null");
        exec("CREATE TABLE bac_text_null (id int PRIMARY KEY, tags text[])");
        exec("INSERT INTO bac_text_null VALUES (1, ARRAY['a',NULL,'c'])");

        Array result = readArrayViaReusedPreparedStatement(
                "SELECT tags FROM bac_text_null WHERE id = 1", 10);
        assertNotNull(result);
        String[] vals = (String[]) result.getArray();
        assertArrayEquals(new String[]{"a", null, "c"}, vals);
    }

    @Test
    void textArray_empty_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS bac_text_empty");
        exec("CREATE TABLE bac_text_empty (id int PRIMARY KEY, tags text[])");
        exec("INSERT INTO bac_text_empty VALUES (1, ARRAY[]::text[])");

        Array result = readArrayViaReusedPreparedStatement(
                "SELECT tags FROM bac_text_empty WHERE id = 1", 10);
        assertNotNull(result);
        String[] vals = (String[]) result.getArray();
        assertEquals(0, vals.length);
    }

    // =========================================================================
    // Array-returning expressions (no table column involved)
    // =========================================================================

    // Note: a bare, uncast "SELECT ARRAY[100,200,300]" is described (Parse/Describe, before
    // execution) with a TEXT column type rather than an array type - a separate, pre-existing
    // Describe-time static-type-inference gap for uncast array literals (not a binary-codec
    // issue: pg_typeof() on the *executed* result correctly reports "integer[]", but Describe
    // needs a static type before execution runs). That gap lives in expression type inference /
    // Describe helpers, not in PgWireBinaryCodec, so it's out of scope here. Casting the
    // expression (as real queries typically do, e.g. array_agg(...) or an explicit ::type[])
    // gives Describe an unambiguous array OID and exercises the same writeBinaryValue array
    // path as a table column.

    @Test
    void int4Array_castExpression_binary() throws SQLException {
        Array result = readArrayViaReusedPreparedStatement(
                "SELECT ARRAY[100,200,300]::int[]", 10);
        assertNotNull(result);
        Integer[] vals = (Integer[]) result.getArray();
        assertArrayEquals(new Integer[]{100, 200, 300}, vals);
    }

    @Test
    void textArray_castExpression_binary() throws SQLException {
        Array result = readArrayViaReusedPreparedStatement(
                "SELECT ARRAY['x','y']::text[]", 10);
        assertNotNull(result);
        String[] vals = (String[]) result.getArray();
        assertArrayEquals(new String[]{"x", "y"}, vals);
    }

    @Test
    void int4Array_arrayAggExpression_binary() throws SQLException {
        Array result = readArrayViaReusedPreparedStatement(
                "SELECT array_agg(x) FROM (VALUES (1),(2),(3)) t(x)", 10);
        assertNotNull(result);
        Integer[] vals = (Integer[]) result.getArray();
        assertArrayEquals(new Integer[]{1, 2, 3}, vals);
    }
}
