package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for round-4 differences fixes.
 *
 * 1. FOREACH SLICE 0 should be allowed (only SLICE N>0 rejected)
 * 2. JSON_SERIALIZE bytea wire encoding (getString should decode properly)
 * 3. RETURNS TABLE with BEGIN ATOMIC function body
 * 4. INOUT multi-value BEGIN ATOMIC function body
 * 5. DROP TABLE CASCADE drops dependent BEGIN ATOMIC functions
 */
class ForeachSliceAndFunctionBodyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // 1. FOREACH SLICE 0 should be allowed
    // ========================================================================

    @Test
    void foreachSlice0Allowed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION test_foreach_slice0() RETURNS text LANGUAGE plpgsql AS $$ "
                    + "DECLARE arr integer[] := ARRAY[1, 2, 3]; elem integer; result text := ''; "
                    + "BEGIN FOREACH elem SLICE 0 IN ARRAY arr LOOP result := result || elem::text || ','; END LOOP; RETURN result; END; $$");
            try (ResultSet rs = s.executeQuery("SELECT test_foreach_slice0()")) {
                assertTrue(rs.next());
                assertEquals("1,2,3,", rs.getString(1), "SLICE 0 should iterate individual elements");
            }
            s.execute("DROP FUNCTION test_foreach_slice0()");
        }
    }

    @Test
    void foreachSlice1Allowed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION test_fs1() RETURNS text LANGUAGE plpgsql AS $$ "
                    + "DECLARE arr integer[] := ARRAY[[1,2],[3,4]]; slice integer[]; result text := ''; "
                    + "BEGIN FOREACH slice SLICE 1 IN ARRAY arr LOOP result := result || array_to_string(slice, ',') || ';'; END LOOP; RETURN result; END; $$");
            try (ResultSet rs = s.executeQuery("SELECT test_fs1()")) {
                assertTrue(rs.next());
                assertEquals("1,2;3,4;", rs.getString(1), "SLICE 1 should iterate over 1D sub-arrays");
            }
            s.execute("DROP FUNCTION test_fs1()");
        }
    }

    // ========================================================================
    // 2. JSON_SERIALIZE bytea — getString should return decoded JSON text
    // ========================================================================

    @Test
    void jsonSerializeByteaGetStringReturnsDecodedText() throws SQLException {
        // JSON_SERIALIZE without RETURNING returns text (SQL standard default)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertEquals("{\"a\": 1}", val, "JSON_SERIALIZE should return normalized JSON text");
        }
    }

    // ========================================================================
    // 3. RETURNS TABLE with BEGIN ATOMIC
    // ========================================================================

    @Test
    void returnsTableBeginAtomicExpandsColumns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r4_people (name text, age integer)");
            s.execute("INSERT INTO r4_people VALUES ('Alice', 30), ('Carol', 35), ('Bob', 25)");
            s.execute("CREATE FUNCTION r4_adults() RETURNS TABLE(name text, age integer) "
                    + "LANGUAGE sql BEGIN ATOMIC "
                    + "SELECT name, age FROM r4_people WHERE age >= 30 ORDER BY name; "
                    + "END");

            try (ResultSet rs = s.executeQuery("SELECT * FROM r4_adults()")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount(), "RETURNS TABLE should expand to 2 columns");
                assertEquals("name", md.getColumnName(1).toLowerCase(), "First column should be 'name'");
                assertEquals("age", md.getColumnName(2).toLowerCase(), "Second column should be 'age'");

                assertTrue(rs.next());
                assertEquals("Alice", rs.getString(1));
                assertEquals(30, rs.getInt(2));

                assertTrue(rs.next());
                assertEquals("Carol", rs.getString(1));
                assertEquals(35, rs.getInt(2));

                assertFalse(rs.next(), "Should only have 2 rows");
            }
            s.execute("DROP FUNCTION r4_adults()");
            s.execute("DROP TABLE r4_people");
        }
    }

    // ========================================================================
    // 4. INOUT multi-value BEGIN ATOMIC
    // ========================================================================

    @Test
    void inoutMultiValueBeginAtomic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION r4_swap(INOUT a integer, INOUT b integer) "
                    + "LANGUAGE sql BEGIN ATOMIC SELECT b, a; END");

            try (ResultSet rs = s.executeQuery("SELECT * FROM r4_swap(10, 20)")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount(), "INOUT swap should return 2 columns");

                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1), "First value should be swapped to 20");
                assertEquals(10, rs.getInt(2), "Second value should be swapped to 10");
            }
            s.execute("DROP FUNCTION r4_swap(integer, integer)");
        }
    }

    // ========================================================================
    // 5. DROP TABLE CASCADE drops dependent BEGIN ATOMIC functions
    // ========================================================================

    @Test
    void dropTableCascadeDropsDependentFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r4_dep_table (id integer PRIMARY KEY, val text)");
            s.execute("CREATE FUNCTION r4_dep_func() RETURNS SETOF text "
                    + "LANGUAGE sql STABLE BEGIN ATOMIC SELECT val FROM r4_dep_table; END");

            // Function should exist
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer FROM pg_proc WHERE proname = 'r4_dep_func'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Function should exist before CASCADE drop");
            }

            s.execute("DROP TABLE r4_dep_table CASCADE");

            // Function should be gone after CASCADE
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer FROM pg_proc WHERE proname = 'r4_dep_func'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Function should be dropped by CASCADE");
            }
        }
    }
}
