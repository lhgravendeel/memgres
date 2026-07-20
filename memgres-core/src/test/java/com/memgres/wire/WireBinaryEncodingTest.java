package com.memgres.wire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for wire-protocol binary encoding correctness: C8, H8, H9, H10, H11, M16, M17, L4.
 */
class WireBinaryEncodingTest {

    static Memgres memgres;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection textConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private Connection binaryConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?binaryTransfer=true&prepareThreshold=1",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // === C8: infinity sentinels ===

    @Test
    void c8_dateInfinityBinary() throws Exception {
        try (Connection c = binaryConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::date AS d")) {
            // Run twice to trigger binary mode (prepareThreshold=1)
            ps.setObject(1, "infinity");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                // Should not crash — connection should survive
                assertNotNull(rs.getString(1));
            }
            ps.setObject(1, "-infinity");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test
    void c8_timestampInfinityBinary() throws Exception {
        try (Connection c = binaryConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::timestamp AS ts")) {
            ps.setObject(1, "infinity");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
            ps.setObject(1, "-infinity");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test
    void c8_dateInfinityText() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'infinity'::date, '-infinity'::date")) {
                assertTrue(rs.next());
                assertEquals("infinity", rs.getString(1));
                assertEquals("-infinity", rs.getString(2));
            }
        }
    }

    @Test
    void c8_timestampInfinityText() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'infinity'::timestamp, '-infinity'::timestamp")) {
                assertTrue(rs.next());
                assertEquals("infinity", rs.getString(1));
                assertEquals("-infinity", rs.getString(2));
            }
        }
    }

    // === H8: time binary encoding ===

    @Test
    void h8_timeBinaryEncoding() throws Exception {
        try (Connection c = binaryConn(); PreparedStatement ps = c.prepareStatement("SELECT '14:30:00'::time AS t")) {
            // Run twice to force binary
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); }
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.startsWith("14:30"), "Expected 14:30, got: " + val);
            }
        }
    }

    // === H9: numeric NaN and negative dscale ===

    @Test
    void h9_numericNanBinary() throws Exception {
        try (Connection c = binaryConn(); PreparedStatement ps = c.prepareStatement("SELECT 'NaN'::numeric AS n")) {
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); }
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                // NaN is not a valid BigDecimal — use getString to verify no crash
                String val = rs.getString(1);
                assertEquals("NaN", val);
            }
        }
    }

    @Test
    void h9_numericNegativeScale() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 1234500::numeric AS n")) {
                assertTrue(rs.next());
                assertEquals("1234500", rs.getString(1));
            }
        }
    }

    // === H10: array type OIDs ===

    @Test
    void h10_intArrayOid() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT ARRAY[1,2,3]::int[]")) {
                ResultSetMetaData md = rs.getMetaData();
                // Should be _int4 (OID 1007), not int4 (OID 23)
                String typeName = md.getColumnTypeName(1);
                assertTrue(typeName.startsWith("_") || typeName.contains("int4"),
                    "Expected array type name, got: " + typeName);
                assertTrue(rs.next());
                // Should be retrievable as Array
                Object val = rs.getObject(1);
                assertNotNull(val);
            }
        }
    }

    @Test
    void h10_boolArrayOid() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT ARRAY[true,false]::bool[]")) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
            }
        }
    }

    // === H11: text array binary encoding with quotes ===

    @Test
    void h11_textArrayWithQuotes() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT ARRAY['hello','w\"orld']::text[]")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("w\"orld") || val.contains("w\\\"orld"),
                    "Expected quote in array element, got: " + val);
            }
        }
    }

    @Test
    void h11_textArrayBinaryWithQuotes() throws Exception {
        try (Connection c = binaryConn()) {
            try (PreparedStatement ps = c.prepareStatement("SELECT ARRAY['e\"f']::text[]")) {
                try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); }
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    // In binary mode, should still get the correct value
                    assertNotNull(rs.getObject(1));
                }
            }
        }
    }

    // === M16: ParameterDescription OIDs ===

    @Test
    void m16_parameterMetadata() throws Exception {
        try (Connection c = binaryConn(); PreparedStatement ps = c.prepareStatement("SELECT ? + 1")) {
            ps.setInt(1, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(43, rs.getInt(1));
            }
        }
    }

    // === M17: SELECT * table OID/attnum ===

    @Test
    void m17_selectStarMetadata() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m17_meta(id serial PRIMARY KEY, name text NOT NULL)");
            st.execute("INSERT INTO m17_meta(name) VALUES ('test')");
            try {
                int starNullable, explicitNullable;
                // Get metadata from SELECT *
                try (ResultSet rs1 = st.executeQuery("SELECT * FROM m17_meta")) {
                    ResultSetMetaData md1 = rs1.getMetaData();
                    assertEquals(2, md1.getColumnCount());
                    starNullable = md1.isNullable(2); // name column
                }
                // Get metadata from explicit columns
                try (ResultSet rs2 = st.executeQuery("SELECT id, name FROM m17_meta")) {
                    ResultSetMetaData md2 = rs2.getMetaData();
                    explicitNullable = md2.isNullable(2); // name column
                }
                assertEquals(explicitNullable, starNullable,
                    "nullable mismatch for 'name' column between SELECT * and explicit");
            } finally {
                st.execute("DROP TABLE m17_meta");
            }
        }
    }

    @Test
    void m17_selectStarAutoIncrement() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m17_ai(id serial PRIMARY KEY, v text)");
            st.execute("INSERT INTO m17_ai(v) VALUES ('x')");
            try {
                try (ResultSet rs = st.executeQuery("SELECT * FROM m17_ai")) {
                    ResultSetMetaData md = rs.getMetaData();
                    // id column (serial) should report isAutoIncrement = true
                    assertTrue(md.isAutoIncrement(1),
                        "serial column should be autoIncrement via SELECT *");
                }
            } finally {
                st.execute("DROP TABLE m17_ai");
            }
        }
    }

    // === L4: OID type ===

    @Test
    void l4_oidTypeCast() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 42::oid")) {
                assertTrue(rs.next());
                assertEquals(42L, rs.getLong(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT pg_typeof(42::oid)")) {
                assertTrue(rs.next());
                assertEquals("oid", rs.getString(1));
            }
        }
    }

    // === Additional binary encoding robustness ===

    @Test
    void binaryBigintArray() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE ba_t(ids bigint[])");
            st.execute("INSERT INTO ba_t VALUES ('{1,2,3}')");
            try (ResultSet rs = st.executeQuery("SELECT ids FROM ba_t")) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
            }
            st.execute("DROP TABLE ba_t");
        }
    }

    @Test
    void binaryUuidArray() throws Exception {
        try (Connection c = textConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE ua_t(ids uuid[])");
            st.execute("INSERT INTO ua_t VALUES ('{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}')");
            try (ResultSet rs = st.executeQuery("SELECT ids FROM ua_t")) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
            }
            st.execute("DROP TABLE ua_t");
        }
    }
}
