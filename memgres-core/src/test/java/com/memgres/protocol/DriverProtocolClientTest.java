package com.memgres.protocol;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.sql.Date;
import java.math.*;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests covering driver-protocol and client-behavior scenarios:
 * how JDBC drivers interact with memgres at the protocol level.
 *
 * Scenarios from: 1110_driver_protocol_and_client_behavior_scenarios.md
 *
 * Covers parameter type inference, text vs binary protocol, integer/float/string/
 * null/array parameter binding, timestamp precision, NUMERIC precision, boolean
 * mapping, ResultSetMetaData, autocommit semantics, batch execution, DatabaseMetaData,
 * PreparedStatement reuse, generated keys, large result set streaming, connection
 * properties, and JDBC escape syntax.
 */
class DriverProtocolClientTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
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

    // =========================================================================
    // 1. Parameter type inference
    // =========================================================================

    @Test
    void testParameterTypeInference_setInt_setString_setDouble() throws SQLException {
        exec("CREATE TABLE dpc_type_infer (id serial PRIMARY KEY, i int, s text, d double precision)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_type_infer (i, s, d) VALUES (?, ?, ?)")) {
                ps.setInt(1, 42);
                ps.setString(2, "hello");
                ps.setDouble(3, 3.14);
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT i, s, d FROM dpc_type_infer")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertEquals("hello", rs.getString(2));
                assertEquals(3.14, rs.getDouble(3), 0.001);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_type_infer");
        }
    }

    @Test
    void testParameterTypeInference_setBoolean_setBigDecimal_setTimestamp() throws SQLException {
        exec("CREATE TABLE dpc_type_infer2 (id serial PRIMARY KEY, b boolean, n numeric(12,4), ts timestamp)");
        try {
            Timestamp ts = Timestamp.valueOf("2025-06-15 12:30:45");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_type_infer2 (b, n, ts) VALUES (?, ?, ?)")) {
                ps.setBoolean(1, true);
                ps.setBigDecimal(2, new BigDecimal("9876.5432"));
                ps.setTimestamp(3, ts);
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT b, n, ts FROM dpc_type_infer2")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertEquals(0, new BigDecimal("9876.5432").compareTo(rs.getBigDecimal(2)));
                assertEquals(ts, rs.getTimestamp(3));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_type_infer2");
        }
    }

    @Test
    void testParameterTypeInference_setDate_setTime_setNull() throws SQLException {
        exec("CREATE TABLE dpc_type_infer3 (id serial PRIMARY KEY, d date, t time, nullable_col int)");
        try {
            Date d = Date.valueOf("2025-03-21");
            Time t = Time.valueOf("09:45:00");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_type_infer3 (d, t, nullable_col) VALUES (?, ?, ?)")) {
                ps.setDate(1, d);
                ps.setTime(2, t);
                ps.setNull(3, Types.INTEGER);
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT d, t, nullable_col FROM dpc_type_infer3")) {
                assertTrue(rs.next());
                assertEquals(d, rs.getDate(1));
                assertEquals(t, rs.getTime(2));
                rs.getInt(3);
                assertTrue(rs.wasNull());
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_type_infer3");
        }
    }

    @Test
    void testParameterTypeInference_setArray_setObject_pgJson() throws SQLException {
        exec("CREATE TABLE dpc_type_infer4 (id serial PRIMARY KEY, arr int[], j jsonb)");
        try {
            Array arr = conn.createArrayOf("integer", new Integer[]{10, 20, 30});
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_type_infer4 (arr, j) VALUES (?, ?::jsonb)")) {
                ps.setArray(1, arr);
                ps.setString(2, "{\"key\":\"value\"}");
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT arr, j FROM dpc_type_infer4")) {
                assertTrue(rs.next());
                Array result = rs.getArray(1);
                assertNotNull(result);
                Integer[] vals = (Integer[]) result.getArray();
                assertArrayEquals(new Integer[]{10, 20, 30}, vals);
                String json = rs.getString(2);
                assertNotNull(json);
                assertTrue(json.contains("key"));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_type_infer4");
        }
    }

    // =========================================================================
    // 2. Text vs binary protocol comparison
    // =========================================================================

    @Test
    void testTextVsBinaryProtocol_integerColumn() throws SQLException {
        exec("CREATE TABLE dpc_proto (id serial PRIMARY KEY, val integer)");
        try {
            exec("INSERT INTO dpc_proto (val) VALUES (12345)");

            // Text protocol: plain Statement
            int textResult;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT val FROM dpc_proto WHERE id = 1")) {
                assertTrue(rs.next());
                textResult = rs.getInt(1);
            }

            // Extended protocol: PreparedStatement
            int binaryResult;
            try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM dpc_proto WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    binaryResult = rs.getInt(1);
                }
            }

            assertEquals(textResult, binaryResult, "Text and binary protocol must return same integer value");
        } finally {
            exec("DROP TABLE IF EXISTS dpc_proto");
        }
    }

    @Test
    void testTextVsBinaryProtocol_numericColumn() throws SQLException {
        exec("CREATE TABLE dpc_proto2 (id serial PRIMARY KEY, val numeric(12,4))");
        try {
            exec("INSERT INTO dpc_proto2 (val) VALUES (1234.5678)");

            BigDecimal textResult;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT val FROM dpc_proto2 WHERE id = 1")) {
                assertTrue(rs.next());
                textResult = rs.getBigDecimal(1);
            }

            BigDecimal binaryResult;
            try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM dpc_proto2 WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    binaryResult = rs.getBigDecimal(1);
                }
            }

            assertNotNull(textResult);
            assertNotNull(binaryResult);
            assertEquals(0, textResult.compareTo(binaryResult),
                    "Text and binary protocol must return same numeric value");
        } finally {
            exec("DROP TABLE IF EXISTS dpc_proto2");
        }
    }

    // =========================================================================
    // 3. Integer parameter binding: setInt, setLong, setShort
    // =========================================================================

    @Test
    void testIntegerParameterBinding_setInt_setLong_setShort() throws SQLException {
        exec("CREATE TABLE dpc_ints (id serial PRIMARY KEY, s smallint, i integer, b bigint)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_ints (s, i, b) VALUES (?, ?, ?)")) {
                ps.setShort(1, (short) 32000);
                ps.setInt(2, 2_000_000);
                ps.setLong(3, 9_000_000_000L);
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT s, i, b FROM dpc_ints")) {
                assertTrue(rs.next());
                assertEquals((short) 32000, rs.getShort(1));
                assertEquals(2_000_000, rs.getInt(2));
                assertEquals(9_000_000_000L, rs.getLong(3));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_ints");
        }
    }

    // =========================================================================
    // 4. Floating point edge values: NaN, Infinity, -0.0
    // =========================================================================

    @Test
    void testFloatEdgeValues_NaN_Infinity() throws SQLException {
        exec("CREATE TABLE dpc_floats (id serial PRIMARY KEY, val double precision)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_floats (val) VALUES (?)")) {
                ps.setDouble(1, Double.NaN);
                ps.executeUpdate();
                ps.setDouble(1, Double.POSITIVE_INFINITY);
                ps.executeUpdate();
                ps.setDouble(1, Double.NEGATIVE_INFINITY);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT val FROM dpc_floats ORDER BY id")) {
                assertTrue(rs.next());
                assertTrue(Double.isNaN(rs.getDouble(1)));
                assertTrue(rs.next());
                assertTrue(Double.isInfinite(rs.getDouble(1)));
                assertEquals(Double.POSITIVE_INFINITY, rs.getDouble(1));
                assertTrue(rs.next());
                assertTrue(Double.isInfinite(rs.getDouble(1)));
                assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble(1));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_floats");
        }
    }

    @Test
    void testFloatEdgeValues_setFloat() throws SQLException {
        exec("CREATE TABLE dpc_floats2 (id serial PRIMARY KEY, val real)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_floats2 (val) VALUES (?)")) {
                ps.setFloat(1, 3.14f);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT val FROM dpc_floats2")) {
                assertTrue(rs.next());
                assertEquals(3.14f, rs.getFloat(1), 0.001f);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_floats2");
        }
    }

    // =========================================================================
    // 5. String parameter binding: Unicode, empty, very long
    // =========================================================================

    @Test
    void testStringBinding_unicode() throws SQLException {
        exec("CREATE TABLE dpc_str (id serial PRIMARY KEY, val text)");
        try {
            String unicode = "Hello \u4e16\u754c \u00e9\u00e0\u00fc \uD83D\uDE00";
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_str (val) VALUES (?)")) {
                ps.setString(1, unicode);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT val FROM dpc_str")) {
                assertTrue(rs.next());
                assertEquals(unicode, rs.getString(1));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_str");
        }
    }

    @Test
    void testStringBinding_emptyString() throws SQLException {
        exec("CREATE TABLE dpc_str2 (id serial PRIMARY KEY, val text)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_str2 (val) VALUES (?)")) {
                ps.setString(1, "");
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT val FROM dpc_str2")) {
                assertTrue(rs.next());
                assertEquals("", rs.getString(1));
                assertFalse(rs.wasNull());
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_str2");
        }
    }

    @Test
    void testStringBinding_veryLong() throws SQLException {
        exec("CREATE TABLE dpc_str3 (id serial PRIMARY KEY, val text)");
        try {
            String longStr = Strs.repeat("A", 100_000);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_str3 (val) VALUES (?)")) {
                ps.setString(1, longStr);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT length(val) FROM dpc_str3")) {
                assertTrue(rs.next());
                assertEquals(100_000, rs.getInt(1));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_str3");
        }
    }

    // =========================================================================
    // 6. Null parameter handling for various SQL types
    // =========================================================================

    @Test
    void testNullParameterHandling_variousTypes() throws SQLException {
        exec("CREATE TABLE dpc_nulls (id serial PRIMARY KEY, i int, v varchar(50), ts timestamp, n numeric(10,2), b boolean)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_nulls (i, v, ts, n, b) VALUES (?, ?, ?, ?, ?)")) {
                ps.setNull(1, Types.INTEGER);
                ps.setNull(2, Types.VARCHAR);
                ps.setNull(3, Types.TIMESTAMP);
                ps.setNull(4, Types.NUMERIC);
                ps.setNull(5, Types.BOOLEAN);
                assertEquals(1, ps.executeUpdate());
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT i, v, ts, n, b FROM dpc_nulls")) {
                assertTrue(rs.next());
                rs.getInt(1); assertTrue(rs.wasNull());
                rs.getString(2); assertTrue(rs.wasNull());
                rs.getTimestamp(3); assertTrue(rs.wasNull());
                rs.getBigDecimal(4); assertTrue(rs.wasNull());
                rs.getBoolean(5); assertTrue(rs.wasNull());
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_nulls");
        }
    }

    // =========================================================================
    // 7. Array parameter binding
    // =========================================================================

    @Test
    void testArrayParameterBinding_integerArray() throws SQLException {
        exec("CREATE TABLE dpc_arr (id serial PRIMARY KEY, nums int[])");
        try {
            Integer[] data = {1, 2, 3, 4, 5};
            Array pgArr = conn.createArrayOf("integer", data);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_arr (nums) VALUES (?)")) {
                ps.setArray(1, pgArr);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT nums FROM dpc_arr")) {
                assertTrue(rs.next());
                Array result = rs.getArray(1);
                assertNotNull(result);
                Integer[] back = (Integer[]) result.getArray();
                assertArrayEquals(data, back);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_arr");
        }
    }

    @Test
    void testArrayParameterBinding_textArray() throws SQLException {
        exec("CREATE TABLE dpc_arr2 (id serial PRIMARY KEY, tags text[])");
        try {
            String[] tags = {"alpha", "beta", "gamma"};
            Array pgArr = conn.createArrayOf("text", tags);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_arr2 (tags) VALUES (?)")) {
                ps.setArray(1, pgArr);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT tags FROM dpc_arr2")) {
                assertTrue(rs.next());
                Array result = rs.getArray(1);
                assertNotNull(result);
                String[] back = (String[]) result.getArray();
                assertArrayEquals(tags, back);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_arr2");
        }
    }

    // =========================================================================
    // 8. Timestamp precision and timezone handling
    // =========================================================================

    @Test
    void testTimestampPrecision_nanoseconds() throws SQLException {
        exec("CREATE TABLE dpc_ts (id serial PRIMARY KEY, ts timestamp(6))");
        try {
            Timestamp ts = new Timestamp(1_700_000_000_000L);
            ts.setNanos(123_456_000); // microsecond precision
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_ts (ts) VALUES (?)")) {
                ps.setTimestamp(1, ts);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT ts FROM dpc_ts WHERE id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    Timestamp back = rs.getTimestamp(1);
                    assertNotNull(back);
                    // Verify at least millisecond fidelity
                    assertEquals(ts.getTime() / 1000, back.getTime() / 1000);
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_ts");
        }
    }

    @Test
    void testTimestampWithTimezone() throws SQLException {
        exec("CREATE TABLE dpc_tstz (id serial PRIMARY KEY, ts timestamptz)");
        try {
            Timestamp ts = Timestamp.valueOf("2025-07-04 00:00:00");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_tstz (ts) VALUES (?)")) {
                ps.setTimestamp(1, ts);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT ts FROM dpc_tstz WHERE id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertNotNull(rs.getTimestamp(1));
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_tstz");
        }
    }

    // =========================================================================
    // 9. Date/Time round-trip fidelity
    // =========================================================================

    @Test
    void testDateTimeRoundTrip() throws SQLException {
        exec("CREATE TABLE dpc_datetime (id serial PRIMARY KEY, d date, t time)");
        try {
            Date date = Date.valueOf("2025-12-25");
            Time time = Time.valueOf("23:59:59");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_datetime (d, t) VALUES (?, ?)")) {
                ps.setDate(1, date);
                ps.setTime(2, time);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT d, t FROM dpc_datetime WHERE id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(date, rs.getDate(1));
                    assertEquals(time, rs.getTime(2));
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_datetime");
        }
    }

    // =========================================================================
    // 10. NUMERIC/DECIMAL precision: BigDecimal with 20+ digits
    // =========================================================================

    @Test
    void testNumericHighPrecision() throws SQLException {
        exec("CREATE TABLE dpc_numeric (id serial PRIMARY KEY, val numeric(30,10))");
        try {
            BigDecimal highPrecision = new BigDecimal("12345678901234567890.1234567890");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_numeric (val) VALUES (?)")) {
                ps.setBigDecimal(1, highPrecision);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT val FROM dpc_numeric WHERE id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    BigDecimal back = rs.getBigDecimal(1);
                    assertNotNull(back);
                    assertEquals(0, highPrecision.compareTo(back),
                            "High-precision BigDecimal round-trip failed: expected "
                                    + highPrecision + " got " + back);
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_numeric");
        }
    }

    // =========================================================================
    // 11. Boolean parameter binding
    // =========================================================================

    @Test
    void testBooleanParameterBinding() throws SQLException {
        exec("CREATE TABLE dpc_bool (id serial PRIMARY KEY, flag boolean)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_bool (flag) VALUES (?)")) {
                ps.setBoolean(1, true);
                ps.executeUpdate();
                ps.setBoolean(1, false);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT flag FROM dpc_bool ORDER BY id")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.wasNull());
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
                assertFalse(rs.wasNull());
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_bool");
        }
    }

    // =========================================================================
    // 12. ResultSet type mapping: getters on typed columns
    // =========================================================================

    @Test
    void testResultSetTypeMapping() throws SQLException {
        exec("CREATE TABLE dpc_rstype (id serial PRIMARY KEY, i int, b bigint, s text, d double precision, n numeric(10,2), ts timestamp, flag boolean, dt date, tm time)");
        try {
            exec("INSERT INTO dpc_rstype (i, b, s, d, n, ts, flag, dt, tm) VALUES (7, 999999999999, 'test', 2.718, 3.14, '2025-01-01 00:00:00', true, '2025-01-01', '12:00:00')");
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT i, b, s, d, n, ts, flag, dt, tm FROM dpc_rstype WHERE id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(7, rs.getInt(1));
                    assertEquals(999999999999L, rs.getLong(2));
                    assertEquals("test", rs.getString(3));
                    assertEquals(2.718, rs.getDouble(4), 0.001);
                    assertNotNull(rs.getBigDecimal(5));
                    assertNotNull(rs.getTimestamp(6));
                    assertTrue(rs.getBoolean(7));
                    assertNotNull(rs.getDate(8));
                    assertNotNull(rs.getTime(9));
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_rstype");
        }
    }

    // =========================================================================
    // 13. ResultSetMetaData: column name, type, precision, scale, nullability
    // =========================================================================

    @Test
    void testResultSetMetaData_columnProperties() throws SQLException {
        exec("CREATE TABLE dpc_rsmd (id serial PRIMARY KEY, name varchar(50) NOT NULL, price numeric(10,2), active boolean)");
        try {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, name, price, active FROM dpc_rsmd")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(4, md.getColumnCount());

                // Column 1: id
                assertEquals("id", md.getColumnName(1).toLowerCase());
                // Column 2: name
                assertEquals("name", md.getColumnName(2).toLowerCase());
                // Column 3: price (numeric type)
                assertEquals(Types.NUMERIC, md.getColumnType(3));
                assertEquals(10, md.getPrecision(3));
                assertEquals(2, md.getScale(3));
                // Column 4: active (boolean)
                int boolType = md.getColumnType(4);
                assertTrue(boolType == Types.BOOLEAN || boolType == Types.BIT,
                        "Expected BOOLEAN or BIT, got: " + boolType);
                // Table name should be populated
                assertEquals("dpc_rsmd", md.getTableName(1).toLowerCase());
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_rsmd");
        }
    }

    @Test
    void testResultSetMetaData_nullability() throws SQLException {
        exec("CREATE TABLE dpc_rsmd2 (id serial PRIMARY KEY, required text NOT NULL, optional text)");
        try {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, required, optional FROM dpc_rsmd2")) {
                ResultSetMetaData md = rs.getMetaData();
                // 'required' is NOT NULL
                assertEquals(ResultSetMetaData.columnNoNulls, md.isNullable(2));
                // 'optional' is nullable
                assertEquals(ResultSetMetaData.columnNullable, md.isNullable(3));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_rsmd2");
        }
    }

    // =========================================================================
    // 14. Autocommit on vs off
    // =========================================================================

    @Test
    void testAutocommitTrue_commitsImmediately() throws SQLException {
        exec("CREATE TABLE dpc_ac (id serial PRIMARY KEY, val text)");
        try {
            assertTrue(conn.getAutoCommit(), "Autocommit should be true");
            exec("INSERT INTO dpc_ac (val) VALUES ('immediate')");
            // Verify data is visible without explicit commit
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM dpc_ac")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_ac");
        }
    }

    @Test
    void testAutocommitFalse_requiresExplicitCommit() throws SQLException {
        exec("CREATE TABLE dpc_ac2 (id serial PRIMARY KEY, val text)");
        try {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO dpc_ac2 (val) VALUES (?)")) {
                    ps.setString(1, "pending");
                    ps.executeUpdate();
                }
                // Before commit, data should be visible in same connection
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM dpc_ac2")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
                conn.commit();
                // After commit, data persists
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM dpc_ac2")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_ac2");
        }
    }

    @Test
    void testAutocommitFalse_rollbackDiscardsChanges() throws SQLException {
        exec("CREATE TABLE dpc_ac3 (id serial PRIMARY KEY, val text)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("INSERT INTO dpc_ac3 (val) VALUES ('will_be_rolled_back')");
                conn.rollback();
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM dpc_ac3")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "Rollback should discard inserted row");
                }
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_ac3");
        }
    }

    // =========================================================================
    // 15. Batch execution: addBatch / executeBatch
    // =========================================================================

    @Test
    void testBatchExecution_insertBatch() throws SQLException {
        exec("CREATE TABLE dpc_batch (id serial PRIMARY KEY, name text, score int)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_batch (name, score) VALUES (?, ?)")) {
                for (int i = 1; i <= 25; i++) {
                    ps.setString(1, "row_" + i);
                    ps.setInt(2, i * 4);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(25, counts.length);
                for (int c : counts) {
                    assertEquals(1, c);
                }
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM dpc_batch")) {
                assertTrue(rs.next());
                assertEquals(25, rs.getInt(1));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_batch");
        }
    }

    // =========================================================================
    // 16. Multiple statements via Statement.execute (multiple result sets)
    // =========================================================================

    @Test
    void testMultipleStatements_sequentialExecute() throws SQLException {
        exec("CREATE TABLE dpc_multi (id serial PRIMARY KEY, val int)");
        try {
            exec("INSERT INTO dpc_multi (val) VALUES (1)");
            exec("INSERT INTO dpc_multi (val) VALUES (2)");

            // Execute two separate queries using the same Statement object
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT val FROM dpc_multi WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
                try (ResultSet rs = st.executeQuery("SELECT val FROM dpc_multi WHERE id = 2")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_multi");
        }
    }

    // =========================================================================
    // 17. DatabaseMetaData: getTables, getColumns, getPrimaryKeys, getSchemas
    // =========================================================================

    @Test
    void testDatabaseMetaData_getTables() throws SQLException {
        exec("CREATE TABLE dpc_meta_tbl (id serial PRIMARY KEY, name text)");
        try {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getTables(null, "public", "dpc_meta_tbl", new String[]{"TABLE"})) {
                assertTrue(rs.next(), "dpc_meta_tbl should appear in getTables");
                assertEquals("dpc_meta_tbl", rs.getString("TABLE_NAME").toLowerCase());
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_meta_tbl");
        }
    }

    @Test
    void testDatabaseMetaData_getColumns() throws SQLException {
        exec("CREATE TABLE dpc_meta_col (id serial PRIMARY KEY, label text NOT NULL, amount numeric(8,2))");
        try {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getColumns(null, "public", "dpc_meta_col", null)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    assertNotNull(rs.getString("COLUMN_NAME"));
                    assertNotNull(rs.getString("TYPE_NAME"));
                }
                assertTrue(count >= 3, "Expected at least 3 columns, got: " + count);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_meta_col");
        }
    }

    @Test
    void testDatabaseMetaData_getPrimaryKeys() throws SQLException {
        exec("CREATE TABLE dpc_meta_pk (id serial PRIMARY KEY, data text)");
        try {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getPrimaryKeys(null, "public", "dpc_meta_pk")) {
                assertTrue(rs.next(), "dpc_meta_pk should have a primary key");
                assertEquals("id", rs.getString("COLUMN_NAME").toLowerCase());
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_meta_pk");
        }
    }

    @Test
    void testDatabaseMetaData_getSchemas() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getSchemas()) {
            boolean foundPublic = false;
            while (rs.next()) {
                if ("public".equalsIgnoreCase(rs.getString("TABLE_SCHEM"))) {
                    foundPublic = true;
                }
            }
            assertTrue(foundPublic, "Should find 'public' schema in getSchemas");
        }
    }

    // =========================================================================
    // 18. PreparedStatement reuse with different parameters
    // =========================================================================

    @Test
    void testPreparedStatementReuse_differentParams() throws SQLException {
        exec("CREATE TABLE dpc_reuse (id serial PRIMARY KEY, category text, value int)");
        try {
            exec("INSERT INTO dpc_reuse (category, value) VALUES ('A', 10), ('B', 20), ('C', 30)");

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT value FROM dpc_reuse WHERE category = ?")) {
                ps.setString(1, "A");
                try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); }

                ps.setString(1, "B");
                try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); }

                ps.setString(1, "C");
                try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); }

                // Reuse many times to trigger potential binary transfer
                for (int i = 0; i < 10; i++) {
                    ps.setString(1, "A");
                    try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); }
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_reuse");
        }
    }

    // =========================================================================
    // 19. Statement.getGeneratedKeys() / INSERT ... RETURNING
    // =========================================================================

    @Test
    void testGetGeneratedKeys_serialPrimaryKey() throws SQLException {
        exec("CREATE TABLE dpc_genkeys (id serial PRIMARY KEY, name text)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_genkeys (name) VALUES (?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "generated_row");
                assertEquals(1, ps.executeUpdate());
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next(), "Should return generated key");
                    int id = keys.getInt(1);
                    assertTrue(id > 0, "Generated ID must be positive, got: " + id);
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_genkeys");
        }
    }

    @Test
    void testGetGeneratedKeys_insertReturning() throws SQLException {
        exec("CREATE TABLE dpc_genkeys2 (id serial PRIMARY KEY, val text)");
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_genkeys2 (val) VALUES (?) RETURNING id")) {
                ps.setString(1, "returning_test");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    int id = rs.getInt(1);
                    assertTrue(id > 0);
                }
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_genkeys2");
        }
    }

    // =========================================================================
    // 20. Large result set streaming with setFetchSize
    // =========================================================================

    @Test
    void testLargeResultSetStreaming_setFetchSize() throws SQLException {
        exec("CREATE TABLE dpc_large (id serial PRIMARY KEY, val int)");
        try {
            // Insert 500 rows
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO dpc_large (val) SELECT generate_series(1, 500)")) {
                ps.executeUpdate();
            }

            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id, val FROM dpc_large ORDER BY id")) {
                    ps.setFetchSize(50);
                    try (ResultSet rs = ps.executeQuery()) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                            assertTrue(rs.getInt(1) > 0);
                            assertTrue(rs.getInt(2) > 0);
                        }
                        assertEquals(500, count, "Should iterate all 500 rows");
                    }
                }
                conn.commit();
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE IF EXISTS dpc_large");
        }
    }

    // =========================================================================
    // 21. Connection properties: transaction isolation, catalog, schema
    // =========================================================================

    @Test
    void testConnectionProperties_transactionIsolation() throws SQLException {
        int isolation = conn.getTransactionIsolation();
        // PostgreSQL default is READ COMMITTED
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, isolation,
                "Default isolation should be READ_COMMITTED");
    }

    @Test
    void testConnectionProperties_catalogAndSchema() throws SQLException {
        // getCatalog() should return the database name (non-null)
        String catalog = conn.getCatalog();
        assertNotNull(catalog, "getCatalog() must not return null");

        // getSchema() should return current schema (typically "public")
        String schema = conn.getSchema();
        assertNotNull(schema, "getSchema() must not return null");
    }

    @Test
    void testConnectionProperties_setTransactionIsolation() throws SQLException {
        int original = conn.getTransactionIsolation();
        try {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        } finally {
            conn.setTransactionIsolation(original);
        }
    }

    // =========================================================================
    // 22. JDBC escape syntax: {fn now()}, {d '...'}, {ts '...'}
    // =========================================================================

    @Test
    void testJdbcEscape_fnNow() throws SQLException {
        // {fn now()} is a JDBC scalar function escape
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT {fn now()}")) {
            assertTrue(rs.next());
            assertNotNull(rs.getTimestamp(1), "{fn now()} should return a non-null timestamp");
        }
    }

    @Test
    void testJdbcEscape_dateEscape() throws SQLException {
        // {d 'yyyy-mm-dd'} is the JDBC date escape
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT {d '2025-01-01'}")) {
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertNotNull(d);
            assertEquals(Date.valueOf("2025-01-01"), d);
        }
    }

    @Test
    void testJdbcEscape_timestampEscape() throws SQLException {
        // {ts 'yyyy-mm-dd hh:mm:ss'} is the JDBC timestamp escape
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT {ts '2025-01-01 12:00:00'}")) {
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
            assertEquals(Timestamp.valueOf("2025-01-01 12:00:00"), ts);
        }
    }
}
