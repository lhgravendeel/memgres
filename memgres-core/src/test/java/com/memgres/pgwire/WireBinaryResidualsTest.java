package com.memgres.pgwire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual wire/binary-protocol bugs from bugs-review.md (H9, H10, C8, M16).
 *
 * <p>These are binary-protocol behaviors that the SQL-verification harness cannot express (it
 * exercises only text-mode results), so they are covered here as unit tests. The connection uses
 * {@code binaryTransfer=true&prepareThreshold=1} and every query is executed several times through a
 * reused {@link PreparedStatement} so pgjdbc's adaptive binary transfer flips the field to binary
 * format — the exact condition under which the residuals surfaced. Expected values are pinned to
 * real PostgreSQL 18 observed via the same driver settings.
 */
class WireBinaryResidualsTest {

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

    /** Run a single-column query {@code iterations} times (forcing binary) and return the last row's object. */
    private Object readObject(String sql, int iterations) throws SQLException {
        Object last = null;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < iterations; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    last = rs.getObject(1);
                }
            }
        }
        return last;
    }

    private String readString(String sql, int iterations) throws SQLException {
        String last = null;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < iterations; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    last = rs.getString(1);
                }
            }
        }
        return last;
    }

    private Object[] readArray(String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            Object[] out = null;
            for (int i = 0; i < 8; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    Object o = rs.getObject(1);
                    assertTrue(o instanceof java.sql.Array,
                            "column should decode as java.sql.Array in binary mode, got "
                                    + (o == null ? "null" : o.getClass().getName()));
                    out = (Object[]) ((Array) o).getArray();
                }
            }
            return out;
        }
    }

    // =====================================================================
    // H9 — binary numeric: negative dscale (silent data corruption) + NaN
    // =====================================================================

    @Test
    void h9_numericNegativeScale_notTruncated() throws SQLException {
        // Was silently returning 12345 (off by 100x) in binary mode.
        Object o = readObject("SELECT 1234500::numeric(10,-2)", 8);
        assertTrue(o instanceof BigDecimal, "expected BigDecimal, got " + (o == null ? "null" : o.getClass()));
        assertEquals(0, ((BigDecimal) o).compareTo(new BigDecimal("1234500")));
        assertEquals("1234500", readString("SELECT 1234500::numeric(10,-2)", 8));
    }

    @Test
    void h9_numericNegativeScale_variousMagnitudes() throws SQLException {
        assertEquals(0, ((BigDecimal) readObject("SELECT 987600::numeric(10,-2)", 8)).compareTo(new BigDecimal("987600")));
        assertEquals(0, ((BigDecimal) readObject("SELECT 12000::numeric(10,-3)", 8)).compareTo(new BigDecimal("12000")));
        assertEquals(0, ((BigDecimal) readObject("SELECT (-45600)::numeric(10,-2)", 8)).compareTo(new BigDecimal("-45600")));
    }

    @Test
    void h9_numericNaN_stillWorks() throws SQLException {
        Object o = readObject("SELECT 'NaN'::numeric", 8);
        assertTrue((o instanceof Double && ((Double) o).isNaN())
                        || (o instanceof BigDecimal),
                "NaN numeric should decode without a client-side exception, got " + o);
        assertEquals("NaN", readString("SELECT 'NaN'::numeric", 8));
    }

    @Test
    void h9_numericNormalScale_regression() throws SQLException {
        assertEquals(0, ((BigDecimal) readObject("SELECT 1234.56::numeric(10,2)", 8)).compareTo(new BigDecimal("1234.56")));
        assertEquals(0, ((BigDecimal) readObject("SELECT 0.00123::numeric", 8)).compareTo(new BigDecimal("0.00123")));
        assertEquals(0, ((BigDecimal) readObject("SELECT 0::numeric", 8)).compareTo(BigDecimal.ZERO));
    }

    // =====================================================================
    // H10 — binary array encoders for the full standard element set
    // =====================================================================

    private void createArrayTable() throws SQLException {
        exec("DROP TABLE IF EXISTS wbr_arr");
        exec("CREATE TABLE wbr_arr (id int primary key, a int2[], b int8[], c float4[], d float8[], " +
                "e varchar[], f bool[], g date[], h timestamp[], i uuid[], j numeric[], k jsonb[])");
        exec("INSERT INTO wbr_arr VALUES (1, ARRAY[1,2]::int2[], ARRAY[10,20]::int8[], ARRAY[1.5,2.5]::float4[], " +
                "ARRAY[1.25,2.25]::float8[], ARRAY['x','y'], ARRAY[true,false], ARRAY['2020-01-02'::date], " +
                "ARRAY['2020-01-02 03:04:05'::timestamp], ARRAY['11111111-1111-1111-1111-111111111111'::uuid], " +
                "ARRAY[1.5,2.25]::numeric[], ARRAY['{\"k\":1}'::jsonb])");
    }

    @Test
    void h10_int2Array_binary() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT a FROM wbr_arr WHERE id=1");
        assertArrayEquals(new Short[]{(short) 1, (short) 2}, v);
    }

    @Test
    void h10_int8Array_binary() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT b FROM wbr_arr WHERE id=1");
        assertArrayEquals(new Long[]{10L, 20L}, v);
    }

    @Test
    void h10_float4Array_binary() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT c FROM wbr_arr WHERE id=1");
        assertArrayEquals(new Float[]{1.5f, 2.5f}, v);
    }

    @Test
    void h10_float8Array_binary() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT d FROM wbr_arr WHERE id=1");
        assertArrayEquals(new Double[]{1.25, 2.25}, v);
    }

    @Test
    void h10_varcharArray_binary() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT e FROM wbr_arr WHERE id=1");
        assertArrayEquals(new String[]{"x", "y"}, v);
    }

    @Test
    void h10_boolArray() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT f FROM wbr_arr WHERE id=1");
        assertArrayEquals(new Boolean[]{true, false}, v);
    }

    @Test
    void h10_dateArray() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT g FROM wbr_arr WHERE id=1");
        assertEquals(1, v.length);
        assertEquals(Date.valueOf("2020-01-02"), v[0]);
    }

    @Test
    void h10_timestampArray_elementFormat() throws SQLException {
        // Was failing with "Bad value for type timestamp: 2020-01-02T03:04:05" (ISO 'T' element format).
        createArrayTable();
        Object[] v = readArray("SELECT h FROM wbr_arr WHERE id=1");
        assertEquals(1, v.length);
        assertEquals(Timestamp.valueOf("2020-01-02 03:04:05"), v[0]);
    }

    @Test
    void h10_uuidArray() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT i FROM wbr_arr WHERE id=1");
        assertArrayEquals(new java.util.UUID[]{java.util.UUID.fromString("11111111-1111-1111-1111-111111111111")}, v);
    }

    @Test
    void h10_numericArray() throws SQLException {
        createArrayTable();
        Object[] v = readArray("SELECT j FROM wbr_arr WHERE id=1");
        assertEquals(2, v.length);
        assertEquals(0, ((BigDecimal) v[0]).compareTo(new BigDecimal("1.5")));
        assertEquals(0, ((BigDecimal) v[1]).compareTo(new BigDecimal("2.25")));
    }

    @Test
    void h10_jsonbArray_returnsSqlArray() throws SQLException {
        // Was returning a single PGobject (column mis-classified as Types.OTHER, not ARRAY).
        createArrayTable();
        Object[] v = readArray("SELECT k FROM wbr_arr WHERE id=1");
        assertEquals(1, v.length);
        assertEquals("{\"k\": 1}", v[0].toString());
    }

    @Test
    void h10_arrayWithNullElement_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS wbr_arr_n");
        exec("CREATE TABLE wbr_arr_n (id int primary key, a int2[], b varchar[])");
        exec("INSERT INTO wbr_arr_n VALUES (1, ARRAY[1,NULL,3]::int2[], ARRAY['a',NULL,'c'])");
        assertArrayEquals(new Short[]{(short) 1, null, (short) 3}, readArray("SELECT a FROM wbr_arr_n WHERE id=1"));
        assertArrayEquals(new String[]{"a", null, "c"}, readArray("SELECT b FROM wbr_arr_n WHERE id=1"));
    }

    @Test
    void h10_emptyArray_binary() throws SQLException {
        exec("DROP TABLE IF EXISTS wbr_arr_e");
        exec("CREATE TABLE wbr_arr_e (id int primary key, a int8[])");
        exec("INSERT INTO wbr_arr_e VALUES (1, ARRAY[]::int8[])");
        assertEquals(0, readArray("SELECT a FROM wbr_arr_e WHERE id=1").length);
    }

    // =====================================================================
    // C8 — timestamptz infinity + BC dates in binary mode
    // =====================================================================

    @Test
    void c8_timestamptzInfinity_binary() throws SQLException {
        assertEquals("infinity", readString("SELECT 'infinity'::timestamptz", 8));
        assertEquals("-infinity", readString("SELECT '-infinity'::timestamptz", 8));
        // getObject must not throw "Unsupported binary encoding" and must be the far-future/past sentinel.
        assertNotNull(readObject("SELECT 'infinity'::timestamptz", 8));
        assertNotNull(readObject("SELECT '-infinity'::timestamptz", 8));
    }

    @Test
    void c8_timestampInfinity_binary_regression() throws SQLException {
        assertEquals("infinity", readString("SELECT 'infinity'::timestamp", 8));
        assertEquals("-infinity", readString("SELECT '-infinity'::timestamp", 8));
    }

    @Test
    void c8_bcDate_binary() throws SQLException {
        // Was failing with "Unsupported binary encoding of date" (text fallback under binary format).
        Object o = readObject("SELECT '0044-03-15 BC'::date", 8);
        assertTrue(o instanceof Date, "expected java.sql.Date, got " + (o == null ? "null" : o.getClass()));
        assertEquals("0044-03-15 BC", readString("SELECT '0044-03-15 BC'::date", 8));
    }

    @Test
    void c8_connectionSurvivesInfinityAndBc() throws SQLException {
        // The original defect killed/hung the connection. Prove it is still usable afterward.
        readString("SELECT 'infinity'::timestamptz", 8);
        readObject("SELECT '0044-03-15 BC'::date", 8);
        assertEquals(Integer.valueOf(7), readObject("SELECT 7", 3));
    }

    // =====================================================================
    // M16 — ParameterDescription infers cast-context parameter types
    // =====================================================================

    @Test
    void m16_castContextParamTypes() throws SQLException {
        assertParamType("SELECT ?::int8", "int8", Types.BIGINT);
        assertParamType("SELECT ?::timestamp", "timestamp", Types.TIMESTAMP);
        assertParamType("SELECT ?::numeric", "numeric", Types.NUMERIC);
        assertParamType("SELECT CAST(? AS int8)", "int8", Types.BIGINT);
        assertParamType("SELECT ?::date", "date", Types.DATE);
        assertParamType("SELECT ?::float8", "float8", Types.DOUBLE);
    }

    private void assertParamType(String sql, String expectedName, int expectedType) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(expectedName, pmd.getParameterTypeName(1), "type name for " + sql);
            assertEquals(expectedType, pmd.getParameterType(1), "sql type for " + sql);
        }
    }
}
