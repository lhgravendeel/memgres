package com.memgres.pgwire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Group 9 (mtask-8 Wave 5): {@code array_agg} result columns advertised {@code _int4} (int4[],
 * OID 1007) regardless of the aggregated element type — {@code ExprEvaluator.inferTypeFromContext}
 * hardcoded {@code DataType.INT4_ARRAY} for {@code array_agg}.
 *
 * <p>For text elements (the app's {@code array_agg(DISTINCT provider_id)} feeding
 * {@code battery_sets_aggregated.supported_providers}), pgjdbc in text mode fails
 * {@code Bad value for type int : tennet}; in binary mode int4[] is in pgjdbc's binary-transfer
 * set, so once prepareThreshold flips the statement to server-prepared+binary it decodes the text
 * payload as a binary int4 array and a garbage dimension count triggers a giant allocation
 * ({@code OutOfMemoryError} at {@code PgArray.readBinaryResultSet}).
 *
 * <p>Fix under test: derive the array type from the argument's element type — int4-family →
 * {@code _int4}, enum element → the enum's own array OID (the wave-4 pg_type machinery), anything
 * else → {@code _text} — including through the scalar-subquery + materialized-view shape the app
 * actually uses.
 */
class ArrayAggResultTypeTest {

    static Memgres memgres;
    static Connection textConn;
    static Connection binaryConn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        String base = memgres.getJdbcUrl();
        textConn = DriverManager.getConnection(base + "?binaryTransfer=false", memgres.getUser(), memgres.getPassword());
        textConn.setAutoCommit(true);
        binaryConn = DriverManager.getConnection(base + "?binaryTransfer=true&prepareThreshold=1", memgres.getUser(), memgres.getPassword());
        binaryConn.setAutoCommit(true);
        try (Statement s = textConn.createStatement()) {
            s.execute("CREATE TABLE aart_prov (battery_set_id int, provider_id text)");
            s.execute("INSERT INTO aart_prov VALUES (101, 'tennet'), (101, 'frank')");
            s.execute("CREATE TABLE aart_bs (id int primary key, name text)");
            s.execute("INSERT INTO aart_bs VALUES (101, 'set A')");
            s.execute("CREATE TABLE aart_nums (id int, n int)");
            s.execute("INSERT INTO aart_nums VALUES (1, 7), (1, 42)");
            s.execute("CREATE TYPE aart_kind AS ENUM ('alpha', 'beta', 'gamma')");
            s.execute("CREATE TABLE aart_kinds (id int, kind aart_kind)");
            s.execute("INSERT INTO aart_kinds VALUES (1, 'alpha'), (1, 'beta')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        try (Statement s = textConn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS aart_prov");
            s.execute("DROP TABLE IF EXISTS aart_bs");
            s.execute("DROP TABLE IF EXISTS aart_nums");
            s.execute("DROP TABLE IF EXISTS aart_kinds");
            s.execute("DROP TYPE IF EXISTS aart_kind");
        } finally {
            if (textConn != null) textConn.close();
            if (binaryConn != null) binaryConn.close();
            if (memgres != null) memgres.close();
        }
    }

    /** Runs the query twice so binaryConn's prepareThreshold=1 actually flips to binary. */
    static void assertTextArrayAgg(Connection conn, String sql) throws SQLException {
        for (int round = 1; round <= 2; round++) {
            try (PreparedStatement p = conn.prepareStatement(sql);
                 ResultSet rs = p.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("_text", rs.getMetaData().getColumnTypeName(1),
                        "array_agg over text elements must advertise _text, not _int4 (round " + round + ")");
                Array a = rs.getArray(1);
                assertNotNull(a);
                Object arr = assertDoesNotThrow((org.junit.jupiter.api.function.ThrowingSupplier<Object>) a::getArray,
                        "getArray() must decode a text-element array_agg without Bad-value/binary-garbage errors (round " + round + ")");
                assertInstanceOf(String[].class, arr);
                List<String> values = new ArrayList<>(Arrays.asList((String[]) arr));
                Collections.sort(values);
                assertEquals(List.of("frank", "tennet"), values);
            }
        }
    }

    @Test
    void arrayAgg_textElements_direct_overTextTransfer() throws SQLException {
        assertTextArrayAgg(textConn, "SELECT array_agg(provider_id) FROM aart_prov");
    }

    @Test
    void arrayAgg_textElements_direct_overBinaryTransfer() throws SQLException {
        assertTextArrayAgg(binaryConn, "SELECT array_agg(provider_id) FROM aart_prov");
    }

    /** The app's exact shape: scalar subquery with array_agg(DISTINCT text_col) per outer row. */
    @Test
    void arrayAgg_textElements_scalarSubquery_overTextTransfer() throws SQLException {
        assertTextArrayAgg(textConn,
                "SELECT (SELECT array_agg(DISTINCT p.provider_id) FROM aart_prov p WHERE p.battery_set_id = bs.id) AS supported_providers FROM aart_bs bs");
    }

    @Test
    void arrayAgg_textElements_scalarSubquery_overBinaryTransfer() throws SQLException {
        assertTextArrayAgg(binaryConn,
                "SELECT (SELECT array_agg(DISTINCT p.provider_id) FROM aart_prov p WHERE p.battery_set_id = bs.id) AS supported_providers FROM aart_bs bs");
    }

    /** Control: array_agg over int elements must keep advertising _int4 and decode as Integer[]. */
    @Test
    void arrayAgg_intElements_staysInt4Array_bothTransferModes() throws SQLException {
        for (Connection conn : new Connection[]{textConn, binaryConn}) {
            for (int round = 1; round <= 2; round++) {
                try (PreparedStatement p = conn.prepareStatement("SELECT array_agg(n) FROM aart_nums");
                     ResultSet rs = p.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("_int4", rs.getMetaData().getColumnTypeName(1));
                    Object arr = rs.getArray(1).getArray();
                    assertInstanceOf(Integer[].class, arr);
                    Integer[] values = (Integer[]) arr;
                    Arrays.sort(values);
                    assertArrayEquals(new Integer[]{7, 42}, values);
                }
            }
        }
    }

    /** array_agg over an enum column advertises the enum's own array type (wave-4 pg_type row). */
    @Test
    void arrayAgg_enumElements_advertisesEnumArrayType() throws SQLException {
        try (PreparedStatement p = textConn.prepareStatement("SELECT array_agg(kind) FROM aart_kinds");
             ResultSet rs = p.executeQuery()) {
            assertTrue(rs.next());
            assertEquals("_aart_kind", rs.getMetaData().getColumnTypeName(1),
                    "array_agg over an enum column must advertise the enum's array type");
            Array a = rs.getArray(1);
            // getResultSet() exercises TypeInfoCache.getArrayDelimiter for the advertised OID
            ResultSet elems = assertDoesNotThrow((org.junit.jupiter.api.function.ThrowingSupplier<ResultSet>) a::getResultSet);
            List<String> values = new ArrayList<>();
            while (elems.next()) values.add(elems.getString(2));
            Collections.sort(values);
            assertEquals(List.of("alpha", "beta"), values);
        }
    }
}
