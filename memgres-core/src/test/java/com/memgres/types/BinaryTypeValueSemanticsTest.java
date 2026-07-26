package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Binary and bit-string values must behave as values, not as Java objects: equal bytea
 * values group together, aggregates over them produce bytea, and bad input is rejected.
 * Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N8 bytea grouping, N9 varbit parameters and typmod, N31 aggregate/concat output,
 * N63 over-accepting macaddr, bytea and money input.
 */
class BinaryTypeValueSemanticsTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N8 — equal bytea values are one group
    // ------------------------------------------------------------------

    @Test
    void byteaGroupsByValue() throws Exception {
        exec("CREATE TABLE bvs_g (b bytea)");
        exec("INSERT INTO bvs_g VALUES ('\\x0102'),('\\x0102'),('\\x03')");

        assertEquals(Arrays.asList("2"),
                rows("SELECT count(*) FROM (SELECT b FROM bvs_g GROUP BY b) g"));
        assertEquals(Arrays.asList("2"), rows("SELECT count(DISTINCT b) FROM bvs_g"));
        assertEquals(Arrays.asList("\\x0102|2", "\\x03|1"),
                rows("SELECT b, count(*) FROM bvs_g GROUP BY b ORDER BY b"));
    }

    @Test
    void byteaDistinctDeduplicates() throws Exception {
        exec("CREATE TABLE bvs_d (b bytea)");
        exec("INSERT INTO bvs_d VALUES ('\\x0102'),('\\x0102'),('\\x03')");

        assertEquals(Arrays.asList("\\x0102", "\\x03"),
                rows("SELECT DISTINCT b FROM bvs_d ORDER BY b"));
    }

    // ------------------------------------------------------------------
    // N31 — aggregates and concat produce values, not Java object strings
    // ------------------------------------------------------------------

    @Test
    void stringAggOverByteaProducesBytea() throws Exception {
        exec("CREATE TABLE bvs_a (b bytea)");
        exec("INSERT INTO bvs_a VALUES ('\\x0102'),('\\x0102'),('\\x03')");

        assertEquals(Arrays.asList("\\x0102010203"),
                rows("SELECT string_agg(b, ''::bytea) FROM bvs_a"));
    }

    @Test
    void concatRendersByteaAndArrays() throws Exception {
        assertEquals(Arrays.asList("a\\x0102"), rows("SELECT concat('a', '\\x0102'::bytea)"));
        assertEquals(Arrays.asList("a{1,2}"), rows("SELECT concat('a', ARRAY[1,2])"));
    }

    @Test
    void bitAggregatesWorkOverBitStrings() throws Exception {
        exec("CREATE TABLE bvs_b (v bit(5))");
        exec("INSERT INTO bvs_b VALUES (B'10101')");

        assertEquals(Arrays.asList("10101|10101|10101"),
                rows("SELECT bit_and(v)::text, bit_or(v)::text, bit_xor(v)::text FROM bvs_b"));
    }

    // ------------------------------------------------------------------
    // N9 — varbit keeps its bits
    // ------------------------------------------------------------------

    @Test
    void varbitTypmodTruncatesInsteadOfErroring() throws Exception {
        assertEquals(Arrays.asList("101"), rows("SELECT B'10101'::varbit(3)"));
    }

    @Test
    void varbitFromTextKeepsItsBits() throws Exception {
        assertEquals(Arrays.asList("1011"), rows("SELECT '1011'::varbit"));
    }

    /** A bound parameter is a bit string, not an integer. */
    @Test
    void varbitParameterIsNotParsedAsInteger() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT ?::varbit")) {
            ps.setString(1, "1011");
            try (ResultSet rs = ps.executeQuery()) {
                org.junit.jupiter.api.Assertions.assertTrue(rs.next());
                assertEquals("1011", rs.getString(1));
            }
        }
    }

    // ------------------------------------------------------------------
    // N63 — bad input for binary-ish types is rejected
    // ------------------------------------------------------------------

    @Test
    void invalidMacaddrOctetIsRejected() {
        assertEquals("22003", state("SELECT '00:11:22:33:44:-6'::macaddr"));
    }

    @Test
    void invalidByteaEscapeIsRejected() {
        assertEquals("22P02", state("SELECT '\\q'::bytea"));
    }

    @Test
    void invalidMoneyInputIsRejected() {
        assertEquals("22P02", state("SELECT '1e3'::money"));
    }

    @Test
    void avgOverMoneyDoesNotExist() {
        assertEquals("42883", state("SELECT avg(x) FROM (SELECT '1'::money AS x) t"));
    }
}
