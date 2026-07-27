package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A value or a row count that is quietly reduced looks exactly like a correct one. These cover the
 * places where a cap returned a short answer, a cast wrapped past the end of its type, or an input
 * was padded into a different valid value — all of which a caller has no way to notice.
 */
class ValueTruncationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static void assertState(String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    // ------------------------------------------------------------ row counts

    @Test
    void temporalSeriesIsNotCapped() throws Exception {
        assertEquals("20001", scalar("SELECT count(*)::text FROM generate_series("
                + "timestamp '2020-01-01', timestamp '2020-01-01' + interval '20000 hours',"
                + " interval '1 hour')"));
        assertEquals("20001", scalar("SELECT count(*)::text FROM generate_series("
                + "date '2020-01-01', date '2020-01-01' + 20000, interval '1 day')"));
        assertEquals("15001", scalar("SELECT count(*)::text FROM generate_series("
                + "timestamptz '2020-01-01 00:00:00+00', timestamptz '2020-01-01 00:00:00+00' + interval '15000 hours',"
                + " interval '1 hour')"));
        // short ranges and the integer overload are unaffected
        assertEquals("20000", scalar("SELECT count(*)::text FROM generate_series(1, 20000)"));
        assertEquals("25", scalar("SELECT count(*)::text FROM generate_series("
                + "timestamp '2020-01-01', timestamp '2020-01-02', interval '1 hour')"));
    }

    @Test
    void recursiveCteIsNotCapped() throws Exception {
        assertEquals("150000", scalar("WITH RECURSIVE r(n) AS"
                + " (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 150000)"
                + " SELECT count(*)::text FROM r"));
        assertEquals("50", scalar("WITH RECURSIVE r(n) AS"
                + " (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 50)"
                + " SELECT count(*)::text FROM r"));
    }

    // -------------------------------------------------------- numeric casts

    @Test
    void numericToIntegerReportsInsteadOfWrapping() {
        assertState("22003", "SELECT '9223372036854775808'::numeric::int8");
        assertState("22003", "SELECT '-9223372036854775809'::numeric::int8");
        assertState("22003", "SELECT '99999999999999999999'::numeric::bigint");
        assertState("22003", "SELECT '2147483648'::numeric::int");
        assertState("22003", "SELECT '-2147483649'::numeric::int");
        assertState("22003", "SELECT '32768'::numeric::smallint");
    }

    @Test
    void numericValuesThatFitAreUnaffected() throws Exception {
        assertEquals("123", scalar("SELECT ('123'::numeric::int)::text"));
        assertEquals("3", scalar("SELECT ('2.7'::numeric::int)::text"));
        assertEquals("-3", scalar("SELECT ('-2.7'::numeric::int)::text"));
        assertEquals("9223372036854775807",
                scalar("SELECT ('9223372036854775807'::numeric::int8)::text"));
        assertEquals("-9223372036854775808",
                scalar("SELECT ('-9223372036854775808'::numeric::int8)::text"));
    }

    // ------------------------------------------------------- LIMIT / OFFSET

    @Test
    void limitAndOffsetAcceptBigintValues() throws Exception {
        assertEquals("1", scalar("SELECT count(*)::text FROM (SELECT 1 LIMIT 2147483648) t"));
        assertEquals("1", scalar("SELECT count(*)::text"
                + " FROM (SELECT 1 LIMIT 9223372036854775807) t"));
        assertEquals("0", scalar("SELECT count(*)::text FROM (SELECT 1 OFFSET 2147483648) t"));
        exec("DROP TABLE IF EXISTS vtt_t CASCADE");
        exec("CREATE TABLE vtt_t (i int)");
        exec("INSERT INTO vtt_t SELECT generate_series(1,5)");
        assertEquals("5", scalar("SELECT count(*)::text"
                + " FROM (SELECT i FROM vtt_t LIMIT 2147483648) t"));
        assertEquals("0", scalar("SELECT count(*)::text"
                + " FROM (SELECT i FROM vtt_t OFFSET 2147483648) t"));
        exec("DROP TABLE vtt_t");
    }

    @Test
    void limitZeroReturnsNothing() throws Exception {
        assertEquals("0", scalar("SELECT count(*)::text FROM (SELECT 1 LIMIT 0) t"));
        exec("DROP TABLE IF EXISTS vtt_z CASCADE");
        exec("CREATE TABLE vtt_z (i int)");
        exec("INSERT INTO vtt_z SELECT generate_series(1,5)");
        assertEquals("0", scalar("SELECT count(*)::text FROM (SELECT i FROM vtt_z LIMIT 0) t"));
        assertEquals("3", scalar("SELECT count(*)::text"
                + " FROM (SELECT i FROM vtt_z ORDER BY i LIMIT 3) t"));
        assertEquals("2", scalar("SELECT count(*)::text"
                + " FROM (SELECT i FROM vtt_z ORDER BY i OFFSET 3) t"));
        exec("DROP TABLE vtt_z");
    }

    @Test
    void negativeLimitAndOffsetAreStillRejected() {
        assertState("2201W", "SELECT 1 LIMIT -1");
        assertState("2201X", "SELECT 1 OFFSET -1");
    }

    // ------------------------------------------------------------ uuid input

    @Test
    void uuidAcceptsTheFormsPostgresAccepts() throws Exception {
        String expected = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        assertEquals(expected, scalar("SELECT ('a0eebc999c0b4ef8bb6d6bb9bd380a11'::uuid)::text"));
        assertEquals(expected,
                scalar("SELECT ('{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'::uuid)::text"));
        assertEquals(expected,
                scalar("SELECT ('a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11'::uuid)::text"));
        assertEquals(expected,
                scalar("SELECT ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid)::text"));
        assertEquals(expected, scalar("SELECT ('" + expected + "'::uuid)::text"));
    }

    @Test
    void uuidRefusesToPadAShortValue() {
        // padding would turn a mistyped identifier into a different valid one
        assertState("22P02", "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1'::uuid");
        assertState("22P02", "SELECT '1-1-1-1-1'::uuid");
        assertState("22P02", "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111'::uuid");
        assertState("22P02", "SELECT 'not-a-uuid'::uuid");
        assertState("22P02", "SELECT ''::uuid");
    }

    // ------------------------------------------------------------- bit input

    @Test
    void bitInputAcceptsRadixPrefixes() throws Exception {
        assertEquals("101", scalar("SELECT ('b101'::varbit)::text"));
        assertEquals("101", scalar("SELECT ('B101'::varbit)::text"));
        assertEquals("00011111", scalar("SELECT ('x1f'::varbit)::text"));
        assertEquals("00011111", scalar("SELECT ('X1F'::varbit)::text"));
        assertEquals("101", scalar("SELECT ('B101'::bit(3))::text"));
        assertEquals("11111111", scalar("SELECT ('xff'::varbit)::text"));
        // plain digits still work and bad input is still refused
        assertEquals("101", scalar("SELECT ('101'::varbit)::text"));
        assertState("22P02", "SELECT '102'::varbit");
        assertState("22P02", "SELECT 'xzz'::varbit");
    }

    // ---------------------------------------------------------- range bounds

    @Test
    void rangeBoundsMustFitTheElementType() {
        assertState("22003", "SELECT '[1,99999999999999999999999)'::int4range");
        assertState("22003", "SELECT '[-99999999999999999999999,1)'::int4range");
        assertState("22003", "SELECT '[1,3000000000)'::int4range");
        assertState("22P02", "SELECT '[1,3.5)'::int4range");
        assertState("22003", "SELECT '[1,99999999999999999999999)'::int8range");
        assertState("22P02", "SELECT '[1,3.5)'::int8range");
    }

    @Test
    void rangeBoundsThatFitAreUnaffected() throws Exception {
        assertEquals("[1,10)", scalar("SELECT ('[1,10)'::int4range)::text"));
        assertEquals("[1,2147483647)", scalar("SELECT ('[1,2147483647)'::int4range)::text"));
        assertEquals("empty", scalar("SELECT ('empty'::int4range)::text"));
        assertEquals("[1.5,3.5)", scalar("SELECT ('[1.5,3.5)'::numrange)::text"));
    }

    // -------------------------------------------------------- column default

    @Test
    void addedColumnDefaultMustFitTheColumn() throws Exception {
        exec("DROP TABLE IF EXISTS vtt_d CASCADE");
        exec("CREATE TABLE vtt_d (i int)");
        exec("INSERT INTO vtt_d VALUES (1)");
        assertState("22001", "ALTER TABLE vtt_d ADD COLUMN c varchar(2) DEFAULT 'abcdef'");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'vtt_d'"));
        // the same check applies when there are no rows to backfill
        exec("DROP TABLE IF EXISTS vtt_e CASCADE");
        exec("CREATE TABLE vtt_e (i int)");
        assertState("22001", "ALTER TABLE vtt_e ADD COLUMN c varchar(2) DEFAULT 'abcdef'");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'vtt_e'"));
        exec("DROP TABLE vtt_e");
    }

    @Test
    void addedColumnDefaultThatFitsIsBackfilled() throws Exception {
        exec("DROP TABLE IF EXISTS vtt_f CASCADE");
        exec("CREATE TABLE vtt_f (i int)");
        exec("INSERT INTO vtt_f VALUES (1)");
        exec("ALTER TABLE vtt_f ADD COLUMN d varchar(10) DEFAULT 'ok'");
        assertEquals("ok", scalar("SELECT d FROM vtt_f"));
        exec("DROP TABLE vtt_f");
    }
}
