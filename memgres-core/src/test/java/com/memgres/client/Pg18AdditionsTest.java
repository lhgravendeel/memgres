package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What PostgreSQL 18 added, and what memgres answered instead.
 *
 * <p>Some of these were missing outright — a call to {@code gamma} or {@code pg_get_loaded_modules}
 * was a function that does not exist. The ones worth more attention were the ones that answered:
 * {@code crc32} and {@code reverse} are declared over bytea, and reading a bytea through
 * {@code toString} hashed and reversed the identity of a Java array, so both returned a number or
 * a string that had nothing to do with the value. {@code 256::bytea} gave back the three
 * characters of "256".
 */
class Pg18AdditionsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
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

    private static SQLException refusalOf(String sql) {
        return assertThrows(SQLException.class, () -> scalar(sql), sql);
    }

    // ---------------------------------------------------------------- gamma

    /**
     * Gamma extends the factorial: it is {@code (n-1)!} at a whole number, and defined between
     * them.
     *
     * <p>The values are read here at {@code real} precision, which is where memgres and PostgreSQL
     * agree exactly. They differ in the last bit of a double, and it is PostgreSQL that is out:
     * its answers come from the C library, whose {@code gamma(10)} is 362880.00000000006 where the
     * answer is 362880 and a double holds it exactly. Matching that would mean reproducing one
     * library's rounding on every platform it is built for.
     */
    @Test
    void gammaExtendsTheFactorial() throws Exception {
        assertEquals("24", scalar("SELECT gamma(5)::real::text"));
        assertEquals("1", scalar("SELECT gamma(1)::real::text"));
        assertEquals("362880", scalar("SELECT gamma(10)::real::text"));
        assertEquals("1.7724539", scalar("SELECT gamma(0.5)::real::text"));
        assertEquals("-3.5449078", scalar("SELECT gamma(-0.5)::real::text"));
        assertEquals("0", scalar("SELECT lgamma(1)::real::text"));
        assertEquals("3.1780539", scalar("SELECT lgamma(5)::real::text"));
        assertEquals("1.2655121", scalar("SELECT lgamma(-0.5)::real::text"));
        assertNull(scalar("SELECT gamma(NULL::float8)::text"));
    }

    /** The poles, and every answer past what a double holds, are reported as an overflow. */
    @Test
    void gammaReportsWhatItCannotAnswer() {
        for (String sql : new String[]{
                "SELECT gamma(0)", "SELECT gamma(-1)", "SELECT gamma(200)",
                "SELECT gamma(1e-320)", "SELECT gamma('-inf'::float8)",
                "SELECT lgamma(0)", "SELECT lgamma(-1)"}) {
            assertEquals("22003", refusalOf(sql).getSQLState(), sql);
        }
    }

    /** Infinity and NaN pass through rather than being refused. */
    @Test
    void gammaCarriesInfinityAndNaN() throws Exception {
        assertEquals("Infinity", scalar("SELECT gamma('inf'::float8)::text"));
        assertEquals("NaN", scalar("SELECT gamma('nan'::float8)::text"));
        assertEquals("Infinity", scalar("SELECT lgamma('inf'::float8)::text"));
        assertEquals("NaN", scalar("SELECT lgamma('nan'::float8)::text"));
    }

    // ---------------------------------------------------------------- bytea

    /** The hashes are declared over bytea, and hash the bytes rather than a description of them. */
    @Test
    void theHashesReadTheBytes() throws Exception {
        assertEquals("891568578", scalar("SELECT crc32('abc'::bytea)::text"));
        assertEquals("910901175", scalar("SELECT crc32c('abc'::bytea)::text"));
        assertEquals("0", scalar("SELECT crc32(''::bytea)::text"));
        assertEquals("0", scalar("SELECT crc32c(''::bytea)::text"));
        assertEquals("3523407757", scalar("SELECT crc32('\\x00'::bytea)::text"));
    }

    /** reverse is declared over bytea too, and turns the bytes around rather than the characters. */
    @Test
    void reverseTurnsBytesAround() throws Exception {
        assertEquals("\\x030201", scalar("SELECT reverse('\\x010203'::bytea)::text"));
        assertEquals("3", scalar("SELECT length(reverse('\\x010203'::bytea))::text"));
        assertEquals("cba", scalar("SELECT reverse('abc')"));
    }

    /**
     * PostgreSQL 18 casts between bytea and the integer types.
     *
     * <p>The bytes are the value's own, big-endian and as wide as the type. Fewer bytes than the
     * width are the low-order ones; more than the width is a value the type cannot hold.
     */
    @Test
    void byteaAndTheIntegersConvertBothWays() throws Exception {
        assertEquals("256", scalar("SELECT '\\x00000100'::bytea::int::text"));
        assertEquals("255", scalar("SELECT '\\xff'::bytea::int::text"));
        assertEquals("-1", scalar("SELECT '\\xffffffff'::bytea::int::text"));
        assertEquals("0", scalar("SELECT '\\x'::bytea::int::text"));
        assertEquals("1", scalar("SELECT '\\x000001'::bytea::int::text"));
        assertEquals("-1", scalar("SELECT '\\xffff'::bytea::smallint::text"));
        assertEquals("-1", scalar("SELECT '\\xffffffffffffffff'::bytea::bigint::text"));
        assertEquals("\\x00000100", scalar("SELECT (256::int)::bytea::text"));
        assertEquals("\\x0100", scalar("SELECT (256::smallint)::bytea::text"));
        assertEquals("\\x0000000000000100", scalar("SELECT (256::bigint)::bytea::text"));
        assertEquals("\\xffffffff", scalar("SELECT ((-1)::int)::bytea::text"));
    }

    @Test
    void aByteaWiderThanTheTypeIsOutOfRange() {
        assertEquals("22003", refusalOf("SELECT '\\x0000000001'::bytea::int").getSQLState());
        assertEquals("22003", refusalOf("SELECT '\\xffffff'::bytea::smallint").getSQLState());
        assertEquals("22003",
                refusalOf("SELECT '\\xffffffffffffffffff'::bytea::bigint").getSQLState());
    }

    // ---------------------------------------------------------------- the rest

    /** A Roman numeral read as the number it spells, and refused where it spells none. */
    @Test
    void toNumberReadsRomanNumerals() throws Exception {
        assertEquals("2025", scalar("SELECT to_number('MMXXV', 'RN')::text"));
        assertEquals("4", scalar("SELECT to_number('IV', 'RN')::text"));
        assertEquals("1994", scalar("SELECT to_number('MCMXCIV', 'RN')::text"));
        assertEquals("2025", scalar("SELECT to_number('mmxxv', 'RN')::text"));
        assertEquals("2025", scalar("SELECT to_number('  MMXXV ', 'RN')::text"));
        // The letters that begin the value are read, and it stops at the first that is not one.
        assertEquals("10", scalar("SELECT to_number('XYZ', 'RN')::text"));
        assertEquals("9", scalar("SELECT to_number('IXY', 'RN')::text"));
        // Letters that are not a numeral are not a number.
        for (String bad : new String[]{"IIII", "VV", "IL", "Q", "123", "-MMXXV"}) {
            assertEquals("22P02", refusalOf("SELECT to_number('" + bad + "', 'RN')").getSQLState(), bad);
        }
    }

    /** array_sort's arguments say which way round it sorts, and where the nulls go. */
    @Test
    void arraySortReadsItsDirection() throws Exception {
        assertEquals("{1,2,3}", scalar("SELECT array_sort(ARRAY[3,1,2])::text"));
        assertEquals("{3,2,1}", scalar("SELECT array_sort(ARRAY[3,1,2], true)::text"));
        assertEquals("{1,2,3}", scalar("SELECT array_sort(ARRAY[3,1,2], false)::text"));
        assertEquals("{NULL,1,3}", scalar("SELECT array_sort(ARRAY[3,NULL,1], false, true)::text"));
        assertEquals("{1,3,NULL}", scalar("SELECT array_sort(ARRAY[3,NULL,1], false, false)::text"));
        // Without a third argument the nulls go where ORDER BY puts them: last ascending, first
        // descending.
        assertEquals("{NULL,3,1}", scalar("SELECT array_sort(ARRAY[3,NULL,1], true)::text"));
    }

    /**
     * The views and functions 18 added for watching a server.
     *
     * <p>memgres does none of this I/O, so every counter is zero — but a monitoring query reads
     * its counters off the rows it expects to find, and finding no rows at all is not the same as
     * finding them at zero. The rows are there, one per way a backend can read or write.
     */
    @Test
    void theStatisticsViewsDescribeThemselves() throws Exception {
        assertEquals("79", scalar("SELECT count(*)::text FROM pg_stat_io"));
        assertEquals("14", scalar("SELECT count(DISTINCT backend_type)::text FROM pg_stat_io"));
        assertEquals("3", scalar("SELECT count(DISTINCT object)::text FROM pg_stat_io"));
        assertEquals("5", scalar("SELECT count(DISTINCT context)::text FROM pg_stat_io"));
        assertEquals("0", scalar("SELECT sum(reads)::text FROM pg_stat_io"));
        assertEquals("8", scalar("SELECT count(*)::text FROM pg_stat_io WHERE backend_type = 'client backend'"));
        // pg_aios lists the asynchronous I/O in flight, of which there is never any.
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_aios"));
        // A backend's own share of the same counters, and nothing for a pid that is nobody's.
        assertEquals("8", scalar("SELECT count(*)::text FROM pg_stat_get_backend_io(pg_backend_pid())"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_stat_get_backend_io(999999)"));
        // Nothing is loaded, so nothing is listed — but the call resolves.
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_get_loaded_modules()"));
        assertNull(scalar("SELECT pg_column_toast_chunk_id('x'::text)::text"));
    }

    /** The parameters 18 added, which a client may ask the value of. */
    @Test
    void theNewSettingsAreThere() throws Exception {
        assertEquals("worker", scalar("SELECT current_setting('io_method')"));
        assertEquals("copy", scalar("SELECT current_setting('file_copy_method')"));
        assertEquals("100000000", scalar("SELECT current_setting('autovacuum_vacuum_max_threshold')"));
        assertEquals("0", scalar("SELECT current_setting('num_os_semaphores')"));
        assertEquals("enum", scalar("SELECT vartype FROM pg_settings WHERE name = 'io_method'"));
        // PostgreSQL lists io_uring here as well, but only where it was built with liburing —
        // the set of methods is a property of the build rather than of the version. memgres
        // performs none of them and lists the two every build has.
        assertEquals("{sync,worker}",
                scalar("SELECT enumvals::text FROM pg_settings WHERE name = 'io_method'"));
        assertEquals("true", scalar("SELECT (enumvals @> ARRAY['sync','worker'])::text"
                + " FROM pg_settings WHERE name = 'io_method'"));
    }

    /** A rejection limit means nothing without permission to reject anything. */
    @Test
    void copyRejectLimitNeedsOnError() throws SQLException {
        // An ordinary table of its own: PostgreSQL opens the relation a COPY names before it
        // reads the option list, so the relation has to be one it can open for the option's
        // refusal to be what comes back.
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pg18_rl (a int)");
        }
        SQLException e = assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute("COPY pg18_rl FROM STDIN WITH (FORMAT csv, REJECT_LIMIT 3)");
            }
        });
        assertEquals("22023", e.getSQLState());
        assertEquals("ERROR: COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
                e.getMessage());
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE pg18_rl");
        }
    }
}
