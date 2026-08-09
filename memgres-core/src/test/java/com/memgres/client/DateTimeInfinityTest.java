package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * infinity is a state a date or a timestamp is in, not an instant it stands on.
 *
 * <p>The sentinels used to be 9999-12-31 23:59:59 and 4713-01-01 BC, which are instants a user can
 * write. So infinity round-tripped as a finite instant, an ordinary timestamp compared equal to it,
 * isfinite said it was finite, and adding a day to it went through the numeric parser and raised.
 */
class DateTimeInfinityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
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

    /** It is written as the word it is. */
    @Test
    void infinityIsWrittenAsTheWord() throws Exception {
        assertEquals("infinity", scalar("SELECT (timestamptz 'infinity')::text"));
        assertEquals("infinity", scalar("SELECT (timestamp 'infinity')::text"));
        assertEquals("infinity", scalar("SELECT (date 'infinity')::text"));
        assertEquals("-infinity", scalar("SELECT (timestamptz '-infinity')::text"));
        assertEquals("-infinity", scalar("SELECT (date '-infinity')::text"));
    }

    /** And it is not finite. */
    @Test
    void infinityIsNotFinite() throws Exception {
        assertEquals("f", scalar("SELECT isfinite(timestamptz 'infinity')"));
        assertEquals("f", scalar("SELECT isfinite(timestamp 'infinity')"));
        assertEquals("f", scalar("SELECT isfinite(date 'infinity')"));
        assertEquals("t", scalar("SELECT isfinite(timestamp '2020-01-01')"));
        assertEquals("t", scalar("SELECT isfinite(date '2020-01-01')"));
    }

    /** No instant a user can write is equal to it. */
    @Test
    void noWritableInstantEqualsInfinity() throws Exception {
        assertEquals("f", scalar("SELECT '9999-12-31 23:59:59'::timestamp = 'infinity'::timestamp"));
        assertEquals("f", scalar("SELECT '9999-12-31'::date = 'infinity'::date"));
        assertEquals("t", scalar("SELECT date 'infinity' > date '9999-12-31'"));
        assertEquals("t", scalar("SELECT date '-infinity' < date '0001-01-01'"));
    }

    /** Arithmetic on it answers it. */
    @Test
    void infinityAbsorbsArithmetic() throws Exception {
        assertEquals("infinity", scalar("SELECT (date 'infinity' + 1)::text"));
        assertEquals("infinity", scalar("SELECT (date 'infinity' - 1)::text"));
        assertEquals("infinity",
                scalar("SELECT (timestamp 'infinity' + interval '1 day')::text"));
    }
}
