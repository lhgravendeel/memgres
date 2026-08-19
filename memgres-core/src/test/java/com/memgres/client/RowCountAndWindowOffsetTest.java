package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A row count is read before the rows are, and a window offset on the row it answers for.
 *
 * <p>The count a limit was written with settles how many rows come back before any of them is
 * looked at; WITH TIES then keeps whatever the sort cannot tell apart from the last of those. So
 * where the count is none there is no last row and nothing to be tied with it. memgres guarded the
 * tie search with {@code lim < size}, which a count of zero satisfies, and then read the row before
 * the first one — an IndexOutOfBounds the client saw as XX000, on every SELECT path.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class RowCountAndWindowOffsetTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_lt (id int, v int)");
            st.execute("INSERT INTO zz_lt VALUES (1, 10), (2, 20), (3, 20), (4, 30)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The first column of every row, rendered as text, in the order they came back. */
    private static List<String> col(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String v = rs.getString(1);
                out.add(rs.wasNull() ? null : v);
            }
        }
        return out;
    }

    private static String stateOf(String sql) {
        PSQLException e = assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
        return e.getSQLState();
    }

    @Test
    void limitOfNoneWithTiesKeepsNoRows() throws Exception {
        assertTrue(col("SELECT id FROM zz_lt ORDER BY id FETCH FIRST 0 ROWS WITH TIES").isEmpty());
        assertTrue(col("SELECT id, count(*) FROM zz_lt GROUP BY id ORDER BY id"
                + " FETCH FIRST 0 ROWS WITH TIES").isEmpty());
        assertTrue(col("SELECT 1 AS a ORDER BY a FETCH FIRST 0 ROWS WITH TIES").isEmpty());
        assertTrue(col("SELECT id, row_number() OVER (ORDER BY id) AS r FROM zz_lt ORDER BY id"
                + " FETCH FIRST 0 ROWS WITH TIES").isEmpty());
        assertTrue(col("SELECT generate_series(1, id) AS s FROM zz_lt ORDER BY s"
                + " FETCH FIRST 0 ROWS WITH TIES").isEmpty());
        assertTrue(col("SELECT id FROM zz_lt ORDER BY id OFFSET 1 FETCH FIRST 0 ROWS WITH TIES")
                .isEmpty());
        assertTrue(col("SELECT id FROM zz_lt UNION SELECT id FROM zz_lt ORDER BY id"
                + " FETCH FIRST 0 ROWS WITH TIES").isEmpty());
    }

    @Test
    void limitOfSomeKeepsEveryRowTiedWithTheLastOfThem() throws Exception {
        assertEquals(java.util.Arrays.asList("10"),
                col("SELECT v FROM zz_lt ORDER BY v FETCH FIRST 1 ROWS WITH TIES"));
        assertEquals(java.util.Arrays.asList("10", "20", "20"),
                col("SELECT v FROM zz_lt ORDER BY v FETCH FIRST 2 ROWS WITH TIES"));
        assertEquals(java.util.Arrays.asList("10", "20", "20"),
                col("SELECT v FROM zz_lt ORDER BY v FETCH FIRST 3 ROWS WITH TIES"));
        assertEquals(java.util.Arrays.asList("10", "20", "20", "30"),
                col("SELECT v FROM zz_lt ORDER BY v FETCH FIRST 10 ROWS WITH TIES"));
        assertEquals(java.util.Arrays.asList("30"),
                col("SELECT v FROM zz_lt ORDER BY v DESC FETCH FIRST 1 ROWS WITH TIES"));
        assertEquals(java.util.Arrays.asList("20", "20"),
                col("SELECT v FROM zz_lt ORDER BY v OFFSET 1 FETCH FIRST 1 ROWS WITH TIES"));
        assertTrue(col("SELECT v FROM zz_lt ORDER BY v OFFSET 10 FETCH FIRST 1 ROWS WITH TIES")
                .isEmpty());
    }

    /** A tie is a tie under the sort that was written, not under the whole row. */
    @Test
    void aTieIsATieUnderTheSortThatWasWritten() throws Exception {
        assertEquals(3, col("SELECT id FROM zz_lt ORDER BY v FETCH FIRST 2 ROWS WITH TIES").size());
        assertEquals(2,
                col("SELECT id FROM zz_lt ORDER BY v, id FETCH FIRST 2 ROWS WITH TIES").size());
    }

    @Test
    void countItselfIsReadAsACount() throws Exception {
        assertEquals(4, col("SELECT v FROM zz_lt ORDER BY v LIMIT ALL").size());
        assertEquals(4, col("SELECT v FROM zz_lt ORDER BY v LIMIT NULL").size());
        assertEquals(4, col("SELECT v FROM zz_lt ORDER BY v OFFSET NULL").size());
        assertEquals(1, col("SELECT v FROM zz_lt ORDER BY v FETCH FIRST ROW ONLY").size());
        assertEquals(2, col("SELECT v FROM zz_lt ORDER BY v FETCH NEXT 2 ROWS ONLY").size());
        assertEquals(2, col("SELECT v FROM zz_lt ORDER BY v LIMIT '2'").size());
        assertEquals(2, col("SELECT v FROM zz_lt ORDER BY v LIMIT (SELECT 2)").size());
        assertEquals("2201W", stateOf("SELECT v FROM zz_lt ORDER BY v LIMIT -1"));
        assertEquals("2201X", stateOf("SELECT v FROM zz_lt ORDER BY v OFFSET -1"));
        assertEquals("22P02", stateOf("SELECT v FROM zz_lt ORDER BY v LIMIT 'x'"));
        assertEquals("42601", stateOf("SELECT v FROM zz_lt LIMIT 1 WITH TIES"));
    }

    /** The offset is a value of the row the answer is being produced for. */
    @Test
    void windowOffsetMayBeAColumnOfTheRow() throws Exception {
        assertEquals(java.util.Arrays.asList(null, null, null),
                col("SELECT lag(v, v) OVER (ORDER BY v) FROM (VALUES (10), (20), (30)) t(v)"
                        + " ORDER BY 1"));
        assertEquals(java.util.Arrays.asList(null, null, null),
                col("SELECT lead(v, v) OVER (ORDER BY v) FROM (VALUES (10), (20), (30)) t(v)"
                        + " ORDER BY 1"));
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT id, lead(v, id, -1) OVER (ORDER BY id) FROM zz_lt ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(-1, rs.getInt(2));
        }
    }

    @Test
    void windowOffsetValueDecidesNothingAboutTheCall() throws Exception {
        assertEquals(java.util.Arrays.asList("10", "20", "20", "30"),
                col("SELECT lag(v, 0) OVER (ORDER BY v) FROM zz_lt ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("20", "20", "30", null),
                col("SELECT lag(v, -1) OVER (ORDER BY v) FROM zz_lt ORDER BY 1"));
        assertEquals(java.util.Arrays.asList(null, null, null, null),
                col("SELECT lag(v, NULL) OVER (ORDER BY v) FROM zz_lt ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("-1", "10", "20", "20"),
                col("SELECT lag(v, 1, -1) OVER (ORDER BY v) FROM zz_lt ORDER BY 1"));
    }

    /** A frame offset is a count the type can hold. */
    @Test
    void frameOffsetLargerThanBigintIsOutOfRange() throws Exception {
        assertEquals("22003", stateOf(
                "SELECT sum(v) OVER (ORDER BY v ROWS 99999999999999999999 PRECEDING) FROM zz_lt"));
        assertEquals("22003", stateOf(
                "SELECT sum(v) OVER (ORDER BY v ROWS 9223372036854775808 PRECEDING) FROM zz_lt"));
        assertEquals("22003", stateOf("SELECT sum(v) OVER (ORDER BY v"
                + " ROWS BETWEEN 1 PRECEDING AND 99999999999999999999 FOLLOWING) FROM zz_lt"));
        assertEquals("22013", stateOf(
                "SELECT sum(v) OVER (ORDER BY v ROWS -1 PRECEDING) FROM zz_lt"));
        assertEquals(4, col("SELECT sum(v) OVER (ORDER BY v ROWS 9223372036854775807 PRECEDING)"
                + " FROM zz_lt ORDER BY 1").size());
    }
}
