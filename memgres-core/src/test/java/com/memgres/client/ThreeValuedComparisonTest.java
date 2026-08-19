package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An unknown is what is left when nothing settled the answer.
 *
 * <p>A comparison against a set is settled by the set. An empty one settles it without comparing
 * anything — IN over nothing is false and ALL over nothing is true — so a null on the left leaves
 * nothing there to be unknown about. memgres answered from the left operand first, so
 * {@code x NOT IN (SELECT 1 WHERE false)} dropped the null rows a WHERE should have kept.
 *
 * <p>BETWEEN is shorthand for a pair of comparisons joined by AND, and that AND is the same one
 * every other pair is joined by: 1 is below 2 whatever the upper bound is. Answering unknown as
 * soon as any operand was null made {@code 1 BETWEEN 2 AND NULL} unknown where it is false.
 *
 * <p>A row is compared as a row: equal when every pair of members is, unequal when some pair is
 * not, unknown otherwise. Comparing rows with the equality of ordinary values made
 * {@code ROW(1,NULL) IN (ROW(1,2))} false where it is unknown, and
 * {@code ROW(1,NULL) IN (ROW(1,2), ROW(1,NULL))} true where it is unknown too.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class ThreeValuedComparisonTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE tv_a (x int)");
            st.execute("INSERT INTO tv_a VALUES (1), (2), (NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** "true", "false" or "unknown" — the three answers a comparison has. */
    private static String truth(String expr) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT (" + expr + ")::text")) {
            assertTrue(rs.next(), "expected a row from: " + expr);
            String v = rs.getString(1);
            return rs.wasNull() ? "unknown" : v;
        }
    }

    private static int count(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getInt(1);
        }
    }

    /** BETWEEN is a pair of comparisons, joined the way every pair is. */
    @Test
    void aComparisonThatCameOutFalseSettlesTheRangeWhateverTheOtherBoundDid() throws Exception {
        assertEquals("false", truth("1 BETWEEN 2 AND NULL"));
        assertEquals("false", truth("3 BETWEEN NULL AND 2"));
        assertEquals("true", truth("1 NOT BETWEEN 2 AND NULL"));
        assertEquals("true", truth("3 NOT BETWEEN NULL AND 2"));
    }

    /** A bound that leaves the answer open is still unknown, and so is an unknown value. */
    @Test
    void aBoundThatSettlesNothingLeavesTheRangeUnknown() throws Exception {
        assertEquals("unknown", truth("1 BETWEEN NULL AND 3"));
        assertEquals("unknown", truth("2 BETWEEN 1 AND NULL"));
        assertEquals("unknown", truth("NULL BETWEEN 1 AND 2"));
        assertEquals("unknown", truth("NULL BETWEEN NULL AND NULL"));
        assertEquals("true", truth("2 BETWEEN 1 AND 3"));
        assertEquals("false", truth("4 BETWEEN 1 AND 3"));
    }

    /** SYMMETRIC is the same pair written both ways round and joined by OR. */
    @Test
    void symmetricIsThatPairWrittenBothWaysRound() throws Exception {
        assertEquals("unknown", truth("1 BETWEEN SYMMETRIC 2 AND NULL"));
        assertEquals("unknown", truth("3 BETWEEN SYMMETRIC NULL AND 2"));
        assertEquals("true", truth("2 BETWEEN SYMMETRIC 3 AND 1"));
        assertEquals("false", truth("4 BETWEEN SYMMETRIC 3 AND 1"));
        assertEquals("true", truth("4 NOT BETWEEN SYMMETRIC 3 AND 1"));
    }

    /** An empty set settles the answer without comparing anything. */
    @Test
    void anEmptySetSettlesTheAnswerWithoutComparingAnything() throws Exception {
        assertEquals("false", truth("NULL::int IN (SELECT 1 WHERE false)"));
        assertEquals("true", truth("NULL::int NOT IN (SELECT 1 WHERE false)"));
        assertEquals("false", truth("NULL::int = ANY (SELECT 1 WHERE false)"));
        assertEquals("true", truth("NULL::int = ALL (SELECT 1 WHERE false)"));
        assertEquals("false", truth("NULL::int > ANY (SELECT 1 WHERE false)"));
        assertEquals("true", truth("NULL::int > ALL (SELECT 1 WHERE false)"));
        assertEquals("false", truth("NULL::int = ANY ('{}'::int[])"));
        assertEquals("true", truth("NULL::int = ALL ('{}'::int[])"));
    }

    /** Which is a row kept or dropped by a WHERE, not a curiosity of a select list. */
    @Test
    void aWhereKeepsTheRowsThatComparisonKeeps() throws Exception {
        assertEquals(3, count("SELECT count(*) FROM tv_a WHERE x NOT IN (SELECT 1 WHERE false)"));
        assertEquals(0, count("SELECT count(*) FROM tv_a WHERE x IN (SELECT 1 WHERE false)"));
        assertEquals(1, count("SELECT count(*) FROM tv_a WHERE x NOT IN (SELECT 1)"));
        assertEquals(1, count("SELECT count(*) FROM tv_a WHERE x IN (SELECT 1)"));
    }

    /** Once something is there to compare against, a null on the left is unknown again. */
    @Test
    void onceSomethingIsThereToCompareAgainstANullIsUnknown() throws Exception {
        assertEquals("unknown", truth("NULL::int IN (SELECT 1)"));
        assertEquals("unknown", truth("NULL::int NOT IN (SELECT 1)"));
        assertEquals("unknown", truth("NULL::int = ANY (SELECT 1)"));
        assertEquals("unknown", truth("NULL::int = ALL (SELECT 1)"));
        assertEquals("unknown", truth("NULL::int IN (1, 2)"));
        assertEquals("unknown", truth("NULL::int = ANY (ARRAY[1, 2])"));
        assertEquals("unknown", truth("NULL::int = ALL (ARRAY[1, 2])"));
    }

    /** And a comparison that came out true or false settles it whatever the unknown ones did. */
    @Test
    void aSettledComparisonOutweighsTheUnknownOnes() throws Exception {
        assertEquals("true", truth("1 = ANY (SELECT v FROM (VALUES (NULL::int), (1)) t(v))"));
        assertEquals("false", truth("1 = ALL (SELECT v FROM (VALUES (NULL::int), (2)) t(v))"));
        assertEquals("unknown", truth("1 = ANY (SELECT v FROM (VALUES (NULL::int), (2)) t(v))"));
        assertEquals("true", truth("1 IN (SELECT v FROM (VALUES (NULL::int), (1)) t(v))"));
        assertEquals("unknown", truth("1 NOT IN (SELECT v FROM (VALUES (NULL::int), (2)) t(v))"));
    }

    /** A row is compared as a row, so a null member leaves the pair open. */
    @Test
    void aRowIsComparedAsARow() throws Exception {
        assertEquals("unknown", truth("ROW(1, NULL) IN (ROW(1, 2))"));
        assertEquals("unknown", truth("ROW(1, NULL) IN (ROW(1, 2), ROW(1, NULL))"));
        assertEquals("false", truth("ROW(1, NULL) IN (ROW(2, 2))"));
        assertEquals("unknown", truth("ROW(NULL::int, 1) IN (ROW(1, 2), ROW(2, 1))"));
        assertEquals("true", truth("ROW(NULL::int, 1) NOT IN (ROW(1, 2))"));
        assertEquals("unknown", truth("ROW(1, NULL) = ROW(1, 2)"));
        assertEquals("false", truth("ROW(1, NULL) = ROW(2, 2)"));
        assertEquals("true", truth("ROW(1, NULL) <> ROW(2, 2)"));
    }

    /** The same rows, read out of a subquery instead of written out. */
    @Test
    void theSameRowsReadOutOfASubquery() throws Exception {
        assertEquals("false", truth("(NULL::int, 1) IN (SELECT 1, 2)"));
        assertEquals("true", truth("(NULL::int, 1) NOT IN (SELECT 1, 2)"));
        assertEquals("unknown", truth("(NULL::int, 1) IN (SELECT 1, 1)"));
        assertEquals("unknown", truth("(1, NULL::int) IN (SELECT 1, 2)"));
        assertEquals("false", truth("(1, NULL::int) IN (SELECT 2, 2)"));
        assertEquals("unknown",
                truth("(NULL::int, 1) IN (SELECT v, w FROM (VALUES (1,2),(2,1)) t(v,w))"));
        assertEquals("false",
                truth("(NULL::int, 1) IN (SELECT v, w FROM (VALUES (1,2),(2,2)) t(v,w))"));
    }

    /**
     * The ANY spelling over an array is the record type's own equality, which reads a null as a
     * value like any other rather than as something unknown.
     */
    @Test
    void theAnySpellingOverAnArrayIsTheOrdinaryEqualityOfTheRecordType() throws Exception {
        assertEquals("false", truth("ROW(1, NULL::int) = ANY (ARRAY[ROW(1, 2)])"));
        assertEquals("true", truth("ARRAY[1, NULL] = ARRAY[1, NULL]"));
        assertEquals("false", truth("ARRAY[1, NULL] = ARRAY[1, 2]"));
        assertEquals("true", truth("ARRAY[1, NULL] IN (ARRAY[1, NULL])"));
        assertEquals("false", truth("ARRAY[1, NULL] IN (ARRAY[1, 2])"));
    }
}
