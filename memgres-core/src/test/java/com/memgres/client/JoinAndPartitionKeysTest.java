package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Matching rows to one another by value: what a join agrees on and what a window is cut into.
 *
 * <p>Both read a key by printing the value and comparing the print, and printing is not equality.
 * A numeric 1.0 and a numeric 1.00 are the same number written two ways, so a USING join over them
 * answered no rows and {@code PARTITION BY} put each in a partition of its own. A
 * {@code character(n)} compares without the blanks its declaration padded it out to, so char(3)
 * 'a' and char(6) 'a' did not match either. And the parts of a composite key were run together
 * with a separator character, so a value holding that character reached into its neighbour.
 *
 * <p>Any join beyond a thousand pairs took a second execution of the whole join, which read its
 * keys differently again and skipped the comparison the first one made, so the same query answered
 * differently on either side of that size.
 */
class JoinAndPartitionKeysTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE jk_one (n numeric(10,1), t text)");
            st.execute("CREATE TABLE jk_two (n numeric(10,2), t text)");
            st.execute("INSERT INTO jk_one VALUES (1.0, 'L'), (3.0, 'Lonly')");
            st.execute("INSERT INTO jk_two VALUES (1.00, 'R'), (4.00, 'Ronly')");
            // A NATURAL join takes every name both sides hold, so these share only the key.
            st.execute("CREATE TABLE jk_nat_one (n numeric(10,1))");
            st.execute("CREATE TABLE jk_nat_two (n numeric(10,2))");
            st.execute("INSERT INTO jk_nat_one VALUES (1.0)");
            st.execute("INSERT INTO jk_nat_two VALUES (1.00)");

            st.execute("CREATE TABLE jk_c3 (n char(3), t text)");
            st.execute("CREATE TABLE jk_c6 (n char(6), t text)");
            st.execute("INSERT INTO jk_c3 VALUES ('a', 'L')");
            st.execute("INSERT INTO jk_c6 VALUES ('a', 'R')");

            // Forty rows either side is sixteen hundred pairs, which is past the point a join
            // stops walking the right relation and indexes it instead.
            st.execute("CREATE TABLE jk_wide_one (n numeric(10,1), i int)");
            st.execute("CREATE TABLE jk_wide_two (n numeric(10,2), i int)");
            st.execute("INSERT INTO jk_wide_one SELECT 1.0, g FROM generate_series(1,40) g");
            st.execute("INSERT INTO jk_wide_two SELECT 1.00, g FROM generate_series(1,40) g");

            st.execute("CREATE TABLE jk_part (n numeric(10,2))");
            st.execute("INSERT INTO jk_part VALUES (1.0), (1.00), (2.0)");
            st.execute("CREATE TABLE jk_sep (a text, b text)");
            st.execute("INSERT INTO jk_sep VALUES ('a', 'b'), ('a' || chr(1) || 'b', '')");

            st.execute("CREATE TABLE jk_lat_a (id int, v int)");
            st.execute("CREATE TABLE jk_lat_b (id int, w int)");
            st.execute("INSERT INTO jk_lat_a VALUES (1, 10), (2, 20)");
            st.execute("INSERT INTO jk_lat_b VALUES (1, 100), (2, 200)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
            return out;
        }
    }

    private static String scalar(String sql) throws SQLException {
        List<String> r = rows(sql);
        assertEquals(1, r.size(), "expected one row from: " + sql);
        return r.get(0);
    }

    // ------------------------------------------------------- the same number written two ways

    /** A numeric is the number it holds, not the digits it was stored with. */
    @Test
    void aUsingJoinMatchesTwoNumericsOfDifferentScale() throws Exception {
        assertEquals(List.of("1.0|L|R"),
                rows("SELECT n, jk_one.t, jk_two.t FROM jk_one JOIN jk_two USING (n)"));
        assertEquals(List.of("1.0"),
                rows("SELECT n FROM jk_nat_one NATURAL JOIN jk_nat_two"));
    }

    /** The same, written as a condition rather than as a merged column. */
    @Test
    void anOnConditionMatchesTwoNumericsOfDifferentScale() throws Exception {
        assertEquals(List.of("L|R"),
                rows("SELECT jk_one.t, jk_two.t FROM jk_one JOIN jk_two ON jk_one.n = jk_two.n"));
    }

    /** An outer join finds the match too, so the row is not padded with NULLs instead. */
    @Test
    void anOuterUsingJoinFindsTheMatchRatherThanPaddingIt() throws Exception {
        assertEquals(List.of("1.0|L|R", "3.0|Lonly|NULL"),
                rows("SELECT n, jk_one.t, jk_two.t FROM jk_one LEFT JOIN jk_two USING (n) ORDER BY n"));
        assertEquals(List.of("1.00|L|R", "4.00|NULL|Ronly"),
                rows("SELECT n, jk_one.t, jk_two.t FROM jk_one RIGHT JOIN jk_two USING (n) ORDER BY n"));
        assertEquals(List.of("1.0|L|R", "3.0|Lonly|NULL", "4.00|NULL|Ronly"),
                rows("SELECT n, jk_one.t, jk_two.t FROM jk_one FULL JOIN jk_two USING (n) ORDER BY n"));
    }

    // ------------------------------------------------------- the merged column's own value

    /**
     * A merged column answers with whichever side is not null, and the two sides of a match are
     * equal without being written alike. PostgreSQL asks the left side first everywhere but a
     * RIGHT join, which asks the side whose rows are all kept.
     */
    @Test
    void aRightJoinMergedColumnAnswersWithTheRightSideValue() throws Exception {
        assertEquals("1.0|L|R",
                scalar("SELECT n, jk_one.t, jk_two.t FROM jk_two RIGHT JOIN jk_one USING (n) WHERE n = 1"));
        assertEquals("1.00|L|R",
                scalar("SELECT n, jk_one.t, jk_two.t FROM jk_one RIGHT JOIN jk_two USING (n) WHERE n = 1"));
    }

    /** Every other join type asks the left side first. */
    @Test
    void everyOtherJoinTypeMergedColumnAnswersWithTheLeftSideValue() throws Exception {
        assertEquals("1.00", scalar("SELECT n FROM jk_two JOIN jk_one USING (n) WHERE n = 1"));
        assertEquals("1.00", scalar("SELECT n FROM jk_two LEFT JOIN jk_one USING (n) WHERE n = 1"));
        assertEquals("1.0", scalar("SELECT n FROM jk_one FULL JOIN jk_two USING (n) WHERE n = 1"));
    }

    // ------------------------------------------------------- blank-padded characters

    /** A bpchar's padding is not part of what it compares as. */
    @Test
    void aUsingJoinMatchesTwoBpcharsOfDifferentWidth() throws Exception {
        assertEquals(List.of("L|R"),
                rows("SELECT jk_c3.t, jk_c6.t FROM jk_c3 JOIN jk_c6 USING (n)"));
        assertEquals(List.of("L|R"),
                rows("SELECT jk_c3.t, jk_c6.t FROM jk_c3 JOIN jk_c6 ON jk_c3.n = jk_c6.n"));
    }

    /** The merged column keeps the type and the width the left side declared. */
    @Test
    void theMergedBpcharColumnIsStillABpchar() throws Exception {
        assertEquals("character|1", scalar(
                "SELECT pg_typeof(n)::text, length(n) FROM jk_c3 JOIN jk_c6 USING (n)"));
    }

    // ------------------------------------------------------- past the size an index takes over

    /** A join too large to walk finds exactly the rows the same join small enough to walk finds. */
    @Test
    void anIndexedJoinFindsWhatTheWalkedOneFinds() throws Exception {
        assertEquals("1600", scalar("SELECT count(*) FROM jk_wide_one JOIN jk_wide_two USING (n)"));
        assertEquals("1600", scalar("SELECT count(*) FROM jk_wide_one LEFT JOIN jk_wide_two USING (n)"));
        assertEquals("1600",
                scalar("SELECT count(*) FROM jk_wide_one JOIN jk_wide_two ON jk_wide_one.n = jk_wide_two.n"));
        assertEquals("1600",
                scalar("SELECT count(*) FROM jk_wide_one LEFT JOIN jk_wide_two ON jk_wide_one.n = jk_wide_two.n"));
    }

    /**
     * An indexed join applies the condition it was given rather than only the equality it indexed
     * on, so a condition holding more than an equality still narrows what it finds.
     */
    @Test
    void anIndexedJoinStillAppliesTheRestOfItsCondition() throws Exception {
        assertEquals("40", scalar(
                "SELECT count(*) FROM jk_wide_one a JOIN jk_wide_two b ON a.n = b.n AND a.i = b.i"));
    }

    /** A condition that is not a predicate is refused whatever size the join is. */
    @Test
    void anIndexedJoinStillRefusesANonBooleanCondition() {
        SQLException e = assertThrows(SQLException.class, () -> rows(
                "SELECT count(*) FROM jk_wide_one a JOIN jk_wide_two b ON a.i"));
        assertEquals("42804", e.getSQLState());
    }

    // ------------------------------------------------------- window partitions

    /** A partition holds the rows whose keys are equal, not the rows whose keys print alike. */
    @Test
    void aPartitionByGroupsTwoNumericsOfDifferentScale() throws Exception {
        assertEquals(List.of("1.00|2", "1.00|2", "2.00|1"),
                rows("SELECT n, count(*) OVER (PARTITION BY n) FROM jk_part ORDER BY n"));
    }

    /** Two parts of a composite key stay two parts however the values are spelled. */
    @Test
    void aCompositePartitionKeyDoesNotRunItsPartsTogether() throws Exception {
        assertEquals(List.of("1|1", "3|1"),
                rows("SELECT length(a), count(*) OVER (PARTITION BY a, b) FROM jk_sep ORDER BY 1"));
    }

    /** A null is not a value, and two of them still fall in one partition. */
    @Test
    void aPartitionByGroupsNullsTogether() throws Exception {
        assertEquals(List.of("NULL|2", "NULL|2", "1|1"), rows(
                "SELECT v, count(*) OVER (PARTITION BY v) FROM (VALUES (NULL::int), (NULL), (1)) t(v)"
                        + " ORDER BY v NULLS FIRST"));
    }

    // ------------------------------------------------------- a LATERAL inside parentheses

    /**
     * Parentheses do not stop a lateral reference: the item reads what is to the left of the
     * whole join it is written inside, not only what is inside the parentheses with it.
     */
    @Test
    void aLateralInsideAParenthesisedJoinReadsPastIt() throws Exception {
        assertEquals(List.of("1|100|110", "2|200|220"), rows(
                "SELECT a.id, b.w, s.x FROM jk_lat_a a"
                        + " JOIN (jk_lat_b b JOIN LATERAL (SELECT a.v + b.w AS x) s ON true)"
                        + " ON a.id = b.id ORDER BY 1"));
    }

    /** The same written with a comma, which is the same join. */
    @Test
    void aLateralInsideAParenthesisedJoinReadsPastACommaToo() throws Exception {
        assertEquals(List.of("1|100|110", "2|200|220"), rows(
                "SELECT a.id, b.w, s.x FROM jk_lat_a a,"
                        + " (jk_lat_b b JOIN LATERAL (SELECT a.v + b.w AS x) s ON true)"
                        + " WHERE a.id = b.id ORDER BY 1"));
    }

    /** A function in FROM reads past the parentheses the same way a LATERAL sub-select does. */
    @Test
    void aFunctionInsideAParenthesisedJoinReadsPastIt() throws Exception {
        assertEquals(List.of("1|1", "2|1", "2|2"), rows(
                "SELECT a.id, t.g FROM jk_lat_a a"
                        + " JOIN (jk_lat_b b CROSS JOIN LATERAL generate_series(1, a.v / 10) t(g))"
                        + " ON a.id = b.id ORDER BY 1, 2"));
    }

    /** The left rows an outer join keeps are kept whether or not the lateral item answered. */
    @Test
    void anOuterJoinOverAParenthesisedLateralKeepsItsLeftRows() throws Exception {
        assertEquals(List.of("1|10", "2|20"), rows(
                "SELECT a.id, s.x FROM jk_lat_a a"
                        + " LEFT JOIN (jk_lat_b b JOIN LATERAL (SELECT a.v AS x) s ON true)"
                        + " ON a.id = b.id ORDER BY 1"));
    }

    /** An arm that is not lateral still may not reach outside itself. */
    @Test
    void anOrdinaryArmStillMayNotReadNamesToItsLeft() {
        SQLException e = assertThrows(SQLException.class, () -> rows(
                "SELECT a.id FROM jk_lat_a a"
                        + " JOIN (jk_lat_b b JOIN jk_lat_b c ON a.v = c.w) ON a.id = b.id"));
        assertEquals("42P01", e.getSQLState());
    }
}
