package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a query means is what the grammar says it means.
 *
 * <p>memgres read a written query by the shapes it expected rather than by the rules the grammar
 * lays down, so a membership test bound the wrong way round, an inheritance star and a named USING
 * clause were unreadable, LATERAL stood before anything at all, an ordering operator was consumed
 * without being looked up, a set operator between two empty select lists became a column name, a
 * ragged VALUES list was reported as a union nobody wrote, a select list grew past what a row can
 * hold, and a relation's untyped column was described by whichever value happened to be read
 * out of it.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class WrittenQueryGrammarTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_wg1 (id int, x int, a int)");
            st.execute("INSERT INTO zz_wg1 VALUES (1, 10, 5), (2, 20, 6)");
            st.execute("CREATE TABLE zz_wg2 (id int, y int)");
            st.execute("INSERT INTO zz_wg2 VALUES (1, 100), (3, 300)");
            st.execute("CREATE TABLE zz_wg3 (t text)");
            st.execute("INSERT INTO zz_wg3 VALUES ('p'), ('q')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String stateOf(String sql) {
        PSQLException e = assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
        return e.getSQLState();
    }

    private static String messageOf(String sql) {
        PSQLException e = assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
        return e.getServerErrorMessage().getMessage();
    }

    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static int rowCount(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) n++;
            return n;
        }
    }

    @Test
    void aMembershipTestBindsTighterThanAComparison() throws SQLException {
        // 1 = (1 IN (true)) has an integer on one side and a boolean on the other.
        assertEquals("42883", stateOf("SELECT 1 = 1 IN (true)"));
        // and the answer of a test is a boolean, whatever the operands were
        assertEquals("t", one("SELECT 1 IN (1) = true"));
        assertEquals("t", one("SELECT 1 IN (2) = false"));
        assertEquals("t", one("SELECT 1 IN (1) IN (true)"));
        assertEquals("t", one("SELECT NOT 1 IN (2)"));
        assertEquals("t", one("SELECT NOT 1 IN (2) AND true"));
    }

    @Test
    void anInListIsResolvedWholeBeforeItIsSearched() {
        assertEquals("42883", stateOf("SELECT 1 IN (1, true)"));
    }

    @Test
    void aRangeOrPatternTestBindsTighterThanAComparisonToo() throws SQLException {
        assertEquals("t", one("SELECT 2 BETWEEN 1 AND 3 = true"));
        assertEquals("t", one("SELECT 'ab' LIKE 'a%' = true"));
        assertEquals("t", one("SELECT 'AB' ILIKE 'a%' = true"));
        // but unlike a membership test, one of these may not be followed by another
        assertEquals("42601", stateOf("SELECT 1 BETWEEN 0 AND 2 BETWEEN 0 AND 2"));
    }

    @Test
    void anyAndAllEndInACloseParenSoAComparisonMayFollow() throws SQLException {
        assertEquals("t", one("SELECT 1 = ANY(ARRAY[1,2]) = true"));
        assertEquals("t", one("SELECT 1 = ALL(ARRAY[1,1]) = true"));
    }

    @Test
    void theInheritanceStarIsReadWhereItMayStand() throws SQLException {
        assertEquals(2, rowCount("SELECT id FROM zz_wg1 *"));
        assertEquals(2, rowCount("SELECT id FROM ONLY zz_wg1"));
        assertEquals(2, rowCount("TABLE ONLY zz_wg2"));
        assertEquals(2, rowCount("TABLE zz_wg2 *"));
        // ONLY takes no star
        assertEquals("42601", stateOf("SELECT id FROM ONLY zz_wg1 * a"));
    }

    @Test
    void lateralTakesASubqueryOrAFunctionAndNotARelation() {
        assertEquals("42601", stateOf("SELECT * FROM LATERAL zz_wg1"));
        assertEquals("42601", stateOf("SELECT * FROM zz_wg2, LATERAL zz_wg1"));
    }

    @Test
    void aUsingClauseMayBeNamedAndAnswersWithTheMergedColumns() throws SQLException {
        assertEquals("1", one("SELECT j.id FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j"));
        assertEquals(1, rowCount("SELECT j.* FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j"));
        assertEquals("integer",
                one("SELECT pg_typeof(j.id)::text FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j"));
        // the relations behind it go on answering to their own names
        assertEquals("10", one("SELECT a.x FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j"));
        assertEquals("100", one("SELECT b.y FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j"));
        // and what the join itself exposes is unchanged by the name
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT * FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j")) {
            assertEquals(4, rs.getMetaData().getColumnCount());
        }
        // a column the clause did not merge is a column the name has not got
        assertEquals("42703", stateOf("SELECT j.x FROM zz_wg1 a JOIN zz_wg2 b USING (id) AS j"));
        // and the name is only taken after AS
        assertEquals("42601", stateOf("SELECT j.id FROM zz_wg1 a JOIN zz_wg2 b USING (id) j"));
    }

    @Test
    void anOrderingOperatorHasToOrderAndHasToExist() throws SQLException {
        assertEquals("5", one("SELECT a FROM zz_wg1 ORDER BY a USING <"));
        assertEquals("6", one("SELECT a FROM zz_wg1 ORDER BY a USING >"));
        assertEquals("5", one("SELECT a FROM zz_wg1 ORDER BY a USING < NULLS FIRST"));
        assertEquals("42809", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING <="));
        assertEquals("42809", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING >="));
        assertEquals("42809", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING ="));
        assertEquals("42809", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING <>"));
        // an operator nothing defines for the type being sorted is a different complaint
        assertEquals("42883", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING @@"));
        assertEquals("operator does not exist: integer @@ integer",
                messageOf("SELECT a FROM zz_wg1 ORDER BY a USING @@"));
        assertEquals("42883", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING @>"));
        assertEquals("42883", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING ||"));
        // and one that does exist for the type has still not got the property a sort asks of it
        assertEquals("42809", stateOf("SELECT t FROM zz_wg3 ORDER BY t USING @@"));
        assertEquals("operator @@ is not a valid ordering operator",
                messageOf("SELECT t FROM zz_wg3 ORDER BY t USING @@"));
        assertEquals("42809", stateOf("SELECT t FROM zz_wg3 ORDER BY t USING ~~"));
        // the sort is read item by item
        assertEquals("42883", stateOf("SELECT a FROM zz_wg1 ORDER BY a USING <, x USING @@"));
    }

    @Test
    void aSelectMayHaveNoColumnsAtAll() throws SQLException {
        assertEquals(1, rowCount("SELECT"));
        assertEquals(1, rowCount("SELECT UNION SELECT"));
        assertEquals(2, rowCount("SELECT UNION ALL SELECT"));
        assertEquals(1, rowCount("SELECT INTERSECT SELECT"));
        assertEquals(0, rowCount("SELECT EXCEPT SELECT"));
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT UNION SELECT")) {
            assertEquals(0, rs.getMetaData().getColumnCount());
        }
    }

    @Test
    void everyRowOfAValuesListIsTheSameRelationsRow() throws SQLException {
        assertEquals("VALUES lists must all be the same length", messageOf("VALUES (1,2),(3)"));
        assertEquals("42601", stateOf("VALUES (1,2),(3)"));
        assertEquals("42601", stateOf("VALUES (1),(2,3)"));
        assertEquals(2, rowCount("VALUES (1,2),(3,4)"));
    }

    @Test
    void aSelectListReachesAsFarAsARowDoes() throws SQLException {
        StringBuilder wide = new StringBuilder("SELECT 1");
        for (int i = 1; i < 1664; i++) wide.append(", 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(wide.toString())) {
            assertEquals(1664, rs.getMetaData().getColumnCount());
        }
        wide.append(", 1");
        assertEquals("54011", stateOf(wide.toString()));
        assertEquals("target lists can have at most 1664 entries", messageOf(wide.toString()));
    }

    @Test
    void aRelationsUntypedColumnIsSettledAsText() throws SQLException {
        assertEquals("text", one("SELECT pg_typeof(column1)::text FROM (VALUES (NULL),(NULL)) v"));
        assertEquals("text", one("SELECT pg_typeof(column1)::text FROM (VALUES (NULL)) v"));
        assertEquals("text", one("SELECT pg_typeof(column1)::text FROM (VALUES (NULL),('x')) v"));
        assertEquals("text", one("SELECT pg_typeof(q)::text FROM (SELECT NULL AS q) s"));
        assertEquals("text", one(
                "SELECT pg_typeof(q)::text FROM (SELECT NULL AS q UNION ALL SELECT NULL) s"));
        // but a written NULL that no relation holds is unknown still
        assertEquals("unknown", one("SELECT pg_typeof(NULL)::text"));
    }
}
