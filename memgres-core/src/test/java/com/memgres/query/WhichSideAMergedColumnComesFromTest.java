package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which side a USING column takes its value from, and what a call in FROM says about its type.
 *
 * <p>A merged column is not either side's column: the two settle on one type both can be read as,
 * and the value comes from whichever side already holds it, because there is nothing to convert
 * there. Taken from the left whatever its type, an int joined to a numeric showed the int's
 * {@code 3} where PostgreSQL shows the numeric's {@code 3.00} -- the same number, written as the
 * column's own type says it is written. Where neither side holds the settled type exactly -- both
 * carry a modifier, or neither is the wider one -- the left supplies it as before.
 *
 * <p>A call in FROM settles the type of what it produces, and where the engine worked that out it
 * is recorded beside the column rather than guessed from the value. That is a declaration like any
 * other: passed over because the binding is a function result, {@code unnest} of a text array
 * offered nothing, so {@code text @> text} was resolved from the shapes of the two strings and
 * answered where PostgreSQL has no such operator.
 */
class WhichSideAMergedColumnComesFromTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The side that already holds the settled type supplies the merged value. */
    @Test
    void whichSideSuppliesTheMergedValue() throws SQLException {
        exec("CREATE TABLE zws_a (n numeric(10,2))");
        exec("CREATE TABLE zws_b (n numeric(10,4))");
        exec("CREATE TABLE zws_c (n numeric)");
        exec("CREATE TABLE zws_d (n int)");
        exec("INSERT INTO zws_a VALUES (3.00)");
        exec("INSERT INTO zws_b VALUES (3.0000)");
        exec("INSERT INTO zws_c VALUES (3.000000)");
        exec("INSERT INTO zws_d VALUES (3)");
        // numeric(10,2) beside numeric: the bare numeric holds the settled type, either way round.
        assertEquals("3.000000", one("SELECT n::text FROM zws_a JOIN zws_c USING (n)"));
        assertEquals("3.000000", one("SELECT n::text FROM zws_c JOIN zws_a USING (n)"));
        // int beside numeric: the numeric holds it.
        assertEquals("3.000000", one("SELECT n::text FROM zws_d JOIN zws_c USING (n)"));
        assertEquals("3.000000", one("SELECT n::text FROM zws_c JOIN zws_d USING (n)"));
        // Two modifiers, or neither side the wider type: the left supplies the value.
        assertEquals("3.00", one("SELECT n::text FROM zws_a JOIN zws_b USING (n)"));
        assertEquals("3.0000", one("SELECT n::text FROM zws_b JOIN zws_a USING (n)"));
        assertEquals("3.00", one("SELECT n::text FROM zws_a JOIN zws_d USING (n)"));
        assertEquals("3", one("SELECT n::text FROM zws_d JOIN zws_a USING (n)"));
        // Whichever side it comes from, the column is the type the two settled on.
        assertEquals("numeric", one("SELECT pg_typeof(n)::text FROM zws_a JOIN zws_c USING (n)"));
        assertEquals("numeric", one("SELECT pg_typeof(n)::text FROM zws_d JOIN zws_a USING (n)"));
        exec("DROP TABLE zws_a");
        exec("DROP TABLE zws_b");
        exec("DROP TABLE zws_c");
        exec("DROP TABLE zws_d");
    }

    /** A call in FROM says what its column is, and the operator is resolved against that. */
    @Test
    void whatACallInFromSaysAboutItsColumn() throws SQLException {
        assertEquals("42883",
                stateOf("SELECT u @> v FROM unnest(ARRAY['{a,b}']) AS u, unnest(ARRAY['{a}']) AS v"));
        assertEquals("42883", stateOf("SELECT u <@ v FROM unnest(ARRAY['{a}']) AS u,"
                + " unnest(ARRAY['{a,b}']) AS v"));
        // A call over a real array still produces the element type, and its operators resolve.
        assertEquals("text", one("SELECT pg_typeof(u)::text FROM unnest(ARRAY['a','b']) AS u"
                + " LIMIT 1"));
        assertEquals("2", one("SELECT count(*)::text FROM unnest(ARRAY['a','b']) AS u"
                + " WHERE u > ''"));
        assertEquals("integer", one("SELECT pg_typeof(g)::text FROM generate_series(1,2) AS g"
                + " LIMIT 1"));
        assertEquals("3", one("SELECT sum(g)::text FROM generate_series(1,2) AS g"));
    }
}
