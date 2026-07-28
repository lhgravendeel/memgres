package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PostgreSQL's operator set is narrower than the values suggest. A geometric value is stored as
 * text, so only the declared type of an operand can decide which operator applies — a
 * value-level rule would catch ordinary string comparison too. Expectations captured from a
 * live PostgreSQL 18.0 server.
 *
 * <p>N59 phantom geometric operators, N63 money compared with numeric.
 */
class PhantomOperatorsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE pho (id int PRIMARY KEY, p point, l lseg, b box)");
        exec("INSERT INTO pho VALUES (1,'(1,2)','[(0,0),(1,1)]','((0,0),(2,2))')");
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

    private static String expr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N59 — a point has no equality operator
    // ------------------------------------------------------------------

    @Test
    void pointHasNoEqualityOperator() throws SQLException {
        assertEquals("42883", state("SELECT (point '(1,2)' = point '(1,2)')"));
        // "<>" IS a real point operator in PG -- pg_operator lists it -- so only "=" is missing,
        // and point equality is spelled ~=
        assertEquals("true", expr("SELECT (point '(1,2)' <> point '(3,4)')::text"));
        assertEquals("true", expr("SELECT (point '(1,2)' ~= point '(1,2)')::text"));
        // a polygon has neither
        assertEquals("42883",
                state("SELECT (polygon '((0,0),(1,1))' = polygon '((0,0),(1,1))')"));
        assertEquals("42883",
                state("SELECT (polygon '((0,0),(1,1))' <> polygon '((0,0),(2,2))')"));
    }

    /** A column carries its declared type into the operator resolution too. */
    @Test
    void aPointColumnIsAlsoRejected() {
        assertEquals("42883", state("SELECT (p = p) FROM pho"));
    }

    @Test
    void tildeEqualsIsThePointComparison() throws Exception {
        assertEquals("t", expr("SELECT (point '(1,2)' ~= point '(1,2)')"));
        assertEquals("f", expr("SELECT (point '(1,2)' ~= point '(3,4)')"));
    }

    // ------------------------------------------------------------------
    // N59 — an lseg contains nothing
    // ------------------------------------------------------------------

    @Test
    void lsegContainsNothing() {
        assertEquals("42883", state("SELECT (lseg '[(0,0),(1,1)]' @> point '(0,0)')"));
        assertEquals("42883", state("SELECT (l @> point '(0,0)') FROM pho"));
    }

    /** An open path has the same text as an lseg but does have the operator. */
    @Test
    void anOpenPathStillContainsAPoint() throws Exception {
        assertEquals("t", expr("SELECT (path '[(0,0),(1,1)]' @> point '(0,0)')"));
        assertEquals("t", expr("SELECT (box '((0,0),(2,2))' @> point '(1,1)')"));
        assertEquals("t", expr("SELECT (b @> point '(1,1)') FROM pho"));
    }

    // ------------------------------------------------------------------
    // N63 — money compares only with money
    // ------------------------------------------------------------------

    @Test
    void moneyDoesNotCompareWithOtherNumericTypes() {
        assertEquals("42883", state("SELECT ('1'::money = 1::numeric)"));
        assertEquals("42883", state("SELECT ('1'::money = 1::int)"));
        assertEquals("42883", state("SELECT (1::numeric < '1'::money)"));
    }

    @Test
    void moneyComparedWithMoneyStillWorks() throws Exception {
        assertEquals("t", expr("SELECT ('1'::money = '1'::money)"));
        assertEquals("t", expr("SELECT ('1'::money < '2'::money)"));
    }

    // ------------------------------------------------------------------
    // Operators PG does define must be untouched
    // ------------------------------------------------------------------

    @Test
    void realOperatorsAreNotAffected() throws Exception {
        assertEquals("t", expr("SELECT (box '((0,0),(2,2))' = box '((0,0),(2,2))')"));
        assertEquals("t", expr("SELECT ('(1,2)'::text = '(1,2)'::text)"));
        assertEquals("t", expr("SELECT ('abc' = 'abc')"));
        assertEquals("t", expr("SELECT (1 = 1)"));
    }
}
