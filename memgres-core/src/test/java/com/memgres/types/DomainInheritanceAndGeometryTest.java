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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A domain built on another domain inherits its constraints, and a geometric shape only has
 * the functions and operators PostgreSQL actually defines for it. Expectations captured from a
 * live PostgreSQL 18.0 server.
 *
 * <p>N22 domain-over-domain constraints, N59 phantom geometric functions and operators.
 */
class DomainInheritanceAndGeometryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE DOMAIN dg_pos AS int CHECK (VALUE > 0)");
        exec("CREATE DOMAIN dg_small AS dg_pos CHECK (VALUE < 100)");
        exec("CREATE DOMAIN dg_nn AS int NOT NULL");
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

    private static SQLException error(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql));
    }

    // ------------------------------------------------------------------
    // N22 — a domain inherits the constraints of the domain it is built on
    // ------------------------------------------------------------------

    @Test
    void theBaseDomainsConstraintIsEnforcedAndNamed() {
        SQLException e = error("SELECT (-5)::dg_small");
        assertEquals("23514", e.getSQLState());
        assertTrue(e.getMessage().contains("dg_pos_check"),
                "PG reports the innermost violated constraint, was: " + e.getMessage());
    }

    @Test
    void theOwnConstraintIsStillEnforced() {
        SQLException e = error("SELECT 500::dg_small");
        assertEquals("23514", e.getSQLState());
        assertTrue(e.getMessage().contains("dg_small_check"), e.getMessage());
    }

    @Test
    void aValueSatisfyingBothIsAccepted() throws Exception {
        assertEquals("50", expr("SELECT 50::dg_small::text"));
    }

    @Test
    void aNotNullDomainRejectsNullThroughACast() {
        SQLException e = error("SELECT NULL::dg_nn");
        assertEquals("23502", e.getSQLState());
    }

    @Test
    void columnAssignmentThroughADomainChainIsAlsoChecked() throws Exception {
        exec("CREATE TABLE dg_t (id int PRIMARY KEY, v dg_small)");
        exec("INSERT INTO dg_t VALUES (1, 50)");
        assertEquals("23514", error("INSERT INTO dg_t VALUES (2, -5)").getSQLState());
        assertEquals("23514", error("INSERT INTO dg_t VALUES (3, 500)").getSQLState());
        assertEquals("1", expr("SELECT count(*)::text FROM dg_t"));
    }

    // ------------------------------------------------------------------
    // N59 — geometric functions and operators PG does not define
    // ------------------------------------------------------------------

    @Test
    void centerExistsOnlyForBoxAndCircle() {
        assertEquals("42883", error("SELECT center(polygon '((0,0),(1,0),(1,1))')").getSQLState());
        assertEquals("42883", error("SELECT center(lseg '((0,0),(1,1))')").getSQLState());
    }

    @Test
    void centerStillWorksForBoxAndCircle() throws Exception {
        assertEquals("(1,1)", expr("SELECT center(box '((0,0),(2,2))')::text"));
        assertEquals("(1,1)", expr("SELECT center(circle '<(1,1),5>')::text"));
    }

    @Test
    void containmentNeedsAContainerAndARegion() {
        assertEquals("42883",
                error("SELECT (box '((0,0),(2,2))' @> lseg '((0,0),(1,1))')").getSQLState());
        assertEquals("42883", error("SELECT (line '{1,0,0}' @> point '(0,0)')").getSQLState());
    }

    /**
     * A closed path and a polygon print identically, so only the declared type of the argument
     * can pick the overload.
     */
    @Test
    void areaExistsForBoxCircleAndPathButNotPolygon() throws Exception {
        assertEquals("42883", error("SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))')").getSQLState());
        assertEquals("42883", error("SELECT area(lseg '[(0,0),(1,1)]')").getSQLState());
        assertEquals("12", expr("SELECT area(path '((0,0),(4,0),(4,3),(0,3))')::text"));
        assertEquals("12", expr("SELECT area(box '((0,0),(4,3))')::text"));
        assertEquals("12.566370614359172", expr("SELECT area(circle '<(0,0),2>')::text"));
    }

    @Test
    void aColumnCarriesItsDeclaredGeometricType() throws Exception {
        exec("CREATE TABLE dg_g (id int PRIMARY KEY, pg polygon, pa path, bx box)");
        exec("INSERT INTO dg_g VALUES (1, '((0,0),(4,0),(4,3),(0,3))',"
                + " '((0,0),(4,0),(4,3),(0,3))', '((0,0),(4,3))')");

        assertEquals("42883", error("SELECT area(pg) FROM dg_g").getSQLState());
        assertEquals("12", expr("SELECT area(pa)::text FROM dg_g"));
        assertEquals("12", expr("SELECT area(bx)::text FROM dg_g"));
    }

    @Test
    void theRealContainmentOperatorsStillWork() throws Exception {
        assertEquals("t", expr("SELECT (box '((0,0),(2,2))' @> point '(1,1)')"));
        assertEquals("t", expr("SELECT (polygon '((0,0),(2,0),(2,2))' @> point '(1,0.5)')"));
        assertEquals("t", expr("SELECT (circle '<(0,0),5>' @> point '(1,1)')"));
    }
}
