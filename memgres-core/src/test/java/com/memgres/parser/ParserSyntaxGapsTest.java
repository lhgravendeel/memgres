package com.memgres.parser;

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
 * Syntax PostgreSQL 18 accepts and memgres rejected outright: a constraint deferred to commit, an
 * assignment to a slice of an array, an arithmetic LIMIT, and a temporal key. A parse error stops
 * an application at the door, so each of these is the whole feature rather than a detail of it.
 * Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>B1 column DEFERRABLE, B2 array slice assignment, B3 LIMIT expressions, B5 WITHOUT OVERLAPS.
 */
class ParserSyntaxGapsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
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

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ---- B1: column-level DEFERRABLE ----

    /** A deferred key is only checked at commit, so a transient duplicate is allowed. */
    @Test
    void aColumnPrimaryKeyCanBeDeferred() throws Exception {
        exec("DROP TABLE IF EXISTS psg_d CASCADE");
        exec("CREATE TABLE psg_d (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)");
        exec("BEGIN");
        exec("INSERT INTO psg_d VALUES (1)");
        exec("INSERT INTO psg_d VALUES (1)");
        assertEquals("23505", state("COMMIT"));
        exec("ROLLBACK");
        exec("DROP TABLE psg_d");
    }

    @Test
    void aColumnUniqueCanBeDeferred() throws Exception {
        exec("DROP TABLE IF EXISTS psg_d2 CASCADE");
        exec("CREATE TABLE psg_d2 (id int UNIQUE DEFERRABLE INITIALLY DEFERRED)");
        exec("DROP TABLE psg_d2");
    }

    @Test
    void aColumnReferenceCanBeDeferred() throws Exception {
        exec("DROP TABLE IF EXISTS psg_d3 CASCADE");
        exec("DROP TABLE IF EXISTS psg_dp CASCADE");
        exec("CREATE TABLE psg_dp (id int PRIMARY KEY)");
        exec("CREATE TABLE psg_d3 (p int REFERENCES psg_dp(id) DEFERRABLE INITIALLY DEFERRED)");
        exec("DROP TABLE psg_d3");
        exec("DROP TABLE psg_dp");
    }

    /** NOT DEFERRABLE is the default and still checks immediately. */
    @Test
    void aColumnKeyMarkedNotDeferrableStillChecksAtOnce() throws Exception {
        exec("DROP TABLE IF EXISTS psg_d4 CASCADE");
        exec("CREATE TABLE psg_d4 (id int PRIMARY KEY NOT DEFERRABLE)");
        exec("INSERT INTO psg_d4 VALUES (1)");
        assertEquals("23505", state("INSERT INTO psg_d4 VALUES (1)"));
        exec("DROP TABLE psg_d4");
    }

    // ---- B2: array slice assignment ----

    @Test
    void aSliceOfAnArrayCanBeAssigned() throws Exception {
        exec("DROP TABLE IF EXISTS psg_a CASCADE");
        exec("CREATE TABLE psg_a (id int PRIMARY KEY, a int[])");
        exec("INSERT INTO psg_a VALUES (1, ARRAY[1,2,3])");
        exec("UPDATE psg_a SET a[1:2] = ARRAY[7,8]");
        assertEquals("{7,8,3}", one("SELECT a::text FROM psg_a"));
        exec("DROP TABLE psg_a");
    }

    /** Assigning past the end extends the array, as PG does. */
    @Test
    void aSliceAssignmentCanExtendTheArray() throws Exception {
        exec("DROP TABLE IF EXISTS psg_a2 CASCADE");
        exec("CREATE TABLE psg_a2 (id int PRIMARY KEY, a int[])");
        exec("INSERT INTO psg_a2 VALUES (1, ARRAY[1,2])");
        exec("UPDATE psg_a2 SET a[3:4] = ARRAY[9,10]");
        assertEquals("{1,2,9,10}", one("SELECT a::text FROM psg_a2"));
        exec("DROP TABLE psg_a2");
    }

    /** Single-element assignment keeps working. */
    @Test
    void anArrayElementCanStillBeAssigned() throws Exception {
        exec("DROP TABLE IF EXISTS psg_a3 CASCADE");
        exec("CREATE TABLE psg_a3 (id int PRIMARY KEY, a int[])");
        exec("INSERT INTO psg_a3 VALUES (1, ARRAY[1,2,3])");
        exec("UPDATE psg_a3 SET a[2] = 9");
        assertEquals("{1,9,3}", one("SELECT a::text FROM psg_a3"));
        exec("DROP TABLE psg_a3");
    }

    @Test
    void aTextArraySliceAssignmentWorksToo() throws Exception {
        exec("DROP TABLE IF EXISTS psg_a4 CASCADE");
        exec("CREATE TABLE psg_a4 (id int PRIMARY KEY, a text[])");
        exec("INSERT INTO psg_a4 VALUES (1, ARRAY['a','b','c'])");
        exec("UPDATE psg_a4 SET a[2:3] = ARRAY['x','y']");
        assertEquals("{a,x,y}", one("SELECT a::text FROM psg_a4"));
        exec("DROP TABLE psg_a4");
    }

    // ---- B3: expressions in LIMIT and OFFSET ----

    @Test
    void limitAcceptsAnArithmeticExpression() throws Exception {
        assertEquals("3", one("SELECT count(*) FROM (SELECT generate_series(1,10) g LIMIT 2+1) s"));
    }

    @Test
    void offsetAcceptsAnArithmeticExpression() throws Exception {
        assertEquals("2", one("SELECT count(*) FROM (SELECT generate_series(1,10) g"
                + " ORDER BY g LIMIT 2 OFFSET 4*2) s"));
    }

    @Test
    void limitAcceptsAFunctionCall() throws Exception {
        assertEquals("2", one("SELECT count(*) FROM (SELECT generate_series(1,10) g"
                + " LIMIT greatest(1,2)) s"));
    }

    @Test
    void aPlainLimitStillWorks() throws Exception {
        assertEquals("4", one("SELECT count(*) FROM (SELECT generate_series(1,10) g LIMIT 4) s"));
    }

    @Test
    void limitAllStillWorks() throws Exception {
        assertEquals("10", one("SELECT count(*) FROM (SELECT generate_series(1,10) g LIMIT ALL) s"));
    }

    // ---- B5: temporal primary keys ----

    /** WITHOUT OVERLAPS makes the key unique per period rather than outright. */
    @Test
    void aTemporalPrimaryKeyRejectsAnOverlappingPeriod() throws Exception {
        exec("DROP TABLE IF EXISTS psg_t CASCADE");
        exec("CREATE TABLE psg_t (id int, valid daterange,"
                + " PRIMARY KEY (id, valid WITHOUT OVERLAPS))");
        exec("INSERT INTO psg_t VALUES (1,'[2020-01-01,2021-01-01)')");
        assertEquals("23P01", state("INSERT INTO psg_t VALUES (1,'[2020-06-01,2022-01-01)')"));
        exec("DROP TABLE psg_t");
    }

    @Test
    void aTemporalPrimaryKeyAllowsAdjacentPeriods() throws Exception {
        exec("DROP TABLE IF EXISTS psg_t2 CASCADE");
        exec("CREATE TABLE psg_t2 (id int, valid daterange,"
                + " PRIMARY KEY (id, valid WITHOUT OVERLAPS))");
        exec("INSERT INTO psg_t2 VALUES (1,'[2020-01-01,2021-01-01)')");
        exec("INSERT INTO psg_t2 VALUES (1,'[2021-01-01,2022-01-01)')");
        assertEquals("2", one("SELECT count(*) FROM psg_t2"));
        exec("DROP TABLE psg_t2");
    }

    @Test
    void aTemporalPrimaryKeyAllowsADifferentKey() throws Exception {
        exec("DROP TABLE IF EXISTS psg_t3 CASCADE");
        exec("CREATE TABLE psg_t3 (id int, valid daterange,"
                + " PRIMARY KEY (id, valid WITHOUT OVERLAPS))");
        exec("INSERT INTO psg_t3 VALUES (1,'[2020-01-01,2021-01-01)')");
        exec("INSERT INTO psg_t3 VALUES (2,'[2020-06-01,2022-01-01)')");
        assertEquals("2", one("SELECT count(*) FROM psg_t3"));
        exec("DROP TABLE psg_t3");
    }
}
