package com.memgres.constraints;

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
 * Guards that PostgreSQL applies before a statement runs. Each one exists to reject something, so
 * accepting it is the failure that matters: a test suite asserting that a read-only transaction
 * holds, or that MATCH FULL rejects a half-filled key, passes against a database that never
 * checks. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>A4 read-only transactions, A5 MATCH FULL, A6 WITH TIES, A9 untyped record.
 */
class StatementValidationGuardsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE svg_p (a int, b int, PRIMARY KEY (a,b))");
        exec("INSERT INTO svg_p VALUES (1,1)");
    }

    /**
     * A child table of its own per check, so one insert cannot colour another's row count, and a
     * connection of its own for the transaction checks, so an aborted transaction stays local.
     */
    private static String child(String name, String matchClause) throws SQLException {
        exec("DROP TABLE IF EXISTS " + name);
        exec("CREATE TABLE " + name + " (x int, y int,"
                + " FOREIGN KEY (x,y) REFERENCES svg_p(a,b) " + matchClause + ")");
        return name;
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static String stateOn(Connection c, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> {
            try (Statement s = c.createStatement()) { s.execute(sql); }
        });
        return e.getSQLState();
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

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    private static int count(String table) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    // ---- A5: MATCH FULL ----

    /** MATCH FULL wants the whole key or none of it; half a key references nothing. */
    @Test
    void matchFullRejectsAPartiallyNullKey() throws Exception {
        String t = child("svg_f1", "MATCH FULL");
        assertEquals("23503", state("INSERT INTO " + t + " VALUES (1,NULL)"));
        assertEquals("23503", state("INSERT INTO " + t + " VALUES (NULL,1)"));
        assertEquals(0, count(t));
    }

    @Test
    void matchFullAcceptsAnAllNullKey() throws Exception {
        String t = child("svg_f2", "MATCH FULL");
        exec("INSERT INTO " + t + " VALUES (NULL,NULL)");
        assertEquals(1, count(t));
    }

    @Test
    void matchFullStillAcceptsAKeyThatExists() throws Exception {
        String t = child("svg_f3", "MATCH FULL");
        exec("INSERT INTO " + t + " VALUES (1,1)");
        assertEquals(1, count(t));
    }

    @Test
    void matchFullStillRejectsAKeyThatDoesNot() throws Exception {
        String t = child("svg_f4", "MATCH FULL");
        assertEquals("23503", state("INSERT INTO " + t + " VALUES (9,9)"));
    }

    /** The default, MATCH SIMPLE, lets any NULL stand for "no reference". */
    @Test
    void matchSimpleAcceptsAPartiallyNullKey() throws Exception {
        String t = child("svg_s1", "");
        exec("INSERT INTO " + t + " VALUES (1,NULL)");
        assertEquals(1, count(t));
    }

    @Test
    void matchSimpleStillRejectsAFullyPresentMissingKey() throws Exception {
        String t = child("svg_s2", "");
        assertEquals("23503", state("INSERT INTO " + t + " VALUES (9,9)"));
    }

    // ---- A4: read-only transactions ----

    @Test
    void aReadOnlyTransactionRefusesDdl() throws Exception {
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "CREATE TABLE svg_ro (i int)"));
        }
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "DROP TABLE svg_p"));
        }
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "ALTER TABLE svg_p ADD COLUMN c int"));
        }
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "TRUNCATE svg_p"));
        }
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "CREATE INDEX svg_i ON svg_p (a)"));
        }
    }

    @Test
    void aReadOnlyTransactionRefusesDml() throws Exception {
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "INSERT INTO svg_p VALUES (7,7)"));
        }
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "UPDATE svg_p SET b = 2"));
        }
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            assertEquals("25006", stateOn(c, "DELETE FROM svg_p"));
        }
    }

    @Test
    void aReadOnlyTransactionStillReads() throws Exception {
        try (Connection c = open()) {
            try (Statement s = c.createStatement()) { s.execute("BEGIN TRANSACTION READ ONLY"); }
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM svg_p")) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    /** A read-write transaction is unaffected. */
    @Test
    void aReadWriteTransactionStillWrites() throws Exception {
        exec("BEGIN");
        exec("CREATE TABLE svg_rw (i int)");
        exec("INSERT INTO svg_rw VALUES (1)");
        exec("COMMIT");
        assertEquals(1, count("svg_rw"));
        exec("DROP TABLE svg_rw");
    }

    // ---- A6: WITH TIES ----

    /** WITH TIES means "and everything equal to the last row", which needs an ordering. */
    @Test
    void withTiesRequiresAnOrderBy() {
        assertEquals("42601", state("SELECT 1 FETCH FIRST 1 ROWS WITH TIES"));
    }

    @Test
    void withTiesIsFineWithAnOrderBy() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a FROM svg_p ORDER BY a FETCH FIRST 1 ROWS WITH TIES")) {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void fetchFirstOnlyStillNeedsNoOrderBy() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 FETCH FIRST 1 ROWS ONLY")) {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    // ---- A9: untyped record ----

    /** Without a concrete row type there is nothing to populate. */
    @Test
    void populatingAnUntypedRecordIsRejected() {
        assertEquals("0A000", state("SELECT jsonb_populate_record(NULL::record, '{}')"));
        assertEquals("0A000", state("SELECT json_populate_record(NULL::record, '{}')"));
    }
}
