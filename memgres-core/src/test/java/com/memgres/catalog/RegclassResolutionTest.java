package com.memgres.catalog;

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
 * An all-digit string is an OID written out, which PostgreSQL takes verbatim without a lookup —
 * that is how a catalog dump round-trips a regclass. Every relation resolves by name, including
 * an index PG materialises from a constraint. Expectations captured from a live PostgreSQL 18.0
 * server.
 *
 * <p>N67 numeric-string and index-name regclass resolution.
 */
class RegclassResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE rcr (id int PRIMARY KEY, v int, u int UNIQUE)");
        exec("CREATE INDEX rcr_v_idx ON rcr (v)");
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

    /** An OID is taken verbatim, without a lookup, and prints back bare. */
    @Test
    void anAllDigitStringIsAnOidNotAName() throws Exception {
        assertEquals("99999999", expr("SELECT '99999999'::regclass::text"));
        assertEquals("1", expr("SELECT '1'::regclass::text"));
    }

    @Test
    void aTableResolvesByName() throws Exception {
        assertEquals("rcr", expr("SELECT 'rcr'::regclass::text"));
    }

    @Test
    void anExplicitlyCreatedIndexResolves() throws Exception {
        assertEquals("rcr_v_idx", expr("SELECT 'rcr_v_idx'::regclass::text"));
    }

    /** A primary-key or unique index is stored as a constraint, but is still a relation. */
    @Test
    void aConstraintBackedIndexResolves() throws Exception {
        assertEquals("rcr_pkey", expr("SELECT 'rcr_pkey'::regclass::text"));
    }

    @Test
    void aNameThatIsNothingIsStillAnError() {
        assertEquals("42P01", state("SELECT 'nosuchtable'::regclass::text"));
    }
}
