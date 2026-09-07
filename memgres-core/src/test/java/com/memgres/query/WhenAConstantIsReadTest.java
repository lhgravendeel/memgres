package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * When the constants in a qualification are read.
 *
 * <p>PostgreSQL works out what a query's constants come to while it plans, so a qualification
 * holding one that cannot be read is refused whether or not the relation has a row in it.
 * Evaluated only as each row was tested, the same query over an empty relation quietly answered
 * nothing -- so a cast that could never have worked looked like a query with no matches, which is
 * a far harder thing to notice than an error.
 *
 * <p>Only a subtree whose leaves are all values written into the query counts: anything reading a
 * column is about a row, and is left to be read when there is one.
 */
class WhenAConstantIsReadTest {

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

    /** A constant that cannot be read is refused with or without a row to test. */
    @Test
    void whenAnUnreadableConstantIsRefused() throws SQLException {
        exec("CREATE TABLE zwc_e (i int)");
        assertEquals("22P02", stateOf("SELECT i FROM zwc_e WHERE i = 'abc'::int"));
        assertEquals("22012", stateOf("SELECT i FROM zwc_e WHERE i = 1/0"));
        assertEquals("42P01", stateOf("SELECT i FROM zwc_e WHERE i::text = 'zwc_x'::regclass::text"));
        // The same with a row present, which is where it was already reported.
        exec("INSERT INTO zwc_e VALUES (1)");
        assertEquals("22P02", stateOf("SELECT i FROM zwc_e WHERE i = 'abc'::int"));
        assertEquals("22012", stateOf("SELECT i FROM zwc_e WHERE i = 1/0"));
        exec("DROP TABLE zwc_e");
    }

    /** A catalogue read whose constant names nothing is refused, not answered empty. */
    @Test
    void whatANameThatReachesNothingDoes() throws SQLException {
        assertEquals("42P01",
                stateOf("SELECT conname FROM pg_constraint WHERE conrelid = 'zwc_none'::regclass"));
        assertEquals("42P01", stateOf("SELECT count(*) FROM pg_constraint"
                + " WHERE conrelid = 'zwc_none'::regclass"));
        // A name that does reach a relation is read as it always was.
        exec("CREATE TABLE zwc_real (i int PRIMARY KEY)");
        assertEquals("1", one("SELECT count(*)::text FROM pg_constraint"
                + " WHERE conrelid = 'zwc_real'::regclass AND contype='p'"));
        exec("DROP TABLE zwc_real");
    }

    /** What reads a column is still left until there is a row to read it from. */
    @Test
    void whatIsStillLeftToTheRows() throws SQLException {
        exec("CREATE TABLE zwc_t (s text)");
        // No row, so nothing tries to read 'abc' as a number.
        assertEquals("0", one("SELECT count(*)::text FROM zwc_t WHERE s::int = 1"));
        exec("INSERT INTO zwc_t VALUES ('abc')");
        assertEquals("22P02", stateOf("SELECT count(*) FROM zwc_t WHERE s::int = 1"));
        exec("DROP TABLE zwc_t");
    }
}
