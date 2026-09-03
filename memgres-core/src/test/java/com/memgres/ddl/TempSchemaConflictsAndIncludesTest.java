package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Writing into the session's own schema, naming an expression as a conflict target, and what may
 * be carried along in an index.
 *
 * <p>{@code pg_temp} is not a schema somebody makes: it is the name a session's own schema answers
 * to, and it comes into being with the first relation put in it. Writing into it is asking for a
 * temporary relation whether or not the word TEMP was written.
 *
 * <p>A conflict target names what a unique index was built on, which may be an expression rather
 * than a column; an included column is a column and never an expression, but a name written in
 * quotes may hold a space and is still a name.
 */
class TempSchemaConflictsAndIncludesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** Writing into pg_temp asks for a temporary relation and makes the schema on the way. */
    @Test
    void writingIntoTheSessionsOwnSchema() throws SQLException {
        exec("CREATE TABLE pg_temp.ztc_qt (a int)");
        assertEquals("0", one("SELECT count(*)::text FROM ztc_qt"));
        exec("DROP TABLE ztc_qt");
    }

    /** A conflict target names what an index was built on, expression and all. */
    @Test
    void anExpressionAsAConflictTarget() throws SQLException {
        exec("CREATE TABLE ztc_fa (i int, t text, v int)");
        exec("CREATE UNIQUE INDEX ztc_fa_l ON ztc_fa (lower(t))");
        exec("INSERT INTO ztc_fa VALUES (1,'abc',1)");
        assertEquals("2", one("INSERT INTO ztc_fa VALUES (2, 'ABC', 2)"
                + " ON CONFLICT (lower(t)) DO UPDATE SET v = EXCLUDED.v RETURNING v::text"));
        exec("DROP TABLE ztc_fa");
    }

    /** An included column is a column; a name in quotes may hold a space and is still one. */
    @Test
    void whatMayBeCarriedAlongInAnIndex() throws SQLException {
        exec("CREATE TABLE ztc_ix (a int, b int, \"my col\" int)");
        assertNull(stateOf("CREATE INDEX ztc_i1 ON ztc_ix (a) INCLUDE (\"my col\")"));
        assertTrue(messageOf("CREATE INDEX ztc_i3 ON ztc_ix (a) INCLUDE (lower(a::text))")
                .contains("expressions are not supported in included columns"));
        assertEquals("42703", stateOf("CREATE INDEX ztc_i4 ON ztc_ix (a) INCLUDE (nosuch)"));
        exec("DROP TABLE ztc_ix");
    }

    /** A code whose last three characters are zeroes names the whole class. */
    @Test
    void aSqlstateClassCatchesEveryCodeInIt() {
        assertNull(stateOf("DO $$ begin perform 1/0;"
                + " exception when sqlstate '22000' then null; end $$"));
        assertNull(stateOf("DO $$ begin raise sqlstate '23514';"
                + " exception when sqlstate '23000' then null; end $$"));
        assertNull(stateOf("DO $$ begin perform 1/0;"
                + " exception when sqlstate '22012' then null; end $$"));
        // A class the code is not in still does not catch it.
        assertEquals("22012", stateOf("DO $$ begin perform 1/0;"
                + " exception when sqlstate '23000' then null; end $$"));
    }
}
