package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DDL is transactional, so what one session has done to the catalog is nobody else's business
 * until it commits.
 *
 * <p>A relation, a schema, a type, a function or a column that an open transaction created does
 * not exist yet for anyone else — the transaction may roll back, and a session that read it would
 * have read something that never existed. The same rule runs the other way for a drop: a relation
 * an open transaction removed is still there for everyone else, so another session must not be
 * told a live relation is missing.
 *
 * <p>These are two-session facts, which is why they need two connections to state. The one thing
 * left out is PostgreSQL's <em>blocking</em>: it makes the second session wait on the first
 * session's ACCESS EXCLUSIVE lock rather than answer from the older catalog. memgres answers
 * instead of waiting, which reaches the same answer for a rolled-back transaction and a different
 * one only in the window before a commit lands.
 */
class UncommittedDdlVisibilityTest {

    static Memgres memgres;
    static Connection a;
    static Connection b;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        a = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        b = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        a.setAutoCommit(true);
        b.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (a != null) a.close();
        if (b != null) b.close();
        if (memgres != null) memgres.close();
    }

    @AfterEach
    void rollBackWhateverIsOpen() {
        try (Statement st = a.createStatement()) { st.execute("ROLLBACK"); } catch (SQLException ignored) { }
        try (Statement st = b.createStatement()) { st.execute("ROLLBACK"); } catch (SQLException ignored) { }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** How many rows a catalog holds for this name, as {@code who} sees it. */
    private static String catalogCount(Connection who, String catalog, String column, String name)
            throws SQLException {
        return scalar(who, "SELECT count(*)::text FROM " + catalog
                + " WHERE " + column + " = '" + name + "'");
    }

    // ---------------------------------------------------------------- SECTION A
    // Nothing an open transaction created exists for anyone else yet.

    @Test
    void anUncommittedRelationIsNotVisible() throws Exception {
        exec(a, "BEGIN");
        exec(a, "CREATE TABLE uv_t (i int)");
        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_t"));
        // ...and its creator can of course see it.
        assertEquals("1", catalogCount(a, "pg_class", "relname", "uv_t"));
        SQLException e = assertThrows(SQLException.class,
                () -> exec(b, "SELECT * FROM uv_t"));
        assertEquals("42P01", e.getSQLState());
        exec(a, "ROLLBACK");
        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_t"));
    }

    @Test
    void everyKindOfUncommittedObjectIsHiddenAndRolledBack() throws Exception {
        exec(a, "CREATE TABLE uv_base (i int)");
        exec(a, "BEGIN");
        exec(a, "CREATE VIEW uv_v AS SELECT 1 AS i");
        exec(a, "CREATE SEQUENCE uv_seq");
        exec(a, "CREATE INDEX uv_ix ON uv_base (i)");
        exec(a, "CREATE SCHEMA uv_s");
        exec(a, "CREATE TYPE uv_e AS ENUM ('x')");
        exec(a, "CREATE TYPE uv_c AS (x int)");
        exec(a, "CREATE DOMAIN uv_d AS int");
        exec(a, "CREATE FUNCTION uv_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");

        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_v"));
        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_seq"));
        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_ix"));
        assertEquals("0", catalogCount(b, "pg_namespace", "nspname", "uv_s"));
        assertEquals("0", catalogCount(b, "pg_type", "typname", "uv_e"));
        assertEquals("0", catalogCount(b, "pg_type", "typname", "uv_c"));
        assertEquals("0", catalogCount(b, "pg_type", "typname", "uv_d"));
        assertEquals("0", catalogCount(b, "pg_proc", "proname", "uv_f"));

        // And a rollback takes every one of them with it, rather than leaving a name taken by an
        // object nobody successfully created.
        exec(a, "ROLLBACK");
        assertEquals("0", catalogCount(a, "pg_class", "relname", "uv_v"));
        assertEquals("0", catalogCount(a, "pg_class", "relname", "uv_seq"));
        assertEquals("0", catalogCount(a, "pg_class", "relname", "uv_ix"));
        assertEquals("0", catalogCount(a, "pg_namespace", "nspname", "uv_s"));
        assertEquals("0", catalogCount(a, "pg_type", "typname", "uv_e"));
        assertEquals("0", catalogCount(a, "pg_type", "typname", "uv_c"));
        assertEquals("0", catalogCount(a, "pg_type", "typname", "uv_d"));
        assertEquals("0", catalogCount(a, "pg_proc", "proname", "uv_f"));
        // The name is free again, which is the point of rolling back.
        exec(a, "CREATE SCHEMA uv_s");
        exec(a, "DROP SCHEMA uv_s");
        exec(a, "DROP TABLE uv_base");
    }

    @Test
    void aCommittedObjectIsVisibleToEveryone() throws Exception {
        exec(a, "BEGIN");
        exec(a, "CREATE TABLE uv_done (i int)");
        exec(a, "COMMIT");
        assertEquals("1", catalogCount(b, "pg_class", "relname", "uv_done"));
        assertEquals("0", scalar(b, "SELECT count(*)::text FROM uv_done"));
        exec(a, "DROP TABLE uv_done");
    }

    /** A column an open transaction added is not part of the relation for anyone else yet. */
    @Test
    void anUncommittedColumnIsNotVisible() throws Exception {
        exec(a, "CREATE TABLE uv_alt (i int)");
        exec(a, "BEGIN");
        exec(a, "ALTER TABLE uv_alt ADD COLUMN j int");
        assertEquals("0", scalar(b, "SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'uv_alt' AND column_name = 'j'"));
        // Its own transaction sees it, or it could not have written to it.
        assertEquals("1", scalar(a, "SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'uv_alt' AND column_name = 'j'"));
        exec(a, "ROLLBACK");
        assertEquals("0", scalar(b, "SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'uv_alt' AND column_name = 'j'"));
        exec(a, "DROP TABLE uv_alt");
    }

    /** A rename is a name appearing and a name going away, and neither has happened yet. */
    @Test
    void anUncommittedRenameIsNotVisible() throws Exception {
        exec(a, "CREATE TABLE uv_rn (i int)");
        exec(a, "BEGIN");
        exec(a, "ALTER TABLE uv_rn RENAME TO uv_rn2");
        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_rn2"));
        assertEquals("1", catalogCount(b, "pg_class", "relname", "uv_rn"));
        exec(a, "ROLLBACK");
        assertEquals("1", catalogCount(a, "pg_class", "relname", "uv_rn"));
        exec(a, "DROP TABLE uv_rn");
    }

    // ---------------------------------------------------------------- SECTION B
    // A relation an open transaction dropped is still there for everyone else.

    @Test
    void anUncommittedDropLeavesTheRelationVisible() throws Exception {
        exec(a, "CREATE TABLE uv_drop (i int)");
        exec(a, "INSERT INTO uv_drop VALUES (1)");
        exec(a, "BEGIN");
        exec(a, "DROP TABLE uv_drop");
        // The dropping session sees it gone...
        SQLException e = assertThrows(SQLException.class,
                () -> exec(a, "SELECT count(*) FROM uv_drop"));
        assertEquals("42P01", e.getSQLState());
        // ...and every other session still sees it, rows and all.
        assertEquals("1", scalar(b, "SELECT count(*)::text FROM uv_drop"));
        assertEquals("1", catalogCount(b, "pg_class", "relname", "uv_drop"));
        // Rolling back brings it back for its own session too.
        exec(a, "ROLLBACK");
        assertEquals("1", scalar(a, "SELECT count(*)::text FROM uv_drop"));
        exec(a, "DROP TABLE uv_drop");
        // Once the drop commits it is gone for everyone.
        assertEquals("0", catalogCount(b, "pg_class", "relname", "uv_drop"));
    }
}
