package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A type's name is the letters it was created with, and so is a target's, a unit's and an index's.
 *
 * <p>A name written without quotes is folded where it is read and a name written with them is not,
 * so a type created as {@code "MiXeD"} is not reached by writing mixed. Matching them case by case
 * afterwards made every object answer to every spelling of its name.
 *
 * <p>The same holds of the names a function is handed as text: {@code pg_stat_reset_shared('WAL')}
 * names no target the server has, and {@code pg_size_bytes('1 byte')} is written in a unit
 * PostgreSQL does not read -- it reads bytes, B, kB and the rest, and not the singular.
 */
class ANameIsTheLettersItWasWrittenWithTest {

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

    /** A type created with quotes keeps the letters it was created with. */
    @Test
    void whichTypeANameReaches() throws SQLException {
        exec("CREATE TYPE \"ZanEnum\" AS ENUM ('Red')");
        assertEquals("42704", stateOf("SELECT 'Red'::ZanEnum"));
        assertTrue(messageOf("SELECT 'Red'::ZanEnum").contains("type \"zanenum\" does not exist"));
        assertEquals("Red", one("SELECT 'Red'::\"ZanEnum\""));
        exec("DROP TYPE \"ZanEnum\"");
        // A type created without them was folded when it was made, so any spelling reaches it.
        exec("CREATE TYPE zan_plain AS ENUM ('a')");
        assertEquals("a", one("SELECT 'a'::ZAN_PLAIN"));
        assertEquals("a", one("SELECT 'a'::zan_plain"));
        exec("DROP TYPE zan_plain");
        // A domain is a type and is reached the same way.
        exec("CREATE DOMAIN \"ZanDom\" AS int");
        assertEquals("42704", stateOf("SELECT 1::ZanDom"));
        assertEquals("1", one("SELECT (1::\"ZanDom\")::text"));
        exec("DROP DOMAIN \"ZanDom\"");
    }

    /** A name with a schema in front of it and no parentheses after it is a column. */
    @Test
    void aQualifiedNameIsAColumnReference() throws SQLException {
        assertEquals("42P01", stateOf("SELECT pg_catalog.current_schema"));
        assertTrue(messageOf("SELECT pg_catalog.current_schema")
                .contains("missing FROM-clause entry for table \"pg_catalog\""));
        assertEquals("42P01", stateOf("SELECT public.current_user"));
        // Written as the call it is, or as the word it is, it answers.
        assertEquals("public", one("SELECT pg_catalog.current_schema()"));
        assertEquals("public", one("SELECT current_schema"));
    }

    /** A unit is one of the ones PostgreSQL reads, and no other. */
    @Test
    void theUnitsASizeMayBeWrittenIn() throws SQLException {
        assertEquals("1", one("SELECT pg_size_bytes('1 bytes')::text"));
        assertEquals("10", one("SELECT pg_size_bytes('10')::text"));
        assertEquals("1024", one("SELECT pg_size_bytes('1kB')::text"));
        assertEquals("22023", stateOf("SELECT pg_size_bytes('1 byte')"));
        assertTrue(messageOf("SELECT pg_size_bytes('1 byte')").contains("invalid size: \"1 byte\""));
    }

    /** A reset target is a name matched letter for letter. */
    @Test
    void whichCountersMayBeReset() throws SQLException {
        assertNull(stateOf("SELECT pg_stat_reset_shared('wal')"));
        assertEquals("22023", stateOf("SELECT pg_stat_reset_shared('WAL')"));
        assertTrue(messageOf("SELECT pg_stat_reset_shared('WAL')")
                .contains("unrecognized reset target: \"WAL\""));
        assertEquals("22023", stateOf("SELECT pg_stat_reset_shared('nosuch')"));
    }

    /** An index a CLUSTER names has to be one the relation has. */
    @Test
    void whichIndexAClusterNames() throws SQLException {
        exec("CREATE TABLE zan_t (a int)");
        exec("CREATE INDEX zan_ix ON zan_t (a)");
        exec("CREATE TABLE zan_u (a int)");
        exec("CREATE INDEX zan_uix ON zan_u (a)");
        assertNull(stateOf("CLUSTER zan_ix ON zan_t"));
        assertEquals("42704", stateOf("CLUSTER zan_nosuch ON zan_t"));
        assertTrue(messageOf("CLUSTER (VERBOSE) zan_t USING zan_nosuch")
                .contains("index \"zan_nosuch\" for table \"zan_t\" does not exist"));
        // An index of another relation is not this one's to order by.
        assertEquals("42809", stateOf("CLUSTER zan_uix ON zan_t"));
        exec("DROP TABLE zan_t");
        exec("DROP TABLE zan_u");
    }

    /** An array is named after what it holds. */
    @Test
    void howAnArrayIsNamedInAComplaint() {
        assertTrue(messageOf("SELECT hstore(ARRAY['a','b'], ARRAY['1'])")
                .contains("function hstore(text[], text[]) does not exist"));
        assertTrue(messageOf("SELECT hstore(ARRAY[1,2], ARRAY[3])")
                .contains("function hstore(integer[], integer[]) does not exist"));
    }

    /** The roles the server ships carry the numbers PostgreSQL gives them. */
    @Test
    void theRolesTheServerShips() throws SQLException {
        assertEquals("3373", one("SELECT oid::text FROM pg_roles WHERE rolname = 'pg_monitor'"));
        assertEquals("6392", one("SELECT oid::text FROM pg_roles"
                + " WHERE rolname = 'pg_signal_autovacuum_worker'"));
        assertEquals("16", one("SELECT count(*)::text FROM pg_roles WHERE rolname LIKE 'pg\\_%'"));
    }
}
