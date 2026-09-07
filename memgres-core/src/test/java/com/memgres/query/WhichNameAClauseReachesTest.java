package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which name a clause reaches, and which catalog a name may name.
 *
 * <p>A relation is visible when its bare name reaches it and not another: the first schema on the
 * search path holding a relation of that name is the one the name gets, and every other relation of
 * that name is shadowed. Answering yes for any schema the path mentions said both of two same-named
 * relations were reachable unqualified, which no search path can make true.
 *
 * <p>HAVING is read against the relations the query is over, never against the names the select
 * list gives its own results. Judged the other way round, a HAVING naming an output alias was
 * reported as a select item that had not been grouped.
 *
 * <p>And a name written with three parts names a catalog: PostgreSQL reaches only the one it is
 * connected to, and says so rather than reading the name as two parts and a stray dot.
 */
class WhichNameAClauseReachesTest {

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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** A relation is visible only where its bare name reaches it rather than another. */
    @Test
    void whichRelationABareNameReaches() throws SQLException {
        exec("CREATE SCHEMA zwc_sc");
        exec("CREATE TABLE zwc_sc.zwc_x (a int)");
        exec("CREATE TABLE zwc_x (b int)");
        exec("SET search_path = zwc_sc, public");
        try {
            assertEquals("false",
                    one("SELECT pg_table_is_visible('public.zwc_x'::regclass)::text"));
            assertEquals("true",
                    one("SELECT pg_table_is_visible('zwc_sc.zwc_x'::regclass)::text"));
        } finally {
            exec("RESET search_path");
        }
        exec("SET search_path = public, zwc_sc");
        try {
            assertEquals("true",
                    one("SELECT pg_table_is_visible('public.zwc_x'::regclass)::text"));
            assertEquals("false",
                    one("SELECT pg_table_is_visible('zwc_sc.zwc_x'::regclass)::text"));
        } finally {
            exec("RESET search_path");
        }
        exec("DROP TABLE zwc_x");
        exec("DROP SCHEMA zwc_sc CASCADE");
    }

    /** HAVING never sees the names the select list gives its own results. */
    @Test
    void whatHavingMayName() throws SQLException {
        exec("CREATE TABLE zwc_s (a int, b int)");
        exec("INSERT INTO zwc_s VALUES (1,1)");
        assertEquals("42703", stateOf("SELECT a AS x FROM zwc_s HAVING x > 0"));
        assertTrue(messageOf("SELECT a AS x FROM zwc_s HAVING x > 0")
                .contains("column \"x\" does not exist"));
        assertEquals("42703", stateOf("SELECT a AS x FROM zwc_s GROUP BY a HAVING x > 0"));
        // GROUP BY and ORDER BY do see them, and a HAVING over what it may name still works.
        assertEquals("1", one("SELECT a AS x FROM zwc_s GROUP BY x"));
        assertEquals("1", one("SELECT a AS x FROM zwc_s ORDER BY x"));
        assertEquals("1", one("SELECT sum(b) AS x FROM zwc_s HAVING sum(b) > 0"));
        // A subquery in HAVING reads its own relations, and those names are not ours to judge.
        exec("CREATE TABLE zwc_o (k int)");
        exec("INSERT INTO zwc_o VALUES (1)");
        assertEquals("1", one("SELECT a FROM zwc_s GROUP BY a"
                + " HAVING EXISTS (SELECT 1 FROM zwc_o WHERE k = zwc_s.a)"));
        exec("DROP TABLE zwc_o");
        exec("DROP TABLE zwc_s");
    }

    /** A three-part name names a catalog, and only the connected one is reached. */
    @Test
    void whichCatalogANameMayName() throws SQLException {
        assertEquals("0A000", stateOf("SELECT * FROM nosuchdb.public.zwc_q1"));
        assertTrue(messageOf("SELECT * FROM nosuchdb.public.zwc_q1")
                .contains("cross-database references are not implemented:"
                        + " \"nosuchdb.public.zwc_q1\""));
        // The catalog this session is connected to is reached like any other name.
        exec("CREATE TABLE zwc_ok (i int)");
        exec("INSERT INTO zwc_ok VALUES (1)");
        assertEquals("1", one("SELECT i::text FROM "
                + one("SELECT current_database()") + ".public.zwc_ok"));
        exec("DROP TABLE zwc_ok");
    }
}
