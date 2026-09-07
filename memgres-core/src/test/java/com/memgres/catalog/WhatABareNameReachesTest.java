package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a bare name reaches, and what a relation has hanging off it.
 *
 * <p>An unqualified function or operator is looked for along the search path and nowhere else.
 * public is on that path because the default path names it, not because it is always reachable: a
 * session that has set the path to something else cannot write a bare name and reach public.
 * Assumed to be reachable regardless, a query found a routine it could never have named, and an
 * operator was built over a function the session could not call.
 *
 * <p>What a relation is made of depends on the relation, and that is most of what pg_depend holds
 * about an ordinary table: its row type belongs to it, each constraint belongs to the columns it
 * is written over, and a foreign key names the columns of the table it points at. Recorded
 * nowhere, a reader following the graph to find what would go with a table was told the table had
 * nothing hanging off it at all.
 */
class WhatABareNameReachesTest {

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

    private static java.util.List<String> rows(String sql) throws SQLException {
        java.util.List<String> out = new java.util.ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
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

    /** A path that does not name public cannot reach a routine in it. */
    @Test
    void whatABareRoutineNameReaches() throws SQLException {
        exec("CREATE FUNCTION public.zwn_add(integer,integer) RETURNS integer"
                + " AS 'SELECT $1+$2' LANGUAGE sql IMMUTABLE");
        exec("SET search_path = 'zwn_no_such_schema'");
        try {
            assertEquals("42883", stateOf("SELECT zwn_add(1,2)"));
            // Written with its schema, it is reached as it always was.
            assertEquals("3", one("SELECT public.zwn_add(1,2)::text"));
            // And an operator cannot be built over a function the session cannot name.
            assertEquals("42883", stateOf("CREATE OPERATOR public.@#@"
                    + " (LEFTARG=integer, RIGHTARG=integer, FUNCTION = zwn_add)"));
            assertNull(stateOf("CREATE OPERATOR public.@#@"
                    + " (LEFTARG=integer, RIGHTARG=integer, FUNCTION = public.zwn_add)"));
            // The operator is in public, which this path does not name either.
            assertEquals("42883", stateOf("SELECT 1 @#@ 2"));
        } finally {
            exec("SET search_path = public");
        }
        // Back on a path that names public, both are reached.
        assertEquals("3", one("SELECT zwn_add(1,2)::text"));
        assertEquals("3", one("SELECT (1 @#@ 2)::text"));
        exec("DROP OPERATOR public.@#@ (integer,integer)");
        exec("DROP FUNCTION public.zwn_add(integer,integer)");
    }

    /** A table's row type, its constraints and the keys pointing at it all depend on it. */
    @Test
    void whatDependsOnATable() throws SQLException {
        exec("CREATE TABLE zwn_d1 (id int PRIMARY KEY)");
        exec("CREATE TABLE zwn_d2 (id int REFERENCES zwn_d1(id))");
        assertEquals(java.util.Arrays.asList("a|true", "i|true", "n|true"),
                rows("SELECT deptype, (count(*) > 0)::text FROM pg_depend"
                        + " WHERE refobjid='zwn_d1'::regclass GROUP BY deptype ORDER BY deptype"));
        // The internal one is the row type, and it names the relation it belongs to.
        assertEquals("zwn_d1", one("SELECT objid::regtype::text FROM pg_depend"
                + " WHERE refobjid='zwn_d1'::regclass AND deptype='i'"));
        // The normal one is the foreign key, and it names the column it points at.
        assertEquals("1", one("SELECT refobjsubid::text FROM pg_depend"
                + " WHERE refobjid='zwn_d1'::regclass AND deptype='n'"));
        exec("DROP TABLE zwn_d2");
        exec("DROP TABLE zwn_d1");
    }
}
