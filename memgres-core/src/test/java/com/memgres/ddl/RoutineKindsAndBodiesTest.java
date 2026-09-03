package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which routine a statement names, and where the statements in a routine's body end.
 *
 * <p>DROP FUNCTION names a function and DROP PROCEDURE a procedure; DROP ROUTINE names either. A
 * routine of the right name and the wrong kind is the wrong kind of object rather than a missing
 * one, and IF EXISTS does not excuse it: what it excuses is a name that reaches nothing.
 *
 * <p>A semicolon ends a statement only where it stands as itself: inside a comment, a string, a
 * dollar-quoted body or a quoted name it is one more character of that thing.
 */
class RoutineKindsAndBodiesTest {

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

    /** A routine of the wrong kind is not the routine the statement named. */
    @Test
    void whichKindOfRoutineWasNamed() throws SQLException {
        exec("CREATE PROCEDURE zrk_p() LANGUAGE sql AS $$ SELECT 1 $$");
        exec("CREATE FUNCTION zrk_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        assertEquals("42809", stateOf("DROP FUNCTION zrk_p()"));
        assertTrue(messageOf("DROP FUNCTION zrk_p()").contains("zrk_p() is not a function"));
        assertEquals("42809", stateOf("DROP PROCEDURE zrk_f()"));
        assertTrue(messageOf("DROP PROCEDURE zrk_f()").contains("zrk_f() is not a procedure"));
        // IF EXISTS excuses a name that reaches nothing, and this name reaches something.
        assertEquals("42809", stateOf("DROP FUNCTION IF EXISTS zrk_p()"));
        assertEquals("1", one("SELECT count(*)::text FROM pg_proc WHERE proname = 'zrk_p'"));
        assertNull(stateOf("DROP FUNCTION IF EXISTS zrk_nosuch()"));
        // ROUTINE names either kind.
        assertNull(stateOf("DROP ROUTINE zrk_p()"));
        assertNull(stateOf("DROP ROUTINE zrk_f()"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_proc WHERE proname = 'zrk_p'"));
    }

    /** There is no replacing a materialized view and no making a temporary one. */
    @Test
    void theFormsAMaterializedViewIsNotWrittenIn() throws SQLException {
        exec("CREATE TABLE zrk_v (i int)");
        assertEquals("42601", stateOf("CREATE OR REPLACE MATERIALIZED VIEW zrk_mv"
                + " AS SELECT i FROM zrk_v"));
        assertTrue(messageOf("CREATE OR REPLACE MATERIALIZED VIEW zrk_mv AS SELECT i FROM zrk_v")
                .contains("syntax error at or near \"MATERIALIZED\""));
        assertEquals("42601", stateOf("CREATE TEMP MATERIALIZED VIEW zrk_mv2 AS SELECT 1"));
        // The form the grammar does have is unaffected.
        assertNull(stateOf("CREATE MATERIALIZED VIEW zrk_mv3 AS SELECT i FROM zrk_v"));
        exec("DROP MATERIALIZED VIEW zrk_mv3");
        exec("DROP TABLE zrk_v");
    }

    /** A routine that answers with a row is read as the fields of that row. */
    @Test
    void whatARoutineAnsweringWithARowIsReadAs() throws SQLException {
        exec("CREATE TYPE zrk_c AS (a int, b text)");
        exec("CREATE FUNCTION zrk_row() RETURNS zrk_c LANGUAGE sql"
                + " AS $$ SELECT ROW(1,'a')::zrk_c $$");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM zrk_row()")) {
            assertEquals(2, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals("a", rs.getString(2));
        }
        // Written where a value belongs it is the one row it is.
        assertEquals("(1,a)", one("SELECT zrk_row()::text"));
        exec("DROP FUNCTION zrk_row()");
        exec("DROP TYPE zrk_c");
    }

    /** A semicolon inside a comment, a string or a name is no end of a statement. */
    @Test
    void whereTheStatementsInABodyEnd() throws SQLException {
        exec("CREATE FUNCTION zrk_c1() RETURNS int LANGUAGE sql AS $$ SELECT 1 -- one; two $$");
        assertEquals("1", one("SELECT zrk_c1()::text"));
        exec("CREATE FUNCTION zrk_c2() RETURNS int LANGUAGE sql"
                + " AS $$ SELECT 1 /* a ; b */ + 1 $$");
        assertEquals("2", one("SELECT zrk_c2()::text"));
        exec("CREATE FUNCTION zrk_c3() RETURNS text LANGUAGE sql AS $$ SELECT $x$a;b$x$ $$");
        assertEquals("a;b", one("SELECT zrk_c3()"));
        exec("CREATE TABLE \"zrk;t\" (a int)");
        exec("CREATE FUNCTION zrk_c4() RETURNS bigint LANGUAGE sql"
                + " AS $$ SELECT count(*) FROM \"zrk;t\" $$");
        assertEquals("0", one("SELECT zrk_c4()::text"));
        // A name in quotes keeps its own spelling wherever it is read.
        exec("CREATE TABLE zrk_mixed (\"MyCol\" int)");
        exec("INSERT INTO zrk_mixed VALUES (7)");
        exec("CREATE FUNCTION zrk_c5() RETURNS int LANGUAGE sql"
                + " AS $$ SELECT \"MyCol\" FROM zrk_mixed $$");
        assertEquals("7", one("SELECT zrk_c5()::text"));
        exec("DROP FUNCTION zrk_c1()");
        exec("DROP FUNCTION zrk_c2()");
        exec("DROP FUNCTION zrk_c3()");
        exec("DROP FUNCTION zrk_c4()");
        exec("DROP FUNCTION zrk_c5()");
        exec("DROP TABLE \"zrk;t\"");
        exec("DROP TABLE zrk_mixed");
    }
}
