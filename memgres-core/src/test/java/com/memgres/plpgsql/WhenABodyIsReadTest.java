package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * When a routine's body is read, and what a composite column takes.
 *
 * <p>PostgreSQL compiles a routine when it is created, so a body it cannot read never becomes a
 * routine that only fails when somebody calls it. A statement the plpgsql reader does not
 * recognise is collected to be run as SQL, and one that is not SQL either -- a misspelled RETURN
 * -- was collected and stored, so the fault surfaced at the first call rather than at the CREATE
 * that wrote it. Only the spelling is judged: a statement naming a relation that does not exist
 * yet is still a statement, and a routine may well be created before the table it reads.
 *
 * <p>The word is quoted back as it was written. Folded first, a misspelled statement was reported
 * at "retrn" where PostgreSQL reports it at "RETRN" -- the word the author actually typed.
 *
 * <p>And a composite and an array of that composite are two types, with no cast either way: a row
 * written where a list of them belongs, or a list where one belongs, is refused. Read as text of
 * the other's shape, the row went in as a malformed literal and the list went in whole -- and a
 * later read of it could not be made sense of at all.
 */
class WhenABodyIsReadTest {

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

    /** A body that is not SQL is refused where it is written. */
    @Test
    void whenABodyThatWillNotReadIsRefused() throws SQLException {
        assertEquals("42601", stateOf("CREATE FUNCTION zwb_f() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN RETRN 1; END $$"));
        assertTrue(messageOf("CREATE FUNCTION zwb_f() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN RETRN 1; END $$").contains("at or near \"RETRN\""));
        assertEquals("0", one("SELECT count(*)::text FROM pg_proc WHERE proname='zwb_f'"));
        // A body that reads is created, and runs.
        exec("CREATE FUNCTION zwb_g() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$");
        assertEquals("1", one("SELECT zwb_g()::text"));
        exec("DROP FUNCTION zwb_g()");
        // A relation the body names need not exist yet: only the spelling is judged.
        exec("CREATE FUNCTION zwb_later() RETURNS int LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN (SELECT count(*) FROM zwb_not_yet); END $$");
        exec("CREATE TABLE zwb_not_yet (i int)");
        assertEquals("0", one("SELECT zwb_later()::text"));
        exec("DROP FUNCTION zwb_later()");
        exec("DROP TABLE zwb_not_yet");
    }

    /** A composite column takes one of them, and an array column takes a list. */
    @Test
    void whatACompositeColumnTakes() throws SQLException {
        exec("CREATE TYPE zwb_c AS (a int, b text)");
        exec("CREATE TABLE zwb_one (id int, c zwb_c)");
        exec("CREATE TABLE zwb_many (id int, cs zwb_c[])");
        assertEquals("42804", stateOf("INSERT INTO zwb_one VALUES (1, ARRAY[ROW(1,'x')::zwb_c])"));
        assertTrue(messageOf("INSERT INTO zwb_one VALUES (1, ARRAY[ROW(1,'x')::zwb_c])")
                .contains("column \"c\" is of type zwb_c but expression is of type zwb_c[]"));
        assertEquals("42804", stateOf("INSERT INTO zwb_many VALUES (1, ROW(1,'x')::zwb_c)"));
        // Each takes what it is declared to take.
        exec("INSERT INTO zwb_one VALUES (2, ROW(1,'x')::zwb_c)");
        exec("INSERT INTO zwb_many VALUES (2, ARRAY[ROW(1,'x')::zwb_c])");
        assertEquals("1", one("SELECT (c).a::text FROM zwb_one"));
        assertEquals("1", one("SELECT (cs[1]).a::text FROM zwb_many"));
        exec("DROP TABLE zwb_many");
        exec("DROP TABLE zwb_one");
        exec("DROP TYPE zwb_c");
    }
}
