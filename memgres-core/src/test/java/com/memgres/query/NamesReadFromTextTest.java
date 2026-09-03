package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A name handed to a function as text is read the way a name written in a statement is read.
 *
 * <p>The quotes around a part of it are identifier quoting rather than characters of the name, and
 * a part written without them is folded to lower case. A sequence created as {@code "MiXeD"} is
 * therefore drawn from by writing the quotes again, and not by writing the letters the same way;
 * and it is the name so read, not the text the statement wrote, that a complaint quotes back.
 */
class NamesReadFromTextTest {

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

    /** A sequence answers to the name it was created under, not to every spelling of it. */
    @Test
    void whichSequenceANameReaches() throws SQLException {
        exec("CREATE SEQUENCE \"znr_MiXeD\"");
        assertEquals("42P01", stateOf("SELECT nextval('znr_MiXeD')"));
        assertTrue(messageOf("SELECT nextval('znr_MiXeD')")
                .contains("relation \"znr_mixed\" does not exist"));
        assertEquals("42P01", stateOf("SELECT currval('znr_MiXeD')"));
        assertEquals("42P01", stateOf("SELECT setval('znr_MiXeD', 5)"));
        // Written with its quotes it is the sequence that was made.
        assertEquals("1", one("SELECT nextval('\"znr_MiXeD\"')::text"));
        assertEquals("1", one("SELECT currval('\"znr_MiXeD\"')::text"));
        exec("DROP SEQUENCE \"znr_MiXeD\"");
        // A sequence created without quotes was folded when it was made, so the letters may be
        // written any way at all.
        exec("CREATE SEQUENCE znr_PlAiN");
        assertEquals("1", one("SELECT nextval('znr_PlAiN')::text"));
        assertEquals("2", one("SELECT nextval('znr_plain')::text"));
        exec("DROP SEQUENCE znr_plain");
    }

    /** The name a sequence is listed under is the name it has. */
    @Test
    void howASequenceIsListed() throws SQLException {
        exec("CREATE SEQUENCE \"znr_LiSt\"");
        assertEquals("znr_LiSt", one("SELECT sequencename FROM pg_sequences"
                + " WHERE sequencename ILIKE 'znr_list'"));
        assertEquals("znr_LiSt", one("SELECT relname FROM pg_class WHERE relname ILIKE 'znr_list'"));
        exec("DROP SEQUENCE \"znr_LiSt\"");
    }

    /** A relation named as text is looked for under the name that text reads as. */
    @Test
    void whichRelationANameReaches() throws SQLException {
        assertEquals("42P01", stateOf("SELECT 'znr_NoSuch'::regclass"));
        assertTrue(messageOf("SELECT 'znr_NoSuch'::regclass")
                .contains("relation \"znr_nosuch\" does not exist"));
        assertTrue(messageOf("SELECT '\"znr_NoSuch\"'::regclass")
                .contains("relation \"znr_NoSuch\" does not exist"));
        assertTrue(messageOf("SELECT 'public.znr_NoSuch'::regclass")
                .contains("relation \"public.znr_nosuch\" does not exist"));
        // The spaces around a name are no part of it.
        assertTrue(messageOf("SELECT '  znr_spaced  '::regclass")
                .contains("relation \"znr_spaced\" does not exist"));
        exec("CREATE TABLE \"znr_TbL\" (a int)");
        assertEquals("\"znr_TbL\"", one("SELECT '\"znr_TbL\"'::regclass::text"));
        assertEquals("42P01", stateOf("SELECT 'znr_TbL'::regclass"));
        assertTrue(messageOf("SELECT pg_get_serial_sequence('znr_TbL','a')")
                .contains("relation \"znr_tbl\" does not exist"));
        exec("DROP TABLE \"znr_TbL\"");
    }
}
