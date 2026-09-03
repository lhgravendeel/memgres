package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The word after a select item is that item's name.
 *
 * <p>There is nothing else it can be, so a word that opens a clause somewhere else is a name here
 * all the same: {@code SELECT a natural} is a column called natural, not the start of a join. The
 * few words this is not true of are the ones the grammar may still be expecting to continue what
 * came before -- {@code varying} after {@code character}, {@code day} after an interval -- and
 * PostgreSQL keeps that list itself, in {@code pg_get_keywords().barelabel}.
 */
class AWordAfterASelectItemTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zaw_a (n int, x int)");
            s.execute("CREATE TABLE zaw_b (n int, y int)");
            s.execute("INSERT INTO zaw_a VALUES (1, 10)");
            s.execute("INSERT INTO zaw_b VALUES (1, 20)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String labelOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.getMetaData().getColumnLabel(1);
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

    /** A word that opens a clause elsewhere still names the item it follows. */
    @Test
    void aClauseWordIsStillAName() throws SQLException {
        for (String word : new String[] {"natural", "inner", "cross", "full", "left", "right",
                "join", "using", "set", "values", "distinct", "all", "collate", "select"}) {
            assertEquals(word, labelOf("SELECT n " + word + " FROM zaw_a"), word);
        }
    }

    /** The words the grammar may still be expecting are not names. */
    @Test
    void theWordsTheGrammarIsStillExpecting() {
        for (String word : new String[] {"from", "where", "group", "order", "limit", "offset",
                "having", "window", "union", "into", "for", "fetch", "on"}) {
            assertNotNull(stateOf("SELECT n " + word + " FROM zaw_a"), word);
        }
        // varying could be the second half of a type name, and day the field of an interval.
        assertNotNull(stateOf("SELECT n varying FROM zaw_a"));
        assertNotNull(stateOf("SELECT n day FROM zaw_a"));
    }

    /** The same words still say what they say in a FROM clause. */
    @Test
    void theSameWordsInAJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM zaw_a NATURAL JOIN zaw_b")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getMetaData().getColumnCount());
            }
            try (ResultSet rs = s.executeQuery("SELECT * FROM zaw_a CROSS JOIN zaw_b")) {
                assertTrue(rs.next());
                assertEquals(4, rs.getMetaData().getColumnCount());
            }
            try (ResultSet rs = s.executeQuery(
                    "SELECT * FROM zaw_a a JOIN zaw_b b USING (n)")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getMetaData().getColumnCount());
            }
        }
    }
}
