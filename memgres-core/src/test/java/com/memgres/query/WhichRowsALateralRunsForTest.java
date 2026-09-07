package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which rows a lateral is run for.
 *
 * <p>A qualification that speaks only of the relations already in hand decides which of their rows
 * a function in FROM is run for. Left until after the join, the function was evaluated for rows
 * the statement had already excluded -- and one of those rows holding something the function could
 * not read failed the whole statement, over a row it was never going to answer with.
 */
class WhichRowsALateralRunsForTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zxh_d (id int, x xml)");
        exec("INSERT INTO zxh_d VALUES"
                + " (1,'<root><row><a>1</a></row><row><a>2</a></row></root>'),"
                + " (2,'<root><row/></root>'),"
                + " (4,'<root><row><a>zz</a></row></root>')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zxh_d");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
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

    /** A row the qualification excludes is never handed to the function. */
    @Test
    void whichRowsReachTheFunction() throws SQLException {
        assertEquals(java.util.Arrays.asList("1|1", "2|2"),
                rows("SELECT t.* FROM zxh_d d, xmltable('/root/row' PASSING d.x"
                        + " COLUMNS n FOR ORDINALITY, a int PATH 'a') t WHERE d.id = 1 ORDER BY n"));
        assertEquals(java.util.Arrays.asList("42"),
                rows("SELECT t.* FROM zxh_d d, xmltable('/root/row' PASSING d.x"
                        + " COLUMNS a int PATH 'a' DEFAULT 42) t WHERE d.id = 2"));
        // The row that cannot be read is still refused when the statement asks for it.
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT t.* FROM zxh_d d, xmltable('/root/row' PASSING d.x"
                    + " COLUMNS a int PATH 'a') t WHERE d.id = 4");
            fail("expected a refusal");
        } catch (SQLException e) {
            assertEquals("22P02", e.getSQLState());
        }
    }
}
