package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whose ROW_COUNT it is.
 *
 * <p>A statement inside a nested BEGIN ... END is a statement of the same function, and what it
 * affected is what GET DIAGNOSTICS reads afterwards. Kept per block, the count died with the
 * block: an INSERT of three rows inside one reported none once the block had closed.
 */
class WhoseRowCountItIsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwr_t (id int)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwr_t");
            conn.close();
        }
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

    /** What a nested block affected is still what the call last affected. */
    @Test
    void whatANestedBlockLeavesBehind() throws SQLException {
        assertEquals("3", counting("BEGIN INSERT INTO zwr_t VALUES (1),(2),(3); END;"));
        // A block with a handler is still a block of the same call.
        assertEquals("1", counting(
                "BEGIN INSERT INTO zwr_t VALUES (4); EXCEPTION WHEN OTHERS THEN NULL; END;"));
        // Two blocks deep is no different.
        assertEquals("2", counting("BEGIN BEGIN INSERT INTO zwr_t VALUES (5),(6); END; END;"));
    }

    /** Run a body, then read ROW_COUNT outside it. */
    private static String counting(String body) throws SQLException {
        exec("CREATE FUNCTION zwr_f() RETURNS int AS $$ DECLARE n int; BEGIN "
                + body + " GET DIAGNOSTICS n = ROW_COUNT; RETURN n; END $$ LANGUAGE plpgsql");
        try {
            return one("SELECT zwr_f()::text");
        } finally {
            exec("DROP FUNCTION zwr_f()");
        }
    }
}
