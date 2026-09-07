package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whose transaction a CALL ends.
 *
 * <p>Only the transaction a CALL opened for itself is that CALL's to end. A transaction the caller
 * opened is the caller's: committing it here made the CALL's work permanent, and the rows written
 * before it too, so a CALL inside {@code BEGIN ... ROLLBACK} kept everything the rollback was
 * there to undo.
 */
class WhoseTransactionACallEndsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zws_t (id int)");
        exec("CREATE PROCEDURE zws_add(n int) LANGUAGE plpgsql AS $$"
                + " BEGIN INSERT INTO zws_t VALUES (n); END $$");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP PROCEDURE zws_add(int)");
            exec("DROP TABLE zws_t");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static List<String> ids() throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM zws_t ORDER BY id")) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** A rolled-back transaction takes the CALL's work with it. */
    @Test
    void whatARollbackTakes() throws SQLException {
        exec("BEGIN");
        exec("INSERT INTO zws_t VALUES (3)");
        exec("CALL zws_add(4)");
        exec("ROLLBACK");
        assertEquals(java.util.Collections.emptyList(), ids());
        // One that commits keeps both.
        exec("BEGIN");
        exec("INSERT INTO zws_t VALUES (5)");
        exec("CALL zws_add(6)");
        exec("COMMIT");
        assertEquals(java.util.Arrays.asList("5", "6"), ids());
        // And a CALL on its own still commits what it wrote.
        exec("CALL zws_add(7)");
        assertEquals(java.util.Arrays.asList("5", "6", "7"), ids());
        exec("DELETE FROM zws_t");
    }
}
