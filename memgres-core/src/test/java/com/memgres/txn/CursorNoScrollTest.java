package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that FETCH PRIOR on a NO SCROLL cursor is rejected with SQLSTATE 55000
 * (object_not_in_prerequisite_state).
 */
class CursorNoScrollTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    @Test
    void fetchPrior_noScrollCursor_shouldError55000() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE ns_cursor NO SCROLL CURSOR FOR SELECT 1");
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("FETCH PRIOR FROM ns_cursor"));
            assertEquals("55000", ex.getSQLState(),
                    "FETCH PRIOR on NO SCROLL cursor should produce 55000; got: " + ex.getMessage());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }
}
