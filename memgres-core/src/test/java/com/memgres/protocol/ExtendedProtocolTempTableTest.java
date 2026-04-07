package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #8: Temp table ON COMMIT DROP not enforced.
 * Table should be dropped when transaction commits.
 * Extended query protocol version.
 */
class ExtendedProtocolTempTableTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void on_commit_drop_removes_table() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TEMP TABLE ext_tt_drop(a int) ON COMMIT DROP");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ext_tt_drop VALUES (?)")) {
            ps.setInt(1, 1);
            ps.execute();
        }
        conn.commit();
        // After commit, the table should be gone
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ext_tt_drop VALUES (?)")) {
            ps.setInt(1, 2);
            assertThrows(SQLException.class, ps::execute,
                "Table should have been dropped on commit");
        } catch (SQLException e) {
            conn.rollback();
        }
    }
}
