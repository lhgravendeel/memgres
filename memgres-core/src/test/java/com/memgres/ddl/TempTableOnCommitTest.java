package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/** Diff #30: Temp table with ON COMMIT DROP should be gone after COMMIT. */
class TempTableOnCommitTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false); // need explicit transactions for ON COMMIT behavior
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void on_commit_drop_removes_table() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TEMP TABLE tt_drop(a int) ON COMMIT DROP");
            s.execute("INSERT INTO tt_drop VALUES (1)");
            conn.commit();
            // After commit, the table should be gone
            assertThrows(SQLException.class, () -> {
                try (Statement s2 = conn.createStatement()) { s2.execute("INSERT INTO tt_drop VALUES (2)"); }
            }, "Table should have been dropped on commit");
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }
}
