package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compatibility test for prepared-statements.sql failure (Stmt 88).
 *
 * PG 18 succeeds on EXECUTE ps_union (a PREPARE with UNION ALL),
 * returning rows [1], [2], [3]. Memgres fails with:
 *   "Internal error: IllegalStateException: Received resultset tuples,
 *    but no field structure for them"
 *
 * This test asserts the correct (PG 18) behavior so it will fail until
 * the bug is fixed.
 */
class PreparedStatementsCompat15Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS ps_test CASCADE");
            st.execute("CREATE TABLE ps_test ("
                    + "id   integer PRIMARY KEY,"
                    + "name text NOT NULL,"
                    + "val  numeric(10,2)"
                    + ")");
            st.execute("INSERT INTO ps_test VALUES "
                    + "(1, 'alpha', 10.50), (2, 'beta', 20.75), (3, 'gamma', 30.00)");
            st.execute("DEALLOCATE ALL");
            st.execute("PREPARE ps_union AS "
                    + "SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY val");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement st = conn.createStatement()) {
                st.execute("DEALLOCATE ALL");
                st.execute("DROP TABLE IF EXISTS ps_test CASCADE");
            } catch (Exception ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    /**
     * Stmt 88 - EXECUTE ps_union must return three rows: 1, 2, 3.
     *
     * The prepared statement uses UNION ALL across literal SELECTs.
     * PG 18 returns column "val" with rows [1], [2], [3].
     * Memgres currently throws an internal IllegalStateException about
     * missing field structure for result tuples.
     */
    @Test
    void executePreparedUnionAllReturnsThreeRows() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("EXECUTE ps_union")) {

            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(1, meta.getColumnCount(), "Should have exactly one column");
            assertEquals("val", meta.getColumnLabel(1).toLowerCase(),
                    "Column should be named 'val'");

            List<Integer> values = new ArrayList<>();
            while (rs.next()) {
                values.add(rs.getInt("val"));
            }
            assertEquals(List.of(1, 2, 3), values,
                    "EXECUTE ps_union should return rows 1, 2, 3 in order");
        }
    }
}
