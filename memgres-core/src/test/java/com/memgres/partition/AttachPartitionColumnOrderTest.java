package com.memgres.partition;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ATTACH PARTITION matches columns by name, and each table keeps its own attribute
 * order, so rows must be permuted as they cross the boundary. Verified against
 * PostgreSQL 18.0.
 *
 * <p>N5: values were stored positionally, silently swapping columns.
 */
class AttachPartitionColumnOrderTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    @Test
    void rowsRoutedIntoAReorderedPartitionKeepTheirColumns() throws Exception {
        exec("CREATE TABLE ap_parent (id int, region text, val int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE ap_child (region text, id int, val int)");
        exec("ALTER TABLE ap_parent ATTACH PARTITION ap_child FOR VALUES FROM (1) TO (100)");

        exec("INSERT INTO ap_parent VALUES (1, 'b', 10)");

        assertEquals(Arrays.asList("1|b|10"), rows("SELECT id, region, val FROM ap_parent"));
        assertEquals(Arrays.asList("1|b|10"), rows("SELECT id, region, val FROM ap_child"));
    }

    /** Each table keeps its own column order for SELECT *. */
    @Test
    void selectStarKeepsEachTablesOwnColumnOrder() throws Exception {
        exec("CREATE TABLE ap_p2 (id int, region text, val int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE ap_c2 (region text, id int, val int)");
        exec("ALTER TABLE ap_p2 ATTACH PARTITION ap_c2 FOR VALUES FROM (1) TO (100)");
        exec("INSERT INTO ap_p2 VALUES (1, 'b', 10)");

        assertEquals(Arrays.asList("b|1|10"), rows("SELECT * FROM ap_c2"));
        assertEquals(Arrays.asList("1|b|10"), rows("SELECT * FROM ap_p2"));
    }

    /** A same-named column with a different type is still rejected. */
    @Test
    void mismatchedColumnTypeIsRejected() throws Exception {
        exec("CREATE TABLE ap_p3 (id int, region text) PARTITION BY RANGE (id)");
        exec("CREATE TABLE ap_c3 (region int, id text)");

        SQLException e = assertThrows(SQLException.class, () -> exec(
                "ALTER TABLE ap_p3 ATTACH PARTITION ap_c3 FOR VALUES FROM (1) TO (100)"));
        assertEquals("42804", e.getSQLState());
    }

    /** A partition whose columns already line up needs no permutation. */
    @Test
    void matchingColumnOrderStillWorks() throws Exception {
        exec("CREATE TABLE ap_p4 (id int, region text, val int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE ap_c4 (id int, region text, val int)");
        exec("ALTER TABLE ap_p4 ATTACH PARTITION ap_c4 FOR VALUES FROM (1) TO (100)");

        exec("INSERT INTO ap_p4 VALUES (5, 'z', 50)");

        assertEquals(Arrays.asList("5|z|50"), rows("SELECT id, region, val FROM ap_p4"));
        assertEquals(Arrays.asList("5|z|50"), rows("SELECT * FROM ap_c4"));
    }
}
