package com.memgres.types;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #11: generate_subscripts on 2D array produces wrong data.
 * The query uses two generate_subscripts calls in the SELECT list which
 * should produce a cross-product of subscripts for each dimension.
 */
class GenerateSubscripts2dTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    // Exact query from 18_arrays_ranges_jsonb_deep.sql
    @Test void generate_subscripts_2d_array_both_dimensions() throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 1), generate_subscripts(ARRAY[[1,2],[3,4]], 2)")) {
            while (rs.next()) rows.add(Cols.listOf(rs.getString(1), rs.getString(2)));
        }
        // PG: produces cross-product of subscripts: (1,1),(1,2),(2,1),(2,2)
        // Both dimensions have subscripts 1,2. The two SRFs in SELECT are zipped.
        // PG returns 2 rows: (1,1) and (2,2). SRFs in SELECT are zipped, not cross-joined.
        assertEquals(2, rows.size(),
            "Two generate_subscripts SRFs in SELECT should produce 2 rows (zipped), got " + rows.size() + ": " + rows);
        // Row 1: (1, 1), Row 2: (2, 2)
        assertEquals("1", rows.get(0).get(0));
        assertEquals("1", rows.get(0).get(1));
        assertEquals("2", rows.get(1).get(0));
        assertEquals("2", rows.get(1).get(1));
    }
}
