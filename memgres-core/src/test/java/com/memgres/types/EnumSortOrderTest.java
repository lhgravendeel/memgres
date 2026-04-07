package com.memgres.types;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that enum values sort by their definition order (ordinal position),
 * not alphabetically.
 *
 * PG defines enum ordering by CREATE TYPE ... AS ENUM ('val1', 'val2', 'val3')
 * where val1 < val2 < val3 regardless of alphabetical order.
 * Values added via ALTER TYPE ADD VALUE respect insertion position.
 */
class EnumSortOrderTest {

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

    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Basic enum ordering by definition position
    // ========================================================================

    @Test
    void enum_order_by_definition_not_alphabetical() throws SQLException {
        exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        try {
            // 'sad' < 'ok' < 'happy' by definition order
            assertEquals("t", scalar("SELECT 'sad'::mood < 'ok'::mood"));
            assertEquals("t", scalar("SELECT 'ok'::mood < 'happy'::mood"));
            assertEquals("t", scalar("SELECT 'sad'::mood < 'happy'::mood"));

            // Alphabetically: happy < ok < sad, but enum order should be different
            assertEquals("f", scalar("SELECT 'happy'::mood < 'sad'::mood"),
                    "happy should NOT be less than sad (enum order, not alphabetical)");
        } finally {
            exec("DROP TYPE mood");
        }
    }

    @Test
    void enum_order_by_in_select() throws SQLException {
        exec("CREATE TYPE prio AS ENUM ('low', 'medium', 'high', 'critical')");
        exec("CREATE TABLE tasks(id int, p prio)");
        try {
            exec("INSERT INTO tasks VALUES (1, 'high'), (2, 'low'), (3, 'critical'), (4, 'medium')");
            List<String> ordered = column("SELECT p FROM tasks ORDER BY p");
            assertEquals(Cols.listOf("low", "medium", "high", "critical"), ordered,
                    "ORDER BY enum should use definition order, not alphabetical");
        } finally {
            exec("DROP TABLE tasks");
            exec("DROP TYPE prio");
        }
    }

    // ========================================================================
    // unnest on enum array sorts by ordinal
    // ========================================================================

    @Test
    void unnest_enum_array_order_by_ordinal() throws SQLException {
        exec("CREATE TYPE mood2 AS ENUM ('sad', 'ok', 'happy')");
        exec("CREATE TABLE people(id int, moods mood2[])");
        try {
            exec("INSERT INTO people VALUES (1, ARRAY['ok'::mood2, 'happy'::mood2])");
            exec("INSERT INTO people VALUES (2, ARRAY['sad'::mood2])");

            List<String> vals = column("SELECT unnest(moods) FROM people ORDER BY 1");
            // Enum order: sad(0) < ok(1) < happy(2) → sorted: sad, ok, happy
            assertEquals(3, vals.size(), "Should have 3 unnested values");
            assertEquals("sad", vals.get(0), "First by enum order should be 'sad'");
            assertEquals("ok", vals.get(1), "Second should be 'ok'");
            assertEquals("happy", vals.get(2), "Third should be 'happy'");
        } finally {
            exec("DROP TABLE people");
            exec("DROP TYPE mood2");
        }
    }

    @Test
    void unnest_enum_array_from_verification_pattern() throws SQLException {
        // Exact pattern from 17_domains_enums_composites.sql
        exec("CREATE TYPE mood3 AS ENUM ('sad', 'ok', 'happy')");
        exec("CREATE TABLE people3(id int PRIMARY KEY, moods mood3[])");
        try {
            exec("INSERT INTO people3 VALUES (1, ARRAY['ok'::mood3, 'happy'::mood3])");
            exec("INSERT INTO people3 VALUES (2, ARRAY['sad'::mood3])");

            List<String> vals = column("SELECT unnest(moods) FROM people3 ORDER BY 1");
            // PG returns: sad, ok, happy (enum order, not alphabetical happy, ok, sad)
            assertEquals("sad", vals.get(0), "First element should be 'sad' (lowest ordinal)");
            assertEquals("happy", vals.get(2), "Last element should be 'happy' (highest ordinal)");
        } finally {
            exec("DROP TABLE people3");
            exec("DROP TYPE mood3");
        }
    }

    // ========================================================================
    // ALTER TYPE ADD VALUE respects position
    // ========================================================================

    @Test
    void alter_type_add_value_at_end() throws SQLException {
        exec("CREATE TYPE color AS ENUM ('red', 'green', 'blue')");
        try {
            exec("ALTER TYPE color ADD VALUE 'yellow'");
            // yellow should be after blue
            assertEquals("t", scalar("SELECT 'blue'::color < 'yellow'::color"));
        } finally {
            exec("DROP TYPE color");
        }
    }

    @Test
    void alter_type_add_value_before() throws SQLException {
        exec("CREATE TYPE size AS ENUM ('small', 'large')");
        try {
            exec("ALTER TYPE size ADD VALUE 'medium' BEFORE 'large'");
            List<String> ordered = column("""
                SELECT unnest(ARRAY['large'::size, 'small'::size, 'medium'::size]) ORDER BY 1
                """);
            assertEquals(Cols.listOf("small", "medium", "large"), ordered,
                    "medium should sort between small and large");
        } finally {
            exec("DROP TYPE size");
        }
    }

    @Test
    void alter_type_add_value_after() throws SQLException {
        exec("CREATE TYPE rank AS ENUM ('bronze', 'gold')");
        try {
            exec("ALTER TYPE rank ADD VALUE 'silver' AFTER 'bronze'");
            List<String> ordered = column("""
                SELECT unnest(ARRAY['gold'::rank, 'bronze'::rank, 'silver'::rank]) ORDER BY 1
                """);
            assertEquals(Cols.listOf("bronze", "silver", "gold"), ordered,
                    "silver should sort between bronze and gold");
        } finally {
            exec("DROP TYPE rank");
        }
    }

    // ========================================================================
    // Enum MIN/MAX should use ordinal order
    // ========================================================================

    @Test
    void min_max_use_enum_order() throws SQLException {
        exec("CREATE TYPE severity AS ENUM ('low', 'medium', 'high')");
        exec("CREATE TABLE alerts(id int, sev severity)");
        try {
            exec("INSERT INTO alerts VALUES (1, 'high'), (2, 'low'), (3, 'medium')");
            assertEquals("low", scalar("SELECT min(sev) FROM alerts"),
                    "MIN should return 'low' (first in definition order)");
            assertEquals("high", scalar("SELECT max(sev) FROM alerts"),
                    "MAX should return 'high' (last in definition order)");
        } finally {
            exec("DROP TABLE alerts");
            exec("DROP TYPE severity");
        }
    }

    // ========================================================================
    // Enum comparison with NULL
    // ========================================================================

    @Test
    void enum_comparison_with_null() throws SQLException {
        exec("CREATE TYPE stat AS ENUM ('a', 'b', 'c')");
        try {
            assertNull(scalar("SELECT 'a'::stat < NULL::stat"),
                    "Comparison with NULL should return NULL");
        } finally {
            exec("DROP TYPE stat");
        }
    }

    // ========================================================================
    // Enum in GREATEST/LEAST
    // ========================================================================

    @Test
    void enum_in_greatest_least() throws SQLException {
        exec("CREATE TYPE lvl AS ENUM ('low', 'mid', 'high')");
        try {
            assertEquals("high", scalar("SELECT greatest('low'::lvl, 'high'::lvl, 'mid'::lvl)"));
            assertEquals("low", scalar("SELECT least('low'::lvl, 'high'::lvl, 'mid'::lvl)"));
        } finally {
            exec("DROP TYPE lvl");
        }
    }
}
