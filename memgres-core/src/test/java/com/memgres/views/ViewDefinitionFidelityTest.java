package com.memgres.views;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * View definition fidelity:
 *  1. SELECT * is expanded (frozen) at CREATE VIEW time — columns added to a base
 *     table later are invisible through pre-existing views.
 *  2. CREATE [MATERIALIZED] VIEW v(a, b) column alias lists are applied to the
 *     view's output columns (and validated: more names than columns is an error).
 *  3. Materialized views created WITH NO DATA are unreadable (SQLSTATE 55000)
 *     until REFRESH MATERIALIZED VIEW; REFRESH ... WITH NO DATA depopulates again.
 */
class ViewDefinitionFidelityTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
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

    private static List<String> resultColumns(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> cols = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i).toLowerCase());
            return cols;
        }
    }

    private static List<String> firstColumnValues(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    // ---- 1. SELECT * frozen at CREATE VIEW time ----

    @Test
    void star_frozen_plain_added_column_invisible() throws SQLException {
        exec("CREATE TABLE sf_t (x int, y text)");
        exec("INSERT INTO sf_t VALUES (1, 'a')");
        exec("CREATE VIEW sf_v AS SELECT * FROM sf_t");
        exec("ALTER TABLE sf_t ADD COLUMN z int");
        try {
            assertEquals(List.of("x", "y"), resultColumns("SELECT * FROM sf_v"));
            // A view created after the ALTER sees the new column
            exec("CREATE VIEW sf_v_new AS SELECT * FROM sf_t");
            assertEquals(List.of("x", "y", "z"), resultColumns("SELECT * FROM sf_v_new"));
        } finally {
            exec("DROP VIEW IF EXISTS sf_v_new");
            exec("DROP VIEW sf_v");
            exec("DROP TABLE sf_t");
        }
    }

    @Test
    void star_frozen_qualified_alias_star() throws SQLException {
        exec("CREATE TABLE sfq_t (x int, y text)");
        exec("INSERT INTO sfq_t VALUES (7, 'q')");
        exec("CREATE VIEW sfq_v AS SELECT t.* FROM sfq_t t");
        exec("ALTER TABLE sfq_t ADD COLUMN z int");
        try {
            assertEquals(List.of("x", "y"), resultColumns("SELECT * FROM sfq_v"));
            assertEquals(List.of("7"), firstColumnValues("SELECT x FROM sfq_v"));
        } finally {
            exec("DROP VIEW sfq_v");
            exec("DROP TABLE sfq_t");
        }
    }

    @Test
    void star_frozen_join_with_using() throws SQLException {
        exec("CREATE TABLE sfj_a (id int, an text)");
        exec("CREATE TABLE sfj_b (id int, bn text)");
        exec("INSERT INTO sfj_a VALUES (1, 'left')");
        exec("INSERT INTO sfj_b VALUES (1, 'right')");
        exec("CREATE VIEW sfj_v AS SELECT * FROM sfj_a JOIN sfj_b USING (id)");
        try {
            // USING-merged column appears once
            assertEquals(List.of("id", "an", "bn"), resultColumns("SELECT * FROM sfj_v"));
            exec("ALTER TABLE sfj_b ADD COLUMN extra int");
            assertEquals(List.of("id", "an", "bn"), resultColumns("SELECT * FROM sfj_v"));
            assertEquals(List.of("left"), firstColumnValues("SELECT an FROM sfj_v WHERE id = 1"));
        } finally {
            exec("DROP VIEW sfj_v");
            exec("DROP TABLE sfj_a");
            exec("DROP TABLE sfj_b");
        }
    }

    @Test
    void star_frozen_join_on_clause_all_columns() throws SQLException {
        exec("CREATE TABLE sfo_a (aid int, av text)");
        exec("CREATE TABLE sfo_b (bid int, bv text)");
        exec("INSERT INTO sfo_a VALUES (1, 'x')");
        exec("INSERT INTO sfo_b VALUES (1, 'y')");
        exec("CREATE VIEW sfo_v AS SELECT * FROM sfo_a a JOIN sfo_b b ON a.aid = b.bid");
        exec("ALTER TABLE sfo_a ADD COLUMN a2 int");
        try {
            assertEquals(List.of("aid", "av", "bid", "bv"), resultColumns("SELECT * FROM sfo_v"));
        } finally {
            exec("DROP VIEW sfo_v");
            exec("DROP TABLE sfo_a");
            exec("DROP TABLE sfo_b");
        }
    }

    @Test
    void star_frozen_view_over_view() throws SQLException {
        exec("CREATE TABLE sfvv_t (x int)");
        exec("INSERT INTO sfvv_t VALUES (5)");
        exec("CREATE VIEW sfvv_v1 AS SELECT * FROM sfvv_t");
        exec("CREATE VIEW sfvv_v2 AS SELECT * FROM sfvv_v1");
        exec("ALTER TABLE sfvv_t ADD COLUMN y int");
        try {
            assertEquals(List.of("x"), resultColumns("SELECT * FROM sfvv_v1"));
            assertEquals(List.of("x"), resultColumns("SELECT * FROM sfvv_v2"));
            assertEquals(List.of("5"), firstColumnValues("SELECT x FROM sfvv_v2"));
        } finally {
            exec("DROP VIEW sfvv_v2");
            exec("DROP VIEW sfvv_v1");
            exec("DROP TABLE sfvv_t");
        }
    }

    @Test
    void star_frozen_values_still_visible_through_view() throws SQLException {
        // Data inserted after view creation flows through; only the column list is frozen
        exec("CREATE TABLE sfd_t (x int)");
        exec("CREATE VIEW sfd_v AS SELECT * FROM sfd_t");
        exec("ALTER TABLE sfd_t ADD COLUMN y int");
        exec("INSERT INTO sfd_t VALUES (1, 10), (2, 20)");
        try {
            assertEquals(List.of("1", "2"), firstColumnValues("SELECT x FROM sfd_v ORDER BY x"));
        } finally {
            exec("DROP VIEW sfd_v");
            exec("DROP TABLE sfd_t");
        }
    }

    // ---- 2. Column alias lists ----

    @Test
    void alias_list_select_and_order_by() throws SQLException {
        exec("CREATE TABLE al_t (x int, y text)");
        exec("INSERT INTO al_t VALUES (2, 'b'), (1, 'a')");
        exec("CREATE VIEW al_v (p, q) AS SELECT x, y FROM al_t");
        try {
            assertEquals(List.of("p", "q"), resultColumns("SELECT * FROM al_v"));
            assertEquals(List.of("1", "2"), firstColumnValues("SELECT p FROM al_v ORDER BY p"));
            assertEquals(List.of("b", "a"), firstColumnValues("SELECT q FROM al_v ORDER BY p DESC"));
            // Old column names are not visible through the view
            assertThrows(SQLException.class, () -> firstColumnValues("SELECT x FROM al_v"));
        } finally {
            exec("DROP VIEW al_v");
            exec("DROP TABLE al_t");
        }
    }

    @Test
    void alias_list_view_over_view() throws SQLException {
        exec("CREATE TABLE alvv_t (x int)");
        exec("INSERT INTO alvv_t VALUES (3)");
        exec("CREATE VIEW alvv_v1 (a) AS SELECT x FROM alvv_t");
        exec("CREATE VIEW alvv_v2 AS SELECT a FROM alvv_v1");
        exec("CREATE VIEW alvv_v3 (b) AS SELECT a FROM alvv_v1");
        try {
            assertEquals(List.of("3"), firstColumnValues("SELECT a FROM alvv_v2"));
            assertEquals(List.of("3"), firstColumnValues("SELECT b FROM alvv_v3"));
        } finally {
            exec("DROP VIEW alvv_v3");
            exec("DROP VIEW alvv_v2");
            exec("DROP VIEW alvv_v1");
            exec("DROP TABLE alvv_t");
        }
    }

    @Test
    void alias_list_applied_to_star() throws SQLException {
        exec("CREATE TABLE als_t (x int, y text)");
        exec("INSERT INTO als_t VALUES (9, 'z')");
        exec("CREATE VIEW als_v (a, b) AS SELECT * FROM als_t");
        try {
            assertEquals(List.of("a", "b"), resultColumns("SELECT * FROM als_v"));
            assertEquals(List.of("9"), firstColumnValues("SELECT a FROM als_v"));
        } finally {
            exec("DROP VIEW als_v");
            exec("DROP TABLE als_t");
        }
    }

    @Test
    void alias_list_in_catalogs() throws SQLException {
        exec("CREATE TABLE alc_t (x int, y text)");
        exec("CREATE VIEW alc_v (p, q) AS SELECT x, y FROM alc_t");
        try {
            assertEquals(List.of("p", "q"), firstColumnValues(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'alc_v' ORDER BY ordinal_position"));
            assertEquals(List.of("p", "q"), firstColumnValues(
                    "SELECT attname FROM pg_attribute WHERE attrelid = 'alc_v'::regclass AND attnum > 0 ORDER BY attnum"));
        } finally {
            exec("DROP VIEW alc_v");
            exec("DROP TABLE alc_t");
        }
    }

    @Test
    void alias_more_names_than_columns_errors() throws SQLException {
        exec("CREATE TABLE alm_t (x int, y text)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("CREATE VIEW alm_v (a, b, c) AS SELECT x, y FROM alm_t"));
            assertEquals("42601", ex.getSQLState());
            assertTrue(ex.getMessage().contains("more column names than columns"),
                    "unexpected message: " + ex.getMessage());
            // View must not exist
            assertThrows(SQLException.class, () -> firstColumnValues("SELECT * FROM alm_v"));
        } finally {
            exec("DROP TABLE alm_t");
        }
    }

    @Test
    void alias_fewer_names_than_columns_ok() throws SQLException {
        exec("CREATE TABLE alf_t (x int, y text)");
        exec("INSERT INTO alf_t VALUES (4, 'd')");
        exec("CREATE VIEW alf_v (a) AS SELECT x, y FROM alf_t");
        try {
            assertEquals(List.of("a", "y"), resultColumns("SELECT * FROM alf_v"));
            assertEquals(List.of("4"), firstColumnValues("SELECT a FROM alf_v"));
            assertEquals(List.of("d"), firstColumnValues("SELECT y FROM alf_v"));
        } finally {
            exec("DROP VIEW alf_v");
            exec("DROP TABLE alf_t");
        }
    }

    @Test
    void matview_alias_list() throws SQLException {
        exec("CREATE TABLE mval_t (x int, y text)");
        exec("INSERT INTO mval_t VALUES (1, 'm')");
        exec("CREATE MATERIALIZED VIEW mval_mv (a, b) AS SELECT x, y FROM mval_t");
        try {
            assertEquals(List.of("a", "b"), resultColumns("SELECT * FROM mval_mv"));
            assertEquals(List.of("m"), firstColumnValues("SELECT b FROM mval_mv"));
            // PG excludes materialized views from information_schema.columns;
            // aliased columns are visible via pg_attribute instead
            assertEquals(List.of(), firstColumnValues(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'mval_mv' ORDER BY ordinal_position"));
            assertEquals(List.of("a", "b"), firstColumnValues(
                    "SELECT attname FROM pg_attribute WHERE attrelid = 'mval_mv'::regclass AND attnum > 0 ORDER BY attnum"));
        } finally {
            exec("DROP MATERIALIZED VIEW mval_mv");
            exec("DROP TABLE mval_t");
        }
    }

    @Test
    void matview_too_many_names_errors() throws SQLException {
        exec("CREATE TABLE mvtm_t (x int)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("CREATE MATERIALIZED VIEW mvtm_mv (a, b) AS SELECT x FROM mvtm_t"));
            assertEquals("42601", ex.getSQLState());
            assertTrue(ex.getMessage().contains("too many column names"),
                    "unexpected message: " + ex.getMessage());
        } finally {
            exec("DROP TABLE mvtm_t");
        }
    }

    // ---- 3. Materialized view WITH NO DATA / populated flag ----

    @Test
    void matview_with_no_data_scan_errors_55000() throws SQLException {
        exec("CREATE TABLE mvnd_t (x int)");
        exec("INSERT INTO mvnd_t VALUES (1)");
        exec("CREATE MATERIALIZED VIEW mvnd_mv AS SELECT x FROM mvnd_t WITH NO DATA");
        try {
            SQLException ex = assertThrows(SQLException.class, () -> firstColumnValues("SELECT * FROM mvnd_mv"));
            assertEquals("55000", ex.getSQLState());
            assertTrue(ex.getMessage().contains("has not been populated"),
                    "unexpected message: " + ex.getMessage());
            // Aggregates over the matview also fail
            SQLException ex2 = assertThrows(SQLException.class, () -> firstColumnValues("SELECT count(*) FROM mvnd_mv"));
            assertEquals("55000", ex2.getSQLState());
        } finally {
            exec("DROP MATERIALIZED VIEW mvnd_mv");
            exec("DROP TABLE mvnd_t");
        }
    }

    @Test
    void matview_refresh_makes_readable_then_no_data_depopulates() throws SQLException {
        exec("CREATE TABLE mvrf_t (x int)");
        exec("INSERT INTO mvrf_t VALUES (1), (2)");
        exec("CREATE MATERIALIZED VIEW mvrf_mv AS SELECT x FROM mvrf_t WITH NO DATA");
        try {
            exec("REFRESH MATERIALIZED VIEW mvrf_mv");
            assertEquals(List.of("2"), firstColumnValues("SELECT count(*) FROM mvrf_mv"));
            // REFRESH ... WITH NO DATA depopulates the view again
            exec("REFRESH MATERIALIZED VIEW mvrf_mv WITH NO DATA");
            SQLException ex = assertThrows(SQLException.class, () -> firstColumnValues("SELECT * FROM mvrf_mv"));
            assertEquals("55000", ex.getSQLState());
            // ... and a plain REFRESH brings it back
            exec("REFRESH MATERIALIZED VIEW mvrf_mv WITH DATA");
            assertEquals(List.of("1", "2"), firstColumnValues("SELECT x FROM mvrf_mv ORDER BY x"));
        } finally {
            exec("DROP MATERIALIZED VIEW mvrf_mv");
            exec("DROP TABLE mvrf_t");
        }
    }

    private static boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getBoolean(1);
        }
    }

    @Test
    void pg_matviews_ispopulated_flag() throws SQLException {
        exec("CREATE TABLE mvip_t (x int)");
        exec("CREATE MATERIALIZED VIEW mvip_mv AS SELECT x FROM mvip_t WITH NO DATA");
        try {
            assertFalse(queryBool("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'mvip_mv'"));
            exec("REFRESH MATERIALIZED VIEW mvip_mv");
            assertTrue(queryBool("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'mvip_mv'"));
            exec("REFRESH MATERIALIZED VIEW mvip_mv WITH NO DATA");
            assertFalse(queryBool("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'mvip_mv'"));
        } finally {
            exec("DROP MATERIALIZED VIEW mvip_mv");
            exec("DROP TABLE mvip_t");
        }
    }

    @Test
    void matview_with_no_data_columns_describable() throws SQLException {
        // PG allows describing an unpopulated matview's columns via pg_attribute
        // (matviews are excluded from information_schema.columns)
        exec("CREATE TABLE mvdc_t (x int, y text)");
        exec("CREATE MATERIALIZED VIEW mvdc_mv (a, b) AS SELECT x, y FROM mvdc_t WITH NO DATA");
        try {
            assertEquals(List.of("a", "b"), firstColumnValues(
                    "SELECT attname FROM pg_attribute WHERE attrelid = 'mvdc_mv'::regclass AND attnum > 0 ORDER BY attnum"));
            assertEquals(List.of(), firstColumnValues(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'mvdc_mv' ORDER BY ordinal_position"));
        } finally {
            exec("DROP MATERIALIZED VIEW mvdc_mv");
            exec("DROP TABLE mvdc_t");
        }
    }
}
