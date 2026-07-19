package com.memgres.views;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for view DML resolution issues:
 * C1 - DML on aggregate views should error 55000
 * H3 - DML through views with renamed/reordered/subset columns
 * H4 - INSTEAD OF triggers on non-auto-updatable (join) views
 * H5 - WITH CASCADED CHECK OPTION re-checks parent-view predicates
 * M5 - CREATE OR REPLACE VIEW rejects column renames/retypes
 */
class ViewDmlResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private String queryStr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // ---- C1: DML on aggregate views must error 55000 ----

    @Test void delete_from_aggregate_view_errors_55000() throws Exception {
        exec("CREATE TABLE c1_t (n INT)");
        exec("INSERT INTO c1_t VALUES (1),(2),(3)");
        exec("CREATE VIEW c1_sum AS SELECT sum(n) AS s FROM c1_t");
        try {
            SQLException ex = assertThrows(SQLException.class, () -> exec("DELETE FROM c1_sum"));
            assertEquals("55000", ex.getSQLState());
        } finally {
            exec("DROP VIEW c1_sum");
            exec("DROP TABLE c1_t");
        }
    }

    @Test void insert_into_aggregate_view_errors_55000() throws Exception {
        exec("CREATE TABLE c1b_t (n INT)");
        exec("CREATE VIEW c1b_v AS SELECT count(*) AS cnt FROM c1b_t");
        try {
            SQLException ex = assertThrows(SQLException.class, () -> exec("INSERT INTO c1b_v VALUES (5)"));
            assertEquals("55000", ex.getSQLState());
        } finally {
            exec("DROP VIEW c1b_v");
            exec("DROP TABLE c1b_t");
        }
    }

    @Test void update_aggregate_view_errors_55000() throws Exception {
        exec("CREATE TABLE c1c_t (n INT)");
        exec("INSERT INTO c1c_t VALUES (10)");
        exec("CREATE VIEW c1c_v AS SELECT max(n) AS mx FROM c1c_t");
        try {
            SQLException ex = assertThrows(SQLException.class, () -> exec("UPDATE c1c_v SET mx = 99"));
            assertEquals("55000", ex.getSQLState());
        } finally {
            exec("DROP VIEW c1c_v");
            exec("DROP TABLE c1c_t");
        }
    }

    // ---- H3: DML through views with renamed columns ----

    @Test void update_through_renamed_column_view() throws Exception {
        exec("CREATE TABLE h3_t (id INT PRIMARY KEY, name TEXT)");
        exec("INSERT INTO h3_t VALUES (1, 'alice'), (2, 'bob')");
        exec("CREATE VIEW h3_v AS SELECT id, name AS label FROM h3_t");
        try {
            exec("UPDATE h3_v SET label = 'ALICE' WHERE id = 1");
            assertEquals("ALICE", queryStr("SELECT name FROM h3_t WHERE id = 1"));
        } finally {
            exec("DROP VIEW h3_v");
            exec("DROP TABLE h3_t");
        }
    }

    @Test void insert_through_subset_column_view() throws Exception {
        exec("CREATE TABLE h3b_t (id SERIAL PRIMARY KEY, val INT, note TEXT DEFAULT 'n/a')");
        exec("CREATE VIEW h3b_v AS SELECT id, val FROM h3b_t");
        try {
            exec("INSERT INTO h3b_v (val) VALUES (42)");
            assertEquals(42, queryInt("SELECT val FROM h3b_t WHERE id = 1"));
        } finally {
            exec("DROP VIEW h3b_v");
            exec("DROP TABLE h3b_t");
        }
    }

    @Test void delete_through_reordered_column_view() throws Exception {
        exec("CREATE TABLE h3c_t (a INT, b INT)");
        exec("INSERT INTO h3c_t VALUES (1, 10), (2, 20)");
        exec("CREATE VIEW h3c_v AS SELECT b, a FROM h3c_t");
        try {
            exec("DELETE FROM h3c_v WHERE a = 2");
            assertEquals(1, queryInt("SELECT count(*) FROM h3c_t"));
        } finally {
            exec("DROP VIEW h3c_v");
            exec("DROP TABLE h3c_t");
        }
    }

    // ---- H4: INSTEAD OF triggers on non-auto-updatable views ----

    @Test void instead_of_trigger_on_join_view() throws Exception {
        exec("CREATE TABLE h4_a (id INT PRIMARY KEY, x TEXT)");
        exec("CREATE TABLE h4_b (id INT PRIMARY KEY, y TEXT)");
        exec("INSERT INTO h4_a VALUES (1, 'hello')");
        exec("INSERT INTO h4_b VALUES (1, 'world')");
        exec("CREATE VIEW h4_jv AS SELECT a.id, a.x, b.y FROM h4_a a JOIN h4_b b ON a.id = b.id");
        exec("CREATE OR REPLACE FUNCTION h4_ins() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO h4_a VALUES (NEW.id, NEW.x); "
                + "INSERT INTO h4_b VALUES (NEW.id, NEW.y); "
                + "RETURN NEW; END; $$");
        try {
            // INSTEAD OF trigger creation should succeed on a join view
            exec("CREATE TRIGGER h4_tr INSTEAD OF INSERT ON h4_jv FOR EACH ROW EXECUTE FUNCTION h4_ins()");
            exec("INSERT INTO h4_jv (id, x, y) VALUES (2, 'hi', 'there')");
            assertEquals("hi", queryStr("SELECT x FROM h4_a WHERE id = 2"));
            assertEquals("there", queryStr("SELECT y FROM h4_b WHERE id = 2"));
        } finally {
            exec("DROP VIEW h4_jv CASCADE");
            exec("DROP TABLE h4_a CASCADE");
            exec("DROP TABLE h4_b CASCADE");
            exec("DROP FUNCTION IF EXISTS h4_ins()");
        }
    }

    @Test void instead_of_trigger_on_aggregate_view() throws Exception {
        exec("CREATE TABLE h4b_t (n INT)");
        exec("CREATE VIEW h4b_v AS SELECT count(*) AS cnt FROM h4b_t");
        exec("CREATE OR REPLACE FUNCTION h4b_ins() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO h4b_t VALUES (NEW.cnt); RETURN NEW; END; $$");
        try {
            exec("CREATE TRIGGER h4b_tr INSTEAD OF INSERT ON h4b_v FOR EACH ROW EXECUTE FUNCTION h4b_ins()");
            exec("INSERT INTO h4b_v VALUES (99)");
            assertEquals(99, queryInt("SELECT n FROM h4b_t WHERE n = 99"));
        } finally {
            exec("DROP VIEW h4b_v CASCADE");
            exec("DROP TABLE h4b_t CASCADE");
            exec("DROP FUNCTION IF EXISTS h4b_ins()");
        }
    }

    // ---- H5: WITH CASCADED CHECK OPTION ----

    @Test void cascaded_check_option_rejects_parent_view_violation() throws Exception {
        exec("CREATE TABLE h5_t (n INT)");
        exec("INSERT INTO h5_t VALUES (5)");
        exec("CREATE VIEW h5_v1 AS SELECT n FROM h5_t WHERE n > 0");
        exec("CREATE VIEW h5_v2 AS SELECT n FROM h5_v1 WHERE n < 10 WITH CASCADED CHECK OPTION");
        try {
            // Inserting -5 violates h5_v1's WHERE (n > 0); CASCADED must catch it
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO h5_v2 VALUES (-5)"));
            assertEquals("44000", ex.getSQLState());
        } finally {
            exec("DROP VIEW h5_v2");
            exec("DROP VIEW h5_v1");
            exec("DROP TABLE h5_t");
        }
    }

    @Test void local_check_option_allows_parent_view_violation() throws Exception {
        exec("CREATE TABLE h5b_t (n INT)");
        exec("CREATE VIEW h5b_v1 AS SELECT n FROM h5b_t WHERE n > 0");
        exec("CREATE VIEW h5b_v2 AS SELECT n FROM h5b_v1 WHERE n < 100 WITH LOCAL CHECK OPTION");
        try {
            // With LOCAL, parent predicate (n > 0) is NOT checked; only local predicate (n < 100) is
            exec("INSERT INTO h5b_v2 VALUES (-5)");
            assertEquals(-5, queryInt("SELECT n FROM h5b_t WHERE n = -5"));
        } finally {
            exec("DROP VIEW h5b_v2");
            exec("DROP VIEW h5b_v1");
            exec("DROP TABLE h5b_t");
        }
    }

    // ---- M5: CREATE OR REPLACE VIEW rejects column renames ----

    @Test void replace_view_rejects_column_rename() throws Exception {
        exec("CREATE TABLE m5_t (a INT, b INT)");
        exec("CREATE VIEW m5_v AS SELECT a, b FROM m5_t");
        try {
            // Renaming column 'a' to 'x' must fail with 42P16
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("CREATE OR REPLACE VIEW m5_v AS SELECT a AS x, b FROM m5_t"));
            assertEquals("42P16", ex.getSQLState());
        } finally {
            exec("DROP VIEW m5_v");
            exec("DROP TABLE m5_t");
        }
    }

    @Test void replace_view_allows_adding_columns() throws Exception {
        exec("CREATE TABLE m5b_t (a INT, b INT, c INT)");
        exec("CREATE VIEW m5b_v AS SELECT a, b FROM m5b_t");
        try {
            // Adding a column at the end is allowed
            exec("CREATE OR REPLACE VIEW m5b_v AS SELECT a, b, c FROM m5b_t");
            // Verify the view now has 3 columns
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM m5b_v")) {
                assertEquals(3, rs.getMetaData().getColumnCount());
            }
        } finally {
            exec("DROP VIEW m5b_v");
            exec("DROP TABLE m5b_t");
        }
    }
}
