package com.memgres.views;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual bugs from bugs-review.md around DML through views:
 *
 * <ul>
 *   <li>H3 part 1 — positional {@code INSERT INTO view VALUES (...)} through a
 *       column-reordered / renamed view must map the VALUES in <em>view-column</em>
 *       order, not base-table order (otherwise data is silently swapped).</li>
 *   <li>H3 part 2 — {@code DELETE}/{@code UPDATE} through a renamed-column view must
 *       resolve the view column names in the {@code WHERE} clause (previously 42703).</li>
 *   <li>H4 — INSTEAD OF UPDATE and INSTEAD OF DELETE triggers on a (join) view must
 *       actually fire (count = 1 and base tables modified), not silently do nothing.</li>
 * </ul>
 */
class ViewDmlResidualsTest {

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

    private int execUpdate(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { return s.executeUpdate(sql); }
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

    // ---- H3 part 1: positional INSERT through a reordered/renamed view ----

    @Test void positional_insert_through_reordered_renamed_view() throws Exception {
        exec("CREATE TABLE r1_t (id INT, n INT)");
        // View exposes columns as (num -> n, id) — reversed and renamed vs the base table.
        exec("CREATE VIEW r1_v AS SELECT n AS num, id FROM r1_t");
        try {
            // Positional VALUES are given in VIEW-column order: num=80, id=8.
            // PG inserts base row id=8, n=80.
            exec("INSERT INTO r1_v VALUES (80, 8)");
            assertEquals(8, queryInt("SELECT id FROM r1_t"));
            assertEquals(80, queryInt("SELECT n FROM r1_t"));
        } finally {
            exec("DROP VIEW r1_v");
            exec("DROP TABLE r1_t");
        }
    }

    @Test void positional_insert_through_single_renamed_subset_view() throws Exception {
        // Base table's non-first column exposed as the only (renamed) view column.
        exec("CREATE TABLE r2_t (id SERIAL PRIMARY KEY, n INT)");
        exec("CREATE VIEW r2_v AS SELECT n AS num FROM r2_t");
        try {
            exec("INSERT INTO r2_v VALUES (77)");
            // The value must land in n, not id.
            assertEquals(77, queryInt("SELECT n FROM r2_t"));
        } finally {
            exec("DROP VIEW r2_v");
            exec("DROP TABLE r2_t");
        }
    }

    // ---- H3 part 2: UPDATE/DELETE WHERE referencing a renamed view column ----

    @Test void update_where_renamed_view_column() throws Exception {
        exec("CREATE TABLE r3_t (id INT PRIMARY KEY, n INT)");
        exec("INSERT INTO r3_t VALUES (5, 50), (6, 60)");
        exec("CREATE VIEW r3_v AS SELECT n AS num, id FROM r3_t");
        try {
            int c = execUpdate("UPDATE r3_v SET num = 51 WHERE num = 50");
            assertEquals(1, c);
            assertEquals(51, queryInt("SELECT n FROM r3_t WHERE id = 5"));
            assertEquals(60, queryInt("SELECT n FROM r3_t WHERE id = 6"));
        } finally {
            exec("DROP VIEW r3_v");
            exec("DROP TABLE r3_t");
        }
    }

    @Test void delete_where_renamed_view_column() throws Exception {
        exec("CREATE TABLE r4_t (id INT PRIMARY KEY, n INT)");
        exec("INSERT INTO r4_t VALUES (5, 50), (6, 60)");
        exec("CREATE VIEW r4_v AS SELECT n AS num, id FROM r4_t");
        try {
            int c = execUpdate("DELETE FROM r4_v WHERE num = 60");
            assertEquals(1, c);
            assertEquals(1, queryInt("SELECT count(*) FROM r4_t"));
            assertEquals(5, queryInt("SELECT id FROM r4_t"));
        } finally {
            exec("DROP VIEW r4_v");
            exec("DROP TABLE r4_t");
        }
    }

    // ---- H4: INSTEAD OF UPDATE / DELETE triggers on a join view ----

    private void setUpJoinView(String prefix) throws Exception {
        exec("CREATE TABLE " + prefix + "_a (id INT PRIMARY KEY, av TEXT)");
        exec("CREATE TABLE " + prefix + "_b (id INT PRIMARY KEY, bv TEXT)");
        exec("INSERT INTO " + prefix + "_a VALUES (1, 'a1'), (2, 'a2')");
        exec("INSERT INTO " + prefix + "_b VALUES (1, 'b1'), (2, 'b2')");
        exec("CREATE VIEW " + prefix + "_jv AS SELECT a.id, a.av, b.bv FROM "
                + prefix + "_a a JOIN " + prefix + "_b b ON a.id = b.id");
    }

    private void dropJoinView(String prefix) throws Exception {
        exec("DROP VIEW " + prefix + "_jv CASCADE");
        exec("DROP TABLE " + prefix + "_a CASCADE");
        exec("DROP TABLE " + prefix + "_b CASCADE");
    }

    @Test void instead_of_update_trigger_fires_on_join_view() throws Exception {
        setUpJoinView("h4u");
        exec("CREATE OR REPLACE FUNCTION h4u_upd() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "UPDATE h4u_a SET av = NEW.av WHERE id = OLD.id; RETURN NEW; END; $$");
        try {
            exec("CREATE TRIGGER h4u_tu INSTEAD OF UPDATE ON h4u_jv "
                    + "FOR EACH ROW EXECUTE FUNCTION h4u_upd()");
            int c = execUpdate("UPDATE h4u_jv SET av = 'updated' WHERE id = 1");
            assertEquals(1, c, "INSTEAD OF UPDATE trigger must report 1 affected row");
            assertEquals("updated", queryStr("SELECT av FROM h4u_a WHERE id = 1"));
            assertEquals("a2", queryStr("SELECT av FROM h4u_a WHERE id = 2"));
        } finally {
            dropJoinView("h4u");
            exec("DROP FUNCTION IF EXISTS h4u_upd()");
        }
    }

    @Test void instead_of_delete_trigger_fires_on_join_view() throws Exception {
        setUpJoinView("h4d");
        exec("CREATE OR REPLACE FUNCTION h4d_del() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "DELETE FROM h4d_a WHERE id = OLD.id; RETURN OLD; END; $$");
        try {
            exec("CREATE TRIGGER h4d_td INSTEAD OF DELETE ON h4d_jv "
                    + "FOR EACH ROW EXECUTE FUNCTION h4d_del()");
            int c = execUpdate("DELETE FROM h4d_jv WHERE id = 2");
            assertEquals(1, c, "INSTEAD OF DELETE trigger must report 1 affected row");
            assertEquals(1, queryInt("SELECT count(*) FROM h4d_a"));
            assertEquals(1, queryInt("SELECT id FROM h4d_a"));
        } finally {
            dropJoinView("h4d");
            exec("DROP FUNCTION IF EXISTS h4d_del()");
        }
    }
}
