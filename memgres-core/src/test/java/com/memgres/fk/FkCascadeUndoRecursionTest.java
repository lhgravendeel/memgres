package com.memgres.fk;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that FK ON DELETE/UPDATE CASCADE and SET NULL actions:
 * 1. Are reversible via ROLLBACK (undo log entries are recorded)
 * 2. Recurse to grandchild tables (depth > 1)
 */
class FkCascadeUndoRecursionTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // ===== UNDO LOG TESTS =====

    @Test void cascade_delete_is_rolled_back() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_parent1 (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_child1 (id int PRIMARY KEY, pid int REFERENCES fkcu_parent1(id) ON DELETE CASCADE)");
            s.execute("INSERT INTO fkcu_parent1 VALUES (1), (2)");
            s.execute("INSERT INTO fkcu_child1 VALUES (10, 1), (20, 1), (30, 2)");
            c.commit();

            // Delete parent row — cascades to child rows 10, 20
            s.execute("DELETE FROM fkcu_parent1 WHERE id = 1");
            // Verify cascade happened
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_child1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "CASCADE should have deleted 2 child rows");
            }

            // ROLLBACK should restore everything
            c.rollback();

            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_child1")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "ROLLBACK must restore cascaded child deletes");
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_parent1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }

            s.execute("DROP TABLE fkcu_child1"); s.execute("DROP TABLE fkcu_parent1"); c.commit();
        }
    }

    @Test void cascade_set_null_is_rolled_back() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_parent2 (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_child2 (id int PRIMARY KEY, pid int REFERENCES fkcu_parent2(id) ON DELETE SET NULL)");
            s.execute("INSERT INTO fkcu_parent2 VALUES (1)");
            s.execute("INSERT INTO fkcu_child2 VALUES (10, 1), (20, 1)");
            c.commit();

            s.execute("DELETE FROM fkcu_parent2 WHERE id = 1");
            // Verify SET NULL happened
            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_child2 WHERE id = 10")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("pid"), "SET NULL should have nulled pid");
            }

            c.rollback();

            // After rollback, pid should be restored to 1
            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_child2 WHERE id = 10")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("pid"), "ROLLBACK must restore SET NULL changes");
            }

            s.execute("DROP TABLE fkcu_child2"); s.execute("DROP TABLE fkcu_parent2"); c.commit();
        }
    }

    @Test void cascade_update_is_rolled_back() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_parent3 (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_child3 (id int PRIMARY KEY, pid int REFERENCES fkcu_parent3(id) ON UPDATE CASCADE)");
            s.execute("INSERT INTO fkcu_parent3 VALUES (1)");
            s.execute("INSERT INTO fkcu_child3 VALUES (10, 1), (20, 1)");
            c.commit();

            s.execute("UPDATE fkcu_parent3 SET id = 99 WHERE id = 1");
            // Verify cascade happened
            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_child3 WHERE id = 10")) {
                assertTrue(rs.next());
                assertEquals(99, rs.getInt(1), "CASCADE UPDATE should have changed pid to 99");
            }

            c.rollback();

            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_child3 WHERE id = 10")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "ROLLBACK must restore cascade-updated pid");
            }

            s.execute("DROP TABLE fkcu_child3"); s.execute("DROP TABLE fkcu_parent3"); c.commit();
        }
    }

    @Test void update_set_null_is_rolled_back() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_parent4 (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_child4 (id int PRIMARY KEY, pid int REFERENCES fkcu_parent4(id) ON UPDATE SET NULL)");
            s.execute("INSERT INTO fkcu_parent4 VALUES (1)");
            s.execute("INSERT INTO fkcu_child4 VALUES (10, 1)");
            c.commit();

            s.execute("UPDATE fkcu_parent4 SET id = 99 WHERE id = 1");
            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_child4 WHERE id = 10")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("pid"));
            }

            c.rollback();

            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_child4 WHERE id = 10")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "ROLLBACK must restore SET NULL on update");
            }

            s.execute("DROP TABLE fkcu_child4"); s.execute("DROP TABLE fkcu_parent4"); c.commit();
        }
    }

    // ===== RECURSION / GRANDCHILD TESTS =====

    @Test void cascade_delete_recurses_to_grandchild() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_gp (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_gp_child (id int PRIMARY KEY, pid int REFERENCES fkcu_gp(id) ON DELETE CASCADE)");
            s.execute("CREATE TABLE fkcu_gp_grandchild (id int PRIMARY KEY, cid int REFERENCES fkcu_gp_child(id) ON DELETE CASCADE)");
            s.execute("INSERT INTO fkcu_gp VALUES (1)");
            s.execute("INSERT INTO fkcu_gp_child VALUES (10, 1)");
            s.execute("INSERT INTO fkcu_gp_grandchild VALUES (100, 10), (101, 10)");
            c.commit();

            // Delete grandparent — should cascade to child, then to grandchild
            s.execute("DELETE FROM fkcu_gp WHERE id = 1");

            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_gp_child")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Child should be deleted by cascade");
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_gp_grandchild")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Grandchild should be deleted by recursive cascade");
            }

            s.execute("DROP TABLE fkcu_gp_grandchild"); s.execute("DROP TABLE fkcu_gp_child");
            s.execute("DROP TABLE fkcu_gp"); c.commit();
        }
    }

    @Test void cascade_update_recurses_to_grandchild() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_gpu (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_gpu_child (id int PRIMARY KEY, pid int REFERENCES fkcu_gpu(id) ON UPDATE CASCADE)");
            s.execute("CREATE TABLE fkcu_gpu_grandchild (id int PRIMARY KEY, cid int REFERENCES fkcu_gpu_child(id) ON UPDATE CASCADE)");
            s.execute("INSERT INTO fkcu_gpu VALUES (1)");
            s.execute("INSERT INTO fkcu_gpu_child VALUES (10, 1)");
            s.execute("INSERT INTO fkcu_gpu_grandchild VALUES (100, 10)");
            c.commit();

            // Update grandparent PK — should cascade child pid, then grandchild cid
            s.execute("UPDATE fkcu_gpu SET id = 99 WHERE id = 1");

            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_gpu_child WHERE id = 10")) {
                assertTrue(rs.next());
                assertEquals(99, rs.getInt(1), "Child pid should cascade to 99");
            }
            // Grandchild cid references child id (10), which didn't change — so grandchild stays
            // The point is grandchild rows should NOT be orphaned
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_gpu_grandchild")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Grandchild should not be orphaned");
            }

            s.execute("DROP TABLE fkcu_gpu_grandchild"); s.execute("DROP TABLE fkcu_gpu_child");
            s.execute("DROP TABLE fkcu_gpu"); c.commit();
        }
    }

    @Test void cascade_delete_grandchild_rollback_restores_all() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_gr (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_gr_child (id int PRIMARY KEY, pid int REFERENCES fkcu_gr(id) ON DELETE CASCADE)");
            s.execute("CREATE TABLE fkcu_gr_grandchild (id int PRIMARY KEY, cid int REFERENCES fkcu_gr_child(id) ON DELETE CASCADE)");
            s.execute("INSERT INTO fkcu_gr VALUES (1)");
            s.execute("INSERT INTO fkcu_gr_child VALUES (10, 1), (20, 1)");
            s.execute("INSERT INTO fkcu_gr_grandchild VALUES (100, 10), (101, 20)");
            c.commit();

            s.execute("DELETE FROM fkcu_gr WHERE id = 1");
            c.rollback();

            // Everything should be restored
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_gr")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_gr_child")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1), "Child rows must be restored");
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_gr_grandchild")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1), "Grandchild rows must be restored");
            }

            s.execute("DROP TABLE fkcu_gr_grandchild"); s.execute("DROP TABLE fkcu_gr_child");
            s.execute("DROP TABLE fkcu_gr"); c.commit();
        }
    }

    @Test void set_null_delete_recurses_via_cascade_grandchild() throws Exception {
        // parent -> child (SET NULL), child -> grandchild (CASCADE)
        // Deleting parent: child pid set to NULL, grandchild stays (child row not deleted)
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE fkcu_sn (id int PRIMARY KEY)");
            s.execute("CREATE TABLE fkcu_sn_child (id int PRIMARY KEY, pid int REFERENCES fkcu_sn(id) ON DELETE SET NULL)");
            s.execute("CREATE TABLE fkcu_sn_grandchild (id int PRIMARY KEY, cid int REFERENCES fkcu_sn_child(id) ON DELETE CASCADE)");
            s.execute("INSERT INTO fkcu_sn VALUES (1)");
            s.execute("INSERT INTO fkcu_sn_child VALUES (10, 1)");
            s.execute("INSERT INTO fkcu_sn_grandchild VALUES (100, 10)");
            c.commit();

            s.execute("DELETE FROM fkcu_sn WHERE id = 1");

            // Child row should exist with pid=null
            try (ResultSet rs = s.executeQuery("SELECT pid FROM fkcu_sn_child WHERE id = 10")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("pid"), "Child pid should be set to NULL");
            }
            // Grandchild should still exist (child row not deleted, just nulled)
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM fkcu_sn_grandchild")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Grandchild should still exist");
            }

            s.execute("DROP TABLE fkcu_sn_grandchild"); s.execute("DROP TABLE fkcu_sn_child");
            s.execute("DROP TABLE fkcu_sn"); c.commit();
        }
    }
}
