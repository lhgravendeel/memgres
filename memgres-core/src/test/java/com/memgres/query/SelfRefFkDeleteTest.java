package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that self-referential FK RESTRICT allows DELETE FROM (all rows)
 * when all referencing rows are also being deleted in the same statement.
 */
class SelfRefFkDeleteTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void delete_all_self_referential() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE emp (id int PRIMARY KEY, mgr_id int REFERENCES emp(id))");
            s.execute("INSERT INTO emp VALUES (1, NULL)");
            s.execute("INSERT INTO emp VALUES (2, 1)");
            s.execute("INSERT INTO emp VALUES (3, 2)");
            // PG allows this — all referencing rows are deleted in same statement
            s.execute("DELETE FROM emp");
            ResultSet rs = s.executeQuery("SELECT count(*) AS c FROM emp");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("c"));
        }
    }

    @Test void delete_where_self_referential() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE emp2 (id int PRIMARY KEY, mgr_id int REFERENCES emp2(id))");
            s.execute("INSERT INTO emp2 VALUES (1, NULL)");
            s.execute("INSERT INTO emp2 VALUES (2, 1)");
            s.execute("INSERT INTO emp2 VALUES (3, 2)");
            // Deleting all rows with WHERE clause
            s.execute("DELETE FROM emp2 WHERE id IN (1, 2, 3)");
            ResultSet rs = s.executeQuery("SELECT count(*) AS c FROM emp2");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("c"));
        }
    }

    @Test void delete_partial_still_restricts() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE emp3 (id int PRIMARY KEY, mgr_id int REFERENCES emp3(id))");
            s.execute("INSERT INTO emp3 VALUES (1, NULL)");
            s.execute("INSERT INTO emp3 VALUES (2, 1)");
            // Deleting only row 1 should fail — row 2 references it
            assertThrows(SQLException.class, () -> s.execute("DELETE FROM emp3 WHERE id = 1"));
        }
    }
}
