package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 9: Constraint Enforcement tests.
 * PRIMARY KEY, UNIQUE, NOT NULL, CHECK, FOREIGN KEY with CASCADE/SET NULL/RESTRICT.
 */
class ConstraintEnforcementTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- NOT NULL ----

    @Test
    void testNotNullInsertValid() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nn_valid (id INTEGER NOT NULL, name TEXT)");
            stmt.execute("INSERT INTO nn_valid (id, name) VALUES (1, 'Alice')");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM nn_valid WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
        }
    }

    @Test
    void testNotNullInsertViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nn_viol (id INTEGER NOT NULL, name TEXT)");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO nn_viol (id, name) VALUES (NULL, 'Bob')"));
            assertTrue(ex.getMessage().contains("not-null constraint"));
        }
    }

    @Test
    void testNotNullAllowsNullOnNullableColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nn_nullable (id INTEGER NOT NULL, name TEXT)");
            stmt.execute("INSERT INTO nn_nullable (id, name) VALUES (2, NULL)");
            ResultSet rs = stmt.executeQuery("SELECT name FROM nn_nullable WHERE id = 2");
            assertTrue(rs.next());
            assertNull(rs.getString("name"));
        }
    }

    // ---- PRIMARY KEY ----

    @Test
    void testPrimaryKeyInsertValid() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE pk_valid (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO pk_valid (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO pk_valid (id, name) VALUES (2, 'Bob')");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pk_valid");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testPrimaryKeyDuplicateViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE pk_dup (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO pk_dup (id, name) VALUES (100, 'First')");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO pk_dup (id, name) VALUES (100, 'Second')"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("primary key"));
        }
    }

    @Test
    void testPrimaryKeyNullViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE pk_null (id INTEGER PRIMARY KEY, name TEXT)");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO pk_null (id, name) VALUES (NULL, 'NoId')"));
            assertTrue(ex.getMessage().contains("not-null"));
        }
    }

    @Test
    void testCompositePrimaryKey() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE pk_comp (a INTEGER, b INTEGER, name TEXT, PRIMARY KEY (a, b))");
            stmt.execute("INSERT INTO pk_comp (a, b, name) VALUES (1, 1, 'First')");
            stmt.execute("INSERT INTO pk_comp (a, b, name) VALUES (1, 2, 'Second')");
            stmt.execute("INSERT INTO pk_comp (a, b, name) VALUES (2, 1, 'Third')");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO pk_comp (a, b, name) VALUES (1, 1, 'Duplicate')"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("primary key"));
        }
    }

    // ---- UNIQUE ----

    @Test
    void testUniqueConstraintValid() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uq_valid (id INTEGER, email TEXT UNIQUE)");
            stmt.execute("INSERT INTO uq_valid (id, email) VALUES (1, 'alice@test.com')");
            stmt.execute("INSERT INTO uq_valid (id, email) VALUES (2, 'bob@test.com')");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM uq_valid");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testUniqueConstraintViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uq_viol (id INTEGER, email TEXT UNIQUE)");
            stmt.execute("INSERT INTO uq_viol (id, email) VALUES (10, 'dup@test.com')");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO uq_viol (id, email) VALUES (11, 'dup@test.com')"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("unique"));
        }
    }

    @Test
    void testUniqueAllowsMultipleNulls() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uq_nulls (id INTEGER, email TEXT UNIQUE)");
            stmt.execute("INSERT INTO uq_nulls (id, email) VALUES (20, NULL)");
            stmt.execute("INSERT INTO uq_nulls (id, email) VALUES (21, NULL)");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM uq_nulls WHERE email IS NULL");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testCompositeUniqueConstraint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uq_comp (a INTEGER, b INTEGER, UNIQUE (a, b))");
            stmt.execute("INSERT INTO uq_comp (a, b) VALUES (1, 1)");
            stmt.execute("INSERT INTO uq_comp (a, b) VALUES (1, 2)");
            stmt.execute("INSERT INTO uq_comp (a, b) VALUES (2, 1)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO uq_comp (a, b) VALUES (1, 1)"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("unique"));
        }
    }

    // ---- CHECK ----

    @Test
    void testCheckConstraintValid() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ck_valid (id INTEGER, age INTEGER, CONSTRAINT age_check CHECK (age >= 0 AND age <= 150))");
            stmt.execute("INSERT INTO ck_valid (id, age) VALUES (1, 25)");
            stmt.execute("INSERT INTO ck_valid (id, age) VALUES (2, 0)");
            stmt.execute("INSERT INTO ck_valid (id, age) VALUES (3, 150)");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ck_valid");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testCheckConstraintViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ck_viol (id INTEGER, age INTEGER, CONSTRAINT ck_age CHECK (age >= 0))");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO ck_viol (id, age) VALUES (10, -1)"));
            assertTrue(ex.getMessage().contains("check constraint"));
        }
    }

    @Test
    void testCheckConstraintViolationUpperBound() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ck_upper (id INTEGER, age INTEGER, CONSTRAINT ck_age_u CHECK (age <= 150))");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO ck_upper (id, age) VALUES (11, 200)"));
            assertTrue(ex.getMessage().contains("check constraint"));
        }
    }

    // ---- FOREIGN KEY ----

    @Test
    void testForeignKeyInsertValid() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_depts (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO fk_depts (id, name) VALUES (1, 'Engineering')");
            stmt.execute("INSERT INTO fk_depts (id, name) VALUES (2, 'Sales')");

            stmt.execute("CREATE TABLE fk_emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES fk_depts(id))");
            stmt.execute("INSERT INTO fk_emps (id, name, dept_id) VALUES (1, 'Alice', 1)");
            stmt.execute("INSERT INTO fk_emps (id, name, dept_id) VALUES (2, 'Bob', 2)");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM fk_emps");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testForeignKeyInsertViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent_v (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO fk_parent_v (id, name) VALUES (1, 'P1')");

            stmt.execute("CREATE TABLE fk_child_v (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES fk_parent_v(id))");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO fk_child_v (id, parent_id) VALUES (1, 999)"));
            assertTrue(ex.getMessage().contains("foreign key"));
        }
    }

    @Test
    void testForeignKeyAllowsNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_pn (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_cn (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES fk_pn(id))");
            stmt.execute("INSERT INTO fk_cn (id, parent_id) VALUES (1, NULL)");
            ResultSet rs = stmt.executeQuery("SELECT id FROM fk_cn WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
        }
    }

    // ---- FK ON DELETE CASCADE ----

    @Test
    void testFkCascadeDelete() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE casc_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE casc_child (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, " +
                    "CONSTRAINT fk_casc FOREIGN KEY (parent_id) REFERENCES casc_parent(id) ON DELETE CASCADE)");

            stmt.execute("INSERT INTO casc_parent (id, name) VALUES (1, 'P1')");
            stmt.execute("INSERT INTO casc_parent (id, name) VALUES (2, 'P2')");
            stmt.execute("INSERT INTO casc_child (id, parent_id, name) VALUES (1, 1, 'C1')");
            stmt.execute("INSERT INTO casc_child (id, parent_id, name) VALUES (2, 1, 'C2')");
            stmt.execute("INSERT INTO casc_child (id, parent_id, name) VALUES (3, 2, 'C3')");

            stmt.execute("DELETE FROM casc_parent WHERE id = 1");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM casc_child");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            rs = stmt.executeQuery("SELECT name FROM casc_child");
            assertTrue(rs.next());
            assertEquals("C3", rs.getString("name"));
        }
    }

    // ---- FK ON DELETE SET NULL ----

    @Test
    void testFkSetNullOnDelete() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE sn_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE sn_child (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, " +
                    "CONSTRAINT fk_sn FOREIGN KEY (parent_id) REFERENCES sn_parent(id) ON DELETE SET NULL)");

            stmt.execute("INSERT INTO sn_parent (id, name) VALUES (1, 'P1')");
            stmt.execute("INSERT INTO sn_child (id, parent_id, name) VALUES (1, 1, 'C1')");
            stmt.execute("INSERT INTO sn_child (id, parent_id, name) VALUES (2, 1, 'C2')");

            stmt.execute("DELETE FROM sn_parent WHERE id = 1");

            ResultSet rs = stmt.executeQuery("SELECT id, parent_id FROM sn_child ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertNull(rs.getObject("parent_id"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertNull(rs.getObject("parent_id"));
        }
    }

    // ---- FK ON DELETE RESTRICT ----

    @Test
    void testFkRestrictOnDelete() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE rst_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE rst_child (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, " +
                    "CONSTRAINT fk_rst FOREIGN KEY (parent_id) REFERENCES rst_parent(id) ON DELETE RESTRICT)");

            stmt.execute("INSERT INTO rst_parent (id, name) VALUES (1, 'P1')");
            stmt.execute("INSERT INTO rst_child (id, parent_id, name) VALUES (1, 1, 'C1')");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("DELETE FROM rst_parent WHERE id = 1"));
            assertTrue(ex.getMessage().contains("foreign key"));
        }
    }

    // ---- UPDATE constraint enforcement ----

    @Test
    void testUpdatePrimaryKeyViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE pk_upd (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO pk_upd (id, name) VALUES (1, 'One')");
            stmt.execute("INSERT INTO pk_upd (id, name) VALUES (2, 'Two')");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("UPDATE pk_upd SET id = 1 WHERE id = 2"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("primary key"));
        }
    }

    @Test
    void testUpdateUniqueViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uq_upd (id INTEGER, email TEXT UNIQUE)");
            stmt.execute("INSERT INTO uq_upd (id, email) VALUES (1, 'a@test.com')");
            stmt.execute("INSERT INTO uq_upd (id, email) VALUES (2, 'b@test.com')");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("UPDATE uq_upd SET email = 'a@test.com' WHERE id = 2"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("unique"));
        }
    }

    @Test
    void testUpdateSameRowNoViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uq_same (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO uq_same (id, name) VALUES (1, 'One')");

            // Updating a row without changing PK should be fine
            stmt.execute("UPDATE uq_same SET name = 'Updated' WHERE id = 1");
            ResultSet rs = stmt.executeQuery("SELECT name FROM uq_same WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Updated", rs.getString("name"));
        }
    }

    @Test
    void testUpdateCheckViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ck_upd (id INTEGER, age INTEGER, CONSTRAINT ck_upd_age CHECK (age >= 0))");
            stmt.execute("INSERT INTO ck_upd (id, age) VALUES (50, 30)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("UPDATE ck_upd SET age = -5 WHERE id = 50"));
            assertTrue(ex.getMessage().contains("check constraint"));
        }
    }

    @Test
    void testUpdateForeignKeyViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_upd_p (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO fk_upd_p (id, name) VALUES (1, 'Eng')");

            stmt.execute("CREATE TABLE fk_upd_c (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES fk_upd_p(id))");
            stmt.execute("INSERT INTO fk_upd_c (id, dept_id) VALUES (1, 1)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("UPDATE fk_upd_c SET dept_id = 999 WHERE id = 1"));
            assertTrue(ex.getMessage().contains("foreign key"));
        }
    }

    // ---- ALTER TABLE ADD/DROP CONSTRAINT ----

    @Test
    void testAlterTableAddUniqueConstraint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE alt_uq (id INTEGER, code TEXT)");
            stmt.execute("INSERT INTO alt_uq (id, code) VALUES (1, 'A')");
            stmt.execute("ALTER TABLE alt_uq ADD CONSTRAINT uq_code UNIQUE (code)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO alt_uq (id, code) VALUES (2, 'A')"));
            assertTrue(ex.getMessage().contains("duplicate key") || ex.getMessage().contains("unique"));
        }
    }

    @Test
    void testAlterTableDropConstraint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE alt_drop (id INTEGER, code TEXT, CONSTRAINT uq_drop UNIQUE (code))");
            stmt.execute("INSERT INTO alt_drop (id, code) VALUES (1, 'X')");

            // With constraint, duplicate should fail
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO alt_drop (id, code) VALUES (2, 'X')"));

            // Drop constraint
            stmt.execute("ALTER TABLE alt_drop DROP CONSTRAINT uq_drop");

            // Now duplicate should work
            stmt.execute("INSERT INTO alt_drop (id, code) VALUES (2, 'X')");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM alt_drop WHERE code = 'X'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // ---- Column-level REFERENCES ----

    @Test
    void testColumnLevelForeignKey() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE cl_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO cl_parent (id, name) VALUES (1, 'P1')");
            stmt.execute("INSERT INTO cl_parent (id, name) VALUES (2, 'P2')");

            stmt.execute("CREATE TABLE cl_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES cl_parent(id))");
            stmt.execute("INSERT INTO cl_child (id, parent_id) VALUES (1, 1)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO cl_child (id, parent_id) VALUES (2, 999)"));
            assertTrue(ex.getMessage().contains("foreign key"));
        }
    }

    // ---- FK default action (NO ACTION) prevents delete ----

    @Test
    void testFkNoActionOnDelete() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE na_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE na_child (id INTEGER PRIMARY KEY, parent_id INTEGER, " +
                    "CONSTRAINT fk_na FOREIGN KEY (parent_id) REFERENCES na_parent(id))");

            stmt.execute("INSERT INTO na_parent (id, name) VALUES (1, 'P1')");
            stmt.execute("INSERT INTO na_child (id, parent_id) VALUES (1, 1)");

            // Default FK action is NO ACTION, so delete should fail
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("DELETE FROM na_parent WHERE id = 1"));
            assertTrue(ex.getMessage().contains("foreign key"));
        }
    }
}
