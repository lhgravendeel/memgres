package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 5: ALTER DOMAIN. Validates that ALTER DOMAIN operations work correctly,
 * matching PG18 behavior. Currently regressed: ALTER DOMAIN is incorrectly
 * routed through GUC settings and rejected as "unrecognized configuration parameter".
 */
class AlterDomainTest {

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

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void assertSqlFails(String sql, String expectedState) {
        try {
            exec(sql);
            fail("Expected error for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got: " + e.getMessage());
        }
    }

    // ========================================================================
    // ALTER DOMAIN SET DEFAULT / DROP DEFAULT
    // ========================================================================

    @Test
    void alter_domain_set_default() throws SQLException {
        exec("CREATE DOMAIN test_dom AS int");
        try {
            exec("ALTER DOMAIN test_dom SET DEFAULT 42");
            // Verify: create a table using the domain, insert without value
            exec("CREATE TABLE dom_test_t (id INT, val test_dom)");
            exec("INSERT INTO dom_test_t (id) VALUES (1)");
            assertEquals("42", q("SELECT val FROM dom_test_t WHERE id = 1"));
            exec("DROP TABLE dom_test_t");
        } finally {
            exec("DROP DOMAIN IF EXISTS test_dom CASCADE");
        }
    }

    @Test
    void alter_domain_drop_default() throws SQLException {
        exec("CREATE DOMAIN test_dom2 AS int DEFAULT 99");
        try {
            exec("ALTER DOMAIN test_dom2 DROP DEFAULT");
            // After dropping default, inserting without value should give NULL
            exec("CREATE TABLE dom_test_t2 (id INT, val test_dom2)");
            exec("INSERT INTO dom_test_t2 (id) VALUES (1)");
            assertNull(q("SELECT val FROM dom_test_t2 WHERE id = 1"));
            exec("DROP TABLE dom_test_t2");
        } finally {
            exec("DROP DOMAIN IF EXISTS test_dom2 CASCADE");
        }
    }

    // ========================================================================
    // ALTER DOMAIN ADD CONSTRAINT / DROP CONSTRAINT
    // ========================================================================

    @Test
    void alter_domain_add_check_constraint() throws SQLException {
        exec("CREATE DOMAIN pos_int AS int");
        try {
            exec("ALTER DOMAIN pos_int ADD CONSTRAINT pos_int_check CHECK (VALUE > 0)");
            // Verify constraint is enforced
            exec("CREATE TABLE dom_ck_t (id INT, val pos_int)");
            exec("INSERT INTO dom_ck_t VALUES (1, 10)"); // should succeed
            assertThrows(SQLException.class, () -> exec("INSERT INTO dom_ck_t VALUES (2, -5)")); // should fail
            exec("DROP TABLE dom_ck_t");
        } finally {
            exec("DROP DOMAIN IF EXISTS pos_int CASCADE");
        }
    }

    @Test
    void alter_domain_drop_constraint() throws SQLException {
        exec("CREATE DOMAIN bounded_int AS int CHECK (VALUE BETWEEN 1 AND 100)");
        try {
            // Constraint name is auto-generated, but we can add a named one
            exec("ALTER DOMAIN bounded_int ADD CONSTRAINT upper_bound CHECK (VALUE < 50)");
            exec("ALTER DOMAIN bounded_int DROP CONSTRAINT upper_bound");
            // After dropping, values up to 100 should work again
            exec("CREATE TABLE dom_drop_t (id INT, val bounded_int)");
            exec("INSERT INTO dom_drop_t VALUES (1, 75)"); // should succeed (< 100 but > 50)
            exec("DROP TABLE dom_drop_t");
        } finally {
            exec("DROP DOMAIN IF EXISTS bounded_int CASCADE");
        }
    }

    @Test
    void alter_domain_add_not_null_constraint() throws SQLException {
        exec("CREATE DOMAIN notnull_dom AS varchar(50)");
        try {
            exec("ALTER DOMAIN notnull_dom ADD CONSTRAINT nn CHECK (VALUE IS NOT NULL)");
            exec("CREATE TABLE dom_nn_t (id INT, val notnull_dom)");
            exec("INSERT INTO dom_nn_t VALUES (1, 'hello')"); // should succeed
            // Inserting NULL should fail
            assertThrows(SQLException.class, () -> exec("INSERT INTO dom_nn_t VALUES (2, NULL)"));
            exec("DROP TABLE dom_nn_t");
        } finally {
            exec("DROP DOMAIN IF EXISTS notnull_dom CASCADE");
        }
    }

    // ========================================================================
    // ALTER DOMAIN on nonexistent domain should fail
    // ========================================================================

    @Test
    void alter_nonexistent_domain_fails() {
        assertSqlFails("ALTER DOMAIN no_such_domain SET DEFAULT 1", "42704");
    }

    // ========================================================================
    // ALTER DOMAIN ADD CONSTRAINT with invalid expression
    // ========================================================================

    @Test
    void alter_domain_bad_constraint_expr_fails() throws SQLException {
        exec("CREATE DOMAIN bad_ck_dom AS int");
        try {
            // missing_func doesn't exist; PG rejects at ALTER time
            assertThrows(SQLException.class, () ->
                exec("ALTER DOMAIN bad_ck_dom ADD CONSTRAINT bad_ck CHECK (missing_func(VALUE))"));
        } finally {
            exec("DROP DOMAIN IF EXISTS bad_ck_dom CASCADE");
        }
    }

    // ========================================================================
    // ALTER DOMAIN VALIDATE CONSTRAINT
    // ========================================================================

    @Test
    void alter_domain_validate_constraint() throws SQLException {
        exec("CREATE DOMAIN val_dom AS int");
        try {
            exec("ALTER DOMAIN val_dom ADD CONSTRAINT val_check CHECK (VALUE > 0)");
            // VALIDATE CONSTRAINT should succeed (validates existing data)
            exec("ALTER DOMAIN val_dom VALIDATE CONSTRAINT val_check");
        } finally {
            exec("DROP DOMAIN IF EXISTS val_dom CASCADE");
        }
    }
}
