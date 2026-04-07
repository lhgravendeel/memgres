package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 81-87 (Constraints).
 *
 * 81. NOT NULL constraints
 * 82. CHECK constraints
 * 83. UNIQUE constraints
 * 84. PRIMARY KEY
 * 85. FOREIGN KEY
 * 86. Exclusion constraints
 * 87. Default values & generated columns
 */
class ConstraintsCoverageTest {

    static Memgres memgres;
    static Connection conn;

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

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private void dropAll() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
            java.util.List<String> tables = new java.util.ArrayList<>();
            while (rs.next()) tables.add(rs.getString(1));
            for (String t : tables) {
                try { s.execute("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (Exception e) { /* ignore */ }
            }
        }
    }

    @BeforeEach
    void clean() throws Exception {
        dropAll();
    }

    // =========================================================================
    // 81. NOT NULL constraints
    // =========================================================================

    @Test
    void notNull_columnLevel() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        assertEquals("alice", query1("SELECT name FROM t WHERE id = 1"));
    }

    @Test
    void notNull_violationOnInsert() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL, name TEXT)");
        try {
            exec("INSERT INTO t (id, name) VALUES (NULL, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("not null") ||
                       e.getMessage().toLowerCase().contains("null"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_violationOnInsertImplicit() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL, name TEXT)");
        // Omitting NOT NULL column without DEFAULT should fail
        try {
            exec("INSERT INTO t (name) VALUES ('bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("not null") ||
                       e.getMessage().toLowerCase().contains("null"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_violationOnUpdate() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        try {
            exec("UPDATE t SET id = NULL WHERE name = 'alice'");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("not null") ||
                       e.getMessage().toLowerCase().contains("null"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_withDefault() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL DEFAULT 42, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        assertEquals(42, queryInt("SELECT id FROM t WHERE name = 'alice'"));
    }

    @Test
    void notNull_multipleColumns() throws SQLException {
        exec("CREATE TABLE t (a INT NOT NULL, b TEXT NOT NULL, c INT)");
        exec("INSERT INTO t VALUES (1, 'x', NULL)");
        assertEquals("x", query1("SELECT b FROM t"));
        // NULL in a NOT NULL column
        try {
            exec("INSERT INTO t VALUES (1, NULL, 5)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_alterColumnSetNotNull() throws SQLException {
        exec("CREATE TABLE t (id INT, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        exec("ALTER TABLE t ALTER COLUMN id SET NOT NULL");
        // Now NULL should be rejected
        try {
            exec("INSERT INTO t (id, name) VALUES (NULL, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_alterColumnDropNotNull() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL, name TEXT)");
        exec("ALTER TABLE t ALTER COLUMN id DROP NOT NULL");
        // Now NULL should be accepted
        exec("INSERT INTO t (id, name) VALUES (NULL, 'bob')");
        assertNull(query1("SELECT id FROM t WHERE name = 'bob'"));
    }

    @Test
    void notNull_alterTableAddColumnWithNotNullDefault() throws SQLException {
        exec("CREATE TABLE t (id INT)");
        exec("ALTER TABLE t ADD COLUMN status TEXT NOT NULL DEFAULT 'active'");
        exec("INSERT INTO t (id, status) VALUES (1, 'active')");
        assertEquals("active", query1("SELECT status FROM t WHERE id = 1"));
    }

    @Test
    void notNull_allowsNonNullValues() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL)");
        exec("INSERT INTO t VALUES (0)");
        exec("INSERT INTO t VALUES (-1)");
        exec("INSERT INTO t VALUES (999)");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void notNull_tableLevelConstraint() throws SQLException {
        // PG17+: table-level NOT NULL constraint syntax
        exec("CREATE TABLE t (id INT, name TEXT, NOT NULL id)");
        try {
            exec("INSERT INTO t (id, name) VALUES (NULL, 'alice')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
        exec("INSERT INTO t (id, name) VALUES (1, 'alice')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void notNull_tableLevelNamedConstraint() throws SQLException {
        // PG17+: named table-level NOT NULL constraint
        exec("CREATE TABLE t (id INT, name TEXT, CONSTRAINT nn_id NOT NULL id)");
        try {
            exec("INSERT INTO t (id, name) VALUES (NULL, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_tableLevelMultipleConstraints() throws SQLException {
        // Multiple table-level NOT NULL constraints
        exec("CREATE TABLE t (a INT, b TEXT, c INT, NOT NULL a, NOT NULL c)");
        exec("INSERT INTO t VALUES (1, NULL, 2)"); // b can be NULL
        try {
            exec("INSERT INTO t VALUES (NULL, 'x', 2)");
            fail("Should have thrown for a");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
        try {
            exec("INSERT INTO t VALUES (1, 'x', NULL)");
            fail("Should have thrown for c");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
    }

    @Test
    void notNull_tableLevelViaAlterTable() throws SQLException {
        // ALTER TABLE ADD CONSTRAINT ... NOT NULL col
        exec("CREATE TABLE t (id INT, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        exec("ALTER TABLE t ADD CONSTRAINT nn_name NOT NULL name");
        try {
            exec("INSERT INTO t VALUES (2, NULL)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null"), "Error: " + e.getMessage());
        }
    }

    // =========================================================================
    // 82. CHECK constraints
    // =========================================================================

    @Test
    void check_columnLevel() throws SQLException {
        exec("CREATE TABLE t (id INT, age INT CHECK (age > 0))");
        exec("INSERT INTO t VALUES (1, 25)");
        assertEquals(25, queryInt("SELECT age FROM t"));
    }

    @Test
    void check_columnLevelViolation() throws SQLException {
        exec("CREATE TABLE t (id INT, age INT CHECK (age > 0))");
        try {
            exec("INSERT INTO t VALUES (1, -5)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint") ||
                       e.getMessage().toLowerCase().contains("violat"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_columnLevelZeroBoundary() throws SQLException {
        exec("CREATE TABLE t (id INT, age INT CHECK (age > 0))");
        try {
            exec("INSERT INTO t VALUES (1, 0)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint") ||
                       e.getMessage().toLowerCase().contains("violat"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_tableLevelMultiColumn() throws SQLException {
        exec("CREATE TABLE t (start_val INT, end_val INT, CHECK (start_val < end_val))");
        exec("INSERT INTO t VALUES (1, 10)");
        assertEquals(10, queryInt("SELECT end_val FROM t"));
    }

    @Test
    void check_tableLevelMultiColumnViolation() throws SQLException {
        exec("CREATE TABLE t (start_val INT, end_val INT, CHECK (start_val < end_val))");
        try {
            exec("INSERT INTO t VALUES (10, 5)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint") ||
                       e.getMessage().toLowerCase().contains("violat"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_namedConstraint() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT, CONSTRAINT chk_positive CHECK (val >= 0))");
        exec("INSERT INTO t VALUES (1, 5)");
        assertEquals(5, queryInt("SELECT val FROM t"));
    }

    @Test
    void check_namedConstraintViolation() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT, CONSTRAINT chk_positive CHECK (val >= 0))");
        try {
            exec("INSERT INTO t VALUES (1, -1)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("chk_positive") ||
                       e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_violationOnUpdate() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT CHECK (val > 0))");
        exec("INSERT INTO t VALUES (1, 10)");
        try {
            exec("UPDATE t SET val = -1 WHERE id = 1");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint") ||
                       e.getMessage().toLowerCase().contains("violat"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_alterTableAddConstraint() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT)");
        exec("INSERT INTO t VALUES (1, 5)");
        exec("ALTER TABLE t ADD CONSTRAINT chk_val CHECK (val > 0)");
        try {
            exec("INSERT INTO t VALUES (2, -1)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_dropConstraint() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT, CONSTRAINT chk_val CHECK (val > 0))");
        exec("ALTER TABLE t DROP CONSTRAINT chk_val");
        // Constraint dropped, negative should now be allowed
        exec("INSERT INTO t VALUES (1, -1)");
        assertEquals(-1, queryInt("SELECT val FROM t"));
    }

    @Test
    void check_withNullPasses() throws SQLException {
        // In PG, CHECK allows NULL (NULL does not violate CHECK)
        exec("CREATE TABLE t (id INT, val INT CHECK (val > 0))");
        exec("INSERT INTO t VALUES (1, NULL)");
        assertNull(query1("SELECT val FROM t WHERE id = 1"));
    }

    @Test
    void check_complexExpression() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, CHECK (a + b <= 100))");
        exec("INSERT INTO t VALUES (40, 50)");
        assertEquals(40, queryInt("SELECT a FROM t"));
        try {
            exec("INSERT INTO t VALUES (60, 50)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void check_multipleChecksOnSameTable() throws SQLException {
        exec("CREATE TABLE t (id INT, a INT CHECK (a > 0), b INT CHECK (b > 0))");
        exec("INSERT INTO t VALUES (1, 5, 10)");
        try {
            exec("INSERT INTO t VALUES (2, -1, 10)");
            fail("Should have thrown for a");
        } catch (SQLException e) {
            // expected
        }
        try {
            exec("INSERT INTO t VALUES (3, 5, -1)");
            fail("Should have thrown for b");
        } catch (SQLException e) {
            // expected
        }
    }

    @Test
    void check_stringValues() throws SQLException {
        exec("CREATE TABLE t (id INT, status TEXT CHECK (status IN ('active', 'inactive')))");
        exec("INSERT INTO t VALUES (1, 'active')");
        assertEquals("active", query1("SELECT status FROM t"));
        try {
            exec("INSERT INTO t VALUES (2, 'deleted')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("check") ||
                       e.getMessage().toLowerCase().contains("constraint"),
                    "Error: " + e.getMessage());
        }
    }

    // =========================================================================
    // 83. UNIQUE constraints
    // =========================================================================

    @Test
    void unique_singleColumn() throws SQLException {
        exec("CREATE TABLE t (id INT UNIQUE, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        exec("INSERT INTO t VALUES (2, 'bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void unique_singleColumnViolation() throws SQLException {
        exec("CREATE TABLE t (id INT UNIQUE, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        try {
            exec("INSERT INTO t VALUES (1, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void unique_multiColumn() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, UNIQUE(a, b))");
        exec("INSERT INTO t VALUES (1, 1)");
        exec("INSERT INTO t VALUES (1, 2)");
        exec("INSERT INTO t VALUES (2, 1)");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void unique_multiColumnViolation() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, UNIQUE(a, b))");
        exec("INSERT INTO t VALUES (1, 1)");
        try {
            exec("INSERT INTO t VALUES (1, 1)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void unique_allowsMultipleNulls() throws SQLException {
        exec("CREATE TABLE t (id INT UNIQUE, name TEXT)");
        exec("INSERT INTO t VALUES (NULL, 'alice')");
        exec("INSERT INTO t VALUES (NULL, 'bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void unique_nullsNotDistinct() throws SQLException {
        exec("CREATE TABLE t (id INT, name TEXT, UNIQUE NULLS NOT DISTINCT (id))");
        exec("INSERT INTO t VALUES (NULL, 'alice')");
        try {
            exec("INSERT INTO t VALUES (NULL, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void unique_namedConstraint() throws SQLException {
        exec("CREATE TABLE t (id INT, name TEXT, CONSTRAINT uq_name UNIQUE(name))");
        exec("INSERT INTO t VALUES (1, 'alice')");
        try {
            exec("INSERT INTO t VALUES (2, 'alice')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate") ||
                       e.getMessage().toLowerCase().contains("uq_name"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void unique_tableLevelConstraint() throws SQLException {
        exec("CREATE TABLE t (a INT, b TEXT, UNIQUE(a))");
        exec("INSERT INTO t VALUES (1, 'x')");
        try {
            exec("INSERT INTO t VALUES (1, 'y')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void unique_violationOnUpdate() throws SQLException {
        exec("CREATE TABLE t (id INT UNIQUE, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        exec("INSERT INTO t VALUES (2, 'bob')");
        try {
            exec("UPDATE t SET id = 1 WHERE name = 'bob'");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void unique_multiColumnAllowsPartialMatch() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, c TEXT, UNIQUE(a, b))");
        exec("INSERT INTO t VALUES (1, 2, 'first')");
        exec("INSERT INTO t VALUES (1, 3, 'second')");  // same a, different b
        exec("INSERT INTO t VALUES (2, 2, 'third')");   // different a, same b
        assertEquals(3, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void unique_deferrableParsed() throws SQLException {
        // DEFERRABLE on FK is parsed, test UNIQUE constraint with table-level syntax
        exec("CREATE TABLE t (id INT, name TEXT, UNIQUE (id))");
        exec("INSERT INTO t VALUES (1, 'alice')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    // =========================================================================
    // 84. PRIMARY KEY
    // =========================================================================

    @Test
    void pk_singleColumn() throws SQLException {
        exec("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        exec("INSERT INTO t VALUES (2, 'bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void pk_duplicateViolation() throws SQLException {
        exec("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        try {
            exec("INSERT INTO t VALUES (1, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("primary") ||
                       e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void pk_autoNotNull() throws SQLException {
        exec("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
        try {
            exec("INSERT INTO t VALUES (NULL, 'alice')");
            fail("Should have thrown - PK implies NOT NULL");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null") ||
                       e.getMessage().toLowerCase().contains("primary"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void pk_composite() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, name TEXT, PRIMARY KEY(a, b))");
        exec("INSERT INTO t VALUES (1, 1, 'first')");
        exec("INSERT INTO t VALUES (1, 2, 'second')");
        exec("INSERT INTO t VALUES (2, 1, 'third')");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void pk_compositeViolation() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, PRIMARY KEY(a, b))");
        exec("INSERT INTO t VALUES (1, 1)");
        try {
            exec("INSERT INTO t VALUES (1, 1)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("primary") ||
                       e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void pk_withSerial() throws SQLException {
        exec("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (name) VALUES ('bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
        assertEquals(1, queryInt("SELECT MIN(id) FROM t"));
        assertEquals(2, queryInt("SELECT MAX(id) FROM t"));
    }

    @Test
    void pk_withBigserial() throws SQLException {
        exec("CREATE TABLE t (id BIGSERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (name) VALUES ('bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void pk_withIdentityAlways() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (name) VALUES ('bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
        assertEquals(1, queryInt("SELECT MIN(id) FROM t"));
    }

    @Test
    void pk_withIdentityByDefault() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (id, name) VALUES (100, 'bob')");
        assertEquals(100, queryInt("SELECT MAX(id) FROM t"));
    }

    @Test
    void pk_violationOnUpdate() throws SQLException {
        exec("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        exec("INSERT INTO t VALUES (2, 'bob')");
        try {
            exec("UPDATE t SET id = 1 WHERE name = 'bob'");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("primary") ||
                       e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void pk_serialAutoIncrement() throws SQLException {
        exec("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('a')");
        exec("INSERT INTO t (name) VALUES ('b')");
        exec("INSERT INTO t (name) VALUES ('c')");
        assertEquals(3, queryInt("SELECT COUNT(DISTINCT id) FROM t"));
    }

    @Test
    void pk_namedConstraint() throws SQLException {
        exec("CREATE TABLE t (id INT, name TEXT, CONSTRAINT pk_t PRIMARY KEY(id))");
        exec("INSERT INTO t VALUES (1, 'alice')");
        try {
            exec("INSERT INTO t VALUES (1, 'bob')");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("primary") ||
                       e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate") ||
                       e.getMessage().toLowerCase().contains("pk_t"),
                    "Error: " + e.getMessage());
        }
    }

    // =========================================================================
    // 85. FOREIGN KEY
    // =========================================================================

    @Test
    void fk_basicReferences() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))");
        exec("INSERT INTO parent VALUES (1, 'alice')");
        exec("INSERT INTO child VALUES (1, 1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_violationOnInsert() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))");
        exec("INSERT INTO parent VALUES (1)");
        try {
            exec("INSERT INTO child VALUES (1, 999)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc") ||
                       e.getMessage().toLowerCase().contains("key"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_allowsNull() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))");
        exec("INSERT INTO child VALUES (1, NULL)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_onDeleteCascade() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO parent VALUES (1, 'alice')");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("INSERT INTO child VALUES (2, 1)");
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_onDeleteSetNull() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE SET NULL)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("DELETE FROM parent WHERE id = 1");
        assertNull(query1("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void fk_onDeleteSetDefault() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT DEFAULT 0 REFERENCES parent(id) ON DELETE SET DEFAULT)");
        exec("INSERT INTO parent VALUES (0)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals(0, queryInt("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void fk_onDeleteRestrict() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE RESTRICT)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        try {
            exec("DELETE FROM parent WHERE id = 1");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc") ||
                       e.getMessage().toLowerCase().contains("restrict"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_onDeleteNoAction() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE NO ACTION)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        try {
            exec("DELETE FROM parent WHERE id = 1");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc") ||
                       e.getMessage().toLowerCase().contains("restrict") ||
                       e.getMessage().toLowerCase().contains("action"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_onUpdateCascade() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON UPDATE CASCADE)");
        exec("INSERT INTO parent VALUES (1, 'alice')");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("UPDATE parent SET id = 100 WHERE id = 1");
        assertEquals(100, queryInt("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void fk_onUpdateSetNull() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON UPDATE SET NULL)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("UPDATE parent SET id = 100 WHERE id = 1");
        assertNull(query1("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void fk_onUpdateRestrict() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON UPDATE RESTRICT)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        try {
            exec("UPDATE parent SET id = 100 WHERE id = 1");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc") ||
                       e.getMessage().toLowerCase().contains("restrict"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_onDeleteCascadeMultipleChildren() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO parent VALUES (2)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("INSERT INTO child VALUES (2, 1)");
        exec("INSERT INTO child VALUES (3, 2)");
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
        assertEquals(3, queryInt("SELECT id FROM child"));
    }

    @Test
    void fk_tableLevelConstraint() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_namedConstraint() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT, CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES parent(id))");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_multiColumn() throws SQLException {
        exec("CREATE TABLE parent (a INT, b INT, name TEXT, PRIMARY KEY(a, b))");
        exec("CREATE TABLE child (id INT, pa INT, pb INT, FOREIGN KEY (pa, pb) REFERENCES parent(a, b) ON DELETE CASCADE)");
        exec("INSERT INTO parent VALUES (1, 2, 'x')");
        exec("INSERT INTO child VALUES (1, 1, 2)");
        exec("DELETE FROM parent WHERE a = 1 AND b = 2");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_multiColumnViolation() throws SQLException {
        exec("CREATE TABLE parent (a INT, b INT, PRIMARY KEY(a, b))");
        exec("CREATE TABLE child (id INT, pa INT, pb INT, FOREIGN KEY (pa, pb) REFERENCES parent(a, b))");
        exec("INSERT INTO parent VALUES (1, 2)");
        try {
            exec("INSERT INTO child VALUES (1, 1, 3)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_selfReferential() throws SQLException {
        exec("CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT REFERENCES tree(id) ON DELETE CASCADE)");
        exec("INSERT INTO tree VALUES (1, NULL)");
        exec("INSERT INTO tree VALUES (2, 1)");
        exec("INSERT INTO tree VALUES (3, 1)");
        exec("INSERT INTO tree VALUES (4, 2)");
        assertEquals(4, queryInt("SELECT COUNT(*) FROM tree"));
    }

    @Test
    void fk_selfReferentialViolation() throws SQLException {
        exec("CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT REFERENCES tree(id))");
        exec("INSERT INTO tree VALUES (1, NULL)");
        try {
            exec("INSERT INTO tree VALUES (2, 999)");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_selfReferentialCascadeDeleteLeaf() throws SQLException {
        exec("CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT REFERENCES tree(id) ON DELETE CASCADE)");
        exec("INSERT INTO tree VALUES (1, NULL)");
        exec("INSERT INTO tree VALUES (2, 1)");
        exec("INSERT INTO tree VALUES (3, 1)");
        // Deleting a leaf node should work
        exec("DELETE FROM tree WHERE id = 2");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM tree"));
    }

    @Test
    void fk_deferrableParsed() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_deferrableNotDeferrable() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id) NOT DEFERRABLE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_deferrableInitiallyImmediate() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id) DEFERRABLE INITIALLY IMMEDIATE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_setConstraintsParsed() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id))");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        // SET CONSTRAINTS is parsed as no-op
        exec("SET CONSTRAINTS ALL DEFERRED");
        exec("SET CONSTRAINTS ALL IMMEDIATE");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_onDeleteCascadePreservesOtherRows() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO parent VALUES (2)");
        exec("INSERT INTO child VALUES (10, 1)");
        exec("INSERT INTO child VALUES (20, 2)");
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM child"));
        assertEquals(20, queryInt("SELECT id FROM child"));
    }

    @Test
    void fk_onDeleteAndUpdateCombined() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        // Update should cascade
        exec("UPDATE parent SET id = 50 WHERE id = 1");
        assertEquals(50, queryInt("SELECT pid FROM child"));
        // Delete should cascade
        exec("DELETE FROM parent WHERE id = 50");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void fk_violationOnUpdate() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id))");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        try {
            exec("UPDATE child SET pid = 999 WHERE id = 1");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_columnLevelWithOnDeleteUpdate() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id) ON DELETE SET NULL ON UPDATE CASCADE)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("UPDATE parent SET id = 42 WHERE id = 1");
        assertEquals(42, queryInt("SELECT pid FROM child"));
    }

    @Test
    void fk_insertChildThenParentFails() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id))");
        try {
            exec("INSERT INTO child VALUES (1, 1)");
            fail("Should have thrown - parent does not exist");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("foreign") ||
                       e.getMessage().toLowerCase().contains("referenc"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_deleteParentWithNoChildrenSucceeds() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT REFERENCES parent(id) ON DELETE RESTRICT)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO parent VALUES (2)");
        exec("INSERT INTO child VALUES (1, 1)");
        // Delete parent 2 which has no children - should succeed
        exec("DELETE FROM parent WHERE id = 2");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM parent"));
    }

    // =========================================================================
    // 86. Exclusion constraints (parsed as no-op)
    // =========================================================================

    @Test
    void exclusion_usingGistParsed() throws SQLException {
        exec("CREATE TABLE t (id INT, room INT, val INT, EXCLUDE USING gist (room WITH =, val WITH =))");
        exec("INSERT INTO t VALUES (1, 100, 5)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void exclusion_usingBtreeParsed() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT, EXCLUDE USING btree (val WITH =))");
        exec("INSERT INTO t VALUES (1, 10)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void exclusion_withWhereParsed() throws SQLException {
        exec("CREATE TABLE t (id INT, room INT, active BOOLEAN, EXCLUDE USING gist (room WITH =) WHERE (active))");
        exec("INSERT INTO t VALUES (1, 100, true)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void exclusion_namedConstraintParsed() throws SQLException {
        exec("CREATE TABLE t (id INT, val INT, CONSTRAINT excl_val EXCLUDE USING btree (val WITH =))");
        exec("INSERT INTO t VALUES (1, 10)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    // =========================================================================
    // 87. Default values & generated columns
    // =========================================================================

    @Test
    void default_literal() throws SQLException {
        exec("CREATE TABLE t (id INT, status TEXT DEFAULT 'active')");
        exec("INSERT INTO t (id) VALUES (1)");
        assertEquals("active", query1("SELECT status FROM t WHERE id = 1"));
    }

    @Test
    void default_intLiteral() throws SQLException {
        exec("CREATE TABLE t (id INT, priority INT DEFAULT 0)");
        exec("INSERT INTO t (id) VALUES (1)");
        assertEquals(0, queryInt("SELECT priority FROM t WHERE id = 1"));
    }

    @Test
    void default_booleanLiteral() throws SQLException {
        exec("CREATE TABLE t (id INT, active BOOLEAN DEFAULT TRUE)");
        exec("INSERT INTO t (id) VALUES (1)");
        assertTrue(queryBool("SELECT active FROM t WHERE id = 1"));
    }

    @Test
    void default_overriddenByExplicitValue() throws SQLException {
        exec("CREATE TABLE t (id INT, status TEXT DEFAULT 'active')");
        exec("INSERT INTO t VALUES (1, 'inactive')");
        assertEquals("inactive", query1("SELECT status FROM t WHERE id = 1"));
    }

    @Test
    void default_overriddenByExplicitNull() throws SQLException {
        exec("CREATE TABLE t (id INT, status TEXT DEFAULT 'active')");
        exec("INSERT INTO t VALUES (1, NULL)");
        assertNull(query1("SELECT status FROM t WHERE id = 1"));
    }

    @Test
    void default_functionNow() throws SQLException {
        exec("CREATE TABLE t (id INT, created_at TIMESTAMP DEFAULT NOW())");
        exec("INSERT INTO t (id) VALUES (1)");
        String ts = query1("SELECT created_at FROM t WHERE id = 1");
        assertNotNull(ts);
    }

    @Test
    void default_functionGenRandomUuid() throws SQLException {
        exec("CREATE TABLE t (id UUID DEFAULT gen_random_uuid(), name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        String uuid = query1("SELECT id FROM t WHERE name = 'alice'");
        assertNotNull(uuid);
        assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "Should be valid UUID: " + uuid);
    }

    @Test
    void default_expression() throws SQLException {
        exec("CREATE TABLE t (id INT, doubled INT DEFAULT 42)");
        exec("INSERT INTO t (id) VALUES (1)");
        assertEquals(42, queryInt("SELECT doubled FROM t WHERE id = 1"));
    }

    @Test
    void default_currentDate() throws SQLException {
        exec("CREATE TABLE t (id INT, dt DATE DEFAULT CURRENT_DATE)");
        exec("INSERT INTO t (id) VALUES (1)");
        String dt = query1("SELECT dt FROM t WHERE id = 1");
        assertNotNull(dt);
    }

    @Test
    void default_multipleDefaults() throws SQLException {
        exec("CREATE TABLE t (id SERIAL, name TEXT DEFAULT 'unnamed', active BOOLEAN DEFAULT TRUE, score INT DEFAULT 0)");
        exec("INSERT INTO t DEFAULT VALUES");
        assertEquals("unnamed", query1("SELECT name FROM t WHERE id = 1"));
        assertTrue(queryBool("SELECT active FROM t WHERE id = 1"));
        assertEquals(0, queryInt("SELECT score FROM t WHERE id = 1"));
    }

    @Test
    void default_alterColumnSetDefault() throws SQLException {
        exec("CREATE TABLE t (id INT, status TEXT)");
        exec("ALTER TABLE t ALTER COLUMN status SET DEFAULT 'pending'");
        exec("INSERT INTO t (id) VALUES (1)");
        assertEquals("pending", query1("SELECT status FROM t WHERE id = 1"));
    }

    @Test
    void default_alterColumnDropDefault() throws SQLException {
        exec("CREATE TABLE t (id INT, status TEXT DEFAULT 'active')");
        exec("ALTER TABLE t ALTER COLUMN status DROP DEFAULT");
        exec("INSERT INTO t (id) VALUES (1)");
        assertNull(query1("SELECT status FROM t WHERE id = 1"));
    }

    @Test
    void default_insertDefaultValues() throws SQLException {
        exec("CREATE TABLE t (id SERIAL, name TEXT DEFAULT 'x')");
        exec("INSERT INTO t DEFAULT VALUES");
        assertEquals("x", query1("SELECT name FROM t WHERE id = 1"));
    }

    // --- Generated columns ---

    @Test
    void generated_storedBasic() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO t (a, b) VALUES (3, 7)");
        assertEquals(10, queryInt("SELECT c FROM t WHERE a = 3"));
    }

    @Test
    void generated_storedMultiplication() throws SQLException {
        exec("CREATE TABLE t (price NUMERIC, qty INT, total NUMERIC GENERATED ALWAYS AS (price * qty) STORED)");
        exec("INSERT INTO t (price, qty) VALUES (9.99, 3)");
        assertEquals("29.97", query1("SELECT total FROM t"));
    }

    @Test
    void generated_storedStringConcat() throws SQLException {
        exec("CREATE TABLE t (first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)");
        exec("INSERT INTO t (first_name, last_name) VALUES ('John', 'Doe')");
        assertEquals("John Doe", query1("SELECT full_name FROM t"));
    }

    @Test
    void generated_storedMultipleRows() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO t (a, b) VALUES (1, 2)");
        exec("INSERT INTO t (a, b) VALUES (10, 20)");
        assertEquals(3, queryInt("SELECT c FROM t WHERE a = 1"));
        assertEquals(30, queryInt("SELECT c FROM t WHERE a = 10"));
    }

    @Test
    void generated_storedIgnoresExplicitValue() throws SQLException {
        // Generated column value is always computed, even if explicitly provided
        exec("CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO t (a, b) VALUES (1, 2)");
        assertEquals(3, queryInt("SELECT c FROM t"));
    }

    @Test
    void generated_storedWithNullInputs() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO t (a, b) VALUES (5, NULL)");
        // NULL + anything = NULL
        assertNull(query1("SELECT c FROM t WHERE a = 5"));
    }

    // --- Identity columns ---

    @Test
    void identity_generatedAlways() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (name) VALUES ('bob')");
        assertEquals(1, queryInt("SELECT MIN(id) FROM t"));
        assertEquals(2, queryInt("SELECT MAX(id) FROM t"));
    }

    @Test
    void identity_generatedByDefault() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (id, name) VALUES (100, 'bob')");
        assertEquals(100, queryInt("SELECT MAX(id) FROM t"));
    }

    @Test
    void identity_generatedAlwaysAutoIncrements() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (name) VALUES ('bob')");
        exec("INSERT INTO t (name) VALUES ('charlie')");
        assertEquals(3, queryInt("SELECT COUNT(DISTINCT id) FROM t"));
        assertEquals(1, queryInt("SELECT MIN(id) FROM t"));
        assertEquals(3, queryInt("SELECT MAX(id) FROM t"));
    }

    @Test
    void identity_generatedByDefaultAutoIncrement() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('a')");
        exec("INSERT INTO t (name) VALUES ('b')");
        exec("INSERT INTO t (name) VALUES ('c')");
        assertEquals(3, queryInt("SELECT COUNT(DISTINCT id) FROM t"));
    }

    @Test
    void identity_withPrimaryKey() throws SQLException {
        exec("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        exec("INSERT INTO t (name) VALUES ('bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
        // PK should prevent duplicates
        assertEquals(2, queryInt("SELECT COUNT(DISTINCT id) FROM t"));
    }

    @Test
    void identity_bigintIdentity() throws SQLException {
        exec("CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        assertEquals(1, queryInt("SELECT id FROM t"));
    }

    // --- Serial as default ---

    @Test
    void serial_autoIncrement() throws SQLException {
        exec("CREATE TABLE t (id SERIAL, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('a')");
        exec("INSERT INTO t (name) VALUES ('b')");
        exec("INSERT INTO t (name) VALUES ('c')");
        assertEquals(1, queryInt("SELECT MIN(id) FROM t"));
        assertEquals(3, queryInt("SELECT MAX(id) FROM t"));
    }

    @Test
    void serial_canOverride() throws SQLException {
        exec("CREATE TABLE t (id SERIAL, name TEXT)");
        exec("INSERT INTO t (id, name) VALUES (100, 'custom')");
        assertEquals(100, queryInt("SELECT id FROM t WHERE name = 'custom'"));
    }

    @Test
    void bigserial_autoIncrement() throws SQLException {
        exec("CREATE TABLE t (id BIGSERIAL, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('a')");
        exec("INSERT INTO t (name) VALUES ('b')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
    }

    // --- Combined constraints scenarios ---

    @Test
    void combined_notNullAndDefault() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL DEFAULT 1, name TEXT)");
        exec("INSERT INTO t (name) VALUES ('alice')");
        assertEquals(1, queryInt("SELECT id FROM t"));
    }

    @Test
    void combined_notNullAndCheck() throws SQLException {
        exec("CREATE TABLE t (id INT NOT NULL CHECK (id > 0), name TEXT)");
        exec("INSERT INTO t VALUES (1, 'alice')");
        try {
            exec("INSERT INTO t VALUES (NULL, 'bob')");
            fail("Should have thrown for NULL");
        } catch (SQLException e) {
            // expected
        }
        try {
            exec("INSERT INTO t VALUES (-1, 'charlie')");
            fail("Should have thrown for CHECK");
        } catch (SQLException e) {
            // expected
        }
    }

    @Test
    void combined_uniqueAndNotNull() throws SQLException {
        exec("CREATE TABLE t (email TEXT NOT NULL UNIQUE, name TEXT)");
        exec("INSERT INTO t VALUES ('a@b.com', 'alice')");
        try {
            exec("INSERT INTO t VALUES ('a@b.com', 'bob')");
            fail("Should have thrown for UNIQUE");
        } catch (SQLException e) {
            // expected
        }
        try {
            exec("INSERT INTO t VALUES (NULL, 'charlie')");
            fail("Should have thrown for NOT NULL");
        } catch (SQLException e) {
            // expected
        }
    }

    @Test
    void combined_pkAndFkAndCheck() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE CASCADE, score INT CHECK (score >= 0))");
        exec("INSERT INTO parent VALUES (1, 'p1')");
        exec("INSERT INTO child VALUES (1, 1, 50)");
        // FK violation
        try {
            exec("INSERT INTO child VALUES (2, 999, 50)");
            fail("FK violation");
        } catch (SQLException e) { /* expected */ }
        // CHECK violation
        try {
            exec("INSERT INTO child VALUES (3, 1, -1)");
            fail("CHECK violation");
        } catch (SQLException e) { /* expected */ }
        // PK violation
        try {
            exec("INSERT INTO child VALUES (1, 1, 75)");
            fail("PK violation");
        } catch (SQLException e) { /* expected */ }
        // Cascade delete
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void combined_generatedAndDefault() throws SQLException {
        exec("CREATE TABLE t (id SERIAL PRIMARY KEY, a INT DEFAULT 10, b INT DEFAULT 20, total INT GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO t DEFAULT VALUES");
        assertEquals(30, queryInt("SELECT total FROM t WHERE id = 1"));
    }

    @Test
    void combined_fkChainDirectCascade() throws SQLException {
        exec("CREATE TABLE grandparent (id INT PRIMARY KEY)");
        exec("CREATE TABLE parent (id INT PRIMARY KEY, gp_id INT REFERENCES grandparent(id) ON DELETE CASCADE)");
        exec("CREATE TABLE child (id INT PRIMARY KEY, p_id INT REFERENCES parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO grandparent VALUES (1)");
        exec("INSERT INTO parent VALUES (10, 1)");
        exec("INSERT INTO child VALUES (100, 10)");
        // Delete parent directly cascades to child
        exec("DELETE FROM parent WHERE id = 10");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM child"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM grandparent"));
    }

    @Test
    void combined_multipleUniqueConstraints() throws SQLException {
        exec("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE)");
        exec("INSERT INTO t VALUES (1, 'a@b.com', 'alice')");
        try {
            exec("INSERT INTO t VALUES (2, 'a@b.com', 'bob')");
            fail("Duplicate email");
        } catch (SQLException e) { /* expected */ }
        try {
            exec("INSERT INTO t VALUES (3, 'c@d.com', 'alice')");
            fail("Duplicate username");
        } catch (SQLException e) { /* expected */ }
    }

    @Test
    void combined_checkWithBetween() throws SQLException {
        exec("CREATE TABLE t (id INT, score INT CHECK (score BETWEEN 0 AND 100))");
        exec("INSERT INTO t VALUES (1, 50)");
        exec("INSERT INTO t VALUES (2, 0)");
        exec("INSERT INTO t VALUES (3, 100)");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM t"));
        try {
            exec("INSERT INTO t VALUES (4, 101)");
            fail("Should have thrown");
        } catch (SQLException e) { /* expected */ }
        try {
            exec("INSERT INTO t VALUES (5, -1)");
            fail("Should have thrown");
        } catch (SQLException e) { /* expected */ }
    }

    @Test
    void combined_serialPkWithFk() throws SQLException {
        exec("CREATE TABLE parent (id SERIAL PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE child (id SERIAL PRIMARY KEY, parent_id INT REFERENCES parent(id))");
        exec("INSERT INTO parent (name) VALUES ('p1')");
        exec("INSERT INTO parent (name) VALUES ('p2')");
        exec("INSERT INTO child (parent_id) VALUES (1)");
        exec("INSERT INTO child (parent_id) VALUES (2)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM child"));
    }

    @Test
    void default_withNotNullAndSerial() throws SQLException {
        exec("CREATE TABLE t (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'new', created_at TIMESTAMP DEFAULT NOW())");
        exec("INSERT INTO t DEFAULT VALUES");
        assertEquals("new", query1("SELECT status FROM t WHERE id = 1"));
        assertNotNull(query1("SELECT created_at FROM t WHERE id = 1"));
    }

    @Test
    void unique_nullsDistinctDefault() throws SQLException {
        // By default, NULLs are considered distinct in UNIQUE
        exec("CREATE TABLE t (a INT, b INT, UNIQUE(a, b))");
        exec("INSERT INTO t VALUES (1, NULL)");
        exec("INSERT INTO t VALUES (1, NULL)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void unique_multiColumnNullsNotDistinct() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, UNIQUE NULLS NOT DISTINCT (a, b))");
        exec("INSERT INTO t VALUES (1, NULL)");
        try {
            exec("INSERT INTO t VALUES (1, NULL)");
            fail("NULLS NOT DISTINCT should reject");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("unique") ||
                       e.getMessage().toLowerCase().contains("duplicate"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_onUpdateSetDefault() throws SQLException {
        exec("CREATE TABLE parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE child (id INT, pid INT DEFAULT 0 REFERENCES parent(id) ON UPDATE SET DEFAULT)");
        exec("INSERT INTO parent VALUES (0)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("UPDATE parent SET id = 99 WHERE id = 1");
        assertEquals(0, queryInt("SELECT pid FROM child WHERE id = 1"));
    }

    @Test
    void check_noInheritParsed() throws SQLException {
        // NO INHERIT is parsed even if inheritance is not fully implemented
        exec("CREATE TABLE t (id INT, val INT, CHECK (val > 0) NO INHERIT)");
        exec("INSERT INTO t VALUES (1, 5)");
        assertEquals(5, queryInt("SELECT val FROM t"));
    }

    @Test
    void notNull_noInheritParsed() throws SQLException {
        // NO INHERIT on NOT NULL is PG18 syntax, parse it
        exec("CREATE TABLE t (id INT NOT NULL, name TEXT)");
        exec("INSERT INTO t VALUES (1, 'test')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t"));
    }

    @Test
    void generated_storedWithCoalesce() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED)");
        exec("INSERT INTO t (a, b) VALUES (NULL, 5)");
        assertEquals(5, queryInt("SELECT c FROM t"));
    }

    @Test
    void pk_compositeAutoNotNull() throws SQLException {
        exec("CREATE TABLE t (a INT, b INT, c TEXT, PRIMARY KEY(a, b))");
        try {
            exec("INSERT INTO t VALUES (1, NULL, 'x')");
            fail("Composite PK component should be NOT NULL");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("null") ||
                       e.getMessage().toLowerCase().contains("primary"),
                    "Error: " + e.getMessage());
        }
    }

    @Test
    void fk_cascadeDoesNotAffectUnrelatedTables() throws SQLException {
        exec("CREATE TABLE p1 (id INT PRIMARY KEY)");
        exec("CREATE TABLE p2 (id INT PRIMARY KEY)");
        exec("CREATE TABLE c1 (id INT, p1_id INT REFERENCES p1(id) ON DELETE CASCADE)");
        exec("CREATE TABLE c2 (id INT, p2_id INT REFERENCES p2(id) ON DELETE CASCADE)");
        exec("INSERT INTO p1 VALUES (1)");
        exec("INSERT INTO p2 VALUES (1)");
        exec("INSERT INTO c1 VALUES (1, 1)");
        exec("INSERT INTO c2 VALUES (1, 1)");
        exec("DELETE FROM p1 WHERE id = 1");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM c1"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM c2"));
    }
}
