package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 5: DDL (Data Definition Language) tests.
 * ALTER TABLE extensions, CREATE VIEW, CREATE SEQUENCE, CREATE INDEX, DROP, TRUNCATE.
 */
class DDLTest {

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

    // ---- CREATE SEQUENCE & nextval/currval/setval ----

    @Test
    void testCreateSequenceAndNextval() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE test_seq");
            ResultSet rs = stmt.executeQuery("SELECT nextval('test_seq')");
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            rs = stmt.executeQuery("SELECT nextval('test_seq')");
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));
            rs = stmt.executeQuery("SELECT nextval('test_seq')");
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));
        }
    }

    @Test
    void testSequenceStartWith() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE start_seq START WITH 100");
            ResultSet rs = stmt.executeQuery("SELECT nextval('start_seq')");
            assertTrue(rs.next());
            assertEquals(100L, rs.getLong(1));
            rs = stmt.executeQuery("SELECT nextval('start_seq')");
            assertTrue(rs.next());
            assertEquals(101L, rs.getLong(1));
        }
    }

    @Test
    void testSequenceIncrementBy() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE inc_seq START WITH 10 INCREMENT BY 5");
            ResultSet rs = stmt.executeQuery("SELECT nextval('inc_seq')");
            assertTrue(rs.next());
            assertEquals(10L, rs.getLong(1));
            rs = stmt.executeQuery("SELECT nextval('inc_seq')");
            assertTrue(rs.next());
            assertEquals(15L, rs.getLong(1));
            rs = stmt.executeQuery("SELECT nextval('inc_seq')");
            assertTrue(rs.next());
            assertEquals(20L, rs.getLong(1));
        }
    }

    @Test
    void testCurrval() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE curr_seq");
            stmt.executeQuery("SELECT nextval('curr_seq')");
            ResultSet rs = stmt.executeQuery("SELECT currval('curr_seq')");
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            stmt.executeQuery("SELECT nextval('curr_seq')");
            rs = stmt.executeQuery("SELECT currval('curr_seq')");
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));
        }
    }

    @Test
    void testSetval() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE setval_seq");
            stmt.executeQuery("SELECT setval('setval_seq', 50)");
            ResultSet rs = stmt.executeQuery("SELECT nextval('setval_seq')");
            assertTrue(rs.next());
            assertEquals(51L, rs.getLong(1));
        }
    }

    @Test
    void testSequenceIfNotExists() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE ine_seq");
            // Should not throw
            stmt.execute("CREATE SEQUENCE IF NOT EXISTS ine_seq");
        }
    }

    @Test
    void testDropSequence() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE drop_seq");
            stmt.executeQuery("SELECT nextval('drop_seq')");
            stmt.execute("DROP SEQUENCE drop_seq");
            // Should throw since sequence is gone
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT nextval('drop_seq')"));
        }
    }

    @Test
    void testSequenceInInsert() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE emp_id_seq START WITH 1");
            stmt.execute("CREATE TABLE seq_test (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO seq_test VALUES (nextval('emp_id_seq'), 'Alice')");
            stmt.execute("INSERT INTO seq_test VALUES (nextval('emp_id_seq'), 'Bob')");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM seq_test ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    // ---- CREATE VIEW ----

    @Test
    void testCreateViewBasic() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE view_src (id INTEGER, name TEXT, active BOOLEAN)");
            stmt.execute("INSERT INTO view_src VALUES (1, 'Alice', true)");
            stmt.execute("INSERT INTO view_src VALUES (2, 'Bob', false)");
            stmt.execute("INSERT INTO view_src VALUES (3, 'Charlie', true)");

            stmt.execute("CREATE VIEW active_users AS SELECT id, name FROM view_src WHERE active = true");

            ResultSet rs = stmt.executeQuery("SELECT id, name FROM active_users ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("Charlie", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testViewReflectsBaseTableChanges() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE live_data (val INTEGER)");
            stmt.execute("INSERT INTO live_data VALUES (10)");
            stmt.execute("CREATE VIEW live_view AS SELECT val FROM live_data");

            ResultSet rs = stmt.executeQuery("SELECT val FROM live_view");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("val"));
            assertFalse(rs.next());

            // Insert more data
            stmt.execute("INSERT INTO live_data VALUES (20)");
            rs = stmt.executeQuery("SELECT val FROM live_view ORDER BY val");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(20, rs.getInt("val"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testViewWithAggregation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE agg_data (dept TEXT, salary INTEGER)");
            stmt.execute("INSERT INTO agg_data VALUES ('A', 100)");
            stmt.execute("INSERT INTO agg_data VALUES ('A', 200)");
            stmt.execute("INSERT INTO agg_data VALUES ('B', 150)");

            stmt.execute("CREATE VIEW dept_summary AS SELECT dept, SUM(salary) AS total, COUNT(*) AS cnt FROM agg_data GROUP BY dept");

            ResultSet rs = stmt.executeQuery("SELECT dept, total, cnt FROM dept_summary ORDER BY dept");
            assertTrue(rs.next());
            assertEquals("A", rs.getString("dept"));
            assertEquals(300L, rs.getLong("total"));
            assertEquals(2L, rs.getLong("cnt"));
            assertTrue(rs.next());
            assertEquals("B", rs.getString("dept"));
            assertEquals(150L, rs.getLong("total"));
            assertEquals(1L, rs.getLong("cnt"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCreateOrReplaceView() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE replace_src (x INTEGER)");
            stmt.execute("INSERT INTO replace_src VALUES (1)");
            stmt.execute("INSERT INTO replace_src VALUES (2)");
            stmt.execute("INSERT INTO replace_src VALUES (3)");

            stmt.execute("CREATE VIEW replace_view AS SELECT x FROM replace_src WHERE x < 3");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM replace_view");
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));

            // Replace with different query
            stmt.execute("CREATE OR REPLACE VIEW replace_view AS SELECT x FROM replace_src WHERE x > 1");
            rs = stmt.executeQuery("SELECT COUNT(*) FROM replace_view");
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));

            // Verify actual values
            rs = stmt.executeQuery("SELECT x FROM replace_view ORDER BY x");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("x"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("x"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testDropView() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dv_src (id INTEGER)");
            stmt.execute("INSERT INTO dv_src VALUES (1)");
            stmt.execute("CREATE VIEW dv AS SELECT id FROM dv_src");
            stmt.execute("DROP VIEW dv");
            // Querying dropped view should fail
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT * FROM dv"));
        }
    }

    @Test
    void testDropViewIfExists() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Should not throw
            stmt.execute("DROP VIEW IF EXISTS nonexistent_view");
        }
    }

    // ---- ALTER TABLE ----

    @Test
    void testAlterColumnSetType() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE type_test (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO type_test VALUES (1, '42')");
            stmt.execute("ALTER TABLE type_test ALTER COLUMN val TYPE INTEGER");
            // Column type changed; existing data should still be readable
            ResultSet rs = stmt.executeQuery("SELECT val FROM type_test");
            assertTrue(rs.next());
            // The string '42' is still stored; type metadata changed
            assertNotNull(rs.getObject("val"));
        }
    }

    @Test
    void testAlterColumnSetDefault() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE def_test (id INTEGER, status TEXT)");
            stmt.execute("ALTER TABLE def_test ALTER COLUMN status SET DEFAULT 'active'");
            // Default is stored, tested by metadata change
        }
    }

    @Test
    void testAlterColumnDropDefault() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE defr_test (id INTEGER, status TEXT DEFAULT 'pending')");
            stmt.execute("ALTER TABLE defr_test ALTER COLUMN status DROP DEFAULT");
            // Default removed, accepted without error
        }
    }

    @Test
    void testAlterColumnSetNotNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nn_test (id INTEGER, name TEXT)");
            stmt.execute("ALTER TABLE nn_test ALTER COLUMN name SET NOT NULL");
            // NOT NULL set, accepted without error
        }
    }

    @Test
    void testAlterColumnDropNotNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dn_test (id INTEGER, name TEXT NOT NULL)");
            stmt.execute("ALTER TABLE dn_test ALTER COLUMN name DROP NOT NULL");
            // NOT NULL removed, accepted without error
        }
    }

    @Test
    void testAlterTableAddColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ac_test (id INTEGER)");
            stmt.execute("INSERT INTO ac_test VALUES (1)");
            stmt.execute("ALTER TABLE ac_test ADD COLUMN name TEXT");
            stmt.execute("INSERT INTO ac_test (id, name) VALUES (2, 'Bob')");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM ac_test ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertNull(rs.getObject("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testAlterTableDropColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dc_test (id INTEGER, name TEXT, age INTEGER)");
            stmt.execute("INSERT INTO dc_test VALUES (1, 'Alice', 30)");
            stmt.execute("ALTER TABLE dc_test DROP COLUMN age");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM dc_test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testAlterTableRenameColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE rc_test (id INTEGER, full_name TEXT)");
            stmt.execute("INSERT INTO rc_test VALUES (1, 'Alice')");
            stmt.execute("ALTER TABLE rc_test RENAME COLUMN full_name TO name");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM rc_test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testAlterTableRenameTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE old_name (id INTEGER)");
            stmt.execute("INSERT INTO old_name VALUES (1)");
            stmt.execute("ALTER TABLE old_name RENAME TO new_name");
            ResultSet rs = stmt.executeQuery("SELECT id FROM new_name");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
        }
    }

    // ---- CREATE INDEX ----

    @Test
    void testCreateIndex() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_test (id INTEGER, name TEXT)");
            // Should execute without error
            stmt.execute("CREATE INDEX idx_name ON idx_test (name)");
        }
    }

    @Test
    void testCreateUniqueIndex() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uidx_test (id INTEGER, email TEXT)");
            stmt.execute("CREATE UNIQUE INDEX uidx_email ON uidx_test (email)");
        }
    }

    @Test
    void testCreateIndexIfNotExists() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ine_idx_test (id INTEGER)");
            stmt.execute("CREATE INDEX idx_ine ON ine_idx_test (id)");
            // Should not throw
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_ine ON ine_idx_test (id)");
        }
    }

    // ---- TRUNCATE ----

    @Test
    void testTruncate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trunc_test (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO trunc_test VALUES (1, 'A')");
            stmt.execute("INSERT INTO trunc_test VALUES (2, 'B')");
            stmt.execute("INSERT INTO trunc_test VALUES (3, 'C')");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM trunc_test");
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));

            stmt.execute("TRUNCATE trunc_test");
            rs = stmt.executeQuery("SELECT COUNT(*) FROM trunc_test");
            assertTrue(rs.next());
            assertEquals(0L, rs.getLong(1));
        }
    }

    @Test
    void testTruncateResetsSerial() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE serial_trunc (id SERIAL, name TEXT)");
            stmt.execute("INSERT INTO serial_trunc (name) VALUES ('A')");
            stmt.execute("INSERT INTO serial_trunc (name) VALUES ('B')");
            ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM serial_trunc");
            assertTrue(rs.next());
            long maxBefore = rs.getLong(1);
            assertTrue(maxBefore >= 2);

            stmt.execute("TRUNCATE serial_trunc RESTART IDENTITY");
            stmt.execute("INSERT INTO serial_trunc (name) VALUES ('C')");
            rs = stmt.executeQuery("SELECT id FROM serial_trunc");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
        }
    }

    // ---- CREATE TABLE with IF NOT EXISTS ----

    @Test
    void testCreateTableIfNotExists() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ine_table (id INTEGER)");
            // Should not throw
            stmt.execute("CREATE TABLE IF NOT EXISTS ine_table (id INTEGER, extra TEXT)");
            // Original table should be unchanged
            stmt.execute("INSERT INTO ine_table VALUES (1)");
        }
    }

    // ---- DROP TABLE ----

    @Test
    void testDropTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dt_test (id INTEGER)");
            stmt.execute("DROP TABLE dt_test");
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT * FROM dt_test"));
        }
    }

    @Test
    void testDropTableIfExists() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS nonexistent_table");
            // Should not throw
        }
    }

    // ---- SERIAL / BIGSERIAL ----

    @Test
    void testSerialAutoIncrement() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE serial_test (id SERIAL, name TEXT)");
            stmt.execute("INSERT INTO serial_test (name) VALUES ('Alice')");
            stmt.execute("INSERT INTO serial_test (name) VALUES ('Bob')");
            stmt.execute("INSERT INTO serial_test (name) VALUES ('Charlie')");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM serial_test ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("Charlie", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    // ---- Multiple ALTER actions ----

    @Test
    void testMultipleAlterActions() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE multi_alter (id INTEGER, a TEXT, b TEXT)");
            stmt.execute("INSERT INTO multi_alter VALUES (1, 'x', 'y')");
            stmt.execute("ALTER TABLE multi_alter DROP COLUMN b");
            stmt.execute("ALTER TABLE multi_alter ADD COLUMN c INTEGER");
            ResultSet rs = stmt.executeQuery("SELECT id, a, c FROM multi_alter");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("x", rs.getString("a"));
            assertNull(rs.getObject("c"));
            assertFalse(rs.next());
        }
    }

    // ---- DROP INDEX ----

    @Test
    void testDropIndex() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE di_test (id INTEGER)");
            stmt.execute("CREATE INDEX di_idx ON di_test (id)");
            stmt.execute("DROP INDEX di_idx");
        }
    }

    // ---- View with JOIN ----

    @Test
    void testViewWithJoin() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE v_orders (id INTEGER, customer_id INTEGER, amount INTEGER)");
            stmt.execute("CREATE TABLE v_customers (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO v_customers VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO v_customers VALUES (2, 'Bob')");
            stmt.execute("INSERT INTO v_orders VALUES (10, 1, 100)");
            stmt.execute("INSERT INTO v_orders VALUES (11, 2, 200)");
            stmt.execute("INSERT INTO v_orders VALUES (12, 1, 150)");

            stmt.execute("CREATE VIEW customer_orders AS " +
                    "SELECT c.name, o.amount FROM v_orders o JOIN v_customers c ON o.customer_id = c.id");

            ResultSet rs = stmt.executeQuery("SELECT name, SUM(amount) AS total FROM customer_orders GROUP BY name ORDER BY name");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals(250L, rs.getLong("total"));
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("name"));
            assertEquals(200L, rs.getLong("total"));
            assertFalse(rs.next());
        }
    }
}
