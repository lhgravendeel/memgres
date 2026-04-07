package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6: DML Extensions tests.
 * RETURNING, UPSERT (ON CONFLICT), INSERT...SELECT, multi-table UPDATE with FROM.
 */
class DMLExtensionsTest {

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

    // ==================== INSERT ... RETURNING ====================

    @Test
    void testInsertReturningAllColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ret_test (id SERIAL PRIMARY KEY, name TEXT, value INTEGER)");
            ResultSet rs = stmt.executeQuery(
                    "INSERT INTO ret_test (name, value) VALUES ('alpha', 10) RETURNING *");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alpha", rs.getString("name"));
            assertEquals(10, rs.getInt("value"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE ret_test");
        }
    }

    @Test
    void testInsertReturningSingleColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ret_single (id SERIAL PRIMARY KEY, name TEXT)");
            ResultSet rs = stmt.executeQuery(
                    "INSERT INTO ret_single (name) VALUES ('beta') RETURNING id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE ret_single");
        }
    }

    @Test
    void testInsertReturningMultipleRows() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ret_multi (id SERIAL PRIMARY KEY, name TEXT)");
            ResultSet rs = stmt.executeQuery(
                    "INSERT INTO ret_multi (name) VALUES ('a'), ('b'), ('c') RETURNING id, name");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("a", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("b", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("c", rs.getString("name"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE ret_multi");
        }
    }

    // ==================== UPDATE ... RETURNING ====================

    @Test
    void testUpdateReturning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upd_ret (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            stmt.execute("INSERT INTO upd_ret VALUES (1, 'x', 10), (2, 'y', 20)");
            ResultSet rs = stmt.executeQuery(
                    "UPDATE upd_ret SET value = value + 100 WHERE id = 1 RETURNING id, value");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(110, rs.getInt("value"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE upd_ret");
        }
    }

    @Test
    void testUpdateReturningAllColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upd_ret2 (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO upd_ret2 VALUES (1, 'old')");
            ResultSet rs = stmt.executeQuery(
                    "UPDATE upd_ret2 SET name = 'new' WHERE id = 1 RETURNING *");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("new", rs.getString("name"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE upd_ret2");
        }
    }

    // ==================== DELETE ... RETURNING ====================

    @Test
    void testDeleteReturning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE del_ret (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO del_ret VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");
            ResultSet rs = stmt.executeQuery(
                    "DELETE FROM del_ret WHERE id > 1 RETURNING id, name");
            int count = 0;
            while (rs.next()) {
                count++;
                assertTrue(rs.getInt("id") > 1);
            }
            assertEquals(2, count);
            stmt.execute("DROP TABLE del_ret");
        }
    }

    @Test
    void testDeleteReturningAll() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE del_ret2 (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO del_ret2 VALUES (1, 'x')");
            ResultSet rs = stmt.executeQuery(
                    "DELETE FROM del_ret2 RETURNING *");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("x", rs.getString("name"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE del_ret2");
        }
    }

    // ==================== INSERT ... ON CONFLICT DO NOTHING ====================

    @Test
    void testUpsertDoNothing() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upsert_dn (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO upsert_dn VALUES (1, 'original')");
            // Should not fail, just skip the conflicting row
            stmt.execute("INSERT INTO upsert_dn VALUES (1, 'duplicate') ON CONFLICT (id) DO NOTHING");
            ResultSet rs = stmt.executeQuery("SELECT name FROM upsert_dn WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("original", rs.getString("name"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE upsert_dn");
        }
    }

    @Test
    void testUpsertDoNothingNonConflicting() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upsert_dn2 (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO upsert_dn2 VALUES (1, 'first')");
            stmt.execute("INSERT INTO upsert_dn2 VALUES (2, 'second') ON CONFLICT (id) DO NOTHING");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM upsert_dn2");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            stmt.execute("DROP TABLE upsert_dn2");
        }
    }

    // ==================== INSERT ... ON CONFLICT DO UPDATE ====================

    @Test
    void testUpsertDoUpdate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upsert_du (id INTEGER PRIMARY KEY, name TEXT, count INTEGER)");
            stmt.execute("INSERT INTO upsert_du VALUES (1, 'item', 1)");
            stmt.execute("INSERT INTO upsert_du VALUES (1, 'item', 1) " +
                    "ON CONFLICT (id) DO UPDATE SET count = upsert_du.count + 1");
            ResultSet rs = stmt.executeQuery("SELECT count FROM upsert_du WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("count"));
            stmt.execute("DROP TABLE upsert_du");
        }
    }

    @Test
    void testUpsertDoUpdateWithExcluded() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upsert_ex (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            stmt.execute("INSERT INTO upsert_ex VALUES (1, 'old', 10)");
            stmt.execute("INSERT INTO upsert_ex VALUES (1, 'new', 99) " +
                    "ON CONFLICT (id) DO UPDATE SET name = excluded.name, value = excluded.value");
            ResultSet rs = stmt.executeQuery("SELECT name, value FROM upsert_ex WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("new", rs.getString("name"));
            assertEquals(99, rs.getInt("value"));
            stmt.execute("DROP TABLE upsert_ex");
        }
    }

    @Test
    void testUpsertDoUpdateReturning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upsert_ret (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO upsert_ret VALUES (1, 'old')");
            ResultSet rs = stmt.executeQuery(
                    "INSERT INTO upsert_ret VALUES (1, 'new') " +
                            "ON CONFLICT (id) DO UPDATE SET name = excluded.name RETURNING id, name");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("new", rs.getString("name"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE upsert_ret");
        }
    }

    // ==================== INSERT ... SELECT ====================

    @Test
    void testInsertSelect() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE src (id INTEGER, name TEXT)");
            stmt.execute("CREATE TABLE dst (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            stmt.execute("INSERT INTO dst SELECT * FROM src WHERE id <= 2");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM dst");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            // Verify data
            rs = stmt.executeQuery("SELECT name FROM dst ORDER BY id");
            assertTrue(rs.next());
            assertEquals("a", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("name"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE src");
            stmt.execute("DROP TABLE dst");
        }
    }

    @Test
    void testInsertSelectWithColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE src2 (id INTEGER, name TEXT, extra TEXT)");
            stmt.execute("CREATE TABLE dst2 (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO src2 VALUES (1, 'x', 'ignore'), (2, 'y', 'ignore')");
            stmt.execute("INSERT INTO dst2 (id, name) SELECT id, name FROM src2");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM dst2");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            stmt.execute("DROP TABLE src2");
            stmt.execute("DROP TABLE dst2");
        }
    }

    @Test
    void testInsertSelectReturning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE src3 (val INTEGER)");
            stmt.execute("CREATE TABLE dst3 (id SERIAL PRIMARY KEY, val INTEGER)");
            stmt.execute("INSERT INTO src3 VALUES (10), (20)");
            ResultSet rs = stmt.executeQuery(
                    "INSERT INTO dst3 (val) SELECT val FROM src3 RETURNING id, val");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(10, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(20, rs.getInt("val"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE src3");
            stmt.execute("DROP TABLE dst3");
        }
    }

    // ==================== Multi-table UPDATE with FROM ====================

    @Test
    void testUpdateWithFromClause() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)");
            stmt.execute("CREATE TABLE discounts (product_id INTEGER, discount INTEGER)");
            stmt.execute("INSERT INTO products VALUES (1, 'Widget', 100), (2, 'Gadget', 200)");
            stmt.execute("INSERT INTO discounts VALUES (1, 10), (2, 50)");
            stmt.execute("UPDATE products SET price = products.price - discounts.discount " +
                    "FROM discounts WHERE products.id = discounts.product_id");
            ResultSet rs = stmt.executeQuery("SELECT id, price FROM products ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(90, rs.getInt("price"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(150, rs.getInt("price"));
            stmt.execute("DROP TABLE discounts");
            stmt.execute("DROP TABLE products");
        }
    }

    @Test
    void testUpdateWithFromReturning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT)");
            stmt.execute("CREATE TABLE updates (item_id INTEGER, new_status TEXT)");
            stmt.execute("INSERT INTO items VALUES (1, 'pending'), (2, 'active')");
            stmt.execute("INSERT INTO updates VALUES (1, 'done')");
            ResultSet rs = stmt.executeQuery(
                    "UPDATE items SET status = updates.new_status " +
                            "FROM updates WHERE items.id = updates.item_id RETURNING items.id, items.status");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("done", rs.getString("status"));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE updates");
            stmt.execute("DROP TABLE items");
        }
    }

    // ==================== COPY TO (basic) ====================

    @Test
    void testCopyToStdout() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE copy_test (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO copy_test VALUES (1, 'hello'), (2, 'world')");
            // COPY TO STDOUT requires PG COPY protocol, not available via JDBC
            assertThrows(SQLException.class, () -> stmt.execute("COPY copy_test TO STDOUT"));
            stmt.execute("DROP TABLE copy_test");
        }
    }
}
