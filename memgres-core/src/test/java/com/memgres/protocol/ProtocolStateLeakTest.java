package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PgWire extended protocol state leaks.
 *
 * The PG extended protocol separates Parse/Bind/Describe/Execute/Sync.
 * If the handler caches results from Describe and returns them in Execute,
 * state can leak between queries:
 *   - A cached result from query A is returned for query B
 *   - A DML Describe executes the INSERT as a side effect, then Execute runs it again
 *   - Column layout mismatch when different queries share the unnamed portal
 *
 * These tests stress the exact patterns that cause:
 *   "null passwordHash" (a column that should have a value appears null
 *   because the Execute returned a stale/mismatched cached result).
 */
class ProtocolStateLeakTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // 1. INSERT then SELECT: verify all columns, 50 iterations
    // =========================================================================

    @Test
    void testInsertThenSelectAllColumns() throws SQLException {
        exec("""
            CREATE TABLE leak_users (
                user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                email_verified BOOLEAN NOT NULL DEFAULT false,
                verification_code TEXT,
                login_code_required BOOLEAN NOT NULL DEFAULT false
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO leak_users (email, password_hash, email_verified, verification_code, login_code_required) "
                             + "VALUES (?, ?, ?, ?, ?)",
                     Statement.RETURN_GENERATED_KEYS);
             PreparedStatement select = conn.prepareStatement(
                     "SELECT user_id, email, password_hash, email_verified, verification_code, login_code_required "
                             + "FROM leak_users WHERE user_id = ?")) {

            for (int i = 0; i < 50; i++) {
                String email = "user" + i + "@test.com";
                String hash = "hash_" + i + "_" + UUID.randomUUID();

                insert.setString(1, email);
                insert.setString(2, hash);
                insert.setBoolean(3, false);
                insert.setString(4, "code_" + i);
                insert.setBoolean(5, false);
                insert.executeUpdate();

                UUID userId;
                try (ResultSet keys = insert.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys must return a row on iteration " + i);
                    userId = (UUID) keys.getObject(1);
                    assertNotNull(userId, "Generated user_id must not be null on iteration " + i);
                }

                select.setObject(1, userId);
                try (ResultSet rs = select.executeQuery()) {
                    assertTrue(rs.next(), "SELECT must find the user on iteration " + i);
                    assertEquals(email, rs.getString("email"),
                            "email mismatch on iteration " + i);
                    String foundHash = rs.getString("password_hash");
                    assertNotNull(foundHash,
                            "password_hash must NOT be null on iteration " + i + " for user " + email);
                    assertEquals(hash, foundHash,
                            "password_hash mismatch on iteration " + i);
                    assertFalse(rs.getBoolean("email_verified"));
                    assertEquals("code_" + i, rs.getString("verification_code"));
                    assertFalse(rs.getBoolean("login_code_required"));
                }
            }
        }
    }

    // =========================================================================
    // 2. Alternating INSERT and SELECT with reused PreparedStatements
    // =========================================================================

    @Test
    void testAlternatingInsertSelectReuse() throws SQLException {
        exec("""
            CREATE TABLE leak_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                active BOOLEAN DEFAULT true
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO leak_items (name, price) VALUES (?, ?) RETURNING id");
             PreparedStatement select = conn.prepareStatement(
                     "SELECT id, name, price, active FROM leak_items WHERE id = ?")) {

            for (int i = 0; i < 50; i++) {
                String name = "item_" + i;
                BigDecimal price = new BigDecimal("10.00").add(new BigDecimal(i));

                insert.setString(1, name);
                insert.setBigDecimal(2, price);
                UUID id;
                try (ResultSet rs = insert.executeQuery()) {
                    assertTrue(rs.next(), "INSERT RETURNING must return a row on iteration " + i);
                    id = (UUID) rs.getObject(1);
                    assertNotNull(id);
                }

                select.setObject(1, id);
                try (ResultSet rs = select.executeQuery()) {
                    assertTrue(rs.next(), "SELECT must find row on iteration " + i);
                    assertEquals(name, rs.getString("name"),
                            "name mismatch on iteration " + i);
                    BigDecimal foundPrice = rs.getBigDecimal("price");
                    assertNotNull(foundPrice, "price must not be null on iteration " + i);
                    assertEquals(0, price.compareTo(foundPrice),
                            "price mismatch on iteration " + i + ": expected " + price + " got " + foundPrice);
                    assertTrue(rs.getBoolean("active"),
                            "active must be true on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 3. TRUNCATE between INSERT/SELECT cycles
    // =========================================================================

    @Test
    void testTruncateBetweenCycles() throws SQLException {
        exec("""
            CREATE TABLE leak_cycle (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                value TEXT NOT NULL,
                counter INT NOT NULL DEFAULT 0
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO leak_cycle (value, counter) VALUES (?, ?) RETURNING id");
             PreparedStatement select = conn.prepareStatement(
                     "SELECT value, counter FROM leak_cycle WHERE id = ?");
             PreparedStatement count = conn.prepareStatement(
                     "SELECT COUNT(*) FROM leak_cycle")) {

            for (int cycle = 0; cycle < 5; cycle++) {
                // Insert 10 rows
                UUID[] ids = new UUID[10];
                for (int i = 0; i < 10; i++) {
                    insert.setString(1, "cycle_" + cycle + "_val_" + i);
                    insert.setInt(2, cycle * 100 + i);
                    try (ResultSet rs = insert.executeQuery()) {
                        assertTrue(rs.next());
                        ids[i] = (UUID) rs.getObject(1);
                    }
                }

                // Verify all rows
                for (int i = 0; i < 10; i++) {
                    select.setObject(1, ids[i]);
                    try (ResultSet rs = select.executeQuery()) {
                        assertTrue(rs.next(),
                                "Must find row on cycle " + cycle + " iteration " + i);
                        assertEquals("cycle_" + cycle + "_val_" + i, rs.getString("value"),
                                "value mismatch on cycle " + cycle + " iteration " + i);
                        assertEquals(cycle * 100 + i, rs.getInt("counter"),
                                "counter mismatch on cycle " + cycle + " iteration " + i);
                    }
                }

                // Verify count
                try (ResultSet rs = count.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(10, rs.getInt(1),
                            "Must have exactly 10 rows on cycle " + cycle);
                }

                // Truncate
                exec("TRUNCATE leak_cycle CASCADE");

                // Verify empty after truncate
                try (ResultSet rs = count.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1),
                            "Must have 0 rows after TRUNCATE on cycle " + cycle);
                }
            }
        }
    }

    // =========================================================================
    // 4. Multiple column types: verify no column position shift
    // =========================================================================

    @Test
    void testMultipleColumnTypesNoShift() throws SQLException {
        exec("""
            CREATE TABLE leak_typed (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                text_col TEXT,
                int_col INTEGER,
                numeric_col NUMERIC(10,2),
                bool_col BOOLEAN,
                ts_col TIMESTAMP DEFAULT now(),
                double_col DOUBLE PRECISION
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO leak_typed (text_col, int_col, numeric_col, bool_col, double_col) "
                             + "VALUES (?, ?, ?, ?, ?) RETURNING id");
             PreparedStatement select = conn.prepareStatement(
                     "SELECT id, text_col, int_col, numeric_col, bool_col, ts_col, double_col "
                             + "FROM leak_typed WHERE id = ?")) {

            for (int i = 0; i < 30; i++) {
                String text = "text_" + i;
                int intVal = i * 42;
                BigDecimal numVal = new BigDecimal(i + ".99");
                boolean boolVal = i % 2 == 0;
                double doubleVal = i * 3.14;

                insert.setString(1, text);
                insert.setInt(2, intVal);
                insert.setBigDecimal(3, numVal);
                insert.setBoolean(4, boolVal);
                insert.setDouble(5, doubleVal);
                UUID id;
                try (ResultSet rs = insert.executeQuery()) {
                    assertTrue(rs.next());
                    id = (UUID) rs.getObject(1);
                }

                select.setObject(1, id);
                try (ResultSet rs = select.executeQuery()) {
                    assertTrue(rs.next(), "Must find row on iteration " + i);
                    assertEquals(text, rs.getString("text_col"),
                            "text_col mismatch on iteration " + i);
                    assertEquals(intVal, rs.getInt("int_col"),
                            "int_col mismatch on iteration " + i);
                    BigDecimal foundNum = rs.getBigDecimal("numeric_col");
                    assertNotNull(foundNum, "numeric_col null on iteration " + i);
                    assertEquals(boolVal, rs.getBoolean("bool_col"),
                            "bool_col mismatch on iteration " + i);
                    assertNotNull(rs.getTimestamp("ts_col"),
                            "ts_col null on iteration " + i);
                    assertEquals(doubleVal, rs.getDouble("double_col"), 0.001,
                            "double_col mismatch on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 5. High-frequency single PreparedStatement reuse (100 iterations)
    // =========================================================================

    @Test
    void testHighFrequencyReuse() throws SQLException {
        exec("CREATE TABLE leak_hf (id serial PRIMARY KEY, val TEXT NOT NULL)");
        for (int i = 1; i <= 100; i++) {
            exec("INSERT INTO leak_hf (val) VALUES ('row_" + i + "')");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM leak_hf WHERE id = ?")) {
            for (int i = 1; i <= 100; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Must find row on iteration " + i);
                    assertEquals("row_" + i, rs.getString(1),
                            "Value mismatch on iteration " + i);
                    assertFalse(rs.next(), "Must have exactly one row on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 6. INSERT without RETURNING + separate SELECT
    // =========================================================================

    @Test
    void testInsertWithoutReturningThenSelect() throws SQLException {
        exec("""
            CREATE TABLE leak_dao (
                user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO leak_dao (email, password_hash) VALUES (?, ?)",
                     Statement.RETURN_GENERATED_KEYS);
             PreparedStatement selectByEmail = conn.prepareStatement(
                     "SELECT user_id, email, password_hash, created_at FROM leak_dao WHERE email = ?")) {

            for (int i = 0; i < 30; i++) {
                String email = "dao_user_" + i + "@test.com";
                String hash = "bcrypt_hash_" + UUID.randomUUID();

                insert.setString(1, email);
                insert.setString(2, hash);
                int affected = insert.executeUpdate();
                assertEquals(1, affected, "INSERT must affect 1 row on iteration " + i);

                UUID userId;
                try (ResultSet keys = insert.getGeneratedKeys()) {
                    assertTrue(keys.next(), "Must get generated key on iteration " + i);
                    userId = (UUID) keys.getObject(1);
                    assertNotNull(userId);
                }

                // Now SELECT by email (different PreparedStatement)
                selectByEmail.setString(1, email);
                try (ResultSet rs = selectByEmail.executeQuery()) {
                    assertTrue(rs.next(), "Must find user by email on iteration " + i);
                    assertEquals(userId, rs.getObject("user_id"),
                            "user_id mismatch on iteration " + i);
                    assertEquals(email, rs.getString("email"));
                    String foundHash = rs.getString("password_hash");
                    assertNotNull(foundHash,
                            "password_hash MUST NOT be null on iteration " + i);
                    assertEquals(hash, foundHash,
                            "password_hash mismatch on iteration " + i);
                    assertNotNull(rs.getTimestamp("created_at"));
                }
            }
        }
    }

    // =========================================================================
    // 7. Interleaved DML: INSERT + UPDATE + SELECT
    // =========================================================================

    @Test
    void testInterleavedDmlOperations() throws SQLException {
        exec("""
            CREATE TABLE leak_interleave (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                status TEXT NOT NULL DEFAULT 'pending',
                amount NUMERIC(10,2) NOT NULL
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO leak_interleave (amount) VALUES (?) RETURNING id");
             PreparedStatement update = conn.prepareStatement(
                     "UPDATE leak_interleave SET status = ?, amount = amount + 1 WHERE id = ?");
             PreparedStatement select = conn.prepareStatement(
                     "SELECT status, amount FROM leak_interleave WHERE id = ?")) {

            for (int i = 0; i < 30; i++) {
                BigDecimal amount = new BigDecimal("100.00").add(new BigDecimal(i));

                // INSERT
                insert.setBigDecimal(1, amount);
                UUID id;
                try (ResultSet rs = insert.executeQuery()) {
                    assertTrue(rs.next());
                    id = (UUID) rs.getObject(1);
                }

                // UPDATE
                update.setString(1, "active");
                update.setObject(2, id);
                assertEquals(1, update.executeUpdate(),
                        "UPDATE must affect 1 row on iteration " + i);

                // SELECT to verify both the status change and amount
                select.setObject(1, id);
                try (ResultSet rs = select.executeQuery()) {
                    assertTrue(rs.next(), "Must find row on iteration " + i);
                    assertEquals("active", rs.getString("status"),
                            "status must be 'active' after UPDATE on iteration " + i);
                    BigDecimal foundAmount = rs.getBigDecimal("amount");
                    assertNotNull(foundAmount, "amount null on iteration " + i);
                    // amount should be original + 1 (from the UPDATE)
                    assertEquals(0, amount.add(BigDecimal.ONE).compareTo(foundAmount),
                            "amount mismatch on iteration " + i);
                }
            }
        }
    }
}
