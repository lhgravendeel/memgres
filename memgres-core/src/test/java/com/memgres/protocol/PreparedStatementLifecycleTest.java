package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 5, 16: Prepared-statement lifecycle, JDBC parameter binding,
 * schema changes invalidating prepared plans, search_path after prepare.
 * Also covers section 1 (JDBC parameter binding behavior) from Java/JDBC section.
 */
class PreparedStatementLifecycleTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // --- PREPARE / EXECUTE lifecycle ---

    @Test void prepare_and_execute_basic() throws Exception {
        exec("CREATE TABLE ps_t1(id int PRIMARY KEY, v text)");
        exec("INSERT INTO ps_t1 VALUES (1,'a'),(2,'b')");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM ps_t1 WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
            }
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
            }
        } finally {
            exec("DROP TABLE ps_t1");
        }
    }

    @Test void prepared_statement_reused_many_times() throws Exception {
        exec("CREATE TABLE ps_reuse(id int PRIMARY KEY)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_reuse VALUES (?)")) {
            for (int i = 0; i < 100; i++) {
                ps.setInt(1, i);
                ps.executeUpdate();
            }
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM ps_reuse")) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        } finally {
            exec("DROP TABLE ps_reuse");
        }
    }

    @Test void deallocate_and_reexecute() throws Exception {
        exec("CREATE TABLE ps_deal(id int PRIMARY KEY, v text)");
        exec("INSERT INTO ps_deal VALUES (1,'x')");
        // SQL-level PREPARE
        exec("PREPARE stmt_deal AS SELECT v FROM ps_deal WHERE id = $1");
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE stmt_deal(1)")) {
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
        }
        exec("DEALLOCATE stmt_deal");
        // After deallocate, executing should fail
        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE stmt_deal(1)")) {}
        });
        exec("DROP TABLE ps_deal");
    }

    @Test void schema_change_invalidates_prepared() throws Exception {
        exec("CREATE TABLE ps_inval(id int, v text)");
        exec("INSERT INTO ps_inval VALUES (1, 'old')");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM ps_inval WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("old", rs.getString(1));
            }
            // Change the table schema
            exec("ALTER TABLE ps_inval ADD COLUMN extra int");
            exec("UPDATE ps_inval SET v = 'new' WHERE id = 1");
            // Re-execute: should still work (plan may be re-prepared)
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("new", rs.getString(1));
            }
        } finally {
            exec("DROP TABLE ps_inval");
        }
    }

    @Test void search_path_change_after_prepare() throws Exception {
        exec("CREATE SCHEMA ps_schema");
        exec("CREATE TABLE ps_schema.sp_t(v text)");
        exec("INSERT INTO ps_schema.sp_t VALUES ('in_schema')");
        exec("SET search_path = ps_schema");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sp_t")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("in_schema", rs.getString(1));
            }
        }
        exec("SET search_path = public");
        exec("DROP SCHEMA ps_schema CASCADE");
    }

    // --- JDBC parameter binding types ---

    @Test void bind_integer() throws Exception {
        exec("CREATE TABLE ps_int(v int)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_int VALUES (?)")) {
            ps.setInt(1, 42);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_int")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
        exec("DROP TABLE ps_int");
    }

    @Test void bind_long() throws Exception {
        exec("CREATE TABLE ps_long(v bigint)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_long VALUES (?)")) {
            ps.setLong(1, 9_000_000_000L);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_long")) {
            assertTrue(rs.next());
            assertEquals(9_000_000_000L, rs.getLong(1));
        }
        exec("DROP TABLE ps_long");
    }

    @Test void bind_big_decimal() throws Exception {
        exec("CREATE TABLE ps_bd(v numeric(20,5))");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_bd VALUES (?)")) {
            ps.setBigDecimal(1, new BigDecimal("12345.67890"));
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_bd")) {
            assertTrue(rs.next());
            assertEquals(new BigDecimal("12345.67890"), rs.getBigDecimal(1));
        }
        exec("DROP TABLE ps_bd");
    }

    @Test void bind_string() throws Exception {
        exec("CREATE TABLE ps_str(v text)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_str VALUES (?)")) {
            ps.setString(1, "hello world");
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_str")) {
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
        }
        exec("DROP TABLE ps_str");
    }

    @Test void bind_boolean() throws Exception {
        exec("CREATE TABLE ps_bool(v boolean)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_bool VALUES (?)")) {
            ps.setBoolean(1, true);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_bool")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
        exec("DROP TABLE ps_bool");
    }

    @Test void bind_null_integer() throws Exception {
        exec("CREATE TABLE ps_ni(v int)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_ni VALUES (?)")) {
            ps.setNull(1, Types.INTEGER);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_ni")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
        exec("DROP TABLE ps_ni");
    }

    @Test void bind_null_text() throws Exception {
        exec("CREATE TABLE ps_nt(v text)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_nt VALUES (?)")) {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_nt")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
        exec("DROP TABLE ps_nt");
    }

    @Test void bind_uuid() throws Exception {
        exec("CREATE TABLE ps_uuid(v uuid)");
        java.util.UUID uuid = java.util.UUID.randomUUID();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_uuid VALUES (?)")) {
            ps.setObject(1, uuid);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_uuid")) {
            assertTrue(rs.next());
            assertEquals(uuid.toString(), rs.getString(1));
        }
        exec("DROP TABLE ps_uuid");
    }

    @Test void bind_date() throws Exception {
        exec("CREATE TABLE ps_date(v date)");
        java.sql.Date d = java.sql.Date.valueOf("2024-03-15");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_date VALUES (?)")) {
            ps.setDate(1, d);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_date")) {
            assertTrue(rs.next());
            assertEquals(d.toString(), rs.getDate(1).toString());
        }
        exec("DROP TABLE ps_date");
    }

    @Test void bind_timestamp() throws Exception {
        exec("CREATE TABLE ps_ts(v timestamp)");
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2024-03-15 10:30:00");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_ts VALUES (?)")) {
            ps.setTimestamp(1, ts);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM ps_ts")) {
            assertTrue(rs.next());
            assertEquals(ts, rs.getTimestamp(1));
        }
        exec("DROP TABLE ps_ts");
    }

    // --- Batch execution ---

    @Test void batch_insert_basic() throws Exception {
        exec("CREATE TABLE ps_batch(id int PRIMARY KEY, v text)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_batch VALUES (?, ?)")) {
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, "val" + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(10, counts.length);
            for (int c : counts) assertEquals(1, c);
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM ps_batch")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
        }
        exec("DROP TABLE ps_batch");
    }

    @Test void batch_with_duplicate_key_violation() throws Exception {
        exec("CREATE TABLE ps_bdup(id int PRIMARY KEY)");
        exec("INSERT INTO ps_bdup VALUES (5)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_bdup VALUES (?)")) {
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 5); // duplicate
            ps.addBatch();
            ps.setInt(1, 10);
            ps.addBatch();
            assertThrows(BatchUpdateException.class, ps::executeBatch);
        }
        exec("DROP TABLE ps_bdup");
    }

    @Test void batch_with_mixed_null_nonnull() throws Exception {
        exec("CREATE TABLE ps_bmix(id int, v text)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ps_bmix VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "has_value");
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setNull(2, Types.VARCHAR);
            ps.addBatch();
            ps.setInt(1, 3);
            ps.setString(2, "another");
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(3, counts.length);
        }
        exec("DROP TABLE ps_bmix");
    }
}
