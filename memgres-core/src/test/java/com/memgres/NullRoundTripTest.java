package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip tests for NULL vs empty/zero/default values across all data types.
 * Verifies that:
 * - WHERE col IS NULL finds NULL rows
 * - WHERE col = NULL finds nothing (three-valued logic)
 * - NULL and "empty" values (empty string, empty bytea, 0, false, etc.) are distinct
 * - UPDATE to/from NULL works correctly with indexes
 * - DELETE WHERE col IS NULL works correctly
 * - IS DISTINCT FROM / IS NOT DISTINCT FROM work with NULL
 */
class NullRoundTripTest {

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

    // ==============================================================
    // TEXT / VARCHAR / CHAR: NULL vs empty string
    // ==============================================================

    @Test
    void textNullVsEmptyString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_text (id SERIAL PRIMARY KEY, val TEXT UNIQUE)");
            s.execute("INSERT INTO nrt_text (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_text (val) VALUES ('')");
            s.execute("INSERT INTO nrt_text (val) VALUES ('hello')");

            // IS NULL finds only the NULL row
            ResultSet rs = s.executeQuery("SELECT id FROM nrt_text WHERE val IS NULL");
            assertTrue(rs.next());
            int nullId = rs.getInt(1);
            assertFalse(rs.next());

            // IS NOT NULL finds empty string and 'hello'
            rs = s.executeQuery("SELECT count(*) FROM nrt_text WHERE val IS NOT NULL");
            rs.next();
            assertEquals(2, rs.getInt(1));

            // = NULL finds nothing (three-valued logic)
            rs = s.executeQuery("SELECT count(*) FROM nrt_text WHERE val = NULL");
            rs.next();
            assertEquals(0, rs.getInt(1));

            // Empty string is findable via =
            rs = s.executeQuery("SELECT id FROM nrt_text WHERE val = ''");
            assertTrue(rs.next());
            assertNotEquals(nullId, rs.getInt(1));

            // UPDATE NULL row to a value
            s.execute("UPDATE nrt_text SET val = 'was_null' WHERE val IS NULL");
            rs = s.executeQuery("SELECT val FROM nrt_text WHERE id = " + nullId);
            rs.next();
            assertEquals("was_null", rs.getString(1));

            // UPDATE back to NULL
            s.execute("UPDATE nrt_text SET val = NULL WHERE val = 'was_null'");
            rs = s.executeQuery("SELECT val FROM nrt_text WHERE id = " + nullId);
            rs.next();
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());

            // DELETE WHERE IS NULL
            s.execute("DELETE FROM nrt_text WHERE val IS NULL");
            rs = s.executeQuery("SELECT count(*) FROM nrt_text WHERE val IS NULL");
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void varcharNullVsEmptyString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_vc (id SERIAL PRIMARY KEY, val VARCHAR(50) UNIQUE)");
            s.execute("INSERT INTO nrt_vc (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_vc (val) VALUES ('')");

            // Both exist, are distinct
            ResultSet rs = s.executeQuery("SELECT count(*) FROM nrt_vc");
            rs.next();
            assertEquals(2, rs.getInt(1));

            rs = s.executeQuery("SELECT val FROM nrt_vc WHERE val IS NULL");
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertFalse(rs.next());

            rs = s.executeQuery("SELECT val FROM nrt_vc WHERE val = ''");
            assertTrue(rs.next());
            assertEquals("", rs.getString(1));
        }
    }

    @Test
    void charNullVsPaddedEmpty() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_char (id SERIAL PRIMARY KEY, val CHAR(5) UNIQUE)");
            s.execute("INSERT INTO nrt_char (val) VALUES (NULL)");
            // Empty string in CHAR(5) gets padded to 5 spaces
            s.execute("INSERT INTO nrt_char (val) VALUES ('')");

            rs(s, "SELECT val FROM nrt_char WHERE val IS NULL", rs -> {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
                assertFalse(rs.next());
            });

            // The empty-padded value is NOT NULL
            rs(s, "SELECT val FROM nrt_char WHERE val IS NOT NULL", rs -> {
                assertTrue(rs.next());
                assertFalse(rs.next());
            });
        }
    }

    // ==============================================================
    // INTEGER / BIGINT / SMALLINT: NULL vs 0
    // ==============================================================

    @Test
    void integerNullVsZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_int (id SERIAL PRIMARY KEY, val INTEGER UNIQUE)");
            s.execute("INSERT INTO nrt_int (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_int (val) VALUES (0)");
            s.execute("INSERT INTO nrt_int (val) VALUES (42)");

            // IS NULL finds only NULL
            rs(s, "SELECT id FROM nrt_int WHERE val IS NULL", rs -> {
                assertTrue(rs.next());
                assertFalse(rs.next());
            });

            // = 0 finds only zero, not NULL
            rs(s, "SELECT val FROM nrt_int WHERE val = 0", rs -> {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
                assertFalse(rs.next());
            });

            // IS DISTINCT FROM NULL finds non-NULL rows
            rs(s, "SELECT count(*) FROM nrt_int WHERE val IS DISTINCT FROM NULL", rs -> {
                rs.next();
                assertEquals(2, rs.getInt(1));
            });

            // IS NOT DISTINCT FROM NULL finds NULL rows
            rs(s, "SELECT count(*) FROM nrt_int WHERE val IS NOT DISTINCT FROM NULL", rs -> {
                rs.next();
                assertEquals(1, rs.getInt(1));
            });
        }
    }

    @Test
    void bigintNullVsZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_big (id SERIAL PRIMARY KEY, val BIGINT UNIQUE)");
            s.execute("INSERT INTO nrt_big (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_big (val) VALUES (0)");

            rs(s, "SELECT count(*) FROM nrt_big WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_big WHERE val = 0", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    @Test
    void smallintNullVsZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_sm (id SERIAL PRIMARY KEY, val SMALLINT UNIQUE)");
            s.execute("INSERT INTO nrt_sm (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_sm (val) VALUES (0)");

            rs(s, "SELECT count(*) FROM nrt_sm WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_sm WHERE val = 0", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // NUMERIC: NULL vs 0 vs 0.00 vs NaN
    // ==============================================================

    @Test
    void numericNullVsZeroVsNaN() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_num (id SERIAL PRIMARY KEY, val NUMERIC(10,2) UNIQUE)");
            s.execute("INSERT INTO nrt_num (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_num (val) VALUES (0)");
            // 0.00 should conflict with 0 since NUMERIC(10,2) normalizes both to 0.00
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO nrt_num (val) VALUES (0.00)"));

            rs(s, "SELECT count(*) FROM nrt_num WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_num WHERE val = 0", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            // 0.00 should also match 0 in WHERE
            rs(s, "SELECT count(*) FROM nrt_num WHERE val = 0.00", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // DOUBLE PRECISION / REAL: NULL vs 0.0 vs NaN vs Infinity
    // ==============================================================

    @Test
    void doubleNullVsZeroVsNaNVsInfinity() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_dbl (id SERIAL PRIMARY KEY, val DOUBLE PRECISION UNIQUE)");
            s.execute("INSERT INTO nrt_dbl (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_dbl (val) VALUES (0.0)");
            s.execute("INSERT INTO nrt_dbl (val) VALUES ('NaN')");
            s.execute("INSERT INTO nrt_dbl (val) VALUES ('Infinity')");
            s.execute("INSERT INTO nrt_dbl (val) VALUES ('-Infinity')");

            // Each is distinct
            rs(s, "SELECT count(*) FROM nrt_dbl", rs -> {
                rs.next(); assertEquals(5, rs.getInt(1));
            });

            // IS NULL finds only NULL
            rs(s, "SELECT count(*) FROM nrt_dbl WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Each special value is selectable
            rs(s, "SELECT id FROM nrt_dbl WHERE val = 0.0", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });
            rs(s, "SELECT id FROM nrt_dbl WHERE val = 'NaN'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });
            rs(s, "SELECT id FROM nrt_dbl WHERE val = 'Infinity'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });
            rs(s, "SELECT id FROM nrt_dbl WHERE val = '-Infinity'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });

            // UPDATE NaN to NULL, then back
            s.execute("UPDATE nrt_dbl SET val = NULL WHERE val = 'NaN'");
            rs(s, "SELECT count(*) FROM nrt_dbl WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1)); // original NULL + former NaN
            });
            // Can re-insert NaN now
            s.execute("INSERT INTO nrt_dbl (val) VALUES ('NaN')");
        }
    }

    @Test
    void realNullVsZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_real (id SERIAL PRIMARY KEY, val REAL UNIQUE)");
            s.execute("INSERT INTO nrt_real (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_real (val) VALUES (0.0)");
            s.execute("INSERT INTO nrt_real (val) VALUES ('NaN')");

            rs(s, "SELECT count(*) FROM nrt_real WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_real WHERE val = 0.0", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_real WHERE val = 'NaN'", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // BOOLEAN: NULL vs FALSE vs TRUE
    // ==============================================================

    @Test
    void booleanNullVsFalse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_bool (id SERIAL PRIMARY KEY, val BOOLEAN)");
            s.execute("INSERT INTO nrt_bool (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_bool (val) VALUES (FALSE)");
            s.execute("INSERT INTO nrt_bool (val) VALUES (TRUE)");

            // IS NULL finds only NULL, not FALSE
            rs(s, "SELECT count(*) FROM nrt_bool WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // = FALSE finds only FALSE, not NULL
            rs(s, "SELECT count(*) FROM nrt_bool WHERE val = FALSE", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Bare WHERE val excludes both NULL and FALSE
            rs(s, "SELECT count(*) FROM nrt_bool WHERE val", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // NOT val includes FALSE but NOT NULL
            rs(s, "SELECT count(*) FROM nrt_bool WHERE NOT val", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // UPDATE NULL to FALSE
            s.execute("UPDATE nrt_bool SET val = FALSE WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_bool WHERE val = FALSE", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // BYTEA: NULL vs empty byte array vs zero byte
    // ==============================================================

    @Test
    void byteaNullVsEmptyVsZeroByte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_bytea (id SERIAL PRIMARY KEY, val BYTEA UNIQUE)");
            s.execute("INSERT INTO nrt_bytea (val) VALUES (NULL)");
            // Empty byte array
            s.execute("INSERT INTO nrt_bytea (val) VALUES ('\\x')");
            // Single zero byte
            s.execute("INSERT INTO nrt_bytea (val) VALUES ('\\x00')");
            // Non-empty bytes
            s.execute("INSERT INTO nrt_bytea (val) VALUES ('\\xDEAD')");

            // IS NULL finds only NULL
            rs(s, "SELECT count(*) FROM nrt_bytea WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // IS NOT NULL finds the three non-null rows
            rs(s, "SELECT count(*) FROM nrt_bytea WHERE val IS NOT NULL", rs -> {
                rs.next(); assertEquals(3, rs.getInt(1));
            });

            // Empty bytea is selectable via =
            rs(s, "SELECT id FROM nrt_bytea WHERE val = '\\x'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });

            // UPDATE NULL to empty bytea; should fail because empty bytea already exists (UNIQUE)
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE nrt_bytea SET val = '\\x' WHERE val IS NULL"));

            // UPDATE NULL to a new unique value should succeed
            s.execute("UPDATE nrt_bytea SET val = '\\xFF' WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_bytea WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });

            // UPDATE back to NULL
            s.execute("UPDATE nrt_bytea SET val = NULL WHERE val = '\\xFF'");
            rs(s, "SELECT count(*) FROM nrt_bytea WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    @Test
    void byteaNullUpdateRoundTrip() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_bytea2 (id SERIAL PRIMARY KEY, val BYTEA)");
            s.execute("INSERT INTO nrt_bytea2 (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_bytea2 (val) VALUES ('\\xCAFE')");

            // Update NULL to a value
            s.execute("UPDATE nrt_bytea2 SET val = '\\xBEEF' WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_bytea2 WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });

            // Update value back to NULL
            s.execute("UPDATE nrt_bytea2 SET val = NULL WHERE val = '\\xBEEF'");
            rs(s, "SELECT count(*) FROM nrt_bytea2 WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // DELETE WHERE IS NULL
            s.execute("DELETE FROM nrt_bytea2 WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_bytea2", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // UUID: NULL vs specific UUID
    // ==============================================================

    @Test
    void uuidNullVsValue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_uuid (id SERIAL PRIMARY KEY, val UUID UNIQUE)");
            s.execute("INSERT INTO nrt_uuid (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_uuid (val) VALUES ('00000000-0000-0000-0000-000000000000')");
            s.execute("INSERT INTO nrt_uuid (val) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");

            rs(s, "SELECT count(*) FROM nrt_uuid WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Zero UUID is not NULL
            rs(s, "SELECT id FROM nrt_uuid WHERE val = '00000000-0000-0000-0000-000000000000'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });

            // Update NULL to UUID, then back
            s.execute("UPDATE nrt_uuid SET val = 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_uuid WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });
            s.execute("UPDATE nrt_uuid SET val = NULL WHERE val = 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
            rs(s, "SELECT count(*) FROM nrt_uuid WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // DATE: NULL vs epoch
    // ==============================================================

    @Test
    void dateNullVsEpoch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_date (id SERIAL PRIMARY KEY, val DATE UNIQUE)");
            s.execute("INSERT INTO nrt_date (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_date (val) VALUES ('1970-01-01')");
            s.execute("INSERT INTO nrt_date (val) VALUES ('2024-06-15')");

            rs(s, "SELECT count(*) FROM nrt_date WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT id FROM nrt_date WHERE val = '1970-01-01'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });

            // Round-trip: NULL -> value -> NULL
            s.execute("UPDATE nrt_date SET val = '2000-01-01' WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_date WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });
            s.execute("UPDATE nrt_date SET val = NULL WHERE val = '2000-01-01'");
            rs(s, "SELECT count(*) FROM nrt_date WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // TIMESTAMP: NULL vs epoch
    // ==============================================================

    @Test
    void timestampNullVsEpoch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_ts (id SERIAL PRIMARY KEY, val TIMESTAMP UNIQUE)");
            s.execute("INSERT INTO nrt_ts (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_ts (val) VALUES ('1970-01-01 00:00:00')");
            s.execute("INSERT INTO nrt_ts (val) VALUES ('2024-06-15 12:30:00')");

            rs(s, "SELECT count(*) FROM nrt_ts WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT id FROM nrt_ts WHERE val = '1970-01-01 00:00:00'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });
        }
    }

    @Test
    void timestamptzNullVsEpoch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_tstz (id SERIAL PRIMARY KEY, val TIMESTAMPTZ UNIQUE)");
            s.execute("INSERT INTO nrt_tstz (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_tstz (val) VALUES ('1970-01-01 00:00:00 UTC')");

            rs(s, "SELECT count(*) FROM nrt_tstz WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_tstz WHERE val IS NOT NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // TIME: NULL vs midnight
    // ==============================================================

    @Test
    void timeNullVsMidnight() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_time (id SERIAL PRIMARY KEY, val TIME UNIQUE)");
            s.execute("INSERT INTO nrt_time (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_time (val) VALUES ('00:00:00')");
            s.execute("INSERT INTO nrt_time (val) VALUES ('12:30:00')");

            rs(s, "SELECT count(*) FROM nrt_time WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT id FROM nrt_time WHERE val = '00:00:00'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });
        }
    }

    // ==============================================================
    // INTERVAL: NULL vs zero interval
    // ==============================================================

    @Test
    void intervalNullVsZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_intv (id SERIAL PRIMARY KEY, val INTERVAL)");
            s.execute("INSERT INTO nrt_intv (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_intv (val) VALUES ('0 seconds')");
            s.execute("INSERT INTO nrt_intv (val) VALUES ('1 hour')");

            rs(s, "SELECT count(*) FROM nrt_intv WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_intv WHERE val IS NOT NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // JSONB: NULL vs JSON null vs empty object vs empty array
    // ==============================================================

    @Test
    void jsonbNullVsJsonNullVsEmptyObject() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_jb (id SERIAL PRIMARY KEY, val JSONB)");
            s.execute("INSERT INTO nrt_jb (val) VALUES (NULL)");       // SQL NULL
            s.execute("INSERT INTO nrt_jb (val) VALUES ('null')");     // JSON null literal
            s.execute("INSERT INTO nrt_jb (val) VALUES ('{}')");       // empty object
            s.execute("INSERT INTO nrt_jb (val) VALUES ('[]')");       // empty array
            s.execute("INSERT INTO nrt_jb (val) VALUES ('{\"a\":1}')"); // non-empty

            // SQL NULL is distinct from JSON null
            rs(s, "SELECT count(*) FROM nrt_jb WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_jb WHERE val IS NOT NULL", rs -> {
                rs.next(); assertEquals(4, rs.getInt(1));
            });

            // JSON null literal is selectable
            rs(s, "SELECT id FROM nrt_jb WHERE val::text = 'null'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });

            // Empty object is selectable
            rs(s, "SELECT id FROM nrt_jb WHERE val = '{}'", rs -> {
                assertTrue(rs.next()); assertFalse(rs.next());
            });
        }
    }

    // ==============================================================
    // INET: NULL vs 0.0.0.0/0 (all addresses) vs specific
    // ==============================================================

    @Test
    void inetNullVsAllAddresses() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_inet (id SERIAL PRIMARY KEY, val INET)");
            s.execute("INSERT INTO nrt_inet (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_inet (val) VALUES ('0.0.0.0/0')");    // all addresses
            s.execute("INSERT INTO nrt_inet (val) VALUES ('0.0.0.0')");      // zero address (host)
            s.execute("INSERT INTO nrt_inet (val) VALUES ('192.168.1.1')");

            rs(s, "SELECT count(*) FROM nrt_inet WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_inet WHERE val IS NOT NULL", rs -> {
                rs.next(); assertEquals(3, rs.getInt(1));
            });

            // UPDATE NULL to specific address
            s.execute("UPDATE nrt_inet SET val = '10.0.0.1' WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_inet WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // MONEY: NULL vs $0.00
    // ==============================================================

    @Test
    void moneyNullVsZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_money (id SERIAL PRIMARY KEY, val MONEY)");
            s.execute("INSERT INTO nrt_money (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_money (val) VALUES ('$0.00')");
            s.execute("INSERT INTO nrt_money (val) VALUES ('$99.99')");

            rs(s, "SELECT count(*) FROM nrt_money WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_money WHERE val IS NOT NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // Multiple NULLs in unique index (allowed by default)
    // ==============================================================

    @Test
    void multipleNullsInUniqueIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_multi (id SERIAL PRIMARY KEY, val TEXT UNIQUE)");
            // Multiple NULLs should be allowed in a UNIQUE index
            s.execute("INSERT INTO nrt_multi (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_multi (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_multi (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_multi (val) VALUES ('a')");

            rs(s, "SELECT count(*) FROM nrt_multi WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(3, rs.getInt(1));
            });

            // Delete one NULL row
            s.execute("DELETE FROM nrt_multi WHERE id = (SELECT min(id) FROM nrt_multi WHERE val IS NULL)");
            rs(s, "SELECT count(*) FROM nrt_multi WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });

            // Update one NULL to a value
            s.execute("UPDATE nrt_multi SET val = 'b' WHERE id = (SELECT min(id) FROM nrt_multi WHERE val IS NULL)");
            rs(s, "SELECT count(*) FROM nrt_multi WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // Composite key with some columns NULL
    // ==============================================================

    @Test
    void compositeKeyPartialNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_comp (a INTEGER, b TEXT, c INTEGER, UNIQUE(a, b))");
            s.execute("INSERT INTO nrt_comp VALUES (1, NULL, 10)");
            s.execute("INSERT INTO nrt_comp VALUES (1, NULL, 20)");  // allowed: NULL makes composite key distinct
            s.execute("INSERT INTO nrt_comp VALUES (NULL, 'x', 30)");
            s.execute("INSERT INTO nrt_comp VALUES (NULL, NULL, 40)");
            s.execute("INSERT INTO nrt_comp VALUES (1, 'x', 50)");

            // WHERE a IS NULL
            rs(s, "SELECT count(*) FROM nrt_comp WHERE a IS NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });

            // WHERE b IS NULL
            rs(s, "SELECT count(*) FROM nrt_comp WHERE b IS NULL", rs -> {
                rs.next(); assertEquals(3, rs.getInt(1));
            });

            // WHERE a IS NULL AND b IS NULL
            rs(s, "SELECT c FROM nrt_comp WHERE a IS NULL AND b IS NULL", rs -> {
                assertTrue(rs.next()); assertEquals(40, rs.getInt(1));
                assertFalse(rs.next());
            });

            // Update a NULL column to non-NULL
            s.execute("UPDATE nrt_comp SET a = 2 WHERE a IS NULL AND b = 'x'");
            rs(s, "SELECT a FROM nrt_comp WHERE c = 30", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // IS DISTINCT FROM round-trip across types
    // ==============================================================

    @Test
    void isDistinctFromWithNullValues() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_dist (id SERIAL PRIMARY KEY, n INTEGER, t TEXT, d DATE)");
            s.execute("INSERT INTO nrt_dist (n, t, d) VALUES (NULL, NULL, NULL)");
            s.execute("INSERT INTO nrt_dist (n, t, d) VALUES (0, '', '1970-01-01')");
            s.execute("INSERT INTO nrt_dist (n, t, d) VALUES (42, 'hello', '2024-01-01')");

            // IS NOT DISTINCT FROM NULL (same as IS NULL)
            rs(s, "SELECT count(*) FROM nrt_dist WHERE n IS NOT DISTINCT FROM NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // IS DISTINCT FROM NULL (same as IS NOT NULL)
            rs(s, "SELECT count(*) FROM nrt_dist WHERE n IS DISTINCT FROM NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });

            // IS NOT DISTINCT FROM 0: finds only 0, not NULL
            rs(s, "SELECT count(*) FROM nrt_dist WHERE n IS NOT DISTINCT FROM 0", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Same for text column
            rs(s, "SELECT count(*) FROM nrt_dist WHERE t IS NOT DISTINCT FROM NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_dist WHERE t IS NOT DISTINCT FROM ''", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Same for date column
            rs(s, "SELECT count(*) FROM nrt_dist WHERE d IS NOT DISTINCT FROM NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // COALESCE with NULL in indexed columns
    // ==============================================================

    @Test
    void coalesceWithNullInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_coal (id SERIAL PRIMARY KEY, val INTEGER UNIQUE, label TEXT)");
            s.execute("INSERT INTO nrt_coal (val, label) VALUES (NULL, 'null_row')");
            s.execute("INSERT INTO nrt_coal (val, label) VALUES (0, 'zero_row')");
            s.execute("INSERT INTO nrt_coal (val, label) VALUES (42, 'answer_row')");

            // COALESCE in SELECT should replace NULL with default
            rs(s, "SELECT COALESCE(val, -1) FROM nrt_coal WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(-1, rs.getInt(1));
            });

            // COALESCE in WHERE
            rs(s, "SELECT label FROM nrt_coal WHERE COALESCE(val, -1) = -1", rs -> {
                assertTrue(rs.next()); assertEquals("null_row", rs.getString(1));
                assertFalse(rs.next());
            });
        }
    }

    // ==============================================================
    // NULL in aggregates with indexed column
    // ==============================================================

    @Test
    void nullInAggregatesWithIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_agg (id SERIAL PRIMARY KEY, val INTEGER UNIQUE)");
            s.execute("INSERT INTO nrt_agg (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_agg (val) VALUES (NULL)");  // allowed, unique allows multiple NULLs
            s.execute("INSERT INTO nrt_agg (val) VALUES (10)");
            s.execute("INSERT INTO nrt_agg (val) VALUES (20)");

            // COUNT(*) counts all rows including NULLs
            rs(s, "SELECT count(*) FROM nrt_agg", rs -> {
                rs.next(); assertEquals(4, rs.getInt(1));
            });

            // COUNT(val) skips NULLs
            rs(s, "SELECT count(val) FROM nrt_agg", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });

            // SUM/AVG skip NULLs
            rs(s, "SELECT sum(val) FROM nrt_agg", rs -> {
                rs.next(); assertEquals(30, rs.getInt(1));
            });
            rs(s, "SELECT avg(val) FROM nrt_agg", rs -> {
                rs.next(); assertEquals(15.0, rs.getDouble(1), 0.01);
            });
        }
    }

    // ==============================================================
    // NULL with prepared statements and parameter binding
    // ==============================================================

    @Test
    void nullWithPreparedStatementBinding() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_ps (id SERIAL PRIMARY KEY, val TEXT UNIQUE, num INTEGER)");
        }

        // Insert NULL via prepared statement
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO nrt_ps (val, num) VALUES (?, ?)")) {
            ps.setNull(1, Types.VARCHAR);
            ps.setInt(2, 1);
            ps.executeUpdate();

            ps.setString(1, "hello");
            ps.setNull(2, Types.INTEGER);
            ps.executeUpdate();

            ps.setNull(1, Types.VARCHAR);
            ps.setNull(2, Types.INTEGER);
            ps.executeUpdate();
        }

        // Query with IS NULL via prepared statement
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM nrt_ps WHERE val IS NULL")) {
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(2, rs.getInt(1));
        }

        // Verify getObject returns null and wasNull works
        try (PreparedStatement ps = conn.prepareStatement("SELECT val, num FROM nrt_ps WHERE id = 1")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());
        }
    }

    // ==============================================================
    // NULL in ON CONFLICT scenarios
    // ==============================================================

    @Test
    void nullInOnConflict() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_oc (id INTEGER PRIMARY KEY, val TEXT UNIQUE)");
            s.execute("INSERT INTO nrt_oc VALUES (1, NULL)");
            s.execute("INSERT INTO nrt_oc VALUES (2, 'a')");

            // ON CONFLICT on PK, setting val to NULL
            s.execute("INSERT INTO nrt_oc VALUES (2, 'b') ON CONFLICT (id) DO UPDATE SET val = NULL");
            rs(s, "SELECT val FROM nrt_oc WHERE id = 2", rs -> {
                rs.next();
                assertNull(rs.getString(1));
            });
            // Now both rows have NULL val, which is fine since NULLs are distinct in unique
            rs(s, "SELECT count(*) FROM nrt_oc WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(2, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // Full round-trip: insert NULL, select, update, select, delete
    // ==============================================================

    @Test
    void fullRoundTripAllTypes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_all ("
                    + "id SERIAL PRIMARY KEY, "
                    + "t TEXT, "
                    + "i INTEGER, "
                    + "b BOOLEAN, "
                    + "d DATE, "
                    + "ts TIMESTAMP, "
                    + "f DOUBLE PRECISION, "
                    + "u UUID, "
                    + "ba BYTEA, "
                    + "j JSONB"
                    + ")");

            // Insert all NULLs
            s.execute("INSERT INTO nrt_all (t,i,b,d,ts,f,u,ba,j) VALUES "
                    + "(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)");

            // Insert all "zero/empty" values
            s.execute("INSERT INTO nrt_all (t,i,b,d,ts,f,u,ba,j) VALUES "
                    + "('',0,FALSE,'1970-01-01','1970-01-01 00:00:00',0.0,"
                    + "'00000000-0000-0000-0000-000000000000','\\x','{}')");

            // Verify NULL row found by IS NULL on each column
            rs(s, "SELECT count(*) FROM nrt_all WHERE t IS NULL AND i IS NULL AND b IS NULL "
                    + "AND d IS NULL AND ts IS NULL AND f IS NULL AND u IS NULL AND ba IS NULL AND j IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Verify zero/empty row is NOT null
            rs(s, "SELECT count(*) FROM nrt_all WHERE t IS NOT NULL AND i IS NOT NULL AND b IS NOT NULL "
                    + "AND d IS NOT NULL AND ts IS NOT NULL AND f IS NOT NULL AND u IS NOT NULL "
                    + "AND ba IS NOT NULL AND j IS NOT NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Update all-NULL row to have values
            s.execute("UPDATE nrt_all SET t='x', i=1, b=TRUE, d='2024-01-01', "
                    + "ts='2024-01-01 12:00:00', f=1.5, "
                    + "u='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', "
                    + "ba='\\xCAFE', j='{\"a\":1}' "
                    + "WHERE t IS NULL AND i IS NULL");
            rs(s, "SELECT count(*) FROM nrt_all WHERE t IS NULL", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });

            // Update back to NULLs
            s.execute("UPDATE nrt_all SET t=NULL, i=NULL, b=NULL, d=NULL, "
                    + "ts=NULL, f=NULL, u=NULL, ba=NULL, j=NULL "
                    + "WHERE t = 'x'");
            rs(s, "SELECT count(*) FROM nrt_all WHERE t IS NULL AND i IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });

            // Delete NULL row
            s.execute("DELETE FROM nrt_all WHERE t IS NULL AND i IS NULL");
            rs(s, "SELECT count(*) FROM nrt_all", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
        }
    }

    // ==============================================================
    // NULL in CASE expressions with indexed columns
    // ==============================================================

    @Test
    void nullInCaseExpression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_case (id SERIAL PRIMARY KEY, val INTEGER UNIQUE)");
            s.execute("INSERT INTO nrt_case (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_case (val) VALUES (0)");
            s.execute("INSERT INTO nrt_case (val) VALUES (1)");

            // CASE with NULL: NULL = NULL doesn't match in CASE WHEN
            rs(s, "SELECT CASE val WHEN NULL THEN 'matched_null' ELSE 'no_match' END FROM nrt_case WHERE id = 1", rs -> {
                rs.next();
                // val is NULL but CASE val WHEN NULL uses = comparison, so it's 'no_match'
                assertEquals("no_match", rs.getString(1));
            });

            // CASE with IS NULL in WHEN
            rs(s, "SELECT CASE WHEN val IS NULL THEN 'is_null' WHEN val = 0 THEN 'zero' ELSE 'other' END "
                    + "FROM nrt_case ORDER BY id", rs -> {
                rs.next(); assertEquals("is_null", rs.getString(1));
                rs.next(); assertEquals("zero", rs.getString(1));
                rs.next(); assertEquals("other", rs.getString(1));
            });
        }
    }

    // ==============================================================
    // NULL in IN/NOT IN lists
    // ==============================================================

    @Test
    void nullInInList() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_in (id SERIAL PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO nrt_in (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_in (val) VALUES ('a')");
            s.execute("INSERT INTO nrt_in (val) VALUES ('b')");
            s.execute("INSERT INTO nrt_in (val) VALUES ('c')");

            // IN with NULL: NULL IN ('a', NULL) is NULL, not TRUE
            rs(s, "SELECT count(*) FROM nrt_in WHERE val IN ('a', NULL)", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1)); // only 'a', not the NULL row
            });

            // NOT IN with NULL: val NOT IN ('a', NULL) is NULL for all non-'a' rows
            // PG returns 0 rows because NULL NOT IN (...) is NULL, and 'b' NOT IN ('a', NULL) is also NULL
            rs(s, "SELECT count(*) FROM nrt_in WHERE val NOT IN ('a', NULL)", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1)); // PG three-valued logic
            });
        }
    }

    // ==============================================================
    // Transaction rollback with NULL operations
    // ==============================================================

    @Test
    void rollbackNullUpdate() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nrt_rb (id SERIAL PRIMARY KEY, val TEXT UNIQUE)");
            s.execute("INSERT INTO nrt_rb (val) VALUES (NULL)");
            s.execute("INSERT INTO nrt_rb (val) VALUES ('keep')");
            conn.commit();

            // Update NULL to 'temp' then rollback
            s.execute("UPDATE nrt_rb SET val = 'temp' WHERE val IS NULL");
            rs(s, "SELECT count(*) FROM nrt_rb WHERE val = 'temp'", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            conn.rollback();

            // Should be back to NULL
            rs(s, "SELECT count(*) FROM nrt_rb WHERE val IS NULL", rs -> {
                rs.next(); assertEquals(1, rs.getInt(1));
            });
            rs(s, "SELECT count(*) FROM nrt_rb WHERE val = 'temp'", rs -> {
                rs.next(); assertEquals(0, rs.getInt(1));
            });
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // Helper to run a query and process the result set
    private void rs(Statement s, String sql, ResultSetConsumer consumer) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            consumer.accept(rs);
        }
    }

    @FunctionalInterface
    interface ResultSetConsumer {
        void accept(ResultSet rs) throws SQLException;
    }
}
