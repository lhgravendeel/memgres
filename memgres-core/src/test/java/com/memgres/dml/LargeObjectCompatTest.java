package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class LargeObjectCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_roundtrip() RETURNS text AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'Hello World'::bytea);\n" +
                "  result := lo_get(loid);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_put_test() RETURNS text AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, ''::bytea);\n" +
                "  PERFORM lo_put(loid, 0, 'Hello PG18!'::bytea);\n" +
                "  result := lo_get(loid);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_get_slice() RETURNS text AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'Hello World'::bytea);\n" +
                "  result := lo_get(loid, 0, 5);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_fd_test() RETURNS text AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  fd integer;\n" +
                "  result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'Test data'::bytea);\n" +
                "  fd := lo_open(loid, x'20000'::int);\n" +
                "  result := loread(fd, 9);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_seek_test() RETURNS text AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  fd integer;\n" +
                "  result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'Hello World'::bytea);\n" +
                "  fd := lo_open(loid, x'20000'::int);\n" +
                "  PERFORM lo_lseek(fd, 6, 0);\n" +
                "  result := loread(fd, 5);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_truncate_test() RETURNS text AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  fd integer;\n" +
                "  result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'Hello World'::bytea);\n" +
                "  fd := lo_open(loid, x'60000'::int);\n" +
                "  PERFORM lo_truncate(fd, 5);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  result := lo_get(loid);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_seek_tell_test() RETURNS text AS $$\n" +
                "DECLARE loid oid; fd integer; pos integer; result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'ABCDEFGH'::bytea);\n" +
                "  fd := lo_open(loid, x'20000'::int);\n" +
                "  PERFORM lo_lseek(fd, 5, 0);\n" +
                "  pos := lo_tell(fd);\n" +
                "  result := loread(fd, 3);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN pos::text || ':' || convert_from(result, 'UTF8');\n" +
                "END; $$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_write_roundtrip() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "DECLARE loid oid; fd integer; data bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_creat(-1);\n" +
                "  fd := lo_open(loid, x'20000'::integer);\n" +
                "  PERFORM lowrite(fd, 'Test data'::bytea);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  fd := lo_open(loid, x'40000'::integer);\n" +
                "  data := loread(fd, 9);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(data, 'UTF8');\n" +
                "END; $$");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_metadata_test() RETURNS boolean AS $$\n" +
                "DECLARE\n" +
                "  loid oid;\n" +
                "  found boolean;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'metadata test'::bytea);\n" +
                "  SELECT EXISTS(SELECT 1 FROM pg_largeobject_metadata WHERE oid = loid) INTO found;\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN found;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("lo_from_bytea + lo_get roundtrip should return original text")
    void lo_roundtrip() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_roundtrip() AS result")) {
            assertTrue(rs.next());
            assertEquals("Hello World", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("lo_put should write data retrievable via lo_get")
    void lo_put_test() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_put_test() AS result")) {
            assertTrue(rs.next());
            assertEquals("Hello PG18!", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("lo_get with offset and length should return a slice")
    void lo_get_slice() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_get_slice() AS result")) {
            assertTrue(rs.next());
            assertEquals("Hello", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("lo_open + loread + lo_close should read large object via fd")
    void lo_fd_test() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_fd_test() AS result")) {
            assertTrue(rs.next());
            assertEquals("Test data", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("lo_open + lo_lseek + loread should seek and read from offset")
    void lo_seek_test() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_seek_test() AS result")) {
            assertTrue(rs.next());
            assertEquals("World", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("lo_open + lo_truncate should truncate large object")
    void lo_truncate_test() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_truncate_test() AS result")) {
            assertTrue(rs.next());
            assertEquals("Hello", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("pg_largeobject_metadata should contain entry after lo_from_bytea")
    void lo_metadata_test() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_metadata_test() AS result")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test
    @DisplayName("lo_tell should report current position after lo_lseek")
    void lo_seek_tell_test() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_seek_tell_test() AS result")) {
            assertTrue(rs.next());
            assertEquals("5:FGH", rs.getString("result"),
                    "lo_tell should return 5 after lseek to 5, loread should return 'FGH'");
        }
    }

    @Test
    @DisplayName("lowrite persists data through close/reopen cycle")
    void lo_write_roundtrip() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_write_roundtrip() AS result")) {
            assertTrue(rs.next());
            assertEquals("Test data", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("lo_unlink on nonexistent OID should throw SQLException")
    void lo_unlink_nonexistent() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT lo_unlink(999999999)");
            }
        });
        assertEquals("42704", ex.getSQLState());
    }
}
