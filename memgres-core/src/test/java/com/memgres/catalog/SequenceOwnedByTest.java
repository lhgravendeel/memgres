package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for M20: ALTER SEQUENCE OWNED BY tracking.
 */
class SequenceOwnedByTest {

    static Memgres memgres;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    @Test
    void ownedBy_pgGetSerialSequence() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m20_t(id int, val text)");
            st.execute("CREATE SEQUENCE m20_seq");
            st.execute("ALTER SEQUENCE m20_seq OWNED BY m20_t.id");
            try (ResultSet rs = st.executeQuery("SELECT pg_get_serial_sequence('m20_t', 'id')")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertNotNull(result, "pg_get_serial_sequence should find OWNED BY sequence");
                assertTrue(result.contains("m20_seq"), "Should contain seq name: " + result);
            } finally {
                st.execute("DROP TABLE m20_t CASCADE");
                st.execute("DROP SEQUENCE IF EXISTS m20_seq");
            }
        }
    }

    @Test
    void ownedBy_none_clears() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m20_n(id int)");
            st.execute("CREATE SEQUENCE m20_nseq");
            st.execute("ALTER SEQUENCE m20_nseq OWNED BY m20_n.id");
            // Verify it's set
            try (ResultSet rs = st.executeQuery("SELECT pg_get_serial_sequence('m20_n', 'id')")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
            // Clear with OWNED BY NONE
            st.execute("ALTER SEQUENCE m20_nseq OWNED BY NONE");
            try (ResultSet rs = st.executeQuery("SELECT pg_get_serial_sequence('m20_n', 'id')")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1), "After OWNED BY NONE, pg_get_serial_sequence should return NULL");
            }
            st.execute("DROP TABLE m20_n");
            st.execute("DROP SEQUENCE m20_nseq");
        }
    }

    @Test
    void ownedBy_pgDepend_row() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m20_d(id int, name text)");
            st.execute("CREATE SEQUENCE m20_dseq");
            st.execute("ALTER SEQUENCE m20_dseq OWNED BY m20_d.name");
            try (ResultSet rs = st.executeQuery(
                    "SELECT d.deptype FROM pg_depend d " +
                    "JOIN pg_class sc ON sc.oid = d.objid " +
                    "JOIN pg_class tc ON tc.oid = d.refobjid " +
                    "WHERE sc.relname = 'm20_dseq' AND tc.relname = 'm20_d'")) {
                assertTrue(rs.next(), "Should have pg_depend row for OWNED BY");
                assertEquals("a", rs.getString("deptype"));
            } finally {
                st.execute("DROP TABLE m20_d CASCADE");
                st.execute("DROP SEQUENCE IF EXISTS m20_dseq");
            }
        }
    }

    @Test
    void identity_deptype_i() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m20_id(id int GENERATED ALWAYS AS IDENTITY)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT d.deptype FROM pg_depend d " +
                    "JOIN pg_class sc ON sc.oid = d.objid " +
                    "JOIN pg_class tc ON tc.oid = d.refobjid " +
                    "WHERE sc.relname = 'm20_id_id_seq' AND tc.relname = 'm20_id'")) {
                assertTrue(rs.next(), "Should have pg_depend row for identity sequence");
                assertEquals("i", rs.getString("deptype"), "Identity sequences should have deptype 'i'");
            } finally {
                st.execute("DROP TABLE m20_id");
            }
        }
    }

    @Test
    void ownedBy_invalidColumn_errors() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m20_e(id int)");
            st.execute("CREATE SEQUENCE m20_eseq");
            try {
                st.execute("ALTER SEQUENCE m20_eseq OWNED BY m20_e.nonexistent");
                fail("Should throw error for nonexistent column");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("does not exist"));
            } finally {
                st.execute("DROP TABLE m20_e");
                st.execute("DROP SEQUENCE m20_eseq");
            }
        }
    }
}
