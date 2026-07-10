package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bug 9 (mtask-6): NUMERIC(p,s) declared scale must be preserved on output.
 *
 * PostgreSQL always emits a NUMERIC(p,s) column's value at its declared scale: inserting 10 into
 * a NUMERIC(10,2) column and selecting it back yields the text {@code 10.00} and
 * {@code getBigDecimal().scale() == 2}. memgres previously echoed whatever scale the stored
 * BigDecimal happened to carry, so the same round-trip produced {@code 10.0} (failing e.g.
 * {@code InstallationDaoTest.testInsertAndUpdate}'s
 * {@code assertEquals(new BigDecimal("10.00"), inserted.getCapacity())}).
 *
 * Exercised over the default pgjdbc connection (extended protocol with bound parameters — the
 * jdbi shape used by the real app) plus literal inserts, and asserts on both the text form and
 * the BigDecimal scale.
 */
class NumericScalePreservationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
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

    /** The acceptance case: literal inserts of 10 / 10.5 / 10.25 into NUMERIC(10,2). */
    @Test
    void literal_inserts_round_trip_at_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_lit (id int PRIMARY KEY, capacity numeric(10, 2))");
        exec("INSERT INTO nsp_lit VALUES (1, 10), (2, 10.5), (3, 10.25)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, capacity FROM nsp_lit ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("10.00", rs.getString("capacity"));
            assertEquals(new BigDecimal("10.00"), rs.getBigDecimal("capacity"));
            assertEquals(2, rs.getBigDecimal("capacity").scale());
            assertTrue(rs.next());
            assertEquals("10.50", rs.getString("capacity"));
            assertEquals(new BigDecimal("10.50"), rs.getBigDecimal("capacity"));
            assertTrue(rs.next());
            assertEquals("10.25", rs.getString("capacity"));
            assertEquals(new BigDecimal("10.25"), rs.getBigDecimal("capacity"));
        } finally {
            exec("DROP TABLE IF EXISTS nsp_lit");
        }
    }

    /** The jdbi/app shape: values bound as prepared-statement parameters (extended protocol). */
    @Test
    void bound_parameter_inserts_round_trip_at_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_bind (id int PRIMARY KEY, capacity numeric(10, 2))");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO nsp_bind VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, new BigDecimal("10"));
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setBigDecimal(2, new BigDecimal("10.5"));
            ps.executeUpdate();
            ps.setInt(1, 3);
            ps.setInt(2, 7); // integer-typed bind into a numeric column
            ps.executeUpdate();
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, capacity FROM nsp_bind ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("10.00", rs.getString("capacity"));
            assertEquals(2, rs.getBigDecimal("capacity").scale());
            assertTrue(rs.next());
            assertEquals("10.50", rs.getString("capacity"));
            assertEquals(2, rs.getBigDecimal("capacity").scale());
            assertTrue(rs.next());
            assertEquals("7.00", rs.getString("capacity"));
            assertEquals(2, rs.getBigDecimal("capacity").scale());
        } finally {
            exec("DROP TABLE IF EXISTS nsp_bind");
        }
    }

    /** UPDATE must also land at the declared scale. */
    @Test
    void update_lands_at_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_upd (id int PRIMARY KEY, capacity numeric(10, 2))");
        exec("INSERT INTO nsp_upd VALUES (1, 1.25)");
        try (PreparedStatement ps = conn.prepareStatement("UPDATE nsp_upd SET capacity = ? WHERE id = ?")) {
            ps.setBigDecimal(1, new BigDecimal("12.5"));
            ps.setInt(2, 1);
            ps.executeUpdate();
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT capacity FROM nsp_upd WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("12.50", rs.getString(1));
            assertEquals(2, rs.getBigDecimal(1).scale());
        } finally {
            exec("DROP TABLE IF EXISTS nsp_upd");
        }
    }

    /** Values with excess scale are rounded (PG rounds half away from zero) to the declared scale. */
    @Test
    void excess_scale_rounds_to_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_round (id int PRIMARY KEY, capacity numeric(10, 2))");
        exec("INSERT INTO nsp_round VALUES (1, 10.255), (2, 10.254)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT capacity FROM nsp_round ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("10.26", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("10.25", rs.getString(1));
        } finally {
            exec("DROP TABLE IF EXISTS nsp_round");
        }
    }

    /**
     * The real app path: installations.capacity got its NUMERIC(10,2) type via
     * {@code ALTER TABLE ... ALTER COLUMN capacity TYPE numeric(10, 2)} (V18 migration).
     * memgres previously stripped the typmod in DdlAlterTableExecutor.executeSetType and kept
     * the old column's (null) precision/scale, so storage coercion never enforced scale 2 and
     * a jdbi round-trip returned 10.0 instead of 10.00
     * (InstallationDaoTest.testInsertAndUpdate's failure).
     */
    @Test
    void alter_column_type_applies_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_alter (id int PRIMARY KEY, capacity numeric)");
        exec("ALTER TABLE nsp_alter ALTER COLUMN capacity TYPE numeric(10, 2)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO nsp_alter VALUES (?, ?) RETURNING capacity")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, new BigDecimal("10.0"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("10.00", rs.getString(1),
                        "capacity typed via ALTER COLUMN TYPE numeric(10,2) must emit scale 2");
                assertEquals(new BigDecimal("10.00"), rs.getBigDecimal(1));
                assertEquals(2, rs.getBigDecimal(1).scale());
            }
        } finally {
            exec("DROP TABLE IF EXISTS nsp_alter");
        }
    }

    /**
     * Conversely, ALTER COLUMN TYPE numeric (no typmod) must REMOVE a previously declared
     * scale, matching PostgreSQL where the new type spec fully replaces the old one.
     */
    @Test
    void alter_column_type_without_typmod_clears_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_clear (id int PRIMARY KEY, v numeric(10, 2))");
        exec("ALTER TABLE nsp_clear ALTER COLUMN v TYPE numeric");
        exec("INSERT INTO nsp_clear VALUES (1, 10.005)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT v FROM nsp_clear WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("10.005", rs.getString(1),
                    "unconstrained numeric must keep the value's own scale, not round to 2");
        } finally {
            exec("DROP TABLE IF EXISTS nsp_clear");
        }
    }

    /** RETURNING must expose the value at the declared scale as well. */
    @Test
    void insert_returning_yields_declared_scale() throws SQLException {
        exec("CREATE TABLE nsp_ret (id int PRIMARY KEY, capacity numeric(10, 2))");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO nsp_ret VALUES (?, ?) RETURNING capacity")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, new BigDecimal("10"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("10.00", rs.getString(1));
                assertEquals(2, rs.getBigDecimal(1).scale());
            }
        } finally {
            exec("DROP TABLE IF EXISTS nsp_ret");
        }
    }
}
