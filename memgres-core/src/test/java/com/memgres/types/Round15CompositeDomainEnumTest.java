package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category C (types): composite types, domains, enums.
 *
 * Covers:
 *  - ALTER TYPE … ADD / DROP / ALTER ATTRIBUTE
 *  - ROW(…)::composite cast
 *  - enum_cmp / enum_lt / enum_first / enum_last / enum_range
 *  - ALTER DOMAIN VALIDATE / RENAME CONSTRAINT
 *  - Domain constraint enforcement on columns of that domain type
 */
class Round15CompositeDomainEnumTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. ALTER TYPE … ADD / DROP / ALTER / RENAME ATTRIBUTE
    // =========================================================================

    @Test
    void alter_type_add_attribute() throws SQLException {
        exec("CREATE TYPE r15_comp_add AS (a int, b text)");
        exec("ALTER TYPE r15_comp_add ADD ATTRIBUTE c double precision");
        int n = scalarInt("SELECT count(*)::int FROM pg_attribute a "
                + "JOIN pg_type t ON a.attrelid = t.typrelid "
                + "WHERE t.typname='r15_comp_add' AND a.attname='c'");
        assertEquals(1, n, "ALTER TYPE ADD ATTRIBUTE must create a new column");
    }

    @Test
    void alter_type_drop_attribute() throws SQLException {
        exec("CREATE TYPE r15_comp_drop AS (a int, b text, c int)");
        exec("ALTER TYPE r15_comp_drop DROP ATTRIBUTE b");
        int n = scalarInt("SELECT count(*)::int FROM pg_attribute a "
                + "JOIN pg_type t ON a.attrelid = t.typrelid "
                + "WHERE t.typname='r15_comp_drop' AND a.attname='b' AND NOT a.attisdropped");
        assertEquals(0, n, "ALTER TYPE DROP ATTRIBUTE must remove the column");
    }

    @Test
    void alter_type_alter_attribute_type() throws SQLException {
        exec("CREATE TYPE r15_comp_alt AS (a int, b text)");
        exec("ALTER TYPE r15_comp_alt ALTER ATTRIBUTE a TYPE bigint");
        String ty = scalarString(
                "SELECT t2.typname FROM pg_attribute a "
                        + "JOIN pg_type t ON a.attrelid = t.typrelid "
                        + "JOIN pg_type t2 ON a.atttypid = t2.oid "
                        + "WHERE t.typname='r15_comp_alt' AND a.attname='a'");
        assertTrue(ty.equals("int8") || ty.equals("bigint"),
                "ALTER ATTRIBUTE TYPE bigint should update attr type; got " + ty);
    }

    @Test
    void alter_type_rename_attribute() throws SQLException {
        exec("CREATE TYPE r15_comp_ren AS (a int, b text)");
        exec("ALTER TYPE r15_comp_ren RENAME ATTRIBUTE b TO c");
        int n = scalarInt("SELECT count(*)::int FROM pg_attribute a "
                + "JOIN pg_type t ON a.attrelid = t.typrelid "
                + "WHERE t.typname='r15_comp_ren' AND a.attname='c'");
        assertEquals(1, n, "RENAME ATTRIBUTE must update attname");
    }

    // =========================================================================
    // B. ROW(…)::composite cast
    // =========================================================================

    @Test
    void row_literal_cast_to_composite() throws SQLException {
        exec("CREATE TYPE r15_rowct AS (a int, b text)");
        String v = scalarString("SELECT (ROW(1, 'x')::r15_rowct)::text");
        assertNotNull(v);
        assertTrue(v.contains("1") && v.contains("x"),
                "ROW(1,'x')::r15_rowct should format as '(1,x)'; got " + v);
    }

    @Test
    void row_value_extract_field() throws SQLException {
        exec("CREATE TYPE r15_rowfd AS (a int, b text)");
        int a = scalarInt("SELECT ((ROW(10, 'y')::r15_rowfd)).a");
        assertEquals(10, a, "composite field selection must work");
    }

    // =========================================================================
    // C. Enum functions
    // =========================================================================

    @Test
    void enum_cmp_returns_ordering() throws SQLException {
        exec("CREATE TYPE r15_en AS ENUM ('a','b','c')");
        int r1 = scalarInt("SELECT enum_cmp('a'::r15_en, 'b'::r15_en)::int");
        int r2 = scalarInt("SELECT enum_cmp('b'::r15_en, 'a'::r15_en)::int");
        int r3 = scalarInt("SELECT enum_cmp('a'::r15_en, 'a'::r15_en)::int");
        assertTrue(r1 < 0, "enum_cmp(a,b) should be negative");
        assertTrue(r2 > 0, "enum_cmp(b,a) should be positive");
        assertEquals(0, r3);
    }

    @Test
    void enum_first_and_last() throws SQLException {
        exec("CREATE TYPE r15_enfl AS ENUM ('low','mid','high')");
        String first = scalarString("SELECT enum_first(NULL::r15_enfl)::text");
        String last = scalarString("SELECT enum_last(NULL::r15_enfl)::text");
        assertEquals("low", first);
        assertEquals("high", last);
    }

    @Test
    void enum_range_full() throws SQLException {
        exec("CREATE TYPE r15_enr AS ENUM ('x','y','z')");
        String v = scalarString("SELECT enum_range(NULL::r15_enr)::text");
        assertNotNull(v);
        assertTrue(v.contains("x") && v.contains("y") && v.contains("z"),
                "enum_range(NULL::T) should return all enum values; got " + v);
    }

    @Test
    void enum_range_bounded() throws SQLException {
        exec("CREATE TYPE r15_enrb AS ENUM ('w','x','y','z')");
        String v = scalarString("SELECT enum_range('x'::r15_enrb, 'y'::r15_enrb)::text");
        assertNotNull(v);
        assertTrue(v.contains("x") && v.contains("y"),
                "enum_range(x,y) should contain exactly x,y; got " + v);
        assertFalse(v.contains("z"), "Bounded enum_range should exclude z; got " + v);
    }

    // =========================================================================
    // D. ALTER DOMAIN VALIDATE / RENAME CONSTRAINT
    // =========================================================================

    @Test
    void alter_domain_rename_constraint() throws SQLException {
        exec("CREATE DOMAIN r15_dom_rc AS int CONSTRAINT r15_dc_pos CHECK (VALUE > 0)");
        exec("ALTER DOMAIN r15_dom_rc RENAME CONSTRAINT r15_dc_pos TO r15_dc_positive");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_constraint c "
                        + "JOIN pg_type t ON c.contypid = t.oid "
                        + "WHERE t.typname='r15_dom_rc' AND c.conname='r15_dc_positive'");
        assertEquals(1, n, "RENAME CONSTRAINT must update conname in pg_constraint");
    }

    @Test
    void alter_domain_validate_constraint() throws SQLException {
        exec("CREATE DOMAIN r15_dom_v AS int");
        // NOT VALID allowed on ADD CHECK
        exec("ALTER DOMAIN r15_dom_v ADD CONSTRAINT r15_dv_pos CHECK (VALUE > 0) NOT VALID");

        // Should initially be marked not-validated
        int nNotValid = scalarInt(
                "SELECT count(*)::int FROM pg_constraint c "
                        + "JOIN pg_type t ON c.contypid = t.oid "
                        + "WHERE t.typname='r15_dom_v' AND c.conname='r15_dv_pos' AND NOT c.convalidated");
        assertTrue(nNotValid >= 1, "NOT VALID constraint should be unvalidated");

        exec("ALTER DOMAIN r15_dom_v VALIDATE CONSTRAINT r15_dv_pos");

        int nValid = scalarInt(
                "SELECT count(*)::int FROM pg_constraint c "
                        + "JOIN pg_type t ON c.contypid = t.oid "
                        + "WHERE t.typname='r15_dom_v' AND c.conname='r15_dv_pos' AND c.convalidated");
        assertEquals(1, nValid, "VALIDATE CONSTRAINT must flip convalidated=true");
    }

    // =========================================================================
    // E. Domain constraint enforcement on columns
    // =========================================================================

    @Test
    void domain_check_enforced_on_column_insert() throws SQLException {
        exec("CREATE DOMAIN r15_dom_e AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE r15_dom_col (id int, v r15_dom_e)");
        // Valid insert
        exec("INSERT INTO r15_dom_col VALUES (1, 5)");
        // Invalid insert must reject
        try {
            exec("INSERT INTO r15_dom_col VALUES (2, -1)");
            fail("Domain CHECK (VALUE > 0) should reject -1 on column");
        } catch (SQLException e) {
            // expected: sqlstate 23514 check_violation
            assertNotNull(e.getMessage());
        }
    }

    @Test
    void domain_check_enforced_on_column_update() throws SQLException {
        exec("CREATE DOMAIN r15_dom_upd AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE r15_dom_u (id int, v r15_dom_upd)");
        exec("INSERT INTO r15_dom_u VALUES (1, 5)");
        try {
            exec("UPDATE r15_dom_u SET v=-5 WHERE id=1");
            fail("Domain CHECK must reject UPDATE with invalid value");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    void domain_not_null() throws SQLException {
        exec("CREATE DOMAIN r15_dom_nn AS int NOT NULL");
        exec("CREATE TABLE r15_dom_nn_c (v r15_dom_nn)");
        try {
            exec("INSERT INTO r15_dom_nn_c VALUES (NULL)");
            fail("Domain NOT NULL must reject NULL insert");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }
}
