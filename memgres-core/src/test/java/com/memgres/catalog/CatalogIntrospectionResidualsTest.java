package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual catalog / introspection bugs from bugs-review.md: H14, H15, H16,
 * M14, M15, M19, M21, M22, L12, L13. Each assertion is the exact PostgreSQL 18
 * output, verified against a live server.
 */
class CatalogIntrospectionResidualsTest {

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

    private String scalar(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ===================== H14: information_schema.columns =====================

    @Test
    void h14_compositeColumnIsUserDefined() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TYPE h14_addr AS (street text, zip int)");
            st.execute("CREATE TABLE h14_ct (id int, home h14_addr)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT data_type, udt_name FROM information_schema.columns " +
                    "WHERE table_name='h14_ct' AND column_name='home'")) {
                assertTrue(rs.next());
                assertEquals("USER-DEFINED", rs.getString("data_type"));
                assertEquals("h14_addr", rs.getString("udt_name"));
            } finally {
                st.execute("DROP TABLE h14_ct");
                st.execute("DROP TYPE h14_addr");
            }
        }
    }

    @Test
    void h14_domainColumnReportsBaseType() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE DOMAIN h14_posint AS integer CHECK (VALUE > 0)");
            st.execute("CREATE TABLE h14_dt (id int, q h14_posint)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT data_type, udt_name, domain_name FROM information_schema.columns " +
                    "WHERE table_name='h14_dt' AND column_name='q'")) {
                assertTrue(rs.next());
                assertEquals("integer", rs.getString("data_type"));
                assertEquals("int4", rs.getString("udt_name"));
                assertEquals("h14_posint", rs.getString("domain_name"));
            } finally {
                st.execute("DROP TABLE h14_dt");
                st.execute("DROP DOMAIN h14_posint");
            }
        }
    }

    @Test
    void h14_nowDefaultRenderedAsNow() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_now (ts timestamptz DEFAULT now())");
            try {
                assertEquals("now()", scalar(st,
                        "SELECT column_default FROM information_schema.columns " +
                        "WHERE table_name='h14_now' AND column_name='ts'"));
            } finally {
                st.execute("DROP TABLE h14_now");
            }
        }
    }

    @Test
    void h14_currentTimestampKeywordUnchanged() throws Exception {
        // The CURRENT_TIMESTAMP keyword still renders as CURRENT_TIMESTAMP (not now()).
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_ct2 (ts timestamptz DEFAULT CURRENT_TIMESTAMP)");
            try {
                assertEquals("CURRENT_TIMESTAMP", scalar(st,
                        "SELECT column_default FROM information_schema.columns " +
                        "WHERE table_name='h14_ct2' AND column_name='ts'"));
            } finally {
                st.execute("DROP TABLE h14_ct2");
            }
        }
    }

    // ===================== H15: check_constraints.check_clause =================

    @Test
    void h15_notNullRowsInCheckConstraints() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h15_nn (id int, name text NOT NULL)");
            try {
                assertEquals("name IS NOT NULL", scalar(st,
                        "SELECT check_clause FROM information_schema.check_constraints " +
                        "WHERE constraint_name='h15_nn_name_not_null'"));
            } finally {
                st.execute("DROP TABLE h15_nn");
            }
        }
    }

    @Test
    void h15_domainCheckRow() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE DOMAIN h15_pos AS integer CHECK (VALUE > 0)");
            try {
                assertEquals("(VALUE > 0)", scalar(st,
                        "SELECT check_clause FROM information_schema.check_constraints " +
                        "WHERE constraint_name='h15_pos_check'"));
            } finally {
                st.execute("DROP DOMAIN h15_pos");
            }
        }
    }

    // ===================== H16: pg_get_constraintdef ===========================

    @Test
    void h16_notNullConstraintDef() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h16_nn (id int, name text NOT NULL)");
            try {
                assertEquals("NOT NULL name", scalar(st,
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                        "WHERE conname='h16_nn_name_not_null'"));
            } finally {
                st.execute("DROP TABLE h16_nn");
            }
        }
    }

    @Test
    void h16_domainCheckConstraintDef() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE DOMAIN h16_pos AS integer CHECK (VALUE > 0)");
            try {
                assertEquals("CHECK ((VALUE > 0))", scalar(st,
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                        "WHERE conname='h16_pos_check'"));
            } finally {
                st.execute("DROP DOMAIN h16_pos");
            }
        }
    }

    @Test
    void h16_fkNotSchemaQualifiedUnderSearchPath() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h16_parent (id int PRIMARY KEY)");
            st.execute("CREATE TABLE h16_child (id int, pid int REFERENCES h16_parent(id))");
            try {
                assertEquals("FOREIGN KEY (pid) REFERENCES h16_parent(id)", scalar(st,
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                        "WHERE conname='h16_child_pid_fkey'"));
            } finally {
                st.execute("DROP TABLE h16_child");
                st.execute("DROP TABLE h16_parent");
            }
        }
    }

    // ===================== M14: identity has no pg_attrdef =====================

    @Test
    void m14_identityHasNoAttrdef() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m14_t (id int GENERATED ALWAYS AS IDENTITY, v int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT a.atthasdef, d.adnum IS NOT NULL AS hasrow " +
                    "FROM pg_attribute a LEFT JOIN pg_attrdef d " +
                    "  ON d.adrelid=a.attrelid AND d.adnum=a.attnum " +
                    "WHERE a.attrelid='m14_t'::regclass AND a.attname='id'")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("atthasdef"), "identity column must have atthasdef=false");
                assertFalse(rs.getBoolean("hasrow"), "identity column must have no pg_attrdef row");
            } finally {
                st.execute("DROP TABLE m14_t");
            }
        }
    }

    @Test
    void m14_serialStillHasAttrdef() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m14_s (id serial)");
            try {
                assertEquals("t", scalar(st,
                        "SELECT atthasdef FROM pg_attribute " +
                        "WHERE attrelid='m14_s'::regclass AND attname='id'"));
            } finally {
                st.execute("DROP TABLE m14_s");
            }
        }
    }

    // ===================== M15: regclass / regtype / regproc ===================

    @Test
    void m15_oidRegtypeReturnsUserTypeName() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TYPE m15_addr AS (a int)");
            try {
                assertEquals("m15_addr", scalar(st,
                        "SELECT (SELECT oid FROM pg_type WHERE typname='m15_addr')::regtype::text"));
            } finally {
                st.execute("DROP TYPE m15_addr");
            }
        }
    }

    @Test
    void m15_regclassQuotesMixedCase() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE \"M15Mixed\" (x int)");
            try {
                assertEquals("\"M15Mixed\"", scalar(st,
                        "SELECT (SELECT oid FROM pg_class WHERE relname='M15Mixed')::regclass::text"));
            } finally {
                st.execute("DROP TABLE \"M15Mixed\"");
            }
        }
    }

    @Test
    void m15_ambiguousRegprocErrors() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT 'lower'::regproc"));
            assertEquals("42725", ex.getSQLState());
        }
    }

    // ===================== M19: view definition ================================

    @Test
    void m19_oneArgViewdefIsPretty() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m19_t (id int, name text)");
            st.execute("CREATE VIEW m19_v AS SELECT id, name FROM m19_t");
            try {
                assertEquals(" SELECT id,\n    name\n   FROM m19_t;", scalar(st,
                        "SELECT pg_get_viewdef('m19_v'::regclass)"));
                assertEquals(" SELECT id,\n    name\n   FROM m19_t;", scalar(st,
                        "SELECT definition FROM pg_views WHERE viewname='m19_v'"));
                assertEquals(" SELECT id,\n    name\n   FROM m19_t;", scalar(st,
                        "SELECT view_definition FROM information_schema.views WHERE table_name='m19_v'"));
            } finally {
                st.execute("DROP VIEW m19_v");
                st.execute("DROP TABLE m19_t");
            }
        }
    }

    @Test
    void m19_ruledefForView() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m19_rt (id int, name text)");
            st.execute("CREATE VIEW m19_rv AS SELECT id, name FROM m19_rt");
            try {
                assertEquals(
                        "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO public.m19_rv DO INSTEAD "
                        + " SELECT id,\n    name\n   FROM m19_rt;",
                        scalar(st, "SELECT pg_get_ruledef(oid) FROM pg_rewrite " +
                                "WHERE ev_class='m19_rv'::regclass"));
            } finally {
                st.execute("DROP VIEW m19_rv");
                st.execute("DROP TABLE m19_rt");
            }
        }
    }

    // ===================== M21: information_schema gaps ========================

    @Test
    void m21_routineExternalLanguageUppercase() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE FUNCTION m21_add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a+b'");
            try {
                assertEquals("SQL", scalar(st,
                        "SELECT external_language FROM information_schema.routines " +
                        "WHERE routine_name='m21_add'"));
            } finally {
                st.execute("DROP FUNCTION m21_add(int, int)");
            }
        }
    }

    @Test
    void m21_excludeNotInTableConstraints() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS btree_gist");
            st.execute("CREATE TABLE m21_ex (a int, b int, EXCLUDE USING gist (a WITH =))");
            try {
                assertEquals("0", scalar(st,
                        "SELECT count(*)::int FROM information_schema.table_constraints " +
                        "WHERE table_name='m21_ex' AND constraint_type='EXCLUDE'"));
            } finally {
                st.execute("DROP TABLE m21_ex");
            }
        }
    }

    // ===================== M22: pg_class / pg_tables ==========================

    @Test
    void m22_hastriggersForFkTables() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m22_parent (id int PRIMARY KEY)");
            st.execute("CREATE TABLE m22_child (id int, pid int REFERENCES m22_parent(id))");
            try {
                assertEquals("t", scalar(st,
                        "SELECT hastriggers FROM pg_tables WHERE tablename='m22_parent'"),
                        "FK-referenced parent has internal RI triggers");
                assertEquals("t", scalar(st,
                        "SELECT hastriggers FROM pg_tables WHERE tablename='m22_child'"),
                        "FK-referencing child has internal RI triggers");
            } finally {
                st.execute("DROP TABLE m22_child");
                st.execute("DROP TABLE m22_parent");
            }
        }
    }

    // ===================== L12: pg_settings TimeZone ==========================

    @Test
    void l12_timeZoneRowPresent() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            assertEquals("1", scalar(st,
                    "SELECT count(*)::int FROM pg_settings WHERE name='TimeZone'"));
        }
    }

    // ===================== L13: partition child NOT NULL name =================

    @Test
    void l13_partitionChildInheritsNotNullName() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE l13_pt (id int NOT NULL, r int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE l13_pt_a PARTITION OF l13_pt FOR VALUES FROM (1) TO (10)");
            try {
                // The child's NOT NULL constraint keeps the parent-derived name.
                assertEquals("1", scalar(st,
                        "SELECT count(*)::int FROM pg_constraint " +
                        "WHERE conrelid='l13_pt_a'::regclass AND contype='n' " +
                        "AND conname='l13_pt_id_not_null'"));
            } finally {
                st.execute("DROP TABLE l13_pt_a");
                st.execute("DROP TABLE l13_pt");
            }
        }
    }
}
