package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for catalog introspection fixes: H13-H17, M14-M22, L11-L13.
 */
class CatalogIntrospectionTest {

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

    // === H13: OPERATOR(pg_catalog.~) regex operators ===

    @Test
    void h13_qualifiedRegexMatch() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'hello' OPERATOR(pg_catalog.~) 'hel' AS m")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("m"));
            }
        }
    }

    @Test
    void h13_qualifiedRegexImatch() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'HELLO' OPERATOR(pg_catalog.~*) 'hel' AS m")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("m"));
            }
        }
    }

    @Test
    void h13_qualifiedNotRegexMatch() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'hello' OPERATOR(pg_catalog.!~) 'xyz' AS m")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("m"));
            }
        }
    }

    @Test
    void h13_qualifiedNotRegexImatch() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'HELLO' OPERATOR(pg_catalog.!~*) 'hel' AS m")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("m"));
            }
        }
    }

    @Test
    void h13_qualifiedLikeOperator() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'hello' OPERATOR(pg_catalog.~~) 'hel%' AS m")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("m"));
            }
        }
    }

    @Test
    void h13_qualifiedNotLikeOperator() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'hello' OPERATOR(pg_catalog.!~~) 'xyz%' AS m")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("m"));
            }
        }
    }

    // === H14: information_schema.columns ===

    @Test
    void h14_charMaxLength() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_t(name varchar(50))");
            try (ResultSet rs = st.executeQuery(
                    "SELECT character_maximum_length FROM information_schema.columns " +
                    "WHERE table_name='h14_t' AND column_name='name'")) {
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1));
            } finally {
                st.execute("DROP TABLE h14_t");
            }
        }
    }

    @Test
    void h14_udtNameForSerial() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_s(id serial)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT udt_name FROM information_schema.columns " +
                    "WHERE table_name='h14_s' AND column_name='id'")) {
                assertTrue(rs.next());
                assertEquals("int4", rs.getString(1));
            } finally {
                st.execute("DROP TABLE h14_s");
            }
        }
    }

    @Test
    void h14_columnDefaultNow() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_d(ts timestamp DEFAULT CURRENT_TIMESTAMP)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT column_default FROM information_schema.columns " +
                    "WHERE table_name='h14_d' AND column_name='ts'")) {
                assertTrue(rs.next());
                assertEquals("CURRENT_TIMESTAMP", rs.getString(1));
            } finally {
                st.execute("DROP TABLE h14_d");
            }
        }
    }

    @Test
    void h14_isIdentity() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_i(id int GENERATED ALWAYS AS IDENTITY)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT is_identity, identity_generation FROM information_schema.columns " +
                    "WHERE table_name='h14_i' AND column_name='id'")) {
                assertTrue(rs.next());
                assertEquals("YES", rs.getString("is_identity"));
                assertEquals("ALWAYS", rs.getString("identity_generation"));
            } finally {
                st.execute("DROP TABLE h14_i");
            }
        }
    }

    @Test
    void h14_arrayDataType() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h14_a(tags text[])");
            try (ResultSet rs = st.executeQuery(
                    "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_name='h14_a' AND column_name='tags'")) {
                assertTrue(rs.next());
                assertEquals("ARRAY", rs.getString(1));
            } finally {
                st.execute("DROP TABLE h14_a");
            }
        }
    }

    // === H15: check_constraints SQL output ===

    @Test
    void h15_checkClauseSql() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h15_t(val int CHECK (val > 0))");
            try (ResultSet rs = st.executeQuery(
                    "SELECT check_clause FROM information_schema.check_constraints " +
                    "WHERE constraint_name LIKE 'h15_t%'")) {
                assertTrue(rs.next());
                String clause = rs.getString(1);
                assertFalse(clause.contains("BinaryExpr"), "Should be SQL, not Java AST: " + clause);
                assertTrue(clause.contains(">"), "Should contain > operator: " + clause);
            } finally {
                st.execute("DROP TABLE h15_t");
            }
        }
    }

    // === H16: pg_get_indexdef UNIQUE ===

    @Test
    void h16_indexDefIncludesUnique() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h16_t(id int PRIMARY KEY)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_indexdef(i.indexrelid) AS def " +
                    "FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid " +
                    "WHERE c.relname = 'h16_t' AND i.indisprimary")) {
                assertTrue(rs.next());
                assertTrue(rs.getString("def").contains("UNIQUE"),
                        "PK index def should contain UNIQUE: " + rs.getString("def"));
            } finally {
                st.execute("DROP TABLE h16_t");
            }
        }
    }

    // === H17: pg_index.indkey int2vector format ===

    @Test
    void h17_indkeyInt2vectorFormat() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h17_t(id int PRIMARY KEY)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT indkey::text AS ik FROM pg_index i " +
                    "JOIN pg_class c ON c.oid = i.indrelid " +
                    "WHERE c.relname = 'h17_t'")) {
                assertTrue(rs.next());
                String indkey = rs.getString("ik");
                assertFalse(indkey.startsWith("{"), "indkey should be int2vector format, not array: " + indkey);
            } finally {
                st.execute("DROP TABLE h17_t");
            }
        }
    }

    // === M14: serial sequence bounds ===

    @Test
    void m14_serialSequenceMaxValue() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m14_t(id serial)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT maximum_value::bigint AS mv FROM information_schema.sequences " +
                    "WHERE sequence_name = 'm14_t_id_seq'")) {
                assertTrue(rs.next());
                assertEquals(2147483647L, rs.getLong("mv"));
            } finally {
                st.execute("DROP TABLE m14_t");
            }
        }
    }

    @Test
    void m14_identitySequenceExcluded() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m14_i(id int GENERATED ALWAYS AS IDENTITY)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::int AS cnt FROM information_schema.sequences " +
                    "WHERE sequence_name LIKE 'm14_i%'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt("cnt"),
                        "Identity sequences should not appear in information_schema.sequences");
            } finally {
                st.execute("DROP TABLE m14_i");
            }
        }
    }

    // === M19: count(*) in view definition ===

    @Test
    void m19_countStarInViewDef() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m19_t(id int)");
            st.execute("CREATE VIEW m19_v AS SELECT count(*) AS cnt FROM m19_t");
            try (ResultSet rs = st.executeQuery(
                    "SELECT definition FROM pg_views WHERE viewname = 'm19_v'")) {
                assertTrue(rs.next());
                String def = rs.getString(1);
                assertTrue(def.contains("count(*)"), "View def should contain count(*): " + def);
            } finally {
                st.execute("DROP VIEW m19_v");
                st.execute("DROP TABLE m19_t");
            }
        }
    }

    // === M21: constraint_column_usage FK referenced column ===

    @Test
    void m21_fkConstraintColumnUsage() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m21_p(id int PRIMARY KEY)");
            st.execute("CREATE TABLE m21_c(pid int REFERENCES m21_p(id))");
            try (ResultSet rs = st.executeQuery(
                    "SELECT table_name, column_name FROM information_schema.constraint_column_usage " +
                    "WHERE constraint_name LIKE '%pid_fkey%'")) {
                assertTrue(rs.next());
                assertEquals("m21_p", rs.getString("table_name"),
                        "FK constraint_column_usage should reference the parent table");
                assertEquals("id", rs.getString("column_name"));
            } finally {
                st.execute("DROP TABLE m21_c");
                st.execute("DROP TABLE m21_p");
            }
        }
    }

    @Test
    void m21_matviewExcludedFromTables() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m21_t(id int)");
            st.execute("CREATE MATERIALIZED VIEW m21_mv AS SELECT * FROM m21_t");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::int AS cnt FROM information_schema.tables " +
                    "WHERE table_name = 'm21_mv'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt("cnt"),
                        "Matviews should not appear in information_schema.tables");
            } finally {
                st.execute("DROP MATERIALIZED VIEW m21_mv");
                st.execute("DROP TABLE m21_t");
            }
        }
    }

    // === M22: pg_class reltuples, pg_tables flags ===

    @Test
    void m22_reltuplesMinusOne() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m22_t(id int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT reltuples FROM pg_class WHERE relname = 'm22_t'")) {
                assertTrue(rs.next());
                assertEquals(-1.0, rs.getDouble("reltuples"), 0.01);
            } finally {
                st.execute("DROP TABLE m22_t");
            }
        }
    }

    @Test
    void m22_pgTablesHasindexes() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE m22_idx(id int PRIMARY KEY)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT hasindexes FROM pg_tables WHERE tablename = 'm22_idx'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("hasindexes"));
            } finally {
                st.execute("DROP TABLE m22_idx");
            }
        }
    }

    // === L11: pg_enum fractional enumsortorder ===

    @Test
    void l11_enumSortorderFractional() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TYPE l11_mood AS ENUM ('sad', 'happy')");
            st.execute("ALTER TYPE l11_mood ADD VALUE 'neutral' BEFORE 'happy'");
            try (ResultSet rs = st.executeQuery(
                    "SELECT enumsortorder FROM pg_enum " +
                    "WHERE enumtypid = 'l11_mood'::regtype AND enumlabel = 'neutral'")) {
                assertTrue(rs.next());
                double order = rs.getDouble(1);
                assertNotEquals(Math.floor(order), order,
                        "enumsortorder should be fractional after ADD VALUE BEFORE: " + order);
            } finally {
                st.execute("DROP TYPE l11_mood");
            }
        }
    }

    // === L12: pg_settings, pg_toast ===

    @Test
    void l12_serverVersionNumVartype() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT vartype FROM pg_settings WHERE name = 'server_version_num'")) {
                assertTrue(rs.next());
                assertEquals("integer", rs.getString(1));
            }
        }
    }

    @Test
    void l12_pgToastInSchemata() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::int FROM information_schema.schemata WHERE schema_name = 'pg_toast'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}
