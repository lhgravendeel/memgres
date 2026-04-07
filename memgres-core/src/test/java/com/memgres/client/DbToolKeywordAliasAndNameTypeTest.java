package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DB tool compatibility fixes:
 * 1. Keywords (AT, TYPE, ROLE, etc.) allowed as bare table/column aliases
 * 2. NAME data type (OID 19) and ::name cast support
 */
class DbToolKeywordAliasAndNameTypeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text NOT NULL)");
            s.execute("CREATE TABLE orders (order_id serial PRIMARY KEY, user_id int REFERENCES users(user_id))");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========== Fix 1: Keywords as bare table aliases ==========

    @Test
    void keywordAtAsTableAlias() throws Exception {
        // DB tool column detail query uses: pg_catalog.pg_attribute at
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT at.attname FROM pg_catalog.pg_attribute at " +
                     "WHERE at.attrelid = (SELECT oid FROM pg_class WHERE relname = 'users') " +
                     "AND at.attnum > 0 AND NOT at.attisdropped LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("attname"));
        }
    }

    @Test
    void keywordTypeAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT type.typname FROM pg_catalog.pg_type type WHERE type.oid = 25")) {
            assertTrue(rs.next());
            assertEquals("text", rs.getString("typname"));
        }
    }

    @Test
    void keywordRoleAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT role.rolname FROM pg_catalog.pg_roles role LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("rolname"));
        }
    }

    @Test
    void keywordValueAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT value.user_id FROM users value WHERE value.user_id IS NOT NULL LIMIT 1")) {
            // Table may be empty but query should parse without error
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    void keywordNameAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT name.nspname FROM pg_catalog.pg_namespace name LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("nspname"));
        }
    }

    @Test
    void keywordKeyAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT key.conname FROM pg_catalog.pg_constraint key LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("conname"));
        }
    }

    @Test
    void keywordCommentAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT comment.description FROM pg_catalog.pg_description comment LIMIT 1")) {
            // pg_description may be empty, just ensure it parses
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    void keywordActionAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT action.user_id FROM users action LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    void keywordInputAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT input.user_id FROM users input LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    void keywordLanguageAsTableAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT language.lanname FROM pg_catalog.pg_language language LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("lanname"));
        }
    }

    // ========== Keywords as bare column aliases ==========

    @Test
    void keywordAtAsColumnAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 at")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("at"));
        }
    }

    @Test
    void keywordTypeAsColumnAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello' type")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString("type"));
        }
    }

    @Test
    void keywordNameAsColumnAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 42 name")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("name"));
        }
    }

    @Test
    void keywordRoleAsColumnAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'admin' role")) {
            assertTrue(rs.next());
            assertEquals("admin", rs.getString("role"));
        }
    }

    @Test
    void keywordKeyAsColumnAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'pk' key")) {
            assertTrue(rs.next());
            assertEquals("pk", rs.getString("key"));
        }
    }

    // ========== Keywords as aliases with AS ==========

    @Test
    void keywordAtAsExplicitAlias() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT at.attname FROM pg_catalog.pg_attribute AS at " +
                     "JOIN pg_catalog.pg_class AS rel ON at.attrelid = rel.oid " +
                     "WHERE rel.relname = 'users' AND at.attnum > 0 AND NOT at.attisdropped LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("attname"));
        }
    }

    // ========== Keywords in JOIN contexts ==========

    @Test
    void keywordAliasInJoin() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT at.attname, ty.typname " +
                     "FROM pg_catalog.pg_attribute at " +
                     "JOIN pg_catalog.pg_type ty ON ty.oid = at.atttypid " +
                     "WHERE at.attrelid = (SELECT oid FROM pg_class WHERE relname = 'users') " +
                     "AND at.attnum > 0 AND NOT at.attisdropped " +
                     "ORDER BY at.attnum LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("attname"));
            assertNotNull(rs.getString("typname"));
        }
    }

    @Test
    void multipleKeywordAliasesInJoin() throws Exception {
        // Multiple keyword aliases used together in a complex join
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT at.attname, type.typname, name.nspname " +
                     "FROM pg_catalog.pg_attribute at " +
                     "JOIN pg_catalog.pg_type type ON type.oid = at.atttypid " +
                     "JOIN pg_catalog.pg_namespace name ON name.oid = type.typnamespace " +
                     "WHERE at.attrelid = (SELECT oid FROM pg_class WHERE relname = 'users') " +
                     "AND at.attnum > 0 AND NOT at.attisdropped " +
                     "ORDER BY at.attnum LIMIT 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("attname"));
        }
    }

    // ========== DB tool exact query pattern (simplified) ==========

    @Test
    void dbToolColumnDetailQueryPattern() throws Exception {
        // Simplified version of the actual DB tool query that was failing
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT att.attname as name, att.atttypid, att.attnum, att.attnotnull, " +
                     "pg_catalog.format_type(ty.oid, NULL) AS typname " +
                     "FROM pg_catalog.pg_attribute att " +
                     "JOIN pg_catalog.pg_type ty ON ty.oid = att.atttypid " +
                     "WHERE att.attrelid = (SELECT oid FROM pg_class WHERE relname = 'users') " +
                     "AND att.attnum > 0 AND NOT att.attisdropped " +
                     "ORDER BY att.attnum")) {
            assertTrue(rs.next());
            // First column should be user_id
            assertNotNull(rs.getString("name"));
        }
    }

    @Test
    void dbToolInheritedColumnsCteWithAtAlias() throws Exception {
        // The actual DB tool CTE pattern that was failing with AT alias
        int tableOid;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_class WHERE relname = 'users'")) {
            assertTrue(rs.next());
            tableOid = rs.getInt(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH INH_TABLES AS " +
                     "(SELECT at.attname AS name, ph.inhparent AS inheritedid " +
                     " FROM pg_catalog.pg_attribute at " +
                     " JOIN pg_catalog.pg_inherits ph ON ph.inhparent = at.attrelid AND ph.inhrelid = " + tableOid + "::oid " +
                     " GROUP BY at.attname, ph.inhparent) " +
                     "SELECT att.attname as name, att.atttypid " +
                     "FROM pg_catalog.pg_attribute att " +
                     "LEFT OUTER JOIN INH_TABLES as INH ON att.attname = INH.name " +
                     "WHERE att.attrelid = " + tableOid + "::oid " +
                     "AND att.attnum > 0 AND att.attisdropped IS FALSE " +
                     "ORDER BY att.attnum")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("name"));
        }
    }

    // ========== Clause keywords must NOT be treated as aliases ==========

    @Test
    void clauseKeywordsNotTreatedAsAliases() throws Exception {
        // FROM, WHERE, etc. must not be confused with aliases
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 FROM users WHERE user_id IS NOT NULL LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    void joinKeywordsNotTreatedAsTableAliases() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT u.user_id FROM users u LEFT JOIN orders o ON u.user_id = o.user_id LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ========== Fix 2: NAME data type and ::name cast ==========

    @Test
    void castToNameType() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello'::name")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void castSubstringToName() throws Exception {
        // This is the exact pattern from DB tool's type listing query
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT substring('_mytype' FROM 2)::name")) {
            assertTrue(rs.next());
            assertEquals("mytype", rs.getString(1));
        }
    }

    @Test
    void nameTypeInPgType() throws Exception {
        // NAME type should exist in pg_type with OID 19
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT oid, typname, typlen FROM pg_type WHERE typname = 'name'")) {
            assertTrue(rs.next());
            assertEquals(19, rs.getInt("oid"));
            assertEquals("name", rs.getString("typname"));
            assertEquals(64, rs.getInt("typlen"));
        }
    }

    @Test
    void castIntegerToName() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 42::name")) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
        }
    }

    @Test
    void nameTypeInFormatType() throws Exception {
        // format_type should return 'name' for OID 19
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_catalog.format_type(19, NULL)")) {
            assertTrue(rs.next());
            assertEquals("name", rs.getString(1));
        }
    }

    // ========== DB tool type listing query pattern ==========

    @Test
    void dbToolTypeListQueryWithNameCast() throws Exception {
        // Simplified version of DB tool's type listing query that uses ::name
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM (" +
                     "SELECT pg_catalog.format_type(t.oid, NULL) AS typname, " +
                     "   CASE WHEN typelem > 0 THEN typelem ELSE t.oid END as elemoid, " +
                     "   typlen, t.oid, nspname " +
                     "FROM pg_catalog.pg_type t " +
                     "JOIN pg_catalog.pg_namespace nsp ON typnamespace = nsp.oid " +
                     "WHERE typisdefined AND typtype IN ('b', 'c', 'd', 'e', 'r', 'm') " +
                     "AND nsp.nspname != 'information_schema' " +
                     "UNION SELECT 'smallserial', 0, 2, 0, 'pg_catalog' " +
                     "UNION SELECT 'bigserial', 0, 8, 0, 'pg_catalog' " +
                     "UNION SELECT 'serial', 0, 4, 0, 'pg_catalog' " +
                     ") AS dummy ORDER BY nspname, 1")) {
            assertTrue(rs.next());
            // At least one type should be returned
            assertNotNull(rs.getString("typname"));
        }
    }

    // ========== DELETE/MERGE with keyword aliases ==========

    @Test
    void deleteWithKeywordAlias() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO users (username) VALUES ('test_delete')");
        }
        try (Statement s = conn.createStatement()) {
            int updated = s.executeUpdate(
                    "DELETE FROM users target WHERE target.username = 'test_delete'");
            assertTrue(updated >= 0);
        }
    }

    @Test
    void updateWithKeywordAlias() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO users (username) VALUES ('test_update')");
        }
        try (Statement s = conn.createStatement()) {
            int updated = s.executeUpdate(
                    "UPDATE users target SET username = 'updated' WHERE target.username = 'test_update'");
            assertTrue(updated >= 0);
        }
    }
}
