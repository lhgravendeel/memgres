package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 8, 25 (Java/JDBC): ORM-generated SQL patterns.
 * Tests pagination, optimistic locking, upsert, quoted identifiers,
 * schema-qualified access, subselect pagination, EXISTS, COALESCE,
 * CASE WHEN, bulk UPDATE, DELETE with JOIN, and count subquery.
 */
class OrmPatternTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }
    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // --- 1. Pagination with LIMIT/OFFSET (Hibernate style) ---

    @Test void pagination_limit_offset() throws Exception {
        exec("CREATE TABLE orm_page(id serial PRIMARY KEY, name text)");
        exec("INSERT INTO orm_page(name) VALUES ('a'),('b'),('c'),('d'),('e')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, name FROM orm_page ORDER BY id LIMIT 2 OFFSET 2")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("id"));
            assertFalse(rs.next(), "Should return exactly 2 rows");
        }
        exec("DROP TABLE orm_page");
    }

    // --- 2. Pagination with FETCH FIRST N ROWS ONLY (SQL standard) ---

    @Test void pagination_fetch_first() throws Exception {
        exec("CREATE TABLE orm_fetch(id serial PRIMARY KEY, name text)");
        exec("INSERT INTO orm_fetch(name) VALUES ('a'),('b'),('c'),('d'),('e')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, name FROM orm_fetch ORDER BY id OFFSET 1 FETCH FIRST 2 ROWS ONLY")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertFalse(rs.next(), "Should return exactly 2 rows");
        }
        exec("DROP TABLE orm_fetch");
    }

    // --- 3. Nullable predicate: WHERE col IS NULL OR col = ? ---

    @Test void nullable_predicate() throws Exception {
        exec("CREATE TABLE orm_nullable(id serial PRIMARY KEY, category text)");
        exec("INSERT INTO orm_nullable(category) VALUES (NULL),('x'),('y'),(NULL),('x')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM orm_nullable WHERE category IS NULL OR category = 'x'")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1), "Should match 2 NULLs + 2 'x'");
        }
        exec("DROP TABLE orm_nullable");
    }

    // --- 4. Optimistic locking: successful version bump ---

    @Test void optimistic_lock_success() throws Exception {
        exec("CREATE TABLE orm_optlock(id int PRIMARY KEY, name text, version int NOT NULL)");
        exec("INSERT INTO orm_optlock VALUES (1, 'item', 0)");
        try (Statement s = conn.createStatement()) {
            int affected = s.executeUpdate(
                    "UPDATE orm_optlock SET name = 'updated', version = version + 1 WHERE id = 1 AND version = 0");
            assertEquals(1, affected, "Should update exactly 1 row");
        }
        assertEquals("1", scalar("SELECT version FROM orm_optlock WHERE id = 1"));
        exec("DROP TABLE orm_optlock");
    }

    // --- 5. Optimistic locking: stale version returns 0 rows ---

    @Test void optimistic_lock_stale() throws Exception {
        exec("CREATE TABLE orm_optlock2(id int PRIMARY KEY, name text, version int NOT NULL)");
        exec("INSERT INTO orm_optlock2 VALUES (1, 'item', 5)");
        try (Statement s = conn.createStatement()) {
            int affected = s.executeUpdate(
                    "UPDATE orm_optlock2 SET name = 'stale', version = version + 1 WHERE id = 1 AND version = 3");
            assertEquals(0, affected, "Stale version should affect 0 rows");
        }
        assertEquals("5", scalar("SELECT version FROM orm_optlock2 WHERE id = 1"));
        exec("DROP TABLE orm_optlock2");
    }

    // --- 6. Upsert: INSERT ... ON CONFLICT DO UPDATE (JPA merge) ---

    @Test void upsert_on_conflict_update() throws Exception {
        exec("CREATE TABLE orm_upsert(id int PRIMARY KEY, name text, updated_at timestamp DEFAULT now())");
        exec("INSERT INTO orm_upsert VALUES (1, 'original', now())");
        try (Statement s = conn.createStatement()) {
            int affected = s.executeUpdate(
                    "INSERT INTO orm_upsert(id, name) VALUES (1, 'merged') " +
                    "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()");
            assertEquals(1, affected);
        }
        assertEquals("merged", scalar("SELECT name FROM orm_upsert WHERE id = 1"));
        exec("DROP TABLE orm_upsert");
    }

    // --- 7. Upsert: INSERT ... ON CONFLICT DO NOTHING ---

    @Test void upsert_on_conflict_do_nothing() throws Exception {
        exec("CREATE TABLE orm_upsert2(id int PRIMARY KEY, name text)");
        exec("INSERT INTO orm_upsert2 VALUES (1, 'existing')");
        try (Statement s = conn.createStatement()) {
            int affected = s.executeUpdate(
                    "INSERT INTO orm_upsert2(id, name) VALUES (1, 'dup') ON CONFLICT DO NOTHING");
            assertEquals(0, affected, "Conflict with DO NOTHING should affect 0 rows");
        }
        assertEquals("existing", scalar("SELECT name FROM orm_upsert2 WHERE id = 1"));
        exec("DROP TABLE orm_upsert2");
    }

    // --- 8. Quoted identifier strategy: "MixedCase" column names ---

    @Test void quoted_identifiers() throws Exception {
        exec("CREATE TABLE orm_quoted(\"Id\" int PRIMARY KEY, \"FirstName\" text, \"lastName\" text)");
        exec("INSERT INTO orm_quoted(\"Id\", \"FirstName\", \"lastName\") VALUES (1, 'Alice', 'Smith')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT \"Id\", \"FirstName\", \"lastName\" FROM orm_quoted WHERE \"Id\" = 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("Id"));
            assertEquals("Alice", rs.getString("FirstName"));
            assertEquals("Smith", rs.getString("lastName"));
        }
        exec("DROP TABLE orm_quoted");
    }

    // --- 9. Schema-qualified table access ---

    @Test void schema_qualified_access() throws Exception {
        exec("CREATE SCHEMA orm_schema");
        exec("CREATE TABLE orm_schema.orm_entity(id serial PRIMARY KEY, val text)");
        exec("INSERT INTO orm_schema.orm_entity(val) VALUES ('hello')");
        assertEquals("hello", scalar("SELECT val FROM orm_schema.orm_entity WHERE id = 1"));
        exec("DROP TABLE orm_schema.orm_entity");
        exec("DROP SCHEMA orm_schema");
    }

    // --- 10. Subselect pagination ---

    @Test void subselect_pagination() throws Exception {
        exec("CREATE TABLE orm_subpage(id serial PRIMARY KEY, name text)");
        exec("INSERT INTO orm_subpage(name) VALUES ('a'),('b'),('c'),('d'),('e'),('f')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM (SELECT id, name FROM orm_subpage ORDER BY id) sub LIMIT 2 OFFSET 3")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("id"));
            assertFalse(rs.next(), "Should return exactly 2 rows from subselect");
        }
        exec("DROP TABLE orm_subpage");
    }

    // --- 11. EXISTS subquery (common JPA pattern) ---

    @Test void exists_subquery() throws Exception {
        exec("CREATE TABLE orm_parent(id int PRIMARY KEY, name text)");
        exec("CREATE TABLE orm_child(id int PRIMARY KEY, parent_id int, val text)");
        exec("INSERT INTO orm_parent VALUES (1, 'with_children'), (2, 'without_children')");
        exec("INSERT INTO orm_child VALUES (10, 1, 'child1'), (11, 1, 'child2')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT p.id, p.name FROM orm_parent p " +
                "WHERE EXISTS (SELECT 1 FROM orm_child c WHERE c.parent_id = p.id) ORDER BY p.id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("with_children", rs.getString("name"));
            assertFalse(rs.next(), "Only parent with children should be returned");
        }
        exec("DROP TABLE orm_child");
        exec("DROP TABLE orm_parent");
    }

    // --- 12. COALESCE for null handling in projections ---

    @Test void coalesce_null_handling() throws Exception {
        exec("CREATE TABLE orm_coalesce(id int PRIMARY KEY, nickname text, full_name text)");
        exec("INSERT INTO orm_coalesce VALUES (1, NULL, 'Alice B'), (2, 'Bob', 'Robert C')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, COALESCE(nickname, full_name) AS display_name FROM orm_coalesce ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("Alice B", rs.getString("display_name"), "Should fall through to full_name when nickname is NULL");
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("display_name"), "Should use nickname when not NULL");
        }
        exec("DROP TABLE orm_coalesce");
    }

    // --- 13. CASE WHEN for conditional logic ---

    @Test void case_when_conditional() throws Exception {
        exec("CREATE TABLE orm_case(id int PRIMARY KEY, score int)");
        exec("INSERT INTO orm_case VALUES (1, 95), (2, 72), (3, 45)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'F' END AS grade " +
                "FROM orm_case ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("A", rs.getString("grade"));
            assertTrue(rs.next());
            assertEquals("B", rs.getString("grade"));
            assertTrue(rs.next());
            assertEquals("F", rs.getString("grade"));
        }
        exec("DROP TABLE orm_case");
    }

    // --- 14. Bulk UPDATE with subquery ---

    @Test void bulk_update_with_subquery() throws Exception {
        exec("CREATE TABLE orm_bulk(id int PRIMARY KEY, category text, active boolean)");
        exec("CREATE TABLE orm_inactive_cat(category text)");
        exec("INSERT INTO orm_bulk VALUES (1,'electronics',true),(2,'clothing',true),(3,'electronics',true)");
        exec("INSERT INTO orm_inactive_cat VALUES ('electronics')");
        try (Statement s = conn.createStatement()) {
            int affected = s.executeUpdate(
                    "UPDATE orm_bulk SET active = false " +
                    "WHERE category IN (SELECT category FROM orm_inactive_cat)");
            assertEquals(2, affected, "Should deactivate 2 electronics rows");
        }
        assertEquals("true", scalar("SELECT active::text FROM orm_bulk WHERE id = 2"));
        assertEquals("false", scalar("SELECT active::text FROM orm_bulk WHERE id = 1"));
        exec("DROP TABLE orm_inactive_cat");
        exec("DROP TABLE orm_bulk");
    }

    // --- 15. DELETE with JOIN (DELETE FROM ... USING ... WHERE) ---

    @Test void delete_with_using() throws Exception {
        exec("CREATE TABLE orm_del_main(id int PRIMARY KEY, ref_id int, val text)");
        exec("CREATE TABLE orm_del_ref(id int PRIMARY KEY, expired boolean)");
        exec("INSERT INTO orm_del_ref VALUES (1, true), (2, false)");
        exec("INSERT INTO orm_del_main VALUES (10, 1, 'old'), (20, 2, 'current'), (30, 1, 'also_old')");
        try (Statement s = conn.createStatement()) {
            int affected = s.executeUpdate(
                    "DELETE FROM orm_del_main m USING orm_del_ref r " +
                    "WHERE m.ref_id = r.id AND r.expired = true");
            assertEquals(2, affected, "Should delete 2 rows with expired references");
        }
        assertEquals("1", scalar("SELECT count(*)::text FROM orm_del_main"));
        exec("DROP TABLE orm_del_main");
        exec("DROP TABLE orm_del_ref");
    }

    // --- 16. Count query: SELECT count(*) FROM (subquery) ---

    @Test void count_over_subquery() throws Exception {
        exec("CREATE TABLE orm_count(id serial PRIMARY KEY, status text, amount int)");
        exec("INSERT INTO orm_count(status, amount) VALUES ('active',10),('active',20),('inactive',30),('active',5)");
        String count = scalar(
                "SELECT count(*) FROM (SELECT id, amount FROM orm_count WHERE status = 'active' AND amount > 5) sub");
        assertEquals("2", count, "Should count 2 active rows with amount > 5");
        exec("DROP TABLE orm_count");
    }
}
