package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DML syntax variants (UPDATE, INSERT, DELETE) found in real-world schemas.
 */
class DmlCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE products (id serial PRIMARY KEY, name text NOT NULL, price numeric, category text, active boolean DEFAULT true)");
            s.execute("CREATE TABLE inventory (product_id int REFERENCES products(id), warehouse text, quantity int)");
            s.execute("INSERT INTO products (name, price, category) VALUES ('Widget', 9.99, 'parts'), ('Gadget', 19.99, 'electronics'), ('Doohickey', 4.99, 'parts')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // UPDATE with table alias
    // =========================================================================

    @Test
    void testUpdateWithAlias() throws SQLException {
        // Common pattern: UPDATE tbl alias SET col = expr FROM other WHERE ...
        exec("CREATE TABLE upd_alias (id serial PRIMARY KEY, val text, extra text)");
        exec("INSERT INTO upd_alias (val) VALUES ('old')");
        exec("UPDATE upd_alias u SET val = 'new' WHERE u.id = 1");
        assertEquals("new", query1("SELECT val FROM upd_alias WHERE id = 1"));
    }

    @Test
    void testUpdateWithAliasAndFrom() throws SQLException {
        exec("CREATE TABLE upd_alias_from (id serial PRIMARY KEY, category text)");
        exec("INSERT INTO upd_alias_from (category) VALUES ('old_cat')");
        exec("CREATE TABLE upd_source (id int, new_category text)");
        exec("INSERT INTO upd_source VALUES (1, 'updated_parts')");
        exec("UPDATE upd_alias_from p SET category = s.new_category FROM upd_source s WHERE p.id = s.id");
        assertEquals("updated_parts", query1("SELECT category FROM upd_alias_from WHERE id = 1"));
    }

    @Test
    void testUpdateWithSubqueryInSet() throws SQLException {
        exec("CREATE TABLE upd_sub_test (id serial PRIMARY KEY, total numeric)");
        exec("INSERT INTO upd_sub_test (total) VALUES (0)");
        exec("UPDATE upd_sub_test SET total = (SELECT SUM(price) FROM products) WHERE id = 1");
        assertNotNull(query1("SELECT total FROM upd_sub_test WHERE id = 1"));
    }

    // =========================================================================
    // INSERT ... ON CONFLICT (UPSERT)
    // =========================================================================

    @Test
    void testInsertOnConflictDoNothing() throws SQLException {
        exec("CREATE TABLE upsert_test (id int PRIMARY KEY, name text)");
        exec("INSERT INTO upsert_test VALUES (1, 'first')");
        exec("INSERT INTO upsert_test VALUES (1, 'second') ON CONFLICT DO NOTHING");
        assertEquals("first", query1("SELECT name FROM upsert_test WHERE id = 1"));
    }

    @Test
    void testInsertOnConflictDoUpdate() throws SQLException {
        exec("CREATE TABLE upsert_update (id int PRIMARY KEY, name text, counter int DEFAULT 1)");
        exec("INSERT INTO upsert_update VALUES (1, 'original', 1)");
        exec("INSERT INTO upsert_update (id, name, counter) VALUES (1, 'updated', 1) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, counter = upsert_update.counter + 1");
        assertEquals("updated", query1("SELECT name FROM upsert_update WHERE id = 1"));
        assertEquals("2", query1("SELECT counter FROM upsert_update WHERE id = 1"));
    }

    @Test
    void testInsertOnConflictOnConstraint() throws SQLException {
        exec("CREATE TABLE upsert_con (id int, code text, val text, CONSTRAINT upsert_con_pkey PRIMARY KEY (id))");
        exec("INSERT INTO upsert_con VALUES (1, 'A', 'first')");
        exec("INSERT INTO upsert_con VALUES (1, 'B', 'second') ON CONFLICT ON CONSTRAINT upsert_con_pkey DO UPDATE SET code = EXCLUDED.code, val = EXCLUDED.val");
        assertEquals("B", query1("SELECT code FROM upsert_con WHERE id = 1"));
    }

    @Test
    void testInsertOnConflictWithWhere() throws SQLException {
        exec("CREATE TABLE upsert_where (id int PRIMARY KEY, name text, active boolean DEFAULT true)");
        exec("INSERT INTO upsert_where VALUES (1, 'keep', true)");
        exec("INSERT INTO upsert_where VALUES (1, 'replace', true) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE upsert_where.active = true");
        assertEquals("replace", query1("SELECT name FROM upsert_where WHERE id = 1"));
    }

    @Test
    void testInsertOnConflictMultipleColumns() throws SQLException {
        exec("CREATE TABLE upsert_multi (a int, b int, val text, PRIMARY KEY (a, b))");
        exec("INSERT INTO upsert_multi VALUES (1, 1, 'original')");
        exec("INSERT INTO upsert_multi VALUES (1, 1, 'updated') ON CONFLICT (a, b) DO UPDATE SET val = EXCLUDED.val");
        assertEquals("updated", query1("SELECT val FROM upsert_multi WHERE a = 1 AND b = 1"));
    }

    // =========================================================================
    // INSERT ... RETURNING
    // =========================================================================

    @Test
    void testInsertReturning() throws SQLException {
        exec("CREATE TABLE ret_test (id serial PRIMARY KEY, name text)");
        String id = query1("INSERT INTO ret_test (name) VALUES ('returned') RETURNING id");
        assertNotNull(id);
    }

    @Test
    void testInsertReturningMultipleColumns() throws SQLException {
        exec("CREATE TABLE ret_multi (id serial PRIMARY KEY, name text, created_at timestamp DEFAULT now())");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("INSERT INTO ret_multi (name) VALUES ('test') RETURNING id, name, created_at")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("id"));
            assertEquals("test", rs.getString("name"));
            assertNotNull(rs.getString("created_at"));
        }
    }

    // =========================================================================
    // UPDATE ... RETURNING
    // =========================================================================

    @Test
    void testUpdateReturning() throws SQLException {
        exec("CREATE TABLE upd_ret (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO upd_ret (val) VALUES (10)");
        String newVal = query1("UPDATE upd_ret SET val = val + 5 WHERE id = 1 RETURNING val");
        assertEquals("15", newVal);
    }

    // =========================================================================
    // DELETE ... RETURNING
    // =========================================================================

    @Test
    void testDeleteReturning() throws SQLException {
        exec("CREATE TABLE del_ret (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO del_ret (name) VALUES ('to_delete')");
        String deleted = query1("DELETE FROM del_ret WHERE id = 1 RETURNING name");
        assertEquals("to_delete", deleted);
    }

    // =========================================================================
    // LATERAL join in SELECT
    // =========================================================================

    @Test
    void testLateralJoin() throws SQLException {
        exec("CREATE TABLE lat_parent (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE lat_child (id serial PRIMARY KEY, parent_id int, score int)");
        exec("INSERT INTO lat_parent (name) VALUES ('A'), ('B')");
        exec("INSERT INTO lat_child (parent_id, score) VALUES (1, 100), (1, 90), (2, 80)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                SELECT p.name, top.max_score
                FROM lat_parent p
                LEFT JOIN LATERAL (
                    SELECT MAX(score) AS max_score FROM lat_child c WHERE c.parent_id = p.id
                ) top ON true
                ORDER BY p.name
            """)) {
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertEquals("100", rs.getString(2));
        }
    }

    @Test
    void testLateralJoinWithLimit() throws SQLException {
        // Common pattern: top-N per group using LATERAL
        exec("CREATE TABLE lat_items (id serial PRIMARY KEY, group_id int, rank_val int)");
        exec("CREATE TABLE lat_groups (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO lat_groups (name) VALUES ('X')");
        exec("INSERT INTO lat_items (group_id, rank_val) VALUES (1, 10), (1, 20), (1, 30)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                SELECT g.name, i.rank_val
                FROM lat_groups g
                LEFT JOIN LATERAL (
                    SELECT rank_val FROM lat_items WHERE group_id = g.id ORDER BY rank_val DESC LIMIT 2
                ) i ON true
                ORDER BY i.rank_val DESC
            """)) {
            assertTrue(rs.next());
            assertEquals("30", rs.getString(2));
        }
    }

    // =========================================================================
    // Aggregate FILTER (WHERE ...)
    // =========================================================================

    @Test
    void testAggregateFilterWhere() throws SQLException {
        assertEquals("2", query1("SELECT COUNT(*) FILTER (WHERE category = 'parts') FROM products"));
    }

    @Test
    void testMultipleAggregateFilters() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE category = 'parts') AS parts_count,
                    SUM(price) FILTER (WHERE category = 'electronics') AS electronics_total,
                    AVG(price) FILTER (WHERE active = true) AS avg_active_price
                FROM products
            """)) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("total"));
        }
    }

    @Test
    void testAggregateFilterInGroupBy() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                SELECT category,
                    COUNT(*) FILTER (WHERE price > 5) AS expensive_count
                FROM products
                GROUP BY category
                ORDER BY category
            """)) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // DELETE ... USING (join in DELETE)
    // =========================================================================

    @Test
    void testDeleteUsing() throws SQLException {
        exec("CREATE TABLE del_main (id serial PRIMARY KEY, status text)");
        exec("CREATE TABLE del_filter (target_id int)");
        exec("INSERT INTO del_main (status) VALUES ('active'), ('inactive')");
        exec("INSERT INTO del_filter (target_id) VALUES (2)");
        exec("DELETE FROM del_main USING del_filter WHERE del_main.id = del_filter.target_id");
        assertEquals("1", query1("SELECT COUNT(*) FROM del_main"));
    }

    // =========================================================================
    // INSERT ... SELECT
    // =========================================================================

    @Test
    void testInsertSelect() throws SQLException {
        exec("CREATE TABLE ins_select_target (id int, name text, price numeric)");
        exec("INSERT INTO ins_select_target SELECT id, name, price FROM products WHERE category = 'parts'");
        assertEquals("2", query1("SELECT COUNT(*) FROM ins_select_target"));
    }

    // =========================================================================
    // UPDATE with FROM and JOIN
    // =========================================================================

    @Test
    void testUpdateWithFromJoin() throws SQLException {
        exec("CREATE TABLE upd_prices (id serial PRIMARY KEY, name text, adjusted_price numeric)");
        exec("INSERT INTO upd_prices (name, adjusted_price) VALUES ('Widget', 0), ('Gadget', 0)");
        exec("""
            UPDATE upd_prices up
            SET adjusted_price = p.price * 1.1
            FROM products p
            WHERE up.name = p.name
        """);
        assertNotNull(query1("SELECT adjusted_price FROM upd_prices WHERE name = 'Widget'"));
    }

    // =========================================================================
    // CTE with DML (WITH ... INSERT/UPDATE/DELETE)
    // =========================================================================

    @Test
    void testCteWithInsert() throws SQLException {
        exec("CREATE TABLE cte_dml_source (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE cte_dml_target (val text)");
        exec("INSERT INTO cte_dml_source (val) VALUES ('a'), ('b'), ('c')");
        exec("""
            WITH selected AS (
                SELECT val FROM cte_dml_source WHERE val != 'b'
            )
            INSERT INTO cte_dml_target SELECT val FROM selected
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM cte_dml_target"));
    }

    @Test
    void testCteWithDelete() throws SQLException {
        exec("CREATE TABLE cte_del_test (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE cte_del_archive (id int, name text)");
        exec("INSERT INTO cte_del_test (name) VALUES ('keep'), ('remove')");
        exec("""
            WITH deleted AS (
                DELETE FROM cte_del_test WHERE name = 'remove' RETURNING id, name
            )
            INSERT INTO cte_del_archive SELECT * FROM deleted
        """);
        assertEquals("1", query1("SELECT COUNT(*) FROM cte_del_test"));
        assertEquals("remove", query1("SELECT name FROM cte_del_archive"));
    }

    // =========================================================================
    // MERGE (PG 15+)
    // =========================================================================

    @Test
    void testMerge() throws SQLException {
        exec("CREATE TABLE merge_target (id int PRIMARY KEY, val text, counter int DEFAULT 0)");
        exec("CREATE TABLE merge_source (id int, val text)");
        exec("INSERT INTO merge_target VALUES (1, 'old', 1)");
        exec("INSERT INTO merge_source VALUES (1, 'updated'), (2, 'new')");
        exec("""
            MERGE INTO merge_target t
            USING merge_source s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val, counter = t.counter + 1
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
        """);
        assertEquals("updated", query1("SELECT val FROM merge_target WHERE id = 1"));
        assertEquals("new", query1("SELECT val FROM merge_target WHERE id = 2"));
    }

    // =========================================================================
    // jsonb operators in DML
    // =========================================================================

    @Test
    void testJsonbSetInUpdate() throws SQLException {
        exec("CREATE TABLE jdml_test (id serial PRIMARY KEY, data jsonb)");
        exec("INSERT INTO jdml_test (data) VALUES ('{\"a\": 1, \"b\": 2}')");
        exec("UPDATE jdml_test SET data = jsonb_set(data, '{a}', '99') WHERE id = 1");
        assertEquals("99", query1("SELECT data->>'a' FROM jdml_test WHERE id = 1"));
    }

    @Test
    void testJsonbDeleteKeyInUpdate() throws SQLException {
        exec("CREATE TABLE jdml_del (id serial PRIMARY KEY, config jsonb)");
        exec("INSERT INTO jdml_del (config) VALUES ('{\"keep\": 1, \"remove\": 2}')");
        exec("UPDATE jdml_del SET config = config - 'remove' WHERE id = 1");
        // 'remove' key should be gone
    }

    @Test
    void testJsonbDeletePathInUpdate() throws SQLException {
        // #- operator (delete path)
        exec("CREATE TABLE jdml_path (id serial PRIMARY KEY, data jsonb)");
        exec("INSERT INTO jdml_path (data) VALUES ('{\"a\": {\"b\": 1, \"c\": 2}}')");
        exec("UPDATE jdml_path SET data = data #- '{a,b}' WHERE id = 1");
    }
}
