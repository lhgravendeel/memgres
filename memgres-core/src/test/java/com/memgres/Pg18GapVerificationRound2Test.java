package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 gap verification round 2: deeper coverage of SQL features.
 */
class Pg18GapVerificationRound2Test {

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

    // ===== Recursive CTEs =====

    @Test
    void recursiveCteSimple() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                "WITH RECURSIVE cnt(n) AS (" +
                "  SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5" +
                ") SELECT n FROM cnt ORDER BY n")) {
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
    }

    @Test
    void recursiveCteHierarchy() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rc_emp (id int, name text, mgr_id int)");
            st.execute("INSERT INTO rc_emp VALUES (1,'CEO',NULL),(2,'VP',1),(3,'Dir',2),(4,'Mgr',3)");
            try (ResultSet rs = st.executeQuery(
                    "WITH RECURSIVE chain AS (" +
                    "  SELECT id, name, mgr_id, 1 as depth FROM rc_emp WHERE id = 4 " +
                    "  UNION ALL " +
                    "  SELECT e.id, e.name, e.mgr_id, c.depth+1 FROM rc_emp e JOIN chain c ON e.id = c.mgr_id" +
                    ") SELECT name FROM chain ORDER BY depth")) {
                assertTrue(rs.next()); assertEquals("Mgr", rs.getString(1));
                assertTrue(rs.next()); assertEquals("Dir", rs.getString(1));
                assertTrue(rs.next()); assertEquals("VP", rs.getString(1));
                assertTrue(rs.next()); assertEquals("CEO", rs.getString(1));
            }
            st.execute("DROP TABLE rc_emp");
        }
    }

    // ===== GROUPING SETS / CUBE / ROLLUP =====

    @Test
    void groupingSets() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE gs_sales (region text, product text, amount int)");
            st.execute("INSERT INTO gs_sales VALUES ('N','A',10),('N','B',20),('S','A',30),('S','B',40)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT region, product, sum(amount) FROM gs_sales " +
                    "GROUP BY GROUPING SETS ((region), (product), ()) ORDER BY region NULLS LAST, product NULLS LAST")) {
                // region groups: N=30, S=70
                assertTrue(rs.next()); assertEquals("N", rs.getString(1)); assertNull(rs.getString(2)); assertEquals(30, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("S", rs.getString(1)); assertNull(rs.getString(2)); assertEquals(70, rs.getInt(3));
                // product groups: A=40, B=60
                assertTrue(rs.next()); assertNull(rs.getString(1)); assertEquals("A", rs.getString(2)); assertEquals(40, rs.getInt(3));
                assertTrue(rs.next()); assertNull(rs.getString(1)); assertEquals("B", rs.getString(2)); assertEquals(60, rs.getInt(3));
                // grand total: 100
                assertTrue(rs.next()); assertNull(rs.getString(1)); assertNull(rs.getString(2)); assertEquals(100, rs.getInt(3));
            }
            st.execute("DROP TABLE gs_sales");
        }
    }

    @Test
    void rollup() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ru_data (a text, b text, val int)");
            st.execute("INSERT INTO ru_data VALUES ('x','p',1),('x','q',2),('y','p',3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT a, b, sum(val) FROM ru_data GROUP BY ROLLUP (a, b) ORDER BY a NULLS LAST, b NULLS LAST")) {
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertEquals("p", rs.getString(2)); assertEquals(1, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertEquals("q", rs.getString(2)); assertEquals(2, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertNull(rs.getString(2)); assertEquals(3, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("y", rs.getString(1)); assertEquals("p", rs.getString(2)); assertEquals(3, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("y", rs.getString(1)); assertNull(rs.getString(2)); assertEquals(3, rs.getInt(3));
                assertTrue(rs.next()); assertNull(rs.getString(1)); assertNull(rs.getString(2)); assertEquals(6, rs.getInt(3));
            }
            st.execute("DROP TABLE ru_data");
        }
    }

    @Test
    void cube() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE cu_data (a text, b text, val int)");
            st.execute("INSERT INTO cu_data VALUES ('x','p',1),('x','q',2),('y','p',3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT a, b, sum(val) FROM cu_data GROUP BY CUBE (a, b) ORDER BY a NULLS LAST, b NULLS LAST")) {
                // x,p=1; x,q=2; x,NULL=3; y,p=3; y,NULL=3; NULL,p=4; NULL,q=2; NULL,NULL=6
                int count = 0;
                while (rs.next()) count++;
                assertEquals(8, count);
            }
            st.execute("DROP TABLE cu_data");
        }
    }

    // ===== DISTINCT ON =====

    @Test
    void distinctOn() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE do_test (category text, val int, label text)");
            st.execute("INSERT INTO do_test VALUES ('a',3,'a3'),('a',1,'a1'),('b',2,'b2'),('b',5,'b5')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT DISTINCT ON (category) category, val, label FROM do_test ORDER BY category, val")) {
                assertTrue(rs.next()); assertEquals("a", rs.getString(1)); assertEquals(1, rs.getInt(2));
                assertTrue(rs.next()); assertEquals("b", rs.getString(1)); assertEquals(2, rs.getInt(2));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE do_test");
        }
    }

    // ===== Domain types =====

    @Test
    void domainWithCheckConstraint() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)");
            st.execute("CREATE TABLE dom_test (id positive_int)");
            st.execute("INSERT INTO dom_test VALUES (5)");
            try {
                st.execute("INSERT INTO dom_test VALUES (-1)");
                fail("Negative value should violate domain constraint");
            } catch (SQLException e) {
                assertTrue(e.getSQLState().startsWith("23"));
            }
            st.execute("DROP TABLE dom_test");
            st.execute("DROP DOMAIN positive_int");
        }
    }

    @Test
    void domainNotNull() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE DOMAIN nn_text AS text NOT NULL");
            st.execute("CREATE TABLE dnn_test (val nn_text)");
            st.execute("INSERT INTO dnn_test VALUES ('hello')");
            try {
                st.execute("INSERT INTO dnn_test VALUES (NULL)");
                fail("NULL should violate domain NOT NULL");
            } catch (SQLException e) {
                assertTrue(e.getSQLState().startsWith("23"));
            }
            st.execute("DROP TABLE dnn_test");
            st.execute("DROP DOMAIN nn_text");
        }
    }

    // ===== ENUM edge cases =====

    @Test
    void enumOrdering() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')");
            st.execute("CREATE TABLE en_test (p priority)");
            st.execute("INSERT INTO en_test VALUES ('high'),('low'),('medium')");
            try (ResultSet rs = st.executeQuery("SELECT p FROM en_test ORDER BY p")) {
                assertTrue(rs.next()); assertEquals("low", rs.getString(1));
                assertTrue(rs.next()); assertEquals("medium", rs.getString(1));
                assertTrue(rs.next()); assertEquals("high", rs.getString(1));
            }
            st.execute("DROP TABLE en_test");
            st.execute("DROP TYPE priority");
        }
    }

    @Test
    void enumComparison() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TYPE sz AS ENUM ('small', 'medium', 'large')");
            try (ResultSet rs = st.executeQuery("SELECT 'small'::sz < 'large'::sz")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
            st.execute("DROP TYPE sz");
        }
    }

    @Test
    void alterTypeAddValue() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TYPE color AS ENUM ('red', 'blue')");
            st.execute("ALTER TYPE color ADD VALUE 'green'");
            st.execute("CREATE TABLE col_test (c color)");
            st.execute("INSERT INTO col_test VALUES ('green')");
            try (ResultSet rs = st.executeQuery("SELECT c FROM col_test")) {
                assertTrue(rs.next());
                assertEquals("green", rs.getString(1));
            }
            st.execute("DROP TABLE col_test");
            st.execute("DROP TYPE color");
        }
    }

    // ===== JSONB operators and subscripting =====

    @Test
    void jsonbArrowOperators() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT '{\"a\": {\"b\": 42}}'::jsonb -> 'a' ->> 'b'")) {
                assertTrue(rs.next());
                assertEquals("42", rs.getString(1));
            }
        }
    }

    @Test
    void jsonbPathOperator() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT '{\"a\": 1, \"b\": 2}'::jsonb #>> '{a}'")) {
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
            }
        }
    }

    @Test
    void jsonbContainsOperator() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT '{\"a\": 1, \"b\": 2}'::jsonb @> '{\"a\": 1}'::jsonb")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void jsonbExistsOperator() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT '{\"a\": 1, \"b\": 2}'::jsonb ? 'a'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void jsonbConcatOperator() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT '{\"a\": 1}'::jsonb || '{\"b\": 2}'::jsonb")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertTrue(result.contains("\"a\"") && result.contains("\"b\""));
            }
        }
    }

    @Test
    void jsonbDeleteKey() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT '{\"a\": 1, \"b\": 2}'::jsonb - 'a'")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertFalse(result.contains("\"a\""));
                assertTrue(result.contains("\"b\""));
            }
        }
    }

    // ===== Array subscripting and slicing =====

    @Test
    void arraySubscript() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT (ARRAY[10,20,30])[2]")) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1));
            }
        }
    }

    @Test
    void arraySlice() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT (ARRAY[10,20,30,40,50])[2:4]")) {
                assertTrue(rs.next());
                assertEquals("{20,30,40}", rs.getString(1));
            }
        }
    }

    @Test
    void arrayConstructorWithSubquery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ac_test (val int)");
            st.execute("INSERT INTO ac_test VALUES (3),(1),(2)");
            try (ResultSet rs = st.executeQuery("SELECT ARRAY(SELECT val FROM ac_test ORDER BY val)")) {
                assertTrue(rs.next());
                assertEquals("{1,2,3}", rs.getString(1));
            }
            st.execute("DROP TABLE ac_test");
        }
    }

    // ===== IS DISTINCT FROM =====

    @Test
    void isDistinctFrom() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT 1 IS DISTINCT FROM 2, 1 IS DISTINCT FROM 1, " +
                    "NULL IS DISTINCT FROM NULL, 1 IS DISTINCT FROM NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));   // 1 != 2
                assertFalse(rs.getBoolean(2));  // 1 = 1
                assertFalse(rs.getBoolean(3));  // NULL = NULL (unlike regular =)
                assertTrue(rs.getBoolean(4));   // 1 != NULL
            }
        }
    }

    @Test
    void isNotDistinctFrom() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT 1 IS NOT DISTINCT FROM 1, NULL IS NOT DISTINCT FROM NULL, " +
                    "1 IS NOT DISTINCT FROM NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertTrue(rs.getBoolean(2));
                assertFalse(rs.getBoolean(3));
            }
        }
    }

    // ===== Interval arithmetic =====

    @Test
    void intervalAddition() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT DATE '2024-01-15' + INTERVAL '1 month 10 days'")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("2024-02-25"), "Expected 2024-02-25, got: " + val);
            }
        }
    }

    @Test
    void intervalSubtraction() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT TIMESTAMP '2024-06-15 12:00:00' - INTERVAL '3 hours 30 minutes'")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("08:30"), "Expected 08:30, got: " + val);
            }
        }
    }

    @Test
    void intervalMultiply() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT INTERVAL '1 hour' * 3")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("03:00") || val.contains("3:00"), "Expected 3 hours, got: " + val);
            }
        }
    }

    @Test
    void dateDifference() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT DATE '2024-03-01' - DATE '2024-01-01'")) {
                assertTrue(rs.next());
                assertEquals(60, rs.getInt(1));
            }
        }
    }

    // ===== PREPARE / EXECUTE / DEALLOCATE =====

    @Test
    void prepareAndExecute() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pr_test (id int, val text)");
            st.execute("INSERT INTO pr_test VALUES (1,'a'),(2,'b'),(3,'c')");
            st.execute("PREPARE my_query AS SELECT val FROM pr_test WHERE id = $1");
            try (ResultSet rs = st.executeQuery("EXECUTE my_query(2)")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
            }
            st.execute("DEALLOCATE my_query");
            st.execute("DROP TABLE pr_test");
        }
    }

    // ===== EXPLAIN =====

    @Test
    void explainBasic() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ex_test (id int)");
            try (ResultSet rs = st.executeQuery("EXPLAIN SELECT * FROM ex_test")) {
                assertTrue(rs.next());
                String plan = rs.getString(1);
                assertNotNull(plan);
                assertTrue(plan.length() > 0);
            }
            st.execute("DROP TABLE ex_test");
        }
    }

    @Test
    void explainAnalyze() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE exa_test (id int)");
            st.execute("INSERT INTO exa_test VALUES (1),(2),(3)");
            try (ResultSet rs = st.executeQuery("EXPLAIN ANALYZE SELECT * FROM exa_test")) {
                assertTrue(rs.next());
            }
            st.execute("DROP TABLE exa_test");
        }
    }

    // ===== SET / SHOW / RESET =====

    @Test
    void showServerVersion() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SHOW server_version")) {
                assertTrue(rs.next());
                String ver = rs.getString(1);
                assertNotNull(ver);
            }
        }
    }

    @Test
    void setAndShowSearchPath() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SET search_path TO public, pg_catalog");
            try (ResultSet rs = st.executeQuery("SHOW search_path")) {
                assertTrue(rs.next());
                String path = rs.getString(1);
                assertTrue(path.contains("public"), "Expected public in search_path: " + path);
            }
        }
    }

    @Test
    void resetSearchPath() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SET search_path TO pg_catalog");
            st.execute("RESET search_path");
            try (ResultSet rs = st.executeQuery("SHOW search_path")) {
                assertTrue(rs.next());
                // Default includes public
            }
        }
    }

    // ===== String escape syntax =====

    @Test
    void escapeStringLiteral() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT E'hello\\nworld'")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("\n"), "Expected newline in: " + val);
            }
        }
    }

    @Test
    void escapeStringTab() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT E'a\\tb'")) {
                assertTrue(rs.next());
                assertTrue(rs.getString(1).contains("\t"));
            }
        }
    }

    @Test
    void dollarQuotedString() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT $$hello 'world'$$")) {
                assertTrue(rs.next());
                assertEquals("hello 'world'", rs.getString(1));
            }
        }
    }

    // ===== Numeric edge cases =====

    @Test
    void numericNaN() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'NaN'::float8")) {
                assertTrue(rs.next());
                assertTrue(Double.isNaN(rs.getDouble(1)));
            }
        }
    }

    @Test
    void numericInfinity() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'Infinity'::float8, '-Infinity'::float8")) {
                assertTrue(rs.next());
                assertTrue(Double.isInfinite(rs.getDouble(1)));
                assertTrue(rs.getDouble(1) > 0);
                assertTrue(Double.isInfinite(rs.getDouble(2)));
                assertTrue(rs.getDouble(2) < 0);
            }
        }
    }

    @Test
    void numericPrecision() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 1.0/3.0")) {
                assertTrue(rs.next());
                double val = rs.getDouble(1);
                assertEquals(0.333333, val, 0.001);
            }
        }
    }

    // ===== ROW constructors =====

    @Test
    void rowComparison() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT ROW(1, 'a') = ROW(1, 'a')")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void rowInequality() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT ROW(1, 'a') < ROW(2, 'a')")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // ===== FILTER clause on aggregates =====

    @Test
    void aggregateFilter() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE af_test (category text, amount int)");
            st.execute("INSERT INTO af_test VALUES ('a',10),('b',20),('a',30),('b',40)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT sum(amount) FILTER (WHERE category = 'a'), " +
                    "sum(amount) FILTER (WHERE category = 'b') FROM af_test")) {
                assertTrue(rs.next());
                assertEquals(40, rs.getInt(1));
                assertEquals(60, rs.getInt(2));
            }
            st.execute("DROP TABLE af_test");
        }
    }

    @Test
    void countFilter() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE cf_test (val int)");
            st.execute("INSERT INTO cf_test VALUES (1),(2),(3),(4),(5)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FILTER (WHERE val > 3) FROM cf_test")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            st.execute("DROP TABLE cf_test");
        }
    }

    // ===== Ordered-set aggregates =====

    @Test
    void percentileCont() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pc_test (val double precision)");
            st.execute("INSERT INTO pc_test VALUES (1),(2),(3),(4),(5)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val) FROM pc_test")) {
                assertTrue(rs.next());
                assertEquals(3.0, rs.getDouble(1), 0.001);
            }
            st.execute("DROP TABLE pc_test");
        }
    }

    @Test
    void percentileDisc() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pd_test (val int)");
            st.execute("INSERT INTO pd_test VALUES (10),(20),(30),(40),(50)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) FROM pd_test")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }
            st.execute("DROP TABLE pd_test");
        }
    }

    @Test
    void modeAggregate() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE mode_test (val int)");
            st.execute("INSERT INTO mode_test VALUES (1),(2),(2),(3),(2),(3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT mode() WITHIN GROUP (ORDER BY val) FROM mode_test")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            st.execute("DROP TABLE mode_test");
        }
    }

    // ===== INSERT ... SELECT =====

    @Test
    void insertSelect() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE is_src (id int, val text)");
            st.execute("CREATE TABLE is_dst (id int, val text)");
            st.execute("INSERT INTO is_src VALUES (1,'a'),(2,'b'),(3,'c')");
            st.execute("INSERT INTO is_dst SELECT * FROM is_src WHERE id <= 2");
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM is_dst")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            st.execute("DROP TABLE is_dst");
            st.execute("DROP TABLE is_src");
        }
    }

    // ===== UPDATE ... FROM =====

    @Test
    void updateFrom() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE uf_main (id int, val text)");
            st.execute("CREATE TABLE uf_ref (id int, new_val text)");
            st.execute("INSERT INTO uf_main VALUES (1,'old'),(2,'old')");
            st.execute("INSERT INTO uf_ref VALUES (1,'new1'),(2,'new2')");
            st.execute("UPDATE uf_main SET val = uf_ref.new_val FROM uf_ref WHERE uf_main.id = uf_ref.id");
            try (ResultSet rs = st.executeQuery("SELECT val FROM uf_main ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("new1", rs.getString(1));
                assertTrue(rs.next()); assertEquals("new2", rs.getString(1));
            }
            st.execute("DROP TABLE uf_ref");
            st.execute("DROP TABLE uf_main");
        }
    }

    // ===== DELETE ... USING =====

    @Test
    void deleteUsing() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE du_main (id int, val text)");
            st.execute("CREATE TABLE du_del (id int)");
            st.execute("INSERT INTO du_main VALUES (1,'a'),(2,'b'),(3,'c')");
            st.execute("INSERT INTO du_del VALUES (1),(3)");
            st.execute("DELETE FROM du_main USING du_del WHERE du_main.id = du_del.id");
            try (ResultSet rs = st.executeQuery("SELECT id FROM du_main")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE du_del");
            st.execute("DROP TABLE du_main");
        }
    }

    // ===== TABLESAMPLE =====

    @Test
    void tablesampleBernoulli() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ts_test (id int)");
            for (int i = 1; i <= 100; i++) st.execute("INSERT INTO ts_test VALUES (" + i + ")");
            // 100% sample should return all rows
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM ts_test TABLESAMPLE BERNOULLI(100)")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
            }
            st.execute("DROP TABLE ts_test");
        }
    }

    // ===== GENERATED ALWAYS AS IDENTITY =====

    @Test
    void generatedAlwaysAsIdentity() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE id_test (id int GENERATED ALWAYS AS IDENTITY, val text)");
            st.execute("INSERT INTO id_test (val) VALUES ('a')");
            st.execute("INSERT INTO id_test (val) VALUES ('b')");
            try (ResultSet rs = st.executeQuery("SELECT id, val FROM id_test ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("a", rs.getString(2));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("b", rs.getString(2));
            }
            // GENERATED ALWAYS should reject explicit id
            try {
                st.execute("INSERT INTO id_test (id, val) VALUES (99, 'c')");
                fail("GENERATED ALWAYS should reject explicit values");
            } catch (SQLException e) {
                // expected
            }
            st.execute("DROP TABLE id_test");
        }
    }

    @Test
    void generatedByDefaultAsIdentity() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE idd_test (id int GENERATED BY DEFAULT AS IDENTITY, val text)");
            st.execute("INSERT INTO idd_test (val) VALUES ('a')");
            st.execute("INSERT INTO idd_test (id, val) VALUES (99, 'b')");
            try (ResultSet rs = st.executeQuery("SELECT id, val FROM idd_test ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(99, rs.getInt(1));
            }
            st.execute("DROP TABLE idd_test");
        }
    }

    // ===== CASE expressions =====

    @Test
    void searchedCase() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT CASE WHEN 1 > 2 THEN 'yes' WHEN 1 < 2 THEN 'no' ELSE 'maybe' END")) {
                assertTrue(rs.next());
                assertEquals("no", rs.getString(1));
            }
        }
    }

    @Test
    void simpleCase() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' END")) {
                assertTrue(rs.next());
                assertEquals("two", rs.getString(1));
            }
        }
    }

    // ===== COALESCE / NULLIF / GREATEST / LEAST =====

    @Test
    void coalesce() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COALESCE(NULL, NULL, 3, 4)")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void nullif() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT NULLIF(1, 1), NULLIF(1, 2)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertEquals(1, rs.getInt(2));
            }
        }
    }

    @Test
    void greatestLeast() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT GREATEST(3, 1, 4, 1, 5), LEAST(3, 1, 4, 1, 5)")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertEquals(1, rs.getInt(2));
            }
        }
    }

    // ===== LATERAL join =====

    @Test
    void lateralJoin() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lj_dept (id int, name text)");
            st.execute("CREATE TABLE lj_emp (id int, dept_id int, salary int)");
            st.execute("INSERT INTO lj_dept VALUES (1,'Eng'),(2,'Sales')");
            st.execute("INSERT INTO lj_emp VALUES (1,1,100),(2,1,200),(3,2,150)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT d.name, top.salary FROM lj_dept d, " +
                    "LATERAL (SELECT salary FROM lj_emp e WHERE e.dept_id = d.id ORDER BY salary DESC LIMIT 1) top " +
                    "ORDER BY d.name")) {
                assertTrue(rs.next()); assertEquals("Eng", rs.getString(1)); assertEquals(200, rs.getInt(2));
                assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(150, rs.getInt(2));
            }
            st.execute("DROP TABLE lj_emp");
            st.execute("DROP TABLE lj_dept");
        }
    }

    // ===== Common string functions =====

    @Test
    void stringFunctions() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT length('hello'), upper('hello'), lower('HELLO'), " +
                    "trim('  hi  '), ltrim('  hi'), rtrim('hi  '), " +
                    "left('hello', 3), right('hello', 3)")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertEquals("HELLO", rs.getString(2));
                assertEquals("hello", rs.getString(3));
                assertEquals("hi", rs.getString(4));
                assertEquals("hi", rs.getString(5));
                assertEquals("hi", rs.getString(6));
                assertEquals("hel", rs.getString(7));
                assertEquals("llo", rs.getString(8));
            }
        }
    }

    @Test
    void substringAndPosition() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT substring('hello world' FROM 7), " +
                    "position('world' IN 'hello world'), " +
                    "replace('hello', 'l', 'r'), " +
                    "repeat('ab', 3)")) {
                assertTrue(rs.next());
                assertEquals("world", rs.getString(1));
                assertEquals(7, rs.getInt(2));
                assertEquals("herro", rs.getString(3));
                assertEquals("ababab", rs.getString(4));
            }
        }
    }

    @Test
    void splitPartAndConcat() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT split_part('a.b.c', '.', 2), " +
                    "concat('hello', ' ', 'world'), " +
                    "concat_ws('-', 'a', 'b', 'c')")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals("hello world", rs.getString(2));
                assertEquals("a-b-c", rs.getString(3));
            }
        }
    }

    // ===== Math functions =====

    @Test
    void mathFunctions() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT abs(-5), ceil(4.3), floor(4.7), round(4.567, 2), " +
                    "sign(-3), sqrt(16.0), power(2, 10), mod(17, 5)")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertEquals(5.0, rs.getDouble(2), 0.01);
                assertEquals(4.0, rs.getDouble(3), 0.01);
                assertEquals(4.57, rs.getDouble(4), 0.001);
                assertEquals(-1, rs.getInt(5));
                assertEquals(4.0, rs.getDouble(6), 0.001);
                assertEquals(1024.0, rs.getDouble(7), 0.01);
                assertEquals(2, rs.getInt(8));
            }
        }
    }

    @Test
    void logAndTrigFunctions() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT ln(1), log(100), log(2, 8), exp(0), " +
                    "pi(), degrees(pi()), radians(180.0)")) {
                assertTrue(rs.next());
                assertEquals(0.0, rs.getDouble(1), 0.001);
                assertEquals(2.0, rs.getDouble(2), 0.001);
                assertEquals(3.0, rs.getDouble(3), 0.001);
                assertEquals(1.0, rs.getDouble(4), 0.001);
                assertEquals(Math.PI, rs.getDouble(5), 0.001);
                assertEquals(180.0, rs.getDouble(6), 0.001);
                assertEquals(Math.PI, rs.getDouble(7), 0.001);
            }
        }
    }

    // ===== UNION / INTERSECT / EXCEPT =====

    @Test
    void unionAll() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2")) {
                int count = 0;
                while (rs.next()) count++;
                assertEquals(3, count);
            }
        }
    }

    @Test
    void unionDistinct() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 1 UNION SELECT 1 UNION SELECT 2")) {
                int count = 0;
                while (rs.next()) count++;
                assertEquals(2, count);
            }
        }
    }

    @Test
    void intersect() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM (VALUES (1),(2),(3)) t1(v) INTERSECT SELECT * FROM (VALUES (2),(3),(4)) t2(v) ORDER BY 1")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void except() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM (VALUES (1),(2),(3)) t1(v) EXCEPT SELECT * FROM (VALUES (2)) t2(v) ORDER BY 1")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ===== VALUES clause =====

    @Test
    void valuesClause() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("VALUES (1, 'a'), (2, 'b'), (3, 'c')")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("a", rs.getString(2));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ===== EXISTS subquery =====

    @Test
    void existsSubquery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ex_main (id int)");
            st.execute("CREATE TABLE ex_ref (main_id int)");
            st.execute("INSERT INTO ex_main VALUES (1),(2),(3)");
            st.execute("INSERT INTO ex_ref VALUES (1),(3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT id FROM ex_main WHERE EXISTS (SELECT 1 FROM ex_ref WHERE ex_ref.main_id = ex_main.id) ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE ex_ref");
            st.execute("DROP TABLE ex_main");
        }
    }

    @Test
    void notExistsSubquery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE nex_main (id int)");
            st.execute("CREATE TABLE nex_ref (main_id int)");
            st.execute("INSERT INTO nex_main VALUES (1),(2),(3)");
            st.execute("INSERT INTO nex_ref VALUES (1),(3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT id FROM nex_main WHERE NOT EXISTS (SELECT 1 FROM nex_ref WHERE nex_ref.main_id = nex_main.id) ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE nex_ref");
            st.execute("DROP TABLE nex_main");
        }
    }

    // ===== pg_typeof =====

    @Test
    void pgTypeof() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_typeof(1), pg_typeof(1.0), pg_typeof('hello'::text), pg_typeof(true)")) {
                assertTrue(rs.next());
                assertEquals("integer", rs.getString(1));
                String numType = rs.getString(2);
                assertTrue("numeric".equals(numType) || "double precision".equals(numType),
                        "Expected numeric type, got: " + numType);
                assertEquals("text", rs.getString(3));
                assertEquals("boolean", rs.getString(4));
            }
        }
    }

    // ===== current_date/time functions =====

    @Test
    void currentDateTimeFunctions() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT current_date, current_timestamp, now(), current_time")) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
                assertNotNull(rs.getObject(2));
                assertNotNull(rs.getObject(3));
                assertNotNull(rs.getObject(4));
            }
        }
    }

    // ===== Boolean logic =====

    @Test
    void booleanLogic() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT true AND false, true OR false, NOT true, " +
                    "true AND NULL, false AND NULL, true OR NULL, false OR NULL")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
                assertTrue(rs.getBoolean(2));
                assertFalse(rs.getBoolean(3));
                // NULL AND logic
                assertNull(rs.getObject(4)); // true AND NULL = NULL
                assertFalse(rs.getBoolean(5)); // false AND NULL = false
                assertTrue(rs.getBoolean(6)); // true OR NULL = true
                assertNull(rs.getObject(7)); // false OR NULL = NULL
            }
        }
    }

    // ===== Views =====

    @Test
    void createAndQueryView() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE v_base (id int, val text)");
            st.execute("INSERT INTO v_base VALUES (1,'a'),(2,'b')");
            st.execute("CREATE VIEW v_test AS SELECT id, val FROM v_base WHERE id = 1");
            try (ResultSet rs = st.executeQuery("SELECT * FROM v_test")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("a", rs.getString(2));
                assertFalse(rs.next());
            }
            st.execute("DROP VIEW v_test");
            st.execute("DROP TABLE v_base");
        }
    }

    // ===== Triggers =====

    @Test
    void basicTrigger() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE trig_log (msg text)");
            st.execute("CREATE TABLE trig_test (id int, val text)");
            st.execute("CREATE FUNCTION trig_fn() RETURNS trigger AS $$ " +
                    "BEGIN INSERT INTO trig_log VALUES ('fired'); RETURN NEW; END $$ LANGUAGE plpgsql");
            st.execute("CREATE TRIGGER trg BEFORE INSERT ON trig_test FOR EACH ROW EXECUTE FUNCTION trig_fn()");
            st.execute("INSERT INTO trig_test VALUES (1, 'x')");
            try (ResultSet rs = st.executeQuery("SELECT msg FROM trig_log")) {
                assertTrue(rs.next());
                assertEquals("fired", rs.getString(1));
            }
            st.execute("DROP TRIGGER trg ON trig_test");
            st.execute("DROP TABLE trig_test");
            st.execute("DROP TABLE trig_log");
            st.execute("DROP FUNCTION trig_fn");
        }
    }

    // ===== Sequences =====

    @Test
    void sequenceOperations() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE SEQUENCE test_seq START 10 INCREMENT 5");
            try (ResultSet rs = st.executeQuery("SELECT nextval('test_seq')")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getLong(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT nextval('test_seq')")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getLong(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT currval('test_seq')")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getLong(1));
            }
            st.execute("SELECT setval('test_seq', 100)");
            try (ResultSet rs = st.executeQuery("SELECT nextval('test_seq')")) {
                assertTrue(rs.next());
                assertEquals(105, rs.getLong(1));
            }
            st.execute("DROP SEQUENCE test_seq");
        }
    }
}
