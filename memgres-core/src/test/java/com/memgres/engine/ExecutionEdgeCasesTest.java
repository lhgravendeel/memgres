package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 22 failures from execution-edge-cases.sql where Memgres diverges from PG 18.
 *
 * G1: SECURITY INVOKER views (Stmts 7-13, 17, 23)
 *   Stmt  7 - SELECT * FROM edge_invoker_view: Memgres errors "relation does not exist"
 *   Stmt  8 - reloptions check for edge_invoker_view: Memgres returns 0 rows
 *   Stmt  9 - count(*) from edge_invoker_view: Memgres errors "relation does not exist"
 *   Stmt 10 - WHERE clause on edge_invoker_view: Memgres errors "relation does not exist"
 *   Stmt 11 - max(id) from edge_invoker_view: Memgres errors "relation does not exist"
 *   Stmt 13 - count(*) from edge_definer_view: Memgres errors "relation does not exist"
 *   Stmt 17 - reloptions check after ALTER VIEW SET security_invoker: Memgres returns NULL
 *   Stmt 23 - SELECT * FROM edge_join_view: Memgres errors "relation does not exist"
 *
 * G2: Composite-type field UPDATE (Stmts 30, 32-34, 38, 41)
 *   Stmt 30 - pos after SET pos.x = 99: Memgres returns (10,20) instead of (99,20)
 *   Stmt 32 - pos after SET pos.x=50, pos.y=60: Memgres returns (30,40) instead of (50,60)
 *   Stmt 33 - (pos).x, (pos).y after field update: Memgres returns 10,20 instead of 99,20
 *   Stmt 34 - WHERE (pos).x = 99: Memgres returns 0 rows instead of 1
 *   Stmt 38 - pos after SET pos.x = 77 (from NULL): Memgres returns (,100) instead of (77,100)
 *   Stmt 41 - ORDER BY (pos).x ASC: Memgres returns wrong order
 *
 * G3: CREATE AGGREGATE catalog verification (Stmts 50-52, 56-58, 62, 68)
 *   Stmt 50 - pg_aggregate has entry for edge_sum_agg: Memgres returns NULL
 *   Stmt 51 - agginitval for edge_sum_agg: Memgres returns 0 rows
 *   Stmt 52 - aggtransfn for edge_sum_agg: Memgres returns 0 rows
 *   Stmt 56 - aggfinalfn for edge_doublesum_agg: Memgres returns 0 rows
 *   Stmt 57 - 'min'::regproc should error (ambiguous), Memgres succeeds
 *   Stmt 58 - aggcombinefn column exists in pg_aggregate: Memgres returns NULL
 *   Stmt 62 - agginitval IS NULL for edge_strcat_agg: Memgres returns 0 rows
 *   Stmt 68 - prokind='a' for edge_sum_agg in pg_proc: Memgres returns 0 rows
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExecutionEdgeCasesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS edge_test CASCADE");
            s.execute("CREATE SCHEMA edge_test");
            s.execute("SET search_path = edge_test, public");

            // G1 setup: SECURITY INVOKER views
            s.execute("CREATE TABLE edge_data (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO edge_data VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            // These WITH (security_invoker=...) views may fail to parse in Memgres;
            // wrap each in try-catch so remaining setup continues.
            try {
                s.execute("CREATE VIEW edge_invoker_view WITH (security_invoker = true) AS "
                        + "SELECT * FROM edge_data");
            } catch (SQLException ignored) { }

            try {
                s.execute("CREATE VIEW edge_definer_view WITH (security_invoker = false) AS "
                        + "SELECT * FROM edge_data");
            } catch (SQLException ignored) { }

            s.execute("CREATE VIEW edge_default_view AS SELECT * FROM edge_data");

            s.execute("CREATE TABLE edge_labels (id integer PRIMARY KEY, label text)");
            s.execute("INSERT INTO edge_labels VALUES (1, 'first'), (2, 'second'), (3, 'third')");

            try {
                s.execute("CREATE VIEW edge_join_view WITH (security_invoker = true) AS "
                        + "SELECT d.id, d.val, l.label FROM edge_data d JOIN edge_labels l ON d.id = l.id");
            } catch (SQLException ignored) { }

            // G2 setup: Composite-type field UPDATE
            s.execute("CREATE TYPE edge_point AS (x integer, y integer)");
            s.execute("CREATE TABLE edge_shapes (id integer PRIMARY KEY, pos edge_point)");
            s.execute("INSERT INTO edge_shapes VALUES (1, ROW(10, 20)::edge_point)");
            s.execute("INSERT INTO edge_shapes VALUES (2, ROW(30, 40)::edge_point)");

            // G3 setup: CREATE AGGREGATE
            s.execute("CREATE FUNCTION edge_int_add(integer, integer) RETURNS integer "
                    + "LANGUAGE sql AS $$ SELECT COALESCE($1, 0) + $2; $$");

            s.execute("CREATE AGGREGATE edge_sum_agg(integer) ("
                    + "SFUNC = edge_int_add, STYPE = integer, INITCOND = '0')");

            s.execute("CREATE FUNCTION edge_double(integer) RETURNS integer "
                    + "LANGUAGE sql AS $$ SELECT $1 * 2; $$");

            s.execute("CREATE AGGREGATE edge_doublesum_agg(integer) ("
                    + "SFUNC = edge_int_add, STYPE = integer, INITCOND = '0', "
                    + "FINALFUNC = edge_double)");

            s.execute("CREATE FUNCTION edge_text_cat(text, text) RETURNS text "
                    + "LANGUAGE sql AS $$ SELECT COALESCE($1 || ',', '') || $2; $$");

            s.execute("CREATE AGGREGATE edge_strcat_agg(text) ("
                    + "SFUNC = edge_text_cat, STYPE = text)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS edge_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // G1: SECURITY INVOKER Views
    // ========================================================================

    /**
     * Stmt 7: SELECT * FROM edge_invoker_view ORDER BY id
     *
     * PG: returns 3 rows [(1,a), (2,b), (3,c)]
     * Memgres: ERROR relation "edge_invoker_view" does not exist
     */
    @Test
    @Order(1)
    void stmt07_selectFromSecurityInvokerView() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM edge_invoker_view ORDER BY id")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(1, rs.getInt("id"));
            assertEquals("a", rs.getString("val"));

            assertTrue(rs.next(), "Expected row 2");
            assertEquals(2, rs.getInt("id"));
            assertEquals("b", rs.getString("val"));

            assertTrue(rs.next(), "Expected row 3");
            assertEquals(3, rs.getInt("id"));
            assertEquals("c", rs.getString("val"));

            assertFalse(rs.next(), "Expected exactly 3 rows");
        }
    }

    /**
     * Stmt 8: Verify security_invoker=true in pg_class reloptions for edge_invoker_view.
     *
     * PG: returns 1 row with has_invoker = true
     * Memgres: returns 0 rows
     */
    @Test
    @Order(2)
    void stmt08_reloptionsSecurityInvokerTrue() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT reloptions @> ARRAY['security_invoker=true'] AS has_invoker "
                     + "FROM pg_class WHERE relname = 'edge_invoker_view'")) {
            assertTrue(rs.next(), "Expected one row from pg_class for edge_invoker_view");
            assertTrue(rs.getBoolean("has_invoker"),
                    "reloptions should contain security_invoker=true");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 9: count(*) from edge_invoker_view.
     *
     * PG: returns cnt = 3
     * Memgres: ERROR relation "edge_invoker_view" does not exist
     */
    @Test
    @Order(3)
    void stmt09_countFromSecurityInvokerView() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::integer AS cnt FROM edge_invoker_view")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(3, rs.getInt("cnt"), "edge_invoker_view should have 3 rows");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 10: WHERE clause on edge_invoker_view.
     *
     * PG: returns (2, b)
     * Memgres: ERROR relation "edge_invoker_view" does not exist
     */
    @Test
    @Order(4)
    void stmt10_whereClauseOnSecurityInvokerView() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM edge_invoker_view WHERE id = 2")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(2, rs.getInt("id"));
            assertEquals("b", rs.getString("val"));
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 11: max(id) from edge_invoker_view.
     *
     * PG: returns max_id = 3
     * Memgres: ERROR relation "edge_invoker_view" does not exist
     */
    @Test
    @Order(5)
    void stmt11_aggregationOnSecurityInvokerView() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT max(id) AS max_id FROM edge_invoker_view")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(3, rs.getInt("max_id"));
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 13: count(*) from edge_definer_view (security_invoker = false).
     *
     * PG: returns cnt = 3
     * Memgres: ERROR relation "edge_definer_view" does not exist
     */
    @Test
    @Order(6)
    void stmt13_countFromSecurityDefinerView() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::integer AS cnt FROM edge_definer_view")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(3, rs.getInt("cnt"), "edge_definer_view should have 3 rows");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 17: After ALTER VIEW edge_default_view SET (security_invoker = true),
     * reloptions should contain security_invoker=true.
     *
     * PG: returns has_invoker = true
     * Memgres: returns has_invoker = NULL
     */
    @Test
    @Order(7)
    void stmt17_alterViewSetSecurityInvoker() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER VIEW edge_default_view SET (security_invoker = true)");
            try (ResultSet rs = s.executeQuery(
                    "SELECT reloptions @> ARRAY['security_invoker=true'] AS has_invoker "
                    + "FROM pg_class WHERE relname = 'edge_default_view'")) {
                assertTrue(rs.next(), "Expected one row from pg_class for edge_default_view");
                assertTrue(rs.getBoolean("has_invoker"),
                        "reloptions should contain security_invoker=true after ALTER VIEW");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    /**
     * Stmt 23: SELECT * FROM edge_join_view (security_invoker view with JOIN).
     *
     * PG: returns 3 rows [(1,a,first), (2,b,second), (3,c,third)]
     * Memgres: ERROR relation "edge_join_view" does not exist
     */
    @Test
    @Order(8)
    void stmt23_selectFromSecurityInvokerJoinView() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM edge_join_view ORDER BY id")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(1, rs.getInt("id"));
            assertEquals("a", rs.getString("val"));
            assertEquals("first", rs.getString("label"));

            assertTrue(rs.next(), "Expected row 2");
            assertEquals(2, rs.getInt("id"));
            assertEquals("b", rs.getString("val"));
            assertEquals("second", rs.getString("label"));

            assertTrue(rs.next(), "Expected row 3");
            assertEquals(3, rs.getInt("id"));
            assertEquals("c", rs.getString("val"));
            assertEquals("third", rs.getString("label"));

            assertFalse(rs.next(), "Expected exactly 3 rows");
        }
    }

    // ========================================================================
    // G2: Composite-Type Field UPDATE
    // ========================================================================

    /**
     * Stmt 30: After UPDATE edge_shapes SET pos.x = 99 WHERE id = 1,
     * pos should be (99,20).
     *
     * PG: (99,20)
     * Memgres: (10,20) -- field update not applied
     */
    @Test
    @Order(9)
    void stmt30_updateCompositeFieldX() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("UPDATE edge_shapes SET pos.x = 99 WHERE id = 1");
            try (ResultSet rs = s.executeQuery(
                    "SELECT pos FROM edge_shapes WHERE id = 1")) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals("(99,20)", rs.getString("pos"),
                        "pos.x should have been updated to 99");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    /**
     * Stmt 32: After UPDATE edge_shapes SET pos.x = 50, pos.y = 60 WHERE id = 2,
     * pos should be (50,60).
     *
     * PG: (50,60)
     * Memgres: (30,40) -- field updates not applied
     */
    @Test
    @Order(10)
    void stmt32_updateMultipleCompositeFields() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("UPDATE edge_shapes SET pos.x = 50, pos.y = 60 WHERE id = 2");
            try (ResultSet rs = s.executeQuery(
                    "SELECT pos FROM edge_shapes WHERE id = 2")) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals("(50,60)", rs.getString("pos"),
                        "Both pos.x and pos.y should have been updated");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    /**
     * Stmt 33: Read individual composite fields after field update on id=1.
     * Depends on stmt30 having run (pos.x = 99 for id=1).
     *
     * PG: x_val=99, y_val=20
     * Memgres: x_val=10, y_val=20
     */
    @Test
    @Order(11)
    void stmt33_readIndividualCompositeFields() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (pos).x AS x_val, (pos).y AS y_val FROM edge_shapes WHERE id = 1")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(99, rs.getInt("x_val"), "(pos).x should be 99 after field update");
            assertEquals(20, rs.getInt("y_val"), "(pos).y should remain 20");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 34: WHERE (pos).x = 99 should find id=1 after field update.
     *
     * PG: returns 1 row [id=1]
     * Memgres: returns 0 rows
     */
    @Test
    @Order(12)
    void stmt34_whereClauseOnCompositeField() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id FROM edge_shapes WHERE (pos).x = 99")) {
            assertTrue(rs.next(), "Expected one row where (pos).x = 99");
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 38: After inserting id=3 with (NULL,100) then UPDATE SET pos.x = 77,
     * pos should be (77,100).
     *
     * PG: (77,100)
     * Memgres: (,100) -- field update from NULL not applied
     */
    @Test
    @Order(13)
    void stmt38_updateCompositeFieldFromNull() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO edge_shapes VALUES (3, ROW(NULL, 100)::edge_point)");
            s.execute("UPDATE edge_shapes SET pos.x = 77 WHERE id = 3");
            try (ResultSet rs = s.executeQuery(
                    "SELECT pos FROM edge_shapes WHERE id = 3")) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals("(77,100)", rs.getString("pos"),
                        "pos.x should have been updated from NULL to 77");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    /**
     * Stmt 41: ORDER BY (pos).x ASC should produce [3, 2, 1] after all updates.
     * id=1: pos=(99,20), id=2: pos=(50,60), id=3: pos=(77,100)
     * Sorted by x ascending: 3(x=1 after whole-row update in SQL), but per the SQL flow:
     * After stmt 38 the state is id=3 x=77, and after whole-row update (stmt 40) x=1.
     * But we only test up to stmt 41 as per the SQL ordering.
     *
     * Per the SQL file, between stmt 38 and stmt 41 there are:
     *   stmt 39 (whole-row): UPDATE edge_shapes SET pos = ROW(1, 2)::edge_point WHERE id = 3
     *   stmt 40: SELECT (verified (1,2))
     * So at stmt 41: id=1 x=99, id=2 x=50, id=3 x=1 => ORDER BY x ASC: [3, 2, 1]
     *
     * PG: [3, 2, 1]
     * Memgres: [3, 1, 2] (wrong order because field updates were not applied)
     */
    @Test
    @Order(14)
    void stmt41_orderByCompositeField() throws Exception {
        try (Statement s = conn.createStatement()) {
            // Apply the intermediate whole-row update from the SQL file (stmt 39)
            s.execute("UPDATE edge_shapes SET pos = ROW(1, 2)::edge_point WHERE id = 3");

            try (ResultSet rs = s.executeQuery(
                    "SELECT id FROM edge_shapes ORDER BY (pos).x ASC")) {
                List<Integer> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                }
                assertEquals(List.of(3, 2, 1), ids,
                        "ORDER BY (pos).x ASC should yield [3(x=1), 2(x=50), 1(x=99)]");
            }
        }
    }

    // ========================================================================
    // G3: CREATE AGGREGATE Catalog Verification
    // ========================================================================

    /**
     * Stmt 50: pg_aggregate should have an entry for edge_sum_agg.
     *
     * PG: has_entry = true
     * Memgres: has_entry = NULL (count returns 0, so 0 > 0 is false... but actually NULL)
     */
    @Test
    @Order(15)
    void stmt50_pgAggregateHasEntryForEdgeSumAgg() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) > 0 AS has_entry FROM pg_aggregate "
                     + "WHERE aggfnoid = 'edge_sum_agg'::regproc")) {
            assertTrue(rs.next(), "Expected one row");
            assertTrue(rs.getBoolean("has_entry"),
                    "pg_aggregate should have an entry for edge_sum_agg");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 51: agginitval for edge_sum_agg should be '0'.
     *
     * PG: initval = 0
     * Memgres: 0 rows
     */
    @Test
    @Order(16)
    void stmt51_aggregateInitcondInCatalog() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT agginitval AS initval FROM pg_aggregate "
                     + "WHERE aggfnoid = 'edge_sum_agg'::regproc")) {
            assertTrue(rs.next(), "Expected one row for edge_sum_agg in pg_aggregate");
            assertEquals("0", rs.getString("initval"),
                    "agginitval should be '0' for edge_sum_agg");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 52: aggtransfn for edge_sum_agg should be 'edge_int_add'.
     *
     * PG: sfunc_name = edge_int_add
     * Memgres: 0 rows
     */
    @Test
    @Order(17)
    void stmt52_aggregateSfuncInCatalog() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT aggtransfn::text AS sfunc_name FROM pg_aggregate "
                     + "WHERE aggfnoid = 'edge_sum_agg'::regproc")) {
            assertTrue(rs.next(), "Expected one row for edge_sum_agg in pg_aggregate");
            assertEquals("edge_int_add", rs.getString("sfunc_name"),
                    "aggtransfn should be edge_int_add");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 56: aggfinalfn for edge_doublesum_agg should be non-zero (has a FINALFUNC).
     *
     * PG: has_finalfn = true
     * Memgres: 0 rows
     */
    @Test
    @Order(18)
    void stmt56_aggregateFinalfuncInCatalog() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT aggfinalfn <> 0 AS has_finalfn FROM pg_aggregate "
                     + "WHERE aggfnoid = 'edge_doublesum_agg'::regproc")) {
            assertTrue(rs.next(), "Expected one row for edge_doublesum_agg in pg_aggregate");
            assertTrue(rs.getBoolean("has_finalfn"),
                    "aggfinalfn should be non-zero for edge_doublesum_agg");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 57: 'min'::regproc is ambiguous and should error with 42725.
     *
     * PG: ERROR [42725] more than one function named "min"
     * Memgres: succeeds with 0 rows (does not raise the expected error)
     */
    @Test
    @Order(19)
    void stmt57_ambiguousRegprocShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT aggsortop IS NOT NULL AS has_sortop FROM pg_aggregate "
                         + "WHERE aggfnoid = 'min'::regproc AND aggtranstype = 'integer'::regtype")) {
                rs.next(); // force execution
            }
        }, "Expected error 42725 for ambiguous 'min'::regproc");
        assertEquals("42725", ex.getSQLState(),
                "Expected SQL state 42725 (more than one function named 'min')");
    }

    /**
     * Stmt 58: pg_attribute should have aggcombinefn column in pg_aggregate.
     *
     * PG: col_exists = true
     * Memgres: col_exists = NULL
     */
    @Test
    @Order(20)
    void stmt58_aggcombinefnColumnExists() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) > 0 AS col_exists FROM pg_attribute "
                     + "WHERE attrelid = 'pg_aggregate'::regclass AND attname = 'aggcombinefn'")) {
            assertTrue(rs.next(), "Expected one row");
            assertTrue(rs.getBoolean("col_exists"),
                    "pg_aggregate should have an aggcombinefn column");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 62: agginitval for edge_strcat_agg should be NULL (no INITCOND specified).
     *
     * PG: initval_null = true
     * Memgres: 0 rows
     */
    @Test
    @Order(21)
    void stmt62_aggregateWithNoInitcond() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT agginitval IS NULL AS initval_null FROM pg_aggregate "
                     + "WHERE aggfnoid = 'edge_strcat_agg'::regproc")) {
            assertTrue(rs.next(), "Expected one row for edge_strcat_agg in pg_aggregate");
            assertTrue(rs.getBoolean("initval_null"),
                    "agginitval should be NULL for edge_strcat_agg (no INITCOND)");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 68: pg_proc entry for edge_sum_agg should have prokind='a' (aggregate).
     *
     * PG: prokind = 'a'
     * Memgres: 0 rows
     */
    @Test
    @Order(22)
    void stmt68_pgProcProkindForAggregate() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT prokind FROM pg_proc "
                     + "WHERE proname = 'edge_sum_agg' AND pronamespace = 'edge_test'::regnamespace")) {
            assertTrue(rs.next(), "Expected one row for edge_sum_agg in pg_proc");
            assertEquals("a", rs.getString("prokind"),
                    "prokind should be 'a' (aggregate) for edge_sum_agg");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }
}
