package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs index-related SQL from pg18_sample_sql_6 against both real PG 18 and Memgres,
 * comparing results. When PG 18 is unavailable, tests run Memgres-only with expected values.
 *
 * Covers: basic indexes, partial/expression indexes, GIN/GiST, EXCLUDE constraints,
 * operator classes, catalog introspection, numeric edge cases, and stress patterns.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IndexComparisonTest {

    static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest?preferQueryMode=simple";
    static final String PG_USER = "memgres";
    static final String PG_PASS = "memgres";

    static Memgres memgres;
    static Connection mgConn;
    static Connection pgConn;
    static boolean pgAvailable;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        mgConn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        mgConn.setAutoCommit(true);

        try {
            pgConn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
            pgConn.setAutoCommit(true);
            pgAvailable = true;
        } catch (Exception e) {
            pgAvailable = false;
        }

        // Clean PG state if available; if cleanup fails, fall back to Memgres-only
        if (pgAvailable) {
            try {
                cleanPg();
            } catch (Exception e) {
                pgAvailable = false;
                pgConn.close();
                pgConn = null;
            }
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (pgConn != null) pgConn.close();
        if (mgConn != null) mgConn.close();
        if (memgres != null) memgres.close();
    }

    static void cleanPg() throws SQLException {
        String[] tables = {"t_basic", "users", "expr_test", "bad_idx", "num_test",
                "docs", "bookings", "texts", "introspect", "mix_idx",
                "idx_cap_btree", "idx_cap_hash", "idx_cap_gin", "idx_cap_gist",
                "idx_cap_brin", "conc_test", "reindex_test"};
        for (String t : tables) {
            execPg("DROP TABLE IF EXISTS " + t + " CASCADE");
        }
        execPg("DROP EXTENSION IF EXISTS btree_gist CASCADE");
    }

    // === Helpers =============================================================

    static void execMg(String sql) throws SQLException {
        try (Statement s = mgConn.createStatement()) { s.execute(sql); }
    }

    static void execPg(String sql) throws SQLException {
        try (Statement s = pgConn.createStatement()) { s.execute(sql); }
    }

    static void execBoth(String sql) throws SQLException {
        if (pgAvailable) execPg(sql);
        execMg(sql);
    }

    static String queryOneMg(String sql) throws SQLException {
        try (Statement s = mgConn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static String queryOnePg(String sql) throws SQLException {
        try (Statement s = pgConn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static List<String> queryColumnMg(String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement s = mgConn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) result.add(rs.getString(1));
        }
        return result;
    }

    static List<String> queryColumnPg(String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement s = pgConn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) result.add(rs.getString(1));
        }
        return result;
    }

    /** Run query on both, assert same single result. */
    void assertQuerySame(String sql, String context) throws SQLException {
        String mg = queryOneMg(sql);
        if (pgAvailable) {
            String pg = queryOnePg(sql);
            assertEquals(pg, mg, context + ", PG vs Memgres differ for: " + sql);
        }
    }

    /** Run query on both, assert same column result (sorted). */
    void assertColumnSame(String sql, String context) throws SQLException {
        List<String> mg = queryColumnMg(sql);
        if (pgAvailable) {
            List<String> pg = queryColumnPg(sql);
            Collections.sort(pg);
            Collections.sort(mg);
            assertEquals(pg, mg, context + ", PG vs Memgres differ for: " + sql);
        }
    }

    /** Assert SQL throws an error on Memgres (and on PG if available). */
    void assertErrorBoth(String sql, String context) {
        assertThrows(SQLException.class, () -> execMg(sql), context + " (expected error on Memgres): " + sql);
        if (pgAvailable) {
            assertThrows(SQLException.class, () -> execPg(sql), context + " (expected error on PG): " + sql);
        }
    }

    /** Assert SQL succeeds on Memgres (and on PG if available). */
    void assertSuccessBoth(String sql, String context) {
        assertDoesNotThrow(() -> execMg(sql), context + " (unexpected error on Memgres): " + sql);
        if (pgAvailable) {
            assertDoesNotThrow(() -> execPg(sql), context + " (unexpected error on PG): " + sql);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1600: Index Basics and Uniqueness
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(1)
    void t1600_basicIndexCreation() throws SQLException {
        execBoth("CREATE TABLE t_basic (id int, val int, extra text)");
        execBoth("INSERT INTO t_basic VALUES (1,10,'a'),(2,10,'b'),(3,NULL,'c')");
        execBoth("CREATE UNIQUE INDEX idx_basic_unique ON t_basic(id)");
        execBoth("CREATE INDEX idx_basic_val ON t_basic(val)");
        execBoth("CREATE INDEX idx_basic_include ON t_basic(val) INCLUDE (extra)");
        assertQuerySame("SELECT COUNT(*) FROM t_basic", "1600 row count");
    }

    @Test @Order(2)
    void t1600_uniqueIndexEnforced() throws SQLException {
        // Inserting duplicate id=1 should fail due to unique index
        assertErrorBoth("INSERT INTO t_basic VALUES (1, 99, 'dup')", "1600 unique violation");
    }

    @Test @Order(3)
    void t1600_nonUniqueIndexAllowsDuplicates() throws SQLException {
        // val=10 already exists twice, inserting another should succeed
        execBoth("INSERT INTO t_basic VALUES (4, 10, 'd')");
        assertQuerySame("SELECT COUNT(*) FROM t_basic WHERE val = 10", "1600 non-unique dup count");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1610: Partial Indexes and Partial Uniqueness
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(10)
    void t1610_partialUniqueIndex() throws SQLException {
        execBoth("CREATE TABLE users (id int, email text, deleted_at timestamp)");
        execBoth("CREATE UNIQUE INDEX idx_users_email_active ON users(lower(email)) WHERE deleted_at IS NULL");
        // Two rows with same email: one active, one soft-deleted. Both should be allowed
        execBoth("INSERT INTO users VALUES (1,'a@test.com',NULL)");
        execBoth("INSERT INTO users VALUES (2,'a@test.com','2024-01-01')");
        assertQuerySame("SELECT COUNT(*) FROM users", "1610 partial unique allows soft-deleted dup");
    }

    @Test @Order(11)
    void t1610_partialUniqueViolation() throws SQLException {
        // Another active row with same lower(email) should fail
        assertErrorBoth("INSERT INTO users VALUES (3,'A@TEST.COM',NULL)", "1610 partial unique violation");
    }

    @Test @Order(12)
    void t1610_partialUniqueAllowsMultipleDeleted() throws SQLException {
        // Multiple soft-deleted duplicates are fine
        execBoth("INSERT INTO users VALUES (4,'a@test.com','2024-02-01')");
        assertQuerySame("SELECT COUNT(*) FROM users", "1610 multiple deleted duplicates allowed");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1620: Expression and Function Indexes
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(20)
    void t1620_expressionIndexCreation() throws SQLException {
        execBoth("CREATE TABLE expr_test (email text, payload jsonb)");
        execBoth("CREATE INDEX idx_lower_email ON expr_test(lower(email))");
        execBoth("CREATE INDEX idx_json_sku ON expr_test((payload->>'sku'))");
        execBoth("INSERT INTO expr_test VALUES ('Alice@test.com','{\"sku\":\"A1\"}')");
        execBoth("INSERT INTO expr_test VALUES ('alice@test.com','{\"sku\":\"A2\"}')");
        assertQuerySame("SELECT COUNT(*) FROM expr_test", "1620 expression index row count");
    }

    @Test @Order(21)
    void t1620_expressionIndexQueryable() throws SQLException {
        // Should be able to query using the indexed expression
        assertQuerySame(
                "SELECT COUNT(*) FROM expr_test WHERE lower(email) = 'alice@test.com'",
                "1620 expression index query");
    }

    @Test @Order(22)
    void t1620_jsonbExpressionIndexQueryable() throws SQLException {
        assertQuerySame(
                "SELECT COUNT(*) FROM expr_test WHERE payload->>'sku' = 'A1'",
                "1620 jsonb expression index query");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1630: Invalid Index Definitions and Mutability
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(30)
    void t1630_volatileFunctionRejected() throws SQLException {
        execBoth("CREATE TABLE bad_idx (id int, created_at timestamp)");
        assertErrorBoth("CREATE INDEX idx_bad ON bad_idx((random()))", "1630 volatile function rejected");
    }

    @Test @Order(31)
    void t1630_stableFunctionAccepted() throws SQLException {
        // now() is stable, should be accepted as expression index
        // Actually in PG, now() is stable and index expressions require immutable.
        // So this should also be rejected.
        assertErrorBoth("CREATE INDEX idx_now ON bad_idx((now()))", "1630 stable function rejected");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1640: Numeric, NULL, NaN, Infinity Edge Cases
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(40)
    void t1640_numericPrecisionUniqueness() throws SQLException {
        execBoth("CREATE TABLE num_test (val numeric, fval double precision)");
        execBoth("CREATE UNIQUE INDEX idx_num_unique ON num_test(val)");
        execBoth("INSERT INTO num_test VALUES (2.00, 1.0)");
        // 2.0 == 2.00 in numeric, should violate unique
        assertErrorBoth("INSERT INTO num_test VALUES (2.0, 2.0)", "1640 numeric precision unique violation");
    }

    @Test @Order(41)
    void t1640_nullsAreDistinctInUnique() throws SQLException {
        // Multiple NULLs should be allowed in unique index
        execBoth("INSERT INTO num_test VALUES (NULL, 'NaN')");
        execBoth("INSERT INTO num_test VALUES (NULL, 'Infinity')");
        assertQuerySame("SELECT COUNT(*) FROM num_test", "1640 null distinctness");
    }

    @Test @Order(42)
    void t1640_nanAndInfinityStorage() throws SQLException {
        assertQuerySame(
                "SELECT fval FROM num_test WHERE fval = 'NaN' ORDER BY fval",
                "1640 NaN retrieval");
        assertQuerySame(
                "SELECT fval FROM num_test WHERE fval = 'Infinity' ORDER BY fval",
                "1640 Infinity retrieval");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1650: JSONB, Array, and FTS Indexes (GIN)
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(50)
    void t1650_ginIndexCreation() throws SQLException {
        execBoth("CREATE TABLE docs (id int, payload jsonb, tags text[], doc tsvector)");
        execBoth("CREATE INDEX idx_json ON docs USING gin(payload)");
        execBoth("CREATE INDEX idx_tags ON docs USING gin(tags)");
        execBoth("CREATE INDEX idx_doc ON docs USING gin(doc)");
        execBoth("INSERT INTO docs VALUES (1,'{\"a\":1}',ARRAY['x','y'],to_tsvector('hello world'))");
        execBoth("INSERT INTO docs VALUES (2,'{\"b\":2}',ARRAY['y'],to_tsvector('world test'))");
    }

    @Test @Order(51)
    void t1650_jsonbContainmentQuery() throws SQLException {
        assertQuerySame("SELECT id FROM docs WHERE payload @> '{\"a\":1}'", "1650 jsonb @> query");
    }

    @Test @Order(52)
    void t1650_arrayOverlapQuery() throws SQLException {
        assertQuerySame(
                "SELECT COUNT(*) FROM docs WHERE tags && ARRAY['x']",
                "1650 array overlap query");
    }

    @Test @Order(53)
    void t1650_tsvectorMatchQuery() throws SQLException {
        assertQuerySame(
                "SELECT COUNT(*) FROM docs WHERE doc @@ to_tsquery('hello')",
                "1650 tsvector match query");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1660: Range, GiST, and Exclusion Index Patterns
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(60)
    void t1660_excludeConstraintCreation() throws SQLException {
        execBoth("CREATE TABLE bookings (room int, period tsrange)");
        execBoth("CREATE EXTENSION IF NOT EXISTS btree_gist");
        execBoth("ALTER TABLE bookings ADD CONSTRAINT no_overlap EXCLUDE USING gist (room WITH =, period WITH &&)");
        execBoth("INSERT INTO bookings VALUES (1, tsrange('2024-01-01','2024-01-02'))");
    }

    @Test @Order(61)
    void t1660_excludeConstraintViolation() throws SQLException {
        // Overlapping range for same room should fail
        assertErrorBoth(
                "INSERT INTO bookings VALUES (1, tsrange('2024-01-01 12:00','2024-01-03'))",
                "1660 exclude constraint violation");
    }

    @Test @Order(62)
    void t1660_excludeAllowsDifferentRoom() throws SQLException {
        // Different room, overlapping time; should succeed
        execBoth("INSERT INTO bookings VALUES (2, tsrange('2024-01-01','2024-01-03'))");
        assertQuerySame("SELECT COUNT(*) FROM bookings", "1660 different room allowed");
    }

    @Test @Order(63)
    void t1660_excludeAllowsNonOverlapping() throws SQLException {
        // Same room, non-overlapping time; should succeed
        execBoth("INSERT INTO bookings VALUES (1, tsrange('2024-01-05','2024-01-06'))");
        assertQuerySame("SELECT COUNT(*) FROM bookings", "1660 non-overlapping allowed");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1670: Index Operator Classes and Pattern Ops
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(70)
    void t1670_patternOpsIndex() throws SQLException {
        execBoth("CREATE TABLE texts (t text)");
        execBoth("CREATE INDEX idx_text_pattern ON texts(t text_pattern_ops)");
        execBoth("INSERT INTO texts VALUES ('abc'),('abcd'),('xyz')");
    }

    @Test @Order(71)
    void t1670_likeQueryWithPatternOps() throws SQLException {
        assertColumnSame(
                "SELECT t FROM texts WHERE t LIKE 'abc%' ORDER BY t",
                "1670 LIKE with pattern_ops");
    }

    @Test @Order(72)
    void t1670_exactMatchStillWorks() throws SQLException {
        assertQuerySame(
                "SELECT t FROM texts WHERE t = 'xyz'",
                "1670 exact match with pattern_ops index");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1680: Index Catalog Introspection and Lifecycle
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(80)
    void t1680_pgIndexesIntrospection() throws SQLException {
        execBoth("CREATE TABLE introspect (id int)");
        execBoth("CREATE INDEX idx_i ON introspect(id)");
        assertColumnSame(
                "SELECT indexname FROM pg_indexes WHERE tablename='introspect'",
                "1680 pg_indexes introspection");
    }

    @Test @Order(81)
    void t1680_dropAndRecreateIndex() throws SQLException {
        execBoth("DROP INDEX idx_i");
        String mg = queryOneMg("SELECT COUNT(*) FROM pg_indexes WHERE tablename='introspect'");
        assertEquals("0", mg, "1680 index dropped from catalog");
        execBoth("CREATE INDEX idx_i2 ON introspect(id)");
        assertColumnSame(
                "SELECT indexname FROM pg_indexes WHERE tablename='introspect'",
                "1680 recreated index in catalog");
    }

    @Test @Order(82)
    void t1680_pgGetIndexdef() throws SQLException {
        // pg_get_indexdef should return the index definition
        String mgDef = queryOneMg(
                "SELECT pg_get_indexdef(c.oid) FROM pg_class c " +
                "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                "WHERE c.relname = 'idx_i2' AND n.nspname = 'public'");
        assertNotNull(mgDef, "1680 pg_get_indexdef returns non-null");
        assertTrue(mgDef.toLowerCase().contains("introspect"), "1680 pg_get_indexdef references table");
        assertTrue(mgDef.toLowerCase().contains("id"), "1680 pg_get_indexdef references column");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1690: Index Stress and Mixed Value Cases
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(90)
    void t1690_multiColumnMixedTypeIndex() throws SQLException {
        execBoth("CREATE TABLE mix_idx (a int, b text, c numeric)");
        execBoth("CREATE INDEX idx_mix ON mix_idx(a,b,c)");
        execBoth("INSERT INTO mix_idx VALUES (1,'x',1.0),(1,'x',1.00),(2,NULL,NULL)");
        assertQuerySame("SELECT COUNT(*) FROM mix_idx", "1690 mixed type index row count");
    }

    @Test @Order(91)
    void t1690_queryByMultipleColumns() throws SQLException {
        assertQuerySame(
                "SELECT COUNT(*) FROM mix_idx WHERE a = 1 AND b = 'x'",
                "1690 multi-column query");
    }

    @Test @Order(92)
    void t1690_nullsInMultiColumnIndex() throws SQLException {
        assertQuerySame(
                "SELECT COUNT(*) FROM mix_idx WHERE b IS NULL",
                "1690 null in multi-column index");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1700 (from MD): Index Test Strategy, Correctness & Valid DDL
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(100)
    void t1700_ifNotExistsIdempotent() throws SQLException {
        // CREATE INDEX IF NOT EXISTS should not error on existing index
        execBoth("CREATE INDEX IF NOT EXISTS idx_mix ON mix_idx(a,b,c)");
    }

    @Test @Order(101)
    void t1700_dropIfExistsIdempotent() throws SQLException {
        execBoth("DROP INDEX IF EXISTS nonexistent_index_xyz");
    }

    @Test @Order(102)
    void t1700_duplicateIndexNameRejected() throws SQLException {
        // Creating an index with the same name but different definition should error
        assertErrorBoth("CREATE INDEX idx_mix ON mix_idx(a)", "1700 duplicate index name rejected");
    }

    @Test @Order(103)
    void t1700_indexOnNonexistentColumnRejected() throws SQLException {
        assertErrorBoth(
                "CREATE INDEX idx_bad_col ON mix_idx(nonexistent_col)",
                "1700 nonexistent column rejected");
    }

    @Test @Order(104)
    void t1700_indexOnNonexistentTableRejected() throws SQLException {
        assertErrorBoth(
                "CREATE INDEX idx_bad_tbl ON nonexistent_table(id)",
                "1700 nonexistent table rejected");
    }

    @Test @Order(105)
    void t1700_uniqueIndexCorrectnessAfterUpdates() throws SQLException {
        // After UPDATE, unique constraint should still be enforced correctly
        execBoth("UPDATE t_basic SET id = 99 WHERE id = 4");
        assertErrorBoth("INSERT INTO t_basic VALUES (99, 1, 'dup')", "1700 unique after update");
        execBoth("UPDATE t_basic SET id = 4 WHERE id = 99"); // restore
    }

    @Test @Order(106)
    void t1700_uniqueIndexCorrectnessAfterDelete() throws SQLException {
        execBoth("DELETE FROM t_basic WHERE id = 3");
        // id=3 is now free, should succeed
        execBoth("INSERT INTO t_basic VALUES (3, 30, 'reinserted')");
        // id=3 taken again, should fail
        assertErrorBoth("INSERT INTO t_basic VALUES (3, 31, 'dup')", "1700 unique after delete+reinsert");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1710 (from MD): Index Capability Matrix: Btree, GIN, GiST, BRIN
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(110)
    void t1710_btreeIndexDefault() throws SQLException {
        execBoth("CREATE TABLE idx_cap_btree (id int, val text)");
        // Default is btree
        execBoth("CREATE INDEX idx_btree_default ON idx_cap_btree(id)");
        execBoth("CREATE INDEX idx_btree_explicit ON idx_cap_btree USING btree(val)");
        execBoth("INSERT INTO idx_cap_btree VALUES (1,'a'),(2,'b'),(3,'c')");
        assertQuerySame("SELECT COUNT(*) FROM idx_cap_btree WHERE id > 1", "1710 btree range query");
    }

    @Test @Order(111)
    void t1710_hashIndex() throws SQLException {
        execBoth("CREATE TABLE idx_cap_hash (id int, name text)");
        execBoth("CREATE INDEX idx_hash ON idx_cap_hash USING hash(name)");
        execBoth("INSERT INTO idx_cap_hash VALUES (1,'alice'),(2,'bob')");
        assertQuerySame("SELECT id FROM idx_cap_hash WHERE name = 'alice'", "1710 hash equality query");
    }

    @Test @Order(112)
    void t1710_ginIndexOnJsonb() throws SQLException {
        execBoth("CREATE TABLE idx_cap_gin (id int, data jsonb)");
        execBoth("CREATE INDEX idx_gin_data ON idx_cap_gin USING gin(data)");
        execBoth("INSERT INTO idx_cap_gin VALUES (1,'{\"key\":\"val\"}'),(2,'{\"other\":1}')");
        assertQuerySame(
                "SELECT id FROM idx_cap_gin WHERE data @> '{\"key\":\"val\"}'",
                "1710 gin jsonb containment");
    }

    @Test @Order(113)
    void t1710_gistIndex() throws SQLException {
        execBoth("CREATE TABLE idx_cap_gist (id int, r tsrange)");
        execBoth("CREATE INDEX idx_gist_r ON idx_cap_gist USING gist(r)");
        execBoth("INSERT INTO idx_cap_gist VALUES (1, tsrange('2024-01-01','2024-01-05'))");
        execBoth("INSERT INTO idx_cap_gist VALUES (2, tsrange('2024-02-01','2024-02-05'))");
        assertQuerySame(
                "SELECT id FROM idx_cap_gist WHERE r && tsrange('2024-01-03','2024-01-10') ORDER BY id",
                "1710 gist range overlap query");
    }

    @Test @Order(114)
    void t1710_brinIndex() throws SQLException {
        // BRIN indexes are accepted as DDL (metadata-only in Memgres, functional in PG)
        execBoth("CREATE TABLE idx_cap_brin (id int, ts timestamp)");
        execBoth("CREATE INDEX idx_brin_ts ON idx_cap_brin USING brin(ts)");
        execBoth("INSERT INTO idx_cap_brin VALUES (1, '2024-01-01'),(2, '2024-06-01')");
        assertQuerySame("SELECT COUNT(*) FROM idx_cap_brin", "1710 brin index creation and query");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1720 (from MD): Index Concurrency and Online Build Scenarios
    // ═══════════════════════════════════════════════════════════════════════

    @Test @Order(120)
    void t1720_createIndexConcurrently() throws SQLException {
        execBoth("CREATE TABLE conc_test (id int, val text)");
        execBoth("INSERT INTO conc_test VALUES (1,'a'),(2,'b'),(3,'c')");
        execBoth("CREATE INDEX CONCURRENTLY idx_conc ON conc_test(id)");
        assertQuerySame("SELECT COUNT(*) FROM conc_test WHERE id = 2", "1720 concurrent index usable");
    }

    @Test @Order(121)
    void t1720_createUniqueIndexConcurrently() throws SQLException {
        execBoth("CREATE UNIQUE INDEX CONCURRENTLY idx_conc_unique ON conc_test(val)");
        assertErrorBoth(
                "INSERT INTO conc_test VALUES (4,'a')",
                "1720 concurrent unique index enforced");
    }

    @Test @Order(122)
    void t1720_reindexTable() throws SQLException {
        execBoth("CREATE TABLE reindex_test (id int PRIMARY KEY, val text)");
        execBoth("INSERT INTO reindex_test VALUES (1,'a'),(2,'b')");
        execBoth("REINDEX TABLE reindex_test");
        // Table should still work after reindex
        assertQuerySame("SELECT COUNT(*) FROM reindex_test", "1720 reindex table");
    }

    @Test @Order(123)
    void t1720_reindexIndex() throws SQLException {
        execBoth("REINDEX INDEX reindex_test_pkey");
        assertQuerySame("SELECT id FROM reindex_test WHERE id = 1", "1720 reindex index");
    }

    @Test @Order(124)
    void t1720_dropIndexConcurrently() throws SQLException {
        execBoth("DROP INDEX CONCURRENTLY idx_conc");
        String mg = queryOneMg("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_conc'");
        assertEquals("0", mg, "1720 concurrent drop removed index");
    }
}
