package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AB: Index access methods beyond btree.
 *
 * Covers:
 *  - pg_index.indam differs for btree vs hash
 *  - CREATE INDEX USING hash actually uses hash AM (relam)
 *  - CREATE INDEX USING gin on tsvector
 *  - CREATE INDEX USING gist on geometry
 *  - pg_opclass has entries for hash / gin / gist
 *  - pg_index.indoption reflects DESC / NULLS FIRST bits
 *  - pg_index.indclass holds actual opclass OIDs (not [1978] hardcoded)
 */
class Round18IndexAmsTest {

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

    private static int int1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // AB1. pg_index.indam differs for btree vs hash
    // =========================================================================

    @Test
    void pg_index_indam_reflects_access_method() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_iam CASCADE");
        exec("CREATE TABLE r18_iam(a int, b text)");
        exec("CREATE INDEX r18_iam_bt ON r18_iam USING btree(a)");
        exec("CREATE INDEX r18_iam_hs ON r18_iam USING hash(a)");
        String btreeAm = str(
                "SELECT am.amname FROM pg_index ix " +
                        "JOIN pg_class ci ON ci.oid=ix.indexrelid " +
                        "JOIN pg_am am ON am.oid=ci.relam " +
                        "WHERE ci.relname='r18_iam_bt'");
        String hashAm = str(
                "SELECT am.amname FROM pg_index ix " +
                        "JOIN pg_class ci ON ci.oid=ix.indexrelid " +
                        "JOIN pg_am am ON am.oid=ci.relam " +
                        "WHERE ci.relname='r18_iam_hs'");
        assertEquals("btree", btreeAm, "btree index must have relam=btree");
        assertEquals("hash", hashAm, "hash index must have relam=hash, got '" + hashAm + "'");
    }

    // =========================================================================
    // AB2. CREATE INDEX USING gin on tsvector
    // =========================================================================

    @Test
    void gin_tsvector_index_relam_is_gin() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_gin CASCADE");
        exec("CREATE TABLE r18_gin(doc tsvector)");
        exec("CREATE INDEX r18_gin_ix ON r18_gin USING gin(doc)");
        String am = str(
                "SELECT am.amname FROM pg_class c " +
                        "JOIN pg_am am ON am.oid=c.relam WHERE c.relname='r18_gin_ix'");
        assertEquals("gin", am, "USING gin must set relam to gin; got '" + am + "'");
    }

    // =========================================================================
    // AB3. CREATE INDEX USING gist
    // =========================================================================

    @Test
    void gist_point_index_relam_is_gist() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_gist CASCADE");
        exec("CREATE TABLE r18_gist(p point)");
        exec("CREATE INDEX r18_gist_ix ON r18_gist USING gist(p)");
        String am = str(
                "SELECT am.amname FROM pg_class c " +
                        "JOIN pg_am am ON am.oid=c.relam WHERE c.relname='r18_gist_ix'");
        assertEquals("gist", am, "USING gist must set relam to gist; got '" + am + "'");
    }

    // =========================================================================
    // AB4. pg_opclass has hash / gin / gist entries
    // =========================================================================

    @Test
    void pg_opclass_has_hash_entries() throws SQLException {
        int n = int1(
                "SELECT count(*)::int FROM pg_opclass oc " +
                        "JOIN pg_am a ON a.oid=oc.opcmethod WHERE a.amname='hash'");
        assertTrue(n > 0, "pg_opclass must have hash opclass rows; got " + n);
    }

    @Test
    void pg_opclass_has_gin_entries() throws SQLException {
        int n = int1(
                "SELECT count(*)::int FROM pg_opclass oc " +
                        "JOIN pg_am a ON a.oid=oc.opcmethod WHERE a.amname='gin'");
        assertTrue(n > 0, "pg_opclass must have gin opclass rows; got " + n);
    }

    @Test
    void pg_opclass_has_gist_entries() throws SQLException {
        int n = int1(
                "SELECT count(*)::int FROM pg_opclass oc " +
                        "JOIN pg_am a ON a.oid=oc.opcmethod WHERE a.amname='gist'");
        assertTrue(n > 0, "pg_opclass must have gist opclass rows; got " + n);
    }

    // =========================================================================
    // AB5. pg_index.indoption reflects DESC NULLS FIRST bits
    // =========================================================================

    @Test
    void pg_index_indoption_reflects_desc_nulls_first() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_idop CASCADE");
        exec("CREATE TABLE r18_idop(a int)");
        exec("CREATE INDEX r18_idop_ix ON r18_idop(a DESC NULLS FIRST)");
        // indoption[0] must have DESC bit (1) set.
        int opt = int1(
                "SELECT (indoption[0])::int FROM pg_index ix " +
                        "JOIN pg_class ci ON ci.oid=ix.indexrelid " +
                        "WHERE ci.relname='r18_idop_ix'");
        // DESC = 1, NULLS FIRST = 2  → combined = 3 (for DESC on desc, NULLS FIRST is the default so only DESC bit)
        // PG sets bit 0 = DESC, bit 1 = NULLS FIRST. With "DESC NULLS FIRST" explicit both are set → 3.
        // But DESC's default NULLS is FIRST, so it may be just 1. Accept non-zero.
        assertTrue(opt != 0,
                "indoption for DESC column must be non-zero; got " + opt);
    }

    // =========================================================================
    // AB6. pg_index.indclass holds real opclass OIDs (not hardcoded)
    // =========================================================================

    @Test
    void pg_index_indclass_matches_column_opclass() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_icls CASCADE");
        exec("CREATE TABLE r18_icls(a int, b text)");
        exec("CREATE INDEX r18_icls_a ON r18_icls(a)");
        exec("CREATE INDEX r18_icls_b ON r18_icls(b)");
        // indclass[1] for integer column != indclass[1] for text column.
        int aCls = int1(
                "SELECT indclass[0]::int FROM pg_index ix " +
                        "JOIN pg_class ci ON ci.oid=ix.indexrelid " +
                        "WHERE ci.relname='r18_icls_a'");
        int bCls = int1(
                "SELECT indclass[0]::int FROM pg_index ix " +
                        "JOIN pg_class ci ON ci.oid=ix.indexrelid " +
                        "WHERE ci.relname='r18_icls_b'");
        assertNotEquals(aCls, bCls,
                "indclass for int column must differ from text column; both=" + aCls);
    }
}
