package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Three questions a relation has to answer about itself: what it inherited, what it declared of
 * its own, and what it is called.
 *
 * <p>None of them was answered the way PostgreSQL answers it. An index declared on a partitioned
 * table was copied onto its direct partitions and no further, so a leaf under an intermediate
 * partitioned table was left unindexed, and a DETACH left the withdrawn partition's index still
 * recorded as a child of the index it came from. A CREATE INDEX with no name registered nothing at
 * all: every form of it reported success and created no index. And the catalogue derived
 * "the relation declared this" from "no parent hands it down", which cannot express the pair
 * PostgreSQL reports for a child that restates its parent's rule, so a second parent went
 * uncounted, a restated NOT NULL was reported under the ancestor's name, and NO INHERIT of one
 * parent let go of every parent the child had.
 *
 * <p>The same three questions, asked of a hierarchy that changes underneath the relation, were
 * answered the same way. A column a parent dropped was taken off every child, even one another
 * parent still declared and one that had listed the column itself; rolling the drop back gave it
 * to the parent alone; a relation that left a hierarchy was renamed after itself, so it could no
 * longer withdraw the rule it was holding; a table was let into a partitioned table without the
 * rules it would have to answer for; and a rule the hierarchy cannot express was accepted where
 * PostgreSQL refuses it. A dropped column was renumbered rather than left a tombstone, so every
 * attribute number a constraint, an index, a default, a comment or a trigger had recorded came to
 * mean a different column. A definition's type was named by its storage key rather than by what
 * the search path would have the reader write, a generation expression was not measured against
 * the column at all, a retype settled its type before its column, and a stored expression's
 * qualifier was dropped instead of resolved. A duplicate key reported the wrong sentence and the
 * wrong key list, and an index key could not be written in two of the three forms PostgreSQL's
 * grammar has for one.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class InheritedDeclarationsAndIndexesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** Every value of the first column, in order, joined with a comma. */
    private static String column(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) got.add(rs.getString(1));
        }
        return String.join(",", got);
    }

    /** The one value the query returns, read as the number it is. */
    private static long num(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try {
            exec(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /**
     * The primary message of the error a statement raises. PostgreSQL sends severity in its own
     * field, so the message on the wire never carries an "ERROR: " prefix.
     */
    private static String messageOf(String sql) {
        return fieldsOf(sql).getMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    /** The hint the error carries, which PostgreSQL sends in a field of its own. */
    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    /** Every row of the query, its columns joined with a slash and its rows with a semicolon. */
    private static String rowsOf(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('/');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return String.join(";", out);
    }

    /** The live attributes of a relation, as name/attislocal/attinhcount, in attribute order. */
    private static String attributesOf(String relation) throws SQLException {
        return scalar("SELECT string_agg(attname||'/'||attislocal::text||'/'||attinhcount::text,"
                + " ',' ORDER BY attnum) FROM pg_attribute"
                + " WHERE attrelid = '" + relation + "'::regclass AND attnum > 0"
                + " AND NOT attisdropped");
    }

    /** The constraints of a relation, as name/conislocal/coninhcount, in name order. */
    private static String constraintsOf(String relation) throws SQLException {
        return scalar("SELECT string_agg(conname||'/'||conislocal::text||'/'||coninhcount::text,"
                + " ',' ORDER BY conname) FROM pg_constraint"
                + " WHERE conrelid = '" + relation + "'::regclass");
    }

    /** The definition pg_indexes reports for one index. */
    private static String indexDef(String indexName) throws SQLException {
        return scalar("SELECT indexdef FROM pg_indexes WHERE indexname = '" + indexName + "'");
    }

    /** Every index of a relation, as name=definition, in name order, separated by semicolons. */
    private static String indexDefsOf(String relation) throws SQLException {
        return scalar("SELECT string_agg(indexname||'='||indexdef, ';' ORDER BY indexname)"
                + " FROM pg_indexes WHERE tablename = '" + relation + "'");
    }

    // ------------------------------------------------------------ An index reaches every relation beneath the one it was declared on

    @Test
    void anIndexOnAPartitionedTableReachesEveryRelationBeneathIt() throws Exception {
        exec("CREATE TABLE zzt9x_h (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_h_0 PARTITION OF zzt9x_h FOR VALUES FROM (0) TO (100)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_h_0_0 PARTITION OF zzt9x_h_0 FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_h_0_0_0 PARTITION OF zzt9x_h_0_0 FOR VALUES FROM (0) TO (5)");
        exec("CREATE INDEX zzt9x_h_idx ON zzt9x_h (s)");

        // Three levels down, and each copy is named for the relation it indexes.
        assertEquals("zzt9x_h/zzt9x_h_idx,zzt9x_h_0/zzt9x_h_0_s_idx,"
                        + "zzt9x_h_0_0/zzt9x_h_0_0_s_idx,zzt9x_h_0_0_0/zzt9x_h_0_0_0_s_idx",
                scalar("SELECT string_agg(tablename||'/'||indexname, ',' ORDER BY tablename,"
                        + " indexname) FROM pg_indexes WHERE tablename LIKE 'zzt9x_h%'"));

        // Only the leaf's copy holds rows, so only it is an ordinary index.
        assertEquals("zzt9x_h/p,zzt9x_h_0/p,zzt9x_h_0_0/p,zzt9x_h_0_0_0/r,"
                        + "zzt9x_h_0_0_0_s_idx/i,zzt9x_h_0_0_s_idx/I,zzt9x_h_0_s_idx/I,"
                        + "zzt9x_h_idx/I",
                scalar("SELECT string_agg(relname||'/'||relkind::text, ',' ORDER BY relname)"
                        + " FROM pg_class WHERE relname LIKE 'zzt9x_h%'"));

        // Each copy is recorded as a child of the copy one level up.
        assertEquals("zzt9x_h/zzt9x_h_0,zzt9x_h_0/zzt9x_h_0_0,zzt9x_h_0_0/zzt9x_h_0_0_0,"
                        + "zzt9x_h_0_0_s_idx/zzt9x_h_0_0_0_s_idx,"
                        + "zzt9x_h_0_s_idx/zzt9x_h_0_0_s_idx,zzt9x_h_idx/zzt9x_h_0_s_idx",
                scalar("SELECT string_agg(pc.relname||'/'||cc.relname, ',' ORDER BY pc.relname,"
                        + " cc.relname) FROM pg_inherits inh"
                        + " JOIN pg_class pc ON pc.oid = inh.inhparent"
                        + " JOIN pg_class cc ON cc.oid = inh.inhrelid"
                        + " WHERE cc.relname LIKE 'zzt9x_h%'"));

        exec("DROP TABLE zzt9x_h");
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname LIKE 'zzt9x_h%'"));
    }

    @Test
    void attachingASubPartitionedTableIndexesTheLeavesItBringsWithIt() throws Exception {
        exec("CREATE TABLE zzt9x_k (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzt9x_k_idx ON zzt9x_k (s)");
        exec("CREATE TABLE zzt9x_k_0 (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_k_0_0 PARTITION OF zzt9x_k_0 FOR VALUES FROM (0) TO (10)");
        assertEquals("zzt9x_k/zzt9x_k_idx",
                scalar("SELECT string_agg(tablename||'/'||indexname, ',' ORDER BY tablename,"
                        + " indexname) FROM pg_indexes WHERE tablename LIKE 'zzt9x_k%'"));

        exec("ALTER TABLE zzt9x_k ATTACH PARTITION zzt9x_k_0 FOR VALUES FROM (0) TO (100)");
        assertEquals("zzt9x_k/zzt9x_k_idx,zzt9x_k_0/zzt9x_k_0_s_idx,"
                        + "zzt9x_k_0_0/zzt9x_k_0_0_s_idx",
                scalar("SELECT string_agg(tablename||'/'||indexname, ',' ORDER BY tablename,"
                        + " indexname) FROM pg_indexes WHERE tablename LIKE 'zzt9x_k%'"));
        assertEquals("zzt9x_k/zzt9x_k_0,zzt9x_k_0/zzt9x_k_0_0,"
                        + "zzt9x_k_0_s_idx/zzt9x_k_0_0_s_idx,zzt9x_k_idx/zzt9x_k_0_s_idx",
                scalar("SELECT string_agg(pc.relname||'/'||cc.relname, ',' ORDER BY pc.relname,"
                        + " cc.relname) FROM pg_inherits inh"
                        + " JOIN pg_class pc ON pc.oid = inh.inhparent"
                        + " JOIN pg_class cc ON cc.oid = inh.inhrelid"
                        + " WHERE cc.relname LIKE 'zzt9x_k%'"));

        // Detaching it again leaves every index it was given in place, and takes away only the
        // one link that pointed at the index it was detached from.
        exec("ALTER TABLE zzt9x_k DETACH PARTITION zzt9x_k_0");
        assertEquals("zzt9x_k/zzt9x_k_idx,zzt9x_k_0/zzt9x_k_0_s_idx,"
                        + "zzt9x_k_0_0/zzt9x_k_0_0_s_idx",
                scalar("SELECT string_agg(tablename||'/'||indexname, ',' ORDER BY tablename,"
                        + " indexname) FROM pg_indexes WHERE tablename LIKE 'zzt9x_k%'"));
        assertEquals("zzt9x_k_0/zzt9x_k_0_0,zzt9x_k_0_s_idx/zzt9x_k_0_0_s_idx",
                scalar("SELECT string_agg(pc.relname||'/'||cc.relname, ',' ORDER BY pc.relname,"
                        + " cc.relname) FROM pg_inherits inh"
                        + " JOIN pg_class pc ON pc.oid = inh.inhparent"
                        + " JOIN pg_class cc ON cc.oid = inh.inhrelid"
                        + " WHERE cc.relname LIKE 'zzt9x_k%'"));

        exec("DROP TABLE zzt9x_k");
        exec("DROP TABLE zzt9x_k_0");
    }

    @Test
    void detachingAPartitionWithdrawsTheLinkToTheIndexItWasGiven() throws Exception {
        exec("CREATE TABLE zzt9x_dx (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzt9x_dx_idx ON zzt9x_dx (s)");
        exec("CREATE TABLE zzt9x_dx_0 PARTITION OF zzt9x_dx FOR VALUES FROM (0) TO (10)");
        assertEquals("zzt9x_dx/zzt9x_dx_0,zzt9x_dx_idx/zzt9x_dx_0_s_idx",
                scalar("SELECT string_agg(pc.relname||'/'||cc.relname, ',' ORDER BY pc.relname,"
                        + " cc.relname) FROM pg_inherits inh"
                        + " JOIN pg_class pc ON pc.oid = inh.inhparent"
                        + " JOIN pg_class cc ON cc.oid = inh.inhrelid"
                        + " WHERE cc.relname LIKE 'zzt9x_dx%'"));

        exec("ALTER TABLE zzt9x_dx DETACH PARTITION zzt9x_dx_0");
        // The detached relation keeps the index; the record that it came from the other one goes.
        assertEquals("zzt9x_dx/zzt9x_dx_idx,zzt9x_dx_0/zzt9x_dx_0_s_idx",
                scalar("SELECT string_agg(tablename||'/'||indexname, ',' ORDER BY tablename,"
                        + " indexname) FROM pg_indexes WHERE tablename LIKE 'zzt9x_dx%'"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_inherits inh"
                + " JOIN pg_class cc ON cc.oid = inh.inhrelid WHERE cc.relname LIKE 'zzt9x_dx%'"));

        exec("DROP TABLE zzt9x_dx");
        exec("DROP TABLE zzt9x_dx_0");
    }

    @Test
    void everyCopyIsNamedForTheRelationItIndexesAndCarriesTheWholeKey() throws Exception {
        exec("CREATE TABLE zzt9x_p (i int, s text, t text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_p_0 PARTITION OF zzt9x_p FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_p_0_0 PARTITION OF zzt9x_p_0 FOR VALUES FROM (0) TO (5)");
        exec("CREATE INDEX ON zzt9x_p (s) INCLUDE (t)");
        exec("CREATE INDEX ON zzt9x_p (s) WHERE i > 1");
        exec("CREATE INDEX ON zzt9x_p ((s || t))");

        // An INCLUDE column takes part in the derived name at every level, and an expression key
        // contributes "expr" rather than the text it was written as.
        assertEquals("zzt9x_p/zzt9x_p_expr_idx,zzt9x_p/zzt9x_p_s_idx,zzt9x_p/zzt9x_p_s_t_idx,"
                        + "zzt9x_p_0/zzt9x_p_0_expr_idx,zzt9x_p_0/zzt9x_p_0_s_idx,"
                        + "zzt9x_p_0/zzt9x_p_0_s_t_idx,zzt9x_p_0_0/zzt9x_p_0_0_expr_idx,"
                        + "zzt9x_p_0_0/zzt9x_p_0_0_s_idx,zzt9x_p_0_0/zzt9x_p_0_0_s_t_idx",
                scalar("SELECT string_agg(tablename||'/'||indexname, ',' ORDER BY tablename,"
                        + " indexname) FROM pg_indexes WHERE tablename LIKE 'zzt9x_p%'"));

        exec("DROP TABLE zzt9x_p");
    }

    @Test
    void theCopiesLandInTheSchemaOfTheRelationTheyIndex() throws Exception {
        exec("CREATE SCHEMA zzt9x_sc");
        exec("SET search_path = zzt9x_sc");
        exec("CREATE TABLE zzt9x_w (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_w_0 PARTITION OF zzt9x_w FOR VALUES FROM (0) TO (100)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_w_0_0 PARTITION OF zzt9x_w_0 FOR VALUES FROM (0) TO (10)");
        exec("CREATE INDEX ON zzt9x_w (s)");

        assertEquals("zzt9x_sc.zzt9x_w/zzt9x_w_s_idx,zzt9x_sc.zzt9x_w_0/zzt9x_w_0_s_idx,"
                        + "zzt9x_sc.zzt9x_w_0_0/zzt9x_w_0_0_s_idx",
                scalar("SELECT string_agg(schemaname||'.'||tablename||'/'||indexname, ','"
                        + " ORDER BY tablename, indexname) FROM pg_indexes"
                        + " WHERE tablename LIKE 'zzt9x_w%'"));

        exec("RESET search_path");
        exec("DROP SCHEMA zzt9x_sc CASCADE");
    }

    // ------------------------------------------------------------ CREATE INDEX with no name derives the name PostgreSQL derives

    @Test
    void anUnnamedIndexTakesTheNamePostgresDerivesForIt() throws Exception {
        exec("CREATE TABLE zzt9x_n (a int, b int, c text)");
        exec("CREATE INDEX ON zzt9x_n (a)");
        exec("CREATE INDEX ON zzt9x_n (a)");
        exec("CREATE INDEX ON zzt9x_n ((a+b))");
        exec("CREATE INDEX ON zzt9x_n ((a+b))");
        exec("CREATE INDEX ON zzt9x_n ((a*2), (b*3))");
        exec("CREATE INDEX ON zzt9x_n (a, (b+1))");
        exec("CREATE INDEX ON zzt9x_n (lower(c))");
        exec("CREATE INDEX ON zzt9x_n (a, b)");
        exec("CREATE UNIQUE INDEX ON zzt9x_n (c)");
        exec("CREATE INDEX ON zzt9x_n (a) WHERE b > 0");
        exec("CREATE INDEX ON zzt9x_n (a, a)");
        exec("CREATE INDEX ON zzt9x_n (b) INCLUDE (c)");
        exec("CREATE INDEX ON zzt9x_n (a DESC NULLS FIRST)");

        // <table>_<key name>..._idx; a name already taken is numbered from 1, and so is a key
        // name repeated inside one index.
        assertEquals("zzt9x_n_a_a1_idx,zzt9x_n_a_b_idx,zzt9x_n_a_expr_idx,zzt9x_n_a_idx,"
                        + "zzt9x_n_a_idx1,zzt9x_n_a_idx2,zzt9x_n_a_idx3,zzt9x_n_b_c_idx,"
                        + "zzt9x_n_c_idx,zzt9x_n_expr_expr1_idx,zzt9x_n_expr_idx,"
                        + "zzt9x_n_expr_idx1,zzt9x_n_lower_idx",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_n'"
                        + " ORDER BY indexname"));

        // Every one of them is a relation that exists, not a statement that reported success.
        assertEquals(13, num("SELECT count(*)::int FROM pg_index i"
                + " JOIN pg_class c ON c.oid = i.indrelid WHERE c.relname = 'zzt9x_n'"));
        // The UNIQUE one is unique, under the derived name rather than <table>_unique.
        assertEquals(1, num("SELECT count(*)::int FROM pg_index i"
                + " JOIN pg_class c ON c.oid = i.indexrelid"
                + " WHERE c.relname = 'zzt9x_n_c_idx' AND i.indisunique"));

        exec("DROP TABLE zzt9x_n");
        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname LIKE 'zzt9x_n%' AND relkind = 'i'"));
    }

    @Test
    void aDerivedNameIsNumberedPastAnyRelationOfThatName() throws Exception {
        exec("CREATE TABLE zzt9x_q (a int, b int, c int)");
        // A table, a view and a sequence are all relations, so each holds the name against
        // the index that would otherwise take it.
        exec("CREATE TABLE zzt9x_q_a_idx (x int)");
        exec("CREATE VIEW zzt9x_q_b_idx AS SELECT 1 AS x");
        exec("CREATE SEQUENCE zzt9x_q_c_idx");
        exec("CREATE INDEX ON zzt9x_q (a)");
        exec("CREATE INDEX ON zzt9x_q (b)");
        exec("CREATE INDEX ON zzt9x_q (c)");

        assertEquals("zzt9x_q_a_idx1,zzt9x_q_b_idx1,zzt9x_q_c_idx1",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_q'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzt9x_q");
        exec("DROP TABLE zzt9x_q_a_idx");
        exec("DROP VIEW zzt9x_q_b_idx");
        exec("DROP SEQUENCE zzt9x_q_c_idx");
    }

    @Test
    void eachKindOfIndexKeyContributesItsOwnNameToTheIndexName() throws Exception {
        exec("CREATE TABLE zzt9x_e (a int, b int, c text, d text[])");
        exec("CREATE INDEX ON zzt9x_e ((a))");
        exec("CREATE INDEX ON zzt9x_e (greatest(a,b))");
        exec("CREATE INDEX ON zzt9x_e (least(a,b))");
        exec("CREATE INDEX ON zzt9x_e (nullif(a,b))");
        exec("CREATE INDEX ON zzt9x_e ((ARRAY[a,b]))");
        exec("CREATE INDEX ON zzt9x_e ((-a))");
        exec("CREATE INDEX ON zzt9x_e ((a IS NULL))");
        exec("CREATE INDEX ON zzt9x_e ((c LIKE 'x%'))");
        exec("CREATE INDEX ON zzt9x_e ((case when a>0 then lower(c) else upper(c) end))");
        exec("CREATE INDEX ON zzt9x_e (((a)::text))");
        exec("CREATE INDEX ON zzt9x_e ((abs(a)::text))");
        exec("CREATE INDEX ON zzt9x_e (c COLLATE \"C\")");
        exec("CREATE INDEX ON zzt9x_e (coalesce(a,0))");

        // A column contributes its own name, a function call its function name, a cast the name
        // of what it casts, a COLLATE clause the name of what it collates, a CASE the name of its
        // last result, and anything else "expr".
        assertEquals("zzt9x_e_a_idx,zzt9x_e_a_idx1,zzt9x_e_abs_idx,zzt9x_e_array_idx,"
                        + "zzt9x_e_c_idx,zzt9x_e_coalesce_idx,zzt9x_e_expr_idx,"
                        + "zzt9x_e_expr_idx1,zzt9x_e_expr_idx2,zzt9x_e_greatest_idx,"
                        + "zzt9x_e_least_idx,zzt9x_e_nullif_idx,zzt9x_e_upper_idx",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_e'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzt9x_e");
    }

    @Test
    void aCastOverSomethingWithNoNameIsNamedAfterTheTypeTheCatalogueSpells() throws Exception {
        exec("CREATE TABLE zzt9x_cc (a int, b int, c text)");
        exec("CREATE INDEX ON zzt9x_cc (((a+b)::text))");
        exec("CREATE INDEX ON zzt9x_cc (((a+b)::bigint))");
        exec("CREATE INDEX ON zzt9x_cc ((CASE WHEN a>0 THEN 1 END))");
        exec("CREATE INDEX ON zzt9x_cc ((CASE WHEN a>0 THEN c::text ELSE c END))");
        exec("CREATE INDEX ON zzt9x_cc (((a+b)::text::int))");
        exec("CREATE INDEX ON zzt9x_cc ((('x')::text))");
        exec("CREATE INDEX ON zzt9x_cc ((c::varchar(10)))");

        assertEquals("zzt9x_cc_c_idx,zzt9x_cc_c_idx1,zzt9x_cc_case_idx,zzt9x_cc_int4_idx,"
                        + "zzt9x_cc_int8_idx,zzt9x_cc_text_idx,zzt9x_cc_text_idx1",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_cc'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzt9x_cc");

        // The type is spelled the way pg_type spells it, not the way the cast was written.
        exec("CREATE TABLE zzt9x_ty (a int, b int)");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::smallint))");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::real))");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::double precision))");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::decimal))");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::boolean))");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::char(3)))");
        exec("CREATE INDEX ON zzt9x_ty (((a+b)::varchar))");

        assertEquals("zzt9x_ty_bool_idx,zzt9x_ty_bpchar_idx,zzt9x_ty_float4_idx,"
                        + "zzt9x_ty_float8_idx,zzt9x_ty_int2_idx,zzt9x_ty_numeric_idx,"
                        + "zzt9x_ty_varchar_idx",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_ty'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzt9x_ty");
    }

    @Test
    void aQuotedColumnKeepsItsOwnSpellingInTheDerivedName() throws Exception {
        exec("CREATE TABLE zzt9x_ix (\"MixEd\" int, \"with space\" int)");
        exec("CREATE INDEX ON zzt9x_ix (\"MixEd\")");
        exec("CREATE INDEX ON zzt9x_ix (\"with space\")");

        assertEquals("zzt9x_ix_MixEd_idx,zzt9x_ix_with space_idx",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_ix'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzt9x_ix");
    }

    @Test
    void aDerivedNameIsCutBackToFitARelationName() throws Exception {
        String t60 = "zzt9x_t" + "x".repeat(53);
        String c50 = "c" + "y".repeat(49);
        String d40 = "d" + "z".repeat(39);
        exec("CREATE TABLE " + t60 + " (a int, " + c50 + " int, " + d40 + " int)");
        exec("CREATE INDEX ON " + t60 + " (a)");
        exec("CREATE INDEX ON " + t60 + " (a)");
        exec("CREATE INDEX ON " + t60 + " (a)");
        exec("CREATE INDEX ON " + t60 + " (" + c50 + ")");
        exec("CREATE INDEX ON " + t60 + " (" + c50 + ", " + d40 + ")");

        // The parts are cut back until the whole name fits in 63 bytes, and the number that
        // separates it from a relation of that name is added after the cut.
        assertEquals("zzt9x_t" + "x".repeat(22) + "_c" + "y".repeat(27) + "_idx1/63,"
                        + "zzt9x_t" + "x".repeat(22) + "_c" + "y".repeat(28) + "_idx/63,"
                        + "zzt9x_t" + "x".repeat(49) + "_a_idx1/63,"
                        + "zzt9x_t" + "x".repeat(49) + "_a_idx2/63,"
                        + "zzt9x_t" + "x".repeat(50) + "_a_idx/63",
                scalar("SELECT string_agg(indexname||'/'||length(indexname)::text, ','"
                        + " ORDER BY indexname) FROM pg_indexes WHERE tablename = '" + t60 + "'"));

        exec("DROP TABLE " + t60);

        exec("CREATE TABLE zzt9x_s (a int, " + c50 + " int, " + d40 + " int)");
        exec("CREATE INDEX ON zzt9x_s (" + c50 + ")");
        exec("CREATE INDEX ON zzt9x_s (" + c50 + ")");
        exec("CREATE INDEX ON zzt9x_s (" + c50 + ", " + d40 + ")");

        // The two-column name is cut where the underscore between the columns is, so it keeps a
        // double underscore and none of the second column.
        assertEquals("zzt9x_s_c" + "y".repeat(49) + "__idx/63,"
                        + "zzt9x_s_c" + "y".repeat(49) + "_idx/62,"
                        + "zzt9x_s_c" + "y".repeat(49) + "_idx1/63",
                scalar("SELECT string_agg(indexname||'/'||length(indexname)::text, ','"
                        + " ORDER BY indexname) FROM pg_indexes WHERE tablename = 'zzt9x_s'"));

        exec("DROP TABLE zzt9x_s");
    }

    @Test
    void theDerivedNameIsTheOneTheDuplicateKeyErrorsReport() throws Exception {
        exec("CREATE TABLE zzt9x_u (a int, b int)");
        exec("INSERT INTO zzt9x_u VALUES (1,1),(1,2)");

        // An index that cannot be built names itself in the error, and is not left behind.
        assertEquals("23505", stateOf("CREATE UNIQUE INDEX ON zzt9x_u (a)"));
        assertEquals("could not create unique index \"zzt9x_u_a_idx\"",
                messageOf("CREATE UNIQUE INDEX ON zzt9x_u (a)"));

        exec("CREATE UNIQUE INDEX ON zzt9x_u (b)");
        assertEquals("23505", stateOf("INSERT INTO zzt9x_u VALUES (3,2)"));
        assertEquals("duplicate key value violates unique constraint \"zzt9x_u_b_idx\"",
                messageOf("INSERT INTO zzt9x_u VALUES (3,2)"));
        assertEquals("Key (b)=(2) already exists.",
                detailOf("INSERT INTO zzt9x_u VALUES (3,2)"));
        assertEquals("zzt9x_u_b_idx",
                fieldsOf("INSERT INTO zzt9x_u VALUES (3,2)").getConstraint());

        assertEquals("zzt9x_u_b_idx",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_u'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzt9x_u");
    }

    // ------------------------------------------------------------ What a relation declared, and how many parents hand it the same thing

    @Test
    void aChildThatRestatesItsParentsRuleDeclaredItAndInheritsIt() throws Exception {
        exec("CREATE TABLE zzt9x_p0 (i int CONSTRAINT zzt9x_ck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_p1 (i int CONSTRAINT zzt9x_ck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_c (i int CONSTRAINT zzt9x_ck CHECK (i > 0))"
                + " INHERITS (zzt9x_p0, zzt9x_p1)");
        exec("CREATE TABLE zzt9x_g () INHERITS (zzt9x_c)");

        String cons = "SELECT string_agg(cl.relname||'/'||c.conislocal::text||'/'"
                + "||c.coninhcount::text, ',' ORDER BY cl.relname) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN"
                + " ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') AND c.contype = 'c'";
        String atts = "SELECT string_agg(cl.relname||'/'||a.attislocal::text||'/'"
                + "||a.attinhcount::text, ',' ORDER BY cl.relname) FROM pg_attribute a"
                + " JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN"
                + " ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g') AND a.attnum > 0";

        // The child declared the rule itself and takes it from both parents; the grandchild only
        // takes it, from the one parent it has.
        assertEquals("zzt9x_c/zzt9x_ck/true/2,zzt9x_g/zzt9x_ck/false/1,"
                        + "zzt9x_p0/zzt9x_ck/true/0,zzt9x_p1/zzt9x_ck/true/0",
                scalar("SELECT string_agg(cl.relname||'/'||c.conname||'/'||c.conislocal::text"
                        + "||'/'||c.coninhcount::text, ',' ORDER BY cl.relname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g')"
                        + " AND c.contype = 'c'"));
        assertEquals("zzt9x_c/i/true/2,zzt9x_g/i/false/1,zzt9x_p0/i/true/0,zzt9x_p1/i/true/0",
                scalar("SELECT string_agg(cl.relname||'/'||a.attname||'/'||a.attislocal::text"
                        + "||'/'||a.attinhcount::text, ',' ORDER BY cl.relname)"
                        + " FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g')"
                        + " AND a.attnum > 0"));

        // NO INHERIT lets go of the parent it names and of no other.
        exec("ALTER TABLE zzt9x_c NO INHERIT zzt9x_p0");
        assertEquals("zzt9x_c/zzt9x_g,zzt9x_p1/zzt9x_c",
                scalar("SELECT string_agg(p.relname||'/'||cl.relname, ',' ORDER BY p.relname,"
                        + " cl.relname) FROM pg_inherits h"
                        + " JOIN pg_class p ON p.oid = h.inhparent"
                        + " JOIN pg_class cl ON cl.oid = h.inhrelid"
                        + " WHERE cl.relname IN ('zzt9x_p0','zzt9x_p1','zzt9x_c','zzt9x_g')"));
        assertEquals("zzt9x_c/true/1,zzt9x_g/false/1,zzt9x_p0/true/0,zzt9x_p1/true/0",
                scalar(cons));
        assertEquals("zzt9x_c/true/1,zzt9x_g/false/1,zzt9x_p0/true/0,zzt9x_p1/true/0",
                scalar(atts));

        // With no parent left the child keeps what it declared and counts nobody.
        exec("ALTER TABLE zzt9x_c NO INHERIT zzt9x_p1");
        assertEquals("zzt9x_c/true/0,zzt9x_g/false/1",
                scalar("SELECT string_agg(cl.relname||'/'||c.conislocal::text||'/'"
                        + "||c.coninhcount::text, ',' ORDER BY cl.relname) FROM pg_constraint c"
                        + " JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname IN ('zzt9x_c','zzt9x_g') AND c.contype = 'c'"));
        assertEquals("zzt9x_c/true/0,zzt9x_g/false/1",
                scalar("SELECT string_agg(cl.relname||'/'||a.attislocal::text||'/'"
                        + "||a.attinhcount::text, ',' ORDER BY cl.relname) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname IN ('zzt9x_c','zzt9x_g') AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_g");
        exec("DROP TABLE zzt9x_c");
        exec("DROP TABLE zzt9x_p0");
        exec("DROP TABLE zzt9x_p1");
    }

    @Test
    void aChildThatOnlyTakesTheRuleFromTwoParentsIsLocalToNeither() throws Exception {
        exec("CREATE TABLE zzt9x_u0 (i int CONSTRAINT zzt9x_uck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_u1 (i int CONSTRAINT zzt9x_uck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_uc () INHERITS (zzt9x_u0, zzt9x_u1)");

        String q = "SELECT string_agg(c.conislocal::text||'/'||c.coninhcount::text, ',')"
                + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                + " WHERE cl.relname = 'zzt9x_uc' AND c.contype = 'c'";
        assertEquals("false/2", scalar(q));

        // Letting go of one parent leaves the rule the other one still hands down.
        exec("ALTER TABLE zzt9x_uc NO INHERIT zzt9x_u0");
        assertEquals("false/1", scalar(q));

        exec("DROP TABLE zzt9x_uc");
        exec("DROP TABLE zzt9x_u0");
        exec("DROP TABLE zzt9x_u1");
    }

    @Test
    void aChildThatRestatesNotNullHoldsAConstraintOfItsOwnName() throws Exception {
        exec("CREATE TABLE zzt9x_n0 (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzt9x_n1 (i int NOT NULL, k int NOT NULL) INHERITS (zzt9x_n0)");

        // i was restated: the child's own name, local, one parent. j was only taken: the parent's
        // name, not local. k is the child's alone: local, no parent.
        assertEquals("zzt9x_n0_j_not_null/n/false/1,zzt9x_n1_i_not_null/n/true/1,"
                        + "zzt9x_n1_k_not_null/n/true/0",
                scalar("SELECT string_agg(c.conname||'/'||c.contype::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text, ',' ORDER BY c.conname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_n1'"));
        assertEquals("i/true/true/1,j/true/false/1,k/true/true/1",
                scalar("SELECT string_agg(a.attname||'/'||a.attnotnull::text||'/'"
                        + "||a.attislocal::text||'/'||a.attinhcount::text, ',' ORDER BY a.attnum)"
                        + " FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_n1' AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_n1");
        exec("DROP TABLE zzt9x_n0");
    }

    @Test
    void aColumnTheChildListsIsTheChildsOwnEvenThoughAParentHasIt() throws Exception {
        exec("CREATE TABLE zzt9x_v1 (i int, j int)");
        exec("CREATE TABLE zzt9x_v2 (i int) INHERITS (zzt9x_v1)");

        String q = "SELECT string_agg(a.attname||'/'||a.attnum::text||'/'||a.attislocal::text"
                + "||'/'||a.attinhcount::text, ',' ORDER BY a.attnum) FROM pg_attribute a"
                + " JOIN pg_class cl ON a.attrelid = cl.oid"
                + " WHERE cl.relname = 'zzt9x_v2' AND a.attnum > 0";
        assertEquals("i/1/true/1,j/2/false/1", scalar(q));

        // A column that arrives later is not the child's own either.
        exec("ALTER TABLE zzt9x_v1 ADD COLUMN m text");
        assertEquals("i/1/true/1,j/2/false/1,m/3/false/1", scalar(q));

        exec("DROP TABLE zzt9x_v2");
        exec("DROP TABLE zzt9x_v1");
    }

    @Test
    void noInheritUnderTwoParentsKeepsTheRemainingParentsCount() throws Exception {
        exec("CREATE TABLE zzt9x_x0 (i int)");
        exec("CREATE TABLE zzt9x_x1 (i int)");
        exec("CREATE TABLE zzt9x_x2 (i int) INHERITS (zzt9x_x0, zzt9x_x1)");
        exec("ALTER TABLE zzt9x_x2 NO INHERIT zzt9x_x0");

        assertEquals("zzt9x_x1",
                column("SELECT p.relname FROM pg_inherits h"
                        + " JOIN pg_class p ON p.oid = h.inhparent"
                        + " JOIN pg_class cl ON cl.oid = h.inhrelid"
                        + " WHERE cl.relname = 'zzt9x_x2' ORDER BY 1"));
        assertEquals("i/true/1",
                scalar("SELECT string_agg(a.attname||'/'||a.attislocal::text||'/'"
                        + "||a.attinhcount::text, ',' ORDER BY a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_x2' AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_x2");
        exec("DROP TABLE zzt9x_x0");
        exec("DROP TABLE zzt9x_x1");
    }

    @Test
    void alterTableInheritRaisesTheCountAndNoInheritLowersIt() throws Exception {
        exec("CREATE TABLE zzt9x_ap (i int NOT NULL, CONSTRAINT zzt9x_ack CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_ac (i int NOT NULL, CONSTRAINT zzt9x_ack CHECK (i > 0))");

        String q = "SELECT string_agg(c.conname||'/'||c.contype::text||'/'||c.conislocal::text"
                + "||'/'||c.coninhcount::text, ',' ORDER BY c.conname) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_ac'";
        assertEquals("zzt9x_ac_i_not_null/n/true/0,zzt9x_ack/c/true/0", scalar(q));

        // The child still declared both; now a parent hands both down as well.
        exec("ALTER TABLE zzt9x_ac INHERIT zzt9x_ap");
        assertEquals("zzt9x_ac_i_not_null/n/true/1,zzt9x_ack/c/true/1", scalar(q));
        assertEquals("i/true/1",
                scalar("SELECT string_agg(a.attname||'/'||a.attislocal::text||'/'"
                        + "||a.attinhcount::text, ',' ORDER BY a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_ac' AND a.attnum > 0"));

        exec("ALTER TABLE zzt9x_ac NO INHERIT zzt9x_ap");
        assertEquals("zzt9x_ac_i_not_null/n/true/0,zzt9x_ack/c/true/0", scalar(q));

        exec("DROP TABLE zzt9x_ac");
        exec("DROP TABLE zzt9x_ap");
    }

    @Test
    void aConstraintAParentStillHandsDownMayNotBeDroppedByTheChild() throws Exception {
        exec("CREATE TABLE zzt9x_bp (i int NOT NULL, CONSTRAINT zzt9x_bck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_bc (i int NOT NULL, CONSTRAINT zzt9x_bck CHECK (i > 0))");
        exec("ALTER TABLE zzt9x_bc INHERIT zzt9x_bp");

        assertEquals("42P16", stateOf("ALTER TABLE zzt9x_bc DROP CONSTRAINT zzt9x_bck"));
        assertEquals("cannot drop inherited constraint \"zzt9x_bck\" of relation \"zzt9x_bc\"",
                messageOf("ALTER TABLE zzt9x_bc DROP CONSTRAINT zzt9x_bck"));

        // With nobody handing it down the drop is allowed again.
        exec("ALTER TABLE zzt9x_bc NO INHERIT zzt9x_bp");
        exec("ALTER TABLE zzt9x_bc DROP CONSTRAINT zzt9x_bck");
        assertEquals("zzt9x_bc_i_not_null",
                column("SELECT c.conname FROM pg_constraint c"
                        + " JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_bc' ORDER BY 1"));

        exec("DROP TABLE zzt9x_bc");
        exec("DROP TABLE zzt9x_bp");
    }

    @Test
    void aPartitionMayNotDropTheKeyItHoldsForThePartitionedTable() throws Exception {
        exec("CREATE TABLE zzt9x_kp (i int, j int, CONSTRAINT zzt9x_kppk PRIMARY KEY (i),"
                + " CONSTRAINT zzt9x_kpck CHECK (j > 0)) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_kp0 PARTITION OF zzt9x_kp FOR VALUES FROM (0) TO (10)");

        assertEquals("42P16", stateOf("ALTER TABLE zzt9x_kp0 DROP CONSTRAINT zzt9x_kp0_pkey"));
        assertEquals("cannot drop inherited constraint \"zzt9x_kp0_pkey\""
                        + " of relation \"zzt9x_kp0\"",
                messageOf("ALTER TABLE zzt9x_kp0 DROP CONSTRAINT zzt9x_kp0_pkey"));

        // A partition declares nothing of its own.
        assertEquals("zzt9x_kp0_pkey/p/false/1,zzt9x_kp_i_not_null/n/false/1,"
                        + "zzt9x_kpck/c/false/1",
                scalar("SELECT string_agg(c.conname||'/'||c.contype::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text, ',' ORDER BY c.conname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_kp0'"));
        assertEquals("i/false/1,j/false/1",
                scalar("SELECT string_agg(a.attname||'/'||a.attislocal::text||'/'"
                        + "||a.attinhcount::text, ',' ORDER BY a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_kp0' AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_kp");
    }

    @Test
    void detachingAPartitionMakesItsDeclarationsItsOwn() throws Exception {
        exec("CREATE TABLE zzt9x_dt (i int NOT NULL, CONSTRAINT zzt9x_dtck CHECK (i > 0))"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_dt0 PARTITION OF zzt9x_dt FOR VALUES FROM (0) TO (10)");

        String q = "SELECT string_agg(c.contype::text||'/'||c.conislocal::text||'/'"
                + "||c.coninhcount::text, ',' ORDER BY c.contype) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzt9x_dt0'";
        assertEquals("c/false/1,n/false/1", scalar(q));

        exec("ALTER TABLE zzt9x_dt DETACH PARTITION zzt9x_dt0");
        assertEquals("c/true/0,n/true/0", scalar(q));
        assertEquals("i/true/0",
                scalar("SELECT string_agg(a.attname||'/'||a.attislocal::text||'/'"
                        + "||a.attinhcount::text, ',' ORDER BY a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_dt0' AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_dt");
        exec("DROP TABLE zzt9x_dt0");
    }

    @Test
    void attachingATableRecordsThatWhatItDeclaredComesFromThePartitionedTable() throws Exception {
        exec("CREATE TABLE zzt9x_at (i int NOT NULL, CONSTRAINT zzt9x_atck CHECK (i > 0))"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt9x_at1 (i int NOT NULL, CONSTRAINT zzt9x_atck CHECK (i > 0))");
        exec("ALTER TABLE zzt9x_at ATTACH PARTITION zzt9x_at1 FOR VALUES FROM (0) TO (10)");

        assertEquals("c/false/1,n/false/1",
                scalar("SELECT string_agg(c.contype::text||'/'||c.conislocal::text||'/'"
                        + "||c.coninhcount::text, ',' ORDER BY c.contype) FROM pg_constraint c"
                        + " JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_at1'"));
        assertEquals("i/false/1",
                scalar("SELECT string_agg(a.attname||'/'||a.attislocal::text||'/'"
                        + "||a.attinhcount::text, ',' ORDER BY a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_at1' AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_at");
    }

    @Test
    void aRefusedDropOfTheParentLeavesEveryCountAlone() throws Exception {
        exec("CREATE TABLE zzt9x_dp (i int CONSTRAINT zzt9x_dck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_dc (i int CONSTRAINT zzt9x_dck CHECK (i > 0))"
                + " INHERITS (zzt9x_dp)");
        exec("CREATE TABLE zzt9x_de () INHERITS (zzt9x_dp)");

        String q = "SELECT string_agg(cl.relname||'/'||c.conislocal::text||'/'"
                + "||c.coninhcount::text, ',' ORDER BY cl.relname) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname IN"
                + " ('zzt9x_dp','zzt9x_dc','zzt9x_de') AND c.contype = 'c'";
        assertEquals("zzt9x_dc/true/1,zzt9x_de/false/1,zzt9x_dp/true/0", scalar(q));

        assertEquals("2BP01", stateOf("DROP TABLE zzt9x_dp"));
        assertEquals("cannot drop table zzt9x_dp because other objects depend on it",
                messageOf("DROP TABLE zzt9x_dp"));
        assertEquals("zzt9x_dc/true/1,zzt9x_de/false/1,zzt9x_dp/true/0", scalar(q));

        // Dropping the parent's constraint takes it off the child that only took it, and leaves
        // the one the other child declared with nobody handing it down.
        exec("ALTER TABLE zzt9x_dp DROP CONSTRAINT zzt9x_dck");
        assertEquals("zzt9x_dc/true/0", scalar(q));

        exec("DROP TABLE zzt9x_de");
        exec("DROP TABLE zzt9x_dc");
        exec("DROP TABLE zzt9x_dp");
    }

    @Test
    void aParentLetGoOfBeforeItIsDroppedLeavesTheRemainingCountAlone() throws Exception {
        exec("CREATE TABLE zzt9x_q0 (i int CONSTRAINT zzt9x_qck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_q1 (i int CONSTRAINT zzt9x_qck CHECK (i > 0))");
        exec("CREATE TABLE zzt9x_qc (i int CONSTRAINT zzt9x_qck CHECK (i > 0))"
                + " INHERITS (zzt9x_q0, zzt9x_q1)");
        exec("ALTER TABLE zzt9x_qc NO INHERIT zzt9x_q0");
        exec("DROP TABLE zzt9x_q0");

        assertEquals("true/1",
                scalar("SELECT string_agg(c.conislocal::text||'/'||c.coninhcount::text, ',')"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_qc' AND c.contype = 'c'"));
        assertEquals("true/1",
                scalar("SELECT string_agg(a.attislocal::text||'/'||a.attinhcount::text, ',')"
                        + " FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_qc' AND a.attnum > 0"));

        exec("DROP TABLE zzt9x_qc");
        exec("DROP TABLE zzt9x_q1");
    }

    @Test
    void aChildThatDeclaresNothingOfItsOwnIsLocalToNothing() throws Exception {
        exec("CREATE TABLE zzt9x_cl (a int CHECK (a > 0), b int NOT NULL, c int PRIMARY KEY)");
        exec("CREATE TABLE zzt9x_clc () INHERITS (zzt9x_cl)");

        // A key is the parent's own business and travels no further than its table.
        assertEquals("zzt9x_cl_a_check/c/false/1,zzt9x_cl_b_not_null/n/false/1,"
                        + "zzt9x_cl_c_not_null/n/false/1",
                scalar("SELECT string_agg(c.conname||'/'||c.contype::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text, ',' ORDER BY c.conname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_clc'"));
        assertEquals("zzt9x_cl_a_check/c/true/0,zzt9x_cl_b_not_null/n/true/0,"
                        + "zzt9x_cl_c_not_null/n/true/0,zzt9x_cl_pkey/p/true/0",
                scalar("SELECT string_agg(c.conname||'/'||c.contype::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text, ',' ORDER BY c.conname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_cl'"));

        // A rule the child adds for itself is nobody else's, and the child may drop it.
        exec("ALTER TABLE zzt9x_clc ADD CONSTRAINT zzt9x_own CHECK (a < 100)");
        assertEquals("true/0",
                scalar("SELECT string_agg(c.conislocal::text||'/'||c.coninhcount::text, ',')"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzt9x_clc' AND c.conname = 'zzt9x_own'"));
        exec("ALTER TABLE zzt9x_clc DROP CONSTRAINT zzt9x_own");
        assertEquals(0, num("SELECT count(*)::int FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid"
                + " WHERE cl.relname = 'zzt9x_clc' AND c.conname = 'zzt9x_own'"));

        exec("DROP TABLE zzt9x_clc");
        exec("DROP TABLE zzt9x_cl");
    }

    // ------------------------------------------------------------ A definition is judged before the relation is built

    @Test
    void addColumnRefusesADefaultOfTheWrongType() throws Exception {
        exec("CREATE SEQUENCE zzt9x_seq");
        exec("CREATE TABLE zzt9x_addc (k int)");

        assertEquals("42804",
                stateOf("ALTER TABLE zzt9x_addc ADD COLUMN c1 int DEFAULT 'abc'::text"));
        assertEquals("column \"c1\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzt9x_addc ADD COLUMN c1 int DEFAULT 'abc'::text"));
        assertEquals("You will need to rewrite or cast the expression.",
                hintOf("ALTER TABLE zzt9x_addc ADD COLUMN c1 int DEFAULT 'abc'::text"));

        // A value that happens to coerce is refused all the same.
        assertEquals("column \"c2\" is of type boolean but default expression is of type integer",
                messageOf("ALTER TABLE zzt9x_addc ADD COLUMN c2 bool DEFAULT 1"));
        assertEquals("column \"c3\" is of type integer but default expression is of type"
                        + " timestamp with time zone",
                messageOf("ALTER TABLE zzt9x_addc ADD COLUMN c3 int DEFAULT now()"));
        assertEquals("column \"c5\" is of type integer but default expression is of type boolean",
                messageOf("ALTER TABLE zzt9x_addc ADD COLUMN c5 int DEFAULT true"));

        // A bare string literal is still of type unknown, so a bad one is bad input rather than
        // a mismatch, and carries no hint.
        assertEquals("22P02",
                stateOf("ALTER TABLE zzt9x_addc ADD COLUMN c4 int DEFAULT 'abc'"));
        assertEquals("invalid input syntax for type integer: \"abc\"",
                messageOf("ALTER TABLE zzt9x_addc ADD COLUMN c4 int DEFAULT 'abc'"));
        assertNull(hintOf("ALTER TABLE zzt9x_addc ADD COLUMN c4 int DEFAULT 'abc'"));

        // The pairs PostgreSQL has an assignment cast for stand.
        exec("ALTER TABLE zzt9x_addc ADD COLUMN c6 date DEFAULT now()");
        exec("ALTER TABLE zzt9x_addc ADD COLUMN c7 text DEFAULT 1");
        exec("ALTER TABLE zzt9x_addc ADD COLUMN c8 int DEFAULT random()");
        exec("ALTER TABLE zzt9x_addc ADD COLUMN c9 int DEFAULT nextval('zzt9x_seq')");

        // Nothing a refusal named was built.
        assertEquals("k:integer,c6:date,c7:text,c8:integer,c9:integer",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzt9x_addc'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        exec("DROP TABLE zzt9x_addc");
        exec("DROP SEQUENCE zzt9x_seq");
    }

    @Test
    void addColumnKeepsTheRefusalsThatComeBeforeTheDefaultIsJudged() throws Exception {
        exec("CREATE TABLE zzt9x_addd (k int)");

        assertEquals("42701",
                stateOf("ALTER TABLE zzt9x_addd ADD COLUMN k int DEFAULT 'x'::text"));
        assertEquals("column \"k\" of relation \"zzt9x_addd\" already exists",
                messageOf("ALTER TABLE zzt9x_addd ADD COLUMN k int DEFAULT 'x'::text"));
        assertEquals("42704",
                stateOf("ALTER TABLE zzt9x_addd ADD COLUMN z nosuchtype DEFAULT 'x'::text"));
        assertEquals("type \"nosuchtype\" does not exist",
                messageOf("ALTER TABLE zzt9x_addd ADD COLUMN z nosuchtype DEFAULT 'x'::text"));
        assertEquals("column name \"ctid\" conflicts with a system column name",
                messageOf("ALTER TABLE zzt9x_addd ADD COLUMN ctid int DEFAULT 'x'::text"));
        assertEquals("0A000",
                stateOf("ALTER TABLE zzt9x_addd ADD COLUMN z int DEFAULT (SELECT 1)"));
        assertEquals("cannot use subquery in DEFAULT expression",
                messageOf("ALTER TABLE zzt9x_addd ADD COLUMN z int DEFAULT (SELECT 1)"));

        // A column-level PRIMARY KEY does not come first: the default is judged before it.
        assertEquals("column \"z\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzt9x_addd ADD COLUMN z int PRIMARY KEY"
                        + " DEFAULT 'x'::text"));

        assertEquals("k", scalar("SELECT string_agg(attname, ',' ORDER BY attnum)"
                + " FROM pg_attribute WHERE attrelid = 'zzt9x_addd'::regclass AND attnum > 0"
                + " AND NOT attisdropped"));

        exec("DROP TABLE zzt9x_addd");
    }

    @Test
    void theMismatchNamesTheColumnsTypeTheWayTheCatalogueNamesIt() throws Exception {
        // "integer", never "int4", and the hint PostgreSQL sends with it.
        assertEquals("42804", stateOf("CREATE TABLE zzt9x_y1 (b int DEFAULT now())"));
        assertEquals("column \"b\" is of type integer but default expression is of type"
                        + " timestamp with time zone",
                messageOf("CREATE TABLE zzt9x_y1 (b int DEFAULT now())"));
        assertEquals("You will need to rewrite or cast the expression.",
                hintOf("CREATE TABLE zzt9x_y1 (b int DEFAULT now())"));

        // A parameterless value function written as a bare keyword has a type of its own.
        assertEquals("column \"b\" is of type integer but default expression is of type"
                        + " timestamp with time zone",
                messageOf("CREATE TABLE zzt9x_y2 (b int DEFAULT current_timestamp)"));
        assertEquals("column \"b\" is of type integer but default expression is of type date",
                messageOf("CREATE TABLE zzt9x_y3 (b int DEFAULT current_date)"));
        assertEquals("column \"b\" is of type integer but default expression is of type"
                        + " time with time zone",
                messageOf("CREATE TABLE zzt9x_y4 (b int DEFAULT current_time)"));
        assertEquals("column \"b\" is of type integer but default expression is of type"
                        + " timestamp without time zone",
                messageOf("CREATE TABLE zzt9x_y5 (b int DEFAULT localtimestamp)"));
        assertEquals("column \"b\" is of type integer but default expression is of type"
                        + " time without time zone",
                messageOf("CREATE TABLE zzt9x_y6 (b int DEFAULT localtime)"));
        assertEquals("column \"b\" is of type integer but default expression is of type name",
                messageOf("CREATE TABLE zzt9x_y7 (b int DEFAULT current_user)"));
        assertEquals("column \"b\" is of type integer but default expression is of type name",
                messageOf("CREATE TABLE zzt9x_y8 (b int DEFAULT session_user)"));
        assertEquals("column \"b\" is of type integer but default expression is of type name",
                messageOf("CREATE TABLE zzt9x_y9 (b int DEFAULT current_role)"));
        assertEquals("column \"b\" is of type integer but default expression is of type name",
                messageOf("CREATE TABLE zzt9x_ya (b int DEFAULT current_catalog)"));

        // An ordinary function call, and an operator.
        assertEquals("column \"b\" is of type integer but default expression is of type"
                        + " timestamp with time zone",
                messageOf("CREATE TABLE zzt9x_yb (b int DEFAULT clock_timestamp())"));
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzt9x_yc (b int DEFAULT upper('x'))"));
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzt9x_yd (b int DEFAULT version())"));
        assertEquals("column \"b\" is of type integer but default expression is of type uuid",
                messageOf("CREATE TABLE zzt9x_ye (b int DEFAULT gen_random_uuid())"));
        assertEquals("column \"b\" is of type integer but default expression is of type boolean",
                messageOf("CREATE TABLE zzt9x_yf (b int DEFAULT 1 = 1)"));

        // A subquery is refused before its type is ever reached, and carries no hint.
        assertEquals("0A000", stateOf("CREATE TABLE zzt9x_yg (b int DEFAULT (SELECT 1))"));
        assertEquals("cannot use subquery in DEFAULT expression",
                messageOf("CREATE TABLE zzt9x_yg (b int DEFAULT (SELECT 1))"));
        assertNull(hintOf("CREATE TABLE zzt9x_yg (b int DEFAULT (SELECT 1))"));

        // The same rule on columns that are not numbers.
        assertEquals("column \"b\" is of type boolean but default expression is of type"
                        + " timestamp with time zone",
                messageOf("CREATE TABLE zzt9x_yh (b bool DEFAULT now())"));
        assertEquals("column \"b\" is of type uuid but default expression is of type"
                        + " timestamp with time zone",
                messageOf("CREATE TABLE zzt9x_yi (b uuid DEFAULT now())"));
        assertEquals("column \"b\" is of type jsonb but default expression is of type integer",
                messageOf("CREATE TABLE zzt9x_yj (b jsonb DEFAULT 1)"));
        assertEquals("column \"b\" is of type bytea but default expression is of type integer",
                messageOf("CREATE TABLE zzt9x_yk (b bytea DEFAULT 1)"));
        assertEquals("column \"b\" is of type inet but default expression is of type integer",
                messageOf("CREATE TABLE zzt9x_yl (b inet DEFAULT 1)"));
        assertEquals("column \"b\" is of type date but default expression is of type integer",
                messageOf("CREATE TABLE zzt9x_ym (b date DEFAULT length('abc'))"));

        // Not one of them was built.
        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname LIKE 'zzt9x_y%' AND relkind = 'r'"));
    }

    @Test
    void aDomainTypedDefaultIsJudgedAgainstTheTypeTheDomainIsBuiltOver() throws Exception {
        exec("CREATE DOMAIN zzt9x_dom AS int");
        exec("CREATE DOMAIN zzt9x_domt AS text");

        // The message names the column's type — the domain — and then the type it read the
        // expression as, and carries PostgreSQL's hint.
        String first = "CREATE TABLE zzt9x_dm1 (b zzt9x_dom DEFAULT 'abc'::text)";
        assertEquals("42804", stateOf(first));
        assertTrue(messageOf(first).startsWith("column \"b\" is of type "),
                "unexpected message: " + messageOf(first));
        assertTrue(messageOf(first).endsWith("but default expression is of type text"),
                "unexpected message: " + messageOf(first));
        assertEquals("You will need to rewrite or cast the expression.", hintOf(first));

        assertEquals("42804", stateOf("CREATE TABLE zzt9x_dm2 (b zzt9x_dom DEFAULT now())"));
        assertTrue(messageOf("CREATE TABLE zzt9x_dm2 (b zzt9x_dom DEFAULT now())")
                .endsWith("but default expression is of type timestamp with time zone"));
        assertEquals("42804", stateOf("CREATE TABLE zzt9x_dm3 (b zzt9x_dom DEFAULT true)"));
        assertTrue(messageOf("CREATE TABLE zzt9x_dm3 (b zzt9x_dom DEFAULT true)")
                .endsWith("but default expression is of type boolean"));

        // The cast PostgreSQL looks for is one to the type the domain is built over, so a bare
        // string literal is read with that type's input function and the complaint names it.
        assertEquals("22P02", stateOf("CREATE TABLE zzt9x_dm4 (b zzt9x_dom DEFAULT 'abc')"));
        assertEquals("invalid input syntax for type integer: \"abc\"",
                messageOf("CREATE TABLE zzt9x_dm4 (b zzt9x_dom DEFAULT 'abc')"));

        // And the pairs the base type has an assignment cast for stand.
        exec("CREATE TABLE zzt9x_dm5 (b zzt9x_dom DEFAULT 1)");
        exec("CREATE TABLE zzt9x_dm6 (b zzt9x_domt DEFAULT 1)");
        exec("CREATE TABLE zzt9x_dm7 (b zzt9x_domt DEFAULT now())");

        // The ADD COLUMN path is held to the same rule.
        exec("CREATE TABLE zzt9x_dma (k int)");
        assertEquals("42804",
                stateOf("ALTER TABLE zzt9x_dma ADD COLUMN c zzt9x_dom DEFAULT 'abc'::text"));
        assertTrue(messageOf("ALTER TABLE zzt9x_dma ADD COLUMN c zzt9x_dom DEFAULT 'abc'::text")
                .endsWith("but default expression is of type text"));
        assertEquals("k", scalar("SELECT string_agg(attname, ',' ORDER BY attnum)"
                + " FROM pg_attribute WHERE attrelid = 'zzt9x_dma'::regclass AND attnum > 0"
                + " AND NOT attisdropped"));

        assertEquals(4, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname LIKE 'zzt9x_dm%' AND relkind = 'r'"));

        exec("DROP TABLE zzt9x_dma");
        exec("DROP TABLE zzt9x_dm5");
        exec("DROP TABLE zzt9x_dm6");
        exec("DROP TABLE zzt9x_dm7");
        exec("DROP DOMAIN zzt9x_dom");
        exec("DROP DOMAIN zzt9x_domt");
    }

    @Test
    void theDefaultsPostgresAcceptsAreStillAcceptedAndStillCompute() throws Exception {
        exec("CREATE TABLE zzt9x_ok (a int DEFAULT length('abc'), b int DEFAULT 1+1,"
                + " c int DEFAULT 1.5*2, e date DEFAULT now(),"
                + " f timestamp DEFAULT current_timestamp,"
                + " g timestamptz DEFAULT current_date, h text DEFAULT 1, i money DEFAULT 0,"
                + " j int DEFAULT extract(year from now()), k int DEFAULT NULL,"
                + " l int DEFAULT pi())");
        exec("INSERT INTO zzt9x_ok (k) VALUES (1)");

        assertEquals("3/2/3/1/3/1",
                scalar("SELECT a||'/'||b||'/'||c||'/'||h||'/'||l||'/'||k FROM zzt9x_ok"));
        assertEquals("true/true",
                scalar("SELECT (e = current_date)::text||'/'"
                        + "||(j = extract(year from now())::int)::text FROM zzt9x_ok"));

        exec("DROP TABLE zzt9x_ok");
    }

    @Test
    void aValueFunctionIsNotAConditionEither() throws Exception {
        assertEquals("42804", stateOf("CREATE TABLE zzt9x_ck1 (a int CHECK (current_date))"));
        assertEquals("argument of CHECK must be type boolean, not type date",
                messageOf("CREATE TABLE zzt9x_ck1 (a int CHECK (current_date))"));
        assertEquals("argument of CHECK must be type boolean, not type time without time zone",
                messageOf("CREATE TABLE zzt9x_ck2 (a int CHECK (localtime))"));
        assertEquals("argument of CHECK must be type boolean, not type name",
                messageOf("CREATE TABLE zzt9x_ck3 (a int CHECK (current_user))"));
        assertEquals("argument of CHECK must be type boolean, not type timestamp with time zone",
                messageOf("CREATE TABLE zzt9x_ck4 (a int CHECK (now()))"));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname LIKE 'zzt9x_ck%' AND relkind = 'r'"));
    }

    @Test
    void renameColumnOfATypedTableIsRefusedBeforeTheColumnIsLookedUp() throws Exception {
        exec("CREATE TYPE zzt9x_ct AS (x int, y text)");
        exec("CREATE TABLE zzt9x_oft OF zzt9x_ct");

        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft RENAME COLUMN y TO z"));
        assertEquals("cannot rename column of typed table",
                messageOf("ALTER TABLE zzt9x_oft RENAME COLUMN y TO z"));
        // The gate is the first thing the rename does, so a column that does not exist and a
        // rename to the name the column already has answer the same way.
        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft RENAME COLUMN nosuch TO z"));
        assertEquals("cannot rename column of typed table",
                messageOf("ALTER TABLE zzt9x_oft RENAME COLUMN nosuch TO z"));
        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft RENAME COLUMN x TO x"));

        assertEquals("x,y", scalar("SELECT string_agg(attname, ',' ORDER BY attnum)"
                + " FROM pg_attribute WHERE attrelid = 'zzt9x_oft'::regclass AND attnum > 0"
                + " AND NOT attisdropped"));

        // The table itself can still be renamed.
        exec("ALTER TABLE zzt9x_oft RENAME TO zzt9x_oft9");
        exec("ALTER TABLE zzt9x_oft9 RENAME TO zzt9x_oft");

        exec("DROP TABLE zzt9x_oft");
        exec("DROP TYPE zzt9x_ct");
    }

    @Test
    void alterColumnTypeOfATypedTableIsRefusedAfterTheColumnIsLookedUp() throws Exception {
        exec("CREATE TYPE zzt9x_ct2 AS (x int, y text)");
        exec("CREATE TABLE zzt9x_oft2 OF zzt9x_ct2");

        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN x TYPE bigint"));
        assertEquals("cannot alter column type of typed table",
                messageOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN x TYPE bigint"));
        // Even a retype to the type the column already has, and one to a type that does not
        // exist, and the serial shorthand.
        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN y TYPE text"));
        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN x TYPE nosuchtype"));
        assertEquals("42809", stateOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN x TYPE serial"));

        // Here, unlike the rename, the column lookup comes first.
        assertEquals("42703", stateOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN nosuch TYPE text"));
        assertEquals("column \"nosuch\" of relation \"zzt9x_oft2\" does not exist",
                messageOf("ALTER TABLE zzt9x_oft2 ALTER COLUMN nosuch TYPE text"));

        // What a typed table's column does accept, and the two gates that were already there.
        exec("ALTER TABLE zzt9x_oft2 ALTER COLUMN x SET DEFAULT 1");
        exec("ALTER TABLE zzt9x_oft2 ALTER COLUMN x SET NOT NULL");
        assertEquals("cannot add column to typed table",
                messageOf("ALTER TABLE zzt9x_oft2 ADD COLUMN q int"));
        assertEquals("cannot drop column from typed table",
                messageOf("ALTER TABLE zzt9x_oft2 DROP COLUMN x"));

        // The table still reads as its type does, and so does the type.
        assertEquals("x:integer,y:text",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzt9x_oft2'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));
        assertEquals("x:integer,y:text",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzt9x_ct2'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        // An ordinary table takes both statements.
        exec("CREATE TABLE zzt9x_plain (x int, y text)");
        exec("ALTER TABLE zzt9x_plain RENAME COLUMN y TO z");
        exec("ALTER TABLE zzt9x_plain ALTER COLUMN x TYPE bigint");
        assertEquals("x:bigint,z:text",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzt9x_plain'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        exec("DROP TABLE zzt9x_plain");
        exec("DROP TABLE zzt9x_oft2");
        exec("DROP TYPE zzt9x_ct2");
    }

    // ------------------------------------------------------------ A column a parent drops goes from the relations that were holding it for that parent, and from no others

    @Test
    void aColumnTwoParentsDeclareGoesOnlyWhenBothHaveLetItGo() throws Exception {
        exec("CREATE TABLE zzx2_b0 (i int, j int)");
        exec("CREATE TABLE zzx2_b1 (i int, j int)");
        exec("CREATE TABLE zzx2_b2 () INHERITS (zzx2_b0, zzx2_b1)");
        exec("INSERT INTO zzx2_b2 VALUES (1, 2)");
        exec("ALTER TABLE zzx2_b0 DROP COLUMN j");

        // One parent has let go; the other still declares the column, so the child keeps it --
        // with the value in it -- and counts the one parent that is left.
        assertEquals("i/false/2,j/false/1", attributesOf("zzx2_b2"));
        assertEquals("1/2", rowsOf("SELECT i, j FROM zzx2_b2 ORDER BY i"));

        exec("ALTER TABLE zzx2_b1 DROP COLUMN j");
        assertEquals("i/false/2", attributesOf("zzx2_b2"));

        exec("DROP TABLE zzx2_b2");
        exec("DROP TABLE zzx2_b0");
        exec("DROP TABLE zzx2_b1");
    }

    @Test
    void aChildThatListedTheColumnItselfKeepsItWithWhatItHoldsInIt() throws Exception {
        exec("CREATE TABLE zzx2_c0 (i int, j int)");
        exec("CREATE TABLE zzx2_c1 (j int) INHERITS (zzx2_c0)");
        exec("INSERT INTO zzx2_c1 VALUES (3, 4)");
        exec("ALTER TABLE zzx2_c0 DROP COLUMN j");

        assertEquals("i/true/0", attributesOf("zzx2_c0"));
        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_c1"));
        assertEquals("3/4", rowsOf("SELECT i, j FROM zzx2_c1 ORDER BY i"));
        assertEquals("3", rowsOf("SELECT i FROM zzx2_c0 ORDER BY i"));

        // Nobody hands it down any longer, so the child may now drop it itself.
        exec("ALTER TABLE zzx2_c1 DROP COLUMN j");
        assertEquals("i/false/1", attributesOf("zzx2_c1"));

        exec("DROP TABLE zzx2_c1");
        exec("DROP TABLE zzx2_c0");
    }

    @Test
    void theDropReachesTheWholeChainAndStopsWhereARelationDeclaredTheColumn() throws Exception {
        exec("CREATE TABLE zzx2_e0 (i int, j int)");
        exec("CREATE TABLE zzx2_e1 () INHERITS (zzx2_e0)");
        exec("CREATE TABLE zzx2_e2 (j int) INHERITS (zzx2_e1)");
        exec("ALTER TABLE zzx2_e0 DROP COLUMN j");

        assertEquals("i/true/0", attributesOf("zzx2_e0"));
        assertEquals("i/false/1", attributesOf("zzx2_e1"));
        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_e2"));

        // A grandchild that declares nothing loses the column all the way down.
        exec("CREATE TABLE zzx2_d0 (i int, j int)");
        exec("CREATE TABLE zzx2_d1 () INHERITS (zzx2_d0)");
        exec("CREATE TABLE zzx2_d2 () INHERITS (zzx2_d1)");
        exec("ALTER TABLE zzx2_d0 DROP COLUMN j");

        assertEquals("i/true/0", attributesOf("zzx2_d0"));
        assertEquals("i/false/1", attributesOf("zzx2_d1"));
        assertEquals("i/false/1", attributesOf("zzx2_d2"));

        exec("DROP TABLE zzx2_e2");
        exec("DROP TABLE zzx2_e1");
        exec("DROP TABLE zzx2_e0");
        exec("DROP TABLE zzx2_d2");
        exec("DROP TABLE zzx2_d1");
        exec("DROP TABLE zzx2_d0");
    }

    @Test
    void cascadeAndRestrictAreAboutTheDependentObjectsNotAboutTheChildren() throws Exception {
        exec("CREATE TABLE zzx2_h0 (i int, j int)");
        exec("CREATE TABLE zzx2_h1 (j int) INHERITS (zzx2_h0)");
        exec("ALTER TABLE zzx2_h0 DROP COLUMN j CASCADE");

        // Identical to the bare form: the child declared the column, so it keeps it.
        assertEquals("i/true/0", attributesOf("zzx2_h0"));
        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_h1"));

        exec("CREATE TABLE zzx2_g0 (i int, j int)");
        exec("CREATE TABLE zzx2_g1 () INHERITS (zzx2_g0)");
        exec("ALTER TABLE zzx2_g0 DROP COLUMN j RESTRICT");

        // RESTRICT does not refuse either: a child is not a dependent object.
        assertEquals("i/true/0", attributesOf("zzx2_g0"));
        assertEquals("i/false/1", attributesOf("zzx2_g1"));

        exec("DROP TABLE zzx2_h1");
        exec("DROP TABLE zzx2_h0");
        exec("DROP TABLE zzx2_g1");
        exec("DROP TABLE zzx2_g0");
    }

    @Test
    void onlyLeavesTheFirstGenerationHoldingTheColumnAsItsOwn() throws Exception {
        exec("CREATE TABLE zzx2_p4 (i int, j int)");
        exec("CREATE TABLE zzx2_q4 (i int, j int)");
        exec("CREATE TABLE zzx2_c4 () INHERITS (zzx2_p4, zzx2_q4)");
        exec("ALTER TABLE ONLY zzx2_p4 DROP COLUMN j");

        // The child is told both that the column is its own and that one parent still hands it
        // down; with a single parent it is told only the first of those.
        assertEquals("i/false/2,j/true/1", attributesOf("zzx2_c4"));

        exec("CREATE TABLE zzx2_p5 (i int, j int)");
        exec("CREATE TABLE zzx2_c5 () INHERITS (zzx2_p5)");
        exec("ALTER TABLE ONLY zzx2_p5 DROP COLUMN j");
        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_c5"));

        // Only the first generation is told anything: a grandchild goes on taking the column
        // from the child, which still declares it.
        exec("CREATE TABLE zzx2_y0 (i int, j int)");
        exec("CREATE TABLE zzx2_y1 () INHERITS (zzx2_y0)");
        exec("CREATE TABLE zzx2_y2 () INHERITS (zzx2_y1)");
        exec("ALTER TABLE ONLY zzx2_y0 DROP COLUMN j");

        assertEquals("i/true/0", attributesOf("zzx2_y0"));
        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_y1"));
        assertEquals("i/false/1,j/false/1", attributesOf("zzx2_y2"));

        // The child may then drop it, which takes it off the grandchild.
        exec("ALTER TABLE zzx2_y1 DROP COLUMN j");
        assertEquals("i/false/1", attributesOf("zzx2_y1"));
        assertEquals("i/false/1", attributesOf("zzx2_y2"));

        exec("DROP TABLE zzx2_c4");
        exec("DROP TABLE zzx2_p4");
        exec("DROP TABLE zzx2_q4");
        exec("DROP TABLE zzx2_c5");
        exec("DROP TABLE zzx2_p5");
        exec("DROP TABLE zzx2_y2");
        exec("DROP TABLE zzx2_y1");
        exec("DROP TABLE zzx2_y0");
    }

    @Test
    void aPartitionLosesTheColumnHoweverItJoined() throws Exception {
        exec("CREATE TABLE zzx2_m0 (i int, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_m1 PARTITION OF zzx2_m0 FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzx2_m2 (i int, j int)");
        exec("ALTER TABLE zzx2_m0 ATTACH PARTITION zzx2_m2 FOR VALUES FROM (10) TO (20)");
        exec("ALTER TABLE zzx2_m0 DROP COLUMN j");

        // The attached table declared its own column list and still loses the column, because a
        // partition declares nothing of its own.
        assertEquals("i/true/0", attributesOf("zzx2_m0"));
        assertEquals("i/false/1", attributesOf("zzx2_m1"));
        assertEquals("i/false/1", attributesOf("zzx2_m2"));

        exec("DROP TABLE zzx2_m0");
    }

    @Test
    void rollingBackADropColumnGivesEveryChildItsColumnAndItsValuesBack() throws Exception {
        exec("CREATE TABLE zzx2_v0 (i int, j int)");
        exec("CREATE TABLE zzx2_v1 () INHERITS (zzx2_v0)");
        exec("CREATE TABLE zzx2_v2 (j int) INHERITS (zzx2_v0)");
        exec("INSERT INTO zzx2_v1 VALUES (1, 2)");
        exec("INSERT INTO zzx2_v2 VALUES (3, 4)");

        exec("BEGIN");
        exec("ALTER TABLE zzx2_v0 DROP COLUMN j");
        exec("ROLLBACK");

        assertEquals("i/true/0,j/true/0", attributesOf("zzx2_v0"));
        assertEquals("i/false/1,j/false/1", attributesOf("zzx2_v1"));
        assertEquals("i/false/1,j/true/1", attributesOf("zzx2_v2"));
        assertEquals("1/2", rowsOf("SELECT i, j FROM zzx2_v1 ORDER BY i"));
        assertEquals("3/4", rowsOf("SELECT i, j FROM zzx2_v2 ORDER BY i"));

        // The ONLY form leaves the children holding the column for the parent, exactly as they
        // were before the statement.
        exec("BEGIN");
        exec("ALTER TABLE ONLY zzx2_v0 DROP COLUMN j");
        exec("ROLLBACK");

        assertEquals("i/true/0,j/true/0", attributesOf("zzx2_v0"));
        assertEquals("i/false/1,j/false/1", attributesOf("zzx2_v1"));
        assertEquals("i/false/1,j/true/1", attributesOf("zzx2_v2"));
        assertEquals("1/2", rowsOf("SELECT i, j FROM zzx2_v1 ORDER BY i"));

        exec("DROP TABLE zzx2_v2");
        exec("DROP TABLE zzx2_v1");
        exec("DROP TABLE zzx2_v0");
    }

    // ------------------------------------------------------------ A rule is counted once, when the relation takes it, and the count is not worked out again

    @Test
    void aRelationKeepingTheColumnKeepsTheCountOfTheRulesOnIt() throws Exception {
        exec("CREATE TABLE zzx2_ky0 (i int, j int NOT NULL, CONSTRAINT zzx2_kyc CHECK (j > 0))");
        exec("CREATE TABLE zzx2_ky1 () INHERITS (zzx2_ky0)");
        exec("CREATE TABLE zzx2_ky2 () INHERITS (zzx2_ky1)");
        exec("ALTER TABLE ONLY zzx2_ky0 DROP COLUMN j");

        // The column is the relation's own from now on...
        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_ky1"));
        // ...while the rules on it are still counted against the parent that let go, under the
        // name that parent gave them.
        assertEquals("zzx2_ky0_j_not_null/false/1,zzx2_kyc/false/1", constraintsOf("zzx2_ky1"));
        assertEquals("zzx2_ky0_j_not_null/false/1,zzx2_kyc/false/1", constraintsOf("zzx2_ky2"));

        // So neither may be withdrawn here.
        assertEquals("42P16", stateOf("ALTER TABLE zzx2_ky1 DROP CONSTRAINT zzx2_kyc"));
        assertEquals("cannot drop inherited constraint \"zzx2_kyc\" of relation \"zzx2_ky1\"",
                messageOf("ALTER TABLE zzx2_ky1 DROP CONSTRAINT zzx2_kyc"));
        assertEquals("cannot drop inherited constraint \"zzx2_ky0_j_not_null\""
                        + " of relation \"zzx2_ky1\"",
                messageOf("ALTER TABLE zzx2_ky1 DROP CONSTRAINT zzx2_ky0_j_not_null"));

        // Leaving the hierarchy does not make them the relation's own: the count is one that
        // nothing left standing can decrement.
        exec("ALTER TABLE zzx2_ky1 NO INHERIT zzx2_ky0");
        assertEquals("zzx2_ky0_j_not_null/false/1,zzx2_kyc/false/1", constraintsOf("zzx2_ky1"));
        assertEquals("42P16", stateOf("ALTER TABLE zzx2_ky1 DROP CONSTRAINT zzx2_kyc"));

        // Dropping the column takes them, and takes the descendant's with them.
        exec("ALTER TABLE zzx2_ky1 DROP COLUMN j");
        assertNull(constraintsOf("zzx2_ky1"));
        assertNull(constraintsOf("zzx2_ky2"));

        exec("DROP TABLE zzx2_ky2");
        exec("DROP TABLE zzx2_ky1");
        exec("DROP TABLE zzx2_ky0");
    }

    @Test
    void aChildThatDeclaredTheColumnItselfStillCountsTheParentThatLetGo() throws Exception {
        exec("CREATE TABLE zzx2_k0 (i int, j int NOT NULL, CONSTRAINT zzx2_kc CHECK (j > 0))");
        exec("CREATE TABLE zzx2_k1 (j int NOT NULL) INHERITS (zzx2_k0)");
        // No ONLY: the drop reaches the child, which keeps the column because it declared it.
        exec("ALTER TABLE zzx2_k0 DROP COLUMN j");

        assertEquals("i/false/1,j/true/0", attributesOf("zzx2_k1"));
        // The child's own NOT NULL keeps its own name and the count it was created with; the
        // CHECK it only took keeps the parent's.
        assertEquals("zzx2_k1_j_not_null/true/1,zzx2_kc/false/1", constraintsOf("zzx2_k1"));
        assertEquals("cannot drop inherited constraint \"zzx2_kc\" of relation \"zzx2_k1\"",
                messageOf("ALTER TABLE zzx2_k1 DROP CONSTRAINT zzx2_kc"));
        assertEquals("cannot drop inherited constraint \"zzx2_k1_j_not_null\""
                        + " of relation \"zzx2_k1\"",
                messageOf("ALTER TABLE zzx2_k1 DROP CONSTRAINT zzx2_k1_j_not_null"));

        // A child that restated the rule as well counts it exactly the same way.
        exec("CREATE TABLE zzx2_nn0 (i int, j int NOT NULL, CONSTRAINT zzx2_nc CHECK (j > 0))");
        exec("CREATE TABLE zzx2_nn1 (j int NOT NULL, CONSTRAINT zzx2_nc CHECK (j > 0))"
                + " INHERITS (zzx2_nn0)");
        exec("ALTER TABLE ONLY zzx2_nn0 DROP COLUMN j");
        assertEquals("zzx2_nc/true/1,zzx2_nn1_j_not_null/true/1", constraintsOf("zzx2_nn1"));
        assertEquals("42P16", stateOf("ALTER TABLE zzx2_nn1 DROP CONSTRAINT zzx2_nc"));

        exec("DROP TABLE zzx2_k1");
        exec("DROP TABLE zzx2_k0");
        exec("DROP TABLE zzx2_nn1");
        exec("DROP TABLE zzx2_nn0");
    }

    @Test
    void underTwoParentsBothAreStillCountedAfterOneLetsGoOfTheColumn() throws Exception {
        exec("CREATE TABLE zzx2_z0 (i int, j int NOT NULL, CONSTRAINT zzx2_zc CHECK (j > 0))");
        exec("CREATE TABLE zzx2_z1 (i int, j int NOT NULL, CONSTRAINT zzx2_zc CHECK (j > 0))");
        exec("CREATE TABLE zzx2_z2 () INHERITS (zzx2_z0, zzx2_z1)");
        exec("ALTER TABLE ONLY zzx2_z0 DROP COLUMN j");

        assertEquals("zzx2_z0_j_not_null/false/2,zzx2_zc/false/2", constraintsOf("zzx2_z2"));
        assertEquals("i/false/2,j/true/1", attributesOf("zzx2_z2"));

        // A rule over another column is untouched by the drop.
        exec("CREATE TABLE zzx2_o0 (i int, j int, CONSTRAINT zzx2_oc CHECK (i > 0))");
        exec("CREATE TABLE zzx2_o1 () INHERITS (zzx2_o0)");
        exec("ALTER TABLE ONLY zzx2_o0 DROP COLUMN j");
        assertEquals("zzx2_oc/false/1", constraintsOf("zzx2_o1"));

        exec("DROP TABLE zzx2_z2");
        exec("DROP TABLE zzx2_z0");
        exec("DROP TABLE zzx2_z1");
        exec("DROP TABLE zzx2_o1");
        exec("DROP TABLE zzx2_o0");
    }

    @Test
    void rollingBackADropColumnRestoresTheRulesOnItAndTheyAreEnforcedAgain() throws Exception {
        exec("CREATE TABLE zzx2_ht (i int, j int, CONSTRAINT zzx2_htc CHECK (j > 0))");
        exec("BEGIN");
        exec("ALTER TABLE zzx2_ht DROP COLUMN j");
        exec("ROLLBACK");

        assertEquals("zzx2_htc/true/0", constraintsOf("zzx2_ht"));
        // And the rule is enforced again, not merely listed.
        exec("INSERT INTO zzx2_ht VALUES (1, 5)");
        assertEquals("23514", stateOf("INSERT INTO zzx2_ht VALUES (2, -1)"));
        assertEquals("new row for relation \"zzx2_ht\" violates check constraint \"zzx2_htc\"",
                messageOf("INSERT INTO zzx2_ht VALUES (2, -1)"));
        assertEquals(1, num("SELECT count(*)::int FROM zzx2_ht"));

        // The rollback reaches the relations below too.
        exec("CREATE TABLE zzx2_hu0 (i int, j int NOT NULL, CONSTRAINT zzx2_huc CHECK (j > 0))");
        exec("CREATE TABLE zzx2_hu1 () INHERITS (zzx2_hu0)");
        exec("BEGIN");
        exec("ALTER TABLE ONLY zzx2_hu0 DROP COLUMN j");
        exec("ROLLBACK");

        assertEquals("zzx2_hu0_j_not_null/false/1,zzx2_huc/false/1", constraintsOf("zzx2_hu1"));
        assertEquals("42P16", stateOf("ALTER TABLE zzx2_hu1 DROP CONSTRAINT zzx2_huc"));

        exec("DROP TABLE zzx2_ht");
        exec("DROP TABLE zzx2_hu1");
        exec("DROP TABLE zzx2_hu0");
    }

    // ------------------------------------------------------------ A constraint is named once, when it is created, and answers to that name afterwards

    @Test
    void aDetachedPartitionKeepsThePartitionedTablesNameForItsNotNull() throws Exception {
        exec("CREATE TABLE zzx2_dt (i int NOT NULL, j int, CONSTRAINT zzx2_dtck CHECK (i > 0))"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_dt0 PARTITION OF zzx2_dt FOR VALUES FROM (0) TO (10)");

        assertEquals("zzx2_dt_i_not_null/false/1,zzx2_dtck/false/1", constraintsOf("zzx2_dt0"));

        exec("ALTER TABLE zzx2_dt DETACH PARTITION zzx2_dt0");
        assertEquals("zzx2_dt_i_not_null/true/0,zzx2_dtck/true/0", constraintsOf("zzx2_dt0"));

        // Being its own is what lets it withdraw the rule -- under the name it holds.
        exec("ALTER TABLE zzx2_dt0 DROP CONSTRAINT zzx2_dt_i_not_null");
        assertEquals("zzx2_dtck/true/0", constraintsOf("zzx2_dt0"));

        // A name the writer chose is kept the same way.
        exec("CREATE TABLE zzx2_nn (i int CONSTRAINT zzx2_nnown NOT NULL, j int)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_nn0 PARTITION OF zzx2_nn FOR VALUES FROM (0) TO (10)");
        assertEquals("zzx2_nnown/false/1", constraintsOf("zzx2_nn0"));
        exec("ALTER TABLE zzx2_nn DETACH PARTITION zzx2_nn0");
        assertEquals("zzx2_nnown/true/0", constraintsOf("zzx2_nn0"));

        // Through two levels, the relation below the detached one is still holding the rule for
        // somebody, and goes on naming it the same way.
        exec("CREATE TABLE zzx2_zp (i int NOT NULL, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_zp0 PARTITION OF zzx2_zp FOR VALUES FROM (0) TO (100)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_zp00 PARTITION OF zzx2_zp0 FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzx2_zp DETACH PARTITION zzx2_zp0");
        assertEquals("zzx2_zp_i_not_null/true/0", constraintsOf("zzx2_zp"));
        assertEquals("zzx2_zp_i_not_null/true/0", constraintsOf("zzx2_zp0"));
        assertEquals("zzx2_zp_i_not_null/false/1", constraintsOf("zzx2_zp00"));

        exec("DROP TABLE zzx2_dt");
        exec("DROP TABLE zzx2_dt0");
        exec("DROP TABLE zzx2_nn");
        exec("DROP TABLE zzx2_nn0");
        exec("DROP TABLE zzx2_zp");
        exec("DROP TABLE zzx2_zp0");
    }

    @Test
    void aTableTakenOutOfAnInheritanceHierarchyKeepsTheNamesItHeld() throws Exception {
        exec("CREATE TABLE zzx2_t0 (i int NOT NULL, j int)");
        exec("CREATE TABLE zzx2_t1 () INHERITS (zzx2_t0)");
        exec("ALTER TABLE zzx2_t0 ALTER COLUMN j SET NOT NULL");

        assertEquals("zzx2_t0_i_not_null/false/1,zzx2_t0_j_not_null/false/1",
                constraintsOf("zzx2_t1"));

        exec("ALTER TABLE zzx2_t1 NO INHERIT zzx2_t0");
        assertEquals("zzx2_t0_i_not_null/true/0,zzx2_t0_j_not_null/true/0",
                constraintsOf("zzx2_t1"));

        exec("DROP TABLE zzx2_t1");
        exec("DROP TABLE zzx2_t0");
    }

    @Test
    void aNotNullAParentDeclaresIsAskedForByTheNameItWasDeclaredWith() throws Exception {
        exec("CREATE TABLE zzx2_sp (i int NOT NULL, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_sp0 PARTITION OF zzx2_sp FOR VALUES FROM (0) TO (10)");

        assertEquals("42P16", stateOf("ALTER TABLE zzx2_sp0 DROP CONSTRAINT zzx2_sp_i_not_null"));
        assertEquals("cannot drop inherited constraint \"zzx2_sp_i_not_null\""
                        + " of relation \"zzx2_sp0\"",
                messageOf("ALTER TABLE zzx2_sp0 DROP CONSTRAINT zzx2_sp_i_not_null"));

        // The name the partition would have chosen for itself belongs to no constraint anywhere.
        assertEquals("42704", stateOf("ALTER TABLE zzx2_sp0 DROP CONSTRAINT zzx2_sp0_i_not_null"));
        assertEquals("constraint \"zzx2_sp0_i_not_null\" of relation \"zzx2_sp0\" does not exist",
                messageOf("ALTER TABLE zzx2_sp0 DROP CONSTRAINT zzx2_sp0_i_not_null"));

        assertEquals("42P16", stateOf("ALTER TABLE zzx2_sp0 ALTER COLUMN i DROP NOT NULL"));
        assertEquals("column \"i\" is marked NOT NULL in parent table",
                messageOf("ALTER TABLE zzx2_sp0 ALTER COLUMN i DROP NOT NULL"));

        // The same two answers for an inheritance child, at any depth.
        exec("CREATE TABLE zzx2_r0 (i int NOT NULL, j int)");
        exec("CREATE TABLE zzx2_r1 () INHERITS (zzx2_r0)");
        exec("CREATE TABLE zzx2_r2 () INHERITS (zzx2_r1)");
        assertEquals("zzx2_r0_i_not_null/false/1", constraintsOf("zzx2_r1"));
        assertEquals("zzx2_r0_i_not_null/false/1", constraintsOf("zzx2_r2"));
        assertEquals("cannot drop inherited constraint \"zzx2_r0_i_not_null\""
                        + " of relation \"zzx2_r1\"",
                messageOf("ALTER TABLE zzx2_r1 DROP CONSTRAINT zzx2_r0_i_not_null"));
        assertEquals("constraint \"zzx2_r2_i_not_null\" of relation \"zzx2_r2\" does not exist",
                messageOf("ALTER TABLE zzx2_r2 DROP CONSTRAINT zzx2_r2_i_not_null"));

        // And LIKE copies the constraint, which means the name it answers to on the source.
        exec("CREATE TABLE zzx2_u3 (LIKE zzx2_r1)");
        assertEquals("zzx2_r0_i_not_null/true/0", constraintsOf("zzx2_u3"));

        exec("DROP TABLE zzx2_sp");
        exec("DROP TABLE zzx2_u3");
        exec("DROP TABLE zzx2_r2");
        exec("DROP TABLE zzx2_r1");
        exec("DROP TABLE zzx2_r0");
    }

    @Test
    void aNotNullKeepsTheNameItWasCreatedWithWhenOneOfTwoParentsIsLetGo() throws Exception {
        exec("CREATE TABLE zzx2_w0 (i int NOT NULL)");
        exec("CREATE TABLE zzx2_w1 (i int NOT NULL)");
        exec("CREATE TABLE zzx2_w2 () INHERITS (zzx2_w0, zzx2_w1)");
        // Both parents agree; the constraint is created under the first one's name.
        assertEquals("zzx2_w0_i_not_null/false/2", constraintsOf("zzx2_w2"));

        exec("ALTER TABLE zzx2_w2 NO INHERIT zzx2_w0");
        // The remaining parent does not rename it.
        assertEquals("zzx2_w0_i_not_null/false/1", constraintsOf("zzx2_w2"));

        exec("ALTER TABLE zzx2_w2 NO INHERIT zzx2_w1");
        assertEquals("zzx2_w0_i_not_null/true/0", constraintsOf("zzx2_w2"));
        exec("ALTER TABLE zzx2_w2 DROP CONSTRAINT zzx2_w0_i_not_null");
        assertNull(constraintsOf("zzx2_w2"));

        // Letting the second parent go first makes no difference to the name.
        exec("CREATE TABLE zzx2_x2 () INHERITS (zzx2_w0, zzx2_w1)");
        exec("ALTER TABLE zzx2_x2 NO INHERIT zzx2_w1");
        assertEquals("zzx2_w0_i_not_null/false/1", constraintsOf("zzx2_x2"));

        exec("DROP TABLE zzx2_x2");
        exec("DROP TABLE zzx2_w2");
        exec("DROP TABLE zzx2_w0");
        exec("DROP TABLE zzx2_w1");
    }

    // ------------------------------------------------------------ A table joining a hierarchy has to carry the rules it will answer for

    @Test
    void attachPartitionReadsTheRulesBeforeItScansTheRows() throws Exception {
        exec("CREATE TABLE zzx2_ao (i int NOT NULL, j int, CONSTRAINT zzx2_aock CHECK (j > 0))"
                + " PARTITION BY RANGE (i)");

        exec("CREATE TABLE zzx2_ao1 (i int, j int)");
        assertEquals("42804", stateOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao1"
                + " FOR VALUES FROM (0) TO (10)"));
        assertEquals("column \"i\" in child table \"zzx2_ao1\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao1"
                        + " FOR VALUES FROM (0) TO (10)"));

        // This table holds a row outside the bound as well, and PostgreSQL still names the
        // missing rule: the rules are read ahead of the rows.
        exec("CREATE TABLE zzx2_ao2 (i int NOT NULL, j int)");
        exec("INSERT INTO zzx2_ao2 VALUES (99, 1)");
        assertEquals("42804", stateOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao2"
                + " FOR VALUES FROM (0) TO (10)"));
        assertEquals("child table is missing constraint \"zzx2_aock\"",
                messageOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao2"
                        + " FOR VALUES FROM (0) TO (10)"));

        exec("CREATE TABLE zzx2_ao6 (i int NOT NULL, j int,"
                + " CONSTRAINT zzx2_aock CHECK (j > 0) NO INHERIT)");
        assertEquals("42P17", stateOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao6"
                + " FOR VALUES FROM (20) TO (30)"));
        assertEquals("constraint \"zzx2_aock\" conflicts with non-inherited constraint"
                        + " on child table \"zzx2_ao6\"",
                messageOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao6"
                        + " FOR VALUES FROM (20) TO (30)"));

        // A rule of that name testing something else is a different rule; a pair of parentheses
        // is not.
        exec("CREATE TABLE zzx2_ao5 (i int NOT NULL, j int,"
                + " CONSTRAINT zzx2_aock CHECK (0 < j))");
        assertEquals("42804", stateOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao5"
                + " FOR VALUES FROM (20) TO (30)"));
        assertEquals("child table \"zzx2_ao5\" has different definition"
                        + " for check constraint \"zzx2_aock\"",
                messageOf("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao5"
                        + " FOR VALUES FROM (20) TO (30)"));

        exec("CREATE TABLE zzx2_ao4 (i int NOT NULL, j int,"
                + " CONSTRAINT zzx2_aock CHECK ((j) > 0))");
        exec("ALTER TABLE zzx2_ao ATTACH PARTITION zzx2_ao4 FOR VALUES FROM (0) TO (10)");

        // The attached table keeps its OWN names, unlike a partition created with PARTITION OF,
        // and gets them back as its own on DETACH.
        assertEquals("zzx2_ao4_i_not_null/false/1,zzx2_aock/false/1", constraintsOf("zzx2_ao4"));
        exec("ALTER TABLE zzx2_ao DETACH PARTITION zzx2_ao4");
        assertEquals("zzx2_ao4_i_not_null/true/0,zzx2_aock/true/0", constraintsOf("zzx2_ao4"));

        exec("DROP TABLE zzx2_ao");
        exec("DROP TABLE zzx2_ao1");
        exec("DROP TABLE zzx2_ao2");
        exec("DROP TABLE zzx2_ao4");
        exec("DROP TABLE zzx2_ao5");
        exec("DROP TABLE zzx2_ao6");
    }

    @Test
    void inheritComparesWhatTheConstraintsSayRatherThanHowTheyWereWritten() throws Exception {
        exec("CREATE TABLE zzx2_ai (i int NOT NULL, j int, CONSTRAINT zzx2_aick CHECK (j > 0))");

        exec("CREATE TABLE zzx2_ai1 (i int NOT NULL, j int)");
        assertEquals("42804", stateOf("ALTER TABLE zzx2_ai1 INHERIT zzx2_ai"));
        assertEquals("child table is missing constraint \"zzx2_aick\"",
                messageOf("ALTER TABLE zzx2_ai1 INHERIT zzx2_ai"));

        exec("CREATE TABLE zzx2_ai2 (i int, j int, CONSTRAINT zzx2_aick CHECK (j > 0))");
        assertEquals("column \"i\" in child table \"zzx2_ai2\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzx2_ai2 INHERIT zzx2_ai"));

        exec("CREATE TABLE zzx2_ai3 (i int NOT NULL, j int,"
                + " CONSTRAINT zzx2_aick CHECK (j > 0) NO INHERIT)");
        assertEquals("42P17", stateOf("ALTER TABLE zzx2_ai3 INHERIT zzx2_ai"));
        assertEquals("constraint \"zzx2_aick\" conflicts with non-inherited constraint"
                        + " on child table \"zzx2_ai3\"",
                messageOf("ALTER TABLE zzx2_ai3 INHERIT zzx2_ai"));

        // The same rule written the other way round is a different rule; so is one over a
        // different constant.
        exec("CREATE TABLE zzx2_ai4 (i int NOT NULL, j int, CONSTRAINT zzx2_aick CHECK (0 < j))");
        assertEquals("child table \"zzx2_ai4\" has different definition"
                        + " for check constraint \"zzx2_aick\"",
                messageOf("ALTER TABLE zzx2_ai4 INHERIT zzx2_ai"));
        exec("CREATE TABLE zzx2_ai5 (i int NOT NULL, j int, CONSTRAINT zzx2_aick CHECK (j > 1))");
        assertEquals("child table \"zzx2_ai5\" has different definition"
                        + " for check constraint \"zzx2_aick\"",
                messageOf("ALTER TABLE zzx2_ai5 INHERIT zzx2_ai"));

        // A pair of parentheses is not.
        exec("CREATE TABLE zzx2_ai6 (i int NOT NULL, j int,"
                + " CONSTRAINT zzx2_aick CHECK ((j) > 0))");
        exec("ALTER TABLE zzx2_ai6 INHERIT zzx2_ai");
        assertEquals("zzx2_ai6_i_not_null/true/1,zzx2_aick/true/1", constraintsOf("zzx2_ai6"));

        exec("DROP TABLE zzx2_ai6");
        exec("DROP TABLE zzx2_ai5");
        exec("DROP TABLE zzx2_ai4");
        exec("DROP TABLE zzx2_ai3");
        exec("DROP TABLE zzx2_ai2");
        exec("DROP TABLE zzx2_ai1");
        exec("DROP TABLE zzx2_ai");
    }

    @Test
    void aNoInheritCheckIsRefusedOnAPartitionedTable() throws Exception {
        assertEquals("42P16", stateOf("CREATE TABLE zzx2_p1 (i int,"
                + " CONSTRAINT zzx2_c1 CHECK (i > 0) NO INHERIT) PARTITION BY RANGE (i)"));
        assertEquals("cannot add NO INHERIT constraint to partitioned table \"zzx2_p1\"",
                messageOf("CREATE TABLE zzx2_p1 (i int,"
                        + " CONSTRAINT zzx2_c1 CHECK (i > 0) NO INHERIT) PARTITION BY RANGE (i)"));
        // The clause is refused whether or not the writer named the constraint, and under every
        // partitioning strategy: what the hierarchy cannot express does not depend on either.
        assertEquals("cannot add NO INHERIT constraint to partitioned table \"zzx2_p2\"",
                messageOf("CREATE TABLE zzx2_p2 (i int, CHECK (i > 0) NO INHERIT)"
                        + " PARTITION BY LIST (i)"));
        assertEquals("cannot add NO INHERIT constraint to partitioned table \"zzx2_p3\"",
                messageOf("CREATE TABLE zzx2_p3 (i int, CHECK (i > 0) NO INHERIT)"
                        + " PARTITION BY HASH (i)"));

        // A NOT NULL saying the same thing is a different refusal, and is read before the
        // partition key and before any of the CHECK constraints.
        assertEquals("0A000", stateOf("CREATE TABLE zzx2_n1 (i int NOT NULL NO INHERIT)"
                + " PARTITION BY RANGE (i)"));
        assertEquals("not-null constraints on partitioned tables cannot be NO INHERIT",
                messageOf("CREATE TABLE zzx2_n1 (i int NOT NULL NO INHERIT)"
                        + " PARTITION BY RANGE (i)"));
        assertEquals("not-null constraints on partitioned tables cannot be NO INHERIT",
                messageOf("CREATE TABLE zzx2_n2 (i int NOT NULL NO INHERIT)"
                        + " PARTITION BY RANGE (nosuchcol)"));
        assertEquals("not-null constraints on partitioned tables cannot be NO INHERIT",
                messageOf("CREATE TABLE zzx2_n3 (i int,"
                        + " CONSTRAINT zzx2_c CHECK (i > 0) NO INHERIT,"
                        + " j int NOT NULL NO INHERIT) PARTITION BY RANGE (i)"));

        // The CHECK is read after the partition key and after its own expression.
        assertEquals("column \"nosuchcol\" named in partition key does not exist",
                messageOf("CREATE TABLE zzx2_o1 (i int,"
                        + " CONSTRAINT zzx2_c CHECK (i > 0) NO INHERIT)"
                        + " PARTITION BY RANGE (nosuchcol)"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("CREATE TABLE zzx2_o2 (i int,"
                        + " CONSTRAINT zzx2_c CHECK (nosuchcol > 0) NO INHERIT)"
                        + " PARTITION BY RANGE (i)"));

        // ALTER TABLE answers the same way.
        exec("CREATE TABLE zzx2_pt (i int) PARTITION BY RANGE (i)");
        assertEquals("42P16", stateOf("ALTER TABLE zzx2_pt ADD CONSTRAINT zzx2_ni"
                + " CHECK (i > 0) NO INHERIT"));
        assertEquals("cannot add NO INHERIT constraint to partitioned table \"zzx2_pt\"",
                messageOf("ALTER TABLE zzx2_pt ADD CONSTRAINT zzx2_ni CHECK (i > 0) NO INHERIT"));
        exec("DROP TABLE zzx2_pt");

        // On an ordinary table the rule can be expressed, and is recorded.
        exec("CREATE TABLE zzx2_q1 (i int, CONSTRAINT zzx2_qc CHECK (i > 0) NO INHERIT)");
        assertEquals("zzx2_qc/true/true/0",
                scalar("SELECT conname||'/'||connoinherit::text||'/'||conislocal::text||'/'"
                        + "||coninhcount::text FROM pg_constraint"
                        + " WHERE conrelid = 'zzx2_q1'::regclass"));
        exec("DROP TABLE zzx2_q1");

        // Not one of the refused statements built anything.
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname IN"
                + " ('zzx2_p1','zzx2_p2','zzx2_p3','zzx2_n1','zzx2_n2','zzx2_n3',"
                + "'zzx2_o1','zzx2_o2')"));
    }

    // ------------------------------------------------------------ What a DROP names as depending on the relation

    @Test
    void aDropNamesEveryGenerationBelowTheRelation() throws Exception {
        exec("CREATE TABLE zzx2_r1x (i int)");
        exec("CREATE TABLE zzx2_r2x (j int) INHERITS (zzx2_r1x)");
        exec("CREATE TABLE zzx2_r4x (m int) INHERITS (zzx2_r1x)");
        exec("CREATE TABLE zzx2_r5x (n int) INHERITS (zzx2_r4x)");
        // Created last and still listed straight after its own parent, because the walk follows
        // a child's dependents before the child's siblings.
        exec("CREATE TABLE zzx2_r3x (k int) INHERITS (zzx2_r2x)");
        exec("CREATE VIEW zzx2_v1x AS SELECT i FROM zzx2_r1x");

        org.postgresql.util.ServerErrorMessage m = fieldsOf("DROP TABLE zzx2_r1x");
        assertEquals("2BP01", m.getSQLState());
        assertEquals("cannot drop table zzx2_r1x because other objects depend on it",
                m.getMessage());
        assertEquals("table zzx2_r2x depends on table zzx2_r1x\n"
                + "table zzx2_r3x depends on table zzx2_r2x\n"
                + "table zzx2_r4x depends on table zzx2_r1x\n"
                + "table zzx2_r5x depends on table zzx2_r4x\n"
                + "view zzx2_v1x depends on table zzx2_r1x", m.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", m.getHint());

        exec("DROP TABLE zzx2_r1x CASCADE");
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname LIKE 'zzx2\\_r_x'"));
    }

    @Test
    void aRelationReachedDownTwoPathsIsNamedOnceAgainstTheSecondOfThem() throws Exception {
        exec("CREATE TABLE zzx2_s1x (i int)");
        exec("CREATE TABLE zzx2_s2x (j int) INHERITS (zzx2_s1x)");
        exec("CREATE TABLE zzx2_s3x (k int) INHERITS (zzx2_s2x)");
        exec("CREATE TABLE zzx2_s4x (m int) INHERITS (zzx2_s1x)");
        exec("CREATE TABLE zzx2_s6x (n int) INHERITS (zzx2_s2x, zzx2_s4x)");

        assertEquals("table zzx2_s2x depends on table zzx2_s1x\n"
                        + "table zzx2_s3x depends on table zzx2_s2x\n"
                        + "table zzx2_s4x depends on table zzx2_s1x\n"
                        + "table zzx2_s6x depends on table zzx2_s4x",
                detailOf("DROP TABLE zzx2_s1x"));

        exec("DROP TABLE zzx2_s1x CASCADE");
    }

    @Test
    void theRefusalsAboutAConstraintCarryNoPosition() throws Exception {
        exec("CREATE TABLE zzx2_ha (i int NOT NULL, CONSTRAINT zzx2_hck CHECK (i > 0))");
        exec("CREATE TABLE zzx2_hb (LIKE zzx2_ha) INHERITS (zzx2_ha)");
        exec("CREATE TABLE zzx2_hp2 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzx2_hg (i int, CONSTRAINT zzx2_hck CHECK (i > 0) NO INHERIT)");
        exec("ALTER TABLE zzx2_hg ALTER COLUMN i SET NOT NULL");
        exec("CREATE TABLE zzx2_hc (i int)");
        exec("CREATE TABLE zzx2_hd (i int, j int)");

        // PostgreSQL reads the constraint out of the catalogue rather than out of the statement
        // text, so it has nowhere in the text to point the reader at and sends no Position.
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_ha DROP CONSTRAINT zzx2_hnosuch").getPosition());
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_hb DROP CONSTRAINT zzx2_hck").getPosition());
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_hb DROP CONSTRAINT zzx2_ha_i_not_null")
                .getPosition());
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_hp2 ADD CONSTRAINT zzx2_hni"
                + " CHECK (i > 0) NO INHERIT").getPosition());
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_hc INHERIT zzx2_ha").getPosition());
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_hg INHERIT zzx2_ha").getPosition());
        assertEquals(0, fieldsOf("ALTER TABLE zzx2_hp2 ATTACH PARTITION zzx2_hd"
                + " FOR VALUES FROM (0) TO (10)").getPosition());

        // The messages themselves are each still the message PostgreSQL sends.
        assertEquals("constraint \"zzx2_hnosuch\" of relation \"zzx2_ha\" does not exist",
                messageOf("ALTER TABLE zzx2_ha DROP CONSTRAINT zzx2_hnosuch"));
        assertEquals("cannot drop inherited constraint \"zzx2_hck\" of relation \"zzx2_hb\"",
                messageOf("ALTER TABLE zzx2_hb DROP CONSTRAINT zzx2_hck"));
        assertEquals("table \"zzx2_hd\" contains column \"j\" not found in parent \"zzx2_hp2\"",
                messageOf("ALTER TABLE zzx2_hp2 ATTACH PARTITION zzx2_hd"
                        + " FOR VALUES FROM (0) TO (10)"));
        assertEquals("The new partition may contain only the columns present in parent.",
                detailOf("ALTER TABLE zzx2_hp2 ATTACH PARTITION zzx2_hd"
                        + " FOR VALUES FROM (0) TO (10)"));

        exec("DROP TABLE zzx2_ha CASCADE");
        exec("DROP TABLE zzx2_hp2");
        exec("DROP TABLE zzx2_hg");
        exec("DROP TABLE zzx2_hc");
        exec("DROP TABLE zzx2_hd");
    }

    // ------------------------------------------------------------ A dropped column keeps its attribute number, and everything that recorded one still means it

    @Test
    void aDroppedColumnLeavesItsRowAndItsNumberBehind() throws Exception {
        exec("CREATE TABLE zzx2_t (a int, b varchar(5), c int, d int DEFAULT 7, CHECK (c > 0))");
        exec("CREATE UNIQUE INDEX zzx2_i ON zzx2_t (c, a)");
        exec("COMMENT ON COLUMN zzx2_t.d IS 'note on d'");
        exec("ALTER TABLE zzx2_t DROP COLUMN b");
        exec("ALTER TABLE zzx2_t ADD COLUMN e text");

        // The attribute stays, under a placeholder name, with no type and with the width, storage
        // and alignment the declared type gave it.
        assertEquals("a/1/f/23/-1/4/p/i"
                        + ";........pg.dropped.2......../2/t/0/9/-1/x/i"
                        + ";c/3/f/23/-1/4/p/i"
                        + ";d/4/f/23/-1/4/p/i"
                        + ";e/5/f/25/-1/-1/x/i",
                rowsOf("SELECT attname, attnum, attisdropped, atttypid, atttypmod, attlen,"
                        + " attstorage, attalign FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_t'::regclass AND attnum > 0 ORDER BY attnum"));

        // And is required of nothing.
        assertEquals("f/f///f/0",
                rowsOf("SELECT attnotnull, atthasdef, attidentity, attgenerated, atthasmissing,"
                        + " attndims FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_t'::regclass AND attnum = 2"));

        assertEquals(5, num("SELECT relnatts FROM pg_class WHERE relname = 'zzx2_t'"));

        // The catalogue of live columns leaves the hole where the drop was.
        assertEquals("a:1:1,c:3:3,d:4:4,e:5:5",
                scalar("SELECT string_agg(column_name||':'||ordinal_position||':'||dtd_identifier,"
                        + " ',' ORDER BY ordinal_position) FROM information_schema.columns"
                        + " WHERE table_name = 'zzx2_t'"));

        // Everything that recorded an attribute number still names the same column.
        assertEquals("zzx2_t_c_check/3",
                rowsOf("SELECT conname, array_to_string(conkey, ' ') FROM pg_constraint"
                        + " WHERE conrelid = 'zzx2_t'::regclass ORDER BY conname"));
        assertEquals("zzx2_i/3 1/2",
                rowsOf("SELECT c.relname, i.indkey::text, i.indnatts FROM pg_index i"
                        + " JOIN pg_class c ON c.oid = i.indexrelid"
                        + " WHERE i.indrelid = 'zzx2_t'::regclass ORDER BY 1"));
        assertEquals("4/7",
                rowsOf("SELECT adnum, pg_get_expr(adbin, adrelid) FROM pg_attrdef"
                        + " WHERE adrelid = 'zzx2_t'::regclass ORDER BY adnum"));
        assertEquals("4/note on d",
                rowsOf("SELECT objsubid, description FROM pg_description"
                        + " WHERE objoid = 'zzx2_t'::regclass ORDER BY objsubid"));
        assertEquals("note on d", scalar("SELECT col_description('zzx2_t'::regclass, 4)"));

        // The relation itself is unchanged: an INSERT with no column list takes four values, not
        // five, because the tombstone is not a column of the relation.
        exec("INSERT INTO zzx2_t VALUES (1, 2, 3, 'x')");
        assertEquals("1/2/3/x", rowsOf("SELECT * FROM zzx2_t"));

        exec("DROP TABLE zzx2_t");
    }

    @Test
    void aForeignKeyAndATriggerHoldAttributeNumbersToo() throws Exception {
        exec("CREATE TABLE zzx2_pk (p int, q int, r int UNIQUE)");
        exec("ALTER TABLE zzx2_pk DROP COLUMN q");
        exec("CREATE TABLE zzx2_fk (x int, y int, z int REFERENCES zzx2_pk(r))");
        exec("ALTER TABLE zzx2_fk DROP COLUMN y");

        // Both sides of the key are attribute numbers, and both relations have a hole.
        assertEquals("zzx2_fk_z_fkey/3/3",
                rowsOf("SELECT conname, array_to_string(conkey,' '),"
                        + " array_to_string(confkey,' ') FROM pg_constraint"
                        + " WHERE conrelid = 'zzx2_fk'::regclass ORDER BY conname"));

        exec("DROP TABLE zzx2_fk");
        exec("DROP TABLE zzx2_pk");

        exec("CREATE FUNCTION zzx2_f() RETURNS trigger LANGUAGE plpgsql"
                + " AS $$ BEGIN RETURN NEW; END $$");
        exec("CREATE TABLE zzx2_tg (m int, n int, o int, p int)");
        exec("ALTER TABLE zzx2_tg DROP COLUMN n");
        exec("CREATE TRIGGER zzx2_trg BEFORE UPDATE OF o, p ON zzx2_tg"
                + " FOR EACH ROW EXECUTE FUNCTION zzx2_f()");

        assertEquals("zzx2_trg/3 4",
                rowsOf("SELECT tgname, tgattr::text FROM pg_trigger"
                        + " WHERE tgrelid = 'zzx2_tg'::regclass AND NOT tgisinternal"));

        // A column added after the drop takes the next number, not the freed one.
        exec("ALTER TABLE zzx2_tg ADD COLUMN n int");
        assertEquals("m:1,........pg.dropped.2........:2,o:3,p:4,n:5",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_tg'::regclass AND attnum > 0"));

        exec("DROP TABLE zzx2_tg");
        exec("DROP FUNCTION zzx2_f()");
    }

    @Test
    void theNumbersTakenAreHeldInAscendingOrderNotInTheOrderTheDropsHappened() throws Exception {
        exec("CREATE TABLE zzx2_m (a int, b int, c int, d int, e int)");
        exec("ALTER TABLE zzx2_m DROP COLUMN d");
        exec("ALTER TABLE zzx2_m DROP COLUMN b");
        exec("ALTER TABLE zzx2_m ADD COLUMN f int");

        assertEquals("a:1,........pg.dropped.2........:2,c:3,"
                        + "........pg.dropped.4........:4,e:5,f:6",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_m'::regclass AND attnum > 0"));
        assertEquals(6, num("SELECT relnatts FROM pg_class WHERE relname = 'zzx2_m'"));
        assertEquals("a:1,c:3,e:5,f:6",
                scalar("SELECT string_agg(column_name||':'||ordinal_position, ','"
                        + " ORDER BY ordinal_position) FROM information_schema.columns"
                        + " WHERE table_name = 'zzx2_m'"));

        exec("DROP TABLE zzx2_m");

        // The name comes back; the number does not.
        exec("CREATE TABLE zzx2_n (a int, b int, c int, d int)");
        exec("ALTER TABLE zzx2_n DROP COLUMN b, DROP COLUMN c");
        exec("ALTER TABLE zzx2_n ADD COLUMN e int");
        exec("ALTER TABLE zzx2_n ADD COLUMN b int");
        assertEquals("a:1,d:4,e:5,b:6",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_n'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        exec("DROP TABLE zzx2_n");
    }

    @Test
    void aColumnAddedAndRolledBackGivesItsNumberBack() throws Exception {
        exec("CREATE TABLE zzx2_rb (a int, b int, c int)");
        exec("BEGIN");
        exec("ALTER TABLE zzx2_rb ADD COLUMN d int");
        exec("ROLLBACK");
        exec("ALTER TABLE zzx2_rb ADD COLUMN e int");

        // e takes 4, not 5: the undo path must not leave a tombstone behind.
        assertEquals("a:1,b:2,c:3,e:4",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_rb'::regclass AND attnum > 0"));

        // And a drop that rolled back leaves nothing behind either.
        exec("BEGIN");
        exec("ALTER TABLE zzx2_rb DROP COLUMN b");
        exec("ROLLBACK");
        assertEquals("a:1,b:2,c:3,e:4",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_rb'::regclass AND attnum > 0"));
        assertEquals(4, num("SELECT relnatts FROM pg_class WHERE relname = 'zzx2_rb'"));

        // The same through a savepoint.
        exec("BEGIN");
        exec("SAVEPOINT zzx2_sp");
        exec("ALTER TABLE zzx2_rb DROP COLUMN b");
        exec("ROLLBACK TO SAVEPOINT zzx2_sp");
        exec("COMMIT");
        assertEquals("a:1,b:2,c:3,e:4",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_rb'::regclass AND attnum > 0"));

        exec("DROP TABLE zzx2_rb");
    }

    @Test
    void aChildAndAPartitionKeepTheHoleTheirParentsDropMade() throws Exception {
        exec("CREATE TABLE zzx2_pa (p int, q int, r int)");
        exec("CREATE TABLE zzx2_ch (s int) INHERITS (zzx2_pa)");
        exec("ALTER TABLE zzx2_pa DROP COLUMN q");

        // The tombstone keeps the attislocal and attinhcount the column had.
        assertEquals("p/1/f/f/1"
                        + ";........pg.dropped.2......../2/t/f/1"
                        + ";r/3/f/f/1"
                        + ";s/4/f/t/0",
                rowsOf("SELECT attname, attnum, attisdropped, attislocal, attinhcount"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_ch'::regclass AND attnum > 0"
                        + " ORDER BY attnum"));
        assertEquals(4, num("SELECT relnatts FROM pg_class WHERE relname = 'zzx2_ch'"));

        exec("DROP TABLE zzx2_ch");
        exec("DROP TABLE zzx2_pa");

        exec("CREATE TABLE zzx2_pt2 (k int, j int, v int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE zzx2_pt21 PARTITION OF zzx2_pt2 FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzx2_pt2 DROP COLUMN j");

        assertEquals("k/1/f/f/1"
                        + ";........pg.dropped.2......../2/t/f/1"
                        + ";v/3/f/f/1",
                rowsOf("SELECT attname, attnum, attisdropped, attislocal, attinhcount"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_pt21'::regclass AND attnum > 0"
                        + " ORDER BY attnum"));
        // The partition key is an attribute number too.
        assertEquals("1", scalar("SELECT partattrs::text FROM pg_partitioned_table"
                + " WHERE partrelid = 'zzx2_pt2'::regclass"));

        exec("DROP TABLE zzx2_pt2");
    }

    @Test
    void aConstraintAndAnIndexWrittenAfterTheDropTakeTheNumbersAsTheyAre() throws Exception {
        exec("CREATE TABLE zzx2_u (a int, b varchar(5), c int, d int, e int)");
        exec("ALTER TABLE zzx2_u DROP COLUMN b");
        exec("ALTER TABLE zzx2_u ADD CONSTRAINT zzx2_u_uc UNIQUE (d, c)");
        exec("CREATE INDEX zzx2_u_ix ON zzx2_u (e) INCLUDE (d)");

        assertEquals("zzx2_u_uc/4 3",
                rowsOf("SELECT conname, array_to_string(conkey, ' ') FROM pg_constraint"
                        + " WHERE conrelid = 'zzx2_u'::regclass ORDER BY conname"));
        // The INCLUDE column's entry in indkey is an attribute number too.
        assertEquals("zzx2_u_ix/5 4/1/2;zzx2_u_uc/4 3/2/2",
                rowsOf("SELECT c.relname, i.indkey::text, i.indnkeyatts, i.indnatts"
                        + " FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
                        + " WHERE i.indrelid = 'zzx2_u'::regclass ORDER BY 1"));

        exec("ALTER TABLE zzx2_u ADD COLUMN g int GENERATED ALWAYS AS (a * 2) STORED");
        assertEquals("6/(a * 2)",
                rowsOf("SELECT adnum, pg_get_expr(adbin, adrelid) FROM pg_attrdef"
                        + " WHERE adrelid = 'zzx2_u'::regclass ORDER BY adnum"));

        exec("DROP TABLE zzx2_u");
    }

    @Test
    void cascadeLeavesATombstoneForEachColumnItTakes() throws Exception {
        exec("CREATE TABLE zzx2_gc (a int, b int GENERATED ALWAYS AS (a * 2) STORED,"
                + " c int, d int)");
        exec("ALTER TABLE zzx2_gc DROP COLUMN a CASCADE");

        assertEquals("........pg.dropped.1........:1:true,........pg.dropped.2........:2:true,"
                        + "c:3:false,d:4:false",
                scalar("SELECT string_agg(attname||':'||attnum||':'||attisdropped::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_gc'::regclass AND attnum > 0"));
        assertEquals(4, num("SELECT relnatts FROM pg_class WHERE relname = 'zzx2_gc'"));

        exec("ALTER TABLE zzx2_gc ADD COLUMN e int");
        assertEquals("c:3,d:4,e:5",
                scalar("SELECT string_agg(attname||':'||attnum, ',' ORDER BY attnum)"
                        + " FROM pg_attribute WHERE attrelid = 'zzx2_gc'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        exec("DROP TABLE zzx2_gc");
    }

    @Test
    void jdbcMetadataReadsThroughTheHoleTheDropLeft() throws Exception {
        exec("CREATE TABLE zzx2_md (a int PRIMARY KEY, b text, d int, f int)");
        exec("CREATE INDEX zzx2_mdi ON zzx2_md (d, f)");
        exec("ALTER TABLE zzx2_md DROP COLUMN b");

        java.sql.DatabaseMetaData md = conn.getMetaData();
        List<String> cols = new ArrayList<>();
        try (ResultSet rs = md.getColumns(null, "public", "zzx2_md", null)) {
            while (rs.next()) {
                cols.add(rs.getString("COLUMN_NAME") + "/" + rs.getInt("ORDINAL_POSITION"));
            }
        }
        // The driver renumbers the live columns for itself, so the hole is not visible here.
        assertEquals("a/1,d/2,f/3", String.join(",", cols));

        List<String> keys = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(null, "public", "zzx2_md")) {
            while (rs.next()) {
                keys.add(rs.getString("COLUMN_NAME") + "/" + rs.getShort("KEY_SEQ"));
            }
        }
        assertEquals("a/1", String.join(",", keys));

        List<String> idx = new ArrayList<>();
        try (ResultSet rs = md.getIndexInfo(null, "public", "zzx2_md", false, false)) {
            while (rs.next()) {
                idx.add(rs.getString("INDEX_NAME") + "/" + rs.getString("COLUMN_NAME")
                        + "/" + rs.getShort("ORDINAL_POSITION"));
            }
        }
        // Each index key is read back as the column it was declared over.
        assertEquals("zzx2_md_pkey/a/1,zzx2_mdi/d/1,zzx2_mdi/f/2", String.join(",", idx));

        exec("DROP TABLE zzx2_md");
    }

    // ------------------------------------------------------------ The type a column is declared with is named the way the search path would have you write it

    @Test
    void aDomainIsNamedTheWayTheSearchPathWouldHaveTheReaderWriteIt() throws Exception {
        exec("CREATE SCHEMA zzx2_ds");
        exec("CREATE DOMAIN zzx2_ds.dom AS int");
        exec("CREATE DOMAIN zzx2_pub AS int");

        // A schema the path does not reach is written out.
        assertEquals("42804",
                stateOf("CREATE TABLE zzx2_dm1 (b zzx2_ds.dom DEFAULT 'abc'::text)"));
        assertEquals("column \"b\" is of type zzx2_ds.dom"
                        + " but default expression is of type text",
                messageOf("CREATE TABLE zzx2_dm1 (b zzx2_ds.dom DEFAULT 'abc'::text)"));
        assertEquals("You will need to rewrite or cast the expression.",
                hintOf("CREATE TABLE zzx2_dm1 (b zzx2_ds.dom DEFAULT 'abc'::text)"));

        // A schema it does reach is not, however the writer wrote it.
        assertEquals("column \"b\" is of type zzx2_pub but default expression is of type text",
                messageOf("CREATE TABLE zzx2_dm5 (b public.zzx2_pub DEFAULT 'abc'::text)"));
        assertEquals("column \"b\" is of type zzx2_pub but default expression is of type text",
                messageOf("CREATE TABLE zzx2_dm6 (b zzx2_pub DEFAULT 'abc'::text)"));
        assertEquals("42704", stateOf("CREATE TABLE zzx2_dm4 (b dom DEFAULT 'abc'::text)"));
        assertEquals("type \"dom\" does not exist",
                messageOf("CREATE TABLE zzx2_dm4 (b dom DEFAULT 'abc'::text)"));

        exec("SET search_path = zzx2_ds, public");
        assertEquals("column \"b\" is of type dom but default expression is of type text",
                messageOf("CREATE TABLE public.zzx2_dq1 (b dom DEFAULT 'abc'::text)"));
        // Writing the qualifier does not make PostgreSQL print it.
        assertEquals("column \"b\" is of type dom but default expression is of type text",
                messageOf("CREATE TABLE public.zzx2_dq2 (b zzx2_ds.dom DEFAULT 'abc'::text)"));
        assertEquals("column \"b\" is of type zzx2_pub but default expression is of type text",
                messageOf("CREATE TABLE public.zzx2_dq3 (b public.zzx2_pub DEFAULT 'abc'::text)"));

        exec("SET search_path = zzx2_ds");
        // A path that leaves public out leaves public's own types qualified.
        assertEquals("column \"b\" is of type public.zzx2_pub"
                        + " but default expression is of type text",
                messageOf("CREATE TABLE public.zzx2_dq5 (b public.zzx2_pub DEFAULT 'abc'::text)"));
        assertEquals("column \"b\" is of type dom but default expression is of type text",
                messageOf("CREATE TABLE public.zzx2_dq6 (b zzx2_ds.dom DEFAULT 'abc'::text)"));

        exec("RESET search_path");

        // A qualifier on a built-in is dropped, and the catalogue's own name printed.
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzx2_dm7 (b pg_catalog.int4 DEFAULT 'abc'::text)"));

        // A bare string literal is read with the base type's input function, so the complaint
        // names the base type and not the domain.
        assertEquals("22P02", stateOf("CREATE TABLE zzx2_dm3 (b zzx2_ds.dom DEFAULT 'abc')"));
        assertEquals("invalid input syntax for type integer: \"abc\"",
                messageOf("CREATE TABLE zzx2_dm3 (b zzx2_ds.dom DEFAULT 'abc')"));

        // The generation and the ADD COLUMN paths name it the same way.
        assertEquals("column \"b\" is of type zzx2_ds.dom"
                        + " but default expression is of type text",
                messageOf("CREATE TABLE zzx2_dm8"
                        + " (b zzx2_ds.dom GENERATED ALWAYS AS ('x'::text) STORED)"));
        exec("CREATE TABLE zzx2_dad (k int)");
        assertEquals("column \"c\" is of type zzx2_ds.dom"
                        + " but default expression is of type text",
                messageOf("ALTER TABLE zzx2_dad ADD COLUMN c zzx2_ds.dom DEFAULT 'abc'::text"));
        assertEquals("k:integer",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_dad'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        // What the domain does accept keeps the domain as the column's type.
        exec("CREATE TABLE zzx2_dm2 (b zzx2_ds.dom DEFAULT 1)");
        assertEquals("b:zzx2_ds.dom",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_dm2'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        // Nothing a refusal named was built.
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname IN"
                + " ('zzx2_dm1','zzx2_dm5','zzx2_dm6','zzx2_dm4','zzx2_dq1','zzx2_dq2','zzx2_dq3',"
                + "'zzx2_dq5','zzx2_dq6','zzx2_dm7','zzx2_dm3','zzx2_dm8')"));

        exec("DROP TABLE zzx2_dm2");
        exec("DROP TABLE zzx2_dad");
        exec("DROP DOMAIN zzx2_ds.dom");
        exec("DROP DOMAIN zzx2_pub");
        exec("DROP SCHEMA zzx2_ds");
    }

    @Test
    void addColumnJudgesItsGenerationExpressionAgainstTheColumnsType() throws Exception {
        exec("CREATE SCHEMA zzx2_ds");
        exec("CREATE DOMAIN zzx2_ds.dom AS int");
        exec("CREATE TABLE zzx2_ge (k int, s text)");

        // The same words a DEFAULT of the wrong type gets, and the same hint.
        assertEquals("42804", stateOf("ALTER TABLE zzx2_ge ADD COLUMN c1 int"
                + " GENERATED ALWAYS AS ('x'::text) STORED"));
        assertEquals("column \"c1\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN c1 int"
                        + " GENERATED ALWAYS AS ('x'::text) STORED"));
        assertEquals("You will need to rewrite or cast the expression.",
                hintOf("ALTER TABLE zzx2_ge ADD COLUMN c1 int"
                        + " GENERATED ALWAYS AS ('x'::text) STORED"));
        assertEquals("column \"c2\" is of type boolean but default expression is of type integer",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN c2 bool"
                        + " GENERATED ALWAYS AS (1) STORED"));
        assertEquals("column \"c4\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN c4 int"
                        + " GENERATED ALWAYS AS (s) STORED"));
        assertEquals("column \"cd\" is of type integer but default expression is of type boolean",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN cd int"
                        + " GENERATED ALWAYS AS (1=1) STORED"));
        // A virtual generated column is judged by the same rule.
        assertEquals("column \"cg\" is of type integer but default expression is of type text",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN cg int"
                        + " GENERATED ALWAYS AS ('x'::text) VIRTUAL"));
        // And a bare string literal is read with the column type's input function.
        assertEquals("22P02", stateOf("ALTER TABLE zzx2_ge ADD COLUMN c5 int"
                + " GENERATED ALWAYS AS ('abc') STORED"));
        assertEquals("invalid input syntax for type integer: \"abc\"",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN c5 int"
                        + " GENERATED ALWAYS AS ('abc') STORED"));
        // Over a domain column, named the way the search path names it.
        assertEquals("column \"cc\" is of type zzx2_ds.dom"
                        + " but default expression is of type text",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN cc zzx2_ds.dom"
                        + " GENERATED ALWAYS AS ('x'::text) STORED"));

        // Each of these is a different fault in the same statement, and the one PostgreSQL
        // reaches first is the one it reports -- the type check comes last of them.
        assertEquals("42P17", stateOf("ALTER TABLE zzx2_ge ADD COLUMN c8 int"
                + " GENERATED ALWAYS AS (now()) STORED"));
        assertEquals("generation expression is not immutable",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN c8 int"
                        + " GENERATED ALWAYS AS (now()) STORED"));
        assertEquals("42703", stateOf("ALTER TABLE zzx2_ge ADD COLUMN c9 int"
                + " GENERATED ALWAYS AS (nosuchcol) STORED"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN c9 int"
                        + " GENERATED ALWAYS AS (nosuchcol) STORED"));
        assertEquals("0A000", stateOf("ALTER TABLE zzx2_ge ADD COLUMN cb int"
                + " GENERATED ALWAYS AS ((SELECT 1)) STORED"));
        assertEquals("cannot use subquery in column generation expression",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN cb int"
                        + " GENERATED ALWAYS AS ((SELECT 1)) STORED"));
        assertEquals("column name \"ctid\" conflicts with a system column name",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN ctid int"
                        + " GENERATED ALWAYS AS ('x'::text) STORED"));
        assertEquals("type \"nosuchtype\" does not exist",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN ce nosuchtype"
                        + " GENERATED ALWAYS AS ('x'::text) STORED"));
        assertEquals("column \"k\" of relation \"zzx2_ge\" already exists",
                messageOf("ALTER TABLE zzx2_ge ADD COLUMN k int"
                        + " GENERATED ALWAYS AS ('x'::text) STORED"));

        // What the statement goes on accepting, and what those columns compute.
        exec("ALTER TABLE zzx2_ge ADD COLUMN c3 int GENERATED ALWAYS AS (k) STORED");
        exec("ALTER TABLE zzx2_ge ADD COLUMN c6 text GENERATED ALWAYS AS (k) STORED");
        exec("ALTER TABLE zzx2_ge ADD COLUMN c7 int GENERATED ALWAYS AS (k+1) STORED");
        assertEquals("k:integer,s:text,c3:integer,c6:text,c7:integer",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_ge'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));
        exec("INSERT INTO zzx2_ge (k, s) VALUES (5, 'x')");
        assertEquals("5/5/6", rowsOf("SELECT c3, c6, c7 FROM zzx2_ge"));

        exec("DROP TABLE zzx2_ge");
        exec("DROP DOMAIN zzx2_ds.dom");
        exec("DROP SCHEMA zzx2_ds");
    }

    @Test
    void alterColumnTypeSettlesWhichColumnBeforeItSettlesWhatType() throws Exception {
        exec("CREATE TABLE zzx2_rt2 (x int, y text)");

        // Every unresolvable type name is reached after the column, not before it.
        for (String type : new String[]{"serial", "bigserial", "smallserial", "nosuchtype",
                "text"}) {
            assertEquals("42703",
                    stateOf("ALTER TABLE zzx2_rt2 ALTER COLUMN nosuch TYPE " + type));
            assertEquals("column \"nosuch\" of relation \"zzx2_rt2\" does not exist",
                    messageOf("ALTER TABLE zzx2_rt2 ALTER COLUMN nosuch TYPE " + type));
        }
        assertEquals("42703",
                stateOf("ALTER TABLE zzx2_rt2 ALTER COLUMN nosuch SET DATA TYPE serial"));
        assertEquals("42703",
                stateOf("ALTER TABLE zzx2_rt2 ALTER COLUMN nosuch TYPE serial USING x"));

        // Where the column is there, the type is what is at fault.
        assertEquals("42704", stateOf("ALTER TABLE zzx2_rt2 ALTER COLUMN x TYPE serial"));
        assertEquals("type \"serial\" does not exist",
                messageOf("ALTER TABLE zzx2_rt2 ALTER COLUMN x TYPE serial"));
        assertEquals("type \"bigserial\" does not exist",
                messageOf("ALTER TABLE zzx2_rt2 ALTER COLUMN x TYPE bigserial"));
        assertEquals("type \"nosuchtype\" does not exist",
                messageOf("ALTER TABLE zzx2_rt2 ALTER COLUMN x TYPE nosuchtype"));

        // And the relation is settled ahead of both.
        assertEquals("42P01",
                stateOf("ALTER TABLE zzx2_nosuchtable ALTER COLUMN nosuch TYPE serial"));
        assertEquals("relation \"zzx2_nosuchtable\" does not exist",
                messageOf("ALTER TABLE zzx2_nosuchtable ALTER COLUMN nosuch TYPE serial"));

        assertEquals("x:integer,y:text",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_rt2'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        exec("DROP TABLE zzx2_rt2");
    }

    // ------------------------------------------------------------ A COLLATE clause is judged against the type the column really has

    @Test
    void anEnumACompositeAndARangeCarryNoCollation() throws Exception {
        exec("CREATE TYPE zzx2_ce AS ENUM ('a','b')");
        exec("CREATE TYPE zzx2_cc AS (x int, y text)");
        exec("CREATE TYPE zzx2_cg AS RANGE (subtype = int4)");
        exec("CREATE DOMAIN zzx2_cde AS zzx2_ce");
        exec("CREATE DOMAIN zzx2_cdt AS text");
        exec("CREATE TABLE zzx2_cr (k int, s text, e zzx2_ce)");

        assertEquals("42804", stateOf("CREATE TABLE zzx2_cr2 (a zzx2_ce COLLATE \"C\")"));
        assertEquals("collations are not supported by type zzx2_ce",
                messageOf("CREATE TABLE zzx2_cr2 (a zzx2_ce COLLATE \"C\")"));
        assertEquals("collations are not supported by type zzx2_ce",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a1 zzx2_ce COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_ce",
                messageOf("ALTER TABLE zzx2_cr ALTER COLUMN e TYPE zzx2_ce COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_ce[]",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a2 zzx2_ce[] COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_cc",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a3 zzx2_cc COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_cc[]",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a4 zzx2_cc[] COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_cg",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a5 zzx2_cg COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_cg",
                messageOf("CREATE TABLE zzx2_cr3 (a zzx2_cg COLLATE \"C\")"));
        assertEquals("collations are not supported by type zzx2_cg",
                messageOf("ALTER TABLE zzx2_cr ALTER COLUMN k TYPE zzx2_cg COLLATE \"C\""));

        // A domain is named by its own name, whatever it was built over.
        assertEquals("collations are not supported by type zzx2_cde",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a6 zzx2_cde COLLATE \"C\""));
        // One built over a type that does carry a collation takes the clause.
        exec("ALTER TABLE zzx2_cr ADD COLUMN a8 zzx2_cdt COLLATE \"C\"");

        // The clause is still judged last of the three questions PostgreSQL asks about it.
        assertEquals("42704",
                stateOf("ALTER TABLE zzx2_cr ADD COLUMN a9 zzx2_ce COLLATE nosuchcoll"));
        assertEquals("collation \"nosuchcoll\" for encoding \"UTF8\" does not exist",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN a9 zzx2_ce COLLATE nosuchcoll"));
        assertEquals("type \"nosuchtype\" does not exist",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN b2 nosuchtype COLLATE nosuchcoll"));
        assertEquals("column \"nosuch\" of relation \"zzx2_cr\" does not exist",
                messageOf("ALTER TABLE zzx2_cr ALTER COLUMN nosuch TYPE int COLLATE nosuchcoll"));
        // A width the type cannot have is settled with the type, ahead of the collation name.
        assertEquals("22023", stateOf("ALTER TABLE zzx2_cr ALTER COLUMN s"
                + " TYPE varchar(20000000) COLLATE nosuchcoll"));
        assertEquals("length for type varchar cannot exceed 10485760",
                messageOf("ALTER TABLE zzx2_cr ALTER COLUMN s"
                        + " TYPE varchar(20000000) COLLATE nosuchcoll"));
        exec("CREATE TYPE zzx2_csh");
        assertEquals("type \"zzx2_csh\" is only a shell",
                messageOf("ALTER TABLE zzx2_cr ALTER COLUMN k TYPE zzx2_csh COLLATE \"C\""));

        // The types that never carried one are unmoved.
        assertEquals("collations are not supported by type bigint",
                messageOf("ALTER TABLE zzx2_cr ALTER COLUMN k TYPE bigint COLLATE \"C\""));
        assertEquals("collations are not supported by type integer[]",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN n2 int[] COLLATE \"C\""));
        assertEquals("collations are not supported by type int4range",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN n4 int4range COLLATE \"C\""));
        assertEquals("collations are not supported by type int4multirange",
                messageOf("ALTER TABLE zzx2_cr ADD COLUMN n5 int4multirange COLLATE \"C\""));
        assertEquals("collations are not supported by type integer",
                messageOf("CREATE TABLE zzx2_cr5 (i int COLLATE \"C\")"));
        assertEquals("collations are not supported by type date",
                messageOf("CREATE TABLE zzx2_cr6 (d date COLLATE \"C\")"));

        // And the ones that do carry a collation still take the clause.
        exec("CREATE TABLE zzx2_cr7 (t text COLLATE \"C\", v varchar(4) COLLATE \"C\")");
        exec("ALTER TABLE zzx2_cr ALTER COLUMN s TYPE varchar(20) COLLATE \"C\"");
        exec("ALTER TABLE zzx2_cr ALTER COLUMN s TYPE text COLLATE \"C\"");

        assertEquals("k:integer,s:text,e:zzx2_ce,a8:zzx2_cdt",
                scalar("SELECT string_agg(attname||':'||atttypid::regtype::text, ','"
                        + " ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzx2_cr'::regclass AND attnum > 0"
                        + " AND NOT attisdropped"));

        exec("DROP TABLE zzx2_cr7");
        exec("DROP TABLE zzx2_cr");
        exec("DROP TYPE zzx2_csh");
        exec("DROP DOMAIN zzx2_cde");
        exec("DROP DOMAIN zzx2_cdt");
        exec("DROP TYPE zzx2_cg");
        exec("DROP TYPE zzx2_cc");
        exec("DROP TYPE zzx2_ce");
    }

    @Test
    void aTypeOffTheSearchPathIsNamedWithItsSchemaInTheCollationRefusal() throws Exception {
        exec("CREATE SCHEMA zzx2_csch");
        exec("CREATE TYPE zzx2_csch.zzx2_ce2 AS ENUM ('p','q')");
        exec("CREATE TYPE zzx2_ce AS ENUM ('a','b')");
        exec("CREATE TABLE zzx2_cq (k int)");

        assertEquals("collations are not supported by type zzx2_csch.zzx2_ce2",
                messageOf("ALTER TABLE zzx2_cq ADD COLUMN c1 zzx2_csch.zzx2_ce2 COLLATE \"C\""));

        exec("SET search_path = zzx2_csch, public");
        assertEquals("collations are not supported by type zzx2_ce2",
                messageOf("ALTER TABLE public.zzx2_cq ADD COLUMN c2 zzx2_ce2 COLLATE \"C\""));
        // Writing the qualifier does not make PostgreSQL print it.
        assertEquals("collations are not supported by type zzx2_ce",
                messageOf("ALTER TABLE public.zzx2_cq ADD COLUMN c3 public.zzx2_ce COLLATE \"C\""));

        exec("SET search_path = zzx2_csch");
        // A path that leaves public out leaves public's own types qualified.
        assertEquals("collations are not supported by type public.zzx2_ce",
                messageOf("ALTER TABLE public.zzx2_cq ADD COLUMN c4 public.zzx2_ce COLLATE \"C\""));
        assertEquals("collations are not supported by type zzx2_ce2",
                messageOf("ALTER TABLE public.zzx2_cq ADD COLUMN c5 zzx2_ce2 COLLATE \"C\""));

        exec("RESET search_path");
        exec("DROP TABLE zzx2_cq");
        exec("DROP TYPE zzx2_csch.zzx2_ce2");
        exec("DROP TYPE zzx2_ce");
        exec("DROP SCHEMA zzx2_csch");
    }

    // ------------------------------------------------------------ serial2, serial4 and serial8 are the shorthands their words stand for

    @Test
    void aNumberedSerialIsTheIntegerColumnItsWordStandsFor() throws Exception {
        exec("CREATE TABLE zzx2_se (a serial2, b serial4, c serial8)");

        assertEquals("a/smallint/nextval('zzx2_se_a_seq'::regclass)/NO,"
                        + "b/integer/nextval('zzx2_se_b_seq'::regclass)/NO,"
                        + "c/bigint/nextval('zzx2_se_c_seq'::regclass)/NO",
                scalar("SELECT string_agg(column_name||'/'||data_type||'/'||column_default||'/'"
                        + "||is_nullable, ',' ORDER BY ordinal_position)"
                        + " FROM information_schema.columns WHERE table_name = 'zzx2_se'"));
        assertEquals("zzx2_se/r,zzx2_se_a_seq/S,zzx2_se_b_seq/S,zzx2_se_c_seq/S",
                scalar("SELECT string_agg(relname||'/'||relkind::text, ',' ORDER BY relname)"
                        + " FROM pg_class WHERE relname LIKE 'zzx2\\_se%'"));
        // Each sequence is bounded by the integer type the shorthand stands for.
        assertEquals("zzx2_se_a_seq/smallint/32767,zzx2_se_b_seq/integer/2147483647,"
                        + "zzx2_se_c_seq/bigint/9223372036854775807",
                scalar("SELECT string_agg(c.relname||'/'||s.seqtypid::regtype::text||'/'"
                        + "||s.seqmax::text, ',' ORDER BY c.relname) FROM pg_sequence s"
                        + " JOIN pg_class c ON c.oid = s.seqrelid"
                        + " WHERE c.relname LIKE 'zzx2\\_se%'"));

        exec("INSERT INTO zzx2_se DEFAULT VALUES");
        assertEquals("1/1/1", rowsOf("SELECT a, b, c FROM zzx2_se"));

        exec("DROP TABLE zzx2_se");
    }

    @Test
    void theSerialShorthandMeansSomethingOnlyWhereAColumnsTypeIsWritten() throws Exception {
        exec("CREATE TABLE zzx2_sf (k int)");
        exec("ALTER TABLE zzx2_sf ADD COLUMN q serial2");
        exec("ALTER TABLE zzx2_sf ADD COLUMN r serial4");
        exec("ALTER TABLE zzx2_sf ADD COLUMN v serial8");

        assertEquals("k/integer/null,q/smallint/nextval('zzx2_sf_q_seq'::regclass),"
                        + "r/integer/nextval('zzx2_sf_r_seq'::regclass),"
                        + "v/bigint/nextval('zzx2_sf_v_seq'::regclass)",
                scalar("SELECT string_agg(column_name||'/'||data_type||'/'"
                        + "||coalesce(column_default,'null'), ',' ORDER BY ordinal_position)"
                        + " FROM information_schema.columns WHERE table_name = 'zzx2_sf'"));

        // Everywhere else the word names no type at all.
        assertEquals("42704", stateOf("ALTER TABLE zzx2_sf ALTER COLUMN k TYPE serial2"));
        assertEquals("type \"serial2\" does not exist",
                messageOf("ALTER TABLE zzx2_sf ALTER COLUMN k TYPE serial2"));
        // And the column is settled before the type it is being retyped to.
        assertEquals("42703", stateOf("ALTER TABLE zzx2_sf ALTER COLUMN nosuch TYPE serial2"));
        assertEquals("column \"nosuch\" of relation \"zzx2_sf\" does not exist",
                messageOf("ALTER TABLE zzx2_sf ALTER COLUMN nosuch TYPE serial2"));
        assertEquals("type \"serial2\" does not exist", messageOf("SELECT 1::serial2"));
        assertEquals("type \"serial4\" does not exist", messageOf("SELECT CAST(1 AS serial4)"));
        assertEquals("0A000", stateOf("CREATE TABLE zzx2_sx (a serial2[])"));
        assertEquals("array of serial is not implemented",
                messageOf("CREATE TABLE zzx2_sx (a serial2[])"));
        assertEquals("42601", stateOf("CREATE TABLE zzx2_sy (a serial2(5))"));
        assertEquals("type modifier is not allowed for type \"smallint\"",
                messageOf("CREATE TABLE zzx2_sy (a serial2(5))"));
        // A name written under a schema is a real type name, and pg_catalog holds no serial2.
        assertEquals("type \"pg_catalog.serial2\" does not exist",
                messageOf("CREATE TABLE zzx2_sz (a pg_catalog.serial2)"));
        // The complaint names the integer type the column would have had.
        assertEquals("collations are not supported by type smallint",
                messageOf("CREATE TABLE zzx2_sw (a serial2 COLLATE \"C\")"));

        exec("DROP TABLE zzx2_sf");
    }

    // ------------------------------------------------------------ A stored expression's names are resolved against the one relation it belongs to

    @Test
    void aUsingExpressionIsResolvedBeforeTheColumnBeingRetyped() throws Exception {
        exec("CREATE TABLE zzx2_ua (a int, s text)");

        // A name the expression cannot resolve is reported with no relation clause, because it
        // was never looked up against one.
        assertEquals("42703", stateOf("ALTER TABLE zzx2_ua ALTER COLUMN nosuch"
                + " TYPE serial USING nosuch::int"));
        assertEquals("column \"nosuch\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN nosuch TYPE serial USING nosuch::int"));
        assertEquals("column \"other\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN nosuch TYPE int USING other::int"));
        assertEquals("column \"nosuch\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING nosuch::int"));

        // With nothing wrong in the expression the column being retyped is reached, and that
        // complaint does name the relation.
        assertEquals("column \"nosuch\" of relation \"zzx2_ua\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN nosuch TYPE int USING a::int"));
        assertEquals("column \"nosuch\" of relation \"zzx2_ua\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN nosuch TYPE int USING 1"));
        assertEquals("type \"serial\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE serial USING a::int"));
        assertEquals("relation \"zzx2_nosuchtable\" does not exist",
                messageOf("ALTER TABLE zzx2_nosuchtable ALTER COLUMN nosuch"
                        + " TYPE int USING nosuch::int"));

        // A qualifier names the relation the expression belongs to, or nothing at all.
        exec("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING zzx2_ua.a::int");
        exec("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING public.zzx2_ua.a::int");
        assertEquals("42703",
                stateOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING zzx2_ua.nosuch::int"));
        // Named as it was written, and without quotes.
        assertEquals("column zzx2_ua.nosuch does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING zzx2_ua.nosuch::int"));
        assertEquals("42P01",
                stateOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING zzz.a::int"));
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING zzz.a::int"));
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING (zzz.a + nosuch)"));
        // Names are resolved left to right.
        assertEquals("column \"nosuch\" does not exist",
                messageOf("ALTER TABLE zzx2_ua ALTER COLUMN a TYPE int USING (nosuch + zzz.a)"));

        exec("DROP TABLE zzx2_ua");
    }

    @Test
    void aQualifierMayNameEitherPartOfARelationInAnotherSchema() throws Exception {
        exec("CREATE SCHEMA zzx2_sq");
        exec("CREATE TABLE zzx2_sq.zzx2_wt (a int, s text)");

        exec("ALTER TABLE zzx2_sq.zzx2_wt ALTER COLUMN a TYPE int USING zzx2_wt.a::int");
        exec("ALTER TABLE zzx2_sq.zzx2_wt ALTER COLUMN a TYPE int"
                + " USING zzx2_sq.zzx2_wt.a::int");

        assertEquals("42P01", stateOf("ALTER TABLE zzx2_sq.zzx2_wt ALTER COLUMN a TYPE int"
                + " USING nosuchschema.zzx2_wt.a::int"));
        assertEquals("invalid reference to FROM-clause entry for table \"zzx2_wt\"",
                messageOf("ALTER TABLE zzx2_sq.zzx2_wt ALTER COLUMN a TYPE int"
                        + " USING nosuchschema.zzx2_wt.a::int"));
        assertEquals("There is an entry for table \"zzx2_wt\", but it cannot be referenced"
                        + " from this part of the query.",
                detailOf("ALTER TABLE zzx2_sq.zzx2_wt ALTER COLUMN a TYPE int"
                        + " USING nosuchschema.zzx2_wt.a::int"));

        // The match is on the name as written: an unquoted qualifier is folded to lower case and
        // a quoted one is not.
        assertEquals("missing FROM-clause entry for table \"ZZX2_WT\"",
                messageOf("ALTER TABLE zzx2_sq.zzx2_wt ADD CONSTRAINT zzx2_wk"
                        + " CHECK (\"ZZX2_WT\".a > 0)"));
        exec("ALTER TABLE zzx2_sq.zzx2_wt ADD CONSTRAINT zzx2_wk CHECK (\"zzx2_wt\".a > 0)");

        exec("DROP TABLE zzx2_sq.zzx2_wt");
        exec("DROP SCHEMA zzx2_sq");
    }

    @Test
    void aCheckIsJudgedByTheSameRuleAndNotValidDefersTheRowsNotTheNames() throws Exception {
        exec("CREATE TABLE zzx2_kk (a int, s text)");
        exec("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc1 CHECK (zzx2_kk.a > 0)");

        // The one that resolved is enforced.
        assertEquals("23514", stateOf("INSERT INTO zzx2_kk VALUES (-1)"));
        assertEquals("new row for relation \"zzx2_kk\" violates check constraint \"zzx2_kc1\"",
                messageOf("INSERT INTO zzx2_kk VALUES (-1)"));

        assertEquals("column zzx2_kk.nosuch does not exist",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc2"
                        + " CHECK (zzx2_kk.nosuch > 0)"));
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc3 CHECK (zzz.a > 0)"));
        // NOT VALID and NOT ENFORCED defer the rows, not the names.
        assertEquals("column \"nosuch\" does not exist",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc4"
                        + " CHECK (nosuch > 0) NOT VALID"));
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc5"
                        + " CHECK (zzz.a > 0) NOT VALID"));
        assertEquals("column \"nosuch\" does not exist",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc6"
                        + " CHECK (nosuch > 0) NOT ENFORCED"));
        // The names are settled before the predicate is asked to be a boolean.
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc7 CHECK (zzz.a)"));
        assertEquals("argument of CHECK must be type boolean, not type integer",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc8 CHECK (a)"));
        // s is a column of the relation, and a qualifier is still read as a relation name.
        assertEquals("missing FROM-clause entry for table \"s\"",
                messageOf("ALTER TABLE zzx2_kk ADD CONSTRAINT zzx2_kc9 CHECK (s.x > 0)"));

        assertEquals("zzx2_kc1",
                scalar("SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid = 'zzx2_kk'::regclass"));
        exec("DROP TABLE zzx2_kk");

        // The same rule inside CREATE TABLE.
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("CREATE TABLE zzx2_kt2 (a int, CHECK (zzz.a > 0))"));
        assertEquals("column zzx2_kt3.nosuch does not exist",
                messageOf("CREATE TABLE zzx2_kt3 (a int, CHECK (zzx2_kt3.nosuch > 0))"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname IN ('zzx2_kt2','zzx2_kt3')"));
    }

    @Test
    void aCompositeFieldIsReachedByWritingTheColumnInParentheses() throws Exception {
        exec("CREATE TYPE zzx2_cty AS (x int, y text)");
        exec("CREATE TABLE zzx2_cpt (a int, c zzx2_cty)");

        exec("ALTER TABLE zzx2_cpt ADD CONSTRAINT zzx2_m1 CHECK ((c).x > 0)");
        exec("ALTER TABLE zzx2_cpt ALTER COLUMN a TYPE int USING (c).x");

        // Written without them, c is read as a relation the statement never named.
        assertEquals("42P01",
                stateOf("ALTER TABLE zzx2_cpt ADD CONSTRAINT zzx2_m2 CHECK (c.x > 0)"));
        assertEquals("missing FROM-clause entry for table \"c\"",
                messageOf("ALTER TABLE zzx2_cpt ADD CONSTRAINT zzx2_m2 CHECK (c.x > 0)"));
        assertEquals("missing FROM-clause entry for table \"c\"",
                messageOf("ALTER TABLE zzx2_cpt ALTER COLUMN a TYPE int USING c.x"));
        // A three-part name is (schema, table, column), so the middle part is the relation.
        assertEquals("missing FROM-clause entry for table \"c\"",
                messageOf("ALTER TABLE zzx2_cpt ADD CONSTRAINT zzx2_m3"
                        + " CHECK (zzx2_cpt.c.x > 0)"));

        exec("DROP TABLE zzx2_cpt");
        exec("DROP TYPE zzx2_cty");
    }

    @Test
    void aGenerationExpressionGoesByTheSameRule() throws Exception {
        exec("CREATE TABLE zzx2_gq (a int)");

        assertEquals("42P01", stateOf("ALTER TABLE zzx2_gq ADD COLUMN g1 int"
                + " GENERATED ALWAYS AS (zzz.a) STORED"));
        assertEquals("missing FROM-clause entry for table \"zzz\"",
                messageOf("ALTER TABLE zzx2_gq ADD COLUMN g1 int"
                        + " GENERATED ALWAYS AS (zzz.a) STORED"));
        assertEquals("column zzx2_gq.nosuch does not exist",
                messageOf("ALTER TABLE zzx2_gq ADD COLUMN g2 int"
                        + " GENERATED ALWAYS AS (zzx2_gq.nosuch) STORED"));

        // A qualifier that names the relation itself resolves, and the column computes.
        exec("ALTER TABLE zzx2_gq ADD COLUMN g3 int GENERATED ALWAYS AS (zzx2_gq.a) STORED");
        exec("INSERT INTO zzx2_gq (a) VALUES (5)");
        assertEquals("5/5", rowsOf("SELECT a, g3 FROM zzx2_gq"));

        exec("DROP TABLE zzx2_gq");
    }

    // ------------------------------------------------------------ What a duplicate key says: the key that is duplicated, and the key that already exists

    @Test
    void aUniqueIndexOverCollidingRowsNamesTheDuplicatedKey() throws Exception {
        exec("CREATE TABLE zzx2_a (a int, b text, c text, d int)");
        exec("INSERT INTO zzx2_a VALUES (1,'p q','x,y',NULL),(1,'p q','x,y',NULL),(2,'z','w',5)");

        // Nothing was written, so what is reported is the state the table was found holding.
        assertEquals("23505", stateOf("CREATE UNIQUE INDEX zzx2_a_u0 ON zzx2_a (a)"));
        assertEquals("could not create unique index \"zzx2_a_u0\"",
                messageOf("CREATE UNIQUE INDEX zzx2_a_u0 ON zzx2_a (a)"));
        assertEquals("Key (a)=(1) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u0 ON zzx2_a (a)"));
        assertEquals("zzx2_a_u0",
                fieldsOf("CREATE UNIQUE INDEX zzx2_a_u0 ON zzx2_a (a)").getConstraint());

        // Several columns: the values are separated by ", " and are not quoted, so a value
        // holding a comma is written into the list as it stands.
        assertEquals("Key (a, b)=(1, p q) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u1 ON zzx2_a (a, b)"));
        assertEquals("Key (b, c)=(p q, x,y) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u2 ON zzx2_a (b, c)"));

        // An expression key is deparsed, with the parentheses its own precedence does not
        // already imply; a bare call is written as it stands.
        assertEquals("Key ((a + 1), upper(b))=(2, P Q) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u3 ON zzx2_a ((a+1), upper(b))"));
        assertEquals("Key ((a::text))=(1) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u4 ON zzx2_a ((a::text))"));
        assertEquals("Key (lower(c))=(x,y) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u5 ON zzx2_a (lower(c))"));

        // INCLUDE columns, a COLLATE clause and a sort direction are no part of the key, so none
        // of them reaches the list.
        assertEquals("Key (a, b)=(1, p q) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u6 ON zzx2_a (a, b) INCLUDE (c)"));
        assertEquals("Key (b)=(p q) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u7 ON zzx2_a (b COLLATE \"C\")"));
        assertEquals("Key (a, b)=(1, p q) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u8 ON zzx2_a (a DESC, b)"));

        // A partial index judges only the rows its predicate lets through -- and both of the
        // colliding rows are among them.
        assertEquals("Key (a)=(1) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u9 ON zzx2_a (a) WHERE d IS NULL"));

        // An index nobody named reports the name it derived, in the message and in CONSTRAINT.
        assertEquals("could not create unique index \"zzx2_a_a_b_idx\"",
                messageOf("CREATE UNIQUE INDEX ON zzx2_a (a, b)"));
        assertEquals("zzx2_a_a_b_idx",
                fieldsOf("CREATE UNIQUE INDEX ON zzx2_a (a, b)").getConstraint());

        // Two rows hold d = NULL, and nulls are distinct unless the index says they are not.
        exec("CREATE UNIQUE INDEX zzx2_a_ok1 ON zzx2_a (d)");
        exec("CREATE UNIQUE INDEX zzx2_a_ok2 ON zzx2_a (a, d)");
        assertEquals("23505",
                stateOf("CREATE UNIQUE INDEX zzx2_a_u10 ON zzx2_a (a, d) NULLS NOT DISTINCT"));
        // A null is written into the value list as the word null.
        assertEquals("Key (a, d)=(1, null) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_a_u10 ON zzx2_a (a, d) NULLS NOT DISTINCT"));

        // Nothing the refused statements asked for was created.
        assertEquals("zzx2_a_ok1,zzx2_a_ok2",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzx2_a'"
                        + " ORDER BY indexname"));

        exec("DROP TABLE zzx2_a");

        // A key that needs quoting gets its quotes.
        exec("CREATE TABLE zzx2_d (\"A b\" int, x int)");
        exec("INSERT INTO zzx2_d VALUES (1,1),(1,2)");
        assertEquals("Key (\"A b\")=(1) is duplicated.",
                detailOf("CREATE UNIQUE INDEX zzx2_d_u ON zzx2_d (\"A b\")"));
        exec("DROP TABLE zzx2_d");
    }

    @Test
    void aRefusedWriteSaysTheKeyAlreadyExistsRatherThanThatItIsDuplicated() throws Exception {
        exec("CREATE TABLE zzx2_b (a int, b text, c int)");
        exec("CREATE UNIQUE INDEX zzx2_b_u1 ON zzx2_b (a)");
        exec("CREATE UNIQUE INDEX zzx2_b_u2 ON zzx2_b (lower(b))");
        exec("CREATE UNIQUE INDEX zzx2_b_u3 ON zzx2_b ((a+c))");
        exec("INSERT INTO zzx2_b VALUES (1,'x',5)");

        assertEquals("23505", stateOf("INSERT INTO zzx2_b VALUES (1,'y',9)"));
        assertEquals("duplicate key value violates unique constraint \"zzx2_b_u1\"",
                messageOf("INSERT INTO zzx2_b VALUES (1,'y',9)"));
        assertEquals("Key (a)=(1) already exists.",
                detailOf("INSERT INTO zzx2_b VALUES (1,'y',9)"));
        assertEquals("zzx2_b_u1",
                fieldsOf("INSERT INTO zzx2_b VALUES (1,'y',9)").getConstraint());

        assertEquals("Key (lower(b))=(x) already exists.",
                detailOf("INSERT INTO zzx2_b VALUES (2,'X',9)"));
        // The expression key is written the same way it is in the "is duplicated." sentence.
        assertEquals("Key ((a + c))=(6) already exists.",
                detailOf("INSERT INTO zzx2_b VALUES (3,'z',3)"));

        exec("DROP TABLE zzx2_b");

        exec("CREATE TABLE zzx2_c (\"A b\" int, x int)");
        exec("CREATE UNIQUE INDEX zzx2_c_u ON zzx2_c (\"A b\")");
        exec("INSERT INTO zzx2_c VALUES (1,1)");
        assertEquals("Key (\"A b\")=(1) already exists.",
                detailOf("INSERT INTO zzx2_c VALUES (1,2)"));
        exec("INSERT INTO zzx2_c VALUES (2,2)");
        assertEquals(2, num("SELECT count(*)::int FROM zzx2_c"));

        exec("DROP TABLE zzx2_c");
    }

    @Test
    void aRetypeThatWouldBreakAKeyIsRefusedByTheIndexItWouldRebuild() throws Exception {
        exec("CREATE TABLE zzx2_rt (\"A b\" text, x int)");
        exec("CREATE UNIQUE INDEX zzx2_rt_u ON zzx2_rt (\"A b\")");
        exec("INSERT INTO zzx2_rt VALUES ('1', 1), ('01', 2)");

        assertEquals("23505", stateOf("ALTER TABLE zzx2_rt ALTER COLUMN \"A b\""
                + " TYPE int USING \"A b\"::int"));
        assertEquals("could not create unique index \"zzx2_rt_u\"",
                messageOf("ALTER TABLE zzx2_rt ALTER COLUMN \"A b\""
                        + " TYPE int USING \"A b\"::int"));
        assertEquals("Key (\"A b\")=(1) is duplicated.",
                detailOf("ALTER TABLE zzx2_rt ALTER COLUMN \"A b\""
                        + " TYPE int USING \"A b\"::int"));

        // The refusal left the column and its rows as they were.
        assertEquals("text", scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'zzx2_rt'::regclass AND attname = 'A b'"));
        assertEquals("1/1;01/2", rowsOf("SELECT \"A b\", x FROM zzx2_rt ORDER BY x"));

        exec("DROP TABLE zzx2_rt");

        exec("CREATE TABLE zzx2_rc (a text, b int)");
        exec("CREATE UNIQUE INDEX zzx2_rc_u ON zzx2_rc ((a || 'x'))");
        exec("INSERT INTO zzx2_rc VALUES ('p', 1)");
        assertEquals("Key ((a || 'x'::text))=(px) already exists.",
                detailOf("INSERT INTO zzx2_rc VALUES ('p', 2)"));
        assertEquals(1, num("SELECT count(*)::int FROM zzx2_rc"));

        exec("DROP TABLE zzx2_rc");
    }

    // ------------------------------------------------------------ What may stand as an index key, and how the definition is written back

    @Test
    void aCallMayNameItsSchemaAndTheQualifierIsNotWrittenBack() throws Exception {
        exec("CREATE TABLE zzx2_e (a int, b text, c text, \"A b\" text)");
        exec("CREATE INDEX zzx2_e_01 ON zzx2_e (pg_catalog.lower(c))");
        exec("CREATE INDEX zzx2_e_02 ON zzx2_e (pg_catalog.length(b))");
        exec("CREATE INDEX zzx2_e_03 ON zzx2_e (pg_catalog.abs(a) DESC)");
        exec("CREATE INDEX zzx2_e_04 ON zzx2_e (upper(b), pg_catalog.lower(c), a DESC)");
        exec("CREATE INDEX zzx2_e_09 ON zzx2_e (lower(\"A b\"))");
        exec("CREATE INDEX zzx2_e_10 ON zzx2_e ((lower(\"A b\")))");

        assertEquals("CREATE INDEX zzx2_e_01 ON public.zzx2_e USING btree (lower(c))",
                indexDef("zzx2_e_01"));
        assertEquals("CREATE INDEX zzx2_e_02 ON public.zzx2_e USING btree (length(b))",
                indexDef("zzx2_e_02"));
        assertEquals("CREATE INDEX zzx2_e_03 ON public.zzx2_e USING btree (abs(a) DESC)",
                indexDef("zzx2_e_03"));
        assertEquals("CREATE INDEX zzx2_e_04 ON public.zzx2_e"
                        + " USING btree (upper(b), lower(c), a DESC)",
                indexDef("zzx2_e_04"));
        // A quoted name inside a call keeps its quotes, whichever of the two forms wrote it.
        assertEquals("CREATE INDEX zzx2_e_09 ON public.zzx2_e USING btree (lower(\"A b\"))",
                indexDef("zzx2_e_09"));
        assertEquals("CREATE INDEX zzx2_e_10 ON public.zzx2_e USING btree (lower(\"A b\"))",
                indexDef("zzx2_e_10"));

        // An unnamed index over a qualified call is named after the function.
        exec("CREATE INDEX ON zzx2_e (pg_catalog.lower(c))");
        assertEquals("zzx2_e_lower_idx",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'zzx2_e'"
                        + " AND indexname LIKE '%lower%'"));

        // Only a column name, a call and a parenthesised expression are keys. A qualified name
        // with no argument list is reported at the token where that list should have begun,
        // because PostgreSQL has already read it as a function name.
        assertEquals("42601", stateOf("CREATE INDEX zzx2_e_x1 ON zzx2_e (zzx2_e.a)"));
        assertEquals("syntax error at or near \")\"",
                messageOf("CREATE INDEX zzx2_e_x1 ON zzx2_e (zzx2_e.a)"));
        assertEquals("syntax error at or near \"DESC\"",
                messageOf("CREATE INDEX zzx2_e_x2 ON zzx2_e (zzx2_e.a DESC)"));
        assertEquals("syntax error at or near \"::\"",
                messageOf("CREATE INDEX zzx2_e_x3 ON zzx2_e (a::text)"));
        assertEquals("syntax error at or near \"+\"",
                messageOf("CREATE INDEX zzx2_e_x4 ON zzx2_e (a + 1)"));
        // The same expressions in parentheses are keys.
        exec("CREATE INDEX zzx2_e_11 ON zzx2_e ((a::text))");
        exec("CREATE INDEX zzx2_e_12 ON zzx2_e ((a + 1))");

        // A collation and an operator class may name their schema too.
        exec("CREATE INDEX zzx2_e_05 ON zzx2_e (b COLLATE pg_catalog.\"C\")");
        exec("CREATE INDEX zzx2_e_07 ON zzx2_e"
                + " (pg_catalog.lower(c) COLLATE \"C\" text_pattern_ops DESC NULLS FIRST)");
        exec("CREATE INDEX zzx2_e_08 ON zzx2_e"
                + " ((a::text) COLLATE \"C\" text_pattern_ops ASC NULLS LAST)");
        assertEquals("CREATE INDEX zzx2_e_05 ON public.zzx2_e USING btree (b COLLATE \"C\")",
                indexDef("zzx2_e_05"));
        assertEquals("CREATE INDEX zzx2_e_07 ON public.zzx2_e"
                        + " USING btree (lower(c) COLLATE \"C\" text_pattern_ops DESC)",
                indexDef("zzx2_e_07"));
        assertEquals("CREATE INDEX zzx2_e_08 ON public.zzx2_e"
                        + " USING btree (((a)::text) COLLATE \"C\" text_pattern_ops)",
                indexDef("zzx2_e_08"));

        // The schema of an operator class is opened before the class is looked for.
        assertEquals("3F000",
                stateOf("CREATE INDEX zzx2_e_x5 ON zzx2_e (a zzx2_nosuch.int4_ops)"));
        assertEquals("schema \"zzx2_nosuch\" does not exist",
                messageOf("CREATE INDEX zzx2_e_x5 ON zzx2_e (a zzx2_nosuch.int4_ops)"));
        assertEquals("42704", stateOf("CREATE INDEX zzx2_e_x6 ON zzx2_e (a zzx2_nosuch_ops)"));
        assertEquals("operator class \"zzx2_nosuch_ops\" does not exist"
                        + " for access method \"btree\"",
                messageOf("CREATE INDEX zzx2_e_x6 ON zzx2_e (a zzx2_nosuch_ops)"));
        // An operator class may be given parameters; none of the btree classes takes any, and
        // the complaint names the class without quoting it.
        assertEquals("22023", stateOf("CREATE INDEX zzx2_e_x7 ON zzx2_e (b text_ops (x = 1))"));
        assertEquals("operator class text_ops has no options",
                messageOf("CREATE INDEX zzx2_e_x7 ON zzx2_e (b text_ops (x = 1))"));
        // A class that does not exist is still reported first.
        assertEquals("operator class \"zzx2_nosuch_ops\" does not exist"
                        + " for access method \"btree\"",
                messageOf("CREATE INDEX zzx2_e_x8 ON zzx2_e (b zzx2_nosuch_ops (x = 1))"));
        // And the relation is opened before any of this.
        assertEquals("42P01",
                stateOf("CREATE INDEX zzx2_e_x9 ON zzx2_nosuchtable (b text_ops (x = 1))"));
        assertEquals("relation \"zzx2_nosuchtable\" does not exist",
                messageOf("CREATE INDEX zzx2_e_x10 ON zzx2_nosuchtable (pg_catalog.lower(b))"));

        exec("DROP TABLE zzx2_e");
    }

    @Test
    void aWordPostgresReservesIsNotAnIndexKey() throws Exception {
        exec("CREATE TABLE zzx2_ks (a int, b text)");

        String[][] refused = {
                {"CASE WHEN a > 1 THEN b ELSE 'z' END", "CASE"},
                {"NOT a", "NOT"},
                {"ARRAY[a]", "ARRAY"},
                {"NULL", "NULL"},
                {"TRUE", "TRUE"},
                {"SELECT a", "SELECT"},
                {"DEFAULT", "DEFAULT"}};
        for (String[] one : refused) {
            String sql = "CREATE INDEX zzx2_bad ON zzx2_ks (" + one[0] + ")";
            assertEquals("42601", stateOf(sql));
            assertEquals("syntax error at or near \"" + one[1] + "\"", messageOf(sql));
        }

        // left cannot be a column name, so PostgreSQL has already read it as a function name and
        // complains where its argument list should be.
        assertEquals("syntax error at or near \")\"",
                messageOf("CREATE INDEX zzx2_bad ON zzx2_ks (left)"));
        // A word that only names a column never heads a call.
        assertEquals("syntax error at or near \"(\"",
                messageOf("CREATE INDEX zzx2_bad ON zzx2_ks (ROW(a))"));
        assertEquals("syntax error at or near \"(\"",
                messageOf("CREATE INDEX zzx2_bad ON zzx2_ks (VALUES(1))"));
        // A string after a key is reported with its quotes.
        assertEquals("syntax error at or near \"'1 day'\"",
                messageOf("CREATE INDEX zzx2_bad ON zzx2_ks (INTERVAL '1 day')"));

        // The calls SQL spells with a keyword are keys.
        exec("CREATE INDEX zzx2_kk_1 ON zzx2_ks (CAST(a AS text))");
        exec("CREATE INDEX zzx2_kk_2 ON zzx2_ks (COALESCE(b, 'z'))");
        exec("CREATE INDEX zzx2_kk_3 ON zzx2_ks (GREATEST(a, 1))");
        assertEquals("zzx2_kk_1/CREATE INDEX zzx2_kk_1 ON public.zzx2_ks"
                        + " USING btree (((a)::text))"
                        + ";zzx2_kk_2/CREATE INDEX zzx2_kk_2 ON public.zzx2_ks"
                        + " USING btree (COALESCE(b, 'z'::text))"
                        + ";zzx2_kk_3/CREATE INDEX zzx2_kk_3 ON public.zzx2_ks"
                        + " USING btree (GREATEST(a, 1))",
                rowsOf("SELECT indexname, indexdef FROM pg_indexes"
                        + " WHERE tablename = 'zzx2_ks' ORDER BY indexname"));

        exec("DROP TABLE zzx2_ks");
    }

    @Test
    void aValueFunctionAsAKeyIsRefusedForBeingNoMoreThanStable() throws Exception {
        exec("CREATE TABLE zzx2_vf (a int)");

        for (String key : new String[]{"CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_USER",
                "LOCALTIME", "CURRENT_TIME", "SESSION_USER", "CURRENT_SCHEMA", "CURRENT_CATALOG",
                "LOCALTIMESTAMP", "USER"}) {
            String sql = "CREATE INDEX zzx2_bad ON zzx2_vf (" + key + ")";
            assertEquals("42P17", stateOf(sql));
            assertEquals("functions in index expression must be marked IMMUTABLE",
                    messageOf(sql));
        }
        assertEquals(0, num("SELECT count(*)::int FROM pg_indexes"
                + " WHERE tablename = 'zzx2_vf'"));

        exec("DROP TABLE zzx2_vf");
    }

    @Test
    void aDefinitionWritesDownOnlyTheOrderingItsDirectionDoesNotImply() throws Exception {
        exec("CREATE TABLE zzx2_f (a int)");
        exec("CREATE INDEX zzx2_f_01 ON zzx2_f (a ASC)");
        exec("CREATE INDEX zzx2_f_02 ON zzx2_f (a DESC)");
        exec("CREATE INDEX zzx2_f_03 ON zzx2_f (a ASC NULLS FIRST)");
        exec("CREATE INDEX zzx2_f_04 ON zzx2_f (a ASC NULLS LAST)");
        exec("CREATE INDEX zzx2_f_05 ON zzx2_f (a DESC NULLS FIRST)");
        exec("CREATE INDEX zzx2_f_06 ON zzx2_f (a DESC NULLS LAST)");
        exec("CREATE INDEX zzx2_f_07 ON zzx2_f (a NULLS FIRST)");
        exec("CREATE INDEX zzx2_f_08 ON zzx2_f (a NULLS LAST)");

        assertEquals("zzx2_f_01=CREATE INDEX zzx2_f_01 ON public.zzx2_f USING btree (a)"
                        + ";zzx2_f_02=CREATE INDEX zzx2_f_02 ON public.zzx2_f USING btree (a DESC)"
                        + ";zzx2_f_03=CREATE INDEX zzx2_f_03 ON public.zzx2_f"
                        + " USING btree (a NULLS FIRST)"
                        + ";zzx2_f_04=CREATE INDEX zzx2_f_04 ON public.zzx2_f USING btree (a)"
                        + ";zzx2_f_05=CREATE INDEX zzx2_f_05 ON public.zzx2_f USING btree (a DESC)"
                        + ";zzx2_f_06=CREATE INDEX zzx2_f_06 ON public.zzx2_f"
                        + " USING btree (a DESC NULLS LAST)"
                        + ";zzx2_f_07=CREATE INDEX zzx2_f_07 ON public.zzx2_f"
                        + " USING btree (a NULLS FIRST)"
                        + ";zzx2_f_08=CREATE INDEX zzx2_f_08 ON public.zzx2_f USING btree (a)",
                indexDefsOf("zzx2_f"));

        // An access method with no order to it refuses a written direction even where that
        // direction is the one it would have taken anyway.
        for (String[] one : new String[][]{
                {"a ASC", "access method \"hash\" does not support ASC/DESC options"},
                {"a DESC", "access method \"hash\" does not support ASC/DESC options"},
                {"a NULLS LAST",
                        "access method \"hash\" does not support NULLS FIRST/LAST options"},
                {"a NULLS FIRST",
                        "access method \"hash\" does not support NULLS FIRST/LAST options"}}) {
            String sql = "CREATE INDEX zzx2_bad ON zzx2_f USING hash (" + one[0] + ")";
            assertEquals("0A000", stateOf(sql));
            assertEquals(one[1], messageOf(sql));
        }
        exec("CREATE INDEX zzx2_f_ok ON zzx2_f USING hash (a)");
        assertEquals("CREATE INDEX zzx2_f_ok ON public.zzx2_f USING hash (a)",
                indexDef("zzx2_f_ok"));

        exec("DROP TABLE zzx2_f");
    }

    @Test
    void anOperatorClassThatIsTheTypesDefaultIsLeftOutOfTheDefinition() throws Exception {
        exec("CREATE TABLE zzx2_oc (a int, b text, n numeric)");
        exec("CREATE INDEX zzx2_oc_1 ON zzx2_oc (b pg_catalog.text_ops)");
        exec("CREATE INDEX zzx2_oc_2 ON zzx2_oc (a int4_ops DESC NULLS LAST)");
        exec("CREATE INDEX zzx2_oc_3 ON zzx2_oc (n numeric_ops)");
        exec("CREATE INDEX zzx2_oc_4 ON zzx2_oc (b text_pattern_ops)");

        assertEquals("zzx2_oc_1=CREATE INDEX zzx2_oc_1 ON public.zzx2_oc USING btree (b)"
                        + ";zzx2_oc_2=CREATE INDEX zzx2_oc_2 ON public.zzx2_oc"
                        + " USING btree (a DESC NULLS LAST)"
                        + ";zzx2_oc_3=CREATE INDEX zzx2_oc_3 ON public.zzx2_oc USING btree (n)"
                        + ";zzx2_oc_4=CREATE INDEX zzx2_oc_4 ON public.zzx2_oc"
                        + " USING btree (b text_pattern_ops)",
                indexDefsOf("zzx2_oc"));

        exec("DROP TABLE zzx2_oc");
    }

    @Test
    void aCollationBelongsToTheKeyItWasWrittenOn() throws Exception {
        exec("CREATE TABLE zzx2_co (b text)");
        exec("CREATE INDEX zzx2_co_1 ON zzx2_co (b COLLATE \"C\")");
        exec("CREATE INDEX zzx2_co_2 ON zzx2_co ((b COLLATE \"C\"))");
        exec("CREATE INDEX zzx2_co_3 ON zzx2_co ((b COLLATE \"C\") DESC)");
        exec("CREATE INDEX zzx2_co_4 ON zzx2_co ((upper(b) COLLATE \"C\"))");
        exec("CREATE INDEX zzx2_co_5 ON zzx2_co (((b || 'x') COLLATE \"C\"))");
        exec("CREATE INDEX zzx2_co_6 ON zzx2_co ((upper(b COLLATE \"C\")))");
        exec("CREATE INDEX zzx2_co_7 ON zzx2_co ((b COLLATE pg_catalog.\"C\"))");
        exec("CREATE INDEX zzx2_co_8 ON zzx2_co ((b COLLATE \"C\") text_pattern_ops)");
        exec("CREATE INDEX zzx2_co_9 ON zzx2_co ((b COLLATE \"C\" || 'x'))");

        // A collation nested inside stays where it was written, and is bracketed as a node.
        assertEquals("zzx2_co_1=CREATE INDEX zzx2_co_1 ON public.zzx2_co"
                        + " USING btree (b COLLATE \"C\")"
                        + ";zzx2_co_2=CREATE INDEX zzx2_co_2 ON public.zzx2_co"
                        + " USING btree (b COLLATE \"C\")"
                        + ";zzx2_co_3=CREATE INDEX zzx2_co_3 ON public.zzx2_co"
                        + " USING btree (b COLLATE \"C\" DESC)"
                        + ";zzx2_co_4=CREATE INDEX zzx2_co_4 ON public.zzx2_co"
                        + " USING btree (upper(b) COLLATE \"C\")"
                        + ";zzx2_co_5=CREATE INDEX zzx2_co_5 ON public.zzx2_co"
                        + " USING btree (((b || 'x'::text)) COLLATE \"C\")"
                        + ";zzx2_co_6=CREATE INDEX zzx2_co_6 ON public.zzx2_co"
                        + " USING btree (upper((b COLLATE \"C\")))"
                        + ";zzx2_co_7=CREATE INDEX zzx2_co_7 ON public.zzx2_co"
                        + " USING btree (b COLLATE \"C\")"
                        + ";zzx2_co_8=CREATE INDEX zzx2_co_8 ON public.zzx2_co"
                        + " USING btree (b COLLATE \"C\" text_pattern_ops)"
                        + ";zzx2_co_9=CREATE INDEX zzx2_co_9 ON public.zzx2_co"
                        + " USING btree ((((b COLLATE \"C\") || 'x'::text)))",
                indexDefsOf("zzx2_co"));

        exec("DROP TABLE zzx2_co");
    }

    @Test
    void aConstantStandingAtATextParameterIsATextConstant() throws Exception {
        exec("CREATE TABLE zzx2_fn (a int, b text, ts timestamp)");
        exec("CREATE INDEX zzx2_fn_1 ON zzx2_fn (date_trunc('month', ts))");
        exec("CREATE INDEX zzx2_fn_2 ON zzx2_fn (date_part('year', ts))");
        exec("CREATE INDEX zzx2_fn_3 ON zzx2_fn (timezone('UTC', ts))");
        exec("CREATE INDEX zzx2_fn_4 ON zzx2_fn (starts_with(b, 'a'))");
        exec("CREATE INDEX zzx2_fn_5 ON zzx2_fn (split_part(b, ',', 1))");
        exec("CREATE INDEX zzx2_fn_6 ON zzx2_fn (make_interval(days => a))");

        // An argument written under a parameter's name keeps the name.
        assertEquals("zzx2_fn_1=CREATE INDEX zzx2_fn_1 ON public.zzx2_fn"
                        + " USING btree (date_trunc('month'::text, ts))"
                        + ";zzx2_fn_2=CREATE INDEX zzx2_fn_2 ON public.zzx2_fn"
                        + " USING btree (date_part('year'::text, ts))"
                        + ";zzx2_fn_3=CREATE INDEX zzx2_fn_3 ON public.zzx2_fn"
                        + " USING btree (timezone('UTC'::text, ts))"
                        + ";zzx2_fn_4=CREATE INDEX zzx2_fn_4 ON public.zzx2_fn"
                        + " USING btree (starts_with(b, 'a'::text))"
                        + ";zzx2_fn_5=CREATE INDEX zzx2_fn_5 ON public.zzx2_fn"
                        + " USING btree (split_part(b, ','::text, 1))"
                        + ";zzx2_fn_6=CREATE INDEX zzx2_fn_6 ON public.zzx2_fn"
                        + " USING btree (make_interval(days => a))",
                indexDefsOf("zzx2_fn"));

        exec("DROP TABLE zzx2_fn");
    }

    @Test
    void aJsonOperatorIsResolvedByWhatItTakes() throws Exception {
        exec("CREATE TABLE zzx2_js (j jsonb, ta text[])");
        exec("CREATE INDEX zzx2_js_1 ON zzx2_js ((j -> 'k'))");
        exec("CREATE INDEX zzx2_js_2 ON zzx2_js ((j ->> 'k'))");
        exec("CREATE INDEX zzx2_js_3 ON zzx2_js ((j -> 0))");
        exec("CREATE INDEX zzx2_js_4 ON zzx2_js ((j #> '{a}'))");
        exec("CREATE INDEX zzx2_js_5 ON zzx2_js ((j #>> '{a}'))");
        exec("CREATE INDEX zzx2_js_6 ON zzx2_js ((j - 'a'))");
        exec("CREATE INDEX zzx2_js_7 ON zzx2_js ((ta && ARRAY['x']))");

        assertEquals("zzx2_js_1=CREATE INDEX zzx2_js_1 ON public.zzx2_js"
                        + " USING btree (((j -> 'k'::text)))"
                        + ";zzx2_js_2=CREATE INDEX zzx2_js_2 ON public.zzx2_js"
                        + " USING btree (((j ->> 'k'::text)))"
                        + ";zzx2_js_3=CREATE INDEX zzx2_js_3 ON public.zzx2_js"
                        + " USING btree (((j -> 0)))"
                        + ";zzx2_js_4=CREATE INDEX zzx2_js_4 ON public.zzx2_js"
                        + " USING btree (((j #> '{a}'::text[])))"
                        + ";zzx2_js_5=CREATE INDEX zzx2_js_5 ON public.zzx2_js"
                        + " USING btree (((j #>> '{a}'::text[])))"
                        + ";zzx2_js_6=CREATE INDEX zzx2_js_6 ON public.zzx2_js"
                        + " USING btree (((j - 'a'::text)))"
                        + ";zzx2_js_7=CREATE INDEX zzx2_js_7 ON public.zzx2_js"
                        + " USING btree (((ta && ARRAY['x'::text])))",
                indexDefsOf("zzx2_js"));

        exec("DROP TABLE zzx2_js");

        // The arrow that reads a member out as text reads it out of json too.
        exec("CREATE TABLE zzx2_jn (k json)");
        exec("CREATE INDEX zzx2_jn_2 ON zzx2_jn ((k ->> 'a'))");
        assertEquals("CREATE INDEX zzx2_jn_2 ON public.zzx2_jn"
                        + " USING btree (((k ->> 'a'::text)))",
                indexDef("zzx2_jn_2"));
        exec("DROP TABLE zzx2_jn");

        // A jsonb constant is printed the way jsonb writes it.
        exec("CREATE TABLE zzx2_jc (j jsonb)");
        exec("ALTER TABLE zzx2_jc ADD CONSTRAINT zzx2_jc_c CHECK (j @> '{\"a\":1}')");
        assertEquals("CHECK ((j @> '{\"a\": 1}'::jsonb))",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzx2_jc_c'"));
        exec("DROP TABLE zzx2_jc");
    }

    @Test
    void atTimeZoneAndXmlserializeKeepTheirOwnSyntax() throws Exception {
        exec("CREATE TABLE zzx2_tz (b text, ts timestamp, tz timestamptz, iv interval)");
        exec("CREATE INDEX zzx2_tz_1 ON zzx2_tz ((ts AT TIME ZONE 'UTC'))");
        exec("CREATE INDEX zzx2_tz_2 ON zzx2_tz ((tz AT TIME ZONE 'UTC'))");
        exec("CREATE INDEX zzx2_tz_3 ON zzx2_tz ((ts AT TIME ZONE b))");
        exec("CREATE INDEX zzx2_tz_4 ON zzx2_tz ((ts AT TIME ZONE iv))");

        // A zone written as a bare string resolves to the call over text; a column keeps its type.
        assertEquals("zzx2_tz_1=CREATE INDEX zzx2_tz_1 ON public.zzx2_tz"
                        + " USING btree ((ts AT TIME ZONE 'UTC'::text))"
                        + ";zzx2_tz_2=CREATE INDEX zzx2_tz_2 ON public.zzx2_tz"
                        + " USING btree ((tz AT TIME ZONE 'UTC'::text))"
                        + ";zzx2_tz_3=CREATE INDEX zzx2_tz_3 ON public.zzx2_tz"
                        + " USING btree ((ts AT TIME ZONE b))"
                        + ";zzx2_tz_4=CREATE INDEX zzx2_tz_4 ON public.zzx2_tz"
                        + " USING btree ((ts AT TIME ZONE iv))",
                indexDefsOf("zzx2_tz"));
        exec("DROP TABLE zzx2_tz");

        // The same syntax reaches a constraint and a view.
        exec("CREATE TABLE zzx2_tzc (ts timestamp)");
        exec("ALTER TABLE zzx2_tzc ADD CONSTRAINT zzx2_tzc_c"
                + " CHECK ((ts AT TIME ZONE 'UTC') IS NOT NULL)");
        assertEquals("CHECK (((ts AT TIME ZONE 'UTC'::text) IS NOT NULL))",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzx2_tzc_c'"));
        exec("CREATE VIEW zzx2_tzc_v AS SELECT ts AT TIME ZONE 'UTC' AS z FROM zzx2_tzc");
        assertEquals(" SELECT (ts AT TIME ZONE 'UTC'::text) AS z\n   FROM zzx2_tzc;",
                scalar("SELECT pg_get_viewdef('zzx2_tzc_v'::regclass, true)"));
        exec("DROP VIEW zzx2_tzc_v");
        exec("DROP TABLE zzx2_tzc");

        exec("CREATE TABLE zzx2_xs (x xml)");
        exec("CREATE INDEX zzx2_xs_1 ON zzx2_xs (XMLSERIALIZE(CONTENT x AS text))");
        exec("CREATE INDEX zzx2_xs_2 ON zzx2_xs (XMLSERIALIZE(DOCUMENT x AS text))");
        exec("CREATE INDEX zzx2_xs_3 ON zzx2_xs (XMLSERIALIZE(CONTENT x AS text INDENT))");
        exec("CREATE INDEX zzx2_xs_4 ON zzx2_xs (XMLSERIALIZE(CONTENT x AS text NO INDENT))");
        exec("CREATE INDEX zzx2_xs_5 ON zzx2_xs (XMLSERIALIZE(CONTENT x AS varchar))");

        // A type that is not text is a coercion sitting on the call, and that takes parentheses.
        assertEquals("zzx2_xs_1=CREATE INDEX zzx2_xs_1 ON public.zzx2_xs"
                        + " USING btree (XMLSERIALIZE(CONTENT x AS text NO INDENT))"
                        + ";zzx2_xs_2=CREATE INDEX zzx2_xs_2 ON public.zzx2_xs"
                        + " USING btree (XMLSERIALIZE(DOCUMENT x AS text NO INDENT))"
                        + ";zzx2_xs_3=CREATE INDEX zzx2_xs_3 ON public.zzx2_xs"
                        + " USING btree (XMLSERIALIZE(CONTENT x AS text INDENT))"
                        + ";zzx2_xs_4=CREATE INDEX zzx2_xs_4 ON public.zzx2_xs"
                        + " USING btree (XMLSERIALIZE(CONTENT x AS text NO INDENT))"
                        + ";zzx2_xs_5=CREATE INDEX zzx2_xs_5 ON public.zzx2_xs"
                        + " USING btree ((XMLSERIALIZE(CONTENT x AS character varying NO INDENT)))",
                indexDefsOf("zzx2_xs"));

        exec("DROP TABLE zzx2_xs");
    }

    @Test
    void aNameTheGrammarKnowsIsWrittenBackInQuotes() throws Exception {
        exec("CREATE TABLE zzx2_kw (\"time\" int, \"substring\" text, \"value\" int,"
                + " \"int\" int, \"row\" int, t text)");
        exec("CREATE INDEX zzx2_kw_1 ON zzx2_kw ((\"time\" + 1))");
        exec("CREATE INDEX zzx2_kw_2 ON zzx2_kw (lower(\"substring\"))");
        exec("CREATE INDEX zzx2_kw_3 ON zzx2_kw ((\"value\" + 1))");
        exec("CREATE INDEX zzx2_kw_4 ON zzx2_kw ((\"int\" + 1))");
        exec("CREATE INDEX zzx2_kw_5 ON zzx2_kw ((\"row\" + 1))");
        exec("CREATE INDEX zzx2_kw_6 ON zzx2_kw (substring(t, 1, 2))");
        exec("CREATE INDEX zzx2_kw_7 ON zzx2_kw (left(t, 1))");
        exec("CREATE INDEX zzx2_kw_8 ON zzx2_kw (right(t, 1))");

        // value is unreserved, so a relation column of that name stays bare.
        assertEquals("zzx2_kw_1=CREATE INDEX zzx2_kw_1 ON public.zzx2_kw"
                        + " USING btree (((\"time\" + 1)))"
                        + ";zzx2_kw_2=CREATE INDEX zzx2_kw_2 ON public.zzx2_kw"
                        + " USING btree (lower(\"substring\"))"
                        + ";zzx2_kw_3=CREATE INDEX zzx2_kw_3 ON public.zzx2_kw"
                        + " USING btree (((value + 1)))"
                        + ";zzx2_kw_4=CREATE INDEX zzx2_kw_4 ON public.zzx2_kw"
                        + " USING btree (((\"int\" + 1)))"
                        + ";zzx2_kw_5=CREATE INDEX zzx2_kw_5 ON public.zzx2_kw"
                        + " USING btree (((\"row\" + 1)))"
                        + ";zzx2_kw_6=CREATE INDEX zzx2_kw_6 ON public.zzx2_kw"
                        + " USING btree (\"substring\"(t, 1, 2))"
                        + ";zzx2_kw_7=CREATE INDEX zzx2_kw_7 ON public.zzx2_kw"
                        + " USING btree (\"left\"(t, 1))"
                        + ";zzx2_kw_8=CREATE INDEX zzx2_kw_8 ON public.zzx2_kw"
                        + " USING btree (\"right\"(t, 1))",
                indexDefsOf("zzx2_kw"));

        exec("DROP TABLE zzx2_kw");

        // The same rule reaches a constraint, where the domain placeholder is the keyword and a
        // column of that name is not.
        exec("CREATE TABLE zzx2_v (\"value\" int, \"time\" int)");
        exec("ALTER TABLE zzx2_v ADD CONSTRAINT zzx2_v_c CHECK (\"value\" > 0 AND \"time\" > 0)");
        assertEquals("CHECK (((value > 0) AND (\"time\" > 0)))",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname = 'zzx2_v_c'"));
        exec("DROP TABLE zzx2_v");

        exec("CREATE DOMAIN zzx2_dom AS int CHECK (VALUE > 0)");
        assertEquals("CHECK ((VALUE > 0))",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname LIKE 'zzx2\\_dom%'"));
        exec("DROP DOMAIN zzx2_dom");
    }

    // ------------------------------------------------------------ A NOT NULL a child holds because its parent declares it

    /** attname/attnotnull/attislocal/attinhcount for every live column of a relation. */
    private static String attsOf(String relation) throws SQLException {
        return scalar("SELECT string_agg(a.attname||'/'||a.attnotnull::text||'/'"
                + "||a.attislocal::text||'/'||a.attinhcount::text, ',' ORDER BY a.attnum)"
                + " FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid"
                + " WHERE cl.relname = '" + relation + "' AND a.attnum > 0"
                + " AND NOT a.attisdropped");
    }

    /** The same, for several relations at once, each value prefixed with the relation's name. */
    private static String attsAcross(String relations) throws SQLException {
        return scalar("SELECT string_agg(cl.relname||':'||a.attname||'/'||a.attnotnull::text||'/'"
                + "||a.attislocal::text||'/'||a.attinhcount::text, ',' ORDER BY cl.relname,"
                + " a.attnum) FROM pg_attribute a JOIN pg_class cl ON a.attrelid = cl.oid"
                + " WHERE cl.relname IN (" + relations + ") AND a.attnum > 0"
                + " AND NOT a.attisdropped");
    }

    /** conname/conislocal/coninhcount for every NOT NULL constraint a relation holds. */
    private static String notNullsOf(String relation) throws SQLException {
        return scalar("SELECT string_agg(c.conname||'/'||c.conislocal::text||'/'"
                + "||c.coninhcount::text, ',' ORDER BY c.conname)"
                + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                + " WHERE cl.relname = '" + relation + "' AND c.contype = 'n'");
    }

    /** The same, for several relations at once, each value prefixed with the relation's name. */
    private static String notNullsAcross(String relations) throws SQLException {
        return scalar("SELECT string_agg(cl.relname||':'||c.conname||'/'||c.conislocal::text||'/'"
                + "||c.coninhcount::text, ',' ORDER BY cl.relname, c.conname)"
                + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                + " WHERE cl.relname IN (" + relations + ") AND c.contype = 'n'");
    }

    /** conname/conislocal/coninhcount/connoinherit for every NOT NULL a relation holds. */
    private static String notNullsWithReachOf(String relation) throws SQLException {
        return scalar("SELECT string_agg(c.conname||'/'||c.conislocal::text||'/'"
                + "||c.coninhcount::text||'/'||c.connoinherit::text, ',' ORDER BY c.conname)"
                + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                + " WHERE cl.relname = '" + relation + "' AND c.contype = 'n'");
    }

    @Test
    void aChildThatRelistsAColumnWithoutNotNullStillHoldsTheParentsRule() throws Exception {
        exec("CREATE TABLE zzy8b1_p (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_c (i int NOT NULL, j int, k int NOT NULL) INHERITS (zzy8b1_p)");

        // j was listed without NOT NULL and keeps the parent's rule all the same: a child cannot
        // be less strict than the relation its rows also belong to.
        assertEquals("i/true/true/1,j/true/true/1,k/true/true/1", attsOf("zzy8b1_c"));
        // The restated one is the child's own and counts the parent too; the one only taken
        // answers to the parent's name; the one nobody above declares counts nobody.
        assertEquals("zzy8b1_c_i_not_null/n/true/1/false,zzy8b1_c_k_not_null/n/true/0/false,"
                        + "zzy8b1_p_j_not_null/n/false/1/false",
                scalar("SELECT string_agg(c.conname||'/'||c.contype::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text||'/'"
                        + "||c.connoinherit::text, ',' ORDER BY c.conname) FROM pg_constraint c"
                        + " JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzy8b1_c' AND c.contype = 'n'"));

        // The rule is enforced, and the refusal names the column and the relation written to.
        assertEquals("23502", stateOf("INSERT INTO zzy8b1_c (i, k) VALUES (1, 1)"));
        assertEquals("null value in column \"j\" of relation \"zzy8b1_c\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_c (i, k) VALUES (1, 1)"));
        assertEquals("Failing row contains (1, null, 1).",
                detailOf("INSERT INTO zzy8b1_c (i, k) VALUES (1, 1)"));
        assertEquals("j", fieldsOf("INSERT INTO zzy8b1_c (i, k) VALUES (1, 1)").getColumn());
        assertEquals("0", scalar("SELECT count(*) FROM zzy8b1_c"));

        // It is about every row the relation holds, not only the ones written into it.
        exec("INSERT INTO zzy8b1_c (i, j, k) VALUES (1, 2, 3)");
        assertEquals("23502", stateOf("UPDATE zzy8b1_c SET j = NULL"));
        assertEquals("Failing row contains (1, null, 3).",
                detailOf("UPDATE zzy8b1_c SET j = NULL"));
        assertEquals("2", scalar("SELECT j FROM zzy8b1_c"));

        exec("DROP TABLE zzy8b1_c");
        exec("DROP TABLE zzy8b1_p");
    }

    @Test
    void theRuleAParentDeclaresIsRefusedToTheChildWhicheverWayItIsAskedFor() throws Exception {
        exec("CREATE TABLE zzy8b1_rp (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_rc (i int NOT NULL, j int, k int NOT NULL)"
                + " INHERITS (zzy8b1_rp)");

        assertEquals("42P16", stateOf("ALTER TABLE zzy8b1_rc ALTER COLUMN j DROP NOT NULL"));
        assertEquals("cannot drop inherited constraint \"zzy8b1_rp_j_not_null\""
                        + " of relation \"zzy8b1_rc\"",
                messageOf("ALTER TABLE zzy8b1_rc ALTER COLUMN j DROP NOT NULL"));
        // The child declared this one for itself, and is still refused while the parent declares
        // it too -- named by the constraint the child holds, not by the parent's.
        assertEquals("cannot drop inherited constraint \"zzy8b1_rc_i_not_null\""
                        + " of relation \"zzy8b1_rc\"",
                messageOf("ALTER TABLE zzy8b1_rc ALTER COLUMN i DROP NOT NULL"));
        // The named spelling of the drop answers the same way.
        assertEquals("42P16",
                stateOf("ALTER TABLE zzy8b1_rc DROP CONSTRAINT zzy8b1_rp_j_not_null"));
        assertEquals("cannot drop inherited constraint \"zzy8b1_rp_j_not_null\""
                        + " of relation \"zzy8b1_rc\"",
                messageOf("ALTER TABLE zzy8b1_rc DROP CONSTRAINT zzy8b1_rp_j_not_null"));

        // Nobody above declares k, so that one is the child's to withdraw.
        exec("ALTER TABLE zzy8b1_rc ALTER COLUMN k DROP NOT NULL");
        assertEquals("zzy8b1_rc_i_not_null/true/1,zzy8b1_rp_j_not_null/false/1",
                notNullsOf("zzy8b1_rc"));

        exec("DROP TABLE zzy8b1_rc");
        exec("DROP TABLE zzy8b1_rp");
    }

    @Test
    void theCountIsOnePerParentThatDeclaresTheColumnNotNull() throws Exception {
        exec("CREATE TABLE zzy8b1_q0 (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_q1 (i int NOT NULL, j int, k int NOT NULL)");
        exec("CREATE TABLE zzy8b1_qc (i int, j int NOT NULL, k int)"
                + " INHERITS (zzy8b1_q0, zzy8b1_q1)");

        // i is declared by both parents and counts two; j and k by one each. The name is the
        // first parent's, and the one the child declared is the child's own.
        assertEquals("i/true/true/2,j/true/true/2,k/true/true/2", attsOf("zzy8b1_qc"));
        assertEquals("zzy8b1_q0_i_not_null/false/2,zzy8b1_q1_k_not_null/false/1,"
                + "zzy8b1_qc_j_not_null/true/1", notNullsOf("zzy8b1_qc"));

        // A grandchild that re-lists them all takes each rule under the name it already has.
        exec("CREATE TABLE zzy8b1_qg (i int, j int, k int) INHERITS (zzy8b1_qc)");
        assertEquals("i/true/true/1,j/true/true/1,k/true/true/1", attsOf("zzy8b1_qg"));
        assertEquals("zzy8b1_q0_i_not_null/false/1,zzy8b1_q1_k_not_null/false/1,"
                + "zzy8b1_qc_j_not_null/false/1", notNullsOf("zzy8b1_qg"));

        // Two generations down, k is still refused.
        assertEquals("23502", stateOf("INSERT INTO zzy8b1_qg (i, j) VALUES (1, 1)"));
        assertEquals("null value in column \"k\" of relation \"zzy8b1_qg\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_qg (i, j) VALUES (1, 1)"));
        exec("INSERT INTO zzy8b1_qg (i, j, k) VALUES (1, 1, 1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_qg"));
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_q0"));
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_q1"));

        // The child declared j itself, and one parent declares it too.
        assertEquals("cannot drop inherited constraint \"zzy8b1_qc_j_not_null\""
                        + " of relation \"zzy8b1_qc\"",
                messageOf("ALTER TABLE zzy8b1_qc ALTER COLUMN j DROP NOT NULL"));
        // The grandchild is refused under the name the rule has carried since it was made.
        assertEquals("cannot drop inherited constraint \"zzy8b1_q1_k_not_null\""
                        + " of relation \"zzy8b1_qg\"",
                messageOf("ALTER TABLE zzy8b1_qg ALTER COLUMN k DROP NOT NULL"));

        exec("DROP TABLE zzy8b1_qg");
        exec("DROP TABLE zzy8b1_qc");
        exec("DROP TABLE zzy8b1_q0");
        exec("DROP TABLE zzy8b1_q1");
    }

    @Test
    void writingNullOnTheChildDoesNotTakeTheParentsRuleOff() throws Exception {
        exec("CREATE TABLE zzy8b1_np (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_nc (i int NULL, j int NULL, k int NULL)"
                + " INHERITS (zzy8b1_np)");

        assertEquals("i/true/true/1,j/true/true/1,k/false/true/1", attsOf("zzy8b1_nc"));
        assertEquals("zzy8b1_np_i_not_null/false/1,zzy8b1_np_j_not_null/false/1",
                notNullsOf("zzy8b1_nc"));

        assertEquals("23502", stateOf("INSERT INTO zzy8b1_nc (i, k) VALUES (1, 1)"));
        assertEquals("null value in column \"j\" of relation \"zzy8b1_nc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_nc (i, k) VALUES (1, 1)"));
        assertEquals("0", scalar("SELECT count(*) FROM zzy8b1_nc"));

        exec("DROP TABLE zzy8b1_nc");
        exec("DROP TABLE zzy8b1_np");
    }

    @Test
    void aRuleWrittenNoInheritIsAboutTheDeclaringRelationsRowsAlone() throws Exception {
        exec("CREATE TABLE zzy8b1_xp (i int NOT NULL NO INHERIT, j int NOT NULL)");
        exec("CREATE TABLE zzy8b1_xc () INHERITS (zzy8b1_xp)");
        exec("CREATE TABLE zzy8b1_xr (i int, j int) INHERITS (zzy8b1_xp)");

        assertEquals("zzy8b1_xp_i_not_null/true/0/true,zzy8b1_xp_j_not_null/true/0/false",
                notNullsWithReachOf("zzy8b1_xp"));
        // Neither the child that took the column nor the one that listed it holds the rule.
        assertEquals("i/false/false/1,j/true/false/1", attsOf("zzy8b1_xc"));
        assertEquals("i/false/true/1,j/true/true/1", attsOf("zzy8b1_xr"));
        assertEquals("zzy8b1_xp_j_not_null/false/1", notNullsOf("zzy8b1_xc"));
        assertEquals("zzy8b1_xp_j_not_null/false/1", notNullsOf("zzy8b1_xr"));

        // The descendants take the null the declaring relation is refused.
        exec("INSERT INTO zzy8b1_xc (j) VALUES (1)");
        exec("INSERT INTO zzy8b1_xr (j) VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_xc WHERE i IS NULL"));
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_xr WHERE i IS NULL"));
        assertEquals("23502", stateOf("INSERT INTO zzy8b1_xp (j) VALUES (1)"));
        assertEquals("null value in column \"i\" of relation \"zzy8b1_xp\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_xp (j) VALUES (1)"));
        // The rule written without NO INHERIT is enforced on the child all the same.
        assertEquals("null value in column \"j\" of relation \"zzy8b1_xc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_xc (i) VALUES (1)"));

        exec("DROP TABLE zzy8b1_xr");
        exec("DROP TABLE zzy8b1_xc");
        exec("DROP TABLE zzy8b1_xp");
    }

    @Test
    void aChildBesideANoInheritParentOwnsTheRuleItDeclaredOutright() throws Exception {
        exec("CREATE TABLE zzy8b1_yp (i int NOT NULL NO INHERIT, j int)");
        exec("CREATE TABLE zzy8b1_yc (i int NOT NULL, j int) INHERITS (zzy8b1_yp)");

        // The parent's rule reaches nobody, so the child counts no parent for its own.
        assertEquals("zzy8b1_yc_i_not_null/true/0/false", notNullsWithReachOf("zzy8b1_yc"));
        exec("ALTER TABLE zzy8b1_yc ALTER COLUMN i DROP NOT NULL");
        assertNull(notNullsOf("zzy8b1_yc"));
        assertEquals("i/false/true/1,j/false/true/1", attsOf("zzy8b1_yc"));

        exec("INSERT INTO zzy8b1_yc (j) VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_yc"));

        exec("DROP TABLE zzy8b1_yc");
        exec("DROP TABLE zzy8b1_yp");
    }

    @Test
    void aRuleTheParentTakesOnLaterIsCountedByTheChildThatDeclaredItFirst() throws Exception {
        exec("CREATE TABLE zzy8b1_gp (i int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_gc (k int NOT NULL) INHERITS (zzy8b1_gp)");

        // Nobody above declares k, so the child's own rule counts no parent.
        assertEquals("zzy8b1_gc_k_not_null/true/0,zzy8b1_gp_i_not_null/false/1",
                notNullsOf("zzy8b1_gc"));

        exec("ALTER TABLE zzy8b1_gp ALTER COLUMN k SET NOT NULL");
        assertEquals("zzy8b1_gc:zzy8b1_gc_k_not_null/true/1,"
                        + "zzy8b1_gc:zzy8b1_gp_i_not_null/false/1,"
                        + "zzy8b1_gp:zzy8b1_gp_i_not_null/true/0,"
                        + "zzy8b1_gp:zzy8b1_gp_k_not_null/true/0",
                notNullsAcross("'zzy8b1_gp','zzy8b1_gc'"));
        assertEquals("cannot drop inherited constraint \"zzy8b1_gc_k_not_null\""
                        + " of relation \"zzy8b1_gc\"",
                messageOf("ALTER TABLE zzy8b1_gc ALTER COLUMN k DROP NOT NULL"));

        // The parent letting go again leaves the child's own rule standing and enforced.
        exec("ALTER TABLE zzy8b1_gp ALTER COLUMN k DROP NOT NULL");
        assertEquals("zzy8b1_gc:zzy8b1_gc_k_not_null/true/0,"
                        + "zzy8b1_gc:zzy8b1_gp_i_not_null/false/1,"
                        + "zzy8b1_gp:zzy8b1_gp_i_not_null/true/0",
                notNullsAcross("'zzy8b1_gp','zzy8b1_gc'"));
        assertEquals("null value in column \"k\" of relation \"zzy8b1_gc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_gc (i) VALUES (1)"));
        exec("INSERT INTO zzy8b1_gp (i) VALUES (1)");
        assertEquals("0", scalar("SELECT count(*) FROM zzy8b1_gc"));
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_gp"));

        exec("DROP TABLE zzy8b1_gc");
        exec("DROP TABLE zzy8b1_gp");
    }

    @Test
    void aParentLettingGoLeavesStandingWhatTheDescendantDeclaredItself() throws Exception {
        exec("CREATE TABLE zzy8b1_dp (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_dc (i int NOT NULL, j int, k int NOT NULL)"
                + " INHERITS (zzy8b1_dp)");
        exec("CREATE TABLE zzy8b1_dg () INHERITS (zzy8b1_dc)");

        // The drop takes away the one count the parent contributed and no more: the child
        // declared this rule itself, so it stands, and the grandchild goes on taking it.
        exec("ALTER TABLE zzy8b1_dp ALTER COLUMN i DROP NOT NULL");
        assertEquals("zzy8b1_dc:i/true/true/1,zzy8b1_dc:j/true/true/1,zzy8b1_dc:k/true/true/1,"
                        + "zzy8b1_dg:i/true/false/1,zzy8b1_dg:j/true/false/1,"
                        + "zzy8b1_dg:k/true/false/1,zzy8b1_dp:i/false/true/0,"
                        + "zzy8b1_dp:j/true/true/0,zzy8b1_dp:k/false/true/0",
                attsAcross("'zzy8b1_dp','zzy8b1_dc','zzy8b1_dg'"));
        assertEquals("zzy8b1_dc:zzy8b1_dc_i_not_null/true/0,"
                        + "zzy8b1_dc:zzy8b1_dc_k_not_null/true/0,"
                        + "zzy8b1_dc:zzy8b1_dp_j_not_null/false/1,"
                        + "zzy8b1_dg:zzy8b1_dc_i_not_null/false/1,"
                        + "zzy8b1_dg:zzy8b1_dc_k_not_null/false/1,"
                        + "zzy8b1_dg:zzy8b1_dp_j_not_null/false/1,"
                        + "zzy8b1_dp:zzy8b1_dp_j_not_null/true/0",
                notNullsAcross("'zzy8b1_dp','zzy8b1_dc','zzy8b1_dg'"));

        // The parent takes the null it no longer refuses; the descendants go on refusing it.
        exec("INSERT INTO zzy8b1_dp (j) VALUES (1)");
        assertEquals("null value in column \"i\" of relation \"zzy8b1_dc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_dc (j, k) VALUES (1, 1)"));
        assertEquals("null value in column \"i\" of relation \"zzy8b1_dg\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_dg (j, k) VALUES (1, 1)"));

        // Nothing above declares it any more, so the child may withdraw it, and that reaches
        // the grandchild.
        exec("ALTER TABLE zzy8b1_dc ALTER COLUMN i DROP NOT NULL");
        assertEquals("zzy8b1_dc:zzy8b1_dc_k_not_null/true/0,"
                        + "zzy8b1_dc:zzy8b1_dp_j_not_null/false/1,"
                        + "zzy8b1_dg:zzy8b1_dc_k_not_null/false/1,"
                        + "zzy8b1_dg:zzy8b1_dp_j_not_null/false/1",
                notNullsAcross("'zzy8b1_dc','zzy8b1_dg'"));
        exec("INSERT INTO zzy8b1_dg (j, k) VALUES (1, 1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_dg"));

        exec("DROP TABLE zzy8b1_dg");
        exec("DROP TABLE zzy8b1_dc");
        exec("DROP TABLE zzy8b1_dp");
    }

    @Test
    void onlyLeavesTheFirstGenerationHoldingTheRuleUnderTheNameItHas() throws Exception {
        exec("CREATE TABLE zzy8b1_op (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_oc (i int NOT NULL, j int, k int NOT NULL)"
                + " INHERITS (zzy8b1_op)");
        exec("CREATE TABLE zzy8b1_og () INHERITS (zzy8b1_oc)");

        exec("ALTER TABLE ONLY zzy8b1_op ALTER COLUMN j DROP NOT NULL");
        exec("ALTER TABLE ONLY zzy8b1_op ALTER COLUMN i DROP NOT NULL");

        // j is the child's own from now on, and keeps the name the parent gave it. Only the
        // first generation is told anything: the grandchild goes on taking both from the child.
        assertEquals("zzy8b1_oc:i/true/true/1,zzy8b1_oc:j/true/true/1,zzy8b1_oc:k/true/true/1,"
                        + "zzy8b1_og:i/true/false/1,zzy8b1_og:j/true/false/1,"
                        + "zzy8b1_og:k/true/false/1,zzy8b1_op:i/false/true/0,"
                        + "zzy8b1_op:j/false/true/0,zzy8b1_op:k/false/true/0",
                attsAcross("'zzy8b1_op','zzy8b1_oc','zzy8b1_og'"));
        assertEquals("zzy8b1_oc:zzy8b1_oc_i_not_null/true/0,"
                        + "zzy8b1_oc:zzy8b1_oc_k_not_null/true/0,"
                        + "zzy8b1_oc:zzy8b1_op_j_not_null/true/0,"
                        + "zzy8b1_og:zzy8b1_oc_i_not_null/false/1,"
                        + "zzy8b1_og:zzy8b1_oc_k_not_null/false/1,"
                        + "zzy8b1_og:zzy8b1_op_j_not_null/false/1",
                notNullsAcross("'zzy8b1_op','zzy8b1_oc','zzy8b1_og'"));
        assertEquals("null value in column \"j\" of relation \"zzy8b1_oc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_oc (i, k) VALUES (1, 1)"));
        assertEquals("null value in column \"j\" of relation \"zzy8b1_og\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_og (i, k) VALUES (1, 1)"));

        // The child owns it now, and withdrawing it reaches the grandchild.
        exec("ALTER TABLE zzy8b1_oc ALTER COLUMN j DROP NOT NULL");
        assertEquals("zzy8b1_oc:zzy8b1_oc_i_not_null/true/0,"
                        + "zzy8b1_oc:zzy8b1_oc_k_not_null/true/0,"
                        + "zzy8b1_og:zzy8b1_oc_i_not_null/false/1,"
                        + "zzy8b1_og:zzy8b1_oc_k_not_null/false/1",
                notNullsAcross("'zzy8b1_oc','zzy8b1_og'"));
        exec("INSERT INTO zzy8b1_og (i, k) VALUES (1, 1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_og"));

        exec("DROP TABLE zzy8b1_og");
        exec("DROP TABLE zzy8b1_oc");
        exec("DROP TABLE zzy8b1_op");
    }

    @Test
    void aRuleMadeTheChildsByOnlySurvivesLeavingTheHierarchy() throws Exception {
        exec("CREATE TABLE zzy8b1_wp (i int, j int NOT NULL)");
        exec("CREATE TABLE zzy8b1_wc (i int, j int) INHERITS (zzy8b1_wp)");

        exec("ALTER TABLE ONLY zzy8b1_wp ALTER COLUMN j DROP NOT NULL");
        assertEquals("zzy8b1_wp_j_not_null/true/0", notNullsOf("zzy8b1_wc"));
        // Leaving the hierarchy leaves the rule, and the name it was given, standing.
        exec("ALTER TABLE zzy8b1_wc NO INHERIT zzy8b1_wp");
        assertEquals("zzy8b1_wp_j_not_null/true/0", notNullsOf("zzy8b1_wc"));
        assertEquals("null value in column \"j\" of relation \"zzy8b1_wc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_wc (i) VALUES (1)"));

        exec("ALTER TABLE zzy8b1_wc ALTER COLUMN j DROP NOT NULL");
        assertNull(notNullsOf("zzy8b1_wc"));
        exec("INSERT INTO zzy8b1_wc (i) VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_wc"));

        exec("DROP TABLE zzy8b1_wc");
        exec("DROP TABLE zzy8b1_wp");
    }

    @Test
    void theNamedSpellingOfTheDropAnswersTheSameWayOnlyAndAll() throws Exception {
        exec("CREATE TABLE zzy8b1_np2 (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_nc2 (i int NOT NULL, j int, k int NOT NULL)"
                + " INHERITS (zzy8b1_np2)");
        exec("CREATE TABLE zzy8b1_ng2 () INHERITS (zzy8b1_nc2)");

        exec("ALTER TABLE ONLY zzy8b1_np2 DROP CONSTRAINT zzy8b1_np2_j_not_null");
        assertEquals("zzy8b1_nc2:zzy8b1_nc2_i_not_null/true/1,"
                        + "zzy8b1_nc2:zzy8b1_nc2_k_not_null/true/0,"
                        + "zzy8b1_nc2:zzy8b1_np2_j_not_null/true/0,"
                        + "zzy8b1_ng2:zzy8b1_nc2_i_not_null/false/1,"
                        + "zzy8b1_ng2:zzy8b1_nc2_k_not_null/false/1,"
                        + "zzy8b1_ng2:zzy8b1_np2_j_not_null/false/1,"
                        + "zzy8b1_np2:zzy8b1_np2_i_not_null/true/0",
                notNullsAcross("'zzy8b1_np2','zzy8b1_nc2','zzy8b1_ng2'"));

        // Without ONLY it reaches the descendants and leaves their own standing.
        exec("ALTER TABLE zzy8b1_np2 DROP CONSTRAINT zzy8b1_np2_i_not_null");
        assertEquals("zzy8b1_nc2:zzy8b1_nc2_i_not_null/true/0,"
                        + "zzy8b1_nc2:zzy8b1_nc2_k_not_null/true/0,"
                        + "zzy8b1_nc2:zzy8b1_np2_j_not_null/true/0,"
                        + "zzy8b1_ng2:zzy8b1_nc2_i_not_null/false/1,"
                        + "zzy8b1_ng2:zzy8b1_nc2_k_not_null/false/1,"
                        + "zzy8b1_ng2:zzy8b1_np2_j_not_null/false/1",
                notNullsAcross("'zzy8b1_np2','zzy8b1_nc2','zzy8b1_ng2'"));
        assertEquals("zzy8b1_nc2:i/true,zzy8b1_nc2:j/true,zzy8b1_nc2:k/true,"
                        + "zzy8b1_ng2:i/true,zzy8b1_ng2:j/true,zzy8b1_ng2:k/true,"
                        + "zzy8b1_np2:i/false,zzy8b1_np2:j/false,zzy8b1_np2:k/false",
                scalar("SELECT string_agg(cl.relname||':'||a.attname||'/'||a.attnotnull::text,"
                        + " ',' ORDER BY cl.relname, a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class cl ON a.attrelid = cl.oid WHERE cl.relname IN"
                        + " ('zzy8b1_np2','zzy8b1_nc2','zzy8b1_ng2') AND a.attnum > 0"
                        + " AND NOT a.attisdropped"));

        assertEquals("null value in column \"j\" of relation \"zzy8b1_ng2\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_ng2 (i, k) VALUES (1, 1)"));
        exec("INSERT INTO zzy8b1_np2 (i) VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy8b1_np2"));

        exec("DROP TABLE zzy8b1_ng2");
        exec("DROP TABLE zzy8b1_nc2");
        exec("DROP TABLE zzy8b1_np2");
    }

    @Test
    void alterTableInheritAsksTheChildToBeNoLessStrictAlready() throws Exception {
        exec("CREATE TABLE zzy8b1_ip (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_ic (i int NOT NULL, j int, k int NOT NULL)");
        exec("INSERT INTO zzy8b1_ic (i, k) VALUES (1, 1)");

        // The INHERITS clause merges the parent's rule in; ALTER TABLE ... INHERIT refuses
        // instead, because the table it is being pointed at may already hold nulls in j.
        assertEquals("42804", stateOf("ALTER TABLE zzy8b1_ic INHERIT zzy8b1_ip"));
        assertEquals("column \"j\" in child table \"zzy8b1_ic\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzy8b1_ic INHERIT zzy8b1_ip"));
        // The refusal leaves the relation as it found it.
        assertEquals("i/true/true/0,j/false/true/0,k/true/true/0", attsOf("zzy8b1_ic"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_inherits h"
                + " JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzy8b1_ic'"));

        exec("DELETE FROM zzy8b1_ic");
        exec("ALTER TABLE zzy8b1_ic ALTER COLUMN j SET NOT NULL");
        exec("ALTER TABLE zzy8b1_ic INHERIT zzy8b1_ip");
        assertEquals("zzy8b1_ic_i_not_null/true/1,zzy8b1_ic_j_not_null/true/1,"
                + "zzy8b1_ic_k_not_null/true/0", notNullsOf("zzy8b1_ic"));
        assertEquals("i/true/true/1,j/true/true/1,k/true/true/1", attsOf("zzy8b1_ic"));

        // What it declared for itself is now a rule a parent declares as well.
        assertEquals("cannot drop inherited constraint \"zzy8b1_ic_j_not_null\""
                        + " of relation \"zzy8b1_ic\"",
                messageOf("ALTER TABLE zzy8b1_ic ALTER COLUMN j DROP NOT NULL"));
        exec("ALTER TABLE zzy8b1_ic NO INHERIT zzy8b1_ip");
        assertEquals("zzy8b1_ic_i_not_null/true/0,zzy8b1_ic_j_not_null/true/0,"
                + "zzy8b1_ic_k_not_null/true/0", notNullsOf("zzy8b1_ic"));
        exec("ALTER TABLE zzy8b1_ic ALTER COLUMN j DROP NOT NULL");
        assertEquals("zzy8b1_ic_i_not_null/true/0,zzy8b1_ic_k_not_null/true/0",
                notNullsOf("zzy8b1_ic"));

        exec("DROP TABLE zzy8b1_ic");
        exec("DROP TABLE zzy8b1_ip");
    }

    @Test
    void aParentThatTakesTheRuleOnLaterReachesEveryChildItHas() throws Exception {
        exec("CREATE TABLE zzy8b1_sp (i int, j int)");
        exec("CREATE TABLE zzy8b1_sc (i int, j int) INHERITS (zzy8b1_sp)");
        exec("INSERT INTO zzy8b1_sc (j) VALUES (1)");

        // The rows of every descendant are read before the rule is taken on.
        assertEquals("23502", stateOf("ALTER TABLE zzy8b1_sp ALTER COLUMN i SET NOT NULL"));
        assertEquals("column \"i\" of relation \"zzy8b1_sc\" contains null values",
                messageOf("ALTER TABLE zzy8b1_sp ALTER COLUMN i SET NOT NULL"));

        exec("DELETE FROM zzy8b1_sc");
        exec("ALTER TABLE zzy8b1_sp ALTER COLUMN i SET NOT NULL");
        assertEquals("zzy8b1_sc:zzy8b1_sp_i_not_null/false/1,"
                        + "zzy8b1_sp:zzy8b1_sp_i_not_null/true/0",
                notNullsAcross("'zzy8b1_sp','zzy8b1_sc'"));
        assertEquals("zzy8b1_sc:i/true/true/1,zzy8b1_sc:j/false/true/1,"
                        + "zzy8b1_sp:i/true/true/0,zzy8b1_sp:j/false/true/0",
                attsAcross("'zzy8b1_sp','zzy8b1_sc'"));
        assertEquals("null value in column \"i\" of relation \"zzy8b1_sc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_sc (j) VALUES (1)"));
        assertEquals("cannot drop inherited constraint \"zzy8b1_sp_i_not_null\""
                        + " of relation \"zzy8b1_sc\"",
                messageOf("ALTER TABLE zzy8b1_sc ALTER COLUMN i DROP NOT NULL"));

        // ONLY takes the rule on for the relation it names and for nobody below.
        exec("ALTER TABLE ONLY zzy8b1_sp ALTER COLUMN j SET NOT NULL");
        assertEquals("zzy8b1_sc:zzy8b1_sp_i_not_null/false/1,"
                        + "zzy8b1_sp:zzy8b1_sp_i_not_null/true/0,"
                        + "zzy8b1_sp:zzy8b1_sp_j_not_null/true/0",
                notNullsAcross("'zzy8b1_sp','zzy8b1_sc'"));

        exec("DROP TABLE zzy8b1_sc");
        exec("DROP TABLE zzy8b1_sp");
    }

    @Test
    void aPartitionDeclaresNothingOfItsOwnAndIsRefusedInWordsOfItsOwn() throws Exception {
        exec("CREATE TABLE zzy8b1_pt (i int NOT NULL, j int NOT NULL, k int)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy8b1_pa PARTITION OF zzy8b1_pt FOR VALUES FROM (1) TO (10)");

        // A partition has no column list of its own, so nothing about it is local.
        assertEquals("i/true/false/1,j/true/false/1,k/false/false/1", attsOf("zzy8b1_pa"));
        assertEquals("zzy8b1_pt_i_not_null/false/1/false,zzy8b1_pt_j_not_null/false/1/false",
                notNullsWithReachOf("zzy8b1_pa"));

        // A partition is refused in words of its own, naming the column rather than the rule.
        assertEquals("42P16", stateOf("ALTER TABLE zzy8b1_pa ALTER COLUMN j DROP NOT NULL"));
        assertEquals("column \"j\" is marked NOT NULL in parent table",
                messageOf("ALTER TABLE zzy8b1_pa ALTER COLUMN j DROP NOT NULL"));
        assertEquals("cannot drop inherited constraint \"zzy8b1_pt_j_not_null\""
                        + " of relation \"zzy8b1_pa\"",
                messageOf("ALTER TABLE zzy8b1_pa DROP CONSTRAINT zzy8b1_pt_j_not_null"));

        assertEquals("null value in column \"j\" of relation \"zzy8b1_pa\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_pa (i) VALUES (1)"));
        // Written through the partitioned table, the refusal names the partition it landed in.
        assertEquals("null value in column \"j\" of relation \"zzy8b1_pa\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_pt (i) VALUES (1)"));
        assertEquals("0", scalar("SELECT count(*) FROM zzy8b1_pa"));

        exec("DROP TABLE zzy8b1_pt");
    }

    @Test
    void attachPartitionAsksTheTableToBeNoLessStrictAlready() throws Exception {
        exec("CREATE TABLE zzy8b1_qt (i int NOT NULL, j int NOT NULL, k int)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy8b1_qb (i int NOT NULL, j int, k int NOT NULL)");
        exec("INSERT INTO zzy8b1_qb (i, k) VALUES (11, 1)");

        assertEquals("42804", stateOf("ALTER TABLE zzy8b1_qt ATTACH PARTITION zzy8b1_qb"
                + " FOR VALUES FROM (10) TO (20)"));
        assertEquals("column \"j\" in child table \"zzy8b1_qb\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzy8b1_qt ATTACH PARTITION zzy8b1_qb"
                        + " FOR VALUES FROM (10) TO (20)"));
        assertEquals("i/true/true/0,j/false/true/0,k/true/true/0", attsOf("zzy8b1_qb"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_inherits h"
                + " JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzy8b1_qb'"));

        exec("DELETE FROM zzy8b1_qb");
        exec("ALTER TABLE zzy8b1_qb ALTER COLUMN j SET NOT NULL");
        exec("ALTER TABLE zzy8b1_qt ATTACH PARTITION zzy8b1_qb FOR VALUES FROM (10) TO (20)");

        // What it declared is now recorded as coming from the partitioned table.
        assertEquals("zzy8b1_qb_i_not_null/false/1,zzy8b1_qb_j_not_null/false/1,"
                + "zzy8b1_qb_k_not_null/true/0", notNullsOf("zzy8b1_qb"));
        assertEquals("i/true/false/1,j/true/false/1,k/true/false/1", attsOf("zzy8b1_qb"));
        assertEquals("column \"j\" is marked NOT NULL in parent table",
                messageOf("ALTER TABLE zzy8b1_qb ALTER COLUMN j DROP NOT NULL"));

        // k is the partition's alone, and that one it may withdraw.
        exec("ALTER TABLE zzy8b1_qb ALTER COLUMN k DROP NOT NULL");
        assertEquals("zzy8b1_qb_i_not_null/false/1,zzy8b1_qb_j_not_null/false/1",
                notNullsOf("zzy8b1_qb"));

        // Withdrawn from the partitioned table, every rule it holds is its own again.
        exec("ALTER TABLE zzy8b1_qt DETACH PARTITION zzy8b1_qb");
        assertEquals("zzy8b1_qb_i_not_null/true/0,zzy8b1_qb_j_not_null/true/0",
                notNullsOf("zzy8b1_qb"));
        assertEquals("i/true/true/0,j/true/true/0,k/false/true/0", attsOf("zzy8b1_qb"));

        exec("DROP TABLE zzy8b1_qb");
        exec("DROP TABLE zzy8b1_qt");
    }

    @Test
    void aPrimaryKeyOnTheChildIsADeclarationOfItsOwn() throws Exception {
        exec("CREATE TABLE zzy8b1_kp (i int NOT NULL, j int NOT NULL, k int)");
        exec("CREATE TABLE zzy8b1_kc (j int PRIMARY KEY) INHERITS (zzy8b1_kp)");

        assertEquals("i/true/false/1,j/true/true/1,k/false/false/1", attsOf("zzy8b1_kc"));
        assertEquals("zzy8b1_kc_j_not_null/n/true/1,zzy8b1_kc_pkey/p/true/0,"
                        + "zzy8b1_kp_i_not_null/n/false/1",
                scalar("SELECT string_agg(c.conname||'/'||c.contype::text||'/'"
                        + "||c.conislocal::text||'/'||c.coninhcount::text, ',' ORDER BY c.conname)"
                        + " FROM pg_constraint c JOIN pg_class cl ON c.conrelid = cl.oid"
                        + " WHERE cl.relname = 'zzy8b1_kc'"));

        assertEquals("null value in column \"j\" of relation \"zzy8b1_kc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy8b1_kc (i) VALUES (1)"));

        exec("DROP TABLE zzy8b1_kc");
        exec("DROP TABLE zzy8b1_kp");
    }

    // ------------------------------------------------------------ A NOT NULL a parent takes on under a name of its own is every descendant's

    /** Runs a body inside one transaction and takes it all back again. */
    private static void rolledBack(SqlBody body) throws Exception {
        conn.setAutoCommit(false);
        try {
            body.run();
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /** A body of statements a test hands to {@link #rolledBack}. */
    private interface SqlBody {
        void run() throws Exception;
    }

    @Test
    void aNamedNotNullAddedToAParentIsTheRuleOfEveryRelationBeneathIt() throws Exception {
        exec("CREATE TABLE zzy9nn_gp (i int, j int)");
        exec("CREATE TABLE zzy9nn_gc (i int, j int) INHERITS (zzy9nn_gp)");
        exec("CREATE TABLE zzy9nn_gg () INHERITS (zzy9nn_gc)");
        exec("CREATE TABLE zzy9nn_gs (i int, j int) INHERITS (zzy9nn_gp)");
        exec("ALTER TABLE zzy9nn_gp ADD CONSTRAINT zzy9nn_gn NOT NULL i");

        // The grandchild and the second child hold it too, each under the name it was made with.
        assertEquals("zzy9nn_gc:zzy9nn_gn/false/1,zzy9nn_gg:zzy9nn_gn/false/1,"
                        + "zzy9nn_gp:zzy9nn_gn/true/0,zzy9nn_gs:zzy9nn_gn/false/1",
                notNullsAcross("'zzy9nn_gp','zzy9nn_gc','zzy9nn_gg','zzy9nn_gs'"));
        assertEquals("zzy9nn_gc:i/true/true/1,zzy9nn_gc:j/false/true/1,"
                        + "zzy9nn_gg:i/true/false/1,zzy9nn_gg:j/false/false/1,"
                        + "zzy9nn_gp:i/true/true/0,zzy9nn_gp:j/false/true/0,"
                        + "zzy9nn_gs:i/true/true/1,zzy9nn_gs:j/false/true/1",
                attsAcross("'zzy9nn_gp','zzy9nn_gc','zzy9nn_gg','zzy9nn_gs'"));

        // And the rule is enforced two generations down and on the side branch.
        assertEquals("23502", stateOf("INSERT INTO zzy9nn_gg (j) VALUES (1)"));
        assertEquals("null value in column \"i\" of relation \"zzy9nn_gg\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_gg (j) VALUES (1)"));
        assertEquals("null value in column \"i\" of relation \"zzy9nn_gs\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_gs (j) VALUES (1)"));
        exec("INSERT INTO zzy9nn_gg (i, j) VALUES (1, 1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_gp"));

        // It is the parent's to withdraw, so the grandchild is sent back to the parent...
        assertEquals("42P16", stateOf("ALTER TABLE zzy9nn_gg ALTER COLUMN i DROP NOT NULL"));
        assertEquals("cannot drop inherited constraint \"zzy9nn_gn\" of relation \"zzy9nn_gg\"",
                messageOf("ALTER TABLE zzy9nn_gg ALTER COLUMN i DROP NOT NULL"));
        // ...and withdrawing it reaches every relation it had reached.
        exec("ALTER TABLE zzy9nn_gp DROP CONSTRAINT zzy9nn_gn");
        assertNull(notNullsAcross("'zzy9nn_gp','zzy9nn_gc','zzy9nn_gg','zzy9nn_gs'"));
        exec("INSERT INTO zzy9nn_gg (j) VALUES (2)");
        assertEquals("2", scalar("SELECT count(*) FROM zzy9nn_gg"));

        exec("DROP TABLE zzy9nn_gg");
        exec("DROP TABLE zzy9nn_gs");
        exec("DROP TABLE zzy9nn_gc");
        exec("DROP TABLE zzy9nn_gp");
    }

    @Test
    void theRowsBeneathDecideWhetherANamedNotNullCanBeDeclaredAtAll() throws Exception {
        exec("CREATE TABLE zzy9nn_wp (i int, j int)");
        exec("CREATE TABLE zzy9nn_wc (i int, j int) INHERITS (zzy9nn_wp)");
        exec("INSERT INTO zzy9nn_wc (j) VALUES (5)");

        // The rows of every descendant are read before the rule is stored, and the refusal
        // names the relation whose rows are in the way.
        assertEquals("23502",
                stateOf("ALTER TABLE zzy9nn_wp ADD CONSTRAINT zzy9nn_wn NOT NULL i"));
        assertEquals("column \"i\" of relation \"zzy9nn_wc\" contains null values",
                messageOf("ALTER TABLE zzy9nn_wp ADD CONSTRAINT zzy9nn_wn NOT NULL i"));
        // ONLY asks for a rule the relations below would not take on, which PostgreSQL will not
        // store either -- and it says so without a hint.
        assertEquals("42P16",
                stateOf("ALTER TABLE ONLY zzy9nn_wp ADD CONSTRAINT zzy9nn_wn NOT NULL i"));
        assertEquals("constraint must be added to child tables too",
                messageOf("ALTER TABLE ONLY zzy9nn_wp ADD CONSTRAINT zzy9nn_wn NOT NULL i"));
        assertNull(hintOf("ALTER TABLE ONLY zzy9nn_wp ADD CONSTRAINT zzy9nn_wn NOT NULL i"));
        assertNull(notNullsAcross("'zzy9nn_wp','zzy9nn_wc'"));

        exec("DELETE FROM zzy9nn_wc");
        exec("ALTER TABLE zzy9nn_wp ADD CONSTRAINT zzy9nn_wn NOT NULL i");
        // A column that already refuses a null has nothing left to hand down, so ONLY is taken
        // there -- and adds nothing: the hierarchy still holds the one rule already made.
        exec("ALTER TABLE ONLY zzy9nn_wp ADD CONSTRAINT zzy9nn_wn2 NOT NULL i");
        assertEquals("zzy9nn_wc:zzy9nn_wn/false/1,zzy9nn_wp:zzy9nn_wn/true/0",
                notNullsAcross("'zzy9nn_wp','zzy9nn_wc'"));

        assertEquals("null value in column \"i\" of relation \"zzy9nn_wc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_wc (j) VALUES (5)"));
        exec("INSERT INTO zzy9nn_wc (i, j) VALUES (1, 5)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_wp"));

        exec("DROP TABLE zzy9nn_wc");
        exec("DROP TABLE zzy9nn_wp");
    }

    @Test
    void aNamedNotNullOnAPartitionedTableReachesTheLeafUnderASubPartition() throws Exception {
        exec("CREATE TABLE zzy9nn_pp (i int, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy9nn_p1 PARTITION OF zzy9nn_pp FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy9nn_p2 PARTITION OF zzy9nn_p1 FOR VALUES FROM (0) TO (5)");
        exec("ALTER TABLE zzy9nn_pp ADD CONSTRAINT zzy9nn_pn NOT NULL j");

        assertEquals("zzy9nn_p1:zzy9nn_pn/false/1,zzy9nn_p2:zzy9nn_pn/false/1,"
                        + "zzy9nn_pp:zzy9nn_pn/true/0",
                notNullsAcross("'zzy9nn_pp','zzy9nn_p1','zzy9nn_p2'"));

        // The row is refused in the leaf it was routed to, and that leaf is the one named.
        assertEquals("23502", stateOf("INSERT INTO zzy9nn_pp (i) VALUES (1)"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_p2\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_pp (i) VALUES (1)"));
        exec("INSERT INTO zzy9nn_pp (i, j) VALUES (1, 1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_p2"));

        exec("DROP TABLE zzy9nn_pp");
    }

    // ------------------------------------------------------------ A change to a NOT NULL that is rolled back leaves the hierarchy as it was

    @Test
    void aRolledBackDropNotNullLeavesEveryRelationRefusingTheNullAgain() throws Exception {
        exec("CREATE TABLE zzy9nn_rp (i int NOT NULL, j int)");
        exec("CREATE TABLE zzy9nn_rc (i int, j int) INHERITS (zzy9nn_rp)");

        rolledBack(() -> {
            exec("ALTER TABLE zzy9nn_rp ALTER COLUMN i DROP NOT NULL");
            // Inside the transaction the child has let go of it too, and takes the null.
            assertNull(notNullsAcross("'zzy9nn_rp','zzy9nn_rc'"));
            exec("INSERT INTO zzy9nn_rc (j) VALUES (99)");
            assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_rc"));
        });

        assertEquals("zzy9nn_rc:zzy9nn_rp_i_not_null/false/1,"
                        + "zzy9nn_rp:zzy9nn_rp_i_not_null/true/0",
                notNullsAcross("'zzy9nn_rp','zzy9nn_rc'"));
        assertEquals("i/true/true/1,j/false/true/1", attsOf("zzy9nn_rc"));
        assertEquals("0", scalar("SELECT count(*) FROM zzy9nn_rc"));
        assertEquals("23502", stateOf("INSERT INTO zzy9nn_rc (j) VALUES (1)"));
        assertEquals("null value in column \"i\" of relation \"zzy9nn_rc\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_rc (j) VALUES (1)"));

        // The named spelling of the drop is undone the same way.
        rolledBack(() -> exec("ALTER TABLE zzy9nn_rp DROP CONSTRAINT zzy9nn_rp_i_not_null"));
        assertEquals("zzy9nn_rc:zzy9nn_rp_i_not_null/false/1,"
                        + "zzy9nn_rp:zzy9nn_rp_i_not_null/true/0",
                notNullsAcross("'zzy9nn_rp','zzy9nn_rc'"));
        assertEquals("23502", stateOf("INSERT INTO zzy9nn_rc (j) VALUES (1)"));

        // ...and so is a savepoint rolled back inside a transaction that goes on to commit.
        conn.setAutoCommit(false);
        try {
            java.sql.Savepoint sp = conn.setSavepoint("zzy9nn_s");
            exec("ALTER TABLE zzy9nn_rp ALTER COLUMN i DROP NOT NULL");
            assertNull(notNullsAcross("'zzy9nn_rp','zzy9nn_rc'"));
            conn.rollback(sp);
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("zzy9nn_rc:zzy9nn_rp_i_not_null/false/1,"
                        + "zzy9nn_rp:zzy9nn_rp_i_not_null/true/0",
                notNullsAcross("'zzy9nn_rp','zzy9nn_rc'"));
        assertEquals("23502", stateOf("INSERT INTO zzy9nn_rc (j) VALUES (1)"));
        assertEquals("0", scalar("SELECT count(*) FROM zzy9nn_rc"));

        exec("DROP TABLE zzy9nn_rc");
        exec("DROP TABLE zzy9nn_rp");
    }

    @Test
    void aRolledBackSetNotNullLeavesTheColumnTakingANullAgain() throws Exception {
        exec("CREATE TABLE zzy9nn_sp (i int, j int)");
        exec("CREATE TABLE zzy9nn_sc (i int, j int) INHERITS (zzy9nn_sp)");

        rolledBack(() -> {
            exec("ALTER TABLE zzy9nn_sp ALTER COLUMN i SET NOT NULL");
            assertEquals("zzy9nn_sc:zzy9nn_sp_i_not_null/false/1,"
                            + "zzy9nn_sp:zzy9nn_sp_i_not_null/true/0",
                    notNullsAcross("'zzy9nn_sp','zzy9nn_sc'"));
        });

        assertNull(notNullsAcross("'zzy9nn_sp','zzy9nn_sc'"));
        assertEquals("i/false/true/1,j/false/true/1", attsOf("zzy9nn_sc"));
        exec("INSERT INTO zzy9nn_sc (j) VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_sc"));

        // A rolled-back ADD CONSTRAINT is undone on the child as well.
        rolledBack(() -> exec("ALTER TABLE zzy9nn_sp ADD CONSTRAINT zzy9nn_sn NOT NULL j"));
        assertNull(notNullsAcross("'zzy9nn_sp','zzy9nn_sc'"));
        exec("INSERT INTO zzy9nn_sc (i) VALUES (2)");
        assertEquals("2", scalar("SELECT count(*) FROM zzy9nn_sc"));

        exec("DROP TABLE zzy9nn_sc");
        exec("DROP TABLE zzy9nn_sp");
    }

    // ------------------------------------------------------------ A NOT NULL two parents declare, and the name it answers to when one lets go

    @Test
    void aMergedNotNullKeepsTheNameItWasMadeWithWhenOneParentLetsGo() throws Exception {
        exec("CREATE TABLE zzy9nn_z1 (i int NOT NULL)");
        exec("CREATE TABLE zzy9nn_z2 (i int NOT NULL)");
        exec("CREATE TABLE zzy9nn_z0 () INHERITS (zzy9nn_z1, zzy9nn_z2)");

        assertEquals("zzy9nn_z1_i_not_null/false/2", notNullsOf("zzy9nn_z0"));
        exec("ALTER TABLE zzy9nn_z1 ALTER COLUMN i DROP NOT NULL");
        // The count falls by one; the name is still the one the constraint was created with.
        assertEquals("zzy9nn_z1_i_not_null/false/1", notNullsOf("zzy9nn_z0"));
        assertEquals("i/true/false/2", attsOf("zzy9nn_z0"));

        // The other parent goes on declaring it, so the child may not withdraw it, and is
        // refused under the name the rule has carried since it was made.
        assertEquals("42P16", stateOf("ALTER TABLE zzy9nn_z0 ALTER COLUMN i DROP NOT NULL"));
        assertEquals("cannot drop inherited constraint \"zzy9nn_z1_i_not_null\""
                        + " of relation \"zzy9nn_z0\"",
                messageOf("ALTER TABLE zzy9nn_z0 ALTER COLUMN i DROP NOT NULL"));
        assertEquals("null value in column \"i\" of relation \"zzy9nn_z0\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_z0 VALUES (NULL)"));
        exec("INSERT INTO zzy9nn_z1 VALUES (NULL)");

        // With the last parent letting go, nothing is left on the child at all.
        exec("ALTER TABLE zzy9nn_z2 ALTER COLUMN i DROP NOT NULL");
        assertNull(notNullsOf("zzy9nn_z0"));
        exec("INSERT INTO zzy9nn_z0 VALUES (NULL)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_z0"));

        exec("DROP TABLE zzy9nn_z0");
        exec("DROP TABLE zzy9nn_z1");
        exec("DROP TABLE zzy9nn_z2");
    }

    // ------------------------------------------------------------ A partition's own NOT NULL, and the rule a table takes on when it is attached

    @Test
    void aPartitionThatDeclaredNotNullFirstHoldsARuleOfItsOwnAfterwards() throws Exception {
        exec("CREATE TABLE zzy9nn_mp (i int, j int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy9nn_ma PARTITION OF zzy9nn_mp FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzy9nn_ma ALTER COLUMN j SET NOT NULL");

        // The partitioned table declares nothing, and the row is refused all the same.
        assertEquals("zzy9nn_ma:zzy9nn_ma_j_not_null/true/0",
                notNullsAcross("'zzy9nn_mp','zzy9nn_ma'"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_ma\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_mp (i) VALUES (1)"));

        // The partitioned table taking it on adds a count and leaves the rule the partition's.
        exec("ALTER TABLE zzy9nn_mp ALTER COLUMN j SET NOT NULL");
        assertEquals("zzy9nn_ma:zzy9nn_ma_j_not_null/true/1,"
                        + "zzy9nn_mp:zzy9nn_mp_j_not_null/true/0",
                notNullsAcross("'zzy9nn_mp','zzy9nn_ma'"));
        assertEquals("i/false/false/1,j/true/false/1", attsOf("zzy9nn_ma"));
        // While the partitioned table declares it, the partition is refused in words of its own.
        assertEquals("42P16", stateOf("ALTER TABLE zzy9nn_ma ALTER COLUMN j DROP NOT NULL"));
        assertEquals("column \"j\" is marked NOT NULL in parent table",
                messageOf("ALTER TABLE zzy9nn_ma ALTER COLUMN j DROP NOT NULL"));

        // The partitioned table letting go takes away the count, not the rule.
        exec("ALTER TABLE zzy9nn_mp ALTER COLUMN j DROP NOT NULL");
        assertEquals("zzy9nn_ma:zzy9nn_ma_j_not_null/true/0",
                notNullsAcross("'zzy9nn_mp','zzy9nn_ma'"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_ma\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_mp (i) VALUES (1)"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_ma\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_ma (i) VALUES (2)"));

        // It is the partition's own, so the partition may withdraw it now.
        exec("ALTER TABLE zzy9nn_ma ALTER COLUMN j DROP NOT NULL");
        assertNull(notNullsOf("zzy9nn_ma"));
        exec("INSERT INTO zzy9nn_mp (i) VALUES (3)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_ma"));

        exec("DROP TABLE zzy9nn_mp");
    }

    @Test
    void aTableAttachedToAPartitionedTableHoldsItsRuleUnderTheNameItAlreadyHad()
            throws Exception {
        exec("CREATE TABLE zzy9nn_qp (i int, j int NOT NULL) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy9nn_q1 PARTITION OF zzy9nn_qp FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE zzy9nn_q2 (i int, j int NOT NULL)");
        exec("ALTER TABLE zzy9nn_qp ATTACH PARTITION zzy9nn_q2 FOR VALUES FROM (10) TO (20)");

        // The attached table keeps the name it made the rule with and stops owning it; the
        // partition created below the partitioned table answers to that table's name.
        assertEquals("zzy9nn_q1:zzy9nn_qp_j_not_null/false/1,"
                        + "zzy9nn_q2:zzy9nn_q2_j_not_null/false/1,"
                        + "zzy9nn_qp:zzy9nn_qp_j_not_null/true/0",
                notNullsAcross("'zzy9nn_qp','zzy9nn_q1','zzy9nn_q2'"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_q2\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_qp (i) VALUES (11)"));

        // A table with no such rule cannot join a hierarchy that has one.
        exec("CREATE TABLE zzy9nn_q3 (i int, j int)");
        assertEquals("42804", stateOf("ALTER TABLE zzy9nn_qp ATTACH PARTITION zzy9nn_q3"
                + " FOR VALUES FROM (20) TO (30)"));
        assertEquals("column \"j\" in child table \"zzy9nn_q3\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzy9nn_qp ATTACH PARTITION zzy9nn_q3"
                        + " FOR VALUES FROM (20) TO (30)"));

        // Detached, the rule is the table's own again, under the name it always answered to,
        // and it goes on being enforced until the table itself withdraws it.
        exec("ALTER TABLE zzy9nn_qp DETACH PARTITION zzy9nn_q2");
        assertEquals("zzy9nn_q2_j_not_null/true/0", notNullsOf("zzy9nn_q2"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_q2\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_q2 (i) VALUES (11)"));
        exec("ALTER TABLE zzy9nn_q2 ALTER COLUMN j DROP NOT NULL");
        exec("INSERT INTO zzy9nn_q2 (i) VALUES (11)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_q2"));

        exec("DROP TABLE zzy9nn_q3");
        exec("DROP TABLE zzy9nn_q2");
        exec("DROP TABLE zzy9nn_qp");
    }

    @Test
    void aTableBelongsToOnePartitionedTableOnly() throws Exception {
        exec("CREATE TABLE zzy9nn_pt (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy9nn_pu (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzy9nn_pb (i int)");
        exec("ALTER TABLE zzy9nn_pt ATTACH PARTITION zzy9nn_pb FOR VALUES FROM (0) TO (10)");

        assertEquals("42809", stateOf("ALTER TABLE zzy9nn_pt ATTACH PARTITION zzy9nn_pb"
                + " FOR VALUES FROM (10) TO (20)"));
        assertEquals("\"zzy9nn_pb\" is already a partition",
                messageOf("ALTER TABLE zzy9nn_pt ATTACH PARTITION zzy9nn_pb"
                        + " FOR VALUES FROM (10) TO (20)"));
        assertEquals("\"zzy9nn_pb\" is already a partition",
                messageOf("ALTER TABLE zzy9nn_pu ATTACH PARTITION zzy9nn_pb"
                        + " FOR VALUES FROM (0) TO (10)"));

        exec("DROP TABLE zzy9nn_pu");
        exec("DROP TABLE zzy9nn_pt");
    }

    // ------------------------------------------------------------ A NOT NULL written NO INHERIT beside one taken from a parent

    @Test
    void aNoInheritNotNullCannotStandBesideOneTakenFromAParent() throws Exception {
        exec("CREATE TABLE zzy9nn_hp (i int NOT NULL, j int)");
        exec("CREATE TABLE zzy9nn_hq (i int, j int)");

        String create = "CREATE TABLE zzy9nn_hc (i int NOT NULL NO INHERIT, j int)"
                + " INHERITS (zzy9nn_hp)";
        assertEquals("42804", stateOf(create));
        assertEquals("cannot define not-null constraint with NO INHERIT on column \"i\"",
                messageOf(create));
        assertEquals("The column has an inherited not-null constraint.", detailOf(create));
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzy9nn_hc'"));

        // Only where the parent hands that column's rule down: NO INHERIT elsewhere is taken.
        exec("CREATE TABLE zzy9nn_h1 (i int NOT NULL NO INHERIT, j int) INHERITS (zzy9nn_hq)");
        exec("CREATE TABLE zzy9nn_h2 (j int NOT NULL NO INHERIT) INHERITS (zzy9nn_hp)");
        assertEquals("zzy9nn_h2_j_not_null/true/0/true,zzy9nn_hp_i_not_null/false/1/false",
                notNullsWithReachOf("zzy9nn_h2"));
        // Both rules are enforced on the relation that holds them.
        assertEquals("null value in column \"i\" of relation \"zzy9nn_h2\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_h2 (j) VALUES (1)"));
        assertEquals("null value in column \"j\" of relation \"zzy9nn_h2\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_h2 (i) VALUES (1)"));
        exec("INSERT INTO zzy9nn_h2 (i, j) VALUES (1, 1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_h2"));

        // The same contradiction reached by ALTER TABLE ... INHERIT, which leaves the relation
        // outside the hierarchy it was refused.
        exec("CREATE TABLE zzy9nn_hd (i int NOT NULL NO INHERIT, j int)");
        assertEquals("42P17", stateOf("ALTER TABLE zzy9nn_hd INHERIT zzy9nn_hp"));
        assertEquals("constraint \"zzy9nn_hd_i_not_null\" conflicts with non-inherited"
                        + " constraint on child table \"zzy9nn_hd\"",
                messageOf("ALTER TABLE zzy9nn_hd INHERIT zzy9nn_hp"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_inherits h"
                + " JOIN pg_class cl ON cl.oid = h.inhrelid WHERE cl.relname = 'zzy9nn_hd'"));

        // A parent that declares nothing of the kind is joined without complaint.
        exec("ALTER TABLE zzy9nn_hd INHERIT zzy9nn_hq");
        assertEquals("zzy9nn_hd_i_not_null/true/0/true", notNullsWithReachOf("zzy9nn_hd"));
        assertEquals("null value in column \"i\" of relation \"zzy9nn_hd\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO zzy9nn_hd (j) VALUES (1)"));
        exec("INSERT INTO zzy9nn_hd (i) VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM zzy9nn_hd"));

        exec("DROP TABLE zzy9nn_hd");
        exec("DROP TABLE zzy9nn_h2");
        exec("DROP TABLE zzy9nn_h1");
        exec("DROP TABLE zzy9nn_hq");
        exec("DROP TABLE zzy9nn_hp");
    }
}
